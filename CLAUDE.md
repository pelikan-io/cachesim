# CLAUDE.md

## Project

Cache trace simulator (`cachesim-rs`) for the [cache-rs](https://github.com/pelikan-io/cache-rs) crate family. Replays Parquet-format traces against segcache, cuckoo-cache, and oracle engines, reporting hit/miss statistics.

Cargo workspace (virtual root `Cargo.toml`, shared versions in `[workspace.dependencies]`):

- `crates/cachetrace` — object-level trace format: Parquet schema, `TraceEntry`, `Op`, `TraceReader`/`TraceWriter`. Importers (libCacheSim binary, cache-trace CSV) behind the `import` feature, default on; the core must build with `--no-default-features` with no direct dependency on `ahash`/`zstd` (both still build through arrow/parquet). `cachetrace::Error` is `#[non_exhaustive]`.
- `crates/kvtrace` — KV-cache request traces: request-level Parquet format, model geometry (HF config → sizes, TP sharding), request → objects mapping (`ObjectMapper`), Mooncake converter, HF fetch behind `hf-fetch` (default on). Must not depend on `cachetrace`, and must build with `--no-default-features` (no HTTP client): a load generator depends on it for keys and value sizes. `kvtrace::Error` is `#[non_exhaustive]`.
- `crates/cachesim` — package `cachesim-rs` (published on crates.io), library `cachesim`, binary `cachesim`. Depends on both. Re-exports `cachetrace` as `cachesim::trace` (keep that path working) and `kvtrace` as `cachesim::kvtrace`. `kv::expand_to_parquet` turns a request trace into an object-level trace. Publish `cachetrace` and `kvtrace` before `cachesim-rs`.
- Rust edition 2021, Apache-2.0

## Build & Test

```bash
cargo build --workspace              # dev build
cargo build --release                # optimized (lto, codegen-units=1)
cargo test --workspace               # all tests
cargo test --workspace --no-default-features
cargo test -p cachetrace --no-default-features   # trace core without importers
cargo test -p kvtrace --no-default-features      # kvtrace without the HTTP client
cargo clippy --workspace --all-targets -- -D warnings   # lint (CI enforces zero warnings)
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo clippy -p cachetrace --all-targets --no-default-features -- -D warnings
cargo clippy -p kvtrace --all-targets --no-default-features -- -D warnings
cargo test --workspace -- --ignored  # network test: fetches a config from huggingface.co
cargo fmt --all --check              # format check (CI enforces)
```

`--workspace --no-default-features` still builds cachetrace with `import`, because cachesim enables it; test the core with `-p cachetrace`.

CI runs on ubuntu, macos, and windows. All four checks (lint, 3x build-and-test) must pass.

## Source Layout

```
crates/cachetrace/src/lib.rs     TraceEntry, Op, trace_schema(), TraceReader/Writer, Error
crates/cachetrace/src/import.rs  libCacheSim binary + cache-trace CSV importers (feature `import`)
crates/kvtrace/src/format.rs     Request trace schema, Request, RequestReader/Writer, kvtrace.* metadata keys
crates/kvtrace/src/geometry.rs   ModelSizes (config.json → sizes), Geometry, Sharding (TP checks)
crates/kvtrace/src/objects.rs    ObjectMapper: request chunks → KvObject (obj_id, size, rank, kind, chunk_index); lookup_ids. obj_id = chunk<<16 | slot<<12 | rank: slot 0 = the chunk's object (combined layout: KV and KV+state writes share it, so one id can have two sizes), slot 1 = separate state object
crates/kvtrace/src/mooncake.rs   Mooncake JSON lines → request trace (full chunks only, chunk_id)
crates/kvtrace/src/output.rs     plan_output: output refusals, temp file + rename
crates/kvtrace/src/hf.rs         Hugging Face config.json fetch + cache (feature `hf-fetch`, default on)
crates/kvtrace/tests/fixtures/   Mooncake trace, model configs (incl. DeepSeek-V3), a pyarrow-written request trace
crates/cachesim/src/lib.rs       Policy enums (SegcachePolicy, CuckooPolicy), Error, `pub use cachetrace as trace`, `pub use kvtrace`
crates/cachesim/src/kv.rs        expand_to_parquet: request trace + Geometry → object-level trace (KV simulation mode goes here)
crates/cachesim/src/oracle.rs    OracleCache with Belady/BeladySize policies
crates/cachesim/src/simulator.rs SimConfig/CuckooConfig, simulate_segcache()/simulate_cuckoo()/simulate_oracle(), SimResult
crates/cachesim/src/main.rs      CLI (clap): simulate, convert, info, kv expand
crates/cachesim/tests/           api_paths.rs (old `cachesim::trace` paths); fixtures/ copies kvtrace fixtures used by kv.rs tests
```

## Architecture

### Engines

Each engine has its own config struct, simulation function, and policy enum:

| Engine | Config | Function | Policy enum | Backing crate |
|--------|--------|----------|-------------|---------------|
| segcache | `SimConfig` | `simulate_segcache()` | `SegcachePolicy` | `segcache` (crates.io) |
| cuckoo | `CuckooConfig` | `simulate_cuckoo()` | `CuckooPolicy` | `cuckoo-cache` (crates.io) |
| oracle | params only | `simulate_oracle()` | `OraclePolicy` | in-tree `oracle.rs` |

### Adding a new engine

1. Add dependency to `crates/cachesim/Cargo.toml`
2. Add policy enum + `From` impl in `crates/cachesim/src/lib.rs`; add error variant to `Error`
3. Add config struct + `simulate_*()` function in `simulator.rs`
4. Add `Engine` variant, CLI arg enum, and handler in `main.rs`
5. Add tests in `simulator.rs`

### Simulation model

- **Read** (GET): lookup, demand-fetch on miss
- **Write** (SET): unconditional insert
- **Delete**: remove from cache
- Objects exceeding `max_obj_size` or with size 0 are skipped
- TTL uses wall-clock time (not trace timestamps)

### Trace format

Parquet with ZSTD compression. Core columns: `timestamp` (ns), `obj_id` (u64), `obj_size` (u32), `next_access_vtime` (i64). Optional: `op` (u8), `ttl` (i32). Import from libCacheSim binary or pelikan-io/cache-trace CSV via `convert`.

KV-cache request traces (kvtrace): one row per request with `chunks` list<u64> of full-chunk ids (< 2^48, enforced by the writer); rows in non-decreasing `timestamp` order (writer and reader enforce it, so consumers can stream); `chunk_tokens` and `max_chunks` are file metadata (`RequestReader::max_chunks()` scans the column when a file lacks the key); no object sizes; only `session_id` may be null (the reader rejects other nulls). `convert -f mooncake` writes one; `kv expand` maps it to object rows with a model geometry. `ObjectMapper` lists the objects a request writes; reuse rules (min over ranks; hybrid: resume only at a state-carrying chunk) and the store's write rule (SET vs insert-if-absent) belong to consumers. Dense/MLA geometry is checked against vLLM/LMCache; hybrid sizing is an approximation (one size per chunk object in LMCache, block-size constraint via `Geometry::vllm_block_size`, one snapshot per request) pending a follow-up; keep the docs saying so. Parquet's `snap` feature is on so files from pyarrow (snappy by default) open.

## Conventions

- Run `cargo fmt` before committing
- `cargo clippy --workspace --all-targets -- -D warnings` must be clean (and the `--no-default-features` forms above)
- Keep README.md in sync with code changes (CLI options, engines, architecture)
- Policy enums in `lib.rs` mirror upstream crate policies with `From` conversions
- Errors use `thiserror` with `#[from]` for automatic conversion; one `Error` per crate. A new `cachetrace::Error` or `kvtrace::Error` variant needs a matching arm in the `From` impls in `crates/cachesim/src/lib.rs`
- Tests use synthetic Parquet traces via `write_synthetic_trace()` helper in `simulator::tests`
- Do not include `claude.ai` session links in commits or PRs
