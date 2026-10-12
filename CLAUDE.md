# CLAUDE.md

## Project

Cache trace simulator (`cachesim-rs`) for the [cache-rs](https://github.com/pelikan-io/cache-rs) crate family. Replays Parquet-format traces against segcache, cuckoo-cache, and oracle engines, reporting hit/miss statistics.

Cargo workspace (virtual root `Cargo.toml`, shared versions in `[workspace.dependencies]`):

- `crates/cachetrace` — object-level trace format: Parquet schema, `TraceEntry`, `Op`, `TraceReader`/`TraceWriter`. Importers (libCacheSim binary, cache-trace CSV) behind the `import` feature, default on; the core must build with `--no-default-features` and without `ahash`/`zstd`.
- `crates/cachesim` — package `cachesim-rs` (published on crates.io), library `cachesim`, binary `cachesim`. Re-exports `cachetrace` as `cachesim::trace`; keep that path working.
- Rust edition 2021, Apache-2.0

## Build & Test

```bash
cargo build --workspace              # dev build
cargo build --release                # optimized (lto, codegen-units=1)
cargo test --workspace               # all tests
cargo test --workspace --no-default-features
cargo test -p cachetrace --no-default-features   # trace core without importers
cargo clippy --workspace --all-targets -- -D warnings   # lint (CI enforces zero warnings)
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo clippy -p cachetrace --all-targets --no-default-features -- -D warnings
cargo test --workspace -- --ignored  # network test: fetches a config from huggingface.co
cargo fmt --all --check              # format check (CI enforces)
```

`--workspace --no-default-features` still builds cachetrace with `import`, because cachesim enables it; test the core with `-p cachetrace`.

CI runs on ubuntu, macos, and windows. All four checks (lint, 3x build-and-test) must pass.

## Source Layout

```
crates/cachetrace/src/lib.rs     TraceEntry, Op, trace_schema(), TraceReader/Writer, Error
crates/cachetrace/src/import.rs  libCacheSim binary + cache-trace CSV importers (feature `import`)
crates/cachesim/src/lib.rs       Policy enums (SegcachePolicy, CuckooPolicy), Error, `pub use cachetrace as trace`
crates/cachesim/src/mooncake.rs  Mooncake trace → KV-cache object trace (ModelSizes, convert_mooncake_to_parquet)
crates/cachesim/src/hf.rs        Hugging Face config.json fetch + cache (feature `hf-fetch`, default on)
crates/cachesim/src/oracle.rs    OracleCache with Belady/BeladySize policies
crates/cachesim/src/simulator.rs SimConfig/CuckooConfig, simulate_segcache()/simulate_cuckoo()/simulate_oracle(), SimResult
crates/cachesim/src/main.rs      CLI (clap): simulate, convert, info subcommands
crates/cachesim/tests/           api_paths.rs (old `cachesim::trace` paths), Mooncake fixtures
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

Parquet with ZSTD compression. Core columns: `timestamp` (ns), `obj_id` (u64), `obj_size` (u32), `next_access_vtime` (i64). Optional: `op` (u8), `ttl` (i32). Import from libCacheSim binary, pelikan-io/cache-trace CSV, or Mooncake JSON lines via `convert` subcommand.

## Conventions

- Run `cargo fmt` before committing
- `cargo clippy -- -D warnings` must be clean
- Keep README.md in sync with code changes (CLI options, engines, architecture)
- Policy enums in `lib.rs` mirror upstream crate policies with `From` conversions
- Errors use `thiserror` with `#[from]` for automatic conversion; one `Error` per crate. A new `cachetrace::Error` variant needs a matching arm in `cachesim`'s `From<cachetrace::Error>`
- Tests use synthetic Parquet traces via `write_synthetic_trace()` helper in `simulator::tests`
- Do not include `claude.ai` session links in commits or PRs
