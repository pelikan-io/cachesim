# CLAUDE.md

## Project

Cache trace simulator (`cachesim-rs`) for the [cache-rs](https://github.com/pelikan-io/cache-rs) crate family. Replays Parquet-format traces against segcache, cuckoo-cache, and oracle engines, reporting hit/miss statistics.

- Binary: `cachesim`, Library: `cachesim`
- Rust edition 2021, Apache-2.0

## Build & Test

```bash
cargo build              # dev build
cargo build --release    # optimized (lto, codegen-units=1)
cargo test               # all unit tests
cargo clippy --all-targets -- -D warnings   # lint (CI enforces zero warnings)
cargo fmt --all --check  # format check (CI enforces)
```

CI runs on ubuntu, macos, and windows. All four checks (lint, 3x build-and-test) must pass.

## Source Layout

```
src/lib.rs          Policy enums (SegcachePolicy, CuckooPolicy), Error type
src/trace.rs        TraceEntry, TraceReader/Writer, Parquet I/O, format converters
src/oracle.rs       OracleCache with Belady/BeladySize policies
src/simulator.rs    SimConfig/CuckooConfig, simulate()/simulate_cuckoo()/simulate_oracle(), SimResult
src/main.rs         CLI (clap): simulate, convert, info subcommands
```

## Architecture

### Engines

Each engine has its own config struct, simulation function, and policy enum:

| Engine | Config | Function | Policy enum | Backing crate |
|--------|--------|----------|-------------|---------------|
| segcache | `SimConfig` | `simulate()` | `SegcachePolicy` | `segcache` (crates.io) |
| cuckoo | `CuckooConfig` | `simulate_cuckoo()` | `CuckooPolicy` | `cuckoo-cache` (git dep) |
| oracle | params only | `simulate_oracle()` | `OraclePolicy` | in-tree `oracle.rs` |

### Adding a new engine

1. Add dependency to `Cargo.toml`
2. Add policy enum + `From` impl in `lib.rs`; add error variant to `Error`
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

Parquet with ZSTD compression. Core columns: `timestamp` (ns), `obj_id` (u64), `obj_size` (u32), `next_access_vtime` (i64). Optional: `op` (u8), `ttl` (i32). Import from libCacheSim binary or pelikan-io/cache-trace CSV via `convert` subcommand.

## Conventions

- Run `cargo fmt` before committing
- `cargo clippy -- -D warnings` must be clean
- Keep README.md in sync with code changes (CLI options, engines, architecture)
- Policy enums in `lib.rs` mirror upstream crate policies with `From` conversions
- Errors use `thiserror` with `#[from]` for automatic conversion
- Tests use synthetic Parquet traces via `write_synthetic_trace()` helper in `simulator::tests`
