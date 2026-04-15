# cachesim

A cache trace simulator for [cache-rs](https://github.com/pelikan-io/cache-rs)
crates, with a trace format inspired by
[libCacheSim](https://github.com/cacheMon/libCacheSim).

## Overview

cachesim replays recorded cache workload traces against cache implementations
from the cache-rs project (currently [segcache](https://github.com/pelikan-io/cache-rs/tree/main/crates/segcache))
and reports hit/miss statistics. Traces are stored on disk as
[Parquet](https://parquet.apache.org/) files, and cachesim can import
libCacheSim's binary trace formats directly.

## Data Format

### Parquet Schema

Each row in a trace file represents a single cache request:

| Column             | Arrow Type | Nullable | Description                                       |
|--------------------|------------|----------|---------------------------------------------------|
| `timestamp`        | `Timestamp(ns, UTC)` | no | Request time in **nanoseconds** since Unix epoch |
| `obj_id`           | `UInt64`   | no       | Object identifier                                  |
| `obj_size`         | `UInt32`   | no       | Object size in bytes                               |
| `next_access_vtime`| `Int64`    | no       | Virtual time of the next access (`-1` = no reuse)  |
| `op`               | `UInt8`    | yes      | Operation type (see below)                         |
| `ttl`              | `Int32`    | yes      | Time-to-live in seconds                            |

Parquet files are compressed with ZSTD by default.

The first four columns correspond directly to libCacheSim's **oracleGeneral**
binary format. `op` and `ttl` are extension columns that support richer trace
data (e.g. the **oracleGeneralOpNs** format).

### Operation Types

The `op` column uses the same integer encoding as libCacheSim's `req_op_e`:

| Value | Name      | Category |
|-------|-----------|----------|
| 0     | NOP       | read     |
| 1     | GET       | read     |
| 2     | GETS      | read     |
| 3     | SET       | write    |
| 4     | ADD       | write    |
| 5     | CAS       | write    |
| 6     | REPLACE   | write    |
| 7     | APPEND    | write    |
| 8     | PREPEND   | write    |
| 9     | DELETE    | delete   |
| 10    | INCR      | write    |
| 11    | DECR      | write    |
| 12    | READ      | read     |
| 13    | WRITE     | write    |
| 14    | UPDATE    | write    |

When the `op` column is absent or null, every request is treated as a **GET**
(read with demand-fetch on miss).

### libCacheSim Binary Formats

cachesim can import these packed little-endian binary formats via
`cachesim convert`:

**oracleGeneral** (24 bytes/record, no header):

```text
offset  size  type     field
─────────────────────────────────
 0      4     u32      timestamp (seconds)
 4      8     u64      obj_id
12      4     u32      obj_size
16      8     i64      next_access_vtime
```

**oracleGeneralOpNs** (27 bytes/record, no header):

```text
offset  size  type     field
─────────────────────────────────
 0      4     u32      timestamp (seconds)
 4      8     u64      obj_id
12      4     u32      obj_size
16      1     u8       op
17      2     u16      namespace (not stored)
19      8     i64      next_access_vtime
```

During import, the u32-second timestamps are widened to u64 nanoseconds.

### pelikan-io/cache-trace CSV

cachesim can also import
[pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace) CSV files
(Twitter production Memcache traces). Files with a `.zst` extension are
decompressed automatically.

CSV columns (no header row):

```
timestamp,key,key_size,value_size,client_id,operation,ttl
```

Field mapping during import:

| CSV field     | Parquet column       | Transformation                              |
|---------------|----------------------|---------------------------------------------|
| `timestamp`   | `timestamp`          | seconds -> nanoseconds (`* 1_000_000_000`)  |
| `key`         | `obj_id`             | deterministic hash (ahash, fixed seed)      |
| `key_size`    | `obj_size` (partial) | `key_size + value_size`                     |
| `value_size`  | `obj_size` (partial) | `key_size + value_size`                     |
| `client_id`   | —                    | not stored                                  |
| `operation`   | `op`                 | string -> `req_op_e` integer                |
| `ttl`         | `ttl`                | 0 -> null, >0 -> Some                       |
| —             | `next_access_vtime`  | set to `-1` (not available in CSV)          |

## Code Architecture

```
src/
├── lib.rs          # Crate root, module declarations, Error type
├── trace.rs        # Trace data model, Parquet I/O, format conversion
├── simulator.rs    # Cache simulation engine
└── main.rs         # CLI (clap)
```

### `trace` module

- **`TraceEntry`** — one cache request (the in-memory row type).
- **`TraceReader`** — opens a Parquet file, iterates over `TraceEntry` values.
- **`TraceWriter`** — buffers entries into Arrow `RecordBatch`es and writes
  them to a ZSTD-compressed Parquet file.
- **`convert_bin_to_parquet()`** — streaming conversion from libCacheSim binary
  format to Parquet.
- **`convert_cache_trace_to_parquet()`** — streaming conversion from
  pelikan-io/cache-trace CSV (with auto zstd decompression) to Parquet.
- **`Op`** — operation enum matching `req_op_e`, with `is_read()` /
  `is_write()` / `is_delete()` classification helpers.

### `simulator` module

- **`SimConfig`** — all knobs for a run: cache size, segment size, hash power,
  eviction policy, default TTL, max object size.
- **`SimResult`** — aggregate counters (hits, misses, inserts, insert failures,
  deletes, skipped) with `hit_rate()` / `miss_rate()`.
- **`simulate()`** — reads a Parquet trace, builds a `segcache::Segcache`
  instance, and replays every request:
  - *Read* ops → lookup; on miss, insert (demand-fetch).
  - *Write* ops → unconditional insert/overwrite.
  - *Delete* ops → remove.
  - Objects exceeding `max_obj_size` are skipped.

### CLI (`main.rs`)

Three subcommands:

| Command     | Description                                      |
|-------------|--------------------------------------------------|
| `simulate`  | Replay a Parquet trace against segcache           |
| `convert`   | Import a trace to Parquet (binary or CSV)          |
| `info`      | Print summary statistics for a Parquet trace file |

## Usage

```bash
# Convert a libCacheSim binary trace to Parquet
cachesim convert -i trace.oracleGeneral.bin -o trace.parquet

# Convert a pelikan-io/cache-trace CSV (zstd-compressed)
cachesim convert -i cluster001.zst -o cluster001.parquet -f cache-trace

# Run a simulation (64 MB cache, FIFO eviction)
cachesim simulate -t trace.parquet -c 64M -e fifo

# Inspect a trace
cachesim info -t trace.parquet
```

### Simulate options

```
-t, --trace <PATH>         Parquet trace file (required)
-c, --cache-size <SIZE>    Cache size with K/M/G suffix [default: 64M]
-s, --segment-size <N>     Segment size in bytes [default: 1048576]
    --hash-power <N>       Hash table buckets = 2^N [default: 16]
-e, --eviction <POLICY>    none|random|random-fifo|fifo|cte|util [default: fifo]
    --default-ttl <SECS>   Default TTL, 0 = none [default: 0]
    --max-obj-size <N>     Skip objects larger than N bytes [default: 1048576]
```

## Building

```bash
cargo build --release
```

## License

Apache-2.0
