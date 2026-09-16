# cachesim

A cache trace simulator for [cache-rs](https://github.com/pelikan-io/cache-rs)
crates, with a trace format inspired by
[libCacheSim](https://github.com/cacheMon/libCacheSim).

## Overview

cachesim replays recorded cache workload traces against cache implementations
from the cache-rs project —
[segcache](https://github.com/pelikan-io/cache-rs/tree/main/crates/segcache)
(segment-structured) and
[cuckoo-cache](https://github.com/pelikan-io/cache-rs/tree/main/crates/cuckoo)
(cuckoo-hashing with fixed-size item slots) — and reports hit/miss statistics.
It also includes oracle (offline-optimal) eviction policies — Belady and
BeladySize — as theoretical baselines.

Traces are stored on disk as [Parquet](https://parquet.apache.org/) files, and
cachesim can import libCacheSim's binary trace formats and
[pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace) CSV files
directly.

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
| `key_size`         | `UInt32`   | yes      | Key component of `obj_size`, when known separately |
| `value_size`       | `UInt32`   | yes      | Value component of `obj_size`, when known separately |

Parquet files are compressed with ZSTD by default.

The first four columns correspond directly to libCacheSim's **oracleGeneral**
binary format. `op` and `ttl` are extension columns that support richer trace
data (e.g. the **oracleGeneralOpNs** format). `key_size` and `value_size`
carry the two components of `obj_size` when the source records them
separately; a replay tool needs the value length on its own, since it renders
key bytes itself. Files written without the extension columns still read, with
the missing fields reported as absent.

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
| `key_size`    | `key_size`, `obj_size` | stored as-is; `obj_size = key_size + value_size` |
| `value_size`  | `value_size`, `obj_size` | stored as-is; `obj_size = key_size + value_size` |
| `client_id`   | —                    | not stored                                  |
| `operation`   | `op`                 | string -> `req_op_e` integer                |
| `ttl`         | `ttl`                | 0 -> null, >0 -> Some                       |
| —             | `next_access_vtime`  | set to `-1`; run `cachesim annotate` to fill it |

## Code Architecture

```
src/
├── lib.rs          # Crate root, module declarations, Error type
├── trace.rs        # Trace data model, Parquet I/O, format conversion
├── oracle.rs       # Belady and BeladySize oracle caches
├── simulator.rs    # Cache simulation engine
└── main.rs         # CLI (clap)
```

### `trace` module

- **`TraceEntry`** — one cache request (the in-memory row type).
- **`TraceReader`** — opens a Parquet file and streams `Result<TraceEntry>` values
  one record batch at a time, so traces larger than memory replay at a fixed
  footprint. The footer's row count is available up front via `total_entries()`.
- **`TraceWriter`** — buffers entries into Arrow `RecordBatch`es and writes
  them to a ZSTD-compressed Parquet file.
- **`convert_bin_to_parquet()`** — streaming conversion from libCacheSim binary
  format to Parquet.
- **`convert_cache_trace_to_parquet()`** — streaming conversion from
  pelikan-io/cache-trace CSV (with auto zstd decompression) to Parquet.
- **`Op`** — operation enum matching `req_op_e`, with `is_read()` /
  `is_write()` / `is_delete()` classification helpers.

### `annotate` module

- **`annotate_next_access()`** — two-pass rewrite that fills
  `next_access_vtime` from the request sequence. A backward pass over row
  groups reads only `obj_id` and spills each row's answer to a temp file; a
  forward pass streams the trace and splices the answers in. Peak memory is
  one row group plus a map of unique objects, independent of trace length.

### `oracle` module

Offline-optimal eviction policies that use the `next_access_vtime` column
(future knowledge) to make perfect eviction decisions. These serve as
theoretical upper bounds when evaluating online policies.

- **`OraclePolicy::Belady`** — evict the object whose next access is farthest
  in the future. Optimal for minimizing miss count with uniform-size objects
  (Bélády, 1966).
- **`OraclePolicy::BeladySize`** — evict the object that maximizes
  `next_access_vtime × obj_size`. This frees the most *cache-byte-time* and
  is a better reference for variable-size workloads, biasing toward evicting
  large objects that will not be needed soon.
- **`OracleCache`** — byte-capacity cache backed by a `BTreeSet` priority
  queue. Supports `lookup` (hit + update oracle data), `insert` (with
  eviction), and `remove`.

### `simulator` module

- **`SimConfig`** — all knobs for a segcache run: cache size, segment size,
  hash power, eviction policy, default TTL, max object size.
- **`CuckooConfig`** — all knobs for a cuckoo-cache run: nitem, item size,
  max displacement depth, eviction policy, TTL settings, max object size.
- **`SimResult`** — aggregate counters (hits, misses, inserts, insert failures,
  deletes, skipped) with `hit_rate()` / `miss_rate()`.
- **`simulate_segcache()`** — reads a Parquet trace, builds a `segcache::Segcache`
  instance, and replays every request:
  - *Read* ops → lookup; on miss, insert (demand-fetch).
  - *Write* ops → unconditional insert/overwrite.
  - *Delete* ops → remove.
  - Objects exceeding `max_obj_size` are skipped.
- **`simulate_cuckoo()`** — same replay loop but against a
  `cuckoo_cache::CuckooCache`. The cache uses fixed-size item slots with
  cuckoo hashing (D=4 candidate positions per key).
- **`simulate_oracle()`** — same replay loop but against an `OracleCache`.
  Only needs cache size and oracle policy (engine-specific knobs do not apply).

### CLI (`main.rs`)

Three subcommands:

| Command     | Description                                                  |
|-------------|--------------------------------------------------------------|
| `simulate`  | Replay a Parquet trace against segcache, cuckoo, or oracle   |
| `convert`   | Import a trace to Parquet (binary or CSV)                    |
| `annotate`  | Compute `next_access_vtime` for a trace that lacks it        |
| `mrc`       | Sweep cache sizes and print the miss-ratio curve             |
| `info`      | Print summary statistics for a Parquet trace file            |

## Usage

```bash
# Convert a libCacheSim binary trace to Parquet
cachesim convert -i trace.oracleGeneral.bin -o trace.parquet

# Convert a pelikan-io/cache-trace CSV (zstd-compressed)
cachesim convert -i cluster001.zst -o cluster001.parquet -f cache-trace

# Fill in next_access_vtime (required before `simulate ... oracle` on a
# CSV-converted trace; the oracle engine refuses a trace without it)
cachesim annotate -i cluster001.parquet -o cluster001.oracle.parquet

# Run a simulation (64 MB cache, FIFO eviction via segcache)
cachesim simulate -t trace.parquet -c 64M segcache -p fifo

# Run with S3-FIFO and a custom admission ratio
cachesim simulate -t trace.parquet -c 64M segcache -p s3-fifo --admission-ratio 0.15

# Run a cuckoo-cache simulation (64 MB cache, 128-byte item slots)
cachesim simulate -t trace.parquet -c 64M cuckoo --item-size 128

# Run cuckoo-cache with expire eviction policy
cachesim simulate -t trace.parquet -c 64M cuckoo -p expire --item-size 256

# Run a Belady (optimal) simulation as a reference baseline
cachesim simulate -t trace.parquet -c 64M oracle -p belady

# Run a size-aware optimal simulation
cachesim simulate -t trace.parquet -c 64M oracle -p belady-size

# Inspect a trace
cachesim info -t trace.parquet

# Miss-ratio curve: one full simulation per size, table to stdout and
# optionally CSV. Takes the same engine subcommands as `simulate`.
cachesim mrc -t trace.parquet -s 64M,128M,256M,512M,1G --csv mrc.csv segcache -p fifo
cachesim mrc -t trace.parquet -s 64M,128M,256M,512M,1G oracle -p belady
```

### MRC options

```
-t, --trace <PATH>         Parquet trace file (required)
-s, --sizes <LIST>         Comma-separated sizes with K/M/G suffixes (required)
    --csv <PATH>           Also write the curve as CSV
```

Sizes run sequentially, so peak memory is one cache plus the streaming
reader, not the sum of the sweep. Watch the `insert_failures` column: a curve
that goes flat while failures climb is a cache that ran out of hash-table
slots, not one that ran out of bytes, and needs a larger `--hash-power`
(segcache) or `--item-size` budget (cuckoo) before the numbers mean anything. The CSV columns are `cache_size, hit_rate,
miss_rate, requests, hits, misses, inserts, insert_failures, deletes,
skipped`; rates are fractions in `[0, 1]`.

### Simulate options

Shared options (before the engine subcommand):

```
-t, --trace <PATH>         Parquet trace file (required)
-c, --cache-size <SIZE>    Cache size with K/M/G suffix [default: 64M]
```

#### `segcache` engine

```
-p, --policy <POLICY>      Eviction policy [default: fifo]
-s, --segment-size <N>     Segment size in bytes [default: 1048576]
    --hash-power <N>       Hash table buckets = 2^N [default: 16]
    --default-ttl <SECS>   Default TTL, 0 = none [default: 0]
    --max-obj-size <N>     Skip objects larger than N bytes [default: 1048576]
    --admission-ratio <F>  S3-FIFO admission-pool ratio [default: 0.10]
```

| Policy | Description |
|--------|-------------|
| `none` | No eviction; inserts fail when full |
| `random` | Random segment eviction |
| `random-fifo` | Random TTL bucket, FIFO within |
| `fifo` | First-in first-out |
| `cte` | Closest-to-expiration |
| `util` | Least-utilized segment |
| `s3-fifo` | S3-FIFO: small, main, and ghost FIFO queues |

#### `cuckoo` engine

```
-p, --policy <POLICY>      Eviction policy [default: random]
    --item-size <N>        Fixed byte size per item slot [default: 64]
    --max-displace <N>     Max displacement chain depth [default: 16]
    --max-ttl <SECS>       Max TTL the cache accepts [default: 2592000]
    --default-ttl <SECS>   Default TTL, 0 = none [default: 0]
    --max-obj-size <N>     Skip objects larger than N bytes [default: 1048576]
```

| Policy | Description |
|--------|-------------|
| `random` | Randomly select a candidate slot for eviction |
| `expire` | Prefer evicting items nearest to expiration |

The number of item slots is derived from `cache_size / item_size`. Each slot
is a fixed-size allocation that must fit the key, value, and internal metadata.
Items that exceed the slot size will fail to insert (counted as insert
failures).

#### `oracle` engine

```
-p, --policy <POLICY>      Oracle eviction policy [default: belady]
```

| Policy | Description |
|--------|-------------|
| `belady` | Optimal: evict farthest future access |
| `belady-size` | Optimal (size-aware): evict max(distance × size) |

Oracle policies use `next_access_vtime` from the trace and ignore
segcache-specific options (segment size, hash power, TTL). A trace whose
`next_access_vtime` is `-1` throughout (anything converted from CSV) is
refused; run `cachesim annotate` on it first.

## Building

```bash
cargo build --release
```

## License

Apache-2.0
