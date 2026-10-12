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
cachesim can import libCacheSim's binary trace formats,
[pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace) CSV files,
and the public [Mooncake](https://github.com/kvcache-ai/Mooncake) LLM serving
traces (expanded into KV-cache object traces) directly.

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

### Mooncake KV-cache traces

`-f mooncake` converts a Mooncake trace (FAST '25, JSON lines) into a trace
of KV-cache objects, as an LLM serving engine with an external KV store
(LMCache-style) would read them. Each input line is one request:

```json
{"timestamp": 27482, "input_length": 6955, "output_length": 52, "hash_ids": [46, 47, 48]}
```

`timestamp` is in milliseconds. Each `hash_ids` entry names one 512-token
prompt block; equal ids mean equal prefix content. The last block holds
`input_length - 512 * (n - 1)` tokens, clamped to 1..512.

Objects (defaults follow LMCache v0.5.5):

- One object is one chunk of `--chunk-tokens` tokens (default 256) of a
  block, across all layers. Its size is tokens × KV bytes per token.
- `--tp N`: each tensor-parallel rank stores its own shard, so each chunk is
  N objects. A rank's KV share follows vLLM v0.31 (db9527a), which splits
  KV heads across ranks and replicates them once N reaches the head count
  (`vllm/model_executor/layers/linear.py:1053-1058`,
  `vllm/config/model.py:1625-1628`): with `h = num_key_value_heads` a rank
  holds 1/min(N, h) of the KV. vLLM requires `h % N == 0` when `N < h` and
  `N % h == 0` otherwise, and so does the converter. With
  `--kv-bytes-per-token` there is no head count and the share is 1/N. The
  recurrent state is split by linear-attention value heads, which N must
  divide (`vllm/model_executor/layers/mamba/mamba_utils.py:295-299`), so a
  rank holds 1/N of it. A per-rank size that is not a whole number of bytes
  is an error, not rounded. MLA models (`kv_lora_rank` in the config) are
  stored by the first rank only, at full size. The divisors are recorded as
  `kvtrace.kv_tp_divisor` and `kvtrace.state_tp_divisor`.
- Hybrid models (linear-attention layers with a recurrent state):
  `--state-mode per-request` (default) stores one state snapshot at each
  request's last chunk; `per-chunk` stores one at every chunk.
  `--state-layout combined` (default) adds the state bytes to that chunk's
  object, which becomes a distinct object kind; `separate` stores the state
  as its own object.

Columns:

| Column              | Value                                                  |
|---------------------|--------------------------------------------------------|
| `timestamp`         | request timestamp, ms → ns                             |
| `obj_id`            | `block << 24 \| chunk << 16 \| kind << 12 \| rank`     |
| `obj_size`          | first size seen for the `obj_id` (see below)           |
| `next_access_vtime` | row index of the next access to the `obj_id`, or `-1`  |
| `op`                | `1` (GET) on every row                                 |
| `ttl`               | null                                                   |

`kind` is 0 for KV, 1 for a separate state object, and 2 for KV and state in
one object. Every row is a GET: on a miss the engine computes the chunk and
stores it, so writes in a replay come from fill on miss. If an id recurs with
a different computed size (a partial last block that later appears full), the
first size is kept and the row is counted in `kvtrace.size_conflicts`.

Model sizes come from one of:

- `--model <repo id>` (with optional `--revision`, default `main`; a single
  branch, tag or commit name, so `refs/pr/N` and names containing `/` are
  not accepted): fetch the
  model's `config.json` from Hugging Face. `HF_TOKEN`, if set, is sent as a
  bearer token for gated models. Fetched files are cached at
  `<cache>/<repo id>/<revision>/config.json`, where `<cache>` is
  `$HF_HOME/cachesim/configs`, else `$XDG_CACHE_HOME/cachesim/hf-configs`,
  else `~/.cache/cachesim/hf-configs` (`%LOCALAPPDATA%\cachesim\hf-configs`
  on Windows). A cached file is used without contacting the network, so later
  conversions work offline; delete it, or pass a commit hash as the
  revision, to fetch another version. Requires the `hf-fetch` cargo feature
  (on by default; build with `--no-default-features` to drop the HTTP
  client).
- `--model-config <path>`: a local `config.json`.
- `--kv-bytes-per-token <bytes>`, optionally with `--state-bytes <bytes>`.

From a config (fields read from `text_config` when present):

- Full-attention layers: the `full_attention` entries of `layer_types`, else
  `num_hidden_layers / full_attention_interval`, else `num_hidden_layers`.
  The remaining layers are linear-attention layers. Configs the converter
  cannot size are rejected: a `layer_types` entry other than
  `full_attention` or `linear_attention` (e.g. `sliding_attention`), a
  `sliding_window` without `layer_types` (unless `use_sliding_window` is
  false), and Mamba-style hybrids (`hybrid_override_pattern`,
  `layers_block_type`).
- KV bytes per token: full layers × `num_key_value_heads` × `head_dim`
  (default `hidden_size / num_attention_heads`) × 2 × dtype bytes; for MLA,
  full layers × (`kv_lora_rank` + `qk_rope_head_dim`) × dtype bytes. The
  dtype is `--kv-dtype`, else `torch_dtype`, else `dtype`, else bfloat16.
- State bytes per sequence: linear layers × `linear_num_value_heads` ×
  `linear_key_head_dim` × `linear_value_head_dim` × state dtype bytes. The
  state dtype is `mamba_ssm_dtype` when the config sets it (Qwen3.5 configs
  set float32, and vLLM copies it: `vllm/model_executor/models/config.py:819-843`
  at v0.31, db9527a). Otherwise it is the model dtype, not `--kv-dtype`:
  vLLM's `mamba_ssm_cache_dtype` "auto" uses the conv-state dtype, which
  defaults to the model dtype (`vllm/model_executor/layers/mamba/mamba_utils.py:98-110`),
  and Qwen3-Next takes that path (`models/qwen3_next.py:879-887`).
  Convolution state is not counted. Size products that overflow 64 bits are
  an error.

For example, Qwen/Qwen3-8B is 36 × 8 × 128 × 2 × 2 = 147,456 bytes per token.

Provenance is written to the Parquet footer as key-value metadata under
`kvtrace.*` keys: source file name and SHA-256, model id, revision and config
SHA-256, derived sizes and layer counts, chunk size, TP degree, state mode and
layout, request count, size-conflict count, and the `obj_id` layout.

The converter reads the input twice, so the input must be an uncompressed
regular file: directories, pipes (including a piped `/dev/stdin`) and
`.zst`/`.zstd`/`.gz` files (any letter case) are rejected; decompress first. The first pass builds the `obj_id` column (8 bytes per
row), which a reverse scan overwrites in place with `next_access_vtime`; the
scan's map of distinct objects is then freed. That column and a map from
each distinct object to its size stay in memory through the second pass,
which regenerates the rows and streams them to the writer. The input's
SHA-256 must match across the two passes. An output that is the input file
(same path, a symlink to it, or a hard link) is refused. The output is
written to a temporary file `.<name>.<pid>.tmp` in the output's directory
and renamed over the output only on success, so a failed run leaves an
existing output unchanged; an output that is a device such as `/dev/null`
is written directly.

## Code Architecture

```
src/
├── lib.rs          # Crate root, module declarations, Error type
├── trace.rs        # Trace data model, Parquet I/O, format conversion
├── mooncake.rs     # Mooncake trace → KV-cache object trace
├── hf.rs           # Hugging Face config.json fetch and cache (hf-fetch feature)
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
- **`TraceWriter::append_key_value_metadata()`** — add a key-value pair to
  the Parquet footer.
- **`Op`** — operation enum matching `req_op_e`, with `is_read()` /
  `is_write()` / `is_delete()` classification helpers.

### `mooncake` module

- **`ModelSizes`** — KV bytes per token and recurrent-state bytes per
  sequence, from a Hugging Face `config.json` (`from_config_file()`,
  `from_config_bytes()`) or given directly (`explicit()`).
- **`MooncakeOptions`** — sizes, chunk size, TP degree, state mode and
  layout, and provenance fields.
- **`convert_mooncake_to_parquet()`** — two-pass conversion described above.

### `hf` module

- **`fetch_config()`** — return a model's `config.json`, from the cache or
  downloaded. Built with the `hf-fetch` feature.

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
| `convert`   | Import a trace to Parquet (binary, CSV, or Mooncake)         |
| `info`      | Print summary statistics for a Parquet trace file            |

## Usage

```bash
# Convert a libCacheSim binary trace to Parquet
cachesim convert -i trace.oracleGeneral.bin -o trace.parquet

# Convert a pelikan-io/cache-trace CSV (zstd-compressed)
cachesim convert -i cluster001.zst -o cluster001.parquet -f cache-trace

# Convert a Mooncake trace to KV-cache objects, model config from Hugging Face
cachesim convert -i conversation_trace.jsonl -o conv.parquet -f mooncake \
    --model Qwen/Qwen3-8B

# Hybrid model, tensor parallel 8, offline with a local config.json.
# Qwen3.8-27B has 4 KV heads, so each of the 8 ranks holds 1/4 of the KV
# (16,384 B/token) and 1/8 of the recurrent state (18,874,368 B).
cachesim convert -i conversation_trace.jsonl -o conv-tp8.parquet -f mooncake \
    --model-config config.json --model-name Qwen/Qwen3.8-27B --tp 8 \
    --state-mode per-request --state-layout combined

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
```

### Convert options

```
-i, --input <PATH>         Input trace file (required)
-o, --output <PATH>        Output Parquet file (required)
-f, --format <FORMAT>      oracle-general | oracle-general-op-ns | cache-trace | mooncake
                           [default: oracle-general]
```

Mooncake options (`-f mooncake`; one of `--model`, `--model-config`,
`--kv-bytes-per-token` is required):

```
    --model <REPO_ID>            Fetch config.json from Hugging Face (cached)
    --revision <REV>             Revision of --model [default: main]
    --model-config <PATH>        Local config.json
    --kv-bytes-per-token <N>     KV bytes per token, instead of a config
    --state-bytes <N>            Recurrent-state bytes per sequence, with --kv-bytes-per-token
    --kv-dtype <DTYPE>           Override the config's KV dtype
                                 (bfloat16, float16, float32, fp8, float8_e4m3fn)
    --model-name <NAME>          Model id for the metadata (with --model-config or explicit sizes)
    --chunk-tokens <N>           Tokens per object; divides 512 [default: 256]
    --tp <N>                     Tensor-parallel degree [default: 1]
    --state-mode <MODE>          per-request | per-chunk [default: per-request]
    --state-layout <LAYOUT>      combined | separate [default: combined]
```

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
segcache-specific options (segment size, hash power, TTL).

## Building

```bash
cargo build --release
```

## License

Apache-2.0
