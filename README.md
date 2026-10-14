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
traces (as KV-cache request traces, which `cachesim kv expand` turns into
object traces for a given model).

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

### Mooncake traces and KV-cache request traces

KV-cache traces have two levels:

1. A request trace (crate `kvtrace`): one row per inference request, listing
   the ids of the request's full prefix chunks. It does not depend on a
   model.
2. An object-level trace in the format above: one row per KV-cache object a
   request writes, sized for one model geometry (sizes, tensor parallelism,
   recurrent-state placement), as an LLM serving engine with an external KV
   store (LMCache-style) would look them up and store them.

Dense and MLA geometry is checked against vLLM and LMCache, with the
citations below. Hybrid (linear-attention) geometry is an approximation
pending a follow-up; its known differences from vLLM and LMCache are listed
under "Hybrid models" below.

`cachesim convert -f mooncake` makes the first from a Mooncake trace;
`cachesim kv expand` makes the second from the first.

CLI change: `convert -f mooncake` used to write the object-level trace
directly and took the model options (`--model`, `--model-config`,
`--kv-bytes-per-token`, `--tp`, `--state-mode`, ...). It now writes a request
trace and takes only `--chunk-tokens`; the model options moved to
`cachesim kv expand`; giving one of them to `convert` is an error that says
so. `simulate` and `info` refuse a request trace.

`expand(convert(x))` differs from the old direct conversion in these ways:

- Rows of partial chunks are dropped (only full chunks are kept).
- With a recurrent state and `--state-mode per-request`, the snapshot moves
  from the dropped partial chunk to the request's last full chunk. With
  `combined`, that chunk's write gains the state bytes; with `separate`, its
  state objects are added after it.
- With `combined`, a write that carries state is keyed on the chunk's own
  id instead of a separate kind-2 id (see object ids below), so those rows
  change `obj_id`, and one id can be written at two sizes.
- `next_access_vtime` is renumbered for the new row sequence and ids.

On the public Mooncake conversation trace, with Qwen3-8B at TP 1, a 27B
hybrid at TP 8 (both state modes and both layouts) and DeepSeek-V3 at TP 8,
the expanded rows equal the old rows with exactly these changes applied.

#### Converting a Mooncake trace

A Mooncake trace (FAST '25) is JSON lines, one request per line:

```json
{"timestamp": 27482, "input_length": 6955, "output_length": 52, "hash_ids": [46, 47, 48]}
```

`timestamp` is in milliseconds. Each `hash_ids` entry names one 512-token
prompt block; equal ids mean equal prefix content. Each block is split into
`512 / --chunk-tokens` chunks (default 256 tokens, as in LMCache v0.5.5), and
chunk `c` of block `h` has id `h << 8 | c`. Only full chunks are kept, as
LMCache does by default (`save_unfull_chunk = False`). Every block but the
last holds 512 tokens; the last holds `input_length - 512 * (n - 1)`, clamped
to 1..512, because `input_length` and `hash_ids` can disagree:

- `input_length > 512 * n`: the last block is taken as full. Counted as
  `kvtrace.converter.input_beyond_blocks`.
- `input_length < 512 * (n - 1) + 1`: the last block is taken as 1 token and
  has no full chunk. Counted as `kvtrace.converter.input_short_of_blocks`.

Both counts are also printed on stderr. `input_tokens` keeps `input_length`
as given.

One token prefix can get two chunk ids. A hash id covers a whole block, so a
partial last block and the full block that later extends it have different
ids even when their first tokens agree. Every full sub-chunk of a partial
last block is keyed on that partial block's id; a later request with the
same prefix that holds the block in full reads the same token positions
under the full block's id. A store keyed on the token prefix (as LMCache's
is) would hit there, so the converted trace undercounts prefix reuse. The
trace cannot show whether the tokens agree, so the count is an upper bound.
At 256 tokens per chunk only sub-chunk 0 of such a block is full, and the
later full-chunk reads affected are: conversation 14,844 of 559,542 (2.7%),
toolagent 21,254 of 782,217 (2.7%), synthetic 3,001 of 237,347 (1.3%).
Smaller chunks give more affected sub-chunks per block.

The input must be an uncompressed regular file: directories, pipes and
`.zst`/`.zstd`/`.gz` files (any letter case) are rejected; decompress first.

#### Request trace format

| Column          | Arrow Type             | Nullable | Description                                         |
|-----------------|------------------------|----------|-----------------------------------------------------|
| `timestamp`     | `Timestamp(ns, UTC)`   | no       | Request arrival in nanoseconds since Unix epoch     |
| `req_id`        | `UInt64`               | no       | Request id (Mooncake: index among non-empty lines)  |
| `input_tokens`  | `UInt32`               | no       | Prompt length as the source gives it                |
| `output_tokens` | `UInt32`               | no       | Generated tokens                                    |
| `chunks`        | `List<UInt64>`         | no       | Full prefix chunk ids, in prefix order; each < 2^48 |
| `session_id`    | `UInt64`               | yes      | Session id, for sources that need reconstruction    |

Chunk ids must be below 2^48 because an object id packs the chunk id with a
key slot and a rank into 64 bits (`chunk << 16 | slot << 12 | rank`); the
writer rejects larger ids, and the reader rejects them with the file, row
and `req_id`. Only `session_id` may be null; the reader rejects nulls in any
other column or in a `chunks` item. Files from other Parquet writers can be
read if they are uncompressed or compressed with snappy (pyarrow's default)
or zstd. gzip, lz4 and brotli codecs are not built in: such a file opens
(the footer is not compressed) and fails at the first batch read, with a
Parquet "Disabled feature at compile time" error.

Rows are in arrival order: each `timestamp` is at least the previous row's.
The writer rejects a request earlier than the one before it, and the reader
rejects such a row with the file, row and `req_id`, so a consumer can replay
the trace while reading it. Both checks (order, and a row's chunk count
against `kvtrace.max_chunks`) run as each row is read, so a consumer gets
the error at the offending row, after the rows before it. `convert -f
mooncake` fails on a Mooncake file that is out of timestamp order; the
three traces in kvcache-ai/Mooncake `FAST25-release/traces/` (conversation,
toolagent, synthetic) are in order.

Footer key-value metadata: `kvtrace.format` = `requests`, `kvtrace.version` =
`1`, `kvtrace.chunk_tokens` (every chunk's length), `kvtrace.max_chunks` (the
most chunks in any request), `kvtrace.created`,
`kvtrace.source.*` (source format, file name, SHA-256, block length),
`kvtrace.converter.*` (chunk id rule, the counts above), `kvtrace.requests`
and `kvtrace.chunks`.

#### Expanding to KV-cache objects (`cachesim kv expand`)

Objects (defaults follow LMCache v0.5.5):

- One object is one chunk across all layers (LMCache's default chunk is 256
  tokens, `lmcache/v1/config.py:90`). Its size is chunk tokens × KV bytes
  per token.
- `--tp N`: each tensor-parallel rank stores its own shard, so each chunk is
  N objects (LMCache's object key includes the worker id,
  `lmcache/utils.py:389-441`). A rank's KV share follows vLLM v0.31 (db9527a), which splits
  KV heads across ranks and replicates them once N reaches the head count
  (`vllm/model_executor/layers/linear.py:1053-1058`,
  `vllm/config/model.py:1625-1628`): with `h = num_key_value_heads` a rank
  holds 1/min(N, h) of the KV. vLLM requires `h % N == 0` when `N < h` and
  `N % h == 0` otherwise, and so does `kv expand`. With
  `--kv-bytes-per-token` there is no head count and the share is 1/N. The
  recurrent state is split by linear-attention value heads, which N must
  divide (`vllm/model_executor/layers/mamba/mamba_utils.py:295-299`), so a
  rank holds 1/N of it. N must also divide `num_attention_heads`
  (`linear.py:1052`) and, for hybrid models, the linear-attention
  convolution width `2 × linear_num_key_heads × linear_key_head_dim +
  linear_num_value_heads × linear_value_head_dim` (`mamba_utils.py:289-293`),
  as vLLM requires. A per-rank size that is not a whole number of bytes
  is an error, not rounded. MLA models (`kv_lora_rank` in the config) are
  stored by the first rank only, at full size (LMCache's
  `save_only_first_rank`, `cache_engine.py:116-119`). The divisors are recorded as
  `kvtrace.kv_tp_divisor` and `kvtrace.state_tp_divisor` (`none` for a
  model without recurrent state).
- Hybrid models (linear-attention layers with a recurrent state):
  `--state-mode per-request` (default) stores one state snapshot at each
  request's last chunk; `per-chunk` stores one at every chunk (the upper
  bound). `--state-layout combined` (default) adds the state bytes to the
  chunk's object; `separate` stores the state as its own object.
  Convolution state is not counted in object sizes.

#### Hybrid models: known differences

The hybrid sizing is a simplified model, not LMCache v0.5.5's layout:

- Sizes, `combined`: LMCache writes every chunk object of a hybrid model at
  one size, the attention pages plus the recurrent pages for the whole
  chunk, with null pages where no snapshot was taken
  (`multiprocess/modules/lmcache_driven_transfer.py:66-93`
  `get_layout_desc`, `platform/cuda/cache_context.py:228`). Here a chunk without
  a snapshot is written at KV size only and one with a snapshot at KV plus
  one state, so the same id is written at two sizes
  (`kvtrace.multi_size_objects` counts them; this is an artefact of the
  simplification) and the KV-only writes are smaller than LMCache's.
- Chunk length: with prefix caching, vLLM v0.31 ("align" mode) raises the
  attention block size until one layer's attention page is at least one
  layer's recurrent page (`vllm/platforms/interface.py:960-994`), and LMCache
  requires the chunk size to be a multiple of the block size
  (`kv_layer_groups.py:750-755`). With a 16-token kernel alignment the
  block is 544 tokens for Qwen3-Next-80B at TP 1, 272 at TP 4, and 400 for
  Qwen3.8-27B at TP 8 (784 at TP 1), so 256-token chunks are not valid for
  them. `kv expand` warns on stderr when `chunk_tokens` is not a multiple
  of the block size and names the smallest valid chunk; when the request
  trace came from Mooncake, whose chunk lengths must divide 512, and no
  such length is a multiple of the block size (true for all four values
  above), the warning says no Mooncake chunk length is valid for the model
  at that TP. It still sizes objects for the trace's chunk length, and an
  invalid TP is reported as an error before any warning. At a valid chunk of k blocks, each
  recurrent object holds k pages per linear layer, each padded to the
  attention page size (`platforms/interface.py:1006-1009`); this model has
  one unpadded snapshot per object.
- Snapshot count: align mode snapshots at the last block boundary of each
  scheduler step (`vllm/config/cache.py:190-198`), so a prompt longer than
  `max_num_batched_tokens` gets several snapshots. `per-request` models one
  per request and understates snapshots for long prompts.

Columns of the expanded trace:

| Column              | Value                                                  |
|---------------------|--------------------------------------------------------|
| `timestamp`         | the request's timestamp                                |
| `obj_id`            | `chunk << 16 \| slot << 12 \| rank`                     |
| `obj_size`          | bytes this request's write of the object holds         |
| `next_access_vtime` | row index of the next access to the `obj_id`, or `-1`  |
| `op`                | `1` (GET) on every row                                 |
| `ttl`               | null                                                   |

Object ids are what the store keys objects on. With `combined`, LMCache has
one object group per chunk when `separate_object_groups` is off (the
default) and stores it as one object keyed by the chunk
(`kv_layer_groups.py:682-687`). So a chunk has one id per rank (slot 0)
whether or not a write carries state. Because of the simplified sizing
above, that id can be written at two sizes; `obj_size` on each row is the
size of that request's write. With `separate`, the state object has its own
key (slot 1) next to the chunk's KV object (slot 0).

Whether a write replaces a stored object depends on the store. LMCache has
two write rules:

- Insert-if-absent: the multiprocess store reserves only keys that are not
  present (`reserve_write(..., "new")`, `distributed/storage_manager.py:182-200`,
  `l1_manager.py:455-458`).
- SET: the in-process remote backends put every object they are given
  without checking whether the store holds the key
  (`storage_backend/remote_backend.py:263-270`, `323-331`).

For hybrid models only insert-if-absent applies in LMCache v0.5.5. vLLM
requires its hybrid KV cache manager (HMA) for hybrid SSM models and fails at
startup when the KV connector does not support it
(`vllm/config/vllm.py:2251-2270`). Only LMCache's multiprocess connector
declares that support (`lmcache/integration/vllm/lmcache_mp_connector.py:448`),
and it uses insert-if-absent; the in-process connector that reaches the SET
path does not (vLLM's `kv_connector/v1/lmcache_connector.py:68`; LMCache's
adapter reads only `block_ids[0]`, `vllm_v1_adapter.py:168-179`). The SET
examples here and below describe a store with that rule, not LMCache.

Under SET the combined layout can lose a stored snapshot. With per-request
state: A = [1, 2, 3] is stored, chunk 2 is evicted, B = [1, 2, 3, 4] misses
at chunk 2 and writes chunks 2-4, replacing chunk 3's state-carrying object
with a KV-only one; a repeat of A then reuses 0 tokens under SET and 768
under insert-if-absent (256-token chunks). The separate layout keeps chunk
3's state object, and A reuses 768 tokens either way.

For a Mooncake trace, `chunk` is `block << 8 | sub-chunk`, so `obj_id` is
`block << 24 | sub-chunk << 16 | slot << 12 | rank`. Every row is a GET: on a
miss the engine computes the chunk and stores it, so writes in a replay come
from fill on miss. Rows follow the requests in order and each request's
prefix in order: per chunk, its rank objects, then its separate state
objects.

The rows are the objects each request writes: every chunk of its prefix.
Which stored objects a later request can reuse is up to the consumer:

- A chunk counts as present only if every storing rank's object is present,
  so the usable prefix is the minimum over ranks. MLA models have one
  storing rank (the first).
- For a model without recurrent state, the whole present prefix is usable.
- For a hybrid model, a prefix is usable only through the last present
  chunk whose stored object carries state: the linear-attention layers need
  the state where computation resumes. kvtrace's `ObjectMapper::lookup_ids`
  gives the ids to probe at a chunk: with `combined` the one id, and the
  consumer must remember whether the write that stored it carried state
  (`KvObject::kind`, `ObjectKind::carries_state()`); with `separate` the KV
  id and the state id, and the chunk is resumable when the state id is
  present.
- Chunks that are present but not resumable: under SET they are not
  rewritten (they are inside the lookup's prefix hit); under
  insert-if-absent only absent keys are written, so the separate layout can
  add a snapshot to such a chunk and the combined layout cannot. With
  per-request state, A = [1, 2, 3] and then [1, 2] twice: the second [1, 2]
  reuses 512 tokens with `separate` under insert-if-absent, and nothing in
  the other three cases.

A replay of the expanded trace with the object-level engines applies none of
these rules: it counts any chunk hit as a hit and fills on a miss.

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
  expansions work offline; delete it, or pass a commit hash as the
  revision, to fetch another version. Requires the `hf-fetch` cargo feature
  (on by default; build with `--no-default-features` to drop the HTTP
  client).
- `--model-config <path>`: a local `config.json`.
- `--kv-bytes-per-token <bytes>`, optionally with `--state-bytes <bytes>`.

From a config (fields read from `text_config` when present):

- Full-attention layers: the `full_attention` entries of `layer_types`, else
  `num_hidden_layers / full_attention_interval`, else `num_hidden_layers`.
  The remaining layers are linear-attention layers. Configs that cannot
  be sized are rejected: a `layer_types` entry other than
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
  an error. Applying `mamba_ssm_dtype` whenever a config sets it is broader
  than vLLM, where only the Qwen3.5 config class copies it; this is part of
  the hybrid approximation.

For example, Qwen/Qwen3-8B is 36 × 8 × 128 × 2 × 2 = 147,456 bytes per token.

Provenance is written to the footer as `kvtrace.*` metadata: the request
trace's source, converter and chunk keys, the request trace's file name and
SHA-256, the model id, revision and config SHA-256, derived sizes and layer
counts, TP degree and divisors, state mode and layout, request count, and the
`obj_id` layout. `kvtrace.format` is `objects` and `kvtrace.version` is `2`
(version 1 was the direct conversion, which kept partial chunks).

`kv expand` reads the request trace twice. The first pass builds the `obj_id`
column (8 bytes per row), which a reverse scan overwrites in place with
`next_access_vtime`; the scan's map of distinct objects is then freed. The
second pass regenerates the rows and streams them to the writer. The request
trace's SHA-256 must match before and after. An object larger than
`u32::MAX` bytes is an error.

Output handling (both `convert -f mooncake` and `kv expand`), checked before
any work is done:

- An output that is the input file (same path, a symlink to it, or a hard
  link), a directory, or an existing read-only file is refused.
- An output that is a symlink is followed: the result is written to the file
  it links to, and the link is kept. A dangling symlink is refused.
- A device or pipe such as `/dev/null` is written directly.
- Otherwise the result is written to `.<name>.<pid>.<n>.tmp` in the
  destination's directory, flushed to disk, and renamed over the destination
  only on success. An ordinary error removes the temporary file and leaves
  an existing output unchanged; a killed run leaves the temporary file
  behind, to be deleted by hand.
- On Windows the final rename can fail if another process has the
  destination open.

## Code Architecture

The repository is a Cargo workspace of three crates:

```
crates/
├── cachetrace/          # Object-level trace format (library)
│   └── src/
│       ├── lib.rs       # Schema, TraceEntry, Op, TraceReader, TraceWriter, Error
│       └── import.rs    # libCacheSim binary and cache-trace CSV importers (`import` feature)
├── kvtrace/             # KV-cache request traces and model geometry (library)
│   ├── src/
│   │   ├── lib.rs       # Error, re-exports, SHA-256 and timestamp helpers
│   │   ├── format.rs    # Request trace schema, Request, RequestReader, RequestWriter
│   │   ├── geometry.rs  # ModelSizes (config.json → sizes), Geometry, Sharding
│   │   ├── objects.rs   # Request → objects: ObjectMapper, KvObject, obj_id
│   │   ├── mooncake.rs  # Mooncake trace → request trace
│   │   ├── output.rs    # Output file refusal and temporary-file rename
│   │   └── hf.rs        # Hugging Face config.json fetch and cache (hf-fetch feature)
│   └── tests/fixtures/  # Mooncake trace and model configs
└── cachesim/            # Package `cachesim-rs`: library `cachesim`, binary `cachesim`
    ├── src/
    │   ├── lib.rs       # Policy enums, Error, re-exports of cachetrace and kvtrace
    │   ├── kv.rs        # Request trace → object-level trace (expand)
    │   ├── oracle.rs    # Belady and BeladySize oracle caches
    │   ├── simulator.rs # Cache simulation engine
    │   └── main.rs      # CLI (clap)
    └── tests/           # Integration tests
```

`cachetrace` and `kvtrace` do not depend on each other; `cachesim` depends on
both. A load generator can depend on `kvtrace` alone (with
`default-features = false` it has no HTTP client) to get the same object ids
and sizes the simulator uses, or on `cachetrace` alone to play back an
object-level trace.

Each crate has its own `Error`. A `cachetrace::Error` or `kvtrace::Error`
converts into the `cachesim::Error` variant of the same name (`Io`,
`Parquet`, `Arrow`, `InvalidFormat`, and `Fetch` from kvtrace), so `?` works
across them. Both are `#[non_exhaustive]`; a variant added later converts to
`cachesim::Error::InvalidFormat` with its message until it gets an arm of its
own.

Publishing: `cachesim-rs` depends on `cachetrace` and `kvtrace` by version,
so both have to be published to crates.io before it.

### `cachetrace` crate

The trace format on its own, for programs that read or write object-level
traces without the simulator. `cachesim` re-exports it as `cachesim::trace`,
so every `cachesim::trace::*` item path from earlier releases still resolves.

The trace functions and `TraceReader`'s iterator now return
`cachetrace::Error` (`cachesim::trace::Error`), not `cachesim::Error`.

The rule: code that writes or infers `cachesim::Error` as the error type of
a trace result, without a conversion in between, no longer compiles. That
covers:

- returning a trace result from a function declared to return
  `Result<_, cachesim::Error>` without `?` (E0308);
- a `let` annotated `Result<_, cachesim::Error>` (E0308);
- `match` or `matches!` on `cachesim::Error` variants of a trace result
  (E0308);
- combinators that unify error types, such as
  `Ok::<_, cachesim::Error>(()).and_then(|_| writer.finish())` (E0308);
- collecting the reader into `Result<_, cachesim::Error>` (E0277);
- passing the reader where `Iterator<Item = Result<TraceEntry,
  cachesim::Error>>` is required, including as a
  `Box<dyn Iterator<...>>` (E0271);
- `map_err` with a closure or function whose parameter is typed
  `cachesim::Error`, e.g. `.map_err(|e: cachesim::Error| ...)` (E0631);
- `?` in a function whose own error type wraps `cachesim::Error` through
  `From` (for example a thiserror `#[from] cachesim::Error` variant)
  (E0277): `?` applies one `From` conversion and there is none from
  `cachesim::trace::Error` to that type.

Still compiling: `?` into `cachesim::Error`, into `Box<dyn Error>` and into
anyhow-style error types; `unwrap`, `expect` and `Display` on the error.

One change shows only at run time. A trace error that reaches
`Box<dyn Error>` or `anyhow::Error` through `?` is now a
`cachesim::trace::Error`, so `downcast_ref::<cachesim::Error>()` returns
`None`, `is::<cachesim::Error>()` is false, and walking `source()` (or
anyhow's `chain()`) never finds a `cachesim::Error`. The `Display` and
`Debug` text and the `source()` text are unchanged. Remedies: downcast to
`cachesim::trace::Error`, or apply `.map_err(cachesim::Error::from)` before
the type is erased.

Separately, `cachesim::trace::Error` is a new item, so a glob import of
`cachesim::trace::*` together with any other glob that brings an `Error` into
scope makes a bare `Error` ambiguous (E0659): `cachesim::*`, `std::fmt::*`, or
`use super::*` in a test module whose parent defines `Error`. Name the type
by path.

The remedy for the conversions is `.map_err(cachesim::Error::from)` on the
trace result (or on each item of the reader), or a
`From<cachesim::trace::Error>` impl on the caller's own error type. Each case
above was checked with an external crate that compiles against the code
before the split and fails as listed against this one; the remedies were
checked to compile.

- **`TraceEntry`** — one cache request (the in-memory row type).
- **`trace_schema()`** — the Arrow schema of a trace file.
- **`TraceReader`** — opens a Parquet file and streams `Result<TraceEntry>` values
  one record batch at a time, so traces larger than memory replay at a fixed
  footprint. The footer's row count is available up front via `total_entries()`.
- **`TraceWriter`** — buffers entries into Arrow `RecordBatch`es and writes
  them to a ZSTD-compressed Parquet file.
- **`TraceWriter::append_key_value_metadata()`** — add a key-value pair to
  the Parquet footer.
- **`Op`** — operation enum matching `req_op_e`, with `is_read()` /
  `is_write()` / `is_delete()` classification helpers.

With the `import` feature (on by default):

- **`convert_bin_to_parquet()`** — streaming conversion from libCacheSim binary
  format to Parquet.
- **`convert_cache_trace_to_parquet()`** — streaming conversion from
  pelikan-io/cache-trace CSV (with auto zstd decompression) to Parquet.

`cachetrace = { version = "0.1", default-features = false }` gives the
schema, row type, reader and writer only: the importer code and cachetrace's
direct dependencies on `ahash` and `zstd` are left out. `ahash` and `zstd`
are still built, through `arrow` and `parquet` (Parquet's ZSTD codec needs
`zstd`), so the feature does not shrink the dependency tree by those two.

### `kvtrace` crate

- **`Request`**, **`RequestWriter`**, **`RequestReader`**, **`request_schema()`**
  (`format`) — the request trace format described above. The reader checks
  `kvtrace.format`, `kvtrace.version` and `kvtrace.chunk_tokens`, rejects
  rows out of arrival order, and exposes the footer metadata.
  `max_chunks()` gives the most chunks in any request, for sizing buffers
  before reading: from `kvtrace.max_chunks` when the footer has it (a row
  with more is an error), otherwise by reading the `chunks` column.
- **`ModelSizes`** (`geometry`) — KV bytes per token and recurrent-state bytes
  per sequence, from a Hugging Face `config.json` (`from_config_file()`,
  `from_config_bytes()`, `from_config()`) or given directly (`explicit()`).
- **`Geometry`** — sizes, TP degree, state mode and layout, and provenance
  fields; `sharding()` checks the TP degree against the model and returns
  the per-rank **`Sharding`**.
- **`ObjectMapper`** (`objects`) — built from a `Geometry` and a chunk
  length; `objects(&chunks)` yields each **`KvObject`** of a request (`obj_id`,
  `size`, `rank`, `kind`, `chunk_index`) in prefix order: the objects the
  request writes. **`lookup_ids(chunk, rank)`** gives the ids a lookup
  probes. **`obj_id()`**, **`obj_id_fields()`**, `SLOT_CHUNK`/`SLOT_STATE`
  and **`ObjectKind::key_slot()`** describe the id layout;
  **`ObjectKind::carries_state()`** marks the writes where a hybrid model's
  prefix can resume.
- **`mooncake::convert_mooncake()`** — Mooncake JSON lines to a request
  trace; **`mooncake::chunk_id()`** is the chunk id rule.
- **`output::plan_output()`** — the output handling described above.
- **`hf::fetch_config()`** — return a model's `config.json`, from the cache or
  downloaded. Built with the `hf-fetch` feature (default on).

### `cachesim` crate

`cachesim::trace` is `cachetrace` and `cachesim::kvtrace` is `kvtrace`.

#### `kv` module

- **`expand_to_parquet()`** — request trace + `Geometry` → object-level
  trace, as described above. A KV simulation mode that replays request
  traces directly belongs here.

#### `oracle` module

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

#### `simulator` module

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

#### CLI (`main.rs`)

| Command     | Description                                                  |
|-------------|--------------------------------------------------------------|
| `simulate`  | Replay a Parquet trace against segcache, cuckoo, or oracle   |
| `convert`   | Import a trace to Parquet (binary, CSV, or Mooncake)         |
| `info`      | Print summary statistics for a Parquet trace file            |
| `kv expand` | Expand a KV-cache request trace into an object-level trace   |

## Usage

```bash
# Convert a libCacheSim binary trace to Parquet
cachesim convert -i trace.oracleGeneral.bin -o trace.parquet

# Convert a pelikan-io/cache-trace CSV (zstd-compressed)
cachesim convert -i cluster001.zst -o cluster001.parquet -f cache-trace

# Convert a Mooncake trace to a KV-cache request trace (256-token chunks)
cachesim convert -i conversation_trace.jsonl -o conv-requests.parquet -f mooncake

# Expand it into KV-cache objects, model config from Hugging Face
cachesim kv expand -i conv-requests.parquet -o conv.parquet --model Qwen/Qwen3-8B

# Hybrid model, tensor parallel 8, offline with a local config.json.
# Qwen3.8-27B has 4 KV heads, so each of the 8 ranks holds 1/4 of the KV
# (16,384 B/token) and 1/8 of the recurrent state (18,874,368 B).
cachesim kv expand -i conv-requests.parquet -o conv-tp8.parquet \
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

Mooncake option (`-f mooncake`):

```
    --chunk-tokens <N>           Tokens per chunk; divides 512 [default: 256]
```

### `kv expand` options

```
-i, --input <PATH>               Request trace (required)
-o, --output <PATH>              Output object-level trace (required)
```

Model geometry (one of `--model`, `--model-config`, `--kv-bytes-per-token`
is required):

```
    --model <REPO_ID>            Fetch config.json from Hugging Face (cached)
    --revision <REV>             Revision of --model [default: main]
    --model-config <PATH>        Local config.json
    --kv-bytes-per-token <N>     KV bytes per token, instead of a config
    --state-bytes <N>            Recurrent-state bytes per sequence, with --kv-bytes-per-token
    --kv-dtype <DTYPE>           Override the config's KV dtype
                                 (bfloat16, float16, float32, fp8, float8_e4m3fn)
    --model-name <NAME>          Model id for the metadata (with --model-config or explicit sizes)
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
cargo build --release            # target/release/cachesim
cargo test --workspace
```

## License

Apache-2.0
