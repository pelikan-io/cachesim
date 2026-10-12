//! Conversion of the public Mooncake trace (FAST '25) into a KV-cache object
//! trace.
//!
//! A Mooncake trace is JSON lines, one inference request per line:
//!
//! ```text
//! {"timestamp": 27482, "input_length": 6955, "output_length": 52, "hash_ids": [46, 47, ...]}
//! ```
//!
//! `timestamp` is in milliseconds and each entry of `hash_ids` names one
//! 512-token block of the prompt; equal ids mean equal prefix content. The
//! last block holds `input_length - 512 * (n - 1)` tokens, clamped to
//! `1..=512` because `input_length` does not always agree with the block
//! count.
//!
//! # Objects
//!
//! The defaults follow LMCache v0.5.5 (commit 05a013b):
//!
//! * One object is one chunk of `chunk_tokens` tokens (default 256,
//!   `lmcache/v1/config.py:90`) across all layers, so each 512-token block
//!   becomes `512 / chunk_tokens` chunks. A chunk's size is its token count
//!   times the model's KV bytes per token.
//! * With tensor parallelism each rank stores its own shard as a separate
//!   object of `1/tp` of the bytes (the key includes the worker id,
//!   `lmcache/utils.py:389-441`). MLA models are the exception: only the first
//!   rank stores, and it stores the full latent (`save_only_first_rank`,
//!   `cache_engine.py:116-119`).
//!
//! # Hybrid models
//!
//! Linear-attention layers keep a fixed-size recurrent state per sequence
//! instead of per-token KV. vLLM v0.31 snapshots that state only at some block
//! boundaries when prefix caching is on ("align" mode,
//! `vllm/config/cache.py:190-198`), so [`StateMode::PerRequest`] (one snapshot
//! at the request's last chunk) is the default and [`StateMode::PerChunk`] is
//! the upper bound. LMCache by default keeps attention and state pages in one
//! object (`separate_object_groups` off, `kv_layer_groups.py:332-358`):
//! [`StateLayout::Combined`] adds the state bytes to that chunk's object, as a
//! distinct object kind because its bytes differ from the same chunk without
//! state; [`StateLayout::Separate`] makes the state its own object. vLLM pads
//! the state page to the attention page size
//! (`platforms/interface.py:1006-1009`); that padding is not modelled.
//! Convolution state is ignored.
//!
//! # Rows
//!
//! Every row is a GET (`op = 1`): a serving engine reads a prefix chunk from
//! the store and, on a miss, computes and stores it, so the writes in a replay
//! come from the replayer's fill on miss. `ttl` is null. `obj_size` is the
//! first size seen for each `obj_id`; later rows that would compute a
//! different size (a partial last block that reappears as a full block) keep
//! the first size and are counted in `kvtrace.size_conflicts`.
//!
//! # Memory
//!
//! The input is read twice. The first pass computes every row's `obj_id` and
//! keeps that column in memory (8 bytes per row) to derive
//! `next_access_vtime` in a reverse pass, plus one entry per distinct object
//! for its size. The second pass regenerates the rows and streams them to the
//! Parquet writer. The input's SHA-256 is checked on both passes, so a file
//! that changes in between is an error.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::trace::{Op, TraceEntry, TraceWriter};
use crate::Error;

/// Tokens per Mooncake `hash_ids` block.
pub const SOURCE_BLOCK_TOKENS: u64 = 512;

/// Version of the `kvtrace.*` metadata written with each converted trace.
pub const METADATA_VERSION: &str = "1";

// ---------------------------------------------------------------------------
// Object ids
// ---------------------------------------------------------------------------

/// Object kind: per-token KV for one chunk.
pub const KIND_KV: u64 = 0;
/// Object kind: a recurrent-state snapshot on its own.
pub const KIND_STATE: u64 = 1;
/// Object kind: a chunk's KV and a state snapshot in one object.
pub const KIND_KV_WITH_STATE: u64 = 2;

const BLOCK_BITS: u32 = 40;
const CHUNK_BITS: u32 = 8;
const KIND_BITS: u32 = 4;
const RANK_BITS: u32 = 12;

/// Pack `(block, chunk, kind, rank)` into an object id:
/// `block << 24 | chunk << 16 | kind << 12 | rank`.
///
/// Errors if a field does not fit its bit width (40, 8, 4, 12 bits).
pub fn obj_id(block: u64, chunk: u64, kind: u64, rank: u64) -> Result<u64, Error> {
    if block >= 1 << BLOCK_BITS
        || chunk >= 1 << CHUNK_BITS
        || kind >= 1 << KIND_BITS
        || rank >= 1 << RANK_BITS
    {
        return Err(Error::InvalidFormat(format!(
            "mooncake: obj_id field out of range (block {block}, chunk {chunk}, \
             kind {kind}, rank {rank})"
        )));
    }
    Ok((((block << CHUNK_BITS | chunk) << KIND_BITS | kind) << RANK_BITS) | rank)
}

// ---------------------------------------------------------------------------
// Model sizes
// ---------------------------------------------------------------------------

/// Bytes per element for the dtype names accepted in a model config or
/// `--kv-dtype`.
pub fn dtype_bytes(name: &str) -> Option<u64> {
    match name {
        "bfloat16" | "float16" => Some(2),
        "float32" => Some(4),
        "fp8" | "float8_e4m3fn" => Some(1),
        _ => None,
    }
}

/// Dtype names accepted by [`dtype_bytes`].
pub const DTYPE_NAMES: &[&str] = &["bfloat16", "float16", "float32", "fp8", "float8_e4m3fn"];

/// KV-cache sizes of one model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSizes {
    /// KV bytes per token, summed over all full-attention layers (all ranks).
    pub kv_bytes_per_token: u64,
    /// Recurrent-state bytes per sequence, summed over all linear-attention
    /// layers (all ranks). Zero for models without linear attention.
    pub state_bytes_per_sequence: u64,
    /// Multi-head latent attention: only the first tensor-parallel rank stores.
    pub mla: bool,
    /// Full-attention layer count, when known from a config.
    pub full_attention_layers: Option<u64>,
    /// Linear-attention layer count, when known from a config.
    pub linear_attention_layers: Option<u64>,
}

impl ModelSizes {
    /// Sizes given directly rather than derived from a config.
    pub fn explicit(kv_bytes_per_token: u64, state_bytes_per_sequence: u64) -> Self {
        Self {
            kv_bytes_per_token,
            state_bytes_per_sequence,
            mla: false,
            full_attention_layers: None,
            linear_attention_layers: None,
        }
    }

    /// Read a Hugging Face `config.json` and derive the sizes.
    pub fn from_config_file(path: impl AsRef<Path>, kv_dtype: Option<&str>) -> Result<Self, Error> {
        let path = path.as_ref();
        Self::from_config_bytes(&std::fs::read(path)?, kv_dtype).map_err(|e| match e {
            Error::InvalidFormat(m) => Error::InvalidFormat(format!("{}: {m}", path.display())),
            e => e,
        })
    }

    /// Parse the bytes of a Hugging Face `config.json` and derive the sizes.
    pub fn from_config_bytes(bytes: &[u8], kv_dtype: Option<&str>) -> Result<Self, Error> {
        let config: Value =
            serde_json::from_slice(bytes).map_err(|e| bad_config(&e.to_string()))?;
        Self::from_config(&config, kv_dtype)
    }

    /// Derive the sizes from a parsed Hugging Face `config.json`.
    ///
    /// Fields are read from `text_config` when present (multimodal configs),
    /// otherwise from the top level.
    ///
    /// * Full-attention layers: the `"full_attention"` entries of
    ///   `layer_types`; else `num_hidden_layers / full_attention_interval`;
    ///   else all of `num_hidden_layers`. The rest are linear-attention layers.
    /// * KV bytes per token: full layers × `num_key_value_heads` × `head_dim`
    ///   (default `hidden_size / num_attention_heads`) × 2 (K and V) × dtype
    ///   bytes. MLA models (`kv_lora_rank` set): full layers ×
    ///   (`kv_lora_rank` + `qk_rope_head_dim`) × dtype bytes.
    /// * KV dtype: `kv_dtype`, else `torch_dtype` (text config, then top
    ///   level), else `dtype` (same order), else bfloat16.
    /// * State bytes per sequence: linear layers × `linear_num_value_heads` ×
    ///   `linear_key_head_dim` × `linear_value_head_dim` × `mamba_ssm_dtype`
    ///   bytes (default float32).
    pub fn from_config(config: &Value, kv_dtype: Option<&str>) -> Result<Self, Error> {
        let t = config.get("text_config").unwrap_or(config);
        let layers = req_u64(t, "num_hidden_layers")?;

        let full = match t.get("layer_types").and_then(Value::as_array) {
            Some(types) if !types.is_empty() => types
                .iter()
                .filter(|x| x.as_str() == Some("full_attention"))
                .count() as u64,
            _ => match opt_u64(t, "full_attention_interval")? {
                Some(interval) if interval > 0 => layers / interval,
                _ => layers,
            },
        };
        let linear = layers.checked_sub(full).ok_or_else(|| {
            bad_config(&format!(
                "{full} full-attention layers exceed num_hidden_layers ({layers})"
            ))
        })?;

        let dtype = match kv_dtype {
            Some(d) => d.to_string(),
            None => ["torch_dtype", "dtype"]
                .iter()
                .flat_map(|k| [t.get(*k), config.get(*k)])
                .flatten()
                .find_map(|v| v.as_str())
                .unwrap_or("bfloat16")
                .to_string(),
        };
        let dt =
            dtype_bytes(&dtype).ok_or_else(|| bad_config(&format!("unknown dtype {dtype:?}")))?;

        let kv_lora_rank = opt_u64(t, "kv_lora_rank")?.filter(|&r| r > 0);
        let kv_bytes_per_token = match kv_lora_rank {
            Some(rank) => full * (rank + req_u64(t, "qk_rope_head_dim")?) * dt,
            None => {
                let head_dim = match opt_u64(t, "head_dim")?.filter(|&h| h > 0) {
                    Some(h) => h,
                    None => req_u64(t, "hidden_size")? / req_u64(t, "num_attention_heads")?,
                };
                full * req_u64(t, "num_key_value_heads")? * head_dim * 2 * dt
            }
        };

        let state_bytes_per_sequence = if linear > 0 {
            let sdtype = t
                .get("mamba_ssm_dtype")
                .and_then(Value::as_str)
                .unwrap_or("float32");
            let sdt = dtype_bytes(sdtype)
                .ok_or_else(|| bad_config(&format!("unknown mamba_ssm_dtype {sdtype:?}")))?;
            linear
                * req_u64(t, "linear_num_value_heads")?
                * req_u64(t, "linear_key_head_dim")?
                * req_u64(t, "linear_value_head_dim")?
                * sdt
        } else {
            0
        };

        Ok(Self {
            kv_bytes_per_token,
            state_bytes_per_sequence,
            mla: kv_lora_rank.is_some(),
            full_attention_layers: Some(full),
            linear_attention_layers: Some(linear),
        })
    }
}

fn opt_u64(v: &Value, key: &str) -> Result<Option<u64>, Error> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => x
            .as_u64()
            .map(Some)
            .ok_or_else(|| bad_config(&format!("{key} is not a non-negative integer: {x}"))),
    }
}

fn req_u64(v: &Value, key: &str) -> Result<u64, Error> {
    opt_u64(v, key)?.ok_or_else(|| bad_config(&format!("missing {key}")))
}

fn bad_config(msg: &str) -> Error {
    Error::InvalidFormat(format!("model config: {msg}"))
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// When a hybrid model's recurrent-state snapshot is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMode {
    /// One snapshot at each request's last chunk.
    PerRequest,
    /// A snapshot at every chunk.
    PerChunk,
}

impl StateMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::PerRequest => "per-request",
            Self::PerChunk => "per-chunk",
        }
    }
}

/// Where a hybrid model's recurrent-state snapshot is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateLayout {
    /// In the chunk's object (object kind [`KIND_KV_WITH_STATE`]).
    Combined,
    /// As its own object (object kind [`KIND_STATE`]).
    Separate,
}

impl StateLayout {
    fn as_str(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Separate => "separate",
        }
    }
}

/// Settings for [`convert_mooncake_to_parquet`].
#[derive(Debug, Clone)]
pub struct MooncakeOptions {
    pub sizes: ModelSizes,
    /// Model id recorded in the metadata, e.g. `Qwen/Qwen3-8B`.
    pub model_name: Option<String>,
    /// Hugging Face revision the config was fetched at, for the metadata.
    pub model_revision: Option<String>,
    /// SHA-256 (hex) of the config.json the sizes came from, for the metadata.
    pub model_config_sha256: Option<String>,
    /// Tokens per object; must divide [`SOURCE_BLOCK_TOKENS`].
    pub chunk_tokens: u64,
    /// Tensor-parallel degree.
    pub tp: u64,
    pub state_mode: StateMode,
    pub state_layout: StateLayout,
}

impl MooncakeOptions {
    /// Options with the defaults: 256-token chunks, TP 1, per-request
    /// combined state.
    pub fn new(sizes: ModelSizes) -> Self {
        Self {
            sizes,
            model_name: None,
            model_revision: None,
            model_config_sha256: None,
            chunk_tokens: 256,
            tp: 1,
            state_mode: StateMode::PerRequest,
            state_layout: StateLayout::Combined,
        }
    }

    fn validate(&self) -> Result<(), Error> {
        let c = self.chunk_tokens;
        if c == 0
            || !SOURCE_BLOCK_TOKENS.is_multiple_of(c)
            || SOURCE_BLOCK_TOKENS / c > 1 << CHUNK_BITS
        {
            return Err(Error::InvalidFormat(format!(
                "mooncake: chunk tokens ({c}) must divide the source block \
                 ({SOURCE_BLOCK_TOKENS}) into at most {} chunks",
                1 << CHUNK_BITS
            )));
        }
        if self.tp == 0 || self.tp > 1 << RANK_BITS {
            return Err(Error::InvalidFormat(format!(
                "mooncake: tp ({}) must be between 1 and {}",
                self.tp,
                1 << RANK_BITS
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Request expansion
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct MooncakeRecord {
    timestamp: i64,
    input_length: i64,
    hash_ids: Vec<u64>,
}

/// One request: timestamp in ns and `(hash_id, tokens)` per block.
struct Request {
    timestamp_ns: i64,
    blocks: Vec<(u64, u64)>,
}

fn parse_request(line: &str, lineno: usize) -> Result<Request, Error> {
    let r: MooncakeRecord = serde_json::from_str(line)
        .map_err(|e| Error::InvalidFormat(format!("mooncake line {lineno}: {e}")))?;
    let n = r.hash_ids.len() as i64;
    let block = SOURCE_BLOCK_TOKENS as i64;
    let last = (r.input_length - block * (n - 1)).clamp(1, block) as u64;
    let blocks = r
        .hash_ids
        .iter()
        .enumerate()
        .map(|(i, &h)| {
            let tokens = if (i as i64) < n - 1 {
                SOURCE_BLOCK_TOKENS
            } else {
                last
            };
            (h, tokens)
        })
        .collect();
    let timestamp_ns = r.timestamp.checked_mul(1_000_000).ok_or_else(|| {
        Error::InvalidFormat(format!("mooncake line {lineno}: timestamp out of range"))
    })?;
    Ok(Request {
        timestamp_ns,
        blocks,
    })
}

/// Call `emit(obj_id, size)` for each object a request reads, in row order.
fn expand_request(
    req: &Request,
    opts: &MooncakeOptions,
    mut emit: impl FnMut(u64, u64) -> Result<(), Error>,
) -> Result<(), Error> {
    let per_block = SOURCE_BLOCK_TOKENS / opts.chunk_tokens;
    let kvt = opts.sizes.kv_bytes_per_token;
    let state = opts.sizes.state_bytes_per_sequence;
    // MLA: only the first rank stores, and it stores the whole latent.
    let ranks = if opts.sizes.mla { 1 } else { opts.tp };

    let mut chunks = Vec::new();
    for &(block, tokens) in &req.blocks {
        for c in 0..per_block {
            let start = c * opts.chunk_tokens;
            if start >= tokens {
                break;
            }
            chunks.push((block, c, opts.chunk_tokens.min(tokens - start)));
        }
    }

    let n = chunks.len();
    for (i, &(block, c, ctok)) in chunks.iter().enumerate() {
        let snap = state > 0
            && match opts.state_mode {
                StateMode::PerChunk => true,
                StateMode::PerRequest => i == n - 1,
            };
        let combined = snap && opts.state_layout == StateLayout::Combined;
        let (kind, bytes) = if combined {
            (KIND_KV_WITH_STATE, kvt * ctok + state)
        } else {
            (KIND_KV, kvt * ctok)
        };
        for rank in 0..ranks {
            emit(obj_id(block, c, kind, rank)?, bytes / ranks)?;
        }
        if snap && !combined {
            for rank in 0..ranks {
                emit(obj_id(block, c, KIND_STATE, rank)?, state / ranks)?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

/// Result of [`convert_mooncake_to_parquet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MooncakeSummary {
    /// Rows written.
    pub rows: usize,
    /// Requests (non-empty input lines) read.
    pub requests: usize,
    /// Distinct object ids.
    pub objects: usize,
    /// Rows whose computed size differed from the first size of their id.
    pub size_conflicts: usize,
}

/// Reader that feeds every byte it returns into a SHA-256.
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Read every request of `path`, call `f` on each, and return the file's
/// SHA-256 as lowercase hex.
fn for_each_request(
    path: &Path,
    mut f: impl FnMut(&Request) -> Result<(), Error>,
) -> Result<String, Error> {
    let mut reader = BufReader::new(HashingReader {
        inner: File::open(path)?,
        hasher: Sha256::new(),
    });
    let mut line = String::new();
    let mut lineno = 0;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        lineno += 1;
        if line.trim().is_empty() {
            continue;
        }
        f(&parse_request(&line, lineno)?)?;
    }
    Ok(hex(&reader.into_inner().hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 of `bytes` as lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Convert a Mooncake trace (JSON lines) into a KV-cache object trace in
/// Parquet. See the [module documentation](self) for the object model.
///
/// Provenance is written to the Parquet footer as key-value metadata under
/// `kvtrace.*` keys.
pub fn convert_mooncake_to_parquet(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    opts: &MooncakeOptions,
    batch_size: usize,
) -> Result<MooncakeSummary, Error> {
    opts.validate()?;
    let input = input.as_ref();
    let created = utc_now_iso8601();

    // Pass 1: object ids and the first size of each.
    let mut ids: Vec<u64> = Vec::new();
    let mut first_size: HashMap<u64, u32> = HashMap::new();
    let mut size_conflicts = 0usize;
    let mut requests = 0usize;
    let sha256 = for_each_request(input, |req| {
        requests += 1;
        expand_request(req, opts, |id, size| {
            let size = u32::try_from(size).map_err(|_| {
                Error::InvalidFormat(format!(
                    "mooncake: an object of {size} bytes exceeds the obj_size column's u32 range"
                ))
            })?;
            if *first_size.entry(id).or_insert(size) != size {
                size_conflicts += 1;
            }
            ids.push(id);
            Ok(())
        })
    })?;

    // Reverse pass: row index of each id's next access.
    let mut next_access = vec![-1i64; ids.len()];
    let mut seen: HashMap<u64, i64> = HashMap::with_capacity(first_size.len());
    for (i, &id) in ids.iter().enumerate().rev() {
        if let Some(j) = seen.insert(id, i as i64) {
            next_access[i] = j;
        }
    }
    drop(seen);
    let rows = ids.len();
    drop(ids);

    let sizes = &opts.sizes;
    let state = sizes.state_bytes_per_sequence;
    let opt_count = |v: Option<u64>| v.map_or("None".to_string(), |n| n.to_string());
    let mut metadata = vec![
        ("kvtrace.version", METADATA_VERSION.to_string()),
        ("kvtrace.created", created),
        ("kvtrace.source.format", "mooncake".to_string()),
        (
            "kvtrace.source.file",
            input
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        ("kvtrace.source.sha256", sha256.clone()),
        (
            "kvtrace.source.block_tokens",
            SOURCE_BLOCK_TOKENS.to_string(),
        ),
        ("kvtrace.model", opts.model_name.clone().unwrap_or_default()),
        (
            "kvtrace.kv_bytes_per_token",
            sizes.kv_bytes_per_token.to_string(),
        ),
        ("kvtrace.state_bytes_per_sequence", state.to_string()),
        (
            "kvtrace.full_attention_layers",
            opt_count(sizes.full_attention_layers),
        ),
        (
            "kvtrace.linear_attention_layers",
            opt_count(sizes.linear_attention_layers),
        ),
        ("kvtrace.chunk_tokens", opts.chunk_tokens.to_string()),
        ("kvtrace.tp", opts.tp.to_string()),
        (
            "kvtrace.state_mode",
            if state > 0 {
                opts.state_mode.as_str()
            } else {
                "none"
            }
            .to_string(),
        ),
        (
            "kvtrace.state_layout",
            if state > 0 {
                opts.state_layout.as_str()
            } else {
                "none"
            }
            .to_string(),
        ),
        (
            "kvtrace.mla_first_rank_only",
            if sizes.mla { "True" } else { "False" }.to_string(),
        ),
        (
            "kvtrace.ops",
            "every row is GET; SETs come from the replayer's backfill on miss".to_string(),
        ),
        (
            "kvtrace.obj_id_layout",
            format!(
                "block<<{} | chunk<<{} | kind<<{} | rank; \
                 kind 0 = KV, 1 = recurrent state, 2 = KV and state in one object",
                CHUNK_BITS + KIND_BITS + RANK_BITS,
                KIND_BITS + RANK_BITS,
                RANK_BITS
            ),
        ),
        ("kvtrace.requests", requests.to_string()),
        ("kvtrace.size_conflicts", size_conflicts.to_string()),
    ];
    if let Some(rev) = &opts.model_revision {
        metadata.push(("kvtrace.model.revision", rev.clone()));
    }
    if let Some(sha) = &opts.model_config_sha256 {
        metadata.push(("kvtrace.model.config_sha256", sha.clone()));
    }

    // Pass 2: regenerate the rows and stream them out.
    let mut writer = TraceWriter::create(output, batch_size)?;
    for (k, v) in metadata {
        writer.append_key_value_metadata(k, v);
    }
    let mut row = 0usize;
    let sha256_again = for_each_request(input, |req| {
        expand_request(req, opts, |id, _| {
            let (size, next) = match (first_size.get(&id), next_access.get(row)) {
                (Some(&s), Some(&n)) => (s, n),
                _ => return Err(changed_input()),
            };
            writer.write(&TraceEntry {
                timestamp: req.timestamp_ns,
                obj_id: id,
                obj_size: size,
                next_access_vtime: next,
                op: Some(Op::Get as u8),
                ttl: None,
            })?;
            row += 1;
            Ok(())
        })
    })?;
    if sha256_again != sha256 || row != rows {
        return Err(changed_input());
    }
    writer.finish()?;

    Ok(MooncakeSummary {
        rows,
        requests,
        objects: first_size.len(),
        size_conflicts,
    })
}

fn changed_input() -> Error {
    Error::InvalidFormat("mooncake: input changed between the two read passes".into())
}

/// Current UTC time as `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`.
fn utc_now_iso8601() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}+00:00",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        d.subsec_micros()
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::TraceReader;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use serde_json::json;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mooncake")
            .join(name)
    }

    #[test]
    fn sizes_dense_model_from_config() {
        // 36 layers x 8 KV heads x head_dim 128 x 2 (K, V) x 2 bytes (bf16).
        let s = ModelSizes::from_config_file(fixture("qwen3-8b.json"), None).unwrap();
        assert_eq!(s.kv_bytes_per_token, 147_456);
        assert_eq!(s.state_bytes_per_sequence, 0);
        assert!(!s.mla);
        assert_eq!(s.full_attention_layers, Some(36));
        assert_eq!(s.linear_attention_layers, Some(0));
    }

    #[test]
    fn sizes_hybrid_model_from_config() {
        // text_config, layer_types: 16 full-attention and 48 linear layers.
        // KV: 16 x 4 KV heads x head_dim 256 x 2 x 2 bytes (dtype bf16).
        // State: 48 x 48 value heads x 128 x 128 x 4 bytes (mamba_ssm_dtype fp32).
        let s = ModelSizes::from_config_file(fixture("qwen3.8-27b.json"), None).unwrap();
        assert_eq!(s.kv_bytes_per_token, 65_536);
        assert_eq!(s.state_bytes_per_sequence, 150_994_944);
        assert_eq!(s.full_attention_layers, Some(16));
        assert_eq!(s.linear_attention_layers, Some(48));
    }

    #[test]
    fn sizes_kv_dtype_override() {
        let s = ModelSizes::from_config_file(fixture("qwen3-8b.json"), Some("fp8")).unwrap();
        assert_eq!(s.kv_bytes_per_token, 147_456 / 2);
        assert!(ModelSizes::from_config_file(fixture("qwen3-8b.json"), Some("int3")).is_err());
    }

    #[test]
    fn sizes_full_attention_interval_without_layer_types() {
        let c = json!({
            "num_hidden_layers": 48, "full_attention_interval": 4,
            "num_key_value_heads": 2, "head_dim": 256, "torch_dtype": "bfloat16",
            "linear_num_value_heads": 32, "linear_key_head_dim": 128,
            "linear_value_head_dim": 128,
        });
        let s = ModelSizes::from_config(&c, None).unwrap();
        // 12 full layers x 2 x 256 x 2 x 2; 36 linear layers x 32 x 128 x 128 x 4.
        assert_eq!(s.kv_bytes_per_token, 24_576);
        assert_eq!(s.state_bytes_per_sequence, 75_497_472);
    }

    #[test]
    fn sizes_mla_and_head_dim_fallback() {
        let mla = json!({
            "num_hidden_layers": 61, "kv_lora_rank": 512, "qk_rope_head_dim": 64,
            "num_key_value_heads": 128, "head_dim": 192, "torch_dtype": "bfloat16",
        });
        let s = ModelSizes::from_config(&mla, None).unwrap();
        assert_eq!(s.kv_bytes_per_token, 61 * (512 + 64) * 2);
        assert!(s.mla);

        // No head_dim: hidden_size / num_attention_heads; float16; no dtype at all
        // falls back to bf16 (also 2 bytes).
        let dense = json!({
            "num_hidden_layers": 32, "hidden_size": 4096, "num_attention_heads": 32,
            "num_key_value_heads": 8,
        });
        let s = ModelSizes::from_config(&dense, None).unwrap();
        assert_eq!(s.kv_bytes_per_token, 32 * 8 * 128 * 2 * 2);
    }

    #[test]
    fn obj_id_layout() {
        assert_eq!(obj_id(0, 0, 0, 0).unwrap(), 0);
        assert_eq!(obj_id(1, 0, 0, 0).unwrap(), 1 << 24);
        assert_eq!(obj_id(0, 1, 0, 0).unwrap(), 1 << 16);
        assert_eq!(obj_id(0, 0, 2, 0).unwrap(), 2 << 12);
        assert_eq!(obj_id(0, 0, 0, 7).unwrap(), 7);
        assert_eq!(obj_id(5, 3, 1, 9).unwrap(), 5 << 24 | 3 << 16 | 1 << 12 | 9);
        assert!(obj_id(1 << 40, 0, 0, 0).is_err());
        assert!(obj_id(0, 256, 0, 0).is_err());
        assert!(obj_id(0, 0, 16, 0).is_err());
        assert!(obj_id(0, 0, 0, 4096).is_err());
    }

    fn expand(line: &str, opts: &MooncakeOptions) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        expand_request(&parse_request(line, 1).unwrap(), opts, |id, size| {
            out.push((id, size));
            Ok(())
        })
        .unwrap();
        out
    }

    fn id(block: u64, chunk: u64, kind: u64, rank: u64) -> u64 {
        obj_id(block, chunk, kind, rank).unwrap()
    }

    #[test]
    fn partial_last_block_is_clamped() {
        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        // 700 tokens: block 0 full, block 1 has 188 tokens.
        let line = r#"{"timestamp": 0, "input_length": 700, "hash_ids": [0, 1]}"#;
        assert_eq!(
            expand(line, &opts),
            [
                (id(0, 0, 0, 0), 2560),
                (id(0, 1, 0, 0), 2560),
                (id(1, 0, 0, 0), 1880)
            ]
        );
        // input_length beyond the blocks: the last block is capped at 512.
        let line = r#"{"timestamp": 0, "input_length": 2000, "hash_ids": [3]}"#;
        assert_eq!(
            expand(line, &opts),
            [(id(3, 0, 0, 0), 2560), (id(3, 1, 0, 0), 2560)]
        );
        // input_length short of the blocks: the last block still has 1 token.
        let line = r#"{"timestamp": 0, "input_length": 300, "hash_ids": [4, 5]}"#;
        assert_eq!(
            expand(line, &opts),
            [
                (id(4, 0, 0, 0), 2560),
                (id(4, 1, 0, 0), 2560),
                (id(5, 0, 0, 0), 10)
            ]
        );
    }

    #[test]
    fn tp_split_and_mla_first_rank() {
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        opts.tp = 4;
        let line = r#"{"timestamp": 0, "input_length": 100, "hash_ids": [7]}"#;
        assert_eq!(
            expand(line, &opts),
            (0..4).map(|r| (id(7, 0, 0, r), 250)).collect::<Vec<_>>()
        );
        opts.sizes.mla = true;
        assert_eq!(expand(line, &opts), [(id(7, 0, 0, 0), 1000)]);
    }

    #[test]
    fn state_modes_and_layouts() {
        let line = r#"{"timestamp": 0, "input_length": 768, "hash_ids": [3, 4]}"#;
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 1000));
        opts.tp = 2;

        // per-request, combined: the last chunk carries the state, as kind 2.
        assert_eq!(
            expand(line, &opts),
            [
                (id(3, 0, 0, 0), 1280),
                (id(3, 0, 0, 1), 1280),
                (id(3, 1, 0, 0), 1280),
                (id(3, 1, 0, 1), 1280),
                (id(4, 0, 2, 0), 1780),
                (id(4, 0, 2, 1), 1780),
            ]
        );

        // per-request, separate: the state is its own kind-1 object.
        opts.state_layout = StateLayout::Separate;
        assert_eq!(
            expand(line, &opts)[4..],
            [
                (id(4, 0, 0, 0), 1280),
                (id(4, 0, 0, 1), 1280),
                (id(4, 0, 1, 0), 500),
                (id(4, 0, 1, 1), 500),
            ]
        );

        // per-chunk, combined: every chunk is kind 2.
        opts.state_mode = StateMode::PerChunk;
        opts.state_layout = StateLayout::Combined;
        let rows = expand(line, &opts);
        assert_eq!(rows.len(), 6);
        assert!(rows
            .iter()
            .all(|&(oid, size)| oid >> 12 & 0xf == 2 && size == 1780));
    }

    #[test]
    fn options_are_validated() {
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        opts.chunk_tokens = 300;
        assert!(opts.validate().is_err());
        opts.chunk_tokens = 1; // 512 chunks per block do not fit 8 bits
        assert!(opts.validate().is_err());
        opts.chunk_tokens = 2;
        assert!(opts.validate().is_ok());
        opts.tp = 0;
        assert!(opts.validate().is_err());
    }

    fn convert_fixture(
        opts: &MooncakeOptions,
    ) -> (MooncakeSummary, Vec<TraceEntry>, HashMap<String, String>) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        // Batch size 4 so the output spans several record batches.
        let summary = convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, opts, 4).unwrap();
        let rows: Vec<TraceEntry> = TraceReader::open(&out)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let reader = SerializedFileReader::new(File::open(&out).unwrap()).unwrap();
        let meta = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .filter(|kv| kv.key.starts_with("kvtrace."))
            .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
            .collect();
        (summary, rows, meta)
    }

    #[test]
    fn end_to_end_dense() {
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        opts.model_name = Some("test/dense".into());
        let (summary, rows, meta) = convert_fixture(&opts);

        let expect: &[(i64, u64, u32, i64)] = &[
            // request 1, t = 0 ms, 700 tokens: [0, 1]
            (0, id(0, 0, 0, 0), 2560, 3),
            (0, id(0, 1, 0, 0), 2560, 4),
            (0, id(1, 0, 0, 0), 1880, 5),
            // request 2, t = 3 ms, 1100 tokens: [0, 1, 2]
            (3, id(0, 0, 0, 0), 2560, -1),
            (3, id(0, 1, 0, 0), 2560, -1),
            (3, id(1, 0, 0, 0), 1880, -1), // computes 2560; keeps first size
            (3, id(1, 1, 0, 0), 2560, -1),
            (3, id(2, 0, 0, 0), 760, -1),
            // request 3, t = 5 ms, 2000 tokens: [3]
            (5, id(3, 0, 0, 0), 2560, -1),
            (5, id(3, 1, 0, 0), 2560, -1),
            // request 4, t = 7 ms, 300 tokens: [4, 5]
            (7, id(4, 0, 0, 0), 2560, -1),
            (7, id(4, 1, 0, 0), 2560, -1),
            (7, id(5, 0, 0, 0), 10, -1),
        ];
        assert_eq!(rows.len(), expect.len());
        for (i, (row, &(ms, oid, size, next))) in rows.iter().zip(expect).enumerate() {
            assert_eq!(row.timestamp, ms * 1_000_000, "row {i}");
            assert_eq!(row.obj_id, oid, "row {i}");
            assert_eq!(row.obj_size, size, "row {i}");
            assert_eq!(row.next_access_vtime, next, "row {i}");
            assert_eq!(row.op, Some(Op::Get as u8), "row {i}");
            assert_eq!(row.ttl, None, "row {i}");
        }
        assert_eq!(
            summary,
            MooncakeSummary {
                rows: 13,
                requests: 4,
                objects: 10,
                size_conflicts: 1
            }
        );

        let source = std::fs::read(fixture("trace.jsonl")).unwrap();
        let sha = sha256_hex(&source);
        for (k, v) in [
            ("kvtrace.version", "1"),
            ("kvtrace.source.format", "mooncake"),
            ("kvtrace.source.file", "trace.jsonl"),
            ("kvtrace.source.sha256", sha.as_str()),
            ("kvtrace.source.block_tokens", "512"),
            ("kvtrace.model", "test/dense"),
            ("kvtrace.kv_bytes_per_token", "10"),
            ("kvtrace.state_bytes_per_sequence", "0"),
            ("kvtrace.full_attention_layers", "None"),
            ("kvtrace.chunk_tokens", "256"),
            ("kvtrace.tp", "1"),
            ("kvtrace.state_mode", "none"),
            ("kvtrace.state_layout", "none"),
            ("kvtrace.mla_first_rank_only", "False"),
            ("kvtrace.requests", "4"),
            ("kvtrace.size_conflicts", "1"),
        ] {
            assert_eq!(meta.get(k).map(String::as_str), Some(v), "{k}");
        }
        assert!(meta["kvtrace.created"].ends_with("+00:00"));
        assert_eq!(meta.len(), 20);
    }

    #[test]
    fn end_to_end_hybrid_tp() {
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 1000));
        opts.tp = 2;
        let (summary, rows, meta) = convert_fixture(&opts);

        let mut expect: Vec<(u64, u32)> = Vec::new();
        let mut both = |oid: fn(u64) -> u64, size| expect.extend((0..2).map(|r| (oid(r), size)));
        // request 1: the last chunk (block 1, 188 tokens) carries the state.
        both(|r| id(0, 0, 0, r), 1280);
        both(|r| id(0, 1, 0, r), 1280);
        both(|r| id(1, 0, 2, r), 1440); // (1880 + 1000) / 2
                                        // request 2
        both(|r| id(0, 0, 0, r), 1280);
        both(|r| id(0, 1, 0, r), 1280);
        both(|r| id(1, 0, 0, r), 1280); // kind 0: a different object from kind 2
        both(|r| id(1, 1, 0, r), 1280);
        both(|r| id(2, 0, 2, r), 880); // (760 + 1000) / 2
                                       // request 3
        both(|r| id(3, 0, 0, r), 1280);
        both(|r| id(3, 1, 2, r), 1780);
        // request 4
        both(|r| id(4, 0, 0, r), 1280);
        both(|r| id(4, 1, 0, r), 1280);
        both(|r| id(5, 0, 2, r), 505); // (10 + 1000) / 2

        let got: Vec<(u64, u32)> = rows.iter().map(|r| (r.obj_id, r.obj_size)).collect();
        assert_eq!(got, expect);
        let next: Vec<i64> = rows.iter().map(|r| r.next_access_vtime).collect();
        let mut want = vec![-1i64; 26];
        want[..4].copy_from_slice(&[6, 7, 8, 9]);
        assert_eq!(next, want);
        assert_eq!(summary.size_conflicts, 0);
        assert_eq!(summary.objects, 22);
        assert_eq!(meta["kvtrace.tp"], "2");
        assert_eq!(meta["kvtrace.state_mode"], "per-request");
        assert_eq!(meta["kvtrace.state_layout"], "combined");
        assert_eq!(meta["kvtrace.state_bytes_per_sequence"], "1000");
    }
}
