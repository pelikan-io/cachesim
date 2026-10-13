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
//!   object (the key includes the worker id, `lmcache/utils.py:389-441`).
//!   A shard is `1/min(tp, kv_heads)` of the KV, since vLLM replicates KV
//!   heads once `tp` reaches their count, and `1/tp` of the recurrent state;
//!   see [`MooncakeOptions`]. MLA models are the exception: only the first
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
//! The input is read twice through one rewound handle, so it must be an
//! uncompressed regular file: a pipe is rejected, while `/dev/stdin`
//! redirected from a regular file works. The first pass computes every
//! row's `obj_id` into one 8-byte-per-row column. A reverse scan then
//! overwrites that column in place with `next_access_vtime`, using a map with
//! one entry per distinct object that is freed afterwards. The column (8 bytes
//! per row) and a second map from each distinct object to its size stay in
//! memory through the second pass, which regenerates the rows and streams
//! them to the Parquet writer. The input's SHA-256 is checked on both passes,
//! so a file that changes in between is an error. Output handling (refusals,
//! symlinks, the temporary file) is described on `plan_output`.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
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
    /// `num_key_value_heads`, when known from a config. Bounds how many ways
    /// tensor parallelism splits the KV (see [`MooncakeOptions`]).
    pub kv_heads: Option<u64>,
    /// `linear_num_value_heads`, when known from a config. Tensor parallelism
    /// must divide it.
    pub state_heads: Option<u64>,
    /// `num_attention_heads`, when known from a config. Tensor parallelism
    /// must divide it.
    pub attention_heads: Option<u64>,
    /// Width of the linear-attention convolution input,
    /// `2 × linear_num_key_heads × linear_key_head_dim +
    /// linear_num_value_heads × linear_value_head_dim`, when known from a
    /// config. Tensor parallelism must divide it.
    pub state_conv_dim: Option<u64>,
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
            kv_heads: None,
            state_heads: None,
            attention_heads: None,
            state_conv_dim: None,
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
    ///   A `layer_types` entry other than `"full_attention"` or
    ///   `"linear_attention"`, a `sliding_window` without `layer_types`, or a
    ///   Mamba-style hybrid layout (`hybrid_override_pattern`,
    ///   `layers_block_type`) is an error, since those layers cannot be sized
    ///   here.
    /// * KV bytes per token: full layers × `num_key_value_heads` × `head_dim`
    ///   (default `hidden_size / num_attention_heads`) × 2 (K and V) × dtype
    ///   bytes. MLA models (`kv_lora_rank` set): full layers ×
    ///   (`kv_lora_rank` + `qk_rope_head_dim`) × dtype bytes.
    /// * KV dtype: `kv_dtype`, else `torch_dtype` (text config, then top
    ///   level), else `dtype` (same order), else bfloat16.
    /// * State bytes per sequence: linear layers × `linear_num_value_heads` ×
    ///   `linear_key_head_dim` × `linear_value_head_dim` × state dtype bytes.
    ///   The state dtype is `mamba_ssm_dtype`, else the model dtype
    ///   (`torch_dtype`, else `dtype`, else bfloat16; not `kv_dtype`).
    ///
    /// Products that overflow `u64` are an error.
    pub fn from_config(config: &Value, kv_dtype: Option<&str>) -> Result<Self, Error> {
        let t = config.get("text_config").unwrap_or(config);
        let layers = req_u64(t, "num_hidden_layers")?;

        for key in ["hybrid_override_pattern", "layers_block_type"] {
            if t.get(key).is_some_and(|v| !v.is_null()) {
                return Err(bad_config(&format!(
                    "{key} (Mamba-style hybrid layers) is not supported"
                )));
            }
        }

        let full = match t.get("layer_types").and_then(Value::as_array) {
            Some(types) if !types.is_empty() => {
                if types.len() as u64 != layers {
                    return Err(bad_config(&format!(
                        "layer_types has {} entries but num_hidden_layers is {layers}",
                        types.len()
                    )));
                }
                let mut full = 0;
                for x in types {
                    match x.as_str() {
                        Some("full_attention") => full += 1,
                        Some("linear_attention") => {}
                        _ => {
                            return Err(bad_config(&format!(
                                "unsupported layer_types entry {x} (only full_attention \
                                 and linear_attention can be sized)"
                            )))
                        }
                    }
                }
                full
            }
            _ if t.get("sliding_window").is_some_and(|v| {
                !v.is_null() && t.get("use_sliding_window") != Some(&Value::Bool(false))
            }) =>
            {
                return Err(bad_config(
                    "sliding_window without layer_types: sliding-window layers cannot be sized",
                ));
            }
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

        let model_dtype = ["torch_dtype", "dtype"]
            .iter()
            .flat_map(|k| [t.get(*k), config.get(*k)])
            .flatten()
            .find_map(|v| v.as_str())
            .unwrap_or("bfloat16");
        let dtype = kv_dtype.unwrap_or(model_dtype);
        let dt =
            dtype_bytes(dtype).ok_or_else(|| bad_config(&format!("unknown dtype {dtype:?}")))?;

        let kv_lora_rank = opt_u64(t, "kv_lora_rank")?.filter(|&r| r > 0);
        let kv_bytes_per_token = match kv_lora_rank {
            Some(rank) => {
                let latent = rank
                    .checked_add(req_u64(t, "qk_rope_head_dim")?)
                    .ok_or_else(overflow)?;
                product(&[full, latent, dt])?
            }
            None => {
                let head_dim = match opt_u64(t, "head_dim")?.filter(|&h| h > 0) {
                    Some(h) => h,
                    None => req_u64(t, "hidden_size")?
                        .checked_div(req_u64(t, "num_attention_heads")?)
                        .ok_or_else(|| bad_config("num_attention_heads is 0"))?,
                };
                product(&[full, req_u64(t, "num_key_value_heads")?, head_dim, 2, dt])?
            }
        };
        let kv_heads = match kv_lora_rank {
            Some(_) => None,
            None => Some(req_u64(t, "num_key_value_heads")?),
        };

        let attention_heads = opt_u64(t, "num_attention_heads")?;
        let (state_heads, state_conv_dim) = if linear > 0 {
            let v_heads = req_u64(t, "linear_num_value_heads")?;
            let k_part = product(&[
                2,
                req_u64(t, "linear_num_key_heads")?,
                req_u64(t, "linear_key_head_dim")?,
            ])?;
            let v_part = product(&[v_heads, req_u64(t, "linear_value_head_dim")?])?;
            let conv_dim = k_part.checked_add(v_part).ok_or_else(overflow)?;
            (Some(v_heads), Some(conv_dim))
        } else {
            (None, None)
        };
        let state_bytes_per_sequence = if linear > 0 {
            // Without mamba_ssm_dtype the state takes the model dtype. vLLM
            // v0.31 (db9527a), vllm/model_executor/layers/mamba/mamba_utils.py:98-110:
            // with mamba_ssm_cache_dtype "auto" the SSM state uses the conv-state
            // dtype, which defaults to the model dtype; Qwen3-Next's gated delta
            // net takes this path (models/qwen3_next.py:879-887). Qwen3.5 instead
            // copies the config's mamba_ssm_dtype (models/config.py:819-843).
            let sdtype = t
                .get("mamba_ssm_dtype")
                .and_then(Value::as_str)
                .unwrap_or(model_dtype);
            let sdt = dtype_bytes(sdtype)
                .ok_or_else(|| bad_config(&format!("unknown state dtype {sdtype:?}")))?;
            product(&[
                linear,
                req_u64(t, "linear_num_value_heads")?,
                req_u64(t, "linear_key_head_dim")?,
                req_u64(t, "linear_value_head_dim")?,
                sdt,
            ])?
        } else {
            0
        };

        Ok(Self {
            kv_bytes_per_token,
            state_bytes_per_sequence,
            mla: kv_lora_rank.is_some(),
            full_attention_layers: Some(full),
            linear_attention_layers: Some(linear),
            kv_heads,
            state_heads,
            attention_heads,
            state_conv_dim,
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

/// Product of `factors`, or an error if it overflows `u64`.
fn product(factors: &[u64]) -> Result<u64, Error> {
    factors
        .iter()
        .try_fold(1u64, |acc, &f| acc.checked_mul(f))
        .ok_or_else(overflow)
}

fn overflow() -> Error {
    Error::InvalidFormat("mooncake: a size computation overflows u64".into())
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
///
/// # Tensor parallelism
///
/// Each of the `tp` ranks stores its own object per chunk. A rank's share of
/// the KV follows vLLM v0.31 (db9527a), which splits KV heads across ranks
/// and replicates them once `tp` reaches the head count
/// (`vllm/model_executor/layers/linear.py:1053-1058`, QKVParallelLinear;
/// `vllm/config/model.py:1625-1628`, `get_num_kv_heads`): with `h` KV heads a
/// rank holds `1/min(tp, h)` of the KV. vLLM requires `h % tp == 0` when
/// `tp < h` and `tp % h == 0` otherwise; other values are an error here too.
/// Without a head count (`--kv-bytes-per-token`) the share is `1/tp`.
///
/// The recurrent state is split by linear-attention value heads, and vLLM
/// requires `tp` to divide their count (`divide(num_v_heads, tp_world_size)`,
/// `vllm/model_executor/layers/mamba/mamba_utils.py:295-299`), so a rank
/// holds `1/tp` of the state and a `tp` that does not divide the count is an
/// error. Qwen3.8-27B: 48 value heads, 6 per rank at `tp` 8.
///
/// vLLM also requires `tp` to divide `num_attention_heads`
/// (`vllm/model_executor/layers/linear.py:1052`) and the linear-attention
/// convolution width (`mamba_utils.py:289-293`); both are checked when the
/// config gives them.
///
/// Per-rank sizes must be whole bytes: a KV size per token or a state size
/// that the divisor does not divide is an error rather than rounded. MLA
/// models are stored by the first rank only, at full size.
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

    /// Objects per chunk and the per-rank sizes; see [the type
    /// docs](Self#tensor-parallelism).
    fn sharding(&self) -> Result<Sharding, Error> {
        self.validate()?;
        let tp = self.tp;
        let s = &self.sizes;
        let tp_err = |msg: String| Error::InvalidFormat(format!("mooncake: tp {tp}: {msg}"));

        // Head counts vLLM requires tp to fit (see the type docs).
        let kv_div = match s.kv_heads {
            Some(0) => return Err(tp_err("num_key_value_heads is 0".into())),
            Some(h) if tp >= h && !tp.is_multiple_of(h) => {
                return Err(tp_err(format!("not a multiple of the {h} KV heads")))
            }
            Some(h) if tp < h && !h.is_multiple_of(tp) => {
                return Err(tp_err(format!("does not divide the {h} KV heads")))
            }
            Some(h) => tp.min(h),
            None => tp,
        };
        let state = s.state_bytes_per_sequence;
        if let Some(h) = s.state_heads.filter(|_| state > 0) {
            if !h.is_multiple_of(tp) {
                return Err(tp_err(format!(
                    "does not divide the {h} linear-attention value heads"
                )));
            }
        }
        // vLLM also splits the attention heads evenly
        // (`self.num_heads = divide(self.total_num_heads, tp_size)`,
        // vllm/model_executor/layers/linear.py:1052) and the linear-attention
        // convolution input (`divide(conv_dim, tp_world_size)`,
        // vllm/model_executor/layers/mamba/mamba_utils.py:289-293).
        if let Some(h) = s.attention_heads {
            if h == 0 || !h.is_multiple_of(tp) {
                return Err(tp_err(format!("does not divide the {h} attention heads")));
            }
        }
        if let Some(d) = s.state_conv_dim.filter(|_| state > 0) {
            if d == 0 || !d.is_multiple_of(tp) {
                return Err(tp_err(format!(
                    "does not divide the linear-attention convolution width {d}"
                )));
            }
        }

        if s.mla {
            // Only the first rank stores, and it stores the whole latent.
            return Ok(Sharding {
                ranks: 1,
                kv_div: 1,
                state_div: 1,
                kv_per_token: s.kv_bytes_per_token,
                state,
            });
        }
        let state_div = tp;
        if !s.kv_bytes_per_token.is_multiple_of(kv_div) {
            return Err(tp_err(format!(
                "{} KV bytes per token do not split evenly {kv_div} ways",
                s.kv_bytes_per_token
            )));
        }
        if !state.is_multiple_of(state_div) {
            return Err(tp_err(format!(
                "{state} state bytes do not split evenly {state_div} ways"
            )));
        }
        Ok(Sharding {
            ranks: tp,
            kv_div,
            state_div,
            kv_per_token: s.kv_bytes_per_token / kv_div,
            state: state / state_div,
        })
    }
}

/// How one chunk's bytes are spread over tensor-parallel ranks.
struct Sharding {
    /// Objects per chunk (one per storing rank).
    ranks: u64,
    kv_div: u64,
    state_div: u64,
    /// KV bytes per token held by one rank.
    kv_per_token: u64,
    /// Recurrent-state bytes per sequence held by one rank.
    state: u64,
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
    if r.input_length < 0 {
        return Err(Error::InvalidFormat(format!(
            "mooncake line {lineno}: negative input_length {}",
            r.input_length
        )));
    }
    let n = r.hash_ids.len() as i64;
    let block = SOURCE_BLOCK_TOKENS as i64;
    let full_blocks = n.saturating_sub(1);
    let last = block
        .checked_mul(full_blocks)
        .and_then(|full| r.input_length.checked_sub(full))
        .ok_or_else(|| {
            Error::InvalidFormat(format!("mooncake line {lineno}: block count out of range"))
        })?
        .clamp(1, block) as u64;
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
    shard: &Sharding,
    mut emit: impl FnMut(u64, u64) -> Result<(), Error>,
) -> Result<(), Error> {
    let per_block = SOURCE_BLOCK_TOKENS / opts.chunk_tokens;
    let kvt = shard.kv_per_token;
    let state = shard.state;
    let ranks = shard.ranks;

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
        let snap = opts.sizes.state_bytes_per_sequence > 0
            && match opts.state_mode {
                StateMode::PerChunk => true,
                StateMode::PerRequest => i == n - 1,
            };
        let combined = snap && opts.state_layout == StateLayout::Combined;
        let kv = kvt.checked_mul(ctok).ok_or_else(overflow)?;
        let (kind, bytes) = if combined {
            (
                KIND_KV_WITH_STATE,
                kv.checked_add(state).ok_or_else(overflow)?,
            )
        } else {
            (KIND_KV, kv)
        };
        for rank in 0..ranks {
            emit(obj_id(block, c, kind, rank)?, bytes)?;
        }
        if snap && !combined {
            for rank in 0..ranks {
                emit(obj_id(block, c, KIND_STATE, rank)?, state)?;
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

/// Read every request of `file` from its start, call `f` on each, and return
/// the file's SHA-256 as lowercase hex.
fn for_each_request(
    file: &mut File,
    mut f: impl FnMut(&Request) -> Result<(), Error>,
) -> Result<String, Error> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(HashingReader {
        inner: file,
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
    let shard = opts.sharding()?;
    let input = input.as_ref();
    let output = output.as_ref();
    // One handle, rewound for the second pass, so a redirected /dev/stdin
    // that is a regular file reads the same bytes twice.
    // Checked by path first because opening a directory fails on Windows.
    let not_regular = |meta: &std::fs::Metadata| {
        Error::InvalidFormat(format!(
            "mooncake: {} is {}, not a regular file; the converter reads its input twice",
            input.display(),
            describe_file_type(meta)
        ))
    };
    if let Ok(meta) = std::fs::metadata(input) {
        if meta.is_dir() {
            return Err(not_regular(&meta));
        }
    }
    let mut file = File::open(input)?;
    let input_meta = file.metadata()?;
    if !input_meta.is_file() {
        return Err(not_regular(&input_meta));
    }
    if input.extension().is_some_and(|e| {
        let e = e.to_string_lossy().to_ascii_lowercase();
        e == "zst" || e == "zstd" || e == "gz"
    }) {
        return Err(Error::InvalidFormat(format!(
            "mooncake: {} looks compressed; decompress it first",
            input.display()
        )));
    }
    let out = plan_output(input, &input_meta, output)?;
    let created = utc_now_iso8601();

    // Pass 1: object ids and the first size of each.
    let mut ids: Vec<u64> = Vec::new();
    let mut first_size: HashMap<u64, u32> = HashMap::new();
    let mut size_conflicts = 0usize;
    let mut requests = 0usize;
    let sha256 = for_each_request(&mut file, |req| {
        requests += 1;
        expand_request(req, opts, &shard, |id, size| {
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

    // Reverse pass: overwrite each id with the row index of its next access,
    // reusing the column. -1 (no next access) is stored as u64::MAX.
    let mut next_access = ids;
    let mut seen: HashMap<u64, u64> = HashMap::with_capacity(first_size.len());
    for i in (0..next_access.len()).rev() {
        let id = next_access[i];
        next_access[i] = seen.insert(id, i as u64).unwrap_or(u64::MAX);
    }
    drop(seen);
    let rows = next_access.len();

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
        ("kvtrace.kv_tp_divisor", shard.kv_div.to_string()),
        (
            "kvtrace.state_tp_divisor",
            if state > 0 {
                shard.state_div.to_string()
            } else {
                "none".to_string()
            },
        ),
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

    // Pass 2: regenerate the rows and stream them out to the file chosen by
    // plan_output().
    let target = out.temp.as_deref().unwrap_or(&out.dest);
    let pass2 = || -> Result<(), Error> {
        let mut writer = TraceWriter::create(target, batch_size)?;
        for (k, v) in metadata {
            writer.append_key_value_metadata(k, v);
        }
        let mut row = 0usize;
        let sha256_again = for_each_request(&mut file, |req| {
            expand_request(req, opts, &shard, |id, _| {
                let (size, next) = match (first_size.get(&id), next_access.get(row)) {
                    (Some(&s), Some(&n)) => (s, n as i64),
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
        if out.temp.is_some() {
            writer.finish_synced()?;
        } else {
            writer.finish()?;
        }
        Ok(())
    };
    let result = pass2().and_then(|()| match &out.temp {
        Some(temp) => std::fs::rename(temp, &out.dest).map_err(Error::from),
        None => Ok(()),
    });
    if let Err(e) = result {
        if let Some(temp) = &out.temp {
            let _ = std::fs::remove_file(temp);
        }
        return Err(e);
    }

    Ok(MooncakeSummary {
        rows,
        requests,
        objects: first_size.len(),
        size_conflicts,
    })
}

/// "a directory", "a pipe", ... for an error message.
fn describe_file_type(meta: &std::fs::Metadata) -> &'static str {
    let ft = meta.file_type();
    if ft.is_dir() {
        return "a directory";
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        if ft.is_fifo() {
            return "a pipe";
        }
        if ft.is_socket() {
            return "a socket";
        }
        if ft.is_char_device() || ft.is_block_device() {
            return "a device";
        }
    }
    "not a regular file"
}

/// Where pass 2 writes.
struct OutputPlan {
    /// The file the result ends up in: the output path, or the file it links to.
    dest: std::path::PathBuf,
    /// Temporary file in `dest`'s directory, renamed over `dest` on success;
    /// `None` when `dest` is a device or pipe that is written directly.
    temp: Option<std::path::PathBuf>,
}

/// Distinguishes temporary files of concurrent conversions in one process.
static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Decide where the output is written, refusing outputs that must not be
/// replaced, before any work is done.
///
/// * A symlink is followed: the result is written to the file it links to,
///   as writing through the link would. A dangling link is an error.
/// * The input file, a directory, or an existing read-only file is an error.
/// * A device or pipe (e.g. `/dev/null`) is written directly.
/// * Otherwise the result goes to `.<name>.<pid>.<n>.tmp` beside the
///   destination and is renamed over it on success.
fn plan_output(
    input: &Path,
    input_meta: &std::fs::Metadata,
    output: &Path,
) -> Result<OutputPlan, Error> {
    let refuse =
        |what: &str| Error::InvalidFormat(format!("mooncake: output {} {what}", output.display()));
    let is_link = std::fs::symlink_metadata(output).is_ok_and(|m| m.file_type().is_symlink());
    let dest = if is_link {
        std::fs::canonicalize(output)
            .map_err(|e| refuse(&format!("is a symlink that cannot be resolved: {e}")))?
    } else {
        output.to_path_buf()
    };
    if same_file(input, input_meta, &dest) {
        return Err(refuse("is the input file"));
    }
    match std::fs::metadata(&dest) {
        Ok(m) if m.is_dir() => return Err(refuse("is a directory")),
        Ok(m) if !m.is_file() => return Ok(OutputPlan { dest, temp: None }),
        Ok(m) if m.permissions().readonly() => return Err(refuse("exists and is read-only")),
        _ => {}
    }
    let name = dest
        .file_name()
        .ok_or_else(|| refuse("has no file name"))?
        .to_string_lossy()
        .into_owned();
    let n = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp = dest.with_file_name(format!(".{name}.{}.{n}.tmp", std::process::id()));
    Ok(OutputPlan {
        dest,
        temp: Some(temp),
    })
}

/// Whether `output` names the input file: the same path after resolving
/// symlinks, or (on Unix) the same device and inode, which also catches hard
/// links.
fn same_file(input: &Path, input_meta: &std::fs::Metadata, output: &Path) -> bool {
    let Ok(out_meta) = std::fs::metadata(output) else {
        return false; // the output does not exist yet
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if input_meta.dev() == out_meta.dev() && input_meta.ino() == out_meta.ino() {
            return true;
        }
    }
    let _ = (input_meta, out_meta);
    matches!(
        (std::fs::canonicalize(input), std::fs::canonicalize(output)),
        (Ok(a), Ok(b)) if a == b
    )
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
            "linear_num_value_heads": 32, "linear_num_key_heads": 16, "linear_key_head_dim": 128,
            "linear_value_head_dim": 128, "mamba_ssm_dtype": "float32",
        });
        let s = ModelSizes::from_config(&c, None).unwrap();
        // 12 full layers x 2 x 256 x 2 x 2; 36 linear layers x 32 x 128 x 128 x 4
        // (mamba_ssm_dtype float32 overrides the bf16 model dtype).
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
    fn sizes_state_dtype_defaults_to_model_dtype() {
        // No mamba_ssm_dtype: the state uses the model dtype (bf16).
        // KV: 12 full layers x 2 KV heads x 256 x 2 x 2 bytes.
        // State: 36 linear layers x 32 value heads x 128 x 128 x 2 bytes.
        let s = ModelSizes::from_config_file(fixture("qwen3-next-80b-a3b.json"), None).unwrap();
        assert_eq!(s.kv_bytes_per_token, 24_576);
        assert_eq!(s.state_bytes_per_sequence, 37_748_736);
        // --kv-dtype changes KV only, not the state.
        let s =
            ModelSizes::from_config_file(fixture("qwen3-next-80b-a3b.json"), Some("fp8")).unwrap();
        assert_eq!(s.kv_bytes_per_token, 12_288);
        assert_eq!(s.state_bytes_per_sequence, 37_748_736);
    }

    fn config_err(c: Value) -> String {
        ModelSizes::from_config(&c, None).unwrap_err().to_string()
    }

    #[test]
    fn sizes_unsupported_layouts_are_errors() {
        let base = json!({
            "num_hidden_layers": 4, "num_key_value_heads": 8, "head_dim": 64,
            "torch_dtype": "bfloat16",
        });
        let with = |k: &str, v: Value| {
            let mut c = base.clone();
            c[k] = v;
            c
        };
        assert!(ModelSizes::from_config(&base, None).is_ok());

        let types = json!([
            "sliding_attention",
            "full_attention",
            "sliding_attention",
            "full_attention"
        ]);
        assert!(config_err(with("layer_types", types)).contains("\"sliding_attention\""));
        let short = json!(["full_attention", "full_attention"]);
        assert!(config_err(with("layer_types", short)).contains("2 entries"));
        assert!(config_err(with("sliding_window", json!(4096))).contains("sliding_window"));
        assert!(config_err(with("hybrid_override_pattern", json!("M-M*"))).contains("hybrid"));
        assert!(config_err(with("layers_block_type", json!(["mamba"]))).contains("hybrid"));

        // A sliding_window that is null or switched off is fine.
        assert!(ModelSizes::from_config(&with("sliding_window", Value::Null), None).is_ok());
        let mut off = with("sliding_window", json!(4096));
        off["use_sliding_window"] = json!(false);
        assert!(ModelSizes::from_config(&off, None).is_ok());
    }

    #[test]
    fn sizes_overflow_is_an_error() {
        let c = json!({
            "num_hidden_layers": 4, "num_key_value_heads": u64::MAX / 2, "head_dim": 64,
        });
        assert!(config_err(c).contains("overflow"));
        let c = json!({
            "num_hidden_layers": 8, "full_attention_interval": 2, "num_key_value_heads": 1,
            "head_dim": 1, "linear_num_key_heads": 1, "linear_num_value_heads": u64::MAX / 3,
            "linear_key_head_dim": 1, "linear_value_head_dim": 1,
        });
        assert!(config_err(c).contains("overflow"));
    }

    #[test]
    fn chunk_size_overflow_is_an_error_and_writes_nothing() {
        // 2^56 + 1 bytes per token: 256 tokens wraps to 256 bytes in u64.
        let opts = MooncakeOptions::new(ModelSizes::explicit(72_057_594_037_927_937, 0));
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        let err = convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &opts, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflow"), "{err}");
        assert!(!out.exists());

        // State added to a chunk's KV.
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(1, u64::MAX - 10));
        opts.tp = 1;
        let err = convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &opts, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflow"), "{err}");
        assert!(!out.exists());
    }

    #[test]
    fn input_must_be_a_regular_uncompressed_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        let err = convert_mooncake_to_parquet(dir.path(), &out, &opts, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a directory"), "{err}");
        for name in ["trace.jsonl.zst", "trace.jsonl.ZST", "trace.jsonl.Gz"] {
            let path = dir.path().join(name);
            std::fs::write(&path, b"").unwrap();
            let err = convert_mooncake_to_parquet(&path, &out, &opts, 4)
                .unwrap_err()
                .to_string();
            assert!(err.contains("decompress"), "{name}: {err}");
        }
        assert!(!out.exists());
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
        let shard = opts.sharding().unwrap();
        expand_request(
            &parse_request(line, 1).unwrap(),
            opts,
            &shard,
            |id, size| {
                out.push((id, size));
                Ok(())
            },
        )
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
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(12, 0));
        opts.tp = 4;
        let line = r#"{"timestamp": 0, "input_length": 100, "hash_ids": [7]}"#;
        assert_eq!(
            expand(line, &opts),
            (0..4).map(|r| (id(7, 0, 0, r), 300)).collect::<Vec<_>>()
        );
        opts.sizes.mla = true;
        assert_eq!(expand(line, &opts), [(id(7, 0, 0, 0), 1200)]);
    }

    fn qwen3_8_27b(tp: u64) -> MooncakeOptions {
        let sizes = ModelSizes::from_config_file(fixture("qwen3.8-27b.json"), None).unwrap();
        let mut opts = MooncakeOptions::new(sizes);
        opts.tp = tp;
        opts
    }

    #[test]
    fn tp_above_kv_heads_replicates_kv() {
        // 4 KV heads, 48 linear value heads. At tp 8 each rank holds 1/4 of the
        // KV (heads replicated) and 1/8 of the state.
        let opts = qwen3_8_27b(8);
        let shard = opts.sharding().unwrap();
        assert_eq!((shard.ranks, shard.kv_div, shard.state_div), (8, 4, 8));
        assert_eq!(shard.kv_per_token, 16_384);
        assert_eq!(shard.state, 18_874_368);
        let line = r#"{"timestamp": 0, "input_length": 1024, "hash_ids": [1, 2]}"#;
        let rows = expand(line, &opts);
        assert_eq!(rows.len(), 4 * 8);
        // KV-only chunk: 256 x 16,384; last chunk adds the state share.
        assert!(rows[..24].iter().all(|&(_, size)| size == 4_194_304));
        assert!(rows[24..]
            .iter()
            .all(|&(_, size)| size == 4_194_304 + 18_874_368));

        // tp 2 is below the head count: plain 1/tp.
        let shard = qwen3_8_27b(2).sharding().unwrap();
        assert_eq!((shard.kv_div, shard.state_div), (2, 2));

        // Qwen3-8B (8 KV heads) at tp 16.
        let sizes = ModelSizes::from_config_file(fixture("qwen3-8b.json"), None).unwrap();
        let mut opts = MooncakeOptions::new(sizes);
        opts.tp = 16;
        let shard = opts.sharding().unwrap();
        assert_eq!((shard.kv_div, shard.kv_per_token), (8, 147_456 / 8));
    }

    #[test]
    fn tp_that_does_not_split_evenly_is_an_error() {
        // 4 KV heads: 3 does not divide them, 6 is not a multiple of them.
        for tp in [3, 6] {
            let err = qwen3_8_27b(tp).sharding().err().unwrap().to_string();
            assert!(err.contains("KV heads"), "tp {tp}: {err}");
        }
        // 48 value heads: 32 is a multiple of the 4 KV heads but does not divide 48.
        let err = qwen3_8_27b(32).sharding().err().unwrap().to_string();
        assert!(err.contains("value heads"), "{err}");

        // Explicit sizes: per-rank bytes must be whole.
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        opts.tp = 4;
        assert!(opts
            .sharding()
            .err()
            .unwrap()
            .to_string()
            .contains("KV bytes"));
        let mut opts = MooncakeOptions::new(ModelSizes::explicit(10, 1001));
        opts.tp = 2;
        assert!(opts
            .sharding()
            .err()
            .unwrap()
            .to_string()
            .contains("state bytes"));
    }

    #[test]
    fn tp_must_divide_attention_heads_and_conv_width() {
        // Qwen3.8-27B: 24 attention heads. tp 16 and 48 fit the 4 KV heads and
        // the 48 value heads but not the attention heads.
        for tp in [16, 48] {
            let err = qwen3_8_27b(tp).sharding().err().unwrap().to_string();
            assert!(err.contains("24 attention heads"), "tp {tp}: {err}");
        }
        // Qwen3-8B: 32 attention heads; tp 64 fits the 8 KV heads by replication.
        let sizes = ModelSizes::from_config_file(fixture("qwen3-8b.json"), None).unwrap();
        let mut opts = MooncakeOptions::new(sizes);
        opts.tp = 64;
        let err = opts.sharding().err().unwrap().to_string();
        assert!(err.contains("32 attention heads"), "{err}");

        // Convolution width 2 x 1 x 1 + 64 x 1 = 66, which tp 32 does not divide
        // although it divides every head count.
        let c = json!({
            "num_hidden_layers": 4, "full_attention_interval": 2,
            "num_attention_heads": 32, "num_key_value_heads": 32, "head_dim": 1,
            "linear_num_key_heads": 1, "linear_key_head_dim": 1,
            "linear_num_value_heads": 64, "linear_value_head_dim": 1,
        });
        let mut opts = MooncakeOptions::new(ModelSizes::from_config(&c, None).unwrap());
        opts.tp = 32;
        let err = opts.sharding().err().unwrap().to_string();
        assert!(err.contains("convolution width 66"), "{err}");
        opts.tp = 2;
        assert!(opts.sharding().is_ok());
    }

    #[test]
    fn input_length_is_validated() {
        let err = parse_request(
            r#"{"timestamp": 0, "input_length": -9223372036854775808, "hash_ids": [1, 2]}"#,
            3,
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            err.contains("line 3") && err.contains("negative input_length"),
            "{err}"
        );
        // No ids: no blocks, whatever the length.
        let r = parse_request(r#"{"timestamp": 0, "input_length": 0, "hash_ids": []}"#, 1);
        assert!(r.unwrap().blocks.is_empty());
    }

    #[test]
    fn output_never_overwrites_the_input() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("trace.jsonl");
        std::fs::copy(fixture("trace.jsonl"), &input).unwrap();
        let before = std::fs::read(&input).unwrap();
        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));

        let mut outputs = vec![input.clone(), dir.path().join(".").join("trace.jsonl")];
        #[cfg(unix)]
        {
            let link = dir.path().join("link.parquet");
            std::os::unix::fs::symlink(&input, &link).unwrap();
            let hard = dir.path().join("hard.parquet");
            std::fs::hard_link(&input, &hard).unwrap();
            outputs.extend([link, hard]);
        }
        for out in outputs {
            let err = convert_mooncake_to_parquet(&input, &out, &opts, 4)
                .err()
                .unwrap()
                .to_string();
            assert!(
                err.contains("is the input file"),
                "{}: {err}",
                out.display()
            );
            assert_eq!(std::fs::read(&input).unwrap(), before);
        }
    }

    #[test]
    fn output_is_replaced_only_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        std::fs::write(&out, b"keep").unwrap();

        // A failing run leaves the existing output and no temporary file.
        let bad = MooncakeOptions::new(ModelSizes::explicit(u64::MAX, 0));
        assert!(convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &bad, 4).is_err());
        assert_eq!(std::fs::read(&out).unwrap(), b"keep");

        // A read-only output is refused up front and left as it was.
        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        let mut perms = std::fs::metadata(&out).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&out, perms.clone()).unwrap();
        let err = convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &opts, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("read-only"), "{err}");
        assert_eq!(std::fs::read(&out).unwrap(), b"keep");
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&out, perms).unwrap();

        // A successful run replaces a writable output.
        convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &opts, 4).unwrap();
        assert_eq!(TraceReader::open(&out).unwrap().total_entries(), 13);
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["out.parquet"]);
    }

    #[test]
    fn output_directory_is_refused_before_any_work() {
        let dir = tempfile::tempdir().unwrap();
        // Sizes that would fail in the first pass: the output check comes first.
        let bad = MooncakeOptions::new(ModelSizes::explicit(u64::MAX, 0));
        let err = convert_mooncake_to_parquet(fixture("trace.jsonl"), dir.path(), &bad, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("is a directory"), "{err}");
        assert!(err.contains(&*dir.path().to_string_lossy()), "{err}");
    }

    #[test]
    fn temp_names_are_unique_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let input = fixture("trace.jsonl");
        let meta = std::fs::metadata(&input).unwrap();
        let out = dir.path().join("out.parquet");
        let a = plan_output(&input, &meta, &out).unwrap();
        let b = plan_output(&input, &meta, &out).unwrap();
        assert_eq!(a.dest, out);
        assert_ne!(a.temp.unwrap(), b.temp.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_output_writes_through_the_link() {
        let dir = tempfile::tempdir().unwrap();
        let real_dir = dir.path().join("real");
        std::fs::create_dir(&real_dir).unwrap();
        let target = real_dir.join("target.parquet");
        std::fs::write(&target, b"old").unwrap();
        let link = dir.path().join("link.parquet");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        convert_mooncake_to_parquet(fixture("trace.jsonl"), &link, &opts, 4).unwrap();
        assert!(std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(TraceReader::open(&target).unwrap().total_entries(), 13);
        let names: Vec<_> = std::fs::read_dir(&real_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["target.parquet"]);

        // A dangling link is refused.
        let dangling = dir.path().join("dangling.parquet");
        std::os::unix::fs::symlink(dir.path().join("missing/x.parquet"), &dangling).unwrap();
        let err = convert_mooncake_to_parquet(fixture("trace.jsonl"), &dangling, &opts, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("symlink"), "{err}");
    }

    /// The writer cannot create its temporary file: the existing output must
    /// survive.
    #[cfg(unix)]
    #[test]
    fn unwritable_output_directory_keeps_existing_output() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("out");
        std::fs::create_dir(&sub).unwrap();
        let out = sub.join("out.parquet");
        std::fs::write(&out, b"keep").unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores the permissions; nothing to test then.
        let probe = sub.join("probe");
        if File::create(&probe).is_ok() {
            std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let opts = MooncakeOptions::new(ModelSizes::explicit(10, 0));
        let result = convert_mooncake_to_parquet(fixture("trace.jsonl"), &out, &opts, 4);
        let kept = std::fs::read(&out).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err());
        assert_eq!(kept, b"keep");
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
        assert_eq!(meta["kvtrace.kv_tp_divisor"], "1");
        assert_eq!(meta["kvtrace.state_tp_divisor"], "none");
        assert_eq!(meta.len(), 22);
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
