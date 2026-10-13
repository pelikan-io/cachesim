//! Model geometry: KV bytes per token and recurrent-state bytes per sequence
//! of a model, and how tensor parallelism splits them into per-rank objects.

use std::path::Path;

use serde_json::Value;

use crate::Error;

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
    /// tensor parallelism splits the KV (see [`Geometry`]).
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

pub(crate) fn overflow() -> Error {
    Error::InvalidFormat("kvtrace: a size computation overflows u64".into())
}

fn bad_config(msg: &str) -> Error {
    Error::InvalidFormat(format!("model config: {msg}"))
}

// ---------------------------------------------------------------------------
// Hybrid-model state
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
    /// Name used on the command line and in trace metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerRequest => "per-request",
            Self::PerChunk => "per-chunk",
        }
    }
}

/// Where a hybrid model's recurrent-state snapshot is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateLayout {
    /// In the chunk's object ([`ObjectKind::KvWithState`](crate::ObjectKind::KvWithState)).
    Combined,
    /// As its own object ([`ObjectKind::State`](crate::ObjectKind::State)).
    Separate,
}

impl StateLayout {
    /// Name used on the command line and in trace metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Separate => "separate",
        }
    }
}

/// How a model's KV cache is laid out as stored objects: its sizes, the
/// tensor-parallel degree, and where a hybrid model's recurrent state goes.
/// The chunk length is not part of it; it is a property of the request trace.
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
pub struct Geometry {
    pub sizes: ModelSizes,
    /// Model id recorded in the metadata, e.g. `Qwen/Qwen3-8B`.
    pub model_name: Option<String>,
    /// Hugging Face revision the config was fetched at, for the metadata.
    pub model_revision: Option<String>,
    /// SHA-256 (hex) of the config.json the sizes came from, for the metadata.
    pub model_config_sha256: Option<String>,
    /// Tensor-parallel degree, 1 to [`MAX_TP`].
    pub tp: u64,
    pub state_mode: StateMode,
    pub state_layout: StateLayout,
}

/// Largest tensor-parallel degree: the rank must fit the 12 bits it has in an
/// object id ([`obj_id`](crate::obj_id)).
pub const MAX_TP: u64 = 1 << crate::objects::RANK_BITS;

impl Geometry {
    /// Geometry with the defaults: TP 1, per-request combined state.
    pub fn new(sizes: ModelSizes) -> Self {
        Self {
            sizes,
            model_name: None,
            model_revision: None,
            model_config_sha256: None,
            tp: 1,
            state_mode: StateMode::PerRequest,
            state_layout: StateLayout::Combined,
        }
    }

    /// Objects per chunk and the per-rank sizes; see [the type
    /// docs](Self#tensor-parallelism).
    pub fn sharding(&self) -> Result<Sharding, Error> {
        if self.tp == 0 || self.tp > MAX_TP {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: tp ({}) must be between 1 and {MAX_TP}",
                self.tp
            )));
        }
        let tp = self.tp;
        let s = &self.sizes;
        let tp_err = |msg: String| Error::InvalidFormat(format!("kvtrace: tp {tp}: {msg}"));

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sharding {
    /// Objects per chunk (one per storing rank).
    pub ranks: u64,
    /// Ways the KV is split: `min(tp, kv_heads)`, or `tp` without a head
    /// count; 1 for MLA.
    pub kv_div: u64,
    /// Ways the recurrent state is split: `tp`; 1 for MLA.
    pub state_div: u64,
    /// KV bytes per token held by one rank.
    pub kv_per_token: u64,
    /// Recurrent-state bytes per sequence held by one rank.
    pub state: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectMapper;
    use serde_json::json;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/models")
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

    fn qwen3_8_27b(tp: u64) -> Geometry {
        let sizes = ModelSizes::from_config_file(fixture("qwen3.8-27b.json"), None).unwrap();
        let mut opts = Geometry::new(sizes);
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
        let mapper = ObjectMapper::new(&opts, 256).unwrap();
        let rows: Vec<(u64, u64)> = mapper
            .objects(&[1 << 8, 1 << 8 | 1, 2 << 8, 2 << 8 | 1])
            .unwrap()
            .map(|o| (o.obj_id, o.size))
            .collect();
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
        let mut opts = Geometry::new(sizes);
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
        let mut opts = Geometry::new(ModelSizes::explicit(10, 0));
        opts.tp = 4;
        assert!(opts
            .sharding()
            .err()
            .unwrap()
            .to_string()
            .contains("KV bytes"));
        let mut opts = Geometry::new(ModelSizes::explicit(10, 1001));
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
        let mut opts = Geometry::new(sizes);
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
        let mut opts = Geometry::new(ModelSizes::from_config(&c, None).unwrap());
        opts.tp = 32;
        let err = opts.sharding().err().unwrap().to_string();
        assert!(err.contains("convolution width 66"), "{err}");
        opts.tp = 2;
        assert!(opts.sharding().is_ok());
    }

    #[test]
    fn tp_range_is_validated() {
        let mut g = Geometry::new(ModelSizes::explicit(4096, 0));
        g.tp = 0;
        assert!(g.sharding().is_err());
        g.tp = MAX_TP;
        assert!(g.sharding().is_ok());
        g.tp = MAX_TP * 2;
        assert!(g.sharding().is_err());
    }
}
