//! KV-cache traces for LLM serving: a request-level trace format, model
//! geometry, and the mapping from a request to the KV-cache objects it writes.
//!
//! * [`format`](mod@format): one Parquet row per inference request, with the ids of the
//!   request's full prefix chunks ([`Request`], [`RequestWriter`],
//!   [`RequestReader`]). The format is model-independent.
//! * [`geometry`]: KV bytes per token and recurrent-state bytes per sequence
//!   of a model, from a Hugging Face `config.json` or given directly
//!   ([`ModelSizes`]), and their split over tensor-parallel ranks
//!   ([`Geometry`]).
//! * [`objects`]: for a request's chunks and a geometry, each object the
//!   request writes (id, size, rank, kind, chunk index) in prefix order, and
//!   the ids a lookup probes at a chunk ([`ObjectMapper`]).
//! * [`mooncake`]: conversion of the public Mooncake trace into the request
//!   format.
//! * `hf` (feature `hf-fetch`, on by default): fetch a model's `config.json`
//!   from Hugging Face. Without the feature the crate has no HTTP client.
//!
//! Dense and MLA geometry is checked against vLLM and LMCache. Hybrid
//! (linear-attention) geometry is an approximation pending a follow-up; the
//! known differences are listed in the [`objects`] module docs.

pub mod format;
pub mod geometry;
#[cfg(feature = "hf-fetch")]
pub mod hf;
pub mod mooncake;
pub mod objects;
pub mod output;

pub use format::{Request, RequestReader, RequestWriter};
pub use geometry::{
    dtype_bytes, Geometry, ModelSizes, Sharding, StateLayout, StateMode, DTYPE_NAMES, MAX_TP,
};
pub use objects::{
    obj_id, obj_id_fields, KvObject, LookupIds, ObjectKind, ObjectMapper, Objects, SLOT_CHUNK,
    SLOT_STATE,
};

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

/// Error type for kvtrace operations.
///
/// Non-exhaustive so that a variant can be added without breaking callers
/// that match on it.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Fetch error: {0}")]
    Fetch(String),
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 of `bytes` as lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// SHA-256 of a file's contents as lowercase hex, read in a stream.
pub fn sha256_file(path: impl AsRef<std::path::Path>) -> Result<String, Error> {
    let mut hasher = Sha256::new();
    std::io::copy(&mut std::fs::File::open(path)?, &mut hasher)?;
    Ok(hex(&hasher.finalize()))
}

/// Current UTC time, the format of `kvtrace.created`: `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`.
pub fn utc_now_iso8601() -> String {
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
