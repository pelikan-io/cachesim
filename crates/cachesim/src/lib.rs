pub mod kv;
pub mod oracle;
pub mod simulator;

/// The object-level trace format, now the `cachetrace` crate. Re-exported
/// here so `cachesim::trace::*` paths from earlier releases keep compiling.
pub use cachetrace as trace;

/// The request-level KV-cache trace format and model geometry.
pub use kvtrace;

// ---------------------------------------------------------------------------
// Cuckoo-cache eviction policies
// ---------------------------------------------------------------------------

/// Eviction policies backed by the cuckoo-cache engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CuckooPolicy {
    /// Randomly select a candidate slot for eviction.
    Random,
    /// Prefer evicting items nearest to expiration.
    Expire,
}

impl From<CuckooPolicy> for cuckoo_cache::Policy {
    fn from(p: CuckooPolicy) -> Self {
        match p {
            CuckooPolicy::Random => Self::Random,
            CuckooPolicy::Expire => Self::Expire,
        }
    }
}

// ---------------------------------------------------------------------------
// Segcache eviction policies
// ---------------------------------------------------------------------------

/// Eviction policies backed by the segcache engine.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SegcachePolicy {
    /// No eviction; inserts fail when full.
    None,
    /// Random segment eviction.
    Random,
    /// Random TTL bucket, FIFO within.
    RandomFifo,
    /// First-in first-out.
    Fifo,
    /// Closest-to-expiration.
    Cte,
    /// Least-utilized segment.
    Util,
    /// S3-FIFO: small, main, and ghost FIFO queues.
    S3Fifo {
        /// Ratio of total segments allocated to the admission pool (0.0–1.0).
        /// Default: 0.10.
        admission_ratio: f64,
    },
}

impl From<SegcachePolicy> for segcache::Policy {
    fn from(p: SegcachePolicy) -> Self {
        match p {
            SegcachePolicy::None => Self::None,
            SegcachePolicy::Random => Self::Random,
            SegcachePolicy::RandomFifo => Self::RandomFifo,
            SegcachePolicy::Fifo => Self::Fifo,
            SegcachePolicy::Cte => Self::Cte,
            SegcachePolicy::Util => Self::Util,
            SegcachePolicy::S3Fifo { admission_ratio } => Self::S3Fifo { admission_ratio },
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Unified error type for cachesim operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Segcache error: {0}")]
    Segcache(#[from] segcache::SegcacheError),

    #[error("Cuckoo-cache error: {0}")]
    Cuckoo(#[from] cuckoo_cache::CuckooCacheError),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Fetch error: {0}")]
    Fetch(String),
}

/// Each `cachetrace` error maps to the variant of the same name, carrying the
/// same source error or message. `cachetrace::Error` is non-exhaustive: a
/// variant added there later becomes `InvalidFormat` with its message until
/// it gets an arm here.
impl From<cachetrace::Error> for Error {
    fn from(e: cachetrace::Error) -> Self {
        match e {
            cachetrace::Error::Io(e) => Self::Io(e),
            cachetrace::Error::Parquet(e) => Self::Parquet(e),
            cachetrace::Error::Arrow(e) => Self::Arrow(e),
            cachetrace::Error::InvalidFormat(m) => Self::InvalidFormat(m),
            other => Self::InvalidFormat(other.to_string()),
        }
    }
}

/// Each `kvtrace` error maps to the variant of the same name, carrying the
/// same source error or message. `kvtrace::Error` is non-exhaustive: a variant
/// added there later becomes `InvalidFormat` with its message until it gets
/// an arm here.
impl From<kvtrace::Error> for Error {
    fn from(e: kvtrace::Error) -> Self {
        match e {
            kvtrace::Error::Io(e) => Self::Io(e),
            kvtrace::Error::Parquet(e) => Self::Parquet(e),
            kvtrace::Error::Arrow(e) => Self::Arrow(e),
            kvtrace::Error::InvalidFormat(m) => Self::InvalidFormat(m),
            kvtrace::Error::Fetch(m) => Self::Fetch(m),
            other => Self::InvalidFormat(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant of the trace and kvtrace errors lands in the cachesim
    /// variant of the same name with its message intact.
    #[test]
    fn errors_map_by_variant() {
        let io = || std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let pq = || parquet::errors::ParquetError::General("pq".into());
        let ar = || arrow::error::ArrowError::ComputeError("ar".into());
        let from_trace = [
            Error::from(cachetrace::Error::Io(io())),
            Error::from(cachetrace::Error::Parquet(pq())),
            Error::from(cachetrace::Error::Arrow(ar())),
            Error::from(cachetrace::Error::InvalidFormat("bad".into())),
        ];
        let from_kv = [
            Error::from(kvtrace::Error::Io(io())),
            Error::from(kvtrace::Error::Parquet(pq())),
            Error::from(kvtrace::Error::Arrow(ar())),
            Error::from(kvtrace::Error::InvalidFormat("bad".into())),
            Error::from(kvtrace::Error::Fetch("net".into())),
        ];
        for errs in [&from_trace[..], &from_kv[..]] {
            assert!(
                matches!(&errs[0], Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound && e.to_string() == "gone")
            );
            assert!(matches!(&errs[1], Error::Parquet(e) if e.to_string().contains("pq")));
            assert!(matches!(&errs[2], Error::Arrow(e) if e.to_string().contains("ar")));
            assert!(matches!(&errs[3], Error::InvalidFormat(m) if m == "bad"));
        }
        assert!(matches!(&from_kv[4], Error::Fetch(m) if m == "net"));
    }
}
