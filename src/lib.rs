pub mod oracle;
pub mod simulator;
pub mod trace;

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

    #[error("Cache error: {0}")]
    Cache(#[from] segcache::SegcacheError),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}
