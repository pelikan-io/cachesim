pub mod oracle;
pub mod simulator;
pub mod trace;

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
