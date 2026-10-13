//! Object-level cache trace format.
//!
//! One row is one cache request. The on-disk format is Parquet, with a schema
//! compatible with libCacheSim's `oracleGeneral` binary trace format:
//!
//! | Field              | Type   | Bytes | Description                        |
//! |--------------------|--------|-------|------------------------------------|
//! | timestamp          | i64    | 8     | Timestamp(ns, UTC) since epoch     |
//! | obj_id             | u64    | 8     | Object identifier                  |
//! | obj_size           | u32    | 4     | Object size in bytes               |
//! | next_access_vtime  | i64    | 8     | Virtual time of next access        |
//!
//! Two optional extension columns (`op`, `ttl`) support the richer
//! `oracleGeneralOpNs` format and workloads with mixed operations.
//!
//! The core of the crate is the schema ([`trace_schema`]), the row type
//! ([`TraceEntry`], [`Op`]), [`TraceReader`] and [`TraceWriter`]. The `import`
//! cargo feature (on by default) adds conversion from libCacheSim binary
//! traces and [pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace)
//! CSV files (optionally zstd-compressed); build with
//! `default-features = false` to leave the importers and their dependencies
//! out.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, TimestampNanosecondArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

#[cfg(feature = "import")]
mod import;
#[cfg(feature = "import")]
pub use import::{convert_bin_to_parquet, convert_cache_trace_to_parquet, BinFormat};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error type for trace I/O and conversion.
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
}

// ---------------------------------------------------------------------------
// Operation types (compatible with libCacheSim req_op_e)
// ---------------------------------------------------------------------------

/// Cache operation type, matching libCacheSim's `req_op_e` enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Nop = 0,
    Get = 1,
    Gets = 2,
    Set = 3,
    Add = 4,
    Cas = 5,
    Replace = 6,
    Append = 7,
    Prepend = 8,
    Delete = 9,
    Incr = 10,
    Decr = 11,
    Read = 12,
    Write = 13,
    Update = 14,
    Invalid = 15,
}

impl Op {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Nop,
            1 => Self::Get,
            2 => Self::Gets,
            3 => Self::Set,
            4 => Self::Add,
            5 => Self::Cas,
            6 => Self::Replace,
            7 => Self::Append,
            8 => Self::Prepend,
            9 => Self::Delete,
            10 => Self::Incr,
            11 => Self::Decr,
            12 => Self::Read,
            13 => Self::Write,
            14 => Self::Update,
            _ => Self::Invalid,
        }
    }

    /// Returns `true` for operations that are logically reads / lookups.
    pub fn is_read(self) -> bool {
        matches!(self, Self::Get | Self::Gets | Self::Read | Self::Nop)
    }

    /// Returns `true` for operations that are logically writes / stores.
    pub fn is_write(self) -> bool {
        matches!(
            self,
            Self::Set
                | Self::Add
                | Self::Cas
                | Self::Replace
                | Self::Append
                | Self::Prepend
                | Self::Write
                | Self::Update
                | Self::Incr
                | Self::Decr
        )
    }

    /// Returns `true` for delete operations.
    pub fn is_delete(self) -> bool {
        matches!(self, Self::Delete)
    }
}

// ---------------------------------------------------------------------------
// Trace entry
// ---------------------------------------------------------------------------

/// A single trace request, field-compatible with libCacheSim's oracleGeneral.
#[derive(Debug, Clone)]
pub struct TraceEntry {
    /// Real time as nanoseconds since Unix epoch (stored as Arrow
    /// `Timestamp(Nanosecond, Some("UTC"))` in Parquet).
    pub timestamp: i64,
    /// Object identifier.
    pub obj_id: u64,
    /// Object size in bytes.
    pub obj_size: u32,
    /// Virtual time of the next access to this object.
    /// `-1` (or `i64::MAX`) means there is no future access.
    pub next_access_vtime: i64,
    /// Operation type (extended field, absent in basic oracleGeneral).
    pub op: Option<u8>,
    /// Time-to-live in seconds (extended field).
    pub ttl: Option<i32>,
}

// ---------------------------------------------------------------------------
// Parquet schema
// ---------------------------------------------------------------------------

/// Canonical Arrow schema for cachesim trace files.
pub fn trace_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new("obj_id", DataType::UInt64, false),
        Field::new("obj_size", DataType::UInt32, false),
        Field::new("next_access_vtime", DataType::Int64, false),
        Field::new("op", DataType::UInt8, true),
        Field::new("ttl", DataType::Int32, true),
    ])
}

// ---------------------------------------------------------------------------
// Parquet reader
// ---------------------------------------------------------------------------

/// Rows decoded per record batch. Large enough that per-batch overhead is
/// negligible against a multi-billion-row trace, small enough (a few MiB for
/// this schema) that the working set stays in cache.
const READ_BATCH_SIZE: usize = 65_536;

/// Reads trace entries from a Parquet file, one record batch at a time.
///
/// The file is never loaded whole: only the current batch is resident, so a
/// trace larger than memory streams through at a fixed footprint. The total
/// row count comes from the Parquet footer and is available before the first
/// row is decoded.
pub struct TraceReader {
    batches: ParquetRecordBatchReader,
    total_entries: usize,
    current: Option<Columns>,
    row_idx: usize,
    /// Set once a batch fails to decode; the iterator is exhausted afterwards
    /// rather than skipping to the next row group and silently dropping rows.
    failed: bool,
}

/// The current batch, downcast once so each row is a plain array index rather
/// than a name lookup plus a dynamic downcast.
struct Columns {
    len: usize,
    timestamp: Option<TimestampNanosecondArray>,
    obj_id: Option<UInt64Array>,
    obj_size: Option<UInt32Array>,
    next_access_vtime: Option<Int64Array>,
    op: Option<UInt8Array>,
    ttl: Option<Int32Array>,
}

impl Columns {
    fn from_batch(batch: &RecordBatch) -> Self {
        fn col<T: Array + Clone + 'static>(batch: &RecordBatch, name: &str) -> Option<T> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<T>())
                .cloned()
        }
        Self {
            len: batch.num_rows(),
            timestamp: col(batch, "timestamp"),
            obj_id: col(batch, "obj_id"),
            obj_size: col(batch, "obj_size"),
            next_access_vtime: col(batch, "next_access_vtime"),
            op: col(batch, "op"),
            ttl: col(batch, "ttl"),
        }
    }

    fn entry(&self, i: usize) -> TraceEntry {
        fn nullable<A, T>(col: &Option<A>, i: usize, get: impl Fn(&A, usize) -> T) -> Option<T>
        where
            A: Array,
        {
            col.as_ref().filter(|a| !a.is_null(i)).map(|a| get(a, i))
        }
        TraceEntry {
            timestamp: self.timestamp.as_ref().map(|a| a.value(i)).unwrap_or(0),
            obj_id: self.obj_id.as_ref().map(|a| a.value(i)).unwrap_or(0),
            obj_size: self.obj_size.as_ref().map(|a| a.value(i)).unwrap_or(0),
            next_access_vtime: self
                .next_access_vtime
                .as_ref()
                .map(|a| a.value(i))
                .unwrap_or(-1),
            op: nullable(&self.op, i, |a, i| a.value(i)),
            ttl: nullable(&self.ttl, i, |a, i| a.value(i)),
        }
    }
}

impl TraceReader {
    /// Open a Parquet trace file.
    ///
    /// Reads the footer only; row groups are decoded on demand as the
    /// iterator advances.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let total_entries = usize::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| Error::InvalidFormat("negative row count in footer".into()))?;
        let batches = builder.with_batch_size(READ_BATCH_SIZE).build()?;
        Ok(Self {
            batches,
            total_entries,
            current: None,
            row_idx: 0,
            failed: false,
        })
    }

    /// Total number of entries in the file, from the Parquet footer.
    pub fn total_entries(&self) -> usize {
        self.total_entries
    }

    /// Advance to the next non-empty batch. `Ok(false)` at end of file.
    fn next_batch(&mut self) -> Result<bool, Error> {
        loop {
            match self.batches.next() {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.current = Some(Columns::from_batch(&batch));
                    self.row_idx = 0;
                    return Ok(true);
                }
                Some(Err(e)) => return Err(e.into()),
                None => {
                    self.current = None;
                    return Ok(false);
                }
            }
        }
    }
}

impl Iterator for TraceReader {
    type Item = Result<TraceEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let exhausted = match &self.current {
            Some(cols) => self.row_idx >= cols.len,
            None => true,
        };
        if exhausted {
            match self.next_batch() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            }
        }
        let cols = self.current.as_ref()?;
        let i = self.row_idx;
        self.row_idx += 1;
        Some(Ok(cols.entry(i)))
    }
}
// ---------------------------------------------------------------------------
// Parquet writer
// ---------------------------------------------------------------------------

/// Writes trace entries to a Parquet file.
pub struct TraceWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    // Column buffers
    timestamps: Vec<i64>,
    obj_ids: Vec<u64>,
    obj_sizes: Vec<u32>,
    next_access_vtimes: Vec<i64>,
    ops: Vec<Option<u8>>,
    ttls: Vec<Option<i32>>,
    batch_size: usize,
}

impl TraceWriter {
    /// Create a new Parquet trace writer.
    pub fn create(path: impl AsRef<Path>, batch_size: usize) -> Result<Self, Error> {
        let schema = Arc::new(trace_schema());
        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(Self {
            writer,
            schema,
            timestamps: Vec::with_capacity(batch_size),
            obj_ids: Vec::with_capacity(batch_size),
            obj_sizes: Vec::with_capacity(batch_size),
            next_access_vtimes: Vec::with_capacity(batch_size),
            ops: Vec::with_capacity(batch_size),
            ttls: Vec::with_capacity(batch_size),
            batch_size,
        })
    }

    /// Add a key-value pair to the Parquet file footer. Pairs appended any
    /// time before [`finish`](Self::finish) are written.
    pub fn append_key_value_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.writer
            .append_key_value_metadata(KeyValue::new(key.into(), value.into()));
    }

    /// Append a single entry (buffered; flushed in batches).
    pub fn write(&mut self, entry: &TraceEntry) -> Result<(), Error> {
        self.timestamps.push(entry.timestamp);
        self.obj_ids.push(entry.obj_id);
        self.obj_sizes.push(entry.obj_size);
        self.next_access_vtimes.push(entry.next_access_vtime);
        self.ops.push(entry.op);
        self.ttls.push(entry.ttl);

        if self.timestamps.len() >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<(), Error> {
        if self.timestamps.is_empty() {
            return Ok(());
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from(std::mem::take(&mut self.timestamps))
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt64Array::from(std::mem::take(&mut self.obj_ids))),
            Arc::new(UInt32Array::from(std::mem::take(&mut self.obj_sizes))),
            Arc::new(Int64Array::from(std::mem::take(
                &mut self.next_access_vtimes,
            ))),
            Arc::new(UInt8Array::from(std::mem::take(&mut self.ops))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.ttls))),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&batch)?;
        Ok(())
    }

    /// Flush remaining buffered entries and close the file.
    pub fn finish(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.writer.close()?;
        Ok(())
    }

    /// Like [`finish`](Self::finish), then flush the file to stable storage
    /// (`File::sync_all`) before returning.
    pub fn finish_synced(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.writer.into_inner()?.sync_all()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let entries = vec![
            TraceEntry {
                timestamp: 1,
                obj_id: 100,
                obj_size: 64,
                next_access_vtime: 3,
                op: Some(1),
                ttl: Some(300),
            },
            TraceEntry {
                timestamp: 2,
                obj_id: 200,
                obj_size: 128,
                next_access_vtime: -1,
                op: None,
                ttl: None,
            },
            TraceEntry {
                timestamp: 3,
                obj_id: 100,
                obj_size: 64,
                next_access_vtime: -1,
                op: Some(9), // DELETE
                ttl: None,
            },
        ];

        // Write
        let mut writer = TraceWriter::create(&path, 1024).unwrap();
        for e in &entries {
            writer.write(e).unwrap();
        }
        writer.finish().unwrap();

        // Read back and verify values
        let reader = TraceReader::open(&path).unwrap();
        assert_eq!(reader.total_entries(), 3);
        let read_entries: Vec<TraceEntry> = reader.collect::<Result<_, _>>().unwrap();

        for (orig, read) in entries.iter().zip(read_entries.iter()) {
            assert_eq!(orig.timestamp, read.timestamp);
            assert_eq!(orig.obj_id, read.obj_id);
            assert_eq!(orig.obj_size, read.obj_size);
            assert_eq!(orig.next_access_vtime, read.next_access_vtime);
            assert_eq!(orig.op, read.op);
            assert_eq!(orig.ttl, read.ttl);
        }
    }

    #[test]
    fn parquet_schema_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema_check.parquet");

        let mut writer = TraceWriter::create(&path, 1024).unwrap();
        writer
            .write(&TraceEntry {
                timestamp: 1_700_000_000_000_000_000,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: -1,
                op: None,
                ttl: None,
            })
            .unwrap();
        writer.finish().unwrap();

        // Re-open and inspect the Parquet schema directly
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let arrow_schema = builder.schema();

        // Column names
        let names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            &[
                "timestamp",
                "obj_id",
                "obj_size",
                "next_access_vtime",
                "op",
                "ttl"
            ]
        );

        // Timestamp column carries Nanosecond + UTC metadata
        let ts_field = arrow_schema.field_with_name("timestamp").unwrap();
        assert_eq!(
            *ts_field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
        );
        assert!(!ts_field.is_nullable());
    }

    #[test]
    fn reader_streams_across_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.parquet");

        // Force small row groups so the file has many of them.
        const ROWS: usize = 10_000;
        const ROW_GROUP: usize = 512;
        let schema = Arc::new(trace_schema());
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), Some(props))
                .unwrap();
        let ids: Vec<u64> = (0..ROWS as u64).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(
                        ids.iter().map(|&i| i as i64).collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                Arc::new(UInt64Array::from(ids.clone())),
                Arc::new(UInt32Array::from(vec![1u32; ROWS])),
                Arc::new(Int64Array::from(vec![-1i64; ROWS])),
                Arc::new(UInt8Array::from(vec![None::<u8>; ROWS])),
                Arc::new(Int32Array::from(vec![None::<i32>; ROWS])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = TraceReader::open(&path).unwrap();
        assert_eq!(reader.total_entries(), ROWS);
        let mut n = 0u64;
        for entry in reader {
            let entry = entry.unwrap();
            assert_eq!(entry.obj_id, n, "rows must arrive in file order");
            assert_eq!(entry.timestamp, n as i64);
            assert_eq!(entry.op, None);
            n += 1;
        }
        assert_eq!(n as usize, ROWS);
    }

    #[test]
    fn op_classification() {
        assert!(Op::Get.is_read());
        assert!(Op::Read.is_read());
        assert!(Op::Nop.is_read());
        assert!(!Op::Get.is_write());
        assert!(!Op::Get.is_delete());

        assert!(Op::Set.is_write());
        assert!(Op::Add.is_write());
        assert!(Op::Incr.is_write());
        assert!(!Op::Set.is_read());

        assert!(Op::Delete.is_delete());
        assert!(!Op::Delete.is_read());
        assert!(!Op::Delete.is_write());
    }
}
