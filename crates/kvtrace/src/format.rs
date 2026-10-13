//! The request-level trace format: one Parquet row per inference request.
//!
//! | Column          | Arrow type              | Nullable | Meaning                                      |
//! |-----------------|-------------------------|----------|----------------------------------------------|
//! | `timestamp`     | `Timestamp(ns, UTC)`    | no       | Request arrival, ns since the Unix epoch      |
//! | `req_id`        | `UInt64`                | no       | Request id                                    |
//! | `input_tokens`  | `UInt32`                | no       | Prompt length in tokens, as the source gives  |
//! | `output_tokens` | `UInt32`                | no       | Generated tokens                              |
//! | `chunks`        | `List<UInt64>`          | no       | Ids of the request's full prefix chunks, in order |
//! | `session_id`    | `UInt64`                | yes      | Session, for sources that need reconstruction |
//!
//! Equal chunk ids mean equal prefix content. Every chunk covers the same
//! number of tokens, given once per file by the `kvtrace.chunk_tokens`
//! metadata key; tokens after the last full chunk are not represented.
//! Nothing in the format depends on a model: object sizes come from a
//! [`Geometry`](crate::Geometry) when the trace is mapped to objects.
//!
//! File key-value metadata written by [`RequestWriter`]:
//!
//! * `kvtrace.format` = `requests`
//! * `kvtrace.version` = [`FORMAT_VERSION`]
//! * `kvtrace.chunk_tokens`
//!
//! Converters add `kvtrace.created`, `kvtrace.source.*` (source format, file
//! name, SHA-256) and `kvtrace.converter.*` (parameters and counts).

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ListArray, ListBuilder, TimestampNanosecondArray, UInt32Array, UInt64Array,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::Error;

/// Version of the request format, in `kvtrace.version`.
pub const FORMAT_VERSION: &str = "1";

/// Metadata key: `requests` for this format.
pub const KEY_FORMAT: &str = "kvtrace.format";
/// Metadata key: format version.
pub const KEY_VERSION: &str = "kvtrace.version";
/// Metadata key: tokens per chunk.
pub const KEY_CHUNK_TOKENS: &str = "kvtrace.chunk_tokens";
/// Value of [`KEY_FORMAT`] in a request trace.
pub const FORMAT_REQUESTS: &str = "requests";

/// One inference request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Arrival time, ns since the Unix epoch.
    pub timestamp: i64,
    pub req_id: u64,
    /// Prompt length as the source gives it. It can disagree with
    /// `chunks.len() * chunk_tokens`, which counts full chunks only.
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// Full prefix chunk ids, in prefix order.
    pub chunks: Vec<u64>,
    pub session_id: Option<u64>,
}

fn chunk_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::UInt64, false))
}

/// Arrow schema of a request trace.
pub fn request_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new("req_id", DataType::UInt64, false),
        Field::new("input_tokens", DataType::UInt32, false),
        Field::new("output_tokens", DataType::UInt32, false),
        Field::new("chunks", DataType::List(chunk_item_field()), false),
        Field::new("session_id", DataType::UInt64, true),
    ])
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writes requests to a ZSTD-compressed Parquet file.
pub struct RequestWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    timestamps: Vec<i64>,
    req_ids: Vec<u64>,
    input_tokens: Vec<u32>,
    output_tokens: Vec<u32>,
    chunks: ListBuilder<UInt64Builder>,
    session_ids: Vec<Option<u64>>,
    batch_size: usize,
}

impl RequestWriter {
    /// Create a request trace whose chunks cover `chunk_tokens` tokens each.
    /// Writes the format, version and chunk length metadata.
    pub fn create(
        path: impl AsRef<Path>,
        chunk_tokens: u32,
        batch_size: usize,
    ) -> Result<Self, Error> {
        if chunk_tokens == 0 {
            return Err(Error::InvalidFormat("kvtrace: chunk_tokens is 0".into()));
        }
        let schema = Arc::new(request_schema());
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props))?;
        for (k, v) in [
            (KEY_FORMAT, FORMAT_REQUESTS.to_string()),
            (KEY_VERSION, FORMAT_VERSION.to_string()),
            (KEY_CHUNK_TOKENS, chunk_tokens.to_string()),
        ] {
            writer.append_key_value_metadata(KeyValue::new(k.to_string(), v));
        }
        let batch_size = batch_size.max(1);
        Ok(Self {
            writer,
            schema,
            timestamps: Vec::with_capacity(batch_size),
            req_ids: Vec::with_capacity(batch_size),
            input_tokens: Vec::with_capacity(batch_size),
            output_tokens: Vec::with_capacity(batch_size),
            chunks: ListBuilder::new(UInt64Builder::new()).with_field(chunk_item_field()),
            session_ids: Vec::with_capacity(batch_size),
            batch_size,
        })
    }

    /// Add a key-value pair to the file footer. Pairs appended any time
    /// before [`finish`](Self::finish) are written.
    pub fn append_key_value_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.writer
            .append_key_value_metadata(KeyValue::new(key.into(), value.into()));
    }

    /// Append one request (buffered; flushed in batches).
    pub fn write(&mut self, r: &Request) -> Result<(), Error> {
        self.timestamps.push(r.timestamp);
        self.req_ids.push(r.req_id);
        self.input_tokens.push(r.input_tokens);
        self.output_tokens.push(r.output_tokens);
        self.chunks.values().append_slice(&r.chunks);
        self.chunks.append(true);
        self.session_ids.push(r.session_id);
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
            Arc::new(UInt64Array::from(std::mem::take(&mut self.req_ids))),
            Arc::new(UInt32Array::from(std::mem::take(&mut self.input_tokens))),
            Arc::new(UInt32Array::from(std::mem::take(&mut self.output_tokens))),
            Arc::new(self.chunks.finish()),
            Arc::new(UInt64Array::from(std::mem::take(&mut self.session_ids))),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&batch)?;
        Ok(())
    }

    /// Flush buffered requests and close the file.
    pub fn finish(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.writer.close()?;
        Ok(())
    }

    /// Like [`finish`](Self::finish), then flush the file to stable storage
    /// (`File::sync_all`).
    pub fn finish_synced(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.writer.into_inner()?.sync_all()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

const READ_BATCH_SIZE: usize = 8192;

/// Reads requests from a request trace, one record batch at a time.
pub struct RequestReader {
    batches: ParquetRecordBatchReader,
    total_requests: usize,
    chunk_tokens: u32,
    metadata: BTreeMap<String, String>,
    current: Option<Columns>,
    row_idx: usize,
    failed: bool,
}

struct Columns {
    timestamp: TimestampNanosecondArray,
    req_id: UInt64Array,
    input_tokens: UInt32Array,
    output_tokens: UInt32Array,
    chunks: ListArray,
    session_id: UInt64Array,
}

impl Columns {
    fn from_batch(batch: &RecordBatch) -> Result<Self, Error> {
        fn col<T: Array + Clone + 'static>(batch: &RecordBatch, name: &str) -> Result<T, Error> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<T>())
                .cloned()
                .ok_or_else(|| {
                    Error::InvalidFormat(format!(
                        "kvtrace: request trace column {name} is missing or has the wrong type"
                    ))
                })
        }
        let chunks: ListArray = col(batch, "chunks")?;
        if chunks
            .values()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .is_none()
        {
            return Err(Error::InvalidFormat(
                "kvtrace: request trace column chunks is not a list of UInt64".into(),
            ));
        }
        Ok(Self {
            timestamp: col(batch, "timestamp")?,
            req_id: col(batch, "req_id")?,
            input_tokens: col(batch, "input_tokens")?,
            output_tokens: col(batch, "output_tokens")?,
            chunks,
            session_id: col(batch, "session_id")?,
        })
    }

    fn request(&self, i: usize) -> Request {
        let list = self.chunks.value(i);
        let ids = list
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked in from_batch");
        Request {
            timestamp: self.timestamp.value(i),
            req_id: self.req_id.value(i),
            input_tokens: self.input_tokens.value(i),
            output_tokens: self.output_tokens.value(i),
            chunks: ids.values().to_vec(),
            session_id: (!self.session_id.is_null(i)).then(|| self.session_id.value(i)),
        }
    }

    fn len(&self) -> usize {
        self.req_id.len()
    }
}

impl RequestReader {
    /// Open a request trace. Errors if the file's metadata does not name
    /// this format and version or lacks a positive `kvtrace.chunk_tokens`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        let file_meta = builder.metadata().file_metadata();
        let metadata: BTreeMap<String, String> = file_meta
            .key_value_metadata()
            .into_iter()
            .flatten()
            .filter_map(|kv| Some((kv.key.clone(), kv.value.clone()?)))
            .collect();
        let bad = |msg: String| Error::InvalidFormat(format!("{}: {msg}", path.display()));
        match metadata.get(KEY_FORMAT).map(String::as_str) {
            Some(FORMAT_REQUESTS) => {}
            other => {
                return Err(bad(format!(
                    "not a kvtrace request trace ({KEY_FORMAT} is {other:?})"
                )))
            }
        }
        match metadata.get(KEY_VERSION).map(String::as_str) {
            Some(FORMAT_VERSION) => {}
            other => {
                return Err(bad(format!(
                    "unsupported request trace version {other:?} (expected {FORMAT_VERSION})"
                )))
            }
        }
        let chunk_tokens = metadata
            .get(KEY_CHUNK_TOKENS)
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|&c| c > 0)
            .ok_or_else(|| {
                bad(format!(
                    "{KEY_CHUNK_TOKENS} missing or not a positive integer"
                ))
            })?;
        let total_requests = usize::try_from(file_meta.num_rows())
            .map_err(|_| bad("negative row count in footer".into()))?;
        let batches = builder.with_batch_size(READ_BATCH_SIZE).build()?;
        Ok(Self {
            batches,
            total_requests,
            chunk_tokens,
            metadata,
            current: None,
            row_idx: 0,
            failed: false,
        })
    }

    /// Requests in the file, from the footer.
    pub fn total_requests(&self) -> usize {
        self.total_requests
    }

    /// Tokens per chunk.
    pub fn chunk_tokens(&self) -> u32 {
        self.chunk_tokens
    }

    /// The file's key-value metadata.
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    fn next_batch(&mut self) -> Result<bool, Error> {
        loop {
            match self.batches.next() {
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                Some(Ok(batch)) => {
                    self.current = Some(Columns::from_batch(&batch)?);
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

impl Iterator for RequestReader {
    type Item = Result<Request, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self
            .current
            .as_ref()
            .is_none_or(|c| self.row_idx >= c.len())
        {
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
        Some(Ok(cols.request(i)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Request> {
        vec![
            Request {
                timestamp: 1_000_000,
                req_id: 0,
                input_tokens: 700,
                output_tokens: 5,
                chunks: vec![0, 1],
                session_id: None,
            },
            Request {
                timestamp: 3_000_000,
                req_id: 1,
                input_tokens: 10,
                output_tokens: 0,
                chunks: vec![],
                session_id: Some(42),
            },
            Request {
                timestamp: 5_000_000,
                req_id: 7,
                input_tokens: u32::MAX,
                output_tokens: 1,
                chunks: vec![u64::MAX, 0, 3],
                session_id: Some(0),
            },
        ]
    }

    fn write(path: &Path, reqs: &[Request], batch: usize) {
        let mut w = RequestWriter::create(path, 256, batch).unwrap();
        w.append_key_value_metadata("kvtrace.source.format", "test");
        for r in reqs {
            w.write(r).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // Batch sizes 1 and 2 put requests in several record batches.
        for batch in [1, 2, 1024] {
            let path = dir.path().join(format!("r{batch}.parquet"));
            write(&path, &sample(), batch);
            let r = RequestReader::open(&path).unwrap();
            assert_eq!(r.total_requests(), 3);
            assert_eq!(r.chunk_tokens(), 256);
            assert_eq!(r.metadata()["kvtrace.source.format"], "test");
            assert_eq!(r.metadata()[KEY_FORMAT], "requests");
            assert_eq!(r.metadata()[KEY_VERSION], FORMAT_VERSION);
            let got: Vec<Request> = r.collect::<Result<_, _>>().unwrap();
            assert_eq!(got, sample(), "batch {batch}");
        }
    }

    #[test]
    fn schema_types() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write(&path, &sample(), 16);
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let schema = b.schema();
        let want = request_schema();
        for f in want.fields() {
            let got = schema.field_with_name(f.name()).unwrap();
            assert_eq!(got.data_type(), f.data_type(), "{}", f.name());
            assert_eq!(got.is_nullable(), f.is_nullable(), "{}", f.name());
        }
        assert_eq!(schema.fields().len(), 6);
    }

    #[test]
    fn other_files_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        // A Parquet file without the format metadata.
        let path = dir.path().join("plain.parquet");
        let schema = Arc::new(request_schema());
        let w = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        w.close().unwrap();
        let err = RequestReader::open(&path).err().unwrap().to_string();
        assert!(err.contains("not a kvtrace request trace"), "{err}");

        // Right format, other version.
        let path = dir.path().join("v9.parquet");
        let mut w = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            Arc::new(request_schema()),
            None,
        )
        .unwrap();
        w.append_key_value_metadata(KeyValue::new(
            KEY_FORMAT.into(),
            FORMAT_REQUESTS.to_string(),
        ));
        w.append_key_value_metadata(KeyValue::new(KEY_VERSION.into(), "9".to_string()));
        w.close().unwrap();
        let err = RequestReader::open(&path).err().unwrap().to_string();
        assert!(err.contains("version"), "{err}");

        assert!(RequestWriter::create(dir.path().join("z.parquet"), 0, 16).is_err());
    }
}
