//! The request-level trace format: one Parquet row per inference request.
//!
//! | Column          | Arrow type              | Nullable | Meaning                                      |
//! |-----------------|-------------------------|----------|----------------------------------------------|
//! | `timestamp`     | `Timestamp(ns, UTC)`    | no       | Request arrival, ns since the Unix epoch      |
//! | `req_id`        | `UInt64`                | no       | Request id                                    |
//! | `input_tokens`  | `UInt32`                | no       | Prompt length in tokens, as the source gives  |
//! | `output_tokens` | `UInt32`                | no       | Generated tokens                              |
//! | `chunks`        | `List<UInt64>`          | no       | Ids of the request's full prefix chunks, in order, each below 2^48 |
//! | `session_id`    | `UInt64`                | yes      | Session, for sources that need reconstruction |
//!
//! Chunk ids must be below 2^48 because an object id packs the chunk id with
//! a key slot and a tensor-parallel rank in 64 bits
//! (`chunk << 16 | slot << 12 | rank`, [`obj_id`](crate::obj_id));
//! [`RequestWriter::write`] rejects larger ids, and [`RequestReader`] rejects
//! them naming the file, row and `req_id`. No column may be null except
//! `session_id`; [`RequestReader`] rejects a batch with nulls elsewhere.
//!
//! Rows are in arrival order: each `timestamp` is at least the previous
//! row's. [`RequestWriter::write`] rejects a request that arrives before the
//! previous one, and [`RequestReader`] rejects such a row. A consumer can
//! therefore replay a trace as it reads it, without sorting it first.
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
//! * `kvtrace.max_chunks`: the most chunks any request has, written by
//!   [`RequestWriter::finish`]. Files from other writers may lack it;
//!   [`RequestReader::max_chunks`] then reads the `chunks` column to find it.
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
use parquet::arrow::{ArrowWriter, ProjectionMask};
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
/// Metadata key: the most chunks any request in the file has.
pub const KEY_MAX_CHUNKS: &str = "kvtrace.max_chunks";
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
    /// Timestamp of the last request written, for the order check.
    last_timestamp: Option<i64>,
    /// Most chunks in any request written, for [`KEY_MAX_CHUNKS`].
    max_chunks: usize,
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
            last_timestamp: None,
            max_chunks: 0,
        })
    }

    /// Add a key-value pair to the file footer. Pairs appended any time
    /// before [`finish`](Self::finish) are written.
    pub fn append_key_value_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.writer
            .append_key_value_metadata(KeyValue::new(key.into(), value.into()));
    }

    /// Append one request (buffered; flushed in batches). Errors if a chunk
    /// id is 2^48 or more, since it would not fit an object id, or if the
    /// request's timestamp is earlier than the previous request's. Nothing
    /// is written for a request that errors.
    pub fn write(&mut self, r: &Request) -> Result<(), Error> {
        if let Some(&c) = r
            .chunks
            .iter()
            .find(|&&c| c >= 1 << crate::objects::CHUNK_ID_BITS)
        {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: request {} has chunk id {c}, which is not below 2^{}",
                r.req_id,
                crate::objects::CHUNK_ID_BITS
            )));
        }
        if let Some(prev) = self.last_timestamp.filter(|&p| r.timestamp < p) {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: request {} has timestamp {}, before the previous request's {prev}; \
                 requests must be written in arrival order",
                r.req_id, r.timestamp
            )));
        }
        self.last_timestamp = Some(r.timestamp);
        self.max_chunks = self.max_chunks.max(r.chunks.len());
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

    fn write_footer_metadata(&mut self) {
        self.writer.append_key_value_metadata(KeyValue::new(
            KEY_MAX_CHUNKS.to_string(),
            self.max_chunks.to_string(),
        ));
    }

    /// Flush buffered requests, write [`KEY_MAX_CHUNKS`], and close the file.
    pub fn finish(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.write_footer_metadata();
        self.writer.close()?;
        Ok(())
    }

    /// Like [`finish`](Self::finish), then flush the file to stable storage
    /// (`File::sync_all`).
    pub fn finish_synced(mut self) -> Result<(), Error> {
        self.flush_batch()?;
        self.write_footer_metadata();
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
    /// [`KEY_MAX_CHUNKS`] from the footer, when present.
    footer_max_chunks: Option<usize>,
    metadata: BTreeMap<String, String>,
    current: Option<Columns>,
    row_idx: usize,
    failed: bool,
    /// For error messages.
    path: std::path::PathBuf,
    /// Rows returned so far, for error messages.
    rows_read: u64,
    /// Timestamp of the last row returned, for the order check.
    last_timestamp: Option<i64>,
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
        let cols = Self {
            timestamp: col(batch, "timestamp")?,
            req_id: col(batch, "req_id")?,
            input_tokens: col(batch, "input_tokens")?,
            output_tokens: col(batch, "output_tokens")?,
            chunks,
            session_id: col(batch, "session_id")?,
        };
        // Files from other writers may declare these columns nullable; a
        // null would otherwise read as 0 or an empty list.
        let null_counts = [
            ("timestamp", cols.timestamp.null_count()),
            ("req_id", cols.req_id.null_count()),
            ("input_tokens", cols.input_tokens.null_count()),
            ("output_tokens", cols.output_tokens.null_count()),
            ("chunks", cols.chunks.null_count()),
            ("chunks item", cols.chunks.values().null_count()),
        ];
        if let Some((name, n)) = null_counts.iter().find(|(_, n)| *n > 0) {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: request trace column {name} has {n} null values in a batch; \
                 only session_id may be null"
            )));
        }
        Ok(cols)
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
    /// this format and version, lacks a positive `kvtrace.chunk_tokens`, or
    /// has a `kvtrace.max_chunks` that is not an integer.
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
        let footer_max_chunks = match metadata.get(KEY_MAX_CHUNKS) {
            Some(v) => Some(
                v.parse::<usize>()
                    .map_err(|_| bad(format!("{KEY_MAX_CHUNKS} {v:?} is not an integer")))?,
            ),
            None => None,
        };
        let total_requests = usize::try_from(file_meta.num_rows())
            .map_err(|_| bad("negative row count in footer".into()))?;
        let batches = builder.with_batch_size(READ_BATCH_SIZE).build()?;
        Ok(Self {
            batches,
            total_requests,
            chunk_tokens,
            footer_max_chunks,
            metadata,
            current: None,
            row_idx: 0,
            failed: false,
            path: path.to_path_buf(),
            rows_read: 0,
            last_timestamp: None,
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

    /// The most chunks any request in the file has, for sizing buffers
    /// before reading the requests. Taken from `kvtrace.max_chunks` when the
    /// footer has it; reading a row with more chunks than that is an error.
    /// Otherwise this reads the file's `chunks` column, once per call, so a
    /// caller should keep the result.
    pub fn max_chunks(&self) -> Result<usize, Error> {
        if let Some(n) = self.footer_max_chunks {
            return Ok(n);
        }
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&self.path)?)?;
        let root = builder
            .parquet_schema()
            .root_schema()
            .get_fields()
            .iter()
            .position(|f| f.name() == "chunks")
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "{}: request trace has no chunks column",
                    self.path.display()
                ))
            })?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), [root]);
        let mut max = 0;
        for batch in builder
            .with_projection(mask)
            .with_batch_size(READ_BATCH_SIZE)
            .build()?
        {
            let batch = batch?;
            let chunks = batch
                .column_by_name("chunks")
                .and_then(|c| c.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| {
                    Error::InvalidFormat(format!(
                        "{}: request trace column chunks is not a list",
                        self.path.display()
                    ))
                })?;
            let longest = chunks
                .offsets()
                .windows(2)
                .map(|w| (w[1] - w[0]) as usize)
                .max()
                .unwrap_or(0);
            max = max.max(longest);
        }
        Ok(max)
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
                    return Some(Err(match e {
                        Error::InvalidFormat(m) => Error::InvalidFormat(format!(
                            "{}: batch after row {}: {m}",
                            self.path.display(),
                            self.rows_read
                        )),
                        e => e,
                    }));
                }
            }
        }
        let cols = self.current.as_ref()?;
        let i = self.row_idx;
        self.row_idx += 1;
        let req = cols.request(i);
        let row = self.rows_read;
        self.rows_read += 1;
        if let Some(&c) = req
            .chunks
            .iter()
            .find(|&&c| c >= 1 << crate::objects::CHUNK_ID_BITS)
        {
            self.failed = true;
            return Some(Err(Error::InvalidFormat(format!(
                "{}: row {row} (req_id {}) has chunk id {c}, which is not below 2^{}",
                self.path.display(),
                req.req_id,
                crate::objects::CHUNK_ID_BITS
            ))));
        }
        if let Some(prev) = self.last_timestamp.filter(|&p| req.timestamp < p) {
            self.failed = true;
            return Some(Err(Error::InvalidFormat(format!(
                "{}: row {row} (req_id {}) has timestamp {}, before the previous row's {prev}; \
                 requests must be in arrival order",
                self.path.display(),
                req.req_id,
                req.timestamp
            ))));
        }
        self.last_timestamp = Some(req.timestamp);
        if let Some(max) = self.footer_max_chunks.filter(|&m| req.chunks.len() > m) {
            self.failed = true;
            return Some(Err(Error::InvalidFormat(format!(
                "{}: row {row} (req_id {}) has {} chunks, more than {KEY_MAX_CHUNKS} {max}",
                self.path.display(),
                req.req_id,
                req.chunks.len()
            ))));
        }
        Some(Ok(req))
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
                chunks: vec![(1 << 48) - 1, 0, 3],
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
            assert_eq!(r.metadata()[KEY_MAX_CHUNKS], "3");
            assert_eq!(r.max_chunks().unwrap(), 3);
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

    #[test]
    fn chunk_ids_must_fit_48_bits() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = RequestWriter::create(dir.path().join("r.parquet"), 256, 16).unwrap();
        let mut r = sample().remove(0);
        r.chunks = vec![1, 1 << 48];
        let err = w.write(&r).unwrap_err().to_string();
        assert!(err.contains("2^48"), "{err}");
        r.chunks = vec![(1 << 48) - 1];
        assert!(w.write(&r).is_ok());
    }

    /// A file in the request schema but with every column nullable, as
    /// another writer might produce, with the given arrays.
    fn write_nullable(path: &Path, compression: Compression, arrays: Vec<ArrayRef>) {
        write_nullable_with(path, compression, arrays, &[]);
    }

    /// [`write_nullable`] with extra footer metadata.
    fn write_nullable_with(
        path: &Path,
        compression: Compression,
        arrays: Vec<ArrayRef>,
        extra: &[(&str, &str)],
    ) {
        let item = Arc::new(Field::new("item", DataType::UInt64, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                true,
            ),
            Field::new("req_id", DataType::UInt64, true),
            Field::new("input_tokens", DataType::UInt32, true),
            Field::new("output_tokens", DataType::UInt32, true),
            Field::new("chunks", DataType::List(item), true),
            Field::new("session_id", DataType::UInt64, true),
        ]));
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), Some(props)).unwrap();
        for (k, v) in [
            (KEY_FORMAT, FORMAT_REQUESTS),
            (KEY_VERSION, FORMAT_VERSION),
            (KEY_CHUNK_TOKENS, "256"),
        ]
        .iter()
        .chain(extra)
        {
            w.append_key_value_metadata(KeyValue::new(k.to_string(), v.to_string()));
        }
        w.write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        w.close().unwrap();
    }

    /// Two requests; `null` picks the column given a null in request 1.
    fn arrays(null: Option<&str>) -> Vec<ArrayRef> {
        let n = |c: &str| null == Some(c);
        let mut chunks = ListBuilder::new(UInt64Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::UInt64,
            true,
        )));
        chunks.values().append_slice(&[1, 2]);
        chunks.append(true);
        if n("chunks") {
            chunks.append(false);
        } else {
            chunks.values().append_value(3);
            if n("chunks item") {
                chunks.values().append_null();
            }
            chunks.append(true);
        }
        vec![
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1), (!n("timestamp")).then_some(2)])
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                (!n("req_id")).then_some(1),
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(512),
                (!n("input_tokens")).then_some(256),
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(1),
                (!n("output_tokens")).then_some(1),
            ])),
            Arc::new(chunks.finish()),
            Arc::new(UInt64Array::from(vec![None, Some(4)])),
        ]
    }

    #[test]
    fn nulls_are_rejected_except_in_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.parquet");
        write_nullable(&path, Compression::UNCOMPRESSED, arrays(None));
        let got: Vec<Request> = RequestReader::open(&path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(got[1].chunks, [3]);
        assert_eq!((got[0].session_id, got[1].session_id), (None, Some(4)));

        for col in [
            "timestamp",
            "req_id",
            "input_tokens",
            "output_tokens",
            "chunks",
            "chunks item",
        ] {
            let path = dir.path().join(format!("{col}.parquet"));
            write_nullable(&path, Compression::UNCOMPRESSED, arrays(Some(col)));
            let err = RequestReader::open(&path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("column {col} has 1 null"))
                    && err.contains(&format!("{col}.parquet")),
                "{col}: {err}"
            );
        }
    }

    #[test]
    fn reader_rejects_chunk_ids_of_2_pow_48() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.parquet");
        let mut a = arrays(None);
        let mut chunks = ListBuilder::new(UInt64Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::UInt64,
            true,
        )));
        chunks.values().append_slice(&[1, (1 << 48) - 1]);
        chunks.append(true);
        chunks.values().append_slice(&[2, 1 << 48]);
        chunks.append(true);
        a[4] = Arc::new(chunks.finish());
        write_nullable(&path, Compression::UNCOMPRESSED, a);
        let mut r = RequestReader::open(&path).unwrap();
        assert_eq!(r.next().unwrap().unwrap().chunks, [1, (1 << 48) - 1]);
        let err = r.next().unwrap().unwrap_err().to_string();
        assert!(
            err.contains("big.parquet")
                && err.contains("row 1 (req_id 1)")
                && err.contains("281474976710656"),
            "{err}"
        );
        assert!(r.next().is_none());
    }

    /// pyarrow 21, `compression="gzip"`, one request. The footer is not
    /// compressed, so the file opens; the first batch needs the gzip codec,
    /// which is not built in.
    #[test]
    fn gzip_file_fails_at_first_batch() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/requests/pyarrow-gzip.parquet");
        let mut r = RequestReader::open(&path).unwrap();
        assert_eq!(r.total_requests(), 1);
        let err = r.next().unwrap().unwrap_err().to_string();
        assert!(err.contains("Disabled feature at compile time"), "{err}");
    }

    #[test]
    fn snappy_files_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snappy.parquet");
        write_nullable(&path, Compression::SNAPPY, arrays(None));
        assert_eq!(RequestReader::open(&path).unwrap().count(), 2);
    }

    /// Written by pyarrow 21 with its defaults (snappy compression, list item
    /// field named `element`):
    ///
    /// ```python
    /// t = pa.table({
    ///     "timestamp": pa.array([1_000_000, 2_000_000], pa.timestamp("ns", tz="UTC")),
    ///     "req_id": pa.array([0, 1], pa.uint64()),
    ///     "input_tokens": pa.array([700, 300], pa.uint32()),
    ///     "output_tokens": pa.array([5, 1], pa.uint32()),
    ///     "chunks": pa.array([[0, 1], []], pa.list_(pa.uint64())),
    ///     "session_id": pa.array([None, 9], pa.uint64()),
    /// }).replace_schema_metadata({"kvtrace.format": "requests",
    ///     "kvtrace.version": "1", "kvtrace.chunk_tokens": "256"})
    /// pq.write_table(t, "pyarrow-snappy.parquet")
    /// ```
    #[test]
    fn pyarrow_file_is_read() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/requests/pyarrow-snappy.parquet");
        let r = RequestReader::open(&path).unwrap();
        assert_eq!(r.chunk_tokens(), 256);
        assert!(!r.metadata().contains_key(KEY_MAX_CHUNKS));
        assert_eq!(r.max_chunks().unwrap(), 2);
        let got: Vec<Request> = r.collect::<Result<_, _>>().unwrap();
        assert_eq!(
            got,
            [
                Request {
                    timestamp: 1_000_000,
                    req_id: 0,
                    input_tokens: 700,
                    output_tokens: 5,
                    chunks: vec![0, 1],
                    session_id: None,
                },
                Request {
                    timestamp: 2_000_000,
                    req_id: 1,
                    input_tokens: 300,
                    output_tokens: 1,
                    chunks: vec![],
                    session_id: Some(9),
                },
            ]
        );
    }

    #[test]
    fn writer_requires_arrival_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        let mut w = RequestWriter::create(&path, 256, 16).unwrap();
        let [a, b, c]: [Request; 3] = sample().try_into().unwrap();
        w.write(&b).unwrap();
        let err = w.write(&a).unwrap_err().to_string();
        assert!(
            err.contains("request 0 has timestamp 1000000, before the previous request's 3000000"),
            "{err}"
        );
        // An equal timestamp is in order, and the rejected request was not
        // written.
        w.write(&b).unwrap();
        w.write(&c).unwrap();
        w.finish().unwrap();
        let got: Vec<u64> = RequestReader::open(&path)
            .unwrap()
            .map(|r| r.unwrap().req_id)
            .collect();
        assert_eq!(got, [1, 1, 7]);
    }

    #[test]
    fn empty_trace_has_max_chunks_0() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.parquet");
        RequestWriter::create(&path, 256, 16)
            .unwrap()
            .finish_synced()
            .unwrap();
        let r = RequestReader::open(&path).unwrap();
        assert_eq!(r.metadata()[KEY_MAX_CHUNKS], "0");
        assert_eq!(r.max_chunks().unwrap(), 0);
    }

    #[test]
    fn reader_rejects_rows_out_of_arrival_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("order.parquet");
        let mut a = arrays(None);
        a[0] = Arc::new(TimestampNanosecondArray::from(vec![5, 4]).with_timezone("UTC"));
        write_nullable(&path, Compression::UNCOMPRESSED, a);
        let mut r = RequestReader::open(&path).unwrap();
        assert_eq!(r.next().unwrap().unwrap().timestamp, 5);
        let err = r.next().unwrap().unwrap_err().to_string();
        assert!(
            err.contains("order.parquet")
                && err.contains("row 1 (req_id 1) has timestamp 4, before the previous row's 5"),
            "{err}"
        );
        assert!(r.next().is_none());
    }

    #[test]
    fn max_chunks_without_footer_key_reads_the_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nokey.parquet");
        // Request 0 has 2 chunks, request 1 has 1.
        write_nullable(&path, Compression::SNAPPY, arrays(None));
        let r = RequestReader::open(&path).unwrap();
        assert!(!r.metadata().contains_key(KEY_MAX_CHUNKS));
        assert_eq!(r.max_chunks().unwrap(), 2);
        assert_eq!(r.count(), 2);
    }

    #[test]
    fn reader_rejects_a_row_longer_than_footer_max_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.parquet");
        write_nullable_with(
            &path,
            Compression::UNCOMPRESSED,
            arrays(None),
            &[(KEY_MAX_CHUNKS, "1")],
        );
        let mut r = RequestReader::open(&path).unwrap();
        assert_eq!(r.max_chunks().unwrap(), 1);
        let err = r.next().unwrap().unwrap_err().to_string();
        assert!(
            err.contains("row 0 (req_id 0) has 2 chunks, more than kvtrace.max_chunks 1"),
            "{err}"
        );
        assert!(r.next().is_none());

        let path = dir.path().join("bad.parquet");
        write_nullable_with(
            &path,
            Compression::UNCOMPRESSED,
            arrays(None),
            &[(KEY_MAX_CHUNKS, "lots")],
        );
        let err = RequestReader::open(&path).err().unwrap().to_string();
        assert!(
            err.contains("kvtrace.max_chunks \"lots\" is not an integer"),
            "{err}"
        );
    }
}
