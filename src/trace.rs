//! Trace data model, Parquet I/O, and format conversion.
//!
//! The on-disk format is Parquet. The schema is compatible with libCacheSim's
//! `oracleGeneral` binary trace format:
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
//! In addition to libCacheSim binary formats, this module can import
//! [pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace) CSV
//! files (optionally zstd-compressed).

use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, TimestampNanosecondArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::Error;

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

/// Reads trace entries from a Parquet file.
pub struct TraceReader {
    batches: Vec<RecordBatch>,
    batch_idx: usize,
    row_idx: usize,
}

impl TraceReader {
    /// Open a Parquet trace file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            batches,
            batch_idx: 0,
            row_idx: 0,
        })
    }

    /// Total number of entries across all row groups.
    pub fn total_entries(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

impl Iterator for TraceReader {
    type Item = TraceEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.batch_idx < self.batches.len() {
            let batch = &self.batches[self.batch_idx];
            if self.row_idx >= batch.num_rows() {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }

            let i = self.row_idx;
            self.row_idx += 1;

            let timestamp = col_ts_ns(batch, "timestamp").map(|a| a.value(i)).unwrap_or(0);
            let obj_id = col_u64(batch, "obj_id").map(|a| a.value(i)).unwrap_or(0);
            let obj_size = col_u32(batch, "obj_size").map(|a| a.value(i)).unwrap_or(0);
            let next_access_vtime = col_i64(batch, "next_access_vtime")
                .map(|a| a.value(i))
                .unwrap_or(-1);
            let op = col_u8(batch, "op").and_then(|a| {
                if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i))
                }
            });
            let ttl = col_i32(batch, "ttl").and_then(|a| {
                if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i))
                }
            });

            return Some(TraceEntry {
                timestamp,
                obj_id,
                obj_size,
                next_access_vtime,
                op,
                ttl,
            });
        }
        None
    }
}

// Column downcast helpers
fn col_ts_ns<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a TimestampNanosecondArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
}
fn col_u8<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a UInt8Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
}
fn col_u32<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a UInt32Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
}
fn col_u64<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
}
fn col_i32<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
}
fn col_i64<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref())
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
            Arc::new(Int64Array::from(std::mem::take(&mut self.next_access_vtimes))),
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
}

// ---------------------------------------------------------------------------
// Binary trace formats
// ---------------------------------------------------------------------------

/// libCacheSim binary trace formats supported for import.
#[derive(Debug, Clone, Copy)]
pub enum BinFormat {
    /// 24 bytes: `u32 timestamp | u64 obj_id | u32 obj_size | i64 next_access_vtime`
    OracleGeneral,
    /// 27 bytes: `u32 timestamp | u64 obj_id | u32 obj_size | u8 op | u16 ns | i64 next_access_vtime`
    OracleGeneralOpNs,
}

impl BinFormat {
    /// Size of a single record in bytes.
    pub fn record_size(self) -> usize {
        match self {
            Self::OracleGeneral => 24,
            Self::OracleGeneralOpNs => 27,
        }
    }
}

/// Stream-convert a libCacheSim binary trace file into Parquet.
///
/// Returns the number of records converted.
pub fn convert_bin_to_parquet(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    format: BinFormat,
    batch_size: usize,
) -> Result<usize, Error> {
    let file = File::open(input)?;
    let file_len = file.metadata()?.len() as usize;
    let rec_size = format.record_size();

    if file_len % rec_size != 0 {
        return Err(Error::InvalidFormat(format!(
            "file size ({file_len}) is not a multiple of record size ({rec_size})"
        )));
    }

    let num_records = file_len / rec_size;
    let mut reader = BufReader::new(file);
    let mut writer = TraceWriter::create(output, batch_size)?;
    let mut buf = vec![0u8; rec_size];

    for _ in 0..num_records {
        reader.read_exact(&mut buf)?;
        let entry = match format {
            BinFormat::OracleGeneral => parse_oracle_general(&buf),
            BinFormat::OracleGeneralOpNs => parse_oracle_general_opns(&buf),
        };
        writer.write(&entry)?;
    }
    writer.finish()?;

    Ok(num_records)
}

const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Parse a 24-byte oracleGeneral record (little-endian, packed).
/// The on-wire timestamp is u32 seconds; we widen to i64 nanoseconds.
fn parse_oracle_general(buf: &[u8]) -> TraceEntry {
    let secs = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    TraceEntry {
        timestamp: secs as i64 * NANOS_PER_SEC,
        obj_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        obj_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        next_access_vtime: i64::from_le_bytes(buf[16..24].try_into().unwrap()),
        op: None,
        ttl: None,
    }
}

/// Parse a 27-byte oracleGeneralOpNs record (little-endian, packed).
/// The on-wire timestamp is u32 seconds; we widen to i64 nanoseconds.
fn parse_oracle_general_opns(buf: &[u8]) -> TraceEntry {
    let secs = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    TraceEntry {
        timestamp: secs as i64 * NANOS_PER_SEC,
        obj_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        obj_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        op: Some(buf[16]),
        // bytes 17..19 = u16 namespace (not stored as a separate column)
        next_access_vtime: i64::from_le_bytes(buf[19..27].try_into().unwrap()),
        ttl: None,
    }
}

// ---------------------------------------------------------------------------
// pelikan-io/cache-trace CSV import
// ---------------------------------------------------------------------------

/// Convert a [pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace)
/// CSV file to Parquet.
///
/// CSV columns (no header): `timestamp,key,key_size,value_size,client_id,op,ttl`
///
/// * `timestamp` (seconds) is widened to u64 nanoseconds.
/// * `key` is hashed to a deterministic `u64` via ahash (fixed seed).
/// * `obj_size` = `key_size + value_size`.
/// * `next_access_vtime` is set to `-1` (not available in CSV).
/// * Files with a `.zst` extension are decompressed automatically.
///
/// Returns the number of records converted.
pub fn convert_cache_trace_to_parquet(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    batch_size: usize,
) -> Result<usize, Error> {
    let path = input.as_ref();
    let file = File::open(path)?;

    let reader: Box<dyn BufRead> =
        if path.extension().is_some_and(|e| e == "zst" || e == "zstd") {
            Box::new(BufReader::new(zstd::stream::read::Decoder::new(file)?))
        } else {
            Box::new(BufReader::new(file))
        };

    let hash_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    let mut writer = TraceWriter::create(output, batch_size)?;
    let mut count: usize = 0;

    for line_result in reader.lines() {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        let entry = parse_cache_trace_line(&line, &hash_state)?;
        writer.write(&entry)?;
        count += 1;
    }

    writer.finish()?;
    Ok(count)
}

/// Parse one CSV line from a pelikan-io/cache-trace file.
fn parse_cache_trace_line(line: &str, hash_state: &ahash::RandomState) -> Result<TraceEntry, Error> {
    // Fields: timestamp, key, key_size, value_size, client_id, operation, ttl
    let mut fields = line.splitn(7, ',');

    let ts_str = fields.next().ok_or_else(|| bad_csv("missing timestamp"))?;
    let key = fields.next().ok_or_else(|| bad_csv("missing key"))?;
    let ks_str = fields.next().ok_or_else(|| bad_csv("missing key_size"))?;
    let vs_str = fields.next().ok_or_else(|| bad_csv("missing value_size"))?;
    let _client = fields.next().ok_or_else(|| bad_csv("missing client_id"))?;
    let op_str = fields.next().ok_or_else(|| bad_csv("missing operation"))?;
    let ttl_str = fields.next().ok_or_else(|| bad_csv("missing ttl"))?;

    let timestamp_secs: i64 = ts_str
        .parse()
        .map_err(|e| bad_csv(&format!("bad timestamp '{ts_str}': {e}")))?;
    let key_size: u32 = ks_str
        .parse()
        .map_err(|e| bad_csv(&format!("bad key_size '{ks_str}': {e}")))?;
    let value_size: u32 = vs_str
        .parse()
        .map_err(|e| bad_csv(&format!("bad value_size '{vs_str}': {e}")))?;
    let ttl: i32 = ttl_str
        .trim_end()
        .parse()
        .map_err(|e| bad_csv(&format!("bad ttl '{ttl_str}': {e}")))?;

    // Deterministic hash of the key string → obj_id
    let mut hasher = hash_state.build_hasher();
    hasher.write(key.as_bytes());
    let obj_id = hasher.finish();

    let op = match op_str {
        "get" => Op::Get,
        "gets" => Op::Gets,
        "set" => Op::Set,
        "add" => Op::Add,
        "cas" => Op::Cas,
        "replace" => Op::Replace,
        "append" => Op::Append,
        "prepend" => Op::Prepend,
        "delete" => Op::Delete,
        "incr" => Op::Incr,
        "decr" => Op::Decr,
        _ => Op::Invalid,
    };

    Ok(TraceEntry {
        timestamp: timestamp_secs * NANOS_PER_SEC,
        obj_id,
        obj_size: key_size.saturating_add(value_size),
        next_access_vtime: -1,
        op: Some(op as u8),
        ttl: if ttl > 0 { Some(ttl) } else { None },
    })
}

fn bad_csv(msg: &str) -> Error {
    Error::InvalidFormat(format!("cache-trace CSV: {msg}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_oracle_general_roundtrip() {
        let ts_secs: u32 = 1_700_000_000;
        let obj_id: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let obj_size: u32 = 4096;
        let next_vtime: i64 = 42;

        // Serialize to the 24-byte on-wire format (timestamp is u32 seconds)
        let mut buf = [0u8; 24];
        buf[0..4].copy_from_slice(&ts_secs.to_le_bytes());
        buf[4..12].copy_from_slice(&obj_id.to_le_bytes());
        buf[12..16].copy_from_slice(&obj_size.to_le_bytes());
        buf[16..24].copy_from_slice(&next_vtime.to_le_bytes());

        let parsed = parse_oracle_general(&buf);
        // Binary parser widens u32 seconds → i64 nanoseconds
        assert_eq!(parsed.timestamp, ts_secs as i64 * NANOS_PER_SEC);
        assert_eq!(parsed.obj_id, obj_id);
        assert_eq!(parsed.obj_size, obj_size);
        assert_eq!(parsed.next_access_vtime, next_vtime);
        assert!(parsed.op.is_none());
        assert!(parsed.ttl.is_none());
    }

    #[test]
    fn parse_oracle_general_opns_fields() {
        let mut buf = [0u8; 27];
        buf[0..4].copy_from_slice(&100u32.to_le_bytes());
        buf[4..12].copy_from_slice(&999u64.to_le_bytes());
        buf[12..16].copy_from_slice(&512u32.to_le_bytes());
        buf[16] = 1; // op = GET
        buf[17..19].copy_from_slice(&7u16.to_le_bytes()); // ns = 7
        buf[19..27].copy_from_slice(&(-1i64).to_le_bytes());

        let parsed = parse_oracle_general_opns(&buf);
        assert_eq!(parsed.timestamp, 100 * NANOS_PER_SEC);
        assert_eq!(parsed.obj_id, 999);
        assert_eq!(parsed.obj_size, 512);
        assert_eq!(parsed.op, Some(1));
        assert_eq!(parsed.next_access_vtime, -1);
    }

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
        let read_entries: Vec<TraceEntry> = reader.collect();

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
        let names: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            &["timestamp", "obj_id", "obj_size", "next_access_vtime", "op", "ttl"]
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
    fn bin_to_parquet_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let bin_path = dir.path().join("trace.bin");
        let pq_path = dir.path().join("trace.parquet");

        // Write 3 oracleGeneral records
        let mut bin_data = Vec::new();
        for i in 0u32..3 {
            bin_data.extend_from_slice(&(i * 10).to_le_bytes());
            bin_data.extend_from_slice(&((i as u64 + 1) * 1000).to_le_bytes());
            bin_data.extend_from_slice(&((i + 1) * 256).to_le_bytes());
            bin_data.extend_from_slice(&(if i < 2 { (i as i64 + 1) * 5 } else { -1i64 }).to_le_bytes());
        }
        std::fs::write(&bin_path, &bin_data).unwrap();

        let count =
            convert_bin_to_parquet(&bin_path, &pq_path, BinFormat::OracleGeneral, 1024).unwrap();
        assert_eq!(count, 3);

        let reader = TraceReader::open(&pq_path).unwrap();
        let entries: Vec<TraceEntry> = reader.collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].timestamp, 0); // 0 seconds → 0 nanoseconds
        assert_eq!(entries[0].obj_id, 1000);
        assert_eq!(entries[0].obj_size, 256);
        assert_eq!(entries[1].obj_id, 2000);
        assert_eq!(entries[2].next_access_vtime, -1);
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

    #[test]
    fn parse_cache_trace_csv_line() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let line = "1583990400,nz:u:eeW511W3dcH3de3d15ec,22,304,127,get,0";
        let entry = parse_cache_trace_line(line, &state).unwrap();

        assert_eq!(entry.timestamp, 1_583_990_400 * NANOS_PER_SEC);
        assert_eq!(entry.obj_size, 22 + 304);
        assert_eq!(entry.op, Some(Op::Get as u8));
        assert_eq!(entry.ttl, None); // ttl=0 → None
        assert_eq!(entry.next_access_vtime, -1);
        // obj_id is a deterministic hash of the key
        assert_ne!(entry.obj_id, 0);
    }

    #[test]
    fn parse_cache_trace_csv_set_with_ttl() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let line = "1583990401,abc:key123,10,512,42,set,3600";
        let entry = parse_cache_trace_line(line, &state).unwrap();

        assert_eq!(entry.timestamp, 1_583_990_401 * NANOS_PER_SEC);
        assert_eq!(entry.obj_size, 10 + 512);
        assert_eq!(entry.op, Some(Op::Set as u8));
        assert_eq!(entry.ttl, Some(3600));
    }

    #[test]
    fn parse_cache_trace_deterministic_hash() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let line1 = "0,mykey,4,100,1,get,0";
        let line2 = "1,mykey,4,100,2,get,0";
        let e1 = parse_cache_trace_line(line1, &state).unwrap();
        let e2 = parse_cache_trace_line(line2, &state).unwrap();
        // Same key → same obj_id regardless of other fields
        assert_eq!(e1.obj_id, e2.obj_id);
    }

    #[test]
    fn cache_trace_to_parquet_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("trace.csv");
        let pq_path = dir.path().join("trace.parquet");

        let csv_data = "\
1583990400,nz:u:key1,10,200,1,get,0
1583990401,nz:u:key2,15,500,2,set,7200
1583990402,nz:u:key1,10,200,1,delete,0
";
        std::fs::write(&csv_path, csv_data).unwrap();

        let count = convert_cache_trace_to_parquet(&csv_path, &pq_path, 1024).unwrap();
        assert_eq!(count, 3);

        let reader = TraceReader::open(&pq_path).unwrap();
        let entries: Vec<TraceEntry> = reader.collect();
        assert_eq!(entries.len(), 3);

        // First and third entries share the same key → same obj_id
        assert_eq!(entries[0].obj_id, entries[2].obj_id);
        // Different keys → different obj_ids
        assert_ne!(entries[0].obj_id, entries[1].obj_id);

        assert_eq!(entries[0].op, Some(Op::Get as u8));
        assert_eq!(entries[1].op, Some(Op::Set as u8));
        assert_eq!(entries[1].ttl, Some(7200));
        assert_eq!(entries[2].op, Some(Op::Delete as u8));
    }
}
