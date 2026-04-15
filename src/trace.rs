//! Trace data model, Parquet I/O, and libCacheSim binary format conversion.
//!
//! The on-disk format is Parquet. The schema is compatible with libCacheSim's
//! `oracleGeneral` binary trace format (24 bytes/record):
//!
//! | Field              | Type   | Bytes | Description                        |
//! |--------------------|--------|-------|------------------------------------|
//! | timestamp          | u64    | 8     | Real time (nanoseconds since epoch)|
//! | obj_id             | u64    | 8     | Object identifier                  |
//! | obj_size           | u32    | 4     | Object size in bytes               |
//! | next_access_vtime  | i64    | 8     | Virtual time of next access        |
//!
//! Two optional extension columns (`op`, `ttl`) support the richer
//! `oracleGeneralOpNs` format and workloads with mixed operations.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
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
    /// Real time (nanoseconds since epoch).
    pub timestamp: u64,
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
        Field::new("timestamp", DataType::UInt64, false),
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

            let timestamp = col_u64(batch, "timestamp").map(|a| a.value(i)).unwrap_or(0);
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
    timestamps: Vec<u64>,
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
            Arc::new(UInt64Array::from(std::mem::take(&mut self.timestamps))),
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

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Parse a 24-byte oracleGeneral record (little-endian, packed).
/// The on-wire timestamp is u32 seconds; we widen to u64 nanoseconds.
fn parse_oracle_general(buf: &[u8]) -> TraceEntry {
    let secs = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    TraceEntry {
        timestamp: secs as u64 * NANOS_PER_SEC,
        obj_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        obj_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        next_access_vtime: i64::from_le_bytes(buf[16..24].try_into().unwrap()),
        op: None,
        ttl: None,
    }
}

/// Parse a 27-byte oracleGeneralOpNs record (little-endian, packed).
/// The on-wire timestamp is u32 seconds; we widen to u64 nanoseconds.
fn parse_oracle_general_opns(buf: &[u8]) -> TraceEntry {
    let secs = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    TraceEntry {
        timestamp: secs as u64 * NANOS_PER_SEC,
        obj_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        obj_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        op: Some(buf[16]),
        // bytes 17..19 = u16 namespace (not stored as a separate column)
        next_access_vtime: i64::from_le_bytes(buf[19..27].try_into().unwrap()),
        ttl: None,
    }
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
        // Binary parser widens u32 seconds → u64 nanoseconds
        assert_eq!(parsed.timestamp, ts_secs as u64 * NANOS_PER_SEC);
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

        // Read
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
}
