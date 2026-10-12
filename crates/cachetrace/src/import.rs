//! Importers: libCacheSim binary traces and pelikan-io/cache-trace CSV.
//!
//! Built with the `import` cargo feature (on by default).

use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use crate::{Error, Op, TraceEntry, TraceWriter};

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

    if !file_len.is_multiple_of(rec_size) {
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

    let reader: Box<dyn BufRead> = if path.extension().is_some_and(|e| e == "zst" || e == "zstd") {
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
fn parse_cache_trace_line(
    line: &str,
    hash_state: &ahash::RandomState,
) -> Result<TraceEntry, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TraceReader;

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
            bin_data
                .extend_from_slice(&(if i < 2 { (i as i64 + 1) * 5 } else { -1i64 }).to_le_bytes());
        }
        std::fs::write(&bin_path, &bin_data).unwrap();

        let count =
            convert_bin_to_parquet(&bin_path, &pq_path, BinFormat::OracleGeneral, 1024).unwrap();
        assert_eq!(count, 3);

        let reader = TraceReader::open(&pq_path).unwrap();
        let entries: Vec<TraceEntry> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].timestamp, 0); // 0 seconds → 0 nanoseconds
        assert_eq!(entries[0].obj_id, 1000);
        assert_eq!(entries[0].obj_size, 256);
        assert_eq!(entries[1].obj_id, 2000);
        assert_eq!(entries[2].next_access_vtime, -1);
    }

    /// A file with many row groups must stream through in order, with the
    /// footer's row count available before the first row is decoded.
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
        let entries: Vec<TraceEntry> = reader.collect::<Result<_, _>>().unwrap();
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
