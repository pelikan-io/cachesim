//! Importers: libCacheSim binary traces, pelikan-io/cache-trace CSV and
//! Meta's key-value cache trace CSV.
//!
//! Built with the `import` cargo feature (on by default).

use std::collections::HashMap;
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
    let reader = open_text(input.as_ref())?;

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

    let timestamp = timestamp_secs
        .checked_mul(NANOS_PER_SEC)
        .ok_or_else(|| bad_csv(&format!("timestamp '{ts_str}' is out of range")))?;

    Ok(TraceEntry {
        timestamp,
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
// Meta key-value cache trace CSV import
// ---------------------------------------------------------------------------

/// The header of Meta's 202401 key-value cache traces.
const META_KV_HEADER: &str =
    "op_time,key,key_size,op,op_count,size,cache_hits,ttl,usecase,sub_usecase";

/// Convert a Meta key-value cache trace (the 202401 release, CacheLib's
/// `kvcache_traces_*.csv`) to Parquet.
///
/// CSV columns, with a header line:
/// `op_time,key,key_size,op,op_count,size,cache_hits,ttl,usecase,sub_usecase`
///
/// * A row stands for `op_count` requests to one key in one second; it is
///   written as `op_count` entries with the same timestamp.
/// * `op_time` (seconds) is widened to nanoseconds; a negative value or one
///   past i64 nanoseconds is an error.
/// * `key` is a 16-digit hex hash and is used as `obj_id`; a key that is
///   not hex is hashed via ahash (fixed seed).
/// * `GET` and `GET_LEASE` become [`Op::Get`], `SET` and `SET_LEASE`
///   [`Op::Set`], `DELETE` [`Op::Delete`]; anything else [`Op::Invalid`].
/// * `obj_size` = `key_size + size`. A GET miss and every DELETE record
///   `size` 0; such a row takes the key's size from its most recent row
///   with a size, or, if it has none yet, from the first one after it. A
///   key that never has a size keeps 0.
/// * `ttl` 0 becomes null. `cache_hits`, `usecase` and `sub_usecase` are
///   dropped; `next_access_vtime` is `-1`.
/// * Files with a `.zst` extension are decompressed automatically.
///
/// The input is read twice: once for each key's first size, once to write.
/// It must therefore be a regular file, not a pipe. The size map holds one
/// entry per key that has a size.
///
/// Returns the number of entries written.
pub fn convert_meta_kv_to_parquet(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    batch_size: usize,
) -> Result<usize, Error> {
    let path = input.as_ref();
    if !std::fs::metadata(path)?.is_file() {
        return Err(bad_meta_kv(
            "input must be a regular file (it is read twice)",
        ));
    }
    let hash_state = ahash::RandomState::with_seeds(0, 0, 0, 0);

    let mut sizes: HashMap<u64, u32, ahash::RandomState> =
        HashMap::with_hasher(ahash::RandomState::with_seeds(0, 0, 0, 0));
    for_each_meta_kv_row(path, &hash_state, |row| {
        if row.value_size > 0 {
            sizes.entry(row.obj_id).or_insert(row.value_size);
        }
        Ok(())
    })?;

    let mut writer = TraceWriter::create(output, batch_size)?;
    let mut count: usize = 0;
    for_each_meta_kv_row(path, &hash_state, |row| {
        let value_size = if row.value_size > 0 {
            sizes.insert(row.obj_id, row.value_size);
            row.value_size
        } else {
            sizes.get(&row.obj_id).copied().unwrap_or(0)
        };
        let entry = TraceEntry {
            timestamp: row.timestamp_ns,
            obj_id: row.obj_id,
            obj_size: row.key_size.saturating_add(value_size),
            next_access_vtime: -1,
            op: Some(row.op as u8),
            ttl: row.ttl,
        };
        for _ in 0..row.op_count {
            writer.write(&entry)?;
        }
        count += row.op_count as usize;
        Ok(())
    })?;

    writer.finish()?;
    Ok(count)
}

/// One row of a Meta key-value cache trace.
#[derive(Debug)]
struct MetaKvRow {
    timestamp_ns: i64,
    obj_id: u64,
    key_size: u32,
    op: Op,
    op_count: u32,
    value_size: u32,
    ttl: Option<i32>,
}

/// Call `f` with each row of the Meta trace at `path`, after checking its
/// header.
fn for_each_meta_kv_row(
    path: &Path,
    hash_state: &ahash::RandomState,
    mut f: impl FnMut(MetaKvRow) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut lines = open_text(path)?.lines();
    let header = lines.next().transpose()?.unwrap_or_default();
    if header.trim_end() != META_KV_HEADER {
        return Err(bad_meta_kv(&format!(
            "header is '{}', expected '{META_KV_HEADER}' (the 202401 release)",
            header.trim_end()
        )));
    }
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        f(parse_meta_kv_line(&line, hash_state)?)?;
    }
    Ok(())
}

/// A text file, decompressed if its extension is `.zst` or `.zstd`.
fn open_text(path: &Path) -> Result<Box<dyn BufRead>, Error> {
    let file = File::open(path)?;
    Ok(
        if path.extension().is_some_and(|e| e == "zst" || e == "zstd") {
            Box::new(BufReader::new(zstd::stream::read::Decoder::new(file)?))
        } else {
            Box::new(BufReader::new(file))
        },
    )
}

/// Parse one data line of a Meta key-value cache trace.
fn parse_meta_kv_line(line: &str, hash_state: &ahash::RandomState) -> Result<MetaKvRow, Error> {
    let mut fields = line.split(',');
    let mut next = |name: &str| {
        fields
            .next()
            .ok_or_else(|| bad_meta_kv(&format!("missing {name} in '{line}'")))
    };
    let ts_str = next("op_time")?;
    let key = next("key")?;
    let ks_str = next("key_size")?;
    let op_str = next("op")?;
    let count_str = next("op_count")?;
    let size_str = next("size")?;
    let _hits = next("cache_hits")?;
    let ttl_str = next("ttl")?;

    fn num<T: std::str::FromStr>(name: &str, s: &str) -> Result<T, Error>
    where
        T::Err: std::fmt::Display,
    {
        s.parse()
            .map_err(|e| bad_meta_kv(&format!("bad {name} '{s}': {e}")))
    }

    let obj_id = match u64::from_str_radix(key, 16) {
        Ok(id) if key.len() == 16 => id,
        _ => {
            let mut hasher = hash_state.build_hasher();
            hasher.write(key.as_bytes());
            hasher.finish()
        }
    };
    let op = match op_str {
        "GET" | "GET_LEASE" => Op::Get,
        "SET" | "SET_LEASE" => Op::Set,
        "DELETE" => Op::Delete,
        _ => Op::Invalid,
    };
    let ttl: i32 = num("ttl", ttl_str)?;
    let secs: i64 = num("op_time", ts_str)?;
    let timestamp_ns = (secs >= 0)
        .then(|| secs.checked_mul(NANOS_PER_SEC))
        .flatten()
        .ok_or_else(|| bad_meta_kv(&format!("op_time '{ts_str}' is negative or too large")))?;

    Ok(MetaKvRow {
        timestamp_ns,
        obj_id,
        key_size: num("key_size", ks_str)?,
        op,
        op_count: num("op_count", count_str)?,
        value_size: num("size", size_str)?,
        ttl: if ttl > 0 { Some(ttl) } else { None },
    })
}

fn bad_meta_kv(msg: &str) -> Error {
    Error::InvalidFormat(format!("Meta key-value CSV: {msg}"))
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
    fn parse_cache_trace_rejects_a_timestamp_past_i64_nanoseconds() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        for t in ["99999999999", "-99999999999"] {
            let err = parse_cache_trace_line(&format!("{t},k,1,1,c,get,0"), &state).unwrap_err();
            assert!(err.to_string().contains("out of range"), "{t}: {err}");
        }
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

    #[test]
    fn parse_meta_kv_line_fields() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let row = parse_meta_kv_line(
            "604038471,82131353f9ddc8c6,48,GET_LEASE,3,87,1,600,366387042,3003229276",
            &state,
        )
        .unwrap();
        assert_eq!(row.timestamp_ns, 604_038_471 * NANOS_PER_SEC);
        assert_eq!(row.obj_id, 0x8213_1353_f9dd_c8c6);
        assert_eq!(row.key_size, 48);
        assert_eq!(row.op, Op::Get);
        assert_eq!(row.op_count, 3);
        assert_eq!(row.value_size, 87);
        assert_eq!(row.ttl, Some(600));
    }

    #[test]
    fn meta_kv_rejects_out_of_range_times() {
        let state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        for t in ["-1", "99999999999"] {
            let line = format!("{t},000000000000000a,8,GET,1,0,0,0,1,1");
            let err = parse_meta_kv_line(&line, &state).unwrap_err();
            assert!(err.to_string().contains("op_time"), "{t}: {err}");
        }
    }

    #[test]
    fn meta_kv_rejects_an_input_that_is_not_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t.parquet");
        let err = convert_meta_kv_to_parquet(dir.path(), &out, 1024).unwrap_err();
        assert!(err.to_string().contains("regular file"), "{err}");
        assert!(!out.exists(), "no output is made");
    }

    #[test]
    fn meta_kv_rejects_another_header() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("trace.csv");
        std::fs::write(&csv_path, "key,op,size,op_count,key_size\nabc,GET,0,1,3\n").unwrap();
        let err =
            convert_meta_kv_to_parquet(&csv_path, dir.path().join("t.parquet"), 1024).unwrap_err();
        assert!(err.to_string().contains("202401"), "{err}");
    }

    #[test]
    fn meta_kv_to_parquet_expands_counts_and_fills_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("trace.csv");
        let pq_path = dir.path().join("trace.parquet");

        // Key a: a miss before any size, then a SET of 100, a miss, a SET
        // of 200 and a miss. Key b never has a size. Key c is not hex.
        let csv_data = "\
op_time,key,key_size,op,op_count,size,cache_hits,ttl,usecase,sub_usecase
10,000000000000000a,8,GET_LEASE,2,0,0,0,1,1
11,000000000000000a,8,SET_LEASE,1,100,0,3600,1,1
12,000000000000000a,8,GET,1,0,0,0,1,1
13,000000000000000a,8,SET,1,200,0,0,1,1
13,000000000000000b,8,DELETE,1,0,0,0,1,1
14,000000000000000a,8,GET,3,0,0,0,1,1
14,c,1,GET,1,5,1,0,1,1
";
        std::fs::write(&csv_path, csv_data).unwrap();

        let count = convert_meta_kv_to_parquet(&csv_path, &pq_path, 1024).unwrap();
        assert_eq!(count, 10);

        let entries: Vec<TraceEntry> = TraceReader::open(&pq_path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        // (seconds, obj_id, obj_size, op, ttl)
        type Row = (i64, u64, u32, Option<u8>, Option<i32>);
        let got: Vec<Row> = entries
            .iter()
            .map(|e| {
                (
                    e.timestamp / NANOS_PER_SEC,
                    e.obj_id,
                    e.obj_size,
                    e.op,
                    e.ttl,
                )
            })
            .collect();
        let (get, set, del) = (
            Some(Op::Get as u8),
            Some(Op::Set as u8),
            Some(Op::Delete as u8),
        );
        let c = {
            let mut h = ahash::RandomState::with_seeds(0, 0, 0, 0).build_hasher();
            h.write(b"c");
            h.finish()
        };
        assert_eq!(
            got,
            vec![
                (10, 0xa, 108, get, None),
                (10, 0xa, 108, get, None),
                (11, 0xa, 108, set, Some(3600)),
                (12, 0xa, 108, get, None),
                (13, 0xa, 208, set, None),
                (13, 0xb, 8, del, None),
                (14, 0xa, 208, get, None),
                (14, 0xa, 208, get, None),
                (14, 0xa, 208, get, None),
                (14, c, 6, get, None),
            ]
        );
        assert!(entries.iter().all(|e| e.next_access_vtime == -1));
    }
}
