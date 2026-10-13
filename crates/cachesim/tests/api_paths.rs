//! The trace items moved to the `cachetrace` crate stay reachable at their
//! `cachesim::trace` paths, and their errors convert into `cachesim::Error`
//! with `?`. This file compiling is most of the test.

use cachesim::oracle::OraclePolicy;
use cachesim::simulator::{simulate_oracle, SimResult};
use cachesim::trace::{
    convert_bin_to_parquet, convert_cache_trace_to_parquet, trace_schema, BinFormat, Op,
    TraceEntry, TraceReader, TraceWriter,
};
use cachesim::Error;

fn write_and_simulate(dir: &std::path::Path) -> Result<SimResult, Error> {
    let path = dir.join("t.parquet");
    let mut w = TraceWriter::create(&path, 16)?;
    for i in 0..4u64 {
        w.write(&TraceEntry {
            timestamp: i as i64,
            obj_id: i % 2,
            obj_size: 10,
            next_access_vtime: if i < 2 { i as i64 + 2 } else { -1 },
            op: Some(Op::Get as u8),
            ttl: None,
        })?;
    }
    w.finish()?;
    let rows: Vec<TraceEntry> = TraceReader::open(&path)?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 4);
    simulate_oracle(&path, 100, OraclePolicy::Belady)
}

#[test]
fn trace_items_keep_their_cachesim_paths() {
    let dir = tempfile::tempdir().unwrap();
    let r = write_and_simulate(dir.path()).unwrap();
    assert_eq!((r.hits, r.misses), (2, 2));
    assert_eq!(trace_schema().fields().len(), 6);
    assert_eq!(BinFormat::OracleGeneral.record_size(), 24);

    // Importer errors convert to the cachesim variant of the same name.
    let bin = dir.path().join("short.bin");
    std::fs::write(&bin, [0u8; 5]).unwrap();
    let out = dir.path().join("o.parquet");
    let err: Error = convert_bin_to_parquet(&bin, &out, BinFormat::OracleGeneral, 16)
        .unwrap_err()
        .into();
    assert!(
        matches!(&err, Error::InvalidFormat(m) if m.contains("multiple of record size")),
        "{err:?}"
    );
    let err: Error = convert_cache_trace_to_parquet(dir.path().join("missing.csv"), &out, 16)
        .unwrap_err()
        .into();
    assert!(
        matches!(&err, Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
        "{err:?}"
    );
}

#[test]
fn writer_reader_and_op_items_keep_their_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("synced.parquet");
    let mut w = TraceWriter::create(&path, 2).unwrap();
    w.append_key_value_metadata("api.key", "api-value");
    for i in 0..3u64 {
        w.write(&TraceEntry {
            timestamp: i as i64,
            obj_id: i,
            obj_size: 1,
            next_access_vtime: -1,
            op: Some(Op::Set as u8),
            ttl: Some(5),
        })
        .unwrap();
    }
    w.finish_synced().unwrap();
    let r = TraceReader::open(&path).unwrap();
    assert_eq!(r.total_entries(), 3);
    let rows: Result<Vec<TraceEntry>, cachesim::trace::Error> = r.collect();
    let rows = rows.unwrap();
    assert_eq!(rows[2].obj_id, 2);
    assert_eq!(rows[0].ttl, Some(5));
    let file = std::fs::File::open(&path).unwrap();
    let meta = parquet::file::reader::FileReader::metadata(
        &parquet::file::reader::SerializedFileReader::new(file).unwrap(),
    )
    .file_metadata()
    .key_value_metadata()
    .unwrap()
    .clone();
    assert!(meta
        .iter()
        .any(|kv| kv.key == "api.key" && kv.value.as_deref() == Some("api-value")));

    assert_eq!(Op::from_u8(3), Op::Set);
    assert_eq!(Op::from_u8(200), Op::Invalid);
    assert!(Op::from_u8(1).is_read() && !Op::Get.is_write() && !Op::Get.is_delete());
    assert!(Op::Set.is_write() && Op::Delete.is_delete());
    assert_eq!(BinFormat::OracleGeneralOpNs.record_size(), 27);

    // A 27-byte oracleGeneralOpNs record through the old path.
    let bin = dir.path().join("opns.bin");
    let mut rec = Vec::new();
    rec.extend_from_slice(&7u32.to_le_bytes());
    rec.extend_from_slice(&42u64.to_le_bytes());
    rec.extend_from_slice(&100u32.to_le_bytes());
    rec.push(Op::Delete as u8);
    rec.extend_from_slice(&0u16.to_le_bytes());
    rec.extend_from_slice(&(-1i64).to_le_bytes());
    std::fs::write(&bin, &rec).unwrap();
    let out = dir.path().join("opns.parquet");
    assert_eq!(
        convert_bin_to_parquet(&bin, &out, BinFormat::OracleGeneralOpNs, 4).unwrap(),
        1
    );
    let row = TraceReader::open(&out).unwrap().next().unwrap().unwrap();
    assert_eq!((row.obj_id, row.op), (42, Some(Op::Delete as u8)));
}

/// Every `cachesim::trace::Error` variant converts to the `cachesim::Error`
/// variant of the same name, keeping its source error or message.
#[test]
fn every_trace_error_variant_converts() {
    use cachesim::trace::Error as TraceError;
    let io: Error = TraceError::Io(std::io::Error::other("io-msg")).into();
    assert!(
        matches!(&io, Error::Io(e) if e.to_string() == "io-msg"),
        "{io:?}"
    );
    let pq: Error =
        TraceError::Parquet(parquet::errors::ParquetError::General("pq-msg".into())).into();
    assert!(
        matches!(&pq, Error::Parquet(e) if e.to_string().contains("pq-msg")),
        "{pq:?}"
    );
    let ar: Error =
        TraceError::Arrow(arrow::error::ArrowError::ComputeError("ar-msg".into())).into();
    assert!(
        matches!(&ar, Error::Arrow(e) if e.to_string().contains("ar-msg")),
        "{ar:?}"
    );
    let inv: Error = TraceError::InvalidFormat("bad".into()).into();
    assert!(
        matches!(&inv, Error::InvalidFormat(m) if m == "bad"),
        "{inv:?}"
    );
}
