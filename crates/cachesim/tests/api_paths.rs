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
