//! KV-cache request traces in the simulator.
//!
//! [`expand_to_parquet`] turns a request trace ([`kvtrace::format`]) into an
//! object-level trace ([`cachetrace`]) for one model geometry, so the
//! object-level engines can replay it. A simulation mode that replays request
//! traces directly (prefix lookups per request) belongs in this module.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use cachetrace::{Op, TraceEntry, TraceWriter};
use kvtrace::output::plan_output;
use kvtrace::{Geometry, ObjectMapper, RequestReader};

use crate::Error;

/// Version of the `kvtrace.*` metadata on an object trace made by
/// [`expand_to_parquet`]. Version 1 was written by the direct Mooncake
/// conversion, which also emitted partial chunks.
pub const OBJECT_METADATA_VERSION: &str = "2";

/// Result of [`expand_to_parquet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandSummary {
    /// Object rows written.
    pub rows: usize,
    /// Requests read.
    pub requests: usize,
    /// Distinct object ids.
    pub objects: usize,
}

/// Expand a request trace into an object-level trace.
///
/// Each request becomes the rows of its objects ([`ObjectMapper::objects`]),
/// in request order and, within a request, in prefix order. Every row is a
/// GET (`op = 1`) at the request's timestamp with a null `ttl`; a serving
/// engine reads each prefix chunk and stores it on a miss, so writes in a
/// replay come from the replayer's fill on miss. `next_access_vtime` is the
/// row index of the next row with the same `obj_id`, or -1.
///
/// The input is read twice: once for the object ids, from which a reverse
/// scan computes `next_access_vtime` in place (8 bytes per row), and once to
/// write the rows. Its SHA-256 must match before and after. An object larger
/// than `u32::MAX` bytes is an error. The output is written as described on
/// [`plan_output`].
///
/// Provenance goes to the footer as `kvtrace.*` metadata: the request
/// trace's `kvtrace.source.*`, `kvtrace.converter.*` and chunk keys, the
/// request trace's file name and SHA-256, and the geometry.
pub fn expand_to_parquet(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    geometry: &Geometry,
    batch_size: usize,
) -> Result<ExpandSummary, Error> {
    let input = input.as_ref();
    let output = output.as_ref();
    let reader = RequestReader::open(input)?;
    let chunk_tokens = reader.chunk_tokens();
    let mapper = ObjectMapper::new(geometry, u64::from(chunk_tokens))?;
    let max = mapper.max_object_size();
    if max > u64::from(u32::MAX) {
        return Err(Error::InvalidFormat(format!(
            "kv expand: an object of {max} bytes exceeds the obj_size column's u32 range"
        )));
    }
    let out = plan_output(input, &std::fs::metadata(input)?, output)?;
    let input_sha256 = kvtrace::sha256_file(input)?;
    let request_meta = reader.metadata().clone();
    let created = kvtrace::utc_now_iso8601();

    // Pass 1: object ids, overwritten by a reverse scan with the row index of
    // each id's next access. -1 (no next access) is stored as u64::MAX.
    let mut next_access: Vec<u64> = Vec::new();
    let mut requests = 0usize;
    for req in reader {
        let req = req?;
        requests += 1;
        next_access.extend(mapper.objects(&req.chunks)?.map(|o| o.obj_id));
    }
    let mut seen: HashMap<u64, u64> = HashMap::new();
    for i in (0..next_access.len()).rev() {
        let id = next_access[i];
        next_access[i] = seen.insert(id, i as u64).unwrap_or(u64::MAX);
    }
    let objects = seen.len();
    drop(seen);
    let rows = next_access.len();

    let metadata = object_metadata(
        &request_meta,
        input,
        &input_sha256,
        created,
        geometry,
        &mapper,
        requests,
    );

    // Pass 2: regenerate the rows and stream them out.
    let changed = || Error::InvalidFormat("kv expand: input changed between the two passes".into());
    let write = || -> Result<(), Error> {
        let mut writer = TraceWriter::create(out.target(), batch_size)?;
        for (k, v) in &metadata {
            writer.append_key_value_metadata(k.clone(), v.clone());
        }
        let mut row = 0usize;
        for req in RequestReader::open(input)? {
            let req = req?;
            for o in mapper.objects(&req.chunks)? {
                let next = *next_access.get(row).ok_or_else(changed)?;
                writer.write(&TraceEntry {
                    timestamp: req.timestamp,
                    obj_id: o.obj_id,
                    // Checked against max_object_size() above.
                    obj_size: o.size as u32,
                    next_access_vtime: next as i64,
                    op: Some(Op::Get as u8),
                    ttl: None,
                })?;
                row += 1;
            }
        }
        if row != rows || kvtrace::sha256_file(input)? != input_sha256 {
            return Err(changed());
        }
        if out.temp.is_some() {
            writer.finish_synced()?;
        } else {
            writer.finish()?;
        }
        Ok(())
    };
    if let Err(e) = write().and_then(|()| out.commit().map_err(Error::from)) {
        out.discard();
        return Err(e);
    }
    Ok(ExpandSummary {
        rows,
        requests,
        objects,
    })
}

/// Footer metadata of an expanded trace.
fn object_metadata(
    request_meta: &BTreeMap<String, String>,
    input: &Path,
    input_sha256: &str,
    created: String,
    geometry: &Geometry,
    mapper: &ObjectMapper,
    requests: usize,
) -> BTreeMap<String, String> {
    // The request trace's provenance, minus the keys that describe that file.
    let mut m: BTreeMap<String, String> = request_meta
        .iter()
        .filter(|(k, _)| {
            k.starts_with("kvtrace.")
                && !matches!(
                    k.as_str(),
                    "kvtrace.format" | "kvtrace.version" | "kvtrace.created"
                )
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let sizes = &geometry.sizes;
    let shard = mapper.sharding();
    let state = mapper.has_state();
    let opt_count = |v: Option<u64>| v.map_or("None".to_string(), |n| n.to_string());
    let none_without_state = |s: &str| if state { s } else { "none" }.to_string();
    let mut set = |k: &str, v: String| {
        m.insert(k.to_string(), v);
    };
    set("kvtrace.format", "objects".into());
    set("kvtrace.version", OBJECT_METADATA_VERSION.into());
    set("kvtrace.created", created);
    set(
        "kvtrace.request_trace.file",
        input
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );
    set("kvtrace.request_trace.sha256", input_sha256.to_string());
    set(
        "kvtrace.request_trace.version",
        request_meta
            .get("kvtrace.version")
            .cloned()
            .unwrap_or_default(),
    );
    set("kvtrace.requests", requests.to_string());
    set(
        "kvtrace.model",
        geometry.model_name.clone().unwrap_or_default(),
    );
    if let Some(rev) = &geometry.model_revision {
        set("kvtrace.model.revision", rev.clone());
    }
    if let Some(sha) = &geometry.model_config_sha256 {
        set("kvtrace.model.config_sha256", sha.clone());
    }
    set(
        "kvtrace.kv_bytes_per_token",
        sizes.kv_bytes_per_token.to_string(),
    );
    set(
        "kvtrace.state_bytes_per_sequence",
        sizes.state_bytes_per_sequence.to_string(),
    );
    set(
        "kvtrace.full_attention_layers",
        opt_count(sizes.full_attention_layers),
    );
    set(
        "kvtrace.linear_attention_layers",
        opt_count(sizes.linear_attention_layers),
    );
    set("kvtrace.tp", geometry.tp.to_string());
    set("kvtrace.kv_tp_divisor", shard.kv_div.to_string());
    set(
        "kvtrace.state_tp_divisor",
        none_without_state(&shard.state_div.to_string()),
    );
    set(
        "kvtrace.state_mode",
        none_without_state(geometry.state_mode.as_str()),
    );
    set(
        "kvtrace.state_layout",
        none_without_state(geometry.state_layout.as_str()),
    );
    set(
        "kvtrace.mla_first_rank_only",
        if sizes.mla { "True" } else { "False" }.to_string(),
    );
    set(
        "kvtrace.ops",
        "every row is GET; SETs come from the replayer's backfill on miss".into(),
    );
    set("kvtrace.obj_id_layout", kvtrace::objects::obj_id_layout());
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use cachetrace::TraceReader;
    use kvtrace::mooncake::{chunk_id, convert_mooncake, MooncakeOptions};
    use kvtrace::{obj_id, ModelSizes, ObjectKind, StateLayout, StateMode};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    /// Copies of kvtrace's fixtures, so the packaged crate's tests have them.
    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    /// The fixture Mooncake trace converted to a request trace.
    fn request_trace(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("requests.parquet");
        convert_mooncake(
            fixture("mooncake/trace.jsonl"),
            &path,
            &MooncakeOptions::default(),
            2,
        )
        .unwrap();
        path
    }

    fn expand(g: &Geometry) -> (ExpandSummary, Vec<TraceEntry>, BTreeMap<String, String>) {
        let dir = tempfile::tempdir().unwrap();
        let input = request_trace(dir.path());
        let out = dir.path().join("objects.parquet");
        // Batch size 4 so the output spans several record batches.
        let summary = expand_to_parquet(&input, &out, g, 4).unwrap();
        let rows: Vec<TraceEntry> = TraceReader::open(&out)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let reader = SerializedFileReader::new(std::fs::File::open(&out).unwrap()).unwrap();
        let meta = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .filter(|kv| kv.key.starts_with("kvtrace."))
            .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
            .collect();
        (summary, rows, meta)
    }

    /// Object id of sub-chunk `sub` of Mooncake block `block`.
    fn id(block: u64, sub: u64, kind: ObjectKind, rank: u64) -> u64 {
        obj_id(chunk_id(block, sub).unwrap(), kind, rank).unwrap()
    }

    #[test]
    fn dense() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 0));
        g.model_name = Some("test/dense".into());
        let (summary, rows, meta) = expand(&g);
        use ObjectKind::Kv;
        let expect: &[(i64, u64, u32, i64)] = &[
            // request 0, t = 0 ms, 700 tokens: [0, 1]; block 1 has no full chunk.
            (0, id(0, 0, Kv, 0), 2560, 2),
            (0, id(0, 1, Kv, 0), 2560, 3),
            // request 1, t = 3 ms, 1100 tokens: [0, 1, 2]
            (3, id(0, 0, Kv, 0), 2560, -1),
            (3, id(0, 1, Kv, 0), 2560, -1),
            (3, id(1, 0, Kv, 0), 2560, -1),
            (3, id(1, 1, Kv, 0), 2560, -1),
            // request 2, t = 5 ms, 2000 tokens: [3]
            (5, id(3, 0, Kv, 0), 2560, -1),
            (5, id(3, 1, Kv, 0), 2560, -1),
            // request 3, t = 7 ms, 300 tokens: [4, 5]
            (7, id(4, 0, Kv, 0), 2560, -1),
            (7, id(4, 1, Kv, 0), 2560, -1),
        ];
        assert_eq!(rows.len(), expect.len());
        for (i, (row, &(ms, oid, size, next))) in rows.iter().zip(expect).enumerate() {
            assert_eq!(row.timestamp, ms * 1_000_000, "row {i}");
            assert_eq!(row.obj_id, oid, "row {i}");
            assert_eq!(row.obj_size, size, "row {i}");
            assert_eq!(row.next_access_vtime, next, "row {i}");
            assert_eq!(row.op, Some(Op::Get as u8), "row {i}");
            assert_eq!(row.ttl, None, "row {i}");
        }
        assert_eq!(
            summary,
            ExpandSummary {
                rows: 10,
                requests: 4,
                objects: 8
            }
        );

        for (k, v) in [
            ("kvtrace.format", "objects"),
            ("kvtrace.version", "2"),
            ("kvtrace.request_trace.file", "requests.parquet"),
            ("kvtrace.request_trace.version", "1"),
            ("kvtrace.source.format", "mooncake"),
            ("kvtrace.source.file", "trace.jsonl"),
            ("kvtrace.source.block_tokens", "512"),
            ("kvtrace.converter.input_beyond_blocks", "1"),
            ("kvtrace.converter.input_short_of_blocks", "1"),
            ("kvtrace.model", "test/dense"),
            ("kvtrace.kv_bytes_per_token", "10"),
            ("kvtrace.state_bytes_per_sequence", "0"),
            ("kvtrace.full_attention_layers", "None"),
            ("kvtrace.chunk_tokens", "256"),
            ("kvtrace.tp", "1"),
            ("kvtrace.kv_tp_divisor", "1"),
            ("kvtrace.state_tp_divisor", "none"),
            ("kvtrace.state_mode", "none"),
            ("kvtrace.state_layout", "none"),
            ("kvtrace.mla_first_rank_only", "False"),
            ("kvtrace.requests", "4"),
        ] {
            assert_eq!(meta.get(k).map(String::as_str), Some(v), "{k}");
        }
        let source_sha =
            kvtrace::sha256_hex(&std::fs::read(fixture("mooncake/trace.jsonl")).unwrap());
        assert_eq!(meta["kvtrace.source.sha256"], source_sha);
        assert_eq!(meta["kvtrace.request_trace.sha256"].len(), 64);
        assert!(meta["kvtrace.obj_id_layout"].starts_with("chunk<<16"));
    }

    #[test]
    fn tp_and_mla() {
        let mut g = Geometry::new(ModelSizes::explicit(12, 0));
        g.tp = 4;
        let (summary, rows, meta) = expand(&g);
        // 10 chunks x 4 ranks, each 256 x 12 / 4 bytes, rank in the low bits.
        assert_eq!(summary.rows, 40);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.obj_size, 768);
            assert_eq!(kvtrace::obj_id_fields(r.obj_id).2, (i % 4) as u64);
        }
        // Rank 2 of request 0's first chunk is next read at row 8 + 2.
        assert_eq!(rows[2].next_access_vtime, 10);
        assert_eq!(meta["kvtrace.kv_tp_divisor"], "4");

        g.sizes.mla = true;
        let (summary, rows, meta) = expand(&g);
        assert_eq!(summary.rows, 10);
        assert!(rows.iter().all(|r| r.obj_size == 3072));
        assert!(rows.iter().all(|r| kvtrace::obj_id_fields(r.obj_id).2 == 0));
        assert_eq!(meta["kvtrace.mla_first_rank_only"], "True");

        // DeepSeek-V3's published config at tp 8: one first-rank object per
        // chunk of 256 x 61 x (512 + 64) x 2 bytes.
        let sizes = ModelSizes::from_config_file(fixture("models/deepseek-v3.json"), None).unwrap();
        let mut g = Geometry::new(sizes);
        g.tp = 8;
        let (summary, rows, meta) = expand(&g);
        assert_eq!(summary.rows, 10);
        assert!(rows.iter().all(|r| r.obj_size == 256 * 70_272));
        assert!(rows.iter().all(|r| kvtrace::obj_id_fields(r.obj_id).2 == 0));
        assert_eq!(meta["kvtrace.kv_tp_divisor"], "1");
        assert_eq!(meta["kvtrace.mla_first_rank_only"], "True");
    }

    #[test]
    fn hybrid_tp() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
        g.tp = 2;
        let (summary, rows, meta) = expand(&g);
        use ObjectKind::{Kv, KvWithState};
        let mut expect: Vec<(u64, u32)> = Vec::new();
        let mut both = |b, s, kind, size| expect.extend((0..2).map(|r| (id(b, s, kind, r), size)));
        // The last full chunk of each request carries the state:
        // (2560 + 1000) / 2 = 1780 per rank.
        both(0, 0, Kv, 1280);
        both(0, 1, KvWithState, 1780);
        both(0, 0, Kv, 1280);
        both(0, 1, Kv, 1280); // a different object from the kind-2 one above
        both(1, 0, Kv, 1280);
        both(1, 1, KvWithState, 1780);
        both(3, 0, Kv, 1280);
        both(3, 1, KvWithState, 1780);
        both(4, 0, Kv, 1280);
        both(4, 1, KvWithState, 1780);
        let got: Vec<(u64, u32)> = rows.iter().map(|r| (r.obj_id, r.obj_size)).collect();
        assert_eq!(got, expect);
        let next: Vec<i64> = rows.iter().map(|r| r.next_access_vtime).collect();
        let mut want = vec![-1i64; 20];
        want[..2].copy_from_slice(&[4, 5]);
        assert_eq!(next, want);
        assert_eq!(summary.objects, 18);
        assert_eq!(meta["kvtrace.state_mode"], "per-request");
        assert_eq!(meta["kvtrace.state_layout"], "combined");
        assert_eq!(meta["kvtrace.state_tp_divisor"], "2");

        // Separate per-chunk state: each chunk is KV rank 0, 1, state rank 0, 1.
        g.state_layout = StateLayout::Separate;
        g.state_mode = StateMode::PerChunk;
        let (_, rows, _) = expand(&g);
        assert_eq!(rows.len(), 40);
        for chunk in rows.chunks(4) {
            let fields: Vec<(u64, u64, u32)> = chunk
                .iter()
                .map(|r| {
                    let (_, kind, rank) = kvtrace::obj_id_fields(r.obj_id);
                    (kind, rank, r.obj_size)
                })
                .collect();
            assert_eq!(
                fields,
                [(0, 0, 1280), (0, 1, 1280), (1, 0, 500), (1, 1, 500)]
            );
        }
    }

    /// An object read three times: each row points at the next read, not the
    /// last one.
    #[test]
    fn next_access_is_the_nearest_later_row() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("req.parquet");
        let mut w = kvtrace::RequestWriter::create(&input, 4, 2).unwrap();
        for (i, chunks) in [vec![1], vec![1, 2], vec![2, 1], vec![1]]
            .into_iter()
            .enumerate()
        {
            w.write(&kvtrace::Request {
                timestamp: i as i64,
                req_id: i as u64,
                input_tokens: 4 * chunks.len() as u32,
                output_tokens: 0,
                chunks,
                session_id: None,
            })
            .unwrap();
        }
        w.finish().unwrap();
        let out = dir.path().join("out.parquet");
        let g = Geometry::new(ModelSizes::explicit(1, 0));
        let s = expand_to_parquet(&input, &out, &g, 4).unwrap();
        assert_eq!((s.rows, s.requests, s.objects), (6, 4, 2));
        let rows: Vec<(u64, i64)> = TraceReader::open(&out)
            .unwrap()
            .map(|r| r.map(|r| (kvtrace::obj_id_fields(r.obj_id).0, r.next_access_vtime)))
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows, [(1, 1), (1, 4), (2, 3), (2, -1), (1, 5), (1, -1)]);
    }

    #[test]
    fn oversized_objects_and_bad_outputs_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let input = request_trace(dir.path());
        let out = dir.path().join("out.parquet");
        // 2^24 bytes per token x 256 tokens = 2^32 bytes.
        let g = Geometry::new(ModelSizes::explicit(1 << 24, 0));
        let err = expand_to_parquet(&input, &out, &g, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("u32"), "{err}");
        assert!(!out.exists());

        let g = Geometry::new(ModelSizes::explicit(10, 0));
        let before = std::fs::read(&input).unwrap();
        let err = expand_to_parquet(&input, &input, &g, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is the input file"), "{err}");
        assert_eq!(std::fs::read(&input).unwrap(), before);

        // An object-level trace is not a request trace.
        expand_to_parquet(&input, &out, &g, 4).unwrap();
        let err = expand_to_parquet(&out, dir.path().join("x.parquet"), &g, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a kvtrace request trace"), "{err}");
    }
}
