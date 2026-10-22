//! Synthetic object-level traces (`cachesim gen`).
//!
//! Writes a trace in the `cachetrace` format from a pattern, for replaying an
//! access sequence that no recorded trace has: a fill that alternates two
//! object sizes, a delete of every other object, a pass over a range, or a
//! zipf-distributed read stream. One call writes one pattern; a sequence of
//! phases is a sequence of files.
//!
//! Every row of a file has the same op, or none (a null `op` column, which
//! `cachesim simulate` reads as a GET). An object's size depends only
//! on its id, `sizes[id % sizes.len()]`, so every row for one object carries
//! the same size. Row `i` has timestamp `i * interval_ns`.
//! `next_access_vtime` is the row index of the next row with the same
//! `obj_id`, or -1, computed the way `kv expand` computes it: the ids are
//! generated once, a reverse scan fills in the next access, and the rows are
//! generated again (deterministically, from the same seed) and written.

use std::collections::HashMap;
use std::path::Path;

use cachetrace::{Op, TraceEntry, TraceWriter};
use kvtrace::output::plan_new_output;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Zipf};

use crate::Error;

/// Which ids the rows touch, in order.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Pattern {
    /// `passes` passes over `start, start + stride, ..., start + (count - 1) * stride`.
    Sequential {
        start: u64,
        count: u64,
        stride: u64,
        passes: u64,
    },
    /// `requests` draws of a zipf rank over `keys` objects with exponent
    /// `theta`; rank `r` (1 is the most popular) is object id `r - 1`.
    /// ChaCha8 (rand_chacha 0.10) seeded with `seed` drives rand_distr 0.6's
    /// Zipf sampler. A seed gives the same rows on one platform. The sampler
    /// uses the platform's floating-point math library, so rows can differ
    /// between operating systems.
    Zipf {
        keys: u64,
        theta: f64,
        requests: u64,
        seed: u64,
    },
}

/// What to write.
#[derive(Debug, Clone)]
pub struct GenSpec {
    pub pattern: Pattern,
    /// Every row's op, or `None` for a null `op` column.
    pub op: Option<Op>,
    /// Object sizes, chosen by `id % sizes.len()`. Not empty, no zero.
    pub sizes: Vec<u32>,
    /// Timestamp step between rows, in nanoseconds.
    pub interval_ns: u64,
    /// Compute `next_access_vtime`. Without it every row has -1 and the
    /// generator holds no per-row or per-object state, which a zipf stream of
    /// hundreds of millions of rows would otherwise need (8 bytes per row,
    /// plus a map entry per distinct object).
    pub next_access: bool,
}

/// What [`generate`] wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GenSummary {
    pub rows: u64,
    /// Distinct object ids; `None` when `next_access` was off and they were
    /// not counted.
    pub objects: Option<u64>,
}

impl GenSpec {
    /// Rows the pattern writes. Valid after the pattern's own checks.
    fn rows(&self) -> u64 {
        match self.pattern {
            Pattern::Sequential { count, passes, .. } => count * passes,
            Pattern::Zipf { requests, .. } => requests,
        }
    }

    fn check(&self) -> Result<(), Error> {
        let bad = |m: String| Err(Error::InvalidFormat(format!("gen: {m}")));
        if self.sizes.is_empty() {
            return bad("at least one size is needed".into());
        }
        if self.sizes.contains(&0) {
            return bad("sizes must be at least 1 byte".into());
        }
        match self.pattern {
            Pattern::Sequential {
                start,
                count,
                stride,
                passes,
            } => {
                if count == 0 || passes == 0 {
                    return bad("count and passes must be at least 1".into());
                }
                if stride == 0 {
                    return bad("stride must be at least 1".into());
                }
                let last = (count - 1)
                    .checked_mul(stride)
                    .and_then(|o| start.checked_add(o));
                if last.is_none() {
                    return bad("the last id does not fit in a u64".into());
                }
                if count.checked_mul(passes).is_none() {
                    return bad("count x passes does not fit in a u64".into());
                }
            }
            Pattern::Zipf {
                keys,
                theta,
                requests,
                ..
            } => {
                if keys == 0 || requests == 0 {
                    return bad("keys and requests must be at least 1".into());
                }
                if !(theta.is_finite() && theta > 0.0) {
                    return bad(format!("theta must be finite and > 0, got {theta}"));
                }
            }
        }
        // The last row's timestamp, (rows - 1) x interval_ns, must fit the
        // i64 timestamp column.
        let last = (self.rows() - 1).checked_mul(self.interval_ns);
        if !last.is_some_and(|t| t <= i64::MAX as u64) {
            return bad("(rows - 1) x interval_ns exceeds i64::MAX ns".into());
        }
        Ok(())
    }

    /// The object ids, in row order.
    fn ids(&self) -> Result<Box<dyn Iterator<Item = u64>>, Error> {
        Ok(match self.pattern {
            Pattern::Sequential {
                start,
                count,
                stride,
                passes,
            } => {
                Box::new((0..passes).flat_map(move |_| (0..count).map(move |i| start + i * stride)))
            }
            Pattern::Zipf {
                keys,
                theta,
                requests,
                seed,
            } => {
                let zipf = Zipf::new(keys as f64, theta)
                    .map_err(|e| Error::InvalidFormat(format!("gen: zipf: {e}")))?;
                let mut rng = ChaCha8Rng::seed_from_u64(seed);
                // A draw is a rank in 1..=keys as an f64; ranks above 2^53 are
                // not all representable, so the cast is clamped to the range.
                Box::new(
                    (0..requests).map(move |_| (zipf.sample(&mut rng) as u64).clamp(1, keys) - 1),
                )
            }
        })
    }

    fn size(&self, id: u64) -> u32 {
        self.sizes[(id % self.sizes.len() as u64) as usize]
    }

    /// The footer metadata that records how the file was made.
    fn metadata(&self) -> Vec<(String, String)> {
        let mut m = vec![
            ("cachesim.gen.version".to_string(), "1".to_string()),
            (
                "cachesim.gen.op".to_string(),
                match self.op {
                    None => "none".to_string(),
                    Some(Op::Get) => "get".to_string(),
                    Some(Op::Set) => "set".to_string(),
                    Some(Op::Delete) => "del".to_string(),
                    Some(op) => format!("{op:?}").to_lowercase(),
                },
            ),
            (
                "cachesim.gen.sizes".to_string(),
                self.sizes
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "cachesim.gen.interval_ns".to_string(),
                self.interval_ns.to_string(),
            ),
        ];
        let mut put = |k: &str, v: String| m.push((format!("cachesim.gen.{k}"), v));
        match self.pattern {
            Pattern::Sequential {
                start,
                count,
                stride,
                passes,
            } => {
                put("pattern", "sequential".into());
                put("start", start.to_string());
                put("count", count.to_string());
                put("stride", stride.to_string());
                put("passes", passes.to_string());
            }
            Pattern::Zipf {
                keys,
                theta,
                requests,
                seed,
            } => {
                put("pattern", "zipf".into());
                put("keys", keys.to_string());
                put("theta", theta.to_string());
                put("requests", requests.to_string());
                put("seed", seed.to_string());
                put(
                    "rng",
                    "rand_chacha 0.10 ChaCha8Rng, rand_distr 0.6 Zipf".into(),
                );
            }
        }
        m
    }
}

/// Write the trace `spec` describes to `output`. Output handling is
/// `kvtrace::output::plan_new_output`'s: a symlink is written through, a
/// directory is refused, a device or pipe is written directly, and a regular
/// file is written to a temporary name beside it and renamed into place once
/// complete. An existing regular file is refused unless `overwrite`. The
/// output is opened before any rows are generated, so a bad path fails at
/// once; a process killed while generating leaves the temporary file.
pub fn generate(
    spec: &GenSpec,
    output: impl AsRef<Path>,
    overwrite: bool,
    batch_size: usize,
) -> Result<GenSummary, Error> {
    spec.check()?;
    let output = output.as_ref();
    let out = plan_new_output(output)?;
    if !overwrite && out.temp.is_some() && std::fs::metadata(&out.dest).is_ok() {
        return Err(Error::InvalidFormat(format!(
            "gen: {} exists; replace it with overwrite (--force)",
            output.display()
        )));
    }
    let writer = TraceWriter::create(out.target(), batch_size).map_err(|e| {
        out.discard();
        Error::InvalidFormat(format!("gen: cannot create {}: {e}", output.display()))
    })?;

    let write = |mut writer: TraceWriter| -> Result<GenSummary, Error> {
        for (k, v) in spec.metadata() {
            writer.append_key_value_metadata(k, v);
        }
        // Pass 1: ids, overwritten by a reverse scan with each row's next
        // access (u64::MAX for none).
        let (next_access, objects) = if spec.next_access {
            let mut next: Vec<u64> = spec.ids()?.collect();
            let mut seen: HashMap<u64, u64> = HashMap::new();
            for i in (0..next.len()).rev() {
                let id = next[i];
                next[i] = seen.insert(id, i as u64).unwrap_or(u64::MAX);
            }
            (Some(next), Some(seen.len() as u64))
        } else {
            (None, None)
        };
        let mut rows = 0u64;
        for (i, id) in spec.ids()?.enumerate() {
            let next = next_access.as_ref().map_or(-1, |n| match n[i] {
                u64::MAX => -1,
                v => v as i64,
            });
            writer.write(&TraceEntry {
                // check() bounds (rows - 1) x interval_ns by i64::MAX.
                timestamp: (i as u64 * spec.interval_ns) as i64,
                obj_id: id,
                obj_size: spec.size(id),
                next_access_vtime: next,
                op: spec.op.map(|op| op as u8),
                ttl: None,
            })?;
            rows += 1;
        }
        if out.temp.is_some() {
            writer.finish_synced()?;
        } else {
            writer.finish()?;
        }
        Ok(GenSummary { rows, objects })
    };
    match write(writer).and_then(|s| out.commit().map(|()| s).map_err(Error::from)) {
        Ok(summary) => Ok(summary),
        Err(e) => {
            out.discard();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cachetrace::TraceReader;

    fn spec(pattern: Pattern, op: Option<Op>, sizes: &[u32]) -> GenSpec {
        GenSpec {
            pattern,
            op,
            sizes: sizes.to_vec(),
            interval_ns: 1_000,
            next_access: true,
        }
    }

    fn rows(spec: &GenSpec) -> Vec<TraceEntry> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let summary = generate(spec, &path, false, 7).unwrap();
        let rows: Vec<_> = TraceReader::open(&path)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(summary.rows, rows.len() as u64);
        rows
    }

    fn seq(start: u64, count: u64, stride: u64, passes: u64) -> Pattern {
        Pattern::Sequential {
            start,
            count,
            stride,
            passes,
        }
    }

    #[test]
    fn sequential_fill_alternates_sizes_by_id() {
        let rows = rows(&spec(seq(0, 6, 1, 1), Some(Op::Set), &[4096, 1_048_576]));
        let got: Vec<(u64, u32, Option<u8>, i64)> = rows
            .iter()
            .map(|r| (r.obj_id, r.obj_size, r.op, r.timestamp))
            .collect();
        assert_eq!(
            got,
            vec![
                (0, 4096, Some(3), 0),
                (1, 1_048_576, Some(3), 1_000),
                (2, 4096, Some(3), 2_000),
                (3, 1_048_576, Some(3), 3_000),
                (4, 4096, Some(3), 4_000),
                (5, 1_048_576, Some(3), 5_000),
            ]
        );
        assert!(rows.iter().all(|r| r.next_access_vtime == -1));
    }

    #[test]
    fn strided_delete_keeps_each_ids_size() {
        // Deleting the odd ids of the fill above: every row is a drop.
        let rows = rows(&spec(seq(1, 3, 2, 1), Some(Op::Delete), &[4096, 1_048_576]));
        let got: Vec<(u64, u32, Option<u8>)> =
            rows.iter().map(|r| (r.obj_id, r.obj_size, r.op)).collect();
        assert_eq!(
            got,
            vec![
                (1, 1_048_576, Some(9)),
                (3, 1_048_576, Some(9)),
                (5, 1_048_576, Some(9))
            ]
        );
    }

    #[test]
    fn passes_link_next_access_across_passes() {
        let rows = rows(&spec(seq(10, 3, 1, 2), Some(Op::Get), &[64]));
        let got: Vec<(u64, i64)> = rows
            .iter()
            .map(|r| (r.obj_id, r.next_access_vtime))
            .collect();
        assert_eq!(
            got,
            vec![(10, 3), (11, 4), (12, 5), (10, -1), (11, -1), (12, -1)]
        );
    }

    #[test]
    fn no_op_writes_a_null_op_column() {
        let rows = rows(&spec(seq(0, 2, 1, 1), None, &[1]));
        assert!(rows.iter().all(|r| r.op.is_none()));
    }

    fn zipf(seed: u64) -> GenSpec {
        spec(
            Pattern::Zipf {
                keys: 1_000,
                theta: 0.99,
                requests: 20_000,
                seed,
            },
            None,
            &[57_344],
        )
    }

    #[test]
    fn zipf_is_deterministic_skewed_and_in_range() {
        let a = rows(&zipf(7));
        let b = rows(&zipf(7));
        let ids = |rows: &[TraceEntry]| rows.iter().map(|r| r.obj_id).collect::<Vec<_>>();
        assert_eq!(ids(&a), ids(&b));
        assert_ne!(ids(&a), ids(&rows(&zipf(8))));
        assert!(a.iter().all(|r| r.obj_id < 1_000));
        let mut counts = vec![0u32; 1_000];
        for r in &a {
            counts[r.obj_id as usize] += 1;
        }
        // Rank 1 (id 0) is the most popular, and the head outweighs the tail.
        assert_eq!(counts.iter().max(), Some(&counts[0]));
        let head: u32 = counts[..10].iter().sum();
        let tail: u32 = counts[990..].iter().sum();
        assert!(head > 20 * tail.max(1), "head {head} tail {tail}");
    }

    #[test]
    fn zipf_next_access_points_at_the_next_row_of_the_same_id() {
        let rows = rows(&zipf(3));
        for (i, r) in rows.iter().enumerate() {
            let want = rows[i + 1..]
                .iter()
                .position(|n| n.obj_id == r.obj_id)
                .map_or(-1, |p| (i + 1 + p) as i64);
            assert_eq!(r.next_access_vtime, want, "row {i}");
        }
    }

    #[test]
    fn refuses_bad_specs_and_an_existing_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        for (s, want) in [
            (spec(seq(0, 1, 1, 1), None, &[]), "at least one size"),
            (spec(seq(0, 1, 1, 1), None, &[0]), "at least 1 byte"),
            (spec(seq(0, 0, 1, 1), None, &[1]), "count and passes"),
            (spec(seq(0, 2, 0, 1), None, &[1]), "stride"),
            (spec(seq(u64::MAX, 2, 1, 1), None, &[1]), "does not fit"),
            (
                spec(
                    Pattern::Zipf {
                        keys: 10,
                        theta: 0.0,
                        requests: 1,
                        seed: 0,
                    },
                    None,
                    &[1],
                ),
                "theta",
            ),
        ] {
            let err = generate(&s, &path, false, 7).unwrap_err().to_string();
            assert!(err.contains(want), "{want}: {err}");
            assert!(!path.exists());
        }
        generate(&spec(seq(0, 1, 1, 1), None, &[1]), &path, false, 7).unwrap();
        let err = generate(&spec(seq(0, 1, 1, 1), None, &[1]), &path, false, 7)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exists"), "{err}");
        generate(&spec(seq(0, 2, 1, 1), None, &[1]), &path, true, 7).unwrap();
        assert_eq!(TraceReader::open(&path).unwrap().total_entries(), 2);
        // No temporary file is left behind.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn refuses_timestamps_past_i64() {
        let mut s = spec(seq(0, 3, 1, 1), None, &[1]);
        s.interval_ns = u64::MAX;
        let dir = tempfile::tempdir().unwrap();
        let err = generate(&s, dir.path().join("t"), false, 7)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds i64::MAX"), "{err}");
        // Two rows at i64::MAX ns apart fit exactly.
        s.pattern = seq(0, 2, 1, 1);
        s.interval_ns = i64::MAX as u64;
        let r = rows(&s);
        assert_eq!(r[1].timestamp, i64::MAX);
    }

    #[test]
    fn output_paths_fail_before_any_work_and_leave_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let s = spec(seq(0, 2, 1, 1), None, &[1]);
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let err = generate(&s, &sub, true, 7).unwrap_err().to_string();
        assert!(err.contains("is a directory"), "{err}");
        let missing = dir.path().join("nodir").join("t.parquet");
        let err = generate(&s, &missing, false, 7).unwrap_err().to_string();
        assert!(
            err.contains("cannot create") && err.contains("nodir"),
            "{err}"
        );
        // Only the directory made above.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn writes_through_a_symlink_and_refuses_a_dangling_one() {
        let dir = tempfile::tempdir().unwrap();
        let s = spec(seq(0, 2, 1, 1), None, &[1]);
        let real = dir.path().join("real.parquet");
        let link = dir.path().join("link.parquet");
        std::fs::write(&real, b"old").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        generate(&s, &link, true, 7).unwrap();
        assert!(std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(TraceReader::open(&real).unwrap().total_entries(), 2);
        let dangling = dir.path().join("dangling.parquet");
        std::os::unix::fs::symlink(dir.path().join("gone"), &dangling).unwrap();
        let err = generate(&s, &dangling, true, 7).unwrap_err().to_string();
        assert!(err.contains("cannot be resolved"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn writes_a_device_directly_without_force() {
        let s = spec(seq(0, 2, 1, 1), None, &[1]);
        assert_eq!(generate(&s, "/dev/null", false, 7).unwrap().rows, 2);
    }

    #[test]
    fn metadata_records_the_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        generate(&zipf(5), &path, false, 7).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        use parquet::file::reader::FileReader;
        let kv: HashMap<String, String> = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
            .collect();
        assert_eq!(kv["cachesim.gen.pattern"], "zipf");
        assert_eq!(kv["cachesim.gen.seed"], "5");
        assert_eq!(kv["cachesim.gen.op"], "none");
        let path = dir.path().join("d.parquet");
        generate(
            &spec(seq(0, 1, 1, 1), Some(Op::Delete), &[1]),
            &path,
            false,
            7,
        )
        .unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let op = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|e| e.key == "cachesim.gen.op")
            .and_then(|e| e.value.clone());
        // The CLI's spelling.
        assert_eq!(op.as_deref(), Some("del"));
    }
}
