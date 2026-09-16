//! Fill in `next_access_vtime` for a trace that lacks it.
//!
//! The oracle policies read `next_access_vtime` as the virtual time (row
//! index) of the object's next request, or `-1` when there is none. Only
//! libCacheSim's oracle binaries carry it; a trace converted from CSV, or
//! captured live, has `-1` in every row, and against that the oracles
//! silently degrade to "evict anything".
//!
//! Computing it needs the future, so this is a two-pass job:
//!
//! 1. **Backward pass.** Walk the row groups last to first, reading only the
//!    `obj_id` column, and keep a map from object to the virtual time it was
//!    most recently (that is, next) seen. Each row's answer is written to a
//!    temp file at its row offset, so the pass holds one row group plus the
//!    map — O(unique objects), which is the floor for this computation.
//! 2. **Forward pass.** Stream the input, splice in the answers from the temp
//!    file, and write the output with every other column unchanged.
//!
//! Peak memory is one row group of ids plus the object map; nothing scales
//! with trace length.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use arrow::array::{Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::trace::{TraceReader, TraceWriter};
use crate::Error;

/// What an annotation pass found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnotateStats {
    /// Rows in the trace.
    pub rows: u64,
    /// Distinct `obj_id` values.
    pub unique_objects: u64,
    /// Rows whose object is requested again later (`next_access_vtime >= 0`).
    pub reused_rows: u64,
}

/// Rewrite `input` to `output` with `next_access_vtime` computed from the
/// request sequence. Every other column is copied through.
///
/// Virtual time is the zero-based row index; a row's `next_access_vtime` is
/// the index of the next row with the same `obj_id`, or `-1` if there is none.
pub fn annotate_next_access(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    batch_size: usize,
) -> Result<AnnotateStats, Error> {
    let input = input.as_ref();

    // Row-group geometry from the footer: how many, how long, where each
    // starts in virtual time.
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?;
    let meta = builder.metadata().clone();
    let group_rows: Vec<usize> = (0..meta.num_row_groups())
        .map(|k| meta.row_group(k).num_rows() as usize)
        .collect();
    let mut bases = Vec::with_capacity(group_rows.len());
    let mut acc = 0usize;
    for &n in &group_rows {
        bases.push(acc);
        acc += n;
    }
    let total_rows = acc;

    let obj_id_leaf = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.name() == "obj_id")
        .ok_or_else(|| Error::InvalidFormat("trace has no obj_id column".into()))?;
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [obj_id_leaf]);
    drop(builder);

    // Backward pass: answers land in a temp file at their row offset.
    let mut navs_file = tempfile::tempfile()?;
    let mut next_seen: HashMap<u64, i64> = HashMap::new();
    let mut reused_rows = 0u64;
    let mut ids: Vec<u64> = Vec::new();
    let mut navs: Vec<u8> = Vec::new();

    for k in (0..group_rows.len()).rev() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?
            .with_row_groups(vec![k])
            .with_projection(mask.clone())
            .with_batch_size(batch_size)
            .build()?;

        ids.clear();
        ids.reserve(group_rows[k]);
        for batch in reader {
            let batch = batch?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::InvalidFormat("obj_id column is not UInt64".into()))?;
            if col.null_count() != 0 {
                return Err(Error::InvalidFormat("obj_id column has nulls".into()));
            }
            ids.extend_from_slice(col.values());
        }
        if ids.len() != group_rows[k] {
            return Err(Error::InvalidFormat(format!(
                "row group {k}: footer says {} rows, read {}",
                group_rows[k],
                ids.len()
            )));
        }

        navs.clear();
        navs.resize(ids.len() * 8, 0);
        for (i, &id) in ids.iter().enumerate().rev() {
            let vtime = (bases[k] + i) as i64;
            let nav = next_seen.insert(id, vtime).unwrap_or(-1);
            if nav >= 0 {
                reused_rows += 1;
            }
            navs[i * 8..i * 8 + 8].copy_from_slice(&nav.to_le_bytes());
        }
        navs_file.seek(SeekFrom::Start((bases[k] * 8) as u64))?;
        navs_file.write_all(&navs)?;
    }
    let unique_objects = next_seen.len() as u64;
    drop(next_seen);

    // Forward pass: splice the answers in, copy everything else through.
    navs_file.seek(SeekFrom::Start(0))?;
    let mut navs_in = BufReader::new(navs_file);
    let mut writer = TraceWriter::create(output, batch_size)?;
    let mut buf = [0u8; 8];
    let mut rows = 0u64;
    for entry in TraceReader::open(input)? {
        let mut entry = entry?;
        navs_in.read_exact(&mut buf)?;
        entry.next_access_vtime = i64::from_le_bytes(buf);
        writer.write(&entry)?;
        rows += 1;
    }
    writer.finish()?;

    if rows as usize != total_rows {
        return Err(Error::InvalidFormat(format!(
            "footer says {total_rows} rows, read {rows}"
        )));
    }

    Ok(AnnotateStats {
        rows,
        unique_objects,
        reused_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{trace_schema, TraceEntry};
    use arrow::array::{Int32Array, Int64Array, TimestampNanosecondArray, UInt32Array, UInt8Array};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    /// The answer computed the obvious way: for each row, scan forward.
    fn naive(ids: &[u64]) -> Vec<i64> {
        ids.iter()
            .enumerate()
            .map(|(i, &id)| {
                ids[i + 1..]
                    .iter()
                    .position(|&j| j == id)
                    .map(|p| (i + 1 + p) as i64)
                    .unwrap_or(-1)
            })
            .collect()
    }

    /// Write `ids` as a trace with `row_group` rows per row group and every
    /// `next_access_vtime` at -1, the way a CSV conversion leaves it.
    fn write_unannotated(path: &Path, ids: &[u64], row_group: usize) {
        let n = ids.len();
        let schema = Arc::new(trace_schema());
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group))
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), Some(props)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampNanosecondArray::from((0..n as i64).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(UInt64Array::from(ids.to_vec())),
                Arc::new(UInt32Array::from(
                    (0..n as u32).map(|i| i + 1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![-1i64; n])),
                Arc::new(UInt8Array::from(vec![Some(3u8); n])),
                Arc::new(Int32Array::from(vec![Some(60i32); n])),
                Arc::new(UInt32Array::from(vec![Some(4u32); n])),
                Arc::new(UInt32Array::from(vec![None::<u32>; n])),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    #[test]
    fn matches_naive_across_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.parquet");
        let output = dir.path().join("out.parquet");

        // A few thousand rows over a small id space so reuse crosses row
        // group boundaries in both directions; row groups of 100 rows.
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let ids: Vec<u64> = (0..5_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % 300
            })
            .collect();
        write_unannotated(&input, &ids, 100);

        let stats = annotate_next_access(&input, &output, 64).unwrap();
        let expected = naive(&ids);

        let got: Vec<TraceEntry> = TraceReader::open(&output)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(got.len(), ids.len());
        for (i, e) in got.iter().enumerate() {
            assert_eq!(e.obj_id, ids[i]);
            assert_eq!(e.next_access_vtime, expected[i], "row {i}");
            // Everything else copied through.
            assert_eq!(e.timestamp, i as i64);
            assert_eq!(e.obj_size, i as u32 + 1);
            assert_eq!(e.op, Some(3));
            assert_eq!(e.ttl, Some(60));
            assert_eq!(e.key_size, Some(4));
            assert_eq!(e.value_size, None);
        }

        assert_eq!(stats.rows, ids.len() as u64);
        assert_eq!(
            stats.unique_objects,
            ids.iter().collect::<std::collections::HashSet<_>>().len() as u64
        );
        assert_eq!(
            stats.reused_rows,
            expected.iter().filter(|&&v| v >= 0).count() as u64
        );
    }

    #[test]
    fn single_access_objects_get_minus_one() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.parquet");
        let output = dir.path().join("out.parquet");
        let ids = [1u64, 2, 3, 2, 1];
        write_unannotated(&input, &ids, 2);

        let stats = annotate_next_access(&input, &output, 64).unwrap();
        let got: Vec<i64> = TraceReader::open(&output)
            .unwrap()
            .map(|e| e.unwrap().next_access_vtime)
            .collect();
        assert_eq!(got, vec![4, 3, -1, -1, -1]);
        assert_eq!(stats.reused_rows, 2);
        assert_eq!(stats.unique_objects, 3);
    }
}
