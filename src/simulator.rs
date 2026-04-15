//! Cache simulation engine.
//!
//! Replays a Parquet trace against a [`segcache::Segcache`] instance and
//! collects hit/miss statistics.
//!
//! ## Simulation model
//!
//! * **Read** operations (GET, READ, …): look up the object; on a miss the
//!   object is inserted ("demand-fetch").
//! * **Write** operations (SET, ADD, …): unconditionally insert/overwrite.
//! * **Delete** operations: remove the object from the cache.
//! * When no `op` column is present in the trace every request is treated as a
//!   read.
//!
//! ## Limitations
//!
//! TTL-based expiration uses wall-clock time inside segcache, not the trace
//! timestamps.  Accurate TTL simulation would require a virtual-clock
//! integration with segcache which is not yet available.

use std::path::Path;
use std::time::Duration;

use segcache::{Policy, Segcache};

use crate::trace::{Op, TraceReader};
use crate::Error;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Parameters for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Total cache heap size in bytes.
    pub cache_size: usize,
    /// Segment size in bytes (segcache granularity).
    pub segment_size: i32,
    /// Hash table power – the table will have 2^`hash_power` buckets.
    pub hash_power: u8,
    /// Eviction policy.
    pub eviction: Policy,
    /// Default TTL applied when the trace entry has no TTL (seconds, 0 = no
    /// expiration).
    pub default_ttl: u32,
    /// Objects larger than this are skipped (bytes).
    pub max_obj_size: u32,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            cache_size: 64 * 1024 * 1024, // 64 MiB
            segment_size: 1024 * 1024,     // 1 MiB
            hash_power: 16,
            eviction: Policy::Fifo,
            default_ttl: 0,
            max_obj_size: 1024 * 1024, // 1 MiB
        }
    }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// Aggregate statistics from a simulation run.
#[derive(Debug, Clone, Default)]
pub struct SimResult {
    pub total_requests: u64,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub insert_failures: u64,
    pub deletes: u64,
    /// Requests skipped because `obj_size > max_obj_size`.
    pub skipped: u64,
}

impl SimResult {
    /// Hit rate in the range `[0.0, 1.0]`.
    pub fn hit_rate(&self) -> f64 {
        let lookups = self.hits + self.misses;
        if lookups == 0 {
            return 0.0;
        }
        self.hits as f64 / lookups as f64
    }

    /// Miss rate (`1.0 − hit_rate`).
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }
}

impl std::fmt::Display for SimResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Simulation Results")?;
        writeln!(f, "  total requests:  {}", self.total_requests)?;
        writeln!(f, "  hits:            {}", self.hits)?;
        writeln!(f, "  misses:          {}", self.misses)?;
        writeln!(f, "  hit rate:        {:.4}%", self.hit_rate() * 100.0)?;
        writeln!(f, "  inserts:         {}", self.inserts)?;
        writeln!(f, "  insert failures: {}", self.insert_failures)?;
        writeln!(f, "  deletes:         {}", self.deletes)?;
        write!(f, "  skipped:         {}", self.skipped)
    }
}

// ---------------------------------------------------------------------------
// Simulation entry point
// ---------------------------------------------------------------------------

/// Run a cache simulation by replaying `trace_path` against segcache.
pub fn simulate(trace_path: impl AsRef<Path>, config: &SimConfig) -> Result<SimResult, Error> {
    let reader = TraceReader::open(trace_path)?;

    let mut cache = Segcache::builder()
        .heap_size(config.cache_size)
        .segment_size(config.segment_size)
        .hash_power(config.hash_power)
        .eviction(config.eviction)
        .build()?;

    let default_ttl = if config.default_ttl == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(config.default_ttl as u64)
    };

    // Pre-allocated zero buffer used as the value payload for inserts.
    // The actual bytes are irrelevant – only the *size* matters for eviction
    // pressure.
    let value_buf = vec![0u8; config.max_obj_size as usize];

    let mut result = SimResult::default();

    for entry in reader {
        result.total_requests += 1;

        if entry.obj_size > config.max_obj_size || entry.obj_size == 0 {
            result.skipped += 1;
            continue;
        }

        let key = entry.obj_id.to_le_bytes();
        let op = entry.op.map(Op::from_u8).unwrap_or(Op::Get);

        let ttl = entry
            .ttl
            .filter(|&t| t > 0)
            .map(|t| Duration::from_secs(t as u64))
            .unwrap_or(default_ttl);

        let value = &value_buf[..entry.obj_size as usize];

        if op.is_delete() {
            cache.delete(&key);
            result.deletes += 1;
        } else if op.is_write() {
            match cache.insert(&key, value, None, ttl) {
                Ok(()) => result.inserts += 1,
                Err(_) => result.insert_failures += 1,
            }
        } else {
            // Read / demand-fetch: lookup, insert on miss.
            if cache.get(&key).is_some() {
                result.hits += 1;
            } else {
                result.misses += 1;
                match cache.insert(&key, value, None, ttl) {
                    Ok(()) => result.inserts += 1,
                    Err(_) => result.insert_failures += 1,
                }
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{TraceEntry, TraceWriter};

    /// Helper: write a small synthetic trace to a temp Parquet file and return
    /// the path.
    fn write_synthetic_trace(entries: &[TraceEntry]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.parquet");
        let mut w = TraceWriter::create(&path, 1024).unwrap();
        for e in entries {
            w.write(e).unwrap();
        }
        w.finish().unwrap();
        (dir, path)
    }

    #[test]
    fn all_hits_after_warmup() {
        // Access the same object twice – second access should be a hit.
        let entries: Vec<TraceEntry> = (0..2)
            .map(|i| TraceEntry {
                timestamp: i,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: if i == 0 { 1 } else { -1 },
                op: None,
                ttl: None,
            })
            .collect();

        let (_dir, path) = write_synthetic_trace(&entries);

        let config = SimConfig {
            cache_size: 4 * 1024 * 1024,
            segment_size: 1024 * 1024,
            hash_power: 8,
            ..Default::default()
        };

        let result = simulate(&path, &config).unwrap();
        assert_eq!(result.total_requests, 2);
        assert_eq!(result.misses, 1);
        assert_eq!(result.hits, 1);
        assert!((result.hit_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn oversized_objects_are_skipped() {
        let entries = vec![TraceEntry {
            timestamp: 0,
            obj_id: 1,
            obj_size: 2_000_000, // > default max_obj_size
            next_access_vtime: -1,
            op: None,
            ttl: None,
        }];

        let (_dir, path) = write_synthetic_trace(&entries);
        let config = SimConfig::default();
        let result = simulate(&path, &config).unwrap();
        assert_eq!(result.skipped, 1);
        assert_eq!(result.misses, 0);
    }

    #[test]
    fn delete_operations() {
        let entries = vec![
            TraceEntry {
                timestamp: 0,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: 1,
                op: Some(Op::Get as u8),
                ttl: None,
            },
            TraceEntry {
                timestamp: 1,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: -1,
                op: Some(Op::Delete as u8),
                ttl: None,
            },
        ];

        let (_dir, path) = write_synthetic_trace(&entries);
        let config = SimConfig {
            cache_size: 4 * 1024 * 1024,
            segment_size: 1024 * 1024,
            hash_power: 8,
            ..Default::default()
        };

        let result = simulate(&path, &config).unwrap();
        assert_eq!(result.misses, 1);
        assert_eq!(result.deletes, 1);
        assert_eq!(result.hits, 0);
    }

    #[test]
    fn write_operations_dont_count_as_hits() {
        let entries = vec![
            TraceEntry {
                timestamp: 0,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: 1,
                op: Some(Op::Set as u8),
                ttl: None,
            },
            TraceEntry {
                timestamp: 1,
                obj_id: 1,
                obj_size: 64,
                next_access_vtime: -1,
                op: Some(Op::Get as u8),
                ttl: None,
            },
        ];

        let (_dir, path) = write_synthetic_trace(&entries);
        let config = SimConfig {
            cache_size: 4 * 1024 * 1024,
            segment_size: 1024 * 1024,
            hash_power: 8,
            ..Default::default()
        };

        let result = simulate(&path, &config).unwrap();
        assert_eq!(result.inserts, 1); // SET inserts
        assert_eq!(result.hits, 1); // GET hits
        assert_eq!(result.misses, 0);
    }
}
