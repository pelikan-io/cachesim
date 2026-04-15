//! Oracle (offline-optimal) cache eviction policies.
//!
//! These policies use the `next_access_vtime` field from the trace to make
//! eviction decisions with perfect future knowledge. They serve as theoretical
//! upper bounds when evaluating online policies like FIFO or LRU.
//!
//! * **Belady** (OPT / MIN): evict the object whose next access is farthest in
//!   the future. Optimal for minimizing miss count when all objects occupy one
//!   slot.
//! * **BeladySize**: evict the object that maximizes
//!   `next_access_distance × obj_size`, freeing the most *cache-byte-time*.
//!   This biases toward evicting large objects that will not be needed soon,
//!   and is a better reference point for variable-size workloads.

use std::collections::{BTreeSet, HashMap};

/// Oracle eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OraclePolicy {
    /// Evict the object with the farthest future access.
    Belady,
    /// Evict the object maximizing `next_access_vtime × obj_size`.
    BeladySize,
}

/// A cache that uses oracle (future-knowledge) eviction.
///
/// Capacity is measured in bytes.  The cache tracks total stored size and
/// evicts according to the configured [`OraclePolicy`] when an insertion
/// would exceed capacity.
pub struct OracleCache {
    /// obj_id → (obj_size, next_access_vtime)
    objects: HashMap<u64, (u32, i64)>,
    /// `(eviction_score, obj_id)` — highest score is evicted first.
    order: BTreeSet<(i128, u64)>,
    used: usize,
    capacity: usize,
    policy: OraclePolicy,
}

impl OracleCache {
    pub fn new(capacity: usize, policy: OraclePolicy) -> Self {
        Self {
            objects: HashMap::new(),
            order: BTreeSet::new(),
            used: 0,
            capacity,
            policy,
        }
    }

    /// Compute the eviction score for an object.  Higher score → evicted first.
    ///
    /// * **Belady**: score = next_access_vtime (farther access → higher score).
    /// * **BeladySize**: score = next_access_vtime × obj_size.
    ///
    /// Objects that are never accessed again (`nav < 0`) receive the maximum
    /// possible score so they are always evicted first.
    fn score(&self, size: u32, nav: i64) -> i128 {
        let d = if nav < 0 { i64::MAX } else { nav };
        match self.policy {
            OraclePolicy::Belady => d as i128,
            OraclePolicy::BeladySize => d as i128 * size as i128,
        }
    }

    /// Look up an object.  If present, update its `next_access_vtime` and
    /// return `true` (hit).  Returns `false` on a miss without inserting.
    pub fn lookup(&mut self, obj_id: u64, nav: i64) -> bool {
        if let Some((size, old_nav)) = self.objects.get(&obj_id).copied() {
            self.order.remove(&(self.score(size, old_nav), obj_id));
            self.objects.insert(obj_id, (size, nav));
            self.order.insert((self.score(size, nav), obj_id));
            true
        } else {
            false
        }
    }

    /// Insert or overwrite an object, evicting as needed to make room.
    ///
    /// Objects larger than the total cache capacity are silently dropped.
    pub fn insert(&mut self, obj_id: u64, obj_size: u32, nav: i64) {
        // Remove any existing entry first.
        if let Some((old_size, old_nav)) = self.objects.remove(&obj_id) {
            self.order.remove(&(self.score(old_size, old_nav), obj_id));
            self.used -= old_size as usize;
        }

        if obj_size as usize > self.capacity {
            return;
        }

        // Evict highest-score objects until there is room.
        while self.used + obj_size as usize > self.capacity {
            if let Some(&(score, victim)) = self.order.iter().next_back() {
                self.order.remove(&(score, victim));
                let (victim_size, _) = self.objects.remove(&victim).unwrap();
                self.used -= victim_size as usize;
            } else {
                break;
            }
        }

        let score = self.score(obj_size, nav);
        self.objects.insert(obj_id, (obj_size, nav));
        self.order.insert((score, obj_id));
        self.used += obj_size as usize;
    }

    /// Remove an object from the cache. Returns `true` if it was present.
    pub fn remove(&mut self, obj_id: u64) -> bool {
        if let Some((size, nav)) = self.objects.remove(&obj_id) {
            self.order.remove(&(self.score(size, nav), obj_id));
            self.used -= size as usize;
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    pub fn used(&self) -> usize {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn belady_evicts_farthest() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        // Insert two objects that together fill the cache.
        c.insert(1, 50, 10); // next access at vtime 10 (soon)
        c.insert(2, 50, 100); // next access at vtime 100 (far)
        assert_eq!(c.len(), 2);

        // Insert a third — must evict; Belady evicts obj 2 (farthest).
        c.insert(3, 50, 50);
        assert_eq!(c.len(), 2);
        assert!(c.lookup(1, 10)); // obj 1 still present
        assert!(!c.lookup(2, 100)); // obj 2 evicted
        assert!(c.lookup(3, 50)); // obj 3 present
    }

    #[test]
    fn belady_evicts_no_reuse_first() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        c.insert(1, 50, 5); // will be accessed again
        c.insert(2, 50, -1); // never accessed again

        c.insert(3, 50, 20);
        assert!(c.lookup(1, 5));
        assert!(!c.lookup(2, -1)); // no-reuse evicted first
    }

    #[test]
    fn belady_size_prefers_evicting_high_score() {
        let mut c = OracleCache::new(100, OraclePolicy::BeladySize);
        // obj 1: size=40, nav=10 → score = 400
        c.insert(1, 40, 10);
        // obj 2: size=40, nav=100 → score = 4000
        c.insert(2, 40, 100);
        assert_eq!(c.used(), 80);

        // Insert obj 3 (40 bytes, nav=30). Must evict one object.
        // BeladySize evicts obj 2 (score 4000 > 400).
        c.insert(3, 40, 30);
        assert_eq!(c.len(), 2);
        assert!(c.lookup(1, 10)); // kept (low score)
        assert!(!c.lookup(2, 100)); // evicted (high score)
        assert!(c.lookup(3, 30)); // newly inserted
    }

    #[test]
    fn belady_size_scores_account_for_size() {
        let mut c = OracleCache::new(100, OraclePolicy::BeladySize);
        // obj 1: size=50, nav=20 → score = 1000
        c.insert(1, 50, 20);
        // obj 2: size=10, nav=50 → score = 500
        c.insert(2, 10, 50);
        assert_eq!(c.used(), 60);

        // Insert obj 3 (50 bytes). Need to free 10+ bytes.
        // BeladySize evicts obj 1 (score 1000 > 500) even though obj 2 has
        // a farther next access, because obj 1's size amplifies its score.
        c.insert(3, 50, 30);
        assert!(!c.lookup(1, 20)); // evicted (large × moderate distance)
        assert!(c.lookup(2, 50)); // kept (small × far distance = lower score)
    }

    #[test]
    fn lookup_updates_nav() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        c.insert(1, 50, 100); // far away
        c.insert(2, 50, 50); // closer

        // Access obj 1 and tell it its NEW next access is very soon (vtime 5).
        assert!(c.lookup(1, 5));

        // Now force eviction — obj 2 (nav=50) should be evicted, not obj 1 (nav=5).
        c.insert(3, 50, 20);
        assert!(c.lookup(1, 5)); // kept
        assert!(!c.lookup(2, 50)); // evicted
    }

    #[test]
    fn insert_overwrites_existing() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        c.insert(1, 60, 10);
        assert_eq!(c.used(), 60);

        // Re-insert with a different size.
        c.insert(1, 40, 20);
        assert_eq!(c.used(), 40);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn oversized_object_is_dropped() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        c.insert(1, 200, 10);
        assert_eq!(c.len(), 0);
        assert_eq!(c.used(), 0);
    }

    #[test]
    fn remove_frees_space() {
        let mut c = OracleCache::new(100, OraclePolicy::Belady);
        c.insert(1, 60, 10);
        c.insert(2, 40, 20);
        assert_eq!(c.used(), 100);

        assert!(c.remove(1));
        assert_eq!(c.used(), 40);
        assert!(!c.remove(1)); // already gone
    }
}
