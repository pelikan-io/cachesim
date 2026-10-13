//! Request to objects: the KV-cache objects a request writes, with their ids
//! and sizes, for one model geometry.
//!
//! A request's chunks are its full prefix chunks in order. Each chunk becomes
//! one object per storing tensor-parallel rank; a hybrid model's recurrent
//! state is added to a chunk's object or stored beside it, at the request's
//! last chunk or at every chunk ([`StateMode`], [`StateLayout`]).
//!
//! Objects come out in prefix order: chunk by chunk, each chunk's objects in
//! rank order, then its separate state objects (if any) in rank order.
//!
//! # Objects
//!
//! The defaults follow LMCache v0.5.5 (commit 05a013b):
//!
//! * One object is one chunk across all layers (LMCache's chunk is 256 tokens
//!   by default, `lmcache/v1/config.py:90`; here the chunk length comes from
//!   the request trace). A chunk's size is its token count times the model's
//!   KV bytes per token.
//! * With tensor parallelism each rank stores its own shard as a separate
//!   object (the key includes the worker id, `lmcache/utils.py:389-441`). A
//!   shard is `1/min(tp, kv_heads)` of the KV, since vLLM replicates KV heads
//!   once `tp` reaches their count, and `1/tp` of the recurrent state; see
//!   [`Geometry`]. MLA models are the exception: only the first rank stores,
//!   and it stores the full latent (`save_only_first_rank`,
//!   `cache_engine.py:116-119`).
//!
//! Dense and MLA geometry (sizes, TP split, first-rank-only MLA) follows
//! vLLM and LMCache as cited. Hybrid geometry is an approximation pending a
//! follow-up; the known differences are listed under "Hybrid models".
//!
//! # Hybrid models
//!
//! Linear-attention layers keep a fixed-size recurrent state per sequence
//! instead of per-token KV. [`StateMode::PerRequest`] stores one snapshot at
//! each request's last chunk (the default) and [`StateMode::PerChunk`] one at
//! every chunk (the upper bound). [`StateLayout::Combined`] adds the state
//! bytes to the chunk's object; [`StateLayout::Separate`] stores the state as
//! its own object. Convolution state is not counted in the object sizes.
//!
//! This is a simplified model, not LMCache v0.5.5's layout. Known
//! differences:
//!
//! * Sizes, combined layout: LMCache writes every chunk object at one size,
//!   the attention pages plus the recurrent pages for the whole chunk, with
//!   null pages where no snapshot was taken
//!   (`multiprocess/modules/lmcache_driven_transfer.py:66-93`
//!   `get_layout_desc`, `platform/cache_context.py:228`). Here a chunk
//!   without a snapshot is written at KV size only and a chunk with one at
//!   KV plus one state, so the same id is written at two sizes and the
//!   KV-only writes are smaller than LMCache's.
//! * Chunk length: with prefix caching vLLM ("align" mode) raises the
//!   attention block size until one layer's attention page is at least one
//!   layer's recurrent page (`vllm/platforms/interface.py:960-994`;
//!   [`Geometry::vllm_block_size`]), and LMCache requires the chunk size to
//!   be a multiple of the block size (`kv_layer_groups.py:750-755`). For the
//!   hybrid models this crate's tests use, that rules out 256-token chunks.
//!   At a valid chunk of k blocks, each recurrent object holds k pages per
//!   linear layer, each padded to the attention page size
//!   (`platforms/interface.py:1006-1009`); this model has one unpadded
//!   snapshot per object.
//! * Snapshot count: vLLM's align mode snapshots the state at the last
//!   block boundary of each scheduler step (`vllm/config/cache.py:190-198`),
//!   so a prompt longer than `max_num_batched_tokens` gets several
//!   snapshots. [`StateMode::PerRequest`] models one per request and so
//!   understates snapshots for long prompts.
//!
//! # Object ids
//!
//! An id is what the store keys the object on:
//! `chunk << 16 | slot << 12 | rank` ([`obj_id`]).
//!
//! * [`StateLayout::Combined`]: with `separate_object_groups` off (the
//!   default), LMCache has one object group per chunk, stored as one object
//!   keyed by the chunk (`kv_layer_groups.py:682-687`). So a chunk has one id
//!   per rank, slot 0, whether or not a write carries state. Because of the
//!   simplified sizing above, a write with a snapshot
//!   ([`ObjectKind::KvWithState`]) is larger than one without
//!   ([`ObjectKind::Kv`]), and the same id can be written at two sizes.
//! * [`StateLayout::Separate`]: the state is its own object under its own
//!   key, slot 1 ([`ObjectKind::State`]); the chunk's KV object keeps slot 0.
//!
//! [`ObjectKind`] says what a write holds; it is not part of the id except
//! that a separate state object has its own slot ([`ObjectKind::key_slot`]).
//!
//! # Writes
//!
//! [`ObjectMapper::objects`] lists the objects a request writes: every chunk
//! of its prefix. Which of those a store actually accepts depends on its
//! write rule, and LMCache has two:
//!
//! * Insert-if-absent: the multiprocess store reserves only keys that are
//!   not present (`reserve_write(..., "new")`,
//!   `distributed/storage_manager.py:182-200`, `l1_manager.py:455-458`; called
//!   from `multiprocess/modules/lmcache_driven_transfer.py:1212-1214` and
//!   `server_transfer.py:213-215`).
//! * SET: the in-process remote backends put every object they are given,
//!   without checking whether the store holds the key
//!   (`storage_backend/remote_backend.py:263-270`, `323-331`). The tests
//!   here model this as a SET of every chunk after the lookup's prefix hit.
//!
//! Under SET the combined layout can lose a stored snapshot: a later request
//! that writes the same chunk without a snapshot replaces the
//! state-carrying object. Example, per-request state: A = [1, 2, 3] is
//! stored, chunk 2 is evicted, B = [1, 2, 3, 4] misses at chunk 2 and
//! writes chunks 2-4, which replaces chunk 3's object with a KV-only one; a
//! repeat of A then reuses nothing under SET and 3 chunks under
//! insert-if-absent. The separate layout keeps chunk 3's state object either
//! way. Chunks that are present but not resumable are not rewritten under
//! SET (they are inside the prefix hit); under insert-if-absent only absent
//! keys are written, so the separate layout can add a snapshot to such a
//! chunk and the combined layout cannot (A = [1, 2, 3], then [1, 2] twice:
//! the second [1, 2] reuses 2 chunks only with separate, insert-if-absent).
//!
//! # Reuse
//!
//! Which stored objects a later request can reuse is the consumer's rule,
//! not this module's:
//!
//! * A chunk counts as present only if every storing rank's object is
//!   present: the usable prefix is the minimum over ranks. MLA models have
//!   one storing rank (the first), so only rank 0 is looked up.
//! * For a model without recurrent state, the whole present prefix is
//!   usable.
//! * For a hybrid model, a prefix is usable only through the last present
//!   chunk whose stored object carries state: the linear-attention layers
//!   need the state where computation resumes, so a prefix that ends at a
//!   KV-only chunk cannot be resumed from. [`ObjectMapper::lookup_ids`] gives
//!   the ids to probe at a chunk. Combined: the one id; whether the object
//!   stored there carries state depends on the write that stored it, so the
//!   consumer must remember it ([`KvObject::kind`],
//!   [`ObjectKind::carries_state`]). Separate: the KV id and the state id;
//!   the chunk is resumable when the state id is present.
//!
//! Under [`StateMode::PerRequest`] a request's snapshot sits on its own last
//! chunk, so a later request with a longer prompt can resume through that
//! chunk but not at an earlier one.

use crate::geometry::{overflow, Geometry, Sharding, StateLayout, StateMode};
use crate::Error;

/// Bits of an object id that hold the chunk id.
pub const CHUNK_ID_BITS: u32 = 48;
pub(crate) const SLOT_BITS: u32 = 4;
pub(crate) const RANK_BITS: u32 = 12;

/// Key slot of a chunk's own object: its KV, or KV and state together with
/// the combined layout.
pub const SLOT_CHUNK: u64 = 0;
/// Key slot of a separate state object.
pub const SLOT_STATE: u64 = 1;

/// What a write holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// Per-token KV of one chunk.
    Kv,
    /// A recurrent-state snapshot on its own (separate layout).
    State,
    /// A chunk's KV and a state snapshot in one object (combined layout).
    /// Same id as the chunk's [`Kv`](Self::Kv) writes; larger.
    KvWithState,
}

impl ObjectKind {
    /// Whether a write of this kind holds a recurrent-state snapshot
    /// ([`State`](Self::State) or [`KvWithState`](Self::KvWithState)). For a
    /// hybrid model, a cached prefix can be resumed only at a chunk whose
    /// stored object carries state; see the
    /// [module docs](self#reuse).
    pub fn carries_state(self) -> bool {
        matches!(self, Self::State | Self::KvWithState)
    }

    /// The key slot this kind is stored under: [`SLOT_STATE`] for
    /// [`State`](Self::State), [`SLOT_CHUNK`] otherwise.
    pub fn key_slot(self) -> u64 {
        match self {
            Self::Kv | Self::KvWithState => SLOT_CHUNK,
            Self::State => SLOT_STATE,
        }
    }
}

/// Pack `(chunk, kind's key slot, rank)` into an object id:
/// `chunk << 16 | slot << 12 | rank`. [`ObjectKind::Kv`] and
/// [`ObjectKind::KvWithState`] give the same id.
///
/// Errors if `chunk` does not fit 48 bits or `rank` 12 bits.
pub fn obj_id(chunk: u64, kind: ObjectKind, rank: u64) -> Result<u64, Error> {
    if chunk >= 1 << CHUNK_ID_BITS || rank >= 1 << RANK_BITS {
        return Err(Error::InvalidFormat(format!(
            "kvtrace: obj_id field out of range (chunk {chunk}, rank {rank})"
        )));
    }
    Ok(pack(chunk, kind.key_slot(), rank))
}

fn pack(chunk: u64, slot: u64, rank: u64) -> u64 {
    (chunk << SLOT_BITS | slot) << RANK_BITS | rank
}

/// Unpack an object id made by [`obj_id`] into `(chunk, slot, rank)`.
pub fn obj_id_fields(id: u64) -> (u64, u64, u64) {
    (
        id >> (SLOT_BITS + RANK_BITS),
        id >> RANK_BITS & ((1 << SLOT_BITS) - 1),
        id & ((1 << RANK_BITS) - 1),
    )
}

/// Text for trace metadata describing the [`obj_id`] layout.
pub fn obj_id_layout() -> String {
    format!(
        "chunk<<{} | slot<<{RANK_BITS} | rank; slot 0 = the chunk's object \
         (KV, or KV and state with the combined layout), 1 = separate state object",
        SLOT_BITS + RANK_BITS
    )
}

/// One object a request writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvObject {
    /// Object id ([`obj_id`]).
    pub obj_id: u64,
    /// Bytes this rank stores for the object in this write.
    pub size: u64,
    /// Tensor-parallel rank that stores the object.
    pub rank: u64,
    /// What this write holds.
    pub kind: ObjectKind,
    /// 0-based position of the object's chunk in the request's prefix. Every
    /// rank's object for a chunk, and its separate state objects, share it.
    pub chunk_index: u32,
}

/// The ids a lookup probes at one chunk and rank; see
/// [`ObjectMapper::lookup_ids`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupIds {
    /// The chunk's object (slot 0): its KV, or KV and state when combined.
    pub object: u64,
    /// The separate state object's id (slot 1), with the separate layout
    /// for a model with recurrent state; `None` otherwise.
    pub state: Option<u64>,
}

/// Maps a request's chunks to objects for one geometry and chunk length.
#[derive(Debug, Clone)]
pub struct ObjectMapper {
    sharding: Sharding,
    state_mode: StateMode,
    state_layout: StateLayout,
    has_state: bool,
    kv_size: u64,
    kv_with_state_size: u64,
}

impl ObjectMapper {
    /// Check the geometry ([`Geometry::sharding`]) and compute the object
    /// sizes for chunks of `chunk_tokens` tokens. A size that overflows `u64`
    /// is an error.
    pub fn new(geometry: &Geometry, chunk_tokens: u64) -> Result<Self, Error> {
        if chunk_tokens == 0 {
            return Err(Error::InvalidFormat("kvtrace: chunk_tokens is 0".into()));
        }
        let sharding = geometry.sharding()?;
        let has_state = geometry.sizes.state_bytes_per_sequence > 0;
        let kv_size = sharding
            .kv_per_token
            .checked_mul(chunk_tokens)
            .ok_or_else(overflow)?;
        let kv_with_state_size = if has_state {
            kv_size.checked_add(sharding.state).ok_or_else(overflow)?
        } else {
            kv_size
        };
        Ok(Self {
            sharding,
            state_mode: geometry.state_mode,
            state_layout: geometry.state_layout,
            has_state,
            kv_size,
            kv_with_state_size,
        })
    }

    /// The per-rank split the sizes come from.
    pub fn sharding(&self) -> &Sharding {
        &self.sharding
    }

    /// Whether the model has a recurrent state to store.
    pub fn has_state(&self) -> bool {
        self.has_state
    }

    /// The largest object this mapper can produce, in bytes.
    pub fn max_object_size(&self) -> u64 {
        match (self.has_state, self.state_layout) {
            (false, _) => self.kv_size,
            (true, StateLayout::Combined) => self.kv_with_state_size,
            (true, StateLayout::Separate) => self.kv_size.max(self.sharding.state),
        }
    }

    /// The ids a lookup probes for `chunk` at `rank`. A hybrid model's prefix
    /// can resume at the chunk only if its stored object carries state: with
    /// the combined layout the consumer knows that from the write that
    /// stored [`LookupIds::object`]; with the separate layout
    /// [`LookupIds::state`] must be present. See the
    /// [module docs](self#reuse).
    ///
    /// Errors if `chunk` does not fit 48 bits or `rank` is not a storing
    /// rank.
    pub fn lookup_ids(&self, chunk: u64, rank: u64) -> Result<LookupIds, Error> {
        if rank >= self.sharding.ranks {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: rank {rank} is not one of the {} storing ranks",
                self.sharding.ranks
            )));
        }
        let object = obj_id(chunk, ObjectKind::Kv, rank)?;
        let state = (self.has_state && self.state_layout == StateLayout::Separate)
            .then(|| pack(chunk, SLOT_STATE, rank));
        Ok(LookupIds { object, state })
    }

    /// The objects a request whose full chunks are `chunks` writes, in prefix
    /// order. Errors if a chunk id does not fit 48 bits or there are more
    /// than `u32::MAX` chunks (a chunk index is a `u32`).
    pub fn objects<'a>(&'a self, chunks: &'a [u64]) -> Result<Objects<'a>, Error> {
        if chunks.len() > u32::MAX as usize {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: a request has {} chunks, more than u32::MAX",
                chunks.len()
            )));
        }
        if let Some(&c) = chunks.iter().find(|&&c| c >= 1 << CHUNK_ID_BITS) {
            return Err(Error::InvalidFormat(format!(
                "kvtrace: chunk id {c} does not fit the {CHUNK_ID_BITS} bits of an obj_id"
            )));
        }
        Ok(Objects {
            mapper: self,
            chunks,
            chunk: 0,
            step: 0,
        })
    }

    /// `(kind, size)` of chunk `i`'s rank objects, and whether a separate
    /// state object follows them.
    fn chunk_plan(&self, i: usize, n: usize) -> (ObjectKind, u64, bool) {
        let snap = self.has_state
            && match self.state_mode {
                StateMode::PerChunk => true,
                StateMode::PerRequest => i + 1 == n,
            };
        match (snap, self.state_layout) {
            (true, StateLayout::Combined) => {
                (ObjectKind::KvWithState, self.kv_with_state_size, false)
            }
            (true, StateLayout::Separate) => (ObjectKind::Kv, self.kv_size, true),
            (false, _) => (ObjectKind::Kv, self.kv_size, false),
        }
    }
}

/// Iterator over a request's objects; see [`ObjectMapper::objects`].
#[derive(Debug, Clone)]
pub struct Objects<'a> {
    mapper: &'a ObjectMapper,
    chunks: &'a [u64],
    /// Index of the current chunk.
    chunk: usize,
    /// Position within the chunk: ranks `0..ranks` are its main objects,
    /// `ranks..2 * ranks` its separate state objects.
    step: u64,
}

impl Iterator for Objects<'_> {
    type Item = KvObject;

    fn next(&mut self) -> Option<KvObject> {
        let ranks = self.mapper.sharding.ranks;
        loop {
            let &chunk = self.chunks.get(self.chunk)?;
            let (kind, size, separate) = self.mapper.chunk_plan(self.chunk, self.chunks.len());
            let per_chunk = if separate { 2 * ranks } else { ranks };
            if self.step >= per_chunk {
                self.chunk += 1;
                self.step = 0;
                continue;
            }
            let (kind, size, rank) = if self.step < ranks {
                (kind, size, self.step)
            } else {
                (
                    ObjectKind::State,
                    self.mapper.sharding.state,
                    self.step - ranks,
                )
            };
            self.step += 1;
            return Some(KvObject {
                // Chunk ids and ranks were range-checked by objects() and
                // Geometry::sharding().
                obj_id: pack(chunk, kind.key_slot(), rank),
                size,
                rank,
                kind,
                // objects() checked that the chunk count fits u32.
                chunk_index: self.chunk as u32,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::ModelSizes;

    fn id(chunk: u64, kind: ObjectKind, rank: u64) -> u64 {
        obj_id(chunk, kind, rank).unwrap()
    }

    /// `(obj_id, size)` of each object.
    fn ids_sizes(g: &Geometry, chunk_tokens: u64, chunks: &[u64]) -> Vec<(u64, u64)> {
        let m = ObjectMapper::new(g, chunk_tokens).unwrap();
        m.objects(chunks)
            .unwrap()
            .map(|o| (o.obj_id, o.size))
            .collect()
    }

    #[test]
    fn obj_id_layout_and_fields() {
        assert_eq!(id(0, ObjectKind::Kv, 0), 0);
        assert_eq!(id(1, ObjectKind::Kv, 0), 1 << 16);
        // KV with state shares the chunk's id; a separate state object has
        // slot 1.
        assert_eq!(id(0, ObjectKind::KvWithState, 0), 0);
        assert_eq!(id(0, ObjectKind::State, 0), 1 << 12);
        assert_eq!(id(0, ObjectKind::Kv, 7), 7);
        assert_eq!(
            id(5 << 8 | 3, ObjectKind::State, 9),
            5 << 24 | 3 << 16 | 1 << 12 | 9
        );
        assert!(obj_id(1 << 48, ObjectKind::Kv, 0).is_err());
        assert!(obj_id(0, ObjectKind::Kv, 4096).is_err());
        for (c, k, r) in [
            (0, ObjectKind::Kv, 0),
            (5, ObjectKind::State, 9),
            ((1 << 48) - 1, ObjectKind::KvWithState, 4095),
        ] {
            assert_eq!(obj_id_fields(id(c, k, r)), (c, k.key_slot(), r));
        }
        assert_eq!(ObjectKind::Kv.key_slot(), SLOT_CHUNK);
        assert_eq!(ObjectKind::KvWithState.key_slot(), SLOT_CHUNK);
        assert_eq!(ObjectKind::State.key_slot(), SLOT_STATE);
        assert!(!ObjectKind::Kv.carries_state());
        assert!(ObjectKind::State.carries_state());
        assert!(ObjectKind::KvWithState.carries_state());
    }

    /// The chunks that carry state, by chunk index, are where a hybrid
    /// prefix can resume: the last chunk under per-request state, every chunk
    /// under per-chunk state, in either layout.
    #[test]
    fn resumable_chunks_follow_the_state_mode() {
        let resumable = |mode, layout| -> Vec<u32> {
            let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
            g.state_mode = mode;
            g.state_layout = layout;
            let m = ObjectMapper::new(&g, 256).unwrap();
            let mut idx: Vec<u32> = m
                .objects(&[1, 2, 3])
                .unwrap()
                .filter(|o| o.kind.carries_state())
                .map(|o| o.chunk_index)
                .collect();
            idx.dedup();
            idx
        };
        for layout in [StateLayout::Combined, StateLayout::Separate] {
            assert_eq!(resumable(StateMode::PerRequest, layout), [2]);
            assert_eq!(resumable(StateMode::PerChunk, layout), [0, 1, 2]);
        }
    }

    #[test]
    fn dense_one_object_per_chunk() {
        let g = Geometry::new(ModelSizes::explicit(10, 0));
        assert_eq!(
            ids_sizes(&g, 256, &[4, 9, 4]),
            [
                (id(4, ObjectKind::Kv, 0), 2560),
                (id(9, ObjectKind::Kv, 0), 2560),
                (id(4, ObjectKind::Kv, 0), 2560),
            ]
        );
        assert!(ids_sizes(&g, 256, &[]).is_empty());
    }

    #[test]
    fn tp_split_and_mla_first_rank() {
        let mut g = Geometry::new(ModelSizes::explicit(12, 0));
        g.tp = 4;
        let m = ObjectMapper::new(&g, 256).unwrap();
        let got: Vec<KvObject> = m.objects(&[7, 8]).unwrap().collect();
        let want: Vec<KvObject> = [(7, 0), (8, 1)]
            .iter()
            .flat_map(|&(c, i)| {
                (0..4).map(move |r| KvObject {
                    obj_id: id(c, ObjectKind::Kv, r),
                    size: 768,
                    rank: r,
                    kind: ObjectKind::Kv,
                    chunk_index: i,
                })
            })
            .collect();
        assert_eq!(got, want);

        g.sizes.mla = true;
        assert_eq!(ids_sizes(&g, 256, &[7]), [(id(7, ObjectKind::Kv, 0), 3072)]);
    }

    #[test]
    fn hybrid_state_modes_and_layouts() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
        g.tp = 2;
        let rows = |g: &Geometry| -> Vec<(u64, ObjectKind, u64, u64, u32)> {
            let m = ObjectMapper::new(g, 256).unwrap();
            m.objects(&[3, 4, 5])
                .unwrap()
                .map(|o| {
                    (
                        obj_id_fields(o.obj_id).0,
                        o.kind,
                        o.rank,
                        o.size,
                        o.chunk_index,
                    )
                })
                .collect()
        };
        use ObjectKind::{Kv, KvWithState, State};

        // per-request, combined: the last chunk carries the state.
        assert_eq!(
            rows(&g),
            [
                (3, Kv, 0, 1280, 0),
                (3, Kv, 1, 1280, 0),
                (4, Kv, 0, 1280, 1),
                (4, Kv, 1, 1280, 1),
                (5, KvWithState, 0, 1780, 2),
                (5, KvWithState, 1, 1780, 2),
            ]
        );

        // per-request, separate: the state objects follow the last chunk's
        // KV objects and share its index.
        g.state_layout = StateLayout::Separate;
        assert_eq!(
            rows(&g)[4..],
            [
                (5, Kv, 0, 1280, 2),
                (5, Kv, 1, 1280, 2),
                (5, State, 0, 500, 2),
                (5, State, 1, 500, 2),
            ]
        );

        // per-chunk, separate: every chunk has its state objects.
        g.state_mode = StateMode::PerChunk;
        let got = rows(&g);
        assert_eq!(got.len(), 12);
        for (i, chunk) in got.chunks(4).enumerate() {
            let c = 3 + i as u64;
            let i = i as u32;
            assert_eq!(
                chunk,
                [
                    (c, Kv, 0, 1280, i),
                    (c, Kv, 1, 1280, i),
                    (c, State, 0, 500, i),
                    (c, State, 1, 500, i),
                ]
            );
        }

        // per-chunk, combined: every chunk is KV with state.
        g.state_layout = StateLayout::Combined;
        let got = rows(&g);
        assert_eq!(got.len(), 6);
        assert!(got
            .iter()
            .all(|&(_, kind, _, size, _)| kind == KvWithState && size == 1780));
    }

    #[test]
    fn model_without_state_ignores_state_options() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 0));
        g.state_mode = StateMode::PerChunk;
        g.state_layout = StateLayout::Separate;
        assert_eq!(
            ids_sizes(&g, 256, &[1, 2]),
            [
                (id(1, ObjectKind::Kv, 0), 2560),
                (id(2, ObjectKind::Kv, 0), 2560)
            ]
        );
    }

    #[test]
    fn sizes_and_ids_are_checked() {
        // 2^56 + 1 bytes per token: 256 tokens wraps in u64.
        let g = Geometry::new(ModelSizes::explicit(72_057_594_037_927_937, 0));
        let err = ObjectMapper::new(&g, 256).unwrap_err().to_string();
        assert!(err.contains("overflow"), "{err}");
        // State added to a chunk's KV.
        let g = Geometry::new(ModelSizes::explicit(1, u64::MAX - 10));
        let err = ObjectMapper::new(&g, 256).unwrap_err().to_string();
        assert!(err.contains("overflow"), "{err}");
        let g = Geometry::new(ModelSizes::explicit(1, 0));
        assert!(ObjectMapper::new(&g, 0).is_err());

        let m = ObjectMapper::new(&g, 256).unwrap();
        let err = m.objects(&[1, 1 << 48]).unwrap_err().to_string();
        assert!(err.contains("48 bits"), "{err}");
        assert!(m.objects(&[(1 << 48) - 1]).is_ok());
    }

    #[test]
    fn max_object_size() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
        assert_eq!(ObjectMapper::new(&g, 256).unwrap().max_object_size(), 3560);
        // Separate: KV and state are never in one object.
        g.state_layout = StateLayout::Separate;
        assert_eq!(ObjectMapper::new(&g, 256).unwrap().max_object_size(), 2560);
        assert_eq!(ObjectMapper::new(&g, 4).unwrap().max_object_size(), 1000);
        let g = Geometry::new(ModelSizes::explicit(10, 0));
        assert_eq!(ObjectMapper::new(&g, 256).unwrap().max_object_size(), 2560);
    }

    #[test]
    fn lookup_ids_follow_the_layout() {
        let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
        g.tp = 2;
        let m = ObjectMapper::new(&g, 256).unwrap();
        assert_eq!(
            m.lookup_ids(7, 1).unwrap(),
            LookupIds {
                object: id(7, ObjectKind::Kv, 1),
                state: None
            }
        );
        g.state_layout = StateLayout::Separate;
        let m = ObjectMapper::new(&g, 256).unwrap();
        assert_eq!(
            m.lookup_ids(7, 1).unwrap(),
            LookupIds {
                object: id(7, ObjectKind::Kv, 1),
                state: Some(id(7, ObjectKind::State, 1))
            }
        );
        assert!(m.lookup_ids(7, 2).is_err());
        assert!(m.lookup_ids(1 << 48, 0).is_err());
        // No recurrent state: no state id even with the separate layout.
        let mut g = Geometry::new(ModelSizes::explicit(10, 0));
        g.state_layout = StateLayout::Separate;
        let m = ObjectMapper::new(&g, 256).unwrap();
        assert_eq!(m.lookup_ids(7, 0).unwrap().state, None);
    }

    /// A consumer's store: the last write to each id.
    type Store = std::collections::HashMap<u64, KvObject>;

    /// Store every object a request writes whose key is absent
    /// (insert-if-absent, LMCache's MP store `reserve_write(..., "new")`).
    fn fill(store: &mut Store, m: &ObjectMapper, chunks: &[u64]) {
        for o in m.objects(chunks).unwrap() {
            store.entry(o.obj_id).or_insert(o);
        }
    }

    /// The documented reuse rule: the last chunk index through which every
    /// rank's object is present and, for a hybrid model, the stored object
    /// carries state.
    fn resume_through(store: &Store, m: &ObjectMapper, chunks: &[u64]) -> Option<usize> {
        let mut last = None;
        for (i, &c) in chunks.iter().enumerate() {
            let mut present = true;
            let mut resumable = true;
            for rank in 0..m.sharding().ranks {
                let ids = m.lookup_ids(c, rank).unwrap();
                let Some(obj) = store.get(&ids.object) else {
                    present = false;
                    break;
                };
                if m.has_state() {
                    resumable &= match ids.state {
                        Some(s) => store.contains_key(&s),
                        None => obj.kind.carries_state(),
                    };
                }
            }
            if !present {
                break;
            }
            if resumable {
                last = Some(i);
            }
        }
        last
    }

    /// A = [1, 2, 3] then B = [1, 2, 3, 4]: B finds all of A's chunks and
    /// can resume through A's last chunk (index 2) in every mode and layout.
    /// With B = [1, 2, 4], only per-chunk state lets it resume (at index 1).
    #[test]
    fn extending_request_reuses_the_prefix() {
        for mode in [StateMode::PerRequest, StateMode::PerChunk] {
            for layout in [StateLayout::Combined, StateLayout::Separate] {
                let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
                g.tp = 2;
                g.state_mode = mode;
                g.state_layout = layout;
                let m = ObjectMapper::new(&g, 256).unwrap();
                let mut store = Store::new();
                fill(&mut store, &m, &[1, 2, 3]);
                let case = format!("{mode:?} {layout:?}");
                assert_eq!(resume_through(&store, &m, &[1, 2, 3, 4]), Some(2), "{case}");
                let diverging = resume_through(&store, &m, &[1, 2, 4]);
                match mode {
                    StateMode::PerRequest => assert_eq!(diverging, None, "{case}"),
                    StateMode::PerChunk => assert_eq!(diverging, Some(1), "{case}"),
                }
                // Under insert-if-absent B's fill adds only chunk 4, so A's
                // state-carrying object at chunk 3 is kept. Under
                // SET-from-miss it can be lost; see
                // set_from_miss_can_lose_a_snapshot.
                fill(&mut store, &m, &[1, 2, 3, 4]);
                assert_eq!(resume_through(&store, &m, &[1, 2, 3]), Some(2), "{case}");
            }
        }
        // Without recurrent state every present chunk is resumable.
        let g = Geometry::new(ModelSizes::explicit(10, 0));
        let m = ObjectMapper::new(&g, 256).unwrap();
        let mut store = Store::new();
        fill(&mut store, &m, &[1, 2, 3]);
        assert_eq!(resume_through(&store, &m, &[1, 2, 4]), Some(1));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WriteRule {
        /// Unconditional SET of the objects after the lookup's prefix hit
        /// (LMCache's remote backends).
        SetFromMiss,
        /// Write every object whose key is absent (LMCache's MP store).
        InsertIfAbsent,
    }

    /// Run one request against `store`: look up the prefix (a chunk counts
    /// only if every storing rank's object is present), find how many chunks
    /// can be reused (for a hybrid model, through the last chunk whose stored
    /// object carries state), then write under `rule`. Returns the reused
    /// chunk count.
    fn run(store: &mut Store, m: &ObjectMapper, chunks: &[u64], rule: WriteRule) -> usize {
        let mut prefix = 0;
        for &c in chunks {
            let all = (0..m.sharding().ranks)
                .all(|r| store.contains_key(&m.lookup_ids(c, r).unwrap().object));
            if !all {
                break;
            }
            prefix += 1;
        }
        let reused = resume_through(store, m, &chunks[..prefix]).map_or(0, |i| i + 1);
        for o in m.objects(chunks).unwrap() {
            match rule {
                WriteRule::SetFromMiss => {
                    if o.chunk_index as usize >= prefix {
                        store.insert(o.obj_id, o);
                    }
                }
                WriteRule::InsertIfAbsent => {
                    store.entry(o.obj_id).or_insert(o);
                }
            }
        }
        reused
    }

    fn hybrid(layout: StateLayout) -> ObjectMapper {
        let mut g = Geometry::new(ModelSizes::explicit(10, 1000));
        g.tp = 2;
        g.state_layout = layout;
        ObjectMapper::new(&g, 256).unwrap()
    }

    /// A = [1, 2, 3]; chunk 2 is evicted; B = [1, 2, 3, 4] runs; then A
    /// again. Per-request state. Under SET-from-miss with the combined layout
    /// B's KV-only write of chunk 3 replaces A's state-carrying object, and
    /// A reuses nothing; under insert-if-absent A reuses all 3 chunks (768
    /// tokens). The separate layout keeps A's state object either way.
    #[test]
    fn set_from_miss_can_lose_a_snapshot() {
        for (layout, rule, want) in [
            (StateLayout::Combined, WriteRule::SetFromMiss, 0),
            (StateLayout::Combined, WriteRule::InsertIfAbsent, 3),
            (StateLayout::Separate, WriteRule::SetFromMiss, 3),
            (StateLayout::Separate, WriteRule::InsertIfAbsent, 3),
        ] {
            let m = hybrid(layout);
            let mut store = Store::new();
            assert_eq!(run(&mut store, &m, &[1, 2, 3], rule), 0);
            store.retain(|&id, _| obj_id_fields(id).0 != 2);
            assert_eq!(run(&mut store, &m, &[1, 2, 3, 4], rule), 0);
            assert_eq!(
                run(&mut store, &m, &[1, 2, 3], rule),
                want,
                "{layout:?} {rule:?}"
            );
        }
    }

    /// A = [1, 2, 3], then [1, 2] twice, per-request state. [1, 2] finds
    /// both chunks present but not resumable. Under SET-from-miss nothing
    /// past the prefix hit is written, so it never reuses anything. Under
    /// insert-if-absent the separate layout stores the state object at chunk
    /// 2 (its key is new), so the second [1, 2] reuses 2 chunks (512
    /// tokens); with the combined layout chunk 2's key already exists, so the
    /// snapshot is never stored.
    #[test]
    fn present_but_not_resumable_chunks() {
        for (layout, rule, want) in [
            (StateLayout::Combined, WriteRule::SetFromMiss, 0),
            (StateLayout::Combined, WriteRule::InsertIfAbsent, 0),
            (StateLayout::Separate, WriteRule::SetFromMiss, 0),
            (StateLayout::Separate, WriteRule::InsertIfAbsent, 2),
        ] {
            let m = hybrid(layout);
            let mut store = Store::new();
            run(&mut store, &m, &[1, 2, 3], rule);
            assert_eq!(run(&mut store, &m, &[1, 2], rule), 0, "{layout:?} {rule:?}");
            assert_eq!(
                run(&mut store, &m, &[1, 2], rule),
                want,
                "{layout:?} {rule:?}"
            );
        }
    }

    /// A chunk counts only if every storing rank's object is present.
    #[test]
    fn a_missing_rank_ends_the_prefix() {
        let m = hybrid(StateLayout::Combined);
        let mut store = Store::new();
        run(&mut store, &m, &[1, 2, 3], WriteRule::InsertIfAbsent);
        assert_eq!(
            run(&mut store, &m, &[1, 2, 3], WriteRule::InsertIfAbsent),
            3
        );
        store.remove(&obj_id(3, ObjectKind::Kv, 1).unwrap());
        // Chunk 3 is missing on rank 1, and chunks 1-2 carry no state.
        assert_eq!(run(&mut store, &m, &[1, 2, 3], WriteRule::SetFromMiss), 0);
        // MLA: one storing rank, so rank 1 is not a lookup rank.
        let mut g = Geometry::new(ModelSizes::explicit(12, 0));
        g.tp = 4;
        g.sizes.mla = true;
        let m = ObjectMapper::new(&g, 256).unwrap();
        assert!(m.lookup_ids(1, 0).is_ok());
        assert!(m.lookup_ids(1, 1).is_err());
    }

    /// With the combined layout, one id is written with and without state.
    #[test]
    fn combined_layout_writes_one_id_at_two_sizes() {
        let g = Geometry::new(ModelSizes::explicit(10, 1000));
        let m = ObjectMapper::new(&g, 256).unwrap();
        let a: Vec<KvObject> = m.objects(&[1, 2, 3]).unwrap().collect();
        let b: Vec<KvObject> = m.objects(&[1, 2, 3, 4]).unwrap().collect();
        assert_eq!(a[2].obj_id, b[2].obj_id);
        assert_eq!((a[2].kind, a[2].size), (ObjectKind::KvWithState, 3560));
        assert_eq!((b[2].kind, b[2].size), (ObjectKind::Kv, 2560));
    }
}
