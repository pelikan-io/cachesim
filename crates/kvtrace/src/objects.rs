//! Request to objects: the KV-cache objects a request reads, with their ids
//! and sizes, for one model geometry.
//!
//! A request's chunks are its full prefix chunks in order. Each chunk becomes
//! one object per storing tensor-parallel rank; a hybrid model's recurrent
//! state is added to a chunk's object or stored beside it, at the request's
//! last chunk or at every chunk ([`StateMode`], [`StateLayout`]).
//!
//! Objects come out in prefix order: chunk by chunk, each chunk's objects in
//! rank order, then its separate state objects (if any) in rank order.

use crate::geometry::{overflow, Geometry, Sharding, StateLayout, StateMode};
use crate::Error;

/// Bits of an object id that hold the chunk id.
pub const CHUNK_ID_BITS: u32 = 48;
pub(crate) const KIND_BITS: u32 = 4;
pub(crate) const RANK_BITS: u32 = 12;

/// What an object holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// Per-token KV of one chunk.
    Kv,
    /// A recurrent-state snapshot on its own.
    State,
    /// A chunk's KV and a state snapshot in one object. A distinct kind
    /// because its bytes differ from the same chunk's [`Kv`](Self::Kv) object.
    KvWithState,
}

impl ObjectKind {
    /// The kind's 4-bit code in an object id: 0, 1, 2.
    pub fn code(self) -> u64 {
        match self {
            Self::Kv => 0,
            Self::State => 1,
            Self::KvWithState => 2,
        }
    }

    /// Inverse of [`code`](Self::code).
    pub fn from_code(code: u64) -> Option<Self> {
        match code {
            0 => Some(Self::Kv),
            1 => Some(Self::State),
            2 => Some(Self::KvWithState),
            _ => None,
        }
    }
}

/// Pack `(chunk, kind, rank)` into an object id:
/// `chunk << 16 | kind << 12 | rank`.
///
/// Errors if `chunk` does not fit 48 bits or `rank` 12 bits.
pub fn obj_id(chunk: u64, kind: ObjectKind, rank: u64) -> Result<u64, Error> {
    if chunk >= 1 << CHUNK_ID_BITS || rank >= 1 << RANK_BITS {
        return Err(Error::InvalidFormat(format!(
            "kvtrace: obj_id field out of range (chunk {chunk}, rank {rank})"
        )));
    }
    Ok((chunk << KIND_BITS | kind.code()) << RANK_BITS | rank)
}

/// Unpack an object id made by [`obj_id`] into `(chunk, kind code, rank)`.
pub fn obj_id_fields(id: u64) -> (u64, u64, u64) {
    (
        id >> (KIND_BITS + RANK_BITS),
        id >> RANK_BITS & ((1 << KIND_BITS) - 1),
        id & ((1 << RANK_BITS) - 1),
    )
}

/// Text for trace metadata describing the [`obj_id`] layout.
pub fn obj_id_layout() -> String {
    format!(
        "chunk<<{} | kind<<{RANK_BITS} | rank; \
         kind 0 = KV, 1 = recurrent state, 2 = KV and state in one object",
        KIND_BITS + RANK_BITS
    )
}

/// One object a request reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvObject {
    /// Object id ([`obj_id`]).
    pub obj_id: u64,
    /// Bytes this rank stores for the object.
    pub size: u64,
    /// Tensor-parallel rank that stores the object.
    pub rank: u64,
    pub kind: ObjectKind,
    /// 0-based position of the object's chunk in the request's prefix. Every
    /// rank's object for a chunk, and its separate state objects, share it.
    pub chunk_index: u32,
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

    /// The objects of a request whose full chunks are `chunks`, in prefix
    /// order. Errors if a chunk id does not fit 48 bits or there are more than
    /// `u32::MAX` chunks.
    pub fn objects<'a>(&'a self, chunks: &'a [u64]) -> Result<Objects<'a>, Error> {
        if chunks.len() > u32::MAX as usize {
            return Err(Error::InvalidFormat(
                "kvtrace: a request has more than 2^32 chunks".into(),
            ));
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
                obj_id: (chunk << KIND_BITS | kind.code()) << RANK_BITS | rank,
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
        assert_eq!(id(0, ObjectKind::KvWithState, 0), 2 << 12);
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
            assert_eq!(obj_id_fields(id(c, k, r)), (c, k.code(), r));
        }
        for k in [ObjectKind::Kv, ObjectKind::State, ObjectKind::KvWithState] {
            assert_eq!(ObjectKind::from_code(k.code()), Some(k));
        }
        assert_eq!(ObjectKind::from_code(3), None);
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
}
