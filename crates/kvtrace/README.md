# kvtrace

KV-cache traces for LLM serving, used by
[cachesim](https://github.com/pelikan-io/cachesim):

- A request-level Parquet trace format: one row per inference request with
  the ids of its full prefix chunks (`chunks`, list of u64 below 2^48) and a
  per-file chunk length. The format does not depend on a model.
- Model geometry: KV bytes per token and recurrent-state bytes per sequence
  from a Hugging Face `config.json` (dense, MLA, and hybrid linear-attention
  models), and their split over tensor-parallel ranks following vLLM.
- The request-to-object mapping: for a request's chunks and a geometry,
  `ObjectMapper::objects` yields each KV-cache object's id, size, rank, kind
  and chunk index, in prefix order. A load generator and the simulator get
  the same keys and sizes from it.
- A converter from the public Mooncake trace (FAST '25) to the request
  format.

The `hf-fetch` feature (on by default) adds `hf::fetch_config`, which
downloads a model's `config.json` from Hugging Face. With
`default-features = false` the crate has no HTTP client.

Dense and MLA geometry is checked against vLLM and LMCache. Hybrid
(linear-attention) geometry is an approximation pending a follow-up: object
sizes, the chunk length vLLM and LMCache allow, and the number of state
snapshots differ from them; the `objects` module docs list the differences.

kvtrace does not depend on the object-level trace format (`cachetrace`);
`cachesim kv expand` combines the two. See the
[cachesim README](https://github.com/pelikan-io/cachesim#readme) for the
format, the geometry rules and their sources.

License: Apache-2.0
