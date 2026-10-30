# cachetrace

Object-level cache trace format used by
[cachesim](https://github.com/pelikan-io/cachesim): one row per cache request,
stored as Parquet with a schema compatible with libCacheSim's `oracleGeneral`
format (`timestamp`, `obj_id`, `obj_size`, `next_access_vtime`, plus optional
`op` and `ttl`).

- `trace_schema()`, `TraceEntry`, `Op`
- `TraceReader`: streams rows one record batch at a time
- `TraceWriter`: writes ZSTD-compressed Parquet, with footer key-value metadata

The `import` feature (on by default) adds `convert_bin_to_parquet` for
libCacheSim `oracleGeneral` / `oracleGeneralOpNs` binaries and
`convert_cache_trace_to_parquet` for
[pelikan-io/cache-trace](https://github.com/pelikan-io/cache-trace) CSV
and `convert_meta_kv_to_parquet` for Meta's key-value cache trace CSV (both
optionally zstd-compressed). With `default-features = false` the crate is
the schema, row type, reader and writer only; it then has no direct
dependency on `ahash` or `zstd`, though both are still built through `arrow`
and `parquet`.

See the [cachesim README](https://github.com/pelikan-io/cachesim#readme) for
the full format description.

License: Apache-2.0
