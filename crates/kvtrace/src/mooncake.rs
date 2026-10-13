//! Conversion of the public Mooncake trace (FAST '25) into a request trace
//! ([`crate::format`]).
//!
//! A Mooncake trace is JSON lines, one inference request per line:
//!
//! ```text
//! {"timestamp": 27482, "input_length": 6955, "output_length": 52, "hash_ids": [46, 47, ...]}
//! ```
//!
//! `timestamp` is in milliseconds and each entry of `hash_ids` names one
//! 512-token block of the prompt; equal ids mean equal prefix content.
//!
//! # Chunks
//!
//! Each block is split into `512 / chunk_tokens` chunks (default 256 tokens,
//! LMCache v0.5.5 `lmcache/v1/config.py:90`). Chunk `c` of block `h` has id
//! `h << 8 | c` ([`chunk_id`]), so equal blocks give equal chunks.
//!
//! Only full chunks are kept, matching LMCache's default
//! `save_unfull_chunk = False`. Every block but the last holds 512 tokens.
//! The last holds `input_length - 512 * (n - 1)` tokens for `n` ids, clamped
//! to `1..=512` because `input_length` and `hash_ids` can disagree:
//!
//! * `input_length > 512 * n`: the prompt is longer than the blocks; the last
//!   block is taken as full. Counted in `kvtrace.converter.input_beyond_blocks`.
//! * `input_length < 512 * (n - 1) + 1`: the last block would hold no token;
//!   it is taken as 1 token, so it has no full chunk. Counted in
//!   `kvtrace.converter.input_short_of_blocks`.
//!
//! Chunks are counted from `hash_ids`, as above, in both cases.
//!
//! # One prefix, two chunk ids
//!
//! A Mooncake hash id covers a whole block, so a partial last block and the
//! full block that later extends it have different ids even when their first
//! tokens agree. When a request's last block is partial with at least
//! `chunk_tokens` tokens, its full sub-chunk 0 is keyed on the partial
//! block's id; a later request with the same prefix that holds the block in
//! full reads that sub-chunk under the full block's id. LMCache, which keys a
//! chunk on the token prefix, would hit; the converted trace misses, so
//! prefix reuse is undercounted. The trace cannot show whether the tokens
//! agree, so this is an upper bound. On the public Mooncake traces at 256
//! tokens per chunk, later full-chunk reads of a block position where an
//! earlier request with the same prefix stored a partial block under another
//! id: conversation 14,844 of 559,542 (2.7%), toolagent 21,254 of 782,217
//! (2.7%), synthetic 3,001 of 237,347 (1.3%).
//! `input_tokens` is `input_length` as given, `output_tokens` is
//! `output_length`, `req_id` is the request's 0-based index among the
//! non-empty input lines, and `session_id` is null.
//!
//! # Input and output
//!
//! The input is read once, but it must be an uncompressed regular file:
//! directories, pipes and `.zst`/`.zstd`/`.gz` files are rejected. Its
//! SHA-256 is recorded. Output handling (refusals, symlinks, the temporary
//! file) is described on [`plan_output`].

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::format::{Request, RequestWriter};
use crate::output::{describe_file_type, plan_output};
use crate::{hex, utc_now_iso8601, Error};

/// Tokens per Mooncake `hash_ids` block.
pub const SOURCE_BLOCK_TOKENS: u64 = 512;

const HASH_ID_BITS: u32 = 40;
const SUB_CHUNK_BITS: u32 = 8;

/// Chunk id of sub-chunk `sub` of the block with hash id `hash_id`:
/// `hash_id << 8 | sub`. Errors if `hash_id` does not fit 40 bits or `sub` 8
/// bits, so the id fits the 48 bits an object id has for it.
pub fn chunk_id(hash_id: u64, sub: u64) -> Result<u64, Error> {
    if hash_id >= 1 << HASH_ID_BITS || sub >= 1 << SUB_CHUNK_BITS {
        return Err(Error::InvalidFormat(format!(
            "mooncake: chunk id field out of range (hash_id {hash_id}, sub-chunk {sub})"
        )));
    }
    Ok(hash_id << SUB_CHUNK_BITS | sub)
}

/// Inverse of [`chunk_id`]: `(hash_id, sub)`.
pub fn chunk_id_fields(chunk: u64) -> (u64, u64) {
    (chunk >> SUB_CHUNK_BITS, chunk & ((1 << SUB_CHUNK_BITS) - 1))
}

/// Settings for [`convert_mooncake`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MooncakeOptions {
    /// Tokens per chunk; must divide [`SOURCE_BLOCK_TOKENS`] into at most 256
    /// chunks.
    pub chunk_tokens: u64,
}

impl Default for MooncakeOptions {
    /// 256-token chunks.
    fn default() -> Self {
        Self { chunk_tokens: 256 }
    }
}

impl MooncakeOptions {
    fn validate(&self) -> Result<(), Error> {
        let c = self.chunk_tokens;
        if c == 0
            || !SOURCE_BLOCK_TOKENS.is_multiple_of(c)
            || SOURCE_BLOCK_TOKENS / c > 1 << SUB_CHUNK_BITS
        {
            return Err(Error::InvalidFormat(format!(
                "mooncake: chunk tokens ({c}) must divide the source block \
                 ({SOURCE_BLOCK_TOKENS}) into at most {} chunks",
                1 << SUB_CHUNK_BITS
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct MooncakeRecord {
    timestamp: i64,
    input_length: i64,
    output_length: i64,
    hash_ids: Vec<u64>,
}

/// How a request's `input_length` relates to its `hash_ids`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Agreement {
    /// `512 * (n - 1) < input_length <= 512 * n`.
    Agrees,
    /// `input_length > 512 * n`.
    Beyond,
    /// `input_length < 512 * (n - 1) + 1`.
    Short,
}

/// One parsed line: the request (with `req_id` unset) and how its length
/// agrees with its blocks.
fn parse_request(
    line: &str,
    lineno: usize,
    chunk_tokens: u64,
) -> Result<(Request, Agreement), Error> {
    let err = |msg: String| Error::InvalidFormat(format!("mooncake line {lineno}: {msg}"));
    let r: MooncakeRecord = serde_json::from_str(line).map_err(|e| err(e.to_string()))?;
    if r.input_length < 0 {
        return Err(err(format!("negative input_length {}", r.input_length)));
    }
    let input_tokens = u32::try_from(r.input_length).map_err(|_| {
        err(format!(
            "input_length {} exceeds the input_tokens column's u32 range",
            r.input_length
        ))
    })?;
    let output_tokens = u32::try_from(r.output_length).map_err(|_| {
        err(format!(
            "output_length {} is not in the output_tokens column's u32 range",
            r.output_length
        ))
    })?;
    let n = r.hash_ids.len() as i64;
    let block = SOURCE_BLOCK_TOKENS as i64;
    // Tokens in the last block before clamping; i64 arithmetic cannot
    // overflow for a u32 input_length and a hash list that fits in memory.
    let last = r.input_length - block * n.saturating_sub(1);
    let agreement = if r.input_length > block * n {
        Agreement::Beyond
    } else if n > 0 && last < 1 {
        Agreement::Short
    } else {
        Agreement::Agrees
    };
    let last = last.clamp(1, block) as u64;

    let per_block = SOURCE_BLOCK_TOKENS / chunk_tokens;
    let mut chunks = Vec::with_capacity(r.hash_ids.len() * per_block as usize);
    for (i, &h) in r.hash_ids.iter().enumerate() {
        if h >= 1 << HASH_ID_BITS {
            return Err(err(format!("hash id {h} does not fit {HASH_ID_BITS} bits")));
        }
        let tokens = if i + 1 < r.hash_ids.len() {
            SOURCE_BLOCK_TOKENS
        } else {
            last
        };
        for c in 0..tokens / chunk_tokens {
            chunks.push(chunk_id(h, c)?);
        }
    }
    let timestamp = r
        .timestamp
        .checked_mul(1_000_000)
        .ok_or_else(|| err("timestamp out of range".into()))?;
    Ok((
        Request {
            timestamp,
            req_id: 0,
            input_tokens,
            output_tokens,
            chunks,
            session_id: None,
        },
        agreement,
    ))
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

/// Result of [`convert_mooncake`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MooncakeSummary {
    /// Requests (non-empty input lines) written.
    pub requests: u64,
    /// Full chunks over all requests.
    pub chunks: u64,
    /// Requests with no full chunk.
    pub requests_without_chunks: u64,
    /// Requests whose `input_length` exceeds `512 × len(hash_ids)`.
    pub input_beyond_blocks: u64,
    /// Requests whose `input_length` leaves the last block empty
    /// (`< 512 × (len(hash_ids) - 1) + 1`).
    pub input_short_of_blocks: u64,
}

/// Reader that feeds every byte it returns into a SHA-256.
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Convert a Mooncake trace (JSON lines) into a request trace. See the
/// [module documentation](self).
pub fn convert_mooncake(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    opts: &MooncakeOptions,
    batch_size: usize,
) -> Result<MooncakeSummary, Error> {
    opts.validate()?;
    let input = input.as_ref();
    let output = output.as_ref();
    // Checked by path first because opening a directory fails on Windows.
    let not_regular = |meta: &std::fs::Metadata| {
        Error::InvalidFormat(format!(
            "mooncake: {} is {}, not a regular file",
            input.display(),
            describe_file_type(meta)
        ))
    };
    if let Ok(meta) = std::fs::metadata(input) {
        if meta.is_dir() {
            return Err(not_regular(&meta));
        }
    }
    let file = File::open(input)?;
    let input_meta = file.metadata()?;
    if !input_meta.is_file() {
        return Err(not_regular(&input_meta));
    }
    if input.extension().is_some_and(|e| {
        let e = e.to_string_lossy().to_ascii_lowercase();
        e == "zst" || e == "zstd" || e == "gz"
    }) {
        return Err(Error::InvalidFormat(format!(
            "mooncake: {} looks compressed; decompress it first",
            input.display()
        )));
    }
    let out = plan_output(input, &input_meta, output)?;
    let created = utc_now_iso8601();
    let chunk_tokens = u32::try_from(opts.chunk_tokens).expect("validated: at most 512");

    let write = || -> Result<MooncakeSummary, Error> {
        let mut writer = RequestWriter::create(out.target(), chunk_tokens, batch_size)?;
        let mut reader = BufReader::new(HashingReader {
            inner: file,
            hasher: Sha256::new(),
        });
        let mut summary = MooncakeSummary::default();
        let mut line = String::new();
        let mut lineno = 0;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            lineno += 1;
            if line.trim().is_empty() {
                continue;
            }
            let (mut req, agreement) = parse_request(&line, lineno, opts.chunk_tokens)?;
            req.req_id = summary.requests;
            summary.requests += 1;
            summary.chunks += req.chunks.len() as u64;
            summary.requests_without_chunks += u64::from(req.chunks.is_empty());
            match agreement {
                Agreement::Agrees => {}
                Agreement::Beyond => summary.input_beyond_blocks += 1,
                Agreement::Short => summary.input_short_of_blocks += 1,
            }
            writer.write(&req)?;
        }
        let sha256 = hex(&reader.into_inner().hasher.finalize());
        let file_name = input
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        for (k, v) in [
            ("kvtrace.created", created.clone()),
            ("kvtrace.source.format", "mooncake".to_string()),
            ("kvtrace.source.file", file_name),
            ("kvtrace.source.sha256", sha256),
            (
                "kvtrace.source.block_tokens",
                SOURCE_BLOCK_TOKENS.to_string(),
            ),
            (
                "kvtrace.converter.chunk_id",
                "hash_id<<8 | sub-chunk index within the 512-token block".to_string(),
            ),
            (
                "kvtrace.converter.partial_chunks",
                "dropped (only full chunks are listed)".to_string(),
            ),
            ("kvtrace.converter.session_id", "null".to_string()),
            ("kvtrace.requests", summary.requests.to_string()),
            ("kvtrace.chunks", summary.chunks.to_string()),
            (
                "kvtrace.converter.requests_without_chunks",
                summary.requests_without_chunks.to_string(),
            ),
            (
                "kvtrace.converter.input_beyond_blocks",
                summary.input_beyond_blocks.to_string(),
            ),
            (
                "kvtrace.converter.input_short_of_blocks",
                summary.input_short_of_blocks.to_string(),
            ),
        ] {
            writer.append_key_value_metadata(k, v);
        }
        if out.temp.is_some() {
            writer.finish_synced()?;
        } else {
            writer.finish()?;
        }
        Ok(summary)
    };
    match write().and_then(|s| out.commit().map(|()| s)) {
        Ok(s) => Ok(s),
        Err(e) => {
            out.discard();
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::RequestReader;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mooncake")
            .join(name)
    }

    /// An input whose second line does not parse.
    fn bad_input() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");
        std::fs::write(
            &path,
            "{\"timestamp\": 0, \"input_length\": 512, \"output_length\": 1, \"hash_ids\": [1]}\n\
             not json\n",
        )
        .unwrap();
        (dir, path)
    }

    fn line(input_length: i64, ids: &[u64]) -> String {
        format!(
            r#"{{"timestamp": 0, "input_length": {input_length}, "output_length": 3, "hash_ids": {ids:?}}}"#
        )
    }

    /// `(hash_id, sub)` of each chunk and the agreement case.
    fn chunks(input_length: i64, ids: &[u64], chunk_tokens: u64) -> (Vec<(u64, u64)>, Agreement) {
        let (r, a) = parse_request(&line(input_length, ids), 1, chunk_tokens).unwrap();
        (r.chunks.iter().map(|&c| chunk_id_fields(c)).collect(), a)
    }

    #[test]
    fn chunk_id_layout() {
        assert_eq!(chunk_id(0, 0).unwrap(), 0);
        assert_eq!(chunk_id(5, 3).unwrap(), 5 << 8 | 3);
        assert_eq!(
            chunk_id_fields(chunk_id((1 << 40) - 1, 255).unwrap()),
            ((1 << 40) - 1, 255)
        );
        assert!(chunk_id(1 << 40, 0).is_err());
        assert!(chunk_id(0, 256).is_err());
    }

    #[test]
    fn full_chunks_only() {
        use Agreement::*;
        // 700 tokens: block 1 has 188 tokens, less than a chunk.
        assert_eq!(chunks(700, &[0, 1], 256), (vec![(0, 0), (0, 1)], Agrees));
        // 812 tokens: block 1 has 300 tokens, one full chunk and 44 dropped.
        assert_eq!(
            chunks(812, &[0, 1], 256),
            (vec![(0, 0), (0, 1), (1, 0)], Agrees)
        );
        // 1024 tokens: both blocks full.
        assert_eq!(
            chunks(1024, &[0, 1], 256),
            (vec![(0, 0), (0, 1), (1, 0), (1, 1)], Agrees)
        );
        // 1100 tokens over 3 blocks: 512 + 512 + 76; the 76 are dropped.
        assert_eq!(chunks(1100, &[0, 1, 2], 256).0.len(), 4);
        // Fewer tokens than one chunk: no chunks at all.
        assert_eq!(chunks(255, &[6], 256), (vec![], Agrees));
        // 128-token chunks: block 1 of 680 tokens has 168, one full chunk.
        assert_eq!(
            chunks(680, &[0, 1], 128),
            (vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0)], Agrees)
        );
    }

    #[test]
    fn input_length_disagreeing_with_hash_ids() {
        use Agreement::*;
        // Beyond the blocks: the last block is taken as full.
        assert_eq!(chunks(2000, &[3], 256), (vec![(3, 0), (3, 1)], Beyond));
        assert_eq!(chunks(513, &[3], 256).1, Beyond);
        assert_eq!(chunks(512, &[3], 256).1, Agrees);
        // Short of the blocks: the last block has 1 token and no chunk, the
        // earlier full block keeps both of its chunks.
        assert_eq!(chunks(300, &[4, 5], 256), (vec![(4, 0), (4, 1)], Short));
        assert_eq!(chunks(512, &[4, 5], 256).1, Short);
        assert_eq!(chunks(513, &[4, 5], 256).1, Agrees);
        // No ids: no chunks; any positive length is beyond them.
        assert_eq!(chunks(0, &[], 256), (vec![], Agrees));
        assert_eq!(chunks(10, &[], 256), (vec![], Beyond));
    }

    #[test]
    fn lines_are_validated() {
        let err = |l: &str| {
            parse_request(l, 3, 256)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        };
        let e = err(
            r#"{"timestamp": 0, "input_length": -9223372036854775808, "output_length": 0, "hash_ids": [1, 2]}"#,
        );
        assert!(
            e.contains("line 3") && e.contains("negative input_length"),
            "{e}"
        );
        let e = err(
            r#"{"timestamp": 0, "input_length": 4294967296, "output_length": 0, "hash_ids": [1]}"#,
        );
        assert!(e.contains("u32"), "{e}");
        let e = err(r#"{"timestamp": 0, "input_length": 1, "output_length": -1, "hash_ids": [1]}"#);
        assert!(e.contains("output_length"), "{e}");
        let e = err(r#"{"timestamp": 0, "input_length": 1, "hash_ids": [1]}"#);
        assert!(e.contains("output_length"), "{e}");
        let e = err(
            r#"{"timestamp": 0, "input_length": 1, "output_length": 0, "hash_ids": [1099511627776]}"#,
        );
        assert!(e.contains("40 bits"), "{e}");
        let e = err(
            r#"{"timestamp": 9223372036854775807, "input_length": 1, "output_length": 0, "hash_ids": []}"#,
        );
        assert!(e.contains("timestamp"), "{e}");
        let (r, _) = parse_request(
            r#"{"timestamp": 7, "input_length": 4294967295, "output_length": 9, "hash_ids": [1]}"#,
            1,
            256,
        )
        .unwrap();
        assert_eq!(
            (r.timestamp, r.input_tokens, r.output_tokens),
            (7_000_000, u32::MAX, 9)
        );
    }

    #[test]
    fn options_are_validated() {
        for (c, ok) in [
            (300, false),
            (0, false),
            (1, false),
            (2, true),
            (256, true),
            (512, true),
        ] {
            let opts = MooncakeOptions { chunk_tokens: c };
            assert_eq!(opts.validate().is_ok(), ok, "chunk_tokens {c}");
        }
    }

    #[test]
    fn input_must_be_a_regular_uncompressed_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        let opts = MooncakeOptions::default();
        let err = convert_mooncake(dir.path(), &out, &opts, 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a directory"), "{err}");
        for name in ["trace.jsonl.zst", "trace.jsonl.ZST", "trace.jsonl.Gz"] {
            let path = dir.path().join(name);
            std::fs::write(&path, b"").unwrap();
            let err = convert_mooncake(&path, &out, &opts, 4)
                .unwrap_err()
                .to_string();
            assert!(err.contains("decompress"), "{name}: {err}");
        }
        assert!(!out.exists());
    }

    fn convert_fixture(opts: &MooncakeOptions) -> (MooncakeSummary, RequestReader) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        // Batch size 3 so the output spans several record batches.
        let summary = convert_mooncake(fixture("trace.jsonl"), &out, opts, 3).unwrap();
        (summary, RequestReader::open(&out).unwrap())
    }

    #[test]
    fn end_to_end() {
        let (summary, reader) = convert_fixture(&MooncakeOptions::default());
        assert_eq!(
            summary,
            MooncakeSummary {
                requests: 4,
                chunks: 10,
                requests_without_chunks: 0,
                input_beyond_blocks: 1,
                input_short_of_blocks: 1,
            }
        );
        assert_eq!(reader.chunk_tokens(), 256);
        let meta = reader.metadata().clone();
        let rows: Vec<Request> = reader.collect::<Result<_, _>>().unwrap();
        let c = |h, s| chunk_id(h, s).unwrap();
        let want = [
            // t = 0 ms, 700 tokens: [0, 1]; block 1's 188 tokens dropped.
            (0, 700, 5, vec![c(0, 0), c(0, 1)]),
            // t = 3 ms, 1100 tokens: [0, 1, 2]; block 2's 76 tokens dropped.
            (3, 1100, 5, vec![c(0, 0), c(0, 1), c(1, 0), c(1, 1)]),
            // t = 5 ms, 2000 tokens: [3], beyond the one block.
            (5, 2000, 1, vec![c(3, 0), c(3, 1)]),
            // t = 7 ms, 300 tokens: [4, 5], short of the two blocks.
            (7, 300, 1, vec![c(4, 0), c(4, 1)]),
        ];
        assert_eq!(rows.len(), want.len());
        for (i, (r, (ms, input, output, chunks))) in rows.iter().zip(want).enumerate() {
            assert_eq!(r.req_id, i as u64);
            assert_eq!(r.timestamp, ms * 1_000_000, "request {i}");
            assert_eq!(
                (r.input_tokens, r.output_tokens),
                (input, output),
                "request {i}"
            );
            assert_eq!(r.chunks, chunks, "request {i}");
            assert_eq!(r.session_id, None);
        }

        let sha = crate::sha256_hex(&std::fs::read(fixture("trace.jsonl")).unwrap());
        for (k, v) in [
            ("kvtrace.format", "requests"),
            ("kvtrace.version", "1"),
            ("kvtrace.chunk_tokens", "256"),
            ("kvtrace.source.format", "mooncake"),
            ("kvtrace.source.file", "trace.jsonl"),
            ("kvtrace.source.sha256", sha.as_str()),
            ("kvtrace.source.block_tokens", "512"),
            ("kvtrace.requests", "4"),
            ("kvtrace.chunks", "10"),
            ("kvtrace.converter.requests_without_chunks", "0"),
            ("kvtrace.converter.input_beyond_blocks", "1"),
            ("kvtrace.converter.input_short_of_blocks", "1"),
        ] {
            assert_eq!(meta.get(k).map(String::as_str), Some(v), "{k}");
        }
        assert!(meta["kvtrace.created"].ends_with("+00:00"));
        assert!(meta["kvtrace.converter.chunk_id"].starts_with("hash_id<<8"));
    }

    #[test]
    fn blank_lines_take_no_request_id() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("trace.jsonl");
        std::fs::write(
            &input,
            "\n{\"timestamp\": 0, \"input_length\": 300, \"output_length\": 1, \"hash_ids\": [1]}\n\
             \n\
             {\"timestamp\": 1, \"input_length\": 0, \"output_length\": 1, \"hash_ids\": []}\n\
             {\"timestamp\": 2, \"input_length\": 600, \"output_length\": 1, \"hash_ids\": [2, 3]}\n",
        )
        .unwrap();
        let out = dir.path().join("out.parquet");
        let summary = convert_mooncake(&input, &out, &MooncakeOptions::default(), 4).unwrap();
        assert_eq!((summary.requests, summary.requests_without_chunks), (3, 1));
        let got: Vec<(u64, usize)> = RequestReader::open(&out)
            .unwrap()
            .map(|r| r.map(|r| (r.req_id, r.chunks.len())))
            .collect::<Result<_, _>>()
            .unwrap();
        // The request without hash_ids is id 1 and has no chunks.
        assert_eq!(got, [(0, 1), (1, 0), (2, 2)]);
    }

    #[test]
    fn output_never_overwrites_the_input() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("trace.jsonl");
        std::fs::copy(fixture("trace.jsonl"), &input).unwrap();
        let before = std::fs::read(&input).unwrap();
        let opts = MooncakeOptions::default();

        let mut outputs = vec![input.clone(), dir.path().join(".").join("trace.jsonl")];
        #[cfg(unix)]
        {
            let link = dir.path().join("link.parquet");
            std::os::unix::fs::symlink(&input, &link).unwrap();
            let hard = dir.path().join("hard.parquet");
            std::fs::hard_link(&input, &hard).unwrap();
            outputs.extend([link, hard]);
        }
        for out in outputs {
            let err = convert_mooncake(&input, &out, &opts, 4)
                .err()
                .unwrap()
                .to_string();
            assert!(
                err.contains("is the input file"),
                "{}: {err}",
                out.display()
            );
            assert_eq!(std::fs::read(&input).unwrap(), before);
        }
    }

    #[test]
    fn output_is_replaced_only_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        std::fs::write(&out, b"keep").unwrap();

        // A failing run leaves the existing output and no temporary file.
        let opts = MooncakeOptions::default();
        let (_bad_dir, bad) = bad_input();
        assert!(convert_mooncake(&bad, &out, &opts, 4).is_err());
        assert_eq!(std::fs::read(&out).unwrap(), b"keep");

        // A read-only output is refused up front and left as it was.
        let mut perms = std::fs::metadata(&out).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&out, perms.clone()).unwrap();
        let err = convert_mooncake(fixture("trace.jsonl"), &out, &opts, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("read-only"), "{err}");
        assert_eq!(std::fs::read(&out).unwrap(), b"keep");
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&out, perms).unwrap();

        // A successful run replaces a writable output.
        convert_mooncake(fixture("trace.jsonl"), &out, &opts, 4).unwrap();
        assert_eq!(RequestReader::open(&out).unwrap().total_requests(), 4);
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["out.parquet"]);
    }

    #[test]
    fn output_directory_is_refused_before_any_work() {
        let dir = tempfile::tempdir().unwrap();
        // An input that fails to parse: the output check comes first.
        let (_bad_dir, bad) = bad_input();
        let opts = MooncakeOptions::default();
        let err = convert_mooncake(&bad, dir.path(), &opts, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("is a directory"), "{err}");
        assert!(err.contains(&*dir.path().to_string_lossy()), "{err}");
    }

    #[test]
    fn temp_names_are_unique_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let input = fixture("trace.jsonl");
        let meta = std::fs::metadata(&input).unwrap();
        let out = dir.path().join("out.parquet");
        let a = plan_output(&input, &meta, &out).unwrap();
        let b = plan_output(&input, &meta, &out).unwrap();
        assert_eq!(a.dest, out);
        assert_ne!(a.temp.unwrap(), b.temp.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_output_writes_through_the_link() {
        let dir = tempfile::tempdir().unwrap();
        let real_dir = dir.path().join("real");
        std::fs::create_dir(&real_dir).unwrap();
        let target = real_dir.join("target.parquet");
        std::fs::write(&target, b"old").unwrap();
        let link = dir.path().join("link.parquet");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let opts = MooncakeOptions::default();
        convert_mooncake(fixture("trace.jsonl"), &link, &opts, 4).unwrap();
        assert!(std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(RequestReader::open(&target).unwrap().total_requests(), 4);
        let names: Vec<_> = std::fs::read_dir(&real_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["target.parquet"]);

        // A dangling link is refused.
        let dangling = dir.path().join("dangling.parquet");
        std::os::unix::fs::symlink(dir.path().join("missing/x.parquet"), &dangling).unwrap();
        let err = convert_mooncake(fixture("trace.jsonl"), &dangling, &opts, 4)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("symlink"), "{err}");
    }

    /// The writer cannot create its temporary file: the existing output must
    /// survive.
    #[cfg(unix)]
    #[test]
    fn unwritable_output_directory_keeps_existing_output() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("out");
        std::fs::create_dir(&sub).unwrap();
        let out = sub.join("out.parquet");
        std::fs::write(&out, b"keep").unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores the permissions; nothing to test then.
        let probe = sub.join("probe");
        if File::create(&probe).is_ok() {
            std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let opts = MooncakeOptions::default();
        let result = convert_mooncake(fixture("trace.jsonl"), &out, &opts, 4);
        let kept = std::fs::read(&out).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err());
        assert_eq!(kept, b"keep");
    }
}
