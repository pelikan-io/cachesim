use std::collections::HashSet;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use cachesim::kv::expand_to_parquet;
use cachesim::kvtrace::format::{FORMAT_REQUESTS, KEY_FORMAT};
use cachesim::kvtrace::mooncake::{convert_mooncake, MooncakeOptions};
use cachesim::kvtrace::{sha256_hex, Geometry, ModelSizes, StateLayout, StateMode, DTYPE_NAMES};
use cachesim::oracle::OraclePolicy;
use cachesim::simulator::{
    simulate_cuckoo, simulate_oracle, simulate_segcache, CuckooConfig, SimConfig,
};
use cachesim::synth::{generate, GenSpec, Pattern};
use cachesim::trace::{
    convert_bin_to_parquet, convert_cache_trace_to_parquet, BinFormat, TraceReader,
};
use cachesim::{CuckooPolicy, SegcachePolicy};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "cachesim")]
#[command(about = "Cache trace simulator for cache-rs crates")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a cache simulation against a Parquet trace file.
    Simulate(SimulateCmd),

    /// Convert a trace file to Parquet.
    Convert {
        /// Input trace file.
        #[arg(short, long)]
        input: PathBuf,

        /// Output Parquet file.
        #[arg(short, long)]
        output: PathBuf,

        /// Format of the input file.
        #[arg(short, long, default_value = "oracle-general")]
        format: InputFormatArg,

        #[command(flatten)]
        mooncake: MooncakeArgs,
    },

    /// Print statistics about a Parquet trace file.
    Info {
        /// Path to the trace file (Parquet format).
        #[arg(short, long)]
        trace: PathBuf,
    },

    /// KV-cache request traces (made by `convert -f mooncake`).
    #[command(subcommand)]
    Kv(KvCommand),

    /// Write a synthetic object-level trace from a pattern.
    #[command(subcommand)]
    Gen(GenCommand),
}

#[derive(Subcommand)]
enum GenCommand {
    /// Passes over an arithmetic range of object ids:
    /// start, start + stride, ..., start + (count - 1) * stride.
    Sequential {
        /// First object id.
        #[arg(long, default_value_t = 0)]
        start: u64,
        /// Ids per pass.
        #[arg(long)]
        count: u64,
        /// Step between ids; 2 from an odd start selects the odd ids.
        #[arg(long, default_value_t = 1)]
        stride: u64,
        /// Times the range is repeated.
        #[arg(long, default_value_t = 1)]
        passes: u64,
        #[command(flatten)]
        common: GenArgs,
    },
    /// Zipf-distributed draws over `--keys` objects; rank 1 (the most popular)
    /// is object id 0.
    Zipf {
        /// Objects in the key space.
        #[arg(long)]
        keys: u64,
        /// Zipf exponent (YCSB's theta).
        #[arg(long)]
        theta: f64,
        /// Rows to write.
        #[arg(long)]
        requests: u64,
        /// ChaCha8 seed; on one platform the same seed writes the same rows.
        #[arg(long, default_value_t = 0)]
        seed: u64,
        #[command(flatten)]
        common: GenArgs,
    },
}

/// Options every `gen` pattern takes.
#[derive(Args)]
struct GenArgs {
    /// Output object-level trace (Parquet).
    #[arg(short, long)]
    output: PathBuf,
    /// Every row's operation; `none` writes a null op, which `cachesim
    /// simulate` reads as a GET.
    #[arg(long, value_enum)]
    op: GenOpArg,
    /// Object size in bytes. Repeat to cycle sizes by object id
    /// (`id % number of sizes`), so every row of an object has one size.
    #[arg(long = "size", required = true, value_name = "BYTES")]
    sizes: Vec<u32>,
    /// Timestamp step between rows, in nanoseconds.
    #[arg(long, default_value_t = 1_000)]
    interval_ns: u64,
    /// Write -1 for every next_access_vtime. Computing it holds 8 bytes per
    /// row and a map entry per object.
    #[arg(long)]
    no_next_access: bool,
    /// Replace an existing regular file at the output path (a read-only file is
    /// still refused).
    #[arg(long)]
    force: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum GenOpArg {
    Get,
    Set,
    Del,
    None,
}

impl GenOpArg {
    fn op(self) -> Option<cachesim::trace::Op> {
        use cachesim::trace::Op;
        match self {
            Self::Get => Some(Op::Get),
            Self::Set => Some(Op::Set),
            Self::Del => Some(Op::Delete),
            Self::None => None,
        }
    }
}

#[derive(Subcommand)]
enum KvCommand {
    /// Expand a request trace into an object-level trace for one model
    /// geometry, readable by `simulate` and `info`.
    Expand {
        /// Request trace (Parquet).
        #[arg(short, long)]
        input: PathBuf,

        /// Output object-level trace (Parquet).
        #[arg(short, long)]
        output: PathBuf,

        #[command(flatten)]
        geometry: GeometryArgs,
    },
    // A KV simulation mode that replays request traces directly goes here.
}

/// Options for `-f mooncake`.
#[derive(Args)]
#[command(next_help_heading = "Mooncake options (-f mooncake)")]
struct MooncakeArgs {
    // No clap default so that main() can tell whether it was given with
    // another format; options() applies the default.
    /// Tokens per chunk; must divide the 512-token source block [default: 256].
    #[arg(long, value_name = "N")]
    chunk_tokens: Option<u64>,
}

impl MooncakeArgs {
    fn options(&self) -> MooncakeOptions {
        MooncakeOptions {
            chunk_tokens: self
                .chunk_tokens
                .unwrap_or(MooncakeOptions::default().chunk_tokens),
        }
    }
}

/// Model geometry for `kv expand`.
#[derive(Args)]
#[command(next_help_heading = "Model geometry")]
struct GeometryArgs {
    /// Hugging Face repository id, e.g. Qwen/Qwen3-8B. Its config.json is
    /// downloaded (or read from the local cache) and sizes derived from it.
    #[arg(long, value_name = "REPO_ID", conflicts_with_all = ["model_config", "kv_bytes_per_token", "model_name"])]
    model: Option<String>,

    /// Branch, tag or commit of --model [default: main]. A single name:
    /// refs/pr/N and names containing '/' are not accepted.
    #[arg(long, value_name = "REV", requires = "model")]
    revision: Option<String>,

    /// Local Hugging Face config.json of the model; sizes are derived from it.
    #[arg(long, value_name = "PATH", conflicts_with = "kv_bytes_per_token")]
    model_config: Option<PathBuf>,

    /// KV bytes per token across all layers, instead of a model config.
    #[arg(long, value_name = "BYTES")]
    kv_bytes_per_token: Option<u64>,

    /// Recurrent-state bytes per sequence, with --kv-bytes-per-token.
    #[arg(long, value_name = "BYTES", requires = "kv_bytes_per_token")]
    state_bytes: Option<u64>,

    /// Override the config's dtype for KV.
    #[arg(long, value_parser = clap::builder::PossibleValuesParser::new(DTYPE_NAMES), conflicts_with = "kv_bytes_per_token")]
    kv_dtype: Option<String>,

    /// Model id recorded in the output metadata, e.g. Qwen/Qwen3-8B.
    #[arg(long, value_name = "NAME")]
    model_name: Option<String>,

    /// Tensor-parallel degree: one object per rank, each holding the rank's
    /// share of the bytes (MLA models: first rank only, full size)
    /// [default: 1].
    #[arg(long, value_name = "N")]
    tp: Option<u64>,

    /// Hybrid models: when a recurrent-state snapshot is stored
    /// [default: per-request].
    #[arg(long)]
    state_mode: Option<StateModeArg>,

    /// Hybrid models: state in the chunk's object or as its own object
    /// [default: combined].
    #[arg(long)]
    state_layout: Option<StateLayoutArg>,
}

impl GeometryArgs {
    fn geometry(&self) -> Result<Geometry, Box<dyn std::error::Error>> {
        let kv_dtype = self.kv_dtype.as_deref();
        let (sizes, config_sha256, model_name, model_revision) =
            match (&self.model, &self.model_config, self.kv_bytes_per_token) {
                (Some(repo), None, None) => {
                    let revision = self.revision.as_deref().unwrap_or("main");
                    let bytes = fetch_model_config(repo, revision)?;
                    (
                        ModelSizes::from_config_bytes(&bytes, kv_dtype)?,
                        Some(sha256_hex(&bytes)),
                        Some(repo.clone()),
                        Some(revision.to_string()),
                    )
                }
                (None, Some(path), None) => {
                    let bytes = std::fs::read(path)
                        .map_err(|e| format!("model config {}: {e}", path.display()))?;
                    (
                        ModelSizes::from_config_bytes(&bytes, kv_dtype)?,
                        Some(sha256_hex(&bytes)),
                        self.model_name.clone(),
                        None,
                    )
                }
                (None, None, Some(kv)) => (
                    ModelSizes::explicit(kv, self.state_bytes.unwrap_or(0)),
                    None,
                    self.model_name.clone(),
                    None,
                ),
                _ => {
                    return Err(
                        "kv expand needs one of --model, --model-config, --kv-bytes-per-token"
                            .into(),
                    )
                }
            };
        Ok(Geometry {
            sizes,
            model_name,
            model_revision,
            model_config_sha256: config_sha256,
            tp: self.tp.unwrap_or(1),
            state_mode: self.state_mode.unwrap_or(StateModeArg::PerRequest).into(),
            state_layout: self.state_layout.unwrap_or(StateLayoutArg::Combined).into(),
        })
    }
}

/// Model options that `convert -f mooncake` took before it wrote request
/// traces; they belong to `kv expand` now.
const GEOMETRY_OPTIONS: &[&str] = &[
    "--model",
    "--revision",
    "--model-config",
    "--kv-bytes-per-token",
    "--state-bytes",
    "--kv-dtype",
    "--model-name",
    "--tp",
    "--state-mode",
    "--state-layout",
];

/// If `args` (without the program name) are a `convert` command line with a
/// model option, the message pointing at `kv expand`.
fn moved_geometry_option(args: impl IntoIterator<Item = String>) -> Option<String> {
    let args: Vec<String> = args.into_iter().collect();
    if args.first().map(String::as_str) != Some("convert") {
        return None;
    }
    let flag = args.iter().find_map(|a| {
        let name = a.split('=').next().unwrap_or(a);
        GEOMETRY_OPTIONS.iter().find(|&&f| f == name)
    })?;
    Some(format!(
        "{flag} is a `cachesim kv expand` option: `convert -f mooncake` now writes a \
         model-independent request trace. Convert without model options, then run \
         `cachesim kv expand -i <request trace> -o <output> {flag} ...`"
    ))
}

/// Kernel block alignment assumed for vLLM's hybrid block size: the
/// FlashAttention backend's `MultipleOf(16)` and vLLM's default 16-token
/// block.
const VLLM_KERNEL_ALIGNMENT: u64 = 16;

/// Check the geometry, then return a warning when a hybrid model's chunk
/// length is not a multiple of the block size vLLM would use, which LMCache
/// requires. `source_block_tokens` is the source's block length when the
/// request trace records one (`kvtrace.source.block_tokens`; Mooncake: 512),
/// since the converter's chunk length must divide it.
fn chunk_size_warning(
    g: &Geometry,
    chunk_tokens: u64,
    source_block_tokens: Option<u64>,
) -> Result<Option<String>, cachesim::Error> {
    // An invalid tp is an error, reported before any warning.
    g.sharding()?;
    let Some(block) = g.vllm_block_size(VLLM_KERNEL_ALIGNMENT) else {
        return Ok(None);
    };
    if chunk_tokens.is_multiple_of(block) {
        return Ok(None);
    }
    let tp = g.tp;
    let mut w = format!(
        "warning: chunk_tokens {chunk_tokens} is not a multiple of {block}, the attention \
         block size vLLM uses for this hybrid model at tp {tp} with prefix caching \
         ({VLLM_KERNEL_ALIGNMENT}-token kernel alignment). LMCache requires the chunk size to \
         be a multiple of the block size, so the smallest valid chunk is {block} tokens."
    );
    if let Some(src) = source_block_tokens {
        // The converter's chunk length divides the source block; a divisor
        // that is a multiple of `block` exists only if `block` divides `src`.
        if !src.is_multiple_of(block) {
            w.push_str(&format!(
                " The source's chunk lengths must divide its {src}-token blocks, and none of \
                 them is a multiple of {block}, so no chunk length from this source is valid \
                 for this model at tp {tp}."
            ));
        }
    }
    w.push_str(&format!(
        " Objects are sized for {chunk_tokens}-token chunks anyway."
    ));
    Ok(Some(w))
}

/// Reject Mooncake options given with another input format.
fn check_format_options(format: &InputFormatArg, mooncake: &MooncakeArgs) -> Result<(), String> {
    if matches!(format, InputFormatArg::Mooncake) || mooncake.chunk_tokens.is_none() {
        return Ok(());
    }
    Err("--chunk-tokens applies only to -f mooncake".into())
}

/// Refuse a request trace where an object-level trace is expected: its
/// columns would read as zeros.
fn check_object_trace(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let reader = SerializedFileReader::new(std::fs::File::open(path)?)?;
    let is_requests = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .any(|kv| kv.key == KEY_FORMAT && kv.value.as_deref() == Some(FORMAT_REQUESTS));
    if is_requests {
        return Err(format!(
            "{} is a KV-cache request trace; run `cachesim kv expand` on it first",
            path.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(feature = "hf-fetch")]
fn fetch_model_config(repo: &str, revision: &str) -> Result<Vec<u8>, cachesim::Error> {
    Ok(cachesim::kvtrace::hf::fetch_config(repo, revision)?)
}

#[cfg(not(feature = "hf-fetch"))]
fn fetch_model_config(_repo: &str, _revision: &str) -> Result<Vec<u8>, cachesim::Error> {
    Err(cachesim::Error::Fetch(
        "--model needs the hf-fetch feature; use --model-config with a local config.json".into(),
    ))
}

#[derive(Args)]
struct SimulateCmd {
    /// Path to the trace file (Parquet format).
    #[arg(short, long)]
    trace: PathBuf,

    /// Cache size (supports K/M/G suffixes, e.g. "64M").
    #[arg(short, long, default_value = "64M")]
    cache_size: String,

    /// Cache engine and its policy.
    #[command(subcommand)]
    engine: Engine,
}

#[derive(Subcommand)]
enum Engine {
    /// Simulate using the segcache engine.
    Segcache {
        /// Eviction policy.
        #[arg(short, long, default_value = "fifo")]
        policy: SegcachePolicyArg,

        /// Segment size in bytes.
        #[arg(short, long, default_value_t = 1_048_576)]
        segment_size: i32,

        /// Hash table power (table has 2^N buckets).
        #[arg(long, default_value_t = 16)]
        hash_power: u8,

        /// Default TTL in seconds (0 = no expiration).
        #[arg(long, default_value_t = 0)]
        default_ttl: u32,

        /// Maximum object size to cache in bytes.
        #[arg(long, default_value_t = 1_048_576)]
        max_obj_size: u32,

        /// S3-FIFO admission-pool ratio (0.0–1.0, only used with `s3-fifo`).
        #[arg(long, default_value_t = 0.10)]
        admission_ratio: f64,
    },

    /// Simulate using the cuckoo-cache engine (fixed-size item slots).
    Cuckoo {
        /// Eviction policy.
        #[arg(short, long, default_value = "random")]
        policy: CuckooPolicyArg,

        /// Fixed byte size per item slot.
        #[arg(long, default_value_t = 64)]
        item_size: usize,

        /// Maximum displacement chain depth before eviction.
        #[arg(long, default_value_t = 16)]
        max_displace: usize,

        /// Maximum TTL the cache will accept (seconds).
        #[arg(long, default_value_t = 2_592_000)]
        max_ttl: u32,

        /// Default TTL in seconds (0 = no expiration).
        #[arg(long, default_value_t = 0)]
        default_ttl: u32,

        /// Maximum object size to cache in bytes.
        #[arg(long, default_value_t = 1_048_576)]
        max_obj_size: u32,
    },

    /// Simulate using an oracle (offline-optimal) engine.
    Oracle {
        /// Oracle eviction policy.
        #[arg(short, long, default_value = "belady")]
        policy: OraclePolicyArg,
    },
}

// ---------------------------------------------------------------------------
// Argument enums
// ---------------------------------------------------------------------------

#[derive(Clone, ValueEnum)]
enum SegcachePolicyArg {
    None,
    Random,
    RandomFifo,
    Fifo,
    Cte,
    Util,
    S3Fifo,
}

impl SegcachePolicyArg {
    fn into_policy(self, admission_ratio: f64) -> SegcachePolicy {
        match self {
            Self::None => SegcachePolicy::None,
            Self::Random => SegcachePolicy::Random,
            Self::RandomFifo => SegcachePolicy::RandomFifo,
            Self::Fifo => SegcachePolicy::Fifo,
            Self::Cte => SegcachePolicy::Cte,
            Self::Util => SegcachePolicy::Util,
            Self::S3Fifo => SegcachePolicy::S3Fifo { admission_ratio },
        }
    }
}

#[derive(Clone, ValueEnum)]
enum CuckooPolicyArg {
    Random,
    Expire,
}

impl From<CuckooPolicyArg> for CuckooPolicy {
    fn from(p: CuckooPolicyArg) -> Self {
        match p {
            CuckooPolicyArg::Random => Self::Random,
            CuckooPolicyArg::Expire => Self::Expire,
        }
    }
}

#[derive(Clone, ValueEnum)]
enum OraclePolicyArg {
    Belady,
    BeladySize,
}

impl From<OraclePolicyArg> for OraclePolicy {
    fn from(p: OraclePolicyArg) -> Self {
        match p {
            OraclePolicyArg::Belady => Self::Belady,
            OraclePolicyArg::BeladySize => Self::BeladySize,
        }
    }
}

#[derive(Clone, Debug, ValueEnum)]
enum InputFormatArg {
    /// libCacheSim oracleGeneral binary (24 B/record)
    OracleGeneral,
    /// libCacheSim oracleGeneralOpNs binary (27 B/record)
    OracleGeneralOpNs,
    /// pelikan-io/cache-trace CSV (optionally zstd-compressed)
    CacheTrace,
    /// Mooncake trace (JSON lines), to a KV-cache request trace
    Mooncake,
}

#[derive(Clone, Copy, ValueEnum)]
enum StateModeArg {
    /// One snapshot at each request's last chunk
    PerRequest,
    /// A snapshot at every chunk
    PerChunk,
}

impl From<StateModeArg> for StateMode {
    fn from(m: StateModeArg) -> Self {
        match m {
            StateModeArg::PerRequest => Self::PerRequest,
            StateModeArg::PerChunk => Self::PerChunk,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum StateLayoutArg {
    /// State bytes added to the chunk's object
    Combined,
    /// State as its own object
    Separate,
}

impl From<StateLayoutArg> for StateLayout {
    fn from(l: StateLayoutArg) -> Self {
        match l {
            StateLayoutArg::Combined => Self::Combined,
            StateLayoutArg::Separate => Self::Separate,
        }
    }
}

// ---------------------------------------------------------------------------
// Size parser
// ---------------------------------------------------------------------------

fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    let (num, mult) = match s.as_bytes().last() {
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1024),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G' | b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    num.parse::<usize>()
        .map(|n| n * mult)
        .map_err(|e| format!("invalid size '{s}': {e}"))
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            if let Some(msg) = moved_geometry_option(std::env::args().skip(1)) {
                eprintln!("error: {msg}");
                std::process::exit(2);
            }
            e.exit()
        }
    };

    match cli.command {
        Command::Simulate(sim) => {
            check_object_trace(&sim.trace)?;
            let cache_size =
                parse_size(&sim.cache_size).map_err(|e| format!("bad --cache-size: {e}"))?;

            let result = match sim.engine {
                Engine::Segcache {
                    policy,
                    segment_size,
                    hash_power,
                    default_ttl,
                    max_obj_size,
                    admission_ratio,
                } => {
                    let config = SimConfig {
                        cache_size,
                        segment_size,
                        hash_power,
                        eviction: policy.into_policy(admission_ratio),
                        default_ttl,
                        max_obj_size,
                    };

                    eprintln!("Running segcache simulation …");
                    eprintln!("  cache size:    {} bytes", config.cache_size);
                    eprintln!("  segment size:  {} bytes", config.segment_size);
                    eprintln!("  hash power:    {}", config.hash_power);
                    eprintln!("  eviction:      {:?}", config.eviction);

                    simulate_segcache(&sim.trace, &config)?
                }

                Engine::Cuckoo {
                    policy,
                    item_size,
                    max_displace,
                    max_ttl,
                    default_ttl,
                    max_obj_size,
                } => {
                    let nitem = cache_size / item_size;
                    let config = CuckooConfig {
                        nitem,
                        item_size,
                        max_displace,
                        eviction: policy.into(),
                        default_ttl,
                        max_ttl,
                        max_obj_size,
                    };

                    eprintln!("Running cuckoo-cache simulation …");
                    eprintln!("  cache size:      {} bytes", cache_size);
                    eprintln!("  item size:       {} bytes", config.item_size);
                    eprintln!("  nitem:           {}", config.nitem);
                    eprintln!("  max displace:    {}", config.max_displace);
                    eprintln!("  eviction:        {:?}", config.eviction);

                    simulate_cuckoo(&sim.trace, &config)?
                }

                Engine::Oracle { policy } => {
                    let oracle_policy: OraclePolicy = policy.into();

                    eprintln!("Running oracle simulation …");
                    eprintln!("  cache size:    {cache_size} bytes");
                    eprintln!("  eviction:      {oracle_policy:?}");

                    simulate_oracle(&sim.trace, cache_size, oracle_policy)?
                }
            };

            println!("{result}");
        }

        Command::Convert {
            input,
            output,
            format,
            mooncake,
        } => {
            check_format_options(&format, &mooncake)?;
            eprintln!("Converting {:?} trace …", format);
            let count = match format {
                InputFormatArg::OracleGeneral => {
                    convert_bin_to_parquet(&input, &output, BinFormat::OracleGeneral, 65_536)?
                }
                InputFormatArg::OracleGeneralOpNs => {
                    convert_bin_to_parquet(&input, &output, BinFormat::OracleGeneralOpNs, 65_536)?
                }
                InputFormatArg::CacheTrace => {
                    convert_cache_trace_to_parquet(&input, &output, 65_536)?
                }
                InputFormatArg::Mooncake => {
                    let opts = mooncake.options();
                    let s = convert_mooncake(&input, &output, &opts, 65_536)?;
                    eprintln!(
                        "  {} requests, {} full chunks of {} tokens, {} requests without a full chunk",
                        s.requests, s.chunks, opts.chunk_tokens, s.requests_without_chunks
                    );
                    eprintln!(
                        "  input_length disagrees with hash_ids: {} beyond 512 x blocks, {} short of the last block",
                        s.input_beyond_blocks, s.input_short_of_blocks
                    );
                    eprintln!(
                        "Converted {} requests → {} (expand with `cachesim kv expand`)",
                        s.requests,
                        output.display()
                    );
                    return Ok(());
                }
            };
            eprintln!("Converted {count} entries → {}", output.display());
        }

        Command::Kv(KvCommand::Expand {
            input,
            output,
            geometry,
        }) => {
            let g = geometry.geometry()?;
            eprintln!("Expanding request trace …");
            let reader = cachesim::kvtrace::RequestReader::open(&input)?;
            let source_block_tokens = reader
                .metadata()
                .get("kvtrace.source.block_tokens")
                .and_then(|v| v.parse::<u64>().ok());
            let chunk_tokens = u64::from(reader.chunk_tokens());
            drop(reader);
            if let Some(w) = chunk_size_warning(&g, chunk_tokens, source_block_tokens)? {
                eprintln!("{w}");
            }
            eprintln!(
                "  KV bytes/token: {}, state bytes/sequence: {}, tp: {}, mla: {}",
                g.sizes.kv_bytes_per_token, g.sizes.state_bytes_per_sequence, g.tp, g.sizes.mla
            );
            let s = expand_to_parquet(&input, &output, &g, 65_536)?;
            eprintln!(
                "  {} requests, {} objects, {} written at more than one size",
                s.requests, s.objects, s.multi_size_objects
            );
            eprintln!("Expanded to {} entries → {}", s.rows, output.display());
        }

        Command::Gen(cmd) => {
            let (pattern, common) = match cmd {
                GenCommand::Sequential {
                    start,
                    count,
                    stride,
                    passes,
                    common,
                } => (
                    Pattern::Sequential {
                        start,
                        count,
                        stride,
                        passes,
                    },
                    common,
                ),
                GenCommand::Zipf {
                    keys,
                    theta,
                    requests,
                    seed,
                    common,
                } => (
                    Pattern::Zipf {
                        keys,
                        theta,
                        requests,
                        seed,
                    },
                    common,
                ),
            };
            let spec = GenSpec {
                pattern,
                op: common.op.op(),
                sizes: common.sizes,
                interval_ns: common.interval_ns,
                next_access: !common.no_next_access,
            };
            let s = generate(&spec, &common.output, common.force, 65_536)?;
            match s.objects {
                Some(objects) => eprintln!(
                    "Generated {} rows over {objects} objects → {}",
                    s.rows,
                    common.output.display()
                ),
                None => eprintln!("Generated {} rows → {}", s.rows, common.output.display()),
            }
        }

        Command::Info { trace } => {
            check_object_trace(&trace)?;
            let reader = TraceReader::open(&trace)?;
            let total = reader.total_entries();

            let mut min_ts = i64::MAX;
            let mut max_ts = 0i64;
            let mut min_size = u32::MAX;
            let mut max_size = 0u32;
            let mut total_size: u64 = 0;
            let mut unique_ids = HashSet::new();

            for entry in reader {
                let entry = entry?;
                min_ts = min_ts.min(entry.timestamp);
                max_ts = max_ts.max(entry.timestamp);
                min_size = min_size.min(entry.obj_size);
                max_size = max_size.max(entry.obj_size);
                total_size += entry.obj_size as u64;
                unique_ids.insert(entry.obj_id);
            }

            println!("Trace Statistics");
            println!("  total requests:  {total}");
            println!("  unique objects:  {}", unique_ids.len());
            if total > 0 {
                let duration_ns = max_ts.saturating_sub(min_ts);
                let duration_s = duration_ns as f64 / 1_000_000_000.0;
                println!("  time range:      {min_ts} – {max_ts} ns ({duration_s:.3} s)",);
                println!("  object sizes:    {min_size} – {max_size} bytes");
                println!(
                    "  avg object size: {:.1} bytes",
                    total_size as f64 / total as f64
                );
                println!("  total footprint: {total_size} bytes");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert(args: &[&str]) -> Result<(), String> {
        let cli = Cli::try_parse_from(
            ["cachesim", "convert", "-i", "in", "-o", "out"]
                .iter()
                .chain(args),
        )
        .map_err(|e| e.to_string())?;
        match cli.command {
            Command::Convert {
                format, mooncake, ..
            } => check_format_options(&format, &mooncake),
            _ => unreachable!(),
        }
    }

    #[test]
    fn mooncake_options_are_rejected_for_other_formats() {
        let err = convert(&["-f", "cache-trace", "--chunk-tokens", "128"]).unwrap_err();
        assert!(err.contains("--chunk-tokens"), "{err}");
        assert!(convert(&["-f", "cache-trace"]).is_ok());
        assert!(convert(&[]).is_ok());
        assert!(convert(&["-f", "mooncake", "--chunk-tokens", "128"]).is_ok());
        // Geometry options belong to `kv expand` now.
        for flags in [
            &["--tp", "8"][..],
            &["--kv-bytes-per-token", "10"],
            &["--model", "Qwen/Qwen3-8B"],
        ] {
            let mut args = vec!["-f", "mooncake"];
            args.extend_from_slice(flags);
            let err = convert(&args).unwrap_err();
            assert!(err.contains(flags[0]), "{flags:?}: {err}");
        }
    }

    fn expand_args(args: &[&str]) -> GeometryArgs {
        let cli = Cli::try_parse_from(
            ["cachesim", "kv", "expand", "-i", "in", "-o", "out"]
                .iter()
                .chain(args),
        )
        .unwrap();
        let Command::Kv(KvCommand::Expand { geometry, .. }) = cli.command else {
            unreachable!()
        };
        geometry
    }

    #[test]
    fn request_traces_are_refused_where_objects_are_expected() {
        let dir = tempfile::tempdir().unwrap();
        let req = dir.path().join("req.parquet");
        cachesim::kvtrace::RequestWriter::create(&req, 256, 16)
            .unwrap()
            .finish()
            .unwrap();
        let err = check_object_trace(&req).unwrap_err().to_string();
        assert!(err.contains("kv expand"), "{err}");
        let obj = dir.path().join("obj.parquet");
        cachesim::trace::TraceWriter::create(&obj, 16)
            .unwrap()
            .finish()
            .unwrap();
        assert!(check_object_trace(&obj).is_ok());
    }

    #[test]
    fn model_options_on_convert_point_at_kv_expand() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for (line, flag) in [
            (
                &[
                    "convert", "-i", "a", "-o", "b", "-f", "mooncake", "--tp", "8",
                ][..],
                "--tp",
            ),
            (
                &["convert", "-f", "mooncake", "--model=Qwen/Qwen3-8B"],
                "--model",
            ),
            (
                &["convert", "--kv-bytes-per-token", "10"],
                "--kv-bytes-per-token",
            ),
        ] {
            // clap rejects the line, and the message names the option and kv expand.
            assert!(
                Cli::try_parse_from(std::iter::once("cachesim").chain(line.iter().copied()))
                    .is_err()
            );
            let msg = moved_geometry_option(args(line)).unwrap();
            assert!(
                msg.starts_with(flag) && msg.contains("cachesim kv expand"),
                "{msg}"
            );
        }
        assert!(moved_geometry_option(args(&["convert", "-f", "mooncake"])).is_none());
        assert!(moved_geometry_option(args(&["kv", "expand", "--tp", "8"])).is_none());
    }

    #[test]
    fn hybrid_chunk_size_warning() {
        let cfg = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/models/qwen3.8-27b.json"
        );
        let mut g = expand_args(&["--model-config", cfg, "--tp", "8"])
            .geometry()
            .unwrap();
        let w = chunk_size_warning(&g, 256, None).unwrap().unwrap();
        assert!(
            w.contains("chunk_tokens 256 is not a multiple of 400")
                && w.contains("smallest valid chunk is 400 tokens")
                && !w.contains("no chunk length"),
            "{w}"
        );
        // From a Mooncake trace (512-token blocks) no chunk length can be a
        // multiple of 400.
        let w = chunk_size_warning(&g, 256, Some(512)).unwrap().unwrap();
        assert!(
            w.contains("no chunk length from this source is valid for this model at tp 8"),
            "{w}"
        );
        assert!(chunk_size_warning(&g, 800, None).unwrap().is_none());
        g.tp = 1;
        let w = chunk_size_warning(&g, 256, Some(512)).unwrap().unwrap();
        assert!(w.contains("784") && w.contains("no chunk length"), "{w}");
        // A block that divides the source block: valid Mooncake chunks exist.
        let mut h = cachesim::kvtrace::ModelSizes::explicit(10, 1000);
        h.hybrid_pages = Some(cachesim::kvtrace::geometry::HybridPages {
            kv_heads: 1,
            kv_head_bytes_per_token: 1,
            conv_dim: 0,
            conv_state_tokens: 0,
            conv_dtype_bytes: 1,
            value_heads: 1,
            recurrent_bytes_per_value_head: 64,
        });
        let small = Geometry::new(h);
        assert_eq!(small.vllm_block_size(VLLM_KERNEL_ALIGNMENT), Some(64));
        let w = chunk_size_warning(&small, 48, Some(512)).unwrap().unwrap();
        assert!(!w.contains("no chunk length"), "{w}");
        // An invalid tp is an error, not a warning.
        g.tp = 3;
        let err = chunk_size_warning(&g, 256, Some(512))
            .unwrap_err()
            .to_string();
        assert!(err.contains("KV heads"), "{err}");
        // Dense: no warning.
        let g = expand_args(&["--kv-bytes-per-token", "10"])
            .geometry()
            .unwrap();
        assert!(chunk_size_warning(&g, 256, Some(512)).unwrap().is_none());
    }

    #[test]
    fn mooncake_defaults() {
        let cli = Cli::try_parse_from([
            "cachesim", "convert", "-i", "in", "-o", "out", "-f", "mooncake",
        ])
        .unwrap();
        let Command::Convert { mooncake, .. } = cli.command else {
            unreachable!()
        };
        assert_eq!(mooncake.options().chunk_tokens, 256);
    }

    #[test]
    fn geometry_defaults() {
        let g = expand_args(&["--kv-bytes-per-token", "10"])
            .geometry()
            .unwrap();
        assert_eq!(g.sizes, ModelSizes::explicit(10, 0));
        assert_eq!(g.tp, 1);
        assert_eq!(g.state_mode, StateMode::PerRequest);
        assert_eq!(g.state_layout, StateLayout::Combined);
        let g = expand_args(&[
            "--kv-bytes-per-token",
            "10",
            "--state-bytes",
            "1000",
            "--tp",
            "2",
            "--state-mode",
            "per-chunk",
            "--state-layout",
            "separate",
            "--model-name",
            "m",
        ])
        .geometry()
        .unwrap();
        assert_eq!(g.sizes, ModelSizes::explicit(10, 1000));
        assert_eq!(g.tp, 2);
        assert_eq!(g.state_mode, StateMode::PerChunk);
        assert_eq!(g.state_layout, StateLayout::Separate);
        assert_eq!(g.model_name.as_deref(), Some("m"));
        let err = expand_args(&[]).geometry().unwrap_err().to_string();
        assert!(err.contains("needs one of"), "{err}");
    }

    #[test]
    fn gen_maps_ops_and_repeated_sizes() {
        use cachesim::trace::Op;
        for (arg, want) in [
            ("get", Some(Op::Get)),
            ("set", Some(Op::Set)),
            ("del", Some(Op::Delete)),
            ("none", None),
        ] {
            let cli = Cli::try_parse_from([
                "cachesim",
                "gen",
                "sequential",
                "-o",
                "x",
                "--op",
                arg,
                "--count",
                "2",
                "--size",
                "1",
                "--size",
                "2",
            ])
            .unwrap();
            let Command::Gen(GenCommand::Sequential { common, count, .. }) = cli.command else {
                panic!("not gen sequential");
            };
            assert_eq!(common.op.op(), want, "{arg}");
            assert_eq!(common.sizes, vec![1, 2]);
            assert_eq!(count, 2);
        }
        // --op and --size are required.
        assert!(Cli::try_parse_from([
            "cachesim",
            "gen",
            "zipf",
            "-o",
            "x",
            "--keys",
            "1",
            "--theta",
            "1",
            "--requests",
            "1",
            "--size",
            "1"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "cachesim",
            "gen",
            "zipf",
            "-o",
            "x",
            "--keys",
            "1",
            "--theta",
            "1",
            "--requests",
            "1",
            "--op",
            "get"
        ])
        .is_err());
    }
}
