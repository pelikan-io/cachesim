use std::collections::HashSet;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use cachesim::mooncake::{
    convert_mooncake_to_parquet, sha256_hex, ModelSizes, MooncakeOptions, StateLayout, StateMode,
    DTYPE_NAMES,
};
use cachesim::oracle::OraclePolicy;
use cachesim::simulator::{
    simulate_cuckoo, simulate_oracle, simulate_segcache, CuckooConfig, SimConfig,
};
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
}

/// Options for `-f mooncake`.
#[derive(Args)]
#[command(next_help_heading = "Mooncake options (-f mooncake)")]
struct MooncakeArgs {
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

    // The options below have no clap defaults so that main() can tell whether
    // they were given with another format; options() applies the defaults.
    /// Tokens per object; must divide the 512-token source block [default: 256].
    #[arg(long, value_name = "N")]
    chunk_tokens: Option<u64>,

    /// Tensor-parallel degree: one object per rank, 1/N of the bytes (MLA
    /// models: first rank only, full size) [default: 1].
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

impl MooncakeArgs {
    fn options(&self) -> Result<MooncakeOptions, Box<dyn std::error::Error>> {
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
                        "-f mooncake needs one of --model, --model-config, --kv-bytes-per-token"
                            .into(),
                    )
                }
            };
        Ok(MooncakeOptions {
            sizes,
            model_name,
            model_revision,
            model_config_sha256: config_sha256,
            chunk_tokens: self.chunk_tokens.unwrap_or(256),
            tp: self.tp.unwrap_or(1),
            state_mode: self.state_mode.unwrap_or(StateModeArg::PerRequest).into(),
            state_layout: self.state_layout.unwrap_or(StateLayoutArg::Combined).into(),
        })
    }

    /// The Mooncake options given on the command line.
    fn given(&self) -> Vec<&'static str> {
        [
            ("--model", self.model.is_some()),
            ("--revision", self.revision.is_some()),
            ("--model-config", self.model_config.is_some()),
            ("--kv-bytes-per-token", self.kv_bytes_per_token.is_some()),
            ("--state-bytes", self.state_bytes.is_some()),
            ("--kv-dtype", self.kv_dtype.is_some()),
            ("--model-name", self.model_name.is_some()),
            ("--chunk-tokens", self.chunk_tokens.is_some()),
            ("--tp", self.tp.is_some()),
            ("--state-mode", self.state_mode.is_some()),
            ("--state-layout", self.state_layout.is_some()),
        ]
        .into_iter()
        .filter_map(|(flag, given)| given.then_some(flag))
        .collect()
    }
}

/// Reject Mooncake options given with another input format.
fn check_format_options(format: &InputFormatArg, mooncake: &MooncakeArgs) -> Result<(), String> {
    let given = mooncake.given();
    if matches!(format, InputFormatArg::Mooncake) || given.is_empty() {
        return Ok(());
    }
    Err(format!("{} apply only to -f mooncake", given.join(", ")))
}

#[cfg(feature = "hf-fetch")]
fn fetch_model_config(repo: &str, revision: &str) -> Result<Vec<u8>, cachesim::Error> {
    cachesim::hf::fetch_config(repo, revision)
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
    /// Mooncake trace (JSON lines), expanded into KV-cache objects
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
    let cli = Cli::parse();

    match cli.command {
        Command::Simulate(sim) => {
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
                    let opts = mooncake.options()?;
                    eprintln!(
                        "  KV bytes/token: {}, state bytes/sequence: {}, tp: {}, mla: {}",
                        opts.sizes.kv_bytes_per_token,
                        opts.sizes.state_bytes_per_sequence,
                        opts.tp,
                        opts.sizes.mla
                    );
                    let s = convert_mooncake_to_parquet(&input, &output, &opts, 65_536)?;
                    eprintln!(
                        "  {} requests, {} objects, {} size conflicts",
                        s.requests, s.objects, s.size_conflicts
                    );
                    s.rows
                }
            };
            eprintln!("Converted {count} entries → {}", output.display());
        }

        Command::Info { trace } => {
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

    fn check(args: &[&str]) -> Result<(), String> {
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
        for flags in [
            &["--tp", "8"][..],
            &["--chunk-tokens", "128"],
            &["--state-mode", "per-chunk"],
            &["--state-layout", "separate"],
            &["--kv-dtype", "fp8"],
            &["--model-name", "x"],
            &["--kv-bytes-per-token", "10"],
            &["--model-config", "c.json"],
            &["--model", "Qwen/Qwen3-8B"],
        ] {
            let mut args = vec!["-f", "cache-trace"];
            args.extend_from_slice(flags);
            let err = check(&args).unwrap_err();
            assert!(err.contains(flags[0]), "{flags:?}: {err}");
        }
        assert!(check(&["-f", "cache-trace"]).is_ok());
        assert!(check(&[]).is_ok());
        assert!(check(&["-f", "mooncake", "--kv-bytes-per-token", "10", "--tp", "8"]).is_ok());
    }

    #[test]
    fn mooncake_defaults() {
        let cli = Cli::try_parse_from([
            "cachesim",
            "convert",
            "-i",
            "in",
            "-o",
            "out",
            "-f",
            "mooncake",
            "--kv-bytes-per-token",
            "10",
        ])
        .unwrap();
        let Command::Convert { mooncake, .. } = cli.command else {
            unreachable!()
        };
        let opts = mooncake.options().unwrap();
        assert_eq!(opts.chunk_tokens, 256);
        assert_eq!(opts.tp, 1);
        assert_eq!(opts.state_mode, StateMode::PerRequest);
        assert_eq!(opts.state_layout, StateLayout::Combined);
    }
}
