use std::collections::HashSet;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

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
    },

    /// Print statistics about a Parquet trace file.
    Info {
        /// Path to the trace file (Parquet format).
        #[arg(short, long)]
        trace: PathBuf,
    },
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
        } => {
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
