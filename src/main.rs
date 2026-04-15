use std::collections::HashSet;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use cachesim::oracle::OraclePolicy;
use cachesim::simulator::{simulate, simulate_oracle, SimConfig};
use cachesim::trace::{
    convert_bin_to_parquet, convert_cache_trace_to_parquet, BinFormat, TraceReader,
};

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
    Simulate {
        /// Path to the trace file (Parquet format).
        #[arg(short, long)]
        trace: PathBuf,

        /// Cache size (supports K/M/G suffixes, e.g. "64M").
        #[arg(short, long, default_value = "64M")]
        cache_size: String,

        /// Segment size in bytes.
        #[arg(short, long, default_value_t = 1_048_576)]
        segment_size: i32,

        /// Hash table power (table has 2^N buckets).
        #[arg(long, default_value_t = 16)]
        hash_power: u8,

        /// Eviction policy.
        #[arg(short, long, default_value = "fifo")]
        eviction: EvictionArg,

        /// Default TTL in seconds (0 = no expiration).
        #[arg(long, default_value_t = 0)]
        default_ttl: u32,

        /// Maximum object size to cache in bytes.
        #[arg(long, default_value_t = 1_048_576)]
        max_obj_size: u32,
    },

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

// ---------------------------------------------------------------------------
// Argument enums
// ---------------------------------------------------------------------------

#[derive(Clone, ValueEnum)]
enum EvictionArg {
    // -- segcache policies --
    None,
    Random,
    RandomFifo,
    Fifo,
    Cte,
    Util,
    // -- oracle (offline-optimal) policies --
    Belady,
    BeladySize,
}

impl EvictionArg {
    fn is_oracle(&self) -> bool {
        matches!(self, Self::Belady | Self::BeladySize)
    }
}

impl From<EvictionArg> for segcache::Policy {
    fn from(p: EvictionArg) -> Self {
        match p {
            EvictionArg::None => Self::None,
            EvictionArg::Random => Self::Random,
            EvictionArg::RandomFifo => Self::RandomFifo,
            EvictionArg::Fifo => Self::Fifo,
            EvictionArg::Cte => Self::Cte,
            EvictionArg::Util => Self::Util,
            EvictionArg::Belady | EvictionArg::BeladySize => {
                unreachable!("oracle policies handled separately")
            }
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
        Command::Simulate {
            trace,
            cache_size,
            segment_size,
            hash_power,
            eviction,
            default_ttl,
            max_obj_size,
        } => {
            let cache_size =
                parse_size(&cache_size).map_err(|e| format!("bad --cache-size: {e}"))?;

            let result = if eviction.is_oracle() {
                let oracle_policy = match eviction {
                    EvictionArg::Belady => OraclePolicy::Belady,
                    EvictionArg::BeladySize => OraclePolicy::BeladySize,
                    _ => unreachable!(),
                };

                eprintln!("Running oracle simulation …");
                eprintln!("  cache size:    {cache_size} bytes");
                eprintln!("  eviction:      {oracle_policy:?}");

                simulate_oracle(&trace, cache_size, oracle_policy)?
            } else {
                let config = SimConfig {
                    cache_size,
                    segment_size,
                    hash_power,
                    eviction: eviction.into(),
                    default_ttl,
                    max_obj_size,
                };

                eprintln!("Running simulation …");
                eprintln!("  cache size:    {} bytes", config.cache_size);
                eprintln!("  segment size:  {} bytes", config.segment_size);
                eprintln!("  hash power:    {}", config.hash_power);
                eprintln!("  eviction:      {:?}", config.eviction);

                simulate(&trace, &config)?
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
