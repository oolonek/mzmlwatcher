//! CLI definitions.

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

/// `.env` / environment variable used for the scan root.
pub const ENV_SCAN_DIR: &str = "MZMLWATCHER_SCAN_DIR";
/// `.env` / environment variable used for the output directory.
pub const ENV_OUTPUT_DIR: &str = "MZMLWATCHER_OUTPUT_DIR";
/// `.env` / environment variable used for the SQLite path.
pub const ENV_SQLITE: &str = "MZMLWATCHER_SQLITE";
/// `.env` / environment variable used for the TSV path.
pub const ENV_TSV: &str = "MZMLWATCHER_TSV";

/// Top-level CLI definition.
#[derive(Debug, Parser)]
#[command(name = "mzmlwatcher", version, about = "Index mzML metadata into SQLite and TSV")]
pub struct Cli {
    /// Increase log verbosity. Pass twice for trace logging.
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    /// The command to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported CLI subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// One-shot scan of an existing directory tree.
    Scan(ScanArgs),
    /// Continuously watch for new or changed mzML files.
    Watch(WatchArgs),
    /// Export the flattened metadata view to TSV.
    ExportTsv(ExportTsvArgs),
    /// Run a read-only SQL query and print TSV to stdout.
    Query(QueryArgs),
    /// Print the database schema DDL.
    Schema(SchemaArgs),
    /// Print the application version.
    Version,
}

/// Checksum selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ChecksumArg {
    /// Disable content hashing.
    None,
    /// Compute SHA-256 for each file.
    Sha256,
}

/// Common scan/watch options.
#[derive(Debug, Args, Clone)]
pub struct IngestArgs {
    /// Output directory used to derive default SQLite / TSV paths.
    #[arg(long, env = ENV_OUTPUT_DIR)]
    pub output_dir: Option<PathBuf>,

    /// SQLite database path.
    #[arg(long, env = ENV_SQLITE)]
    pub sqlite: Option<PathBuf>,

    /// Optionally export the flattened metadata view after updates.
    #[arg(long, env = ENV_TSV)]
    pub tsv: Option<PathBuf>,

    /// Recurse into subdirectories.
    #[arg(long)]
    pub recursive: bool,

    /// Time to wait before parsing a new or changed file.
    #[arg(long, default_value_t = 3)]
    pub settle_seconds: u64,

    /// Checksum algorithm used for stronger identity tracking.
    #[arg(long, value_enum, default_value_t = ChecksumArg::None)]
    pub checksum: ChecksumArg,

    /// Include failed parses when exporting TSV snapshots.
    #[arg(long)]
    pub include_failed: bool,
}

/// Arguments for `scan`.
#[derive(Debug, Args, Clone)]
pub struct ScanArgs {
    /// Directory containing `.mzML` files.
    #[arg(env = ENV_SCAN_DIR)]
    pub directory: Option<PathBuf>,

    /// Shared ingest configuration.
    #[command(flatten)]
    pub ingest: IngestArgs,
}

/// Arguments for `watch`.
#[derive(Debug, Args, Clone)]
pub struct WatchArgs {
    /// Directory containing `.mzML` files.
    #[arg(env = ENV_SCAN_DIR)]
    pub directory: Option<PathBuf>,

    /// Shared ingest configuration.
    #[command(flatten)]
    pub ingest: IngestArgs,

    /// Use polling watcher mode, expressed in seconds.
    #[arg(long)]
    pub poll_interval: Option<u64>,
}

/// Arguments for `export-tsv`.
#[derive(Debug, Args, Clone)]
pub struct ExportTsvArgs {
    /// Output directory used to derive default SQLite / TSV paths.
    #[arg(long, env = ENV_OUTPUT_DIR)]
    pub output_dir: Option<PathBuf>,

    /// SQLite database path.
    #[arg(env = ENV_SQLITE)]
    pub sqlite_path: Option<PathBuf>,

    /// Output TSV path.
    #[arg(env = ENV_TSV)]
    pub output_tsv: Option<PathBuf>,

    /// Include rows for files that failed parsing.
    #[arg(long)]
    pub include_failed: bool,
}

/// Arguments for `query`.
#[derive(Debug, Args, Clone)]
pub struct QueryArgs {
    /// Output directory used to derive the default SQLite path.
    #[arg(long, env = ENV_OUTPUT_DIR)]
    pub output_dir: Option<PathBuf>,

    /// SQLite database path.
    #[arg(env = ENV_SQLITE)]
    pub sqlite_path: Option<PathBuf>,

    /// Read-only SQL query to execute.
    #[arg(long)]
    pub sql: Option<String>,
}

/// Arguments for `schema`.
#[derive(Debug, Args, Clone)]
pub struct SchemaArgs {
    /// Output directory used to derive the default SQLite path.
    #[arg(long, env = ENV_OUTPUT_DIR)]
    pub output_dir: Option<PathBuf>,

    /// SQLite database path.
    #[arg(env = ENV_SQLITE)]
    pub sqlite_path: Option<PathBuf>,
}
