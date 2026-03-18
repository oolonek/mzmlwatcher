//! CLI definitions.

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

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
    /// SQLite database path.
    #[arg(long, default_value = "mzmlwatcher.sqlite")]
    pub sqlite: PathBuf,

    /// Optionally export the flattened metadata view after updates.
    #[arg(long)]
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
    pub directory: PathBuf,

    /// Shared ingest configuration.
    #[command(flatten)]
    pub ingest: IngestArgs,
}

/// Arguments for `watch`.
#[derive(Debug, Args, Clone)]
pub struct WatchArgs {
    /// Directory containing `.mzML` files.
    pub directory: PathBuf,

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
    /// SQLite database path.
    pub sqlite_path: PathBuf,

    /// Output TSV path.
    pub output_tsv: PathBuf,

    /// Include rows for files that failed parsing.
    #[arg(long)]
    pub include_failed: bool,
}

/// Arguments for `query`.
#[derive(Debug, Args, Clone)]
pub struct QueryArgs {
    /// SQLite database path.
    pub sqlite_path: PathBuf,

    /// Read-only SQL query to execute.
    #[arg(long)]
    pub sql: Option<String>,
}

/// Arguments for `schema`.
#[derive(Debug, Args, Clone)]
pub struct SchemaArgs {
    /// SQLite database path.
    pub sqlite_path: PathBuf,
}
