//! Runtime configuration.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use crate::cli::{ChecksumArg, ExportTsvArgs, QueryArgs, ScanArgs, SchemaArgs, WatchArgs};
use crate::model::ChecksumAlgorithm;

const DEFAULT_SQLITE_FILE: &str = "mzmlwatcher.sqlite";
const DEFAULT_TSV_FILE: &str = "mzmlwatcher.tsv";

/// Runtime settings shared by scan and watch flows.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Input root directory.
    pub root: PathBuf,
    /// SQLite database path.
    pub sqlite_path: PathBuf,
    /// Optional TSV export path.
    pub tsv_path: Option<PathBuf>,
    /// Whether to recurse into subdirectories.
    pub recursive: bool,
    /// How long a file must remain stable before parsing.
    pub settle_delay: Duration,
    /// Checksum mode.
    pub checksum: ChecksumAlgorithm,
    /// Optional watcher polling interval.
    pub poll_interval: Option<Duration>,
    /// Whether TSV exports should include failed rows.
    pub include_failed: bool,
}

impl Settings {
    /// Build settings for the `scan` command.
    pub fn from_scan_args(args: ScanArgs) -> Result<Self> {
        Ok(Self {
            root: required_path(args.directory, "scan directory")?,
            sqlite_path: resolve_sqlite_path(args.ingest.output_dir.clone(), args.ingest.sqlite),
            tsv_path: resolve_optional_tsv_path(args.ingest.output_dir, args.ingest.tsv),
            recursive: args.ingest.recursive,
            settle_delay: Duration::from_secs(args.ingest.settle_seconds),
            checksum: map_checksum(args.ingest.checksum),
            poll_interval: None,
            include_failed: args.ingest.include_failed,
        })
    }

    /// Build settings for the `watch` command.
    pub fn from_watch_args(args: WatchArgs) -> Result<Self> {
        Ok(Self {
            root: required_path(args.directory, "watch directory")?,
            sqlite_path: resolve_sqlite_path(args.ingest.output_dir.clone(), args.ingest.sqlite),
            tsv_path: resolve_optional_tsv_path(args.ingest.output_dir, args.ingest.tsv),
            recursive: args.ingest.recursive,
            settle_delay: Duration::from_secs(args.ingest.settle_seconds),
            checksum: map_checksum(args.ingest.checksum),
            poll_interval: args.poll_interval.map(Duration::from_secs),
            include_failed: args.ingest.include_failed,
        })
    }
}

/// Derived arguments for `export-tsv`.
#[derive(Debug, Clone)]
pub struct ExportTsvSettings {
    /// SQLite database path.
    pub sqlite_path: PathBuf,
    /// Output TSV path.
    pub output_tsv: PathBuf,
    /// Whether failed rows should be exported.
    pub include_failed: bool,
}

impl ExportTsvSettings {
    /// Build settings for the `export-tsv` command.
    pub fn from_args(args: ExportTsvArgs) -> Self {
        Self {
            sqlite_path: resolve_sqlite_path(args.output_dir.clone(), args.sqlite_path),
            output_tsv: resolve_required_tsv_path(args.output_dir, args.output_tsv),
            include_failed: args.include_failed,
        }
    }
}

/// Derived arguments for commands that only need a SQLite path.
#[derive(Debug, Clone)]
pub struct DatabasePathSettings {
    /// SQLite database path.
    pub sqlite_path: PathBuf,
}

impl DatabasePathSettings {
    /// Build settings for the `query` command.
    pub fn from_query_args(args: QueryArgs) -> Self {
        Self {
            sqlite_path: resolve_sqlite_path(args.output_dir, args.sqlite_path),
        }
    }

    /// Build settings for the `schema` command.
    pub fn from_schema_args(args: SchemaArgs) -> Self {
        Self {
            sqlite_path: resolve_sqlite_path(args.output_dir, args.sqlite_path),
        }
    }
}

/// Ensure the output parent directory exists for a file path.
pub fn ensure_parent_dir(path: &std::path::Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty() && *parent != std::path::Path::new("."));
    if let Some(parent) = parent {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("failed to create parent directory for {}", path.display())
        })?;
    }
    Ok(())
}

fn map_checksum(value: ChecksumArg) -> ChecksumAlgorithm {
    match value {
        ChecksumArg::None => ChecksumAlgorithm::None,
        ChecksumArg::Sha256 => ChecksumAlgorithm::Sha256,
    }
}

fn required_path(path: Option<PathBuf>, label: &str) -> Result<PathBuf> {
    path.ok_or_else(|| anyhow!("{label} was not provided; pass it on the command line or via .env"))
}

fn resolve_sqlite_path(output_dir: Option<PathBuf>, sqlite_path: Option<PathBuf>) -> PathBuf {
    sqlite_path.unwrap_or_else(|| match output_dir {
        Some(dir) => dir.join(DEFAULT_SQLITE_FILE),
        None => PathBuf::from(DEFAULT_SQLITE_FILE),
    })
}

fn resolve_optional_tsv_path(output_dir: Option<PathBuf>, tsv_path: Option<PathBuf>) -> Option<PathBuf> {
    match (output_dir, tsv_path) {
        (_, Some(tsv_path)) => Some(tsv_path),
        (Some(dir), None) => Some(dir.join(DEFAULT_TSV_FILE)),
        (None, None) => None,
    }
}

fn resolve_required_tsv_path(output_dir: Option<PathBuf>, tsv_path: Option<PathBuf>) -> PathBuf {
    resolve_optional_tsv_path(output_dir, tsv_path).unwrap_or_else(|| PathBuf::from(DEFAULT_TSV_FILE))
}
