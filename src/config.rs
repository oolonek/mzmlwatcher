//! Runtime configuration.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::cli::{ChecksumArg, ScanArgs, WatchArgs};
use crate::model::ChecksumAlgorithm;

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
            root: args.directory,
            sqlite_path: args.ingest.sqlite,
            tsv_path: args.ingest.tsv,
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
            root: args.directory,
            sqlite_path: args.ingest.sqlite,
            tsv_path: args.ingest.tsv,
            recursive: args.ingest.recursive,
            settle_delay: Duration::from_secs(args.ingest.settle_seconds),
            checksum: map_checksum(args.ingest.checksum),
            poll_interval: args.poll_interval.map(Duration::from_secs),
            include_failed: args.ingest.include_failed,
        })
    }
}

fn map_checksum(value: ChecksumArg) -> ChecksumAlgorithm {
    match value {
        ChecksumArg::None => ChecksumAlgorithm::None,
        ChecksumArg::Sha256 => ChecksumAlgorithm::Sha256,
    }
}
