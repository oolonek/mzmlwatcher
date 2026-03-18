use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use mzmlwatcher::config::Settings;
use mzmlwatcher::db::Database;
use mzmlwatcher::model::ChecksumAlgorithm;
use mzmlwatcher::watch::run_scan;

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/pma/02_tmp/mzml"));
    let sqlite_path = std::env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("example.sqlite"));

    let settings = Settings {
        root,
        sqlite_path: sqlite_path.clone(),
        tsv_path: None,
        recursive: false,
        settle_delay: Duration::from_secs(0),
        checksum: ChecksumAlgorithm::None,
        poll_interval: None,
        include_failed: false,
    };

    let mut db = Database::open(&sqlite_path)?;
    let summary = run_scan(&settings, &mut db)?;
    println!(
        "scanned={} changed={} skipped={} failed={}",
        summary.scanned, summary.changed, summary.skipped, summary.failed
    );
    Ok(())
}
