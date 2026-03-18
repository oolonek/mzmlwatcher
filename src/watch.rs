//! Scanning and watcher orchestration.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use notify::{Config as NotifyConfig, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use rayon::prelude::*;
use tracing::{debug, info, warn};

use crate::config::Settings;
use crate::db::Database;
use crate::export::export_view_to_tsv;
use crate::fs::{build_candidate, collect_candidates, is_mzml_path, is_temporary_path};
use crate::model::ScanSummary;
use crate::parser::{failed_metadata, parse_mzml};

/// Perform a one-shot directory scan and persist new or changed files.
pub fn run_scan(settings: &Settings, db: &mut Database) -> Result<ScanSummary> {
    let mut summary = ScanSummary::default();
    let mut to_parse = Vec::new();
    for candidate in collect_candidates(&settings.root, settings.recursive)? {
        summary.scanned += 1;
        if !candidate.is_settled(settings.settle_delay, SystemTime::now()) {
            debug!("skipping unsettled file {}", candidate.path.display());
            summary.skipped += 1;
            continue;
        }
        let identity = candidate.to_identity(settings.checksum)?;
        if db.is_unchanged(&identity)? {
            summary.skipped += 1;
            continue;
        }
        to_parse.push(identity);
    }

    let progress = progress_bar(to_parse.len());
    let worker_bar = progress.clone();
    let mut parsed = to_parse
        .into_par_iter()
        .map(move |identity| {
            let result = match parse_mzml(identity.clone()) {
                Ok(metadata) => (false, metadata),
                Err(error) => (true, failed_metadata(identity, &error)),
            };
            worker_bar.inc(1);
            result
        })
        .collect::<Vec<_>>();
    progress.finish_and_clear();
    parsed.sort_by(|left, right| {
        left.1
            .file
            .identity
            .canonical_path
            .cmp(&right.1.file.identity.canonical_path)
    });

    for (failed, metadata) in parsed {
        if failed {
            summary.failed += 1;
        }
        db.upsert_metadata(&metadata)?;
        summary.changed += 1;
    }
    Ok(summary)
}

fn progress_bar(total: usize) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(
        Some(total as u64),
        ProgressDrawTarget::stderr_with_hz(10),
    )
    .with_style(progress_style())
    .with_message("parsing mzML headers");
    bar.enable_steady_tick(Duration::from_millis(100));
    bar.tick();
    bar
}

fn progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>4}/{len:4} {msg}",
    )
    .expect("progress template must be valid")
    .progress_chars("##-")
}

/// Watch a directory continuously and reprocess changed files idempotently.
pub fn watch_directory(settings: &Settings, db: &mut Database) -> Result<()> {
    let initial = run_scan(settings, db)?;
    info!(
        scanned = initial.scanned,
        changed = initial.changed,
        skipped = initial.skipped,
        failed = initial.failed,
        "initial scan complete"
    );
    if let Some(path) = &settings.tsv_path {
        export_view_to_tsv(db.connection(), path, settings.include_failed)?;
    }

    let (sender, receiver) = mpsc::channel::<notify::Result<Event>>();
    let mut watcher = new_watcher(settings, sender)?;
    watcher.watch(
        &settings.root,
        if settings.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        },
    )?;

    let mut pending = PendingPaths::default();
    loop {
        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(Ok(event)) => {
                for path in event.paths {
                    if should_track(&path) {
                        pending.insert(path, settings.settle_delay);
                    }
                }
            }
            Ok(Err(error)) => warn!("watcher error: {error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        let ready = pending.drain_ready();
        if ready.is_empty() {
            continue;
        }

        let mut summary = ScanSummary::default();
        for path in ready {
            if !path.exists() {
                continue;
            }
            match build_candidate(&path) {
                Ok(candidate) => {
                    if !candidate.is_settled(settings.settle_delay, SystemTime::now()) {
                        pending.insert(path, settings.settle_delay);
                        continue;
                    }
                    summary.scanned += 1;
                    process_candidate(candidate, settings, db, &mut summary)?;
                }
                Err(error) => warn!("failed to inspect {}: {error:#}", path.display()),
            }
        }

        if summary.changed > 0 {
            info!(
                changed = summary.changed,
                skipped = summary.skipped,
                failed = summary.failed,
                "watch batch processed"
            );
            if let Some(path) = &settings.tsv_path {
                export_view_to_tsv(db.connection(), path, settings.include_failed)?;
            }
        }
    }

    Ok(())
}

fn process_candidate(
    candidate: crate::fs::FileCandidate,
    settings: &Settings,
    db: &mut Database,
    summary: &mut ScanSummary,
) -> Result<()> {
    let identity = candidate.to_identity(settings.checksum)?;
    if db.is_unchanged(&identity)? {
        summary.skipped += 1;
        return Ok(());
    }
    let metadata = match parse_mzml(identity.clone()) {
        Ok(metadata) => metadata,
        Err(error) => {
            summary.failed += 1;
            failed_metadata(identity, &error)
        }
    };
    db.upsert_metadata(&metadata)?;
    summary.changed += 1;
    Ok(())
}

fn new_watcher(
    settings: &Settings,
    sender: mpsc::Sender<notify::Result<Event>>,
) -> Result<Box<dyn Watcher>> {
    if let Some(interval) = settings.poll_interval {
        let watcher = PollWatcher::new(
            move |result| {
                let _ = sender.send(result);
            },
            NotifyConfig::default().with_poll_interval(interval),
        )?;
        info!("using polling watcher mode with interval {:?}", interval);
        Ok(Box::new(watcher))
    } else {
        let watcher = RecommendedWatcher::new(
            move |result| {
                let _ = sender.send(result);
            },
            NotifyConfig::default(),
        )?;
        info!("using native notify watcher mode");
        Ok(Box::new(watcher))
    }
}

fn should_track(path: &Path) -> bool {
    is_mzml_path(path) && !is_temporary_path(path)
}

#[derive(Debug, Default)]
struct PendingPaths {
    deadlines: BTreeMap<PathBuf, Instant>,
}

impl PendingPaths {
    fn insert(&mut self, path: PathBuf, delay: Duration) {
        self.deadlines.insert(path, Instant::now() + delay);
    }

    fn drain_ready(&mut self) -> Vec<PathBuf> {
        let now = Instant::now();
        let ready = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        for path in &ready {
            self.deadlines.remove(path);
        }
        ready
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PendingPaths;

    #[test]
    fn drains_only_ready_paths() {
        let mut pending = PendingPaths::default();
        pending.insert("a.mzML".into(), Duration::from_millis(0));
        pending.insert("b.mzML".into(), Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(10));
        let ready = pending.drain_ready();
        assert_eq!(ready.len(), 1);
    }
}
