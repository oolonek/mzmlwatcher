//! Filesystem discovery, filtering, and fingerprinting helpers.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::error::AppError;
use crate::model::{ChecksumAlgorithm, FileIdentity};

/// A discovered mzML file candidate before checksum calculation.
#[derive(Debug, Clone)]
pub(crate) struct FileCandidate {
    pub(crate) path: PathBuf,
    pub(crate) canonical_path: String,
    pub(crate) file_name: String,
    pub(crate) file_size_bytes: u64,
    pub(crate) modified_time: SystemTime,
}

impl FileCandidate {
    pub(crate) fn to_identity(&self, checksum: ChecksumAlgorithm) -> Result<FileIdentity> {
        let checksum = match checksum {
            ChecksumAlgorithm::None => None,
            ChecksumAlgorithm::Sha256 => Some(compute_sha256(&self.path)?),
        };
        Ok(FileIdentity {
            path: self.path.clone(),
            canonical_path: self.canonical_path.clone(),
            file_name: self.file_name.clone(),
            file_size_bytes: self.file_size_bytes,
            modified_time: format_system_time(self.modified_time),
            checksum,
        })
    }

    pub(crate) fn is_settled(&self, settle_delay: Duration, now: SystemTime) -> bool {
        now.duration_since(self.modified_time)
            .map(|age| age >= settle_delay)
            .unwrap_or(true)
    }
}

pub(crate) fn collect_candidates(root: &Path, recursive: bool) -> Result<Vec<FileCandidate>> {
    let mut candidates = Vec::new();
    let walker = WalkDir::new(root).follow_links(false).max_depth(if recursive { usize::MAX } else { 1 });
    for entry in walker {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if !is_mzml_path(path) || is_temporary_path(path) {
            continue;
        }
        candidates.push(build_candidate(path)?);
    }
    candidates.sort_by(|left, right| left.canonical_path.cmp(&right.canonical_path));
    Ok(candidates)
}

pub(crate) fn build_candidate(path: &Path) -> Result<FileCandidate> {
    let metadata = path.metadata().map_err(|source| AppError::Filesystem {
        path: path.to_path_buf(),
        source,
    })?;
    let canonical_path = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| canonical_path.clone());
    let modified_time = metadata.modified().map_err(|source| AppError::Filesystem {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(FileCandidate {
        path: path.to_path_buf(),
        canonical_path,
        file_name,
        file_size_bytes: metadata.len(),
        modified_time,
    })
}

pub(crate) fn is_mzml_path(path: &Path) -> bool {
    path.extension()
        .map(|ext| {
            let ext = ext.to_string_lossy();
            ext.eq_ignore_ascii_case("mzml")
        })
        .unwrap_or(false)
}

pub(crate) fn is_temporary_path(path: &Path) -> bool {
    path.file_name()
        .map(|name| {
            let name = name.to_string_lossy().to_ascii_lowercase();
            name.starts_with('.')
                || name.ends_with(".tmp")
                || name.ends_with(".part")
                || name.ends_with(".partial")
                || name.ends_with(".crdownload")
                || name.ends_with(".download")
        })
        .unwrap_or(false)
}

pub(crate) fn format_system_time(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn compute_sha256(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;

    use super::{compute_sha256, is_mzml_path, is_temporary_path};

    #[test]
    fn detects_mzml_extensions_case_insensitively() {
        assert!(is_mzml_path(Path::new("a.mzML")));
        assert!(is_mzml_path(Path::new("a.mzml")));
        assert!(!is_mzml_path(Path::new("a.raw")));
    }

    #[test]
    fn filters_common_temporary_names() {
        assert!(is_temporary_path(Path::new("sample.mzML.part")));
        assert!(is_temporary_path(Path::new(".sample.mzML")));
        assert!(!is_temporary_path(Path::new("sample.mzML")));
    }

    #[test]
    fn computes_stable_sha256() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("sample.mzML");
        fs::write(&file, b"abc").unwrap();
        assert_eq!(
            compute_sha256(&file).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
