//! Error types used by `mzmlwatcher`.

use std::path::PathBuf;

use thiserror::Error;

/// Application error variants for internal modules.
#[derive(Debug, Error)]
pub enum AppError {
    /// Filesystem operation failed.
    #[error("filesystem error for {path}: {source}")]
    Filesystem {
        /// Path associated with the failure.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// XML parsing failed.
    #[error("xml parsing error for {path}: {message}")]
    Xml {
        /// Path associated with the failure.
        path: PathBuf,
        /// Human-readable error message.
        message: String,
    },

    /// mzML validation with `mzdata` failed.
    #[error("mzdata could not read {path}: {message}")]
    MzData {
        /// Path associated with the failure.
        path: PathBuf,
        /// Human-readable error message.
        message: String,
    },

    /// SQLite operation failed.
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
}
