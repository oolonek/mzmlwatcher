//! `mzmlwatcher` indexes mzML header and provenance metadata into SQLite and TSV.
//!
//! The crate is intentionally focused on compact, queryable metadata. It does
//! not store peak arrays or attempt to build a spectrum warehouse.
//!
//! Typical usage is through the CLI:
//!
//! ```text
//! mzmlwatcher scan /path/to/mzml --sqlite metadata.sqlite --recursive
//! mzmlwatcher export-tsv metadata.sqlite metadata.tsv
//! mzmlwatcher query metadata.sqlite --sql "SELECT file_path, sample_name FROM v_metadata_flat"
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// Command-line parsing.
pub mod cli;
/// Runtime configuration derived from CLI arguments.
pub mod config;
/// SQLite schema and persistence.
pub mod db;
/// Error types used by the application.
pub mod error;
/// TSV export helpers.
pub mod export;
/// Filesystem traversal, fingerprinting, and filtering.
pub mod fs;
/// In-memory metadata model shared between parser, DB, and export layers.
pub mod model;
/// mzML parsing and metadata extraction.
pub mod parser;
/// Directory watching logic.
pub mod watch;
