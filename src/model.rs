//! In-memory domain model for mzML metadata.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Supported checksum algorithms.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    /// Do not compute a checksum.
    None,
    /// Compute a SHA-256 checksum.
    Sha256,
}

/// File processing status.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ParseStatus {
    /// Parsing succeeded.
    Success,
    /// Parsing failed.
    Failed,
}

impl ParseStatus {
    /// Convert the status to the database representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }
}

/// Canonical file identity used for deduplication.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileIdentity {
    /// Original discovered path.
    pub path: PathBuf,
    /// Canonicalized absolute path, when available.
    pub canonical_path: String,
    /// Basename of the file.
    pub file_name: String,
    /// File size in bytes.
    pub file_size_bytes: u64,
    /// Modification timestamp in RFC 3339 format.
    pub modified_time: String,
    /// Optional content checksum.
    pub checksum: Option<String>,
}

/// Top-level file record stored in SQLite.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileRecord {
    /// Canonical identity data.
    pub identity: FileIdentity,
    /// Parse timestamp in RFC 3339 format.
    pub parse_timestamp: String,
    /// Parser/application version.
    pub parser_version: String,
    /// mzML version if present.
    pub mzml_version: Option<String>,
    /// Converted mzML file SHA-1 from the trailing `<fileChecksum>` element, when present.
    pub converted_file_sha1: Option<String>,
    /// Whether parsing succeeded.
    pub status: ParseStatus,
    /// Optional parse error text.
    pub parse_error: Option<String>,
}

/// Run-level metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RunRecord {
    /// Run identifier.
    pub run_id: Option<String>,
    /// Acquisition timestamp.
    pub start_time_stamp: Option<String>,
    /// Default instrument configuration reference.
    pub default_instrument_configuration_ref: Option<String>,
    /// Default source file reference.
    pub default_source_file_ref: Option<String>,
    /// Sample reference.
    pub sample_ref: Option<String>,
    /// Native ID format name.
    pub native_id_format: Option<String>,
    /// Observed scan polarity.
    pub polarity: Option<String>,
    /// Observed ms levels.
    pub ms_level_coverage: Option<String>,
    /// Number of spectra.
    pub spectrum_count: Option<u64>,
    /// Number of chromatograms.
    pub chromatogram_count: Option<u64>,
    /// Observed signal continuity.
    pub signal_continuity: Option<String>,
}

/// Instrument configuration metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct InstrumentConfigRecord {
    /// Instrument configuration ID.
    pub id: String,
    /// Instrument model.
    pub model: Option<String>,
    /// Ionization source names.
    pub ionization_sources: Vec<String>,
    /// Analyzer names.
    pub analyzers: Vec<String>,
    /// Detector names.
    pub detectors: Vec<String>,
    /// Stable CV terms aggregated for debugging/querying.
    pub important_cv_terms: Vec<String>,
}

/// Software metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SoftwareRecord {
    /// Software ID.
    pub id: String,
    /// Software name.
    pub name: Option<String>,
    /// Software version.
    pub version: Option<String>,
    /// Stable CV terms.
    pub important_cv_terms: Vec<String>,
}

/// Sample metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SampleRecord {
    /// Sample ID.
    pub id: String,
    /// Sample name.
    pub name: Option<String>,
    /// Stable CV terms.
    pub important_cv_terms: Vec<String>,
}

/// Data processing metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct DataProcessingRecord {
    /// Data processing ID.
    pub id: String,
    /// Associated software reference.
    pub software_ref: Option<String>,
    /// Stable action names.
    pub processing_actions: Vec<String>,
}

/// Source file provenance metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SourceFileRecord {
    /// Source file ID.
    pub id: String,
    /// Source file name.
    pub name: Option<String>,
    /// Source file location.
    pub location: Option<String>,
    /// Native ID format if present.
    pub native_id_format: Option<String>,
    /// Source file format if present.
    pub file_format: Option<String>,
    /// Embedded SHA-1 value if present in mzML.
    pub checksum_sha1: Option<String>,
    /// Stable CV terms.
    pub important_cv_terms: Vec<String>,
}

/// Controlled vocabulary definition declared in `<cvList>`.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct OntologyRecord {
    /// Short CV identifier, for example `MS`.
    pub cv_id: String,
    /// Full ontology name.
    pub full_name: Option<String>,
    /// Ontology version string.
    pub version: Option<String>,
    /// Ontology URI link.
    pub uri: Option<String>,
}

/// Distinct CURIE observed in the parsed mzML metadata.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct CurieRecord {
    /// CURIE source kind, either `cv_param` or `unit`.
    pub source_kind: String,
    /// CV reference used by the term, for example `MS`.
    pub cv_ref: Option<String>,
    /// CURIE such as `MS:1000569`.
    pub accession: String,
    /// Label associated with the CURIE.
    pub name: Option<String>,
    /// Ontology URI resolved from the file's `<cvList>`.
    pub ontology_uri: Option<String>,
}

/// Parsed metadata for one mzML file.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedMetadata {
    /// File-level record.
    pub file: FileRecord,
    /// Run-level metadata.
    pub run: RunRecord,
    /// Instrument configuration list.
    pub instrument_configs: Vec<InstrumentConfigRecord>,
    /// Software list.
    pub software: Vec<SoftwareRecord>,
    /// Sample list.
    pub samples: Vec<SampleRecord>,
    /// Data processing list.
    pub data_processings: Vec<DataProcessingRecord>,
    /// Source file list.
    pub source_files: Vec<SourceFileRecord>,
    /// Ontology definitions found in `<cvList>`.
    pub ontologies: Vec<OntologyRecord>,
    /// Distinct CURIEs found across parsed metadata.
    pub curies: Vec<CurieRecord>,
}

/// Summary returned by a scan cycle.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ScanSummary {
    /// Number of discovered candidates.
    pub scanned: usize,
    /// Number of files inserted or updated.
    pub changed: usize,
    /// Number of files skipped due to deduplication.
    pub skipped: usize,
    /// Number of parse failures stored.
    pub failed: usize,
}

/// Aggregate spectrum-derived properties.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SpectrumSummary {
    /// Distinct MS levels encountered.
    pub ms_levels: BTreeSet<u32>,
    /// Whether positive polarity was observed.
    pub saw_positive: bool,
    /// Whether negative polarity was observed.
    pub saw_negative: bool,
    /// Whether centroid spectra were observed.
    pub saw_centroid: bool,
    /// Whether profile spectra were observed.
    pub saw_profile: bool,
}

impl SpectrumSummary {
    /// Convert aggregate flags into a normalized polarity string.
    pub fn polarity(&self) -> Option<String> {
        match (self.saw_positive, self.saw_negative) {
            (true, true) => Some("mixed".to_string()),
            (true, false) => Some("positive".to_string()),
            (false, true) => Some("negative".to_string()),
            (false, false) => None,
        }
    }

    /// Convert the set of observed MS levels into a stable comma-separated list.
    pub fn ms_level_coverage(&self) -> Option<String> {
        if self.ms_levels.is_empty() {
            None
        } else {
            Some(
                self.ms_levels
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
    }

    /// Convert the set of observed MS levels into a CURIE-prefixed representation.
    pub fn ms_level_coverage_label(&self) -> Option<String> {
        self.ms_level_coverage()
            .map(|value| format!("MS:1000511|{value}"))
    }

    /// Convert aggregate flags into a normalized continuity string.
    pub fn signal_continuity(&self) -> Option<String> {
        match (self.saw_centroid, self.saw_profile) {
            (true, true) => Some("mixed".to_string()),
            (true, false) => Some("centroid".to_string()),
            (false, true) => Some("profile".to_string()),
            (false, false) => None,
        }
    }

    /// Convert aggregate polarity flags into CURIE-prefixed labels.
    pub fn polarity_label(&self) -> Option<String> {
        match (self.saw_positive, self.saw_negative) {
            (true, true) => Some("MS:1000130|positive scan; MS:1000129|negative scan".to_string()),
            (true, false) => Some("MS:1000130|positive scan".to_string()),
            (false, true) => Some("MS:1000129|negative scan".to_string()),
            (false, false) => None,
        }
    }

    /// Convert aggregate continuity flags into CURIE-prefixed labels.
    pub fn signal_continuity_label(&self) -> Option<String> {
        match (self.saw_centroid, self.saw_profile) {
            (true, true) => Some("MS:1000127|centroid spectrum; MS:1000128|profile spectrum".to_string()),
            (true, false) => Some("MS:1000127|centroid spectrum".to_string()),
            (false, true) => Some("MS:1000128|profile spectrum".to_string()),
            (false, false) => None,
        }
    }
}
