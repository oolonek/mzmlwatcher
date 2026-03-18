//! mzML parsing and metadata extraction.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::Result;
use mzdata::io::MZFileReader;
use mzdata::MzMLReader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use quick_xml::Reader;

use crate::error::AppError;
use crate::fs::now_rfc3339;
use crate::model::{
    DataProcessingRecord, FileIdentity, FileRecord, InstrumentConfigRecord, ParseStatus,
    ParsedMetadata, RunRecord, SampleRecord, SoftwareRecord, SourceFileRecord, SpectrumSummary,
};

#[derive(Debug, Clone, Default)]
struct CvTerm {
    accession: Option<String>,
    name: String,
    value: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ComponentKind {
    Source,
    Analyzer,
    Detector,
}

#[derive(Debug, Clone, Default)]
struct ParsingState {
    mzml_version: Option<String>,
    run: RunRecord,
    spectrum: SpectrumSummary,
    instrument_configs: Vec<InstrumentConfigRecord>,
    software: Vec<SoftwareRecord>,
    samples: Vec<SampleRecord>,
    data_processings: Vec<DataProcessingRecord>,
    source_files: Vec<SourceFileRecord>,
    referenceable_groups: HashMap<String, Vec<CvTerm>>,
    current_referenceable_group: Option<(String, Vec<CvTerm>)>,
    current_instrument: Option<InstrumentConfigRecord>,
    current_component: Option<ComponentKind>,
    current_software: Option<SoftwareRecord>,
    current_sample: Option<SampleRecord>,
    current_data_processing: Option<DataProcessingRecord>,
    current_source_file: Option<SourceFileRecord>,
    in_spectrum: bool,
}

pub(crate) fn parse_mzml(identity: FileIdentity) -> Result<ParsedMetadata> {
    validate_with_mzdata(&identity.path)?;
    let header = parse_header(&identity.path)?;

    Ok(ParsedMetadata {
        file: FileRecord {
            identity,
            parse_timestamp: now_rfc3339(),
            parser_version: env!("CARGO_PKG_VERSION").to_string(),
            mzml_version: header.mzml_version,
            status: ParseStatus::Success,
            parse_error: None,
        },
        run: header.run,
        instrument_configs: header.instrument_configs,
        software: header.software,
        samples: header.samples,
        data_processings: header.data_processings,
        source_files: header.source_files,
    })
}

pub(crate) fn failed_metadata(identity: FileIdentity, error: &anyhow::Error) -> ParsedMetadata {
    ParsedMetadata {
        file: FileRecord {
            identity,
            parse_timestamp: now_rfc3339(),
            parser_version: env!("CARGO_PKG_VERSION").to_string(),
            mzml_version: None,
            status: ParseStatus::Failed,
            parse_error: Some(format!("{error:#}")),
        },
        run: RunRecord::default(),
        instrument_configs: Vec::new(),
        software: Vec::new(),
        samples: Vec::new(),
        data_processings: Vec::new(),
        source_files: Vec::new(),
    }
}

fn validate_with_mzdata(path: &Path) -> Result<()> {
    MzMLReader::open_path(path).map_err(|error| AppError::MzData {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    Ok(())
}

fn parse_header(path: &Path) -> Result<ParsingState> {
    let file = File::open(path).map_err(|source| AppError::Filesystem {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);
    let mut state = ParsingState::default();
    let mut buffer = Vec::new();

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => handle_start(&event, &mut state)?,
            Ok(Event::Empty(event)) => handle_empty(&event, &mut state)?,
            Ok(Event::End(event)) => handle_end(event.local_name().as_ref(), &mut state),
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(AppError::Xml {
                    path: path.to_path_buf(),
                    message: error.to_string(),
                }
                .into())
            }
        }
        buffer.clear();
    }

    if state.run.native_id_format.is_none() {
        state.run.native_id_format = state
            .source_files
            .iter()
            .find_map(|source| source.native_id_format.clone());
    }
    state.run.polarity = state.spectrum.polarity();
    state.run.ms_level_coverage = state.spectrum.ms_level_coverage();
    state.run.signal_continuity = state.spectrum.signal_continuity();

    Ok(state)
}

fn handle_start(event: &BytesStart<'_>, state: &mut ParsingState) -> Result<()> {
    match event.local_name().as_ref() {
        b"mzML" => state.mzml_version = attr_value(event, b"version")?,
        b"referenceableParamGroup" => {
            let id = attr_value(event, b"id")?.unwrap_or_default();
            state.current_referenceable_group = Some((id, Vec::new()));
        }
        b"software" => {
            state.current_software = Some(SoftwareRecord {
                id: attr_value(event, b"id")?.unwrap_or_default(),
                version: attr_value(event, b"version")?,
                ..SoftwareRecord::default()
            });
        }
        b"sample" => {
            state.current_sample = Some(SampleRecord {
                id: attr_value(event, b"id")?.unwrap_or_default(),
                name: attr_value(event, b"name")?,
                ..SampleRecord::default()
            });
        }
        b"instrumentConfiguration" => {
            state.current_instrument = Some(InstrumentConfigRecord {
                id: attr_value(event, b"id")?.unwrap_or_default(),
                ..InstrumentConfigRecord::default()
            });
        }
        b"source" => state.current_component = Some(ComponentKind::Source),
        b"analyzer" => state.current_component = Some(ComponentKind::Analyzer),
        b"detector" => state.current_component = Some(ComponentKind::Detector),
        b"dataProcessing" => {
            state.current_data_processing = Some(DataProcessingRecord {
                id: attr_value(event, b"id")?.unwrap_or_default(),
                ..DataProcessingRecord::default()
            });
        }
        b"processingMethod" => {
            if let Some(data_processing) = state.current_data_processing.as_mut() {
                data_processing.software_ref = attr_value(event, b"softwareRef")?;
            }
        }
        b"sourceFile" => {
            state.current_source_file = Some(SourceFileRecord {
                id: attr_value(event, b"id")?.unwrap_or_default(),
                name: attr_value(event, b"name")?,
                location: attr_value(event, b"location")?,
                ..SourceFileRecord::default()
            });
        }
        b"run" => {
            state.run.run_id = attr_value(event, b"id")?;
            state.run.start_time_stamp = attr_value(event, b"startTimeStamp")?;
            state.run.default_instrument_configuration_ref =
                attr_value(event, b"defaultInstrumentConfigurationRef")?;
            state.run.default_source_file_ref = attr_value(event, b"defaultSourceFileRef")?;
            state.run.sample_ref = attr_value(event, b"sampleRef")?;
        }
        b"spectrumList" => {
            state.run.spectrum_count =
                attr_value(event, b"count")?.and_then(|value| value.parse::<u64>().ok());
        }
        b"chromatogramList" => {
            state.run.chromatogram_count =
                attr_value(event, b"count")?.and_then(|value| value.parse::<u64>().ok());
        }
        b"spectrum" => state.in_spectrum = true,
        _ => {}
    }
    Ok(())
}

fn handle_empty(event: &BytesStart<'_>, state: &mut ParsingState) -> Result<()> {
    match event.local_name().as_ref() {
        b"cvParam" => apply_cv_term(parse_cv_term(event)?, state),
        b"referenceableParamGroupRef" => apply_referenceable_group(event, state)?,
        _ => {}
    }
    Ok(())
}

fn handle_end(tag: &[u8], state: &mut ParsingState) {
    match tag {
        b"referenceableParamGroup" => {
            if let Some((id, terms)) = state.current_referenceable_group.take() {
                state.referenceable_groups.insert(id, terms);
            }
        }
        b"software" => {
            if let Some(software) = state.current_software.take() {
                state.software.push(software);
            }
        }
        b"sample" => {
            if let Some(sample) = state.current_sample.take() {
                state.samples.push(sample);
            }
        }
        b"instrumentConfiguration" => {
            if let Some(instrument) = state.current_instrument.take() {
                state.instrument_configs.push(instrument);
            }
        }
        b"source" | b"analyzer" | b"detector" => state.current_component = None,
        b"dataProcessing" => {
            if let Some(record) = state.current_data_processing.take() {
                state.data_processings.push(record);
            }
        }
        b"sourceFile" => {
            if let Some(record) = state.current_source_file.take() {
                state.source_files.push(record);
            }
        }
        b"spectrum" => state.in_spectrum = false,
        _ => {}
    }
}

fn apply_referenceable_group(event: &BytesStart<'_>, state: &mut ParsingState) -> Result<()> {
    let Some(reference) = attr_value(event, b"ref")? else {
        return Ok(());
    };
    let Some(terms) = state.referenceable_groups.get(&reference) else {
        return Ok(());
    };
    for term in terms.clone() {
        apply_cv_term(term, state);
    }
    Ok(())
}

fn apply_cv_term(term: CvTerm, state: &mut ParsingState) {
    if let Some((_, terms)) = state.current_referenceable_group.as_mut() {
        terms.push(term);
        return;
    }

    if state.in_spectrum {
        apply_spectrum_cv_term(&term, &mut state.spectrum);
    }

    if let Some(instrument) = state.current_instrument.as_mut() {
        apply_instrument_cv_term(&term, instrument, state.current_component);
        return;
    }
    if let Some(software) = state.current_software.as_mut() {
        apply_software_cv_term(&term, software);
        return;
    }
    if let Some(sample) = state.current_sample.as_mut() {
        sample.important_cv_terms.push(format_cv_term(&term));
        return;
    }
    if let Some(data_processing) = state.current_data_processing.as_mut() {
        if !data_processing.processing_actions.contains(&term.name) {
            data_processing.processing_actions.push(term.name);
        }
        return;
    }
    if let Some(source_file) = state.current_source_file.as_mut() {
        apply_source_file_cv_term(&term, source_file);
    }
}

fn apply_instrument_cv_term(
    term: &CvTerm,
    instrument: &mut InstrumentConfigRecord,
    component: Option<ComponentKind>,
) {
    match component {
        Some(ComponentKind::Source) => instrument.ionization_sources.push(term.name.clone()),
        Some(ComponentKind::Analyzer) => instrument.analyzers.push(term.name.clone()),
        Some(ComponentKind::Detector) => instrument.detectors.push(term.name.clone()),
        None => {
            if instrument.model.is_none() && !looks_like_non_model_instrument_term(term) {
                instrument.model = Some(term.name.clone());
            }
        }
    }
    instrument.important_cv_terms.push(format_cv_term(term));
}

fn apply_software_cv_term(term: &CvTerm, software: &mut SoftwareRecord) {
    if software.name.is_none() {
        software.name = Some(term.name.clone());
    }
    software.important_cv_terms.push(format_cv_term(term));
}

fn apply_source_file_cv_term(term: &CvTerm, source_file: &mut SourceFileRecord) {
    let lower = term.name.to_ascii_lowercase();
    if lower.contains("nativeid format") && source_file.native_id_format.is_none() {
        source_file.native_id_format = Some(term.name.clone());
    } else if lower.ends_with(" format") && source_file.file_format.is_none() {
        source_file.file_format = Some(term.name.clone());
    } else if lower == "sha-1" && source_file.checksum_sha1.is_none() {
        source_file.checksum_sha1 = term.value.clone();
    }
    source_file.important_cv_terms.push(format_cv_term(term));
}

fn apply_spectrum_cv_term(term: &CvTerm, spectrum: &mut SpectrumSummary) {
    match term.name.as_str() {
        "ms level" => {
            if let Some(value) = &term.value
                && let Ok(level) = value.parse::<u32>()
            {
                spectrum.ms_levels.insert(level);
            }
        }
        "positive scan" => spectrum.saw_positive = true,
        "negative scan" => spectrum.saw_negative = true,
        "centroid spectrum" => spectrum.saw_centroid = true,
        "profile spectrum" => spectrum.saw_profile = true,
        _ => {}
    }
}

fn looks_like_non_model_instrument_term(term: &CvTerm) -> bool {
    term.name == "instrument serial number"
}

fn parse_cv_term(event: &BytesStart<'_>) -> Result<CvTerm> {
    Ok(CvTerm {
        accession: attr_value(event, b"accession")?,
        name: attr_value(event, b"name")?.unwrap_or_default(),
        value: attr_value(event, b"value")?,
    })
}

fn format_cv_term(term: &CvTerm) -> String {
    match (&term.accession, &term.value) {
        (Some(accession), Some(value)) if !value.is_empty() => {
            format!("{accession}|{}={value}", term.name)
        }
        (Some(accession), _) => format!("{accession}|{}", term.name),
        (None, Some(value)) if !value.is_empty() => format!("{}={value}", term.name),
        (None, _) => term.name.clone(),
    }
}

fn attr_value(event: &BytesStart<'_>, key: &[u8]) -> Result<Option<String>> {
    for attribute in event.attributes().with_checks(false) {
        let attribute = attribute?;
        if attribute.key == QName(key) {
            return Ok(Some(
                String::from_utf8_lossy(attribute.value.as_ref()).into_owned(),
            ));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{apply_spectrum_cv_term, CvTerm};
    use crate::model::SpectrumSummary;

    #[test]
    fn summarizes_spectrum_properties() {
        let mut summary = SpectrumSummary {
            ms_levels: BTreeSet::new(),
            ..SpectrumSummary::default()
        };
        apply_spectrum_cv_term(
            &CvTerm {
                accession: None,
                name: "ms level".to_string(),
                value: Some("1".to_string()),
            },
            &mut summary,
        );
        apply_spectrum_cv_term(
            &CvTerm {
                accession: None,
                name: "positive scan".to_string(),
                value: None,
            },
            &mut summary,
        );
        apply_spectrum_cv_term(
            &CvTerm {
                accession: None,
                name: "centroid spectrum".to_string(),
                value: None,
            },
            &mut summary,
        );
        assert_eq!(summary.ms_level_coverage().as_deref(), Some("1"));
        assert_eq!(summary.polarity().as_deref(), Some("positive"));
        assert_eq!(summary.signal_continuity().as_deref(), Some("centroid"));
    }
}
