//! TSV export and read-only query helpers.

use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Result};
use csv::WriterBuilder;
use rusqlite::{types::ValueRef, Connection};

use crate::db::{CREATE_VIEW_SQL, DROP_VIEW_SQL, SCHEMA_SQL};

const DEFAULT_EXPORT_SQL: &str = r#"
SELECT
    file_path,
    file_name,
    file_size_bytes,
    modified_time,
    checksum,
    converted_file_sha1,
    parse_timestamp,
    parser_version,
    mzml_version,
    parse_status,
    parse_error,
    run_id,
    acquisition_date,
    default_instrument_configuration_ref,
    default_source_file_ref,
    sample_ref,
    native_id_format,
    polarity,
    ms_level_coverage,
    spectrum_count,
    chromatogram_count,
    signal_continuity,
    sample_name,
    instrument_model,
    ionization_source,
    analyzer,
    detector,
    software_names,
    software_versions,
    data_processing_ids,
    processing_actions,
    source_file_names,
    source_file_paths,
    raw_file_sha1,
    ontology_links
FROM v_metadata_flat
"#;

/// Export the flattened metadata view to a TSV file.
pub fn export_view_to_tsv(conn: &Connection, output_tsv: &Path, include_failed: bool) -> Result<()> {
    let file = std::fs::File::create(output_tsv)?;
    let sql = if include_failed {
        format!("{DEFAULT_EXPORT_SQL} ORDER BY file_path")
    } else {
        format!("{DEFAULT_EXPORT_SQL} WHERE parse_status = 'success' ORDER BY file_path")
    };
    export_query_to_writer(conn, &sql, file)
}

/// Execute a validated read-only query and write TSV to the provided writer.
pub fn export_query_to_writer<W: Write>(
    conn: &Connection,
    sql: &str,
    writer: W,
) -> Result<()> {
    validate_query(sql)?;
    let mut statement = conn.prepare(sql)?;
    if !statement.readonly() {
        return Err(anyhow!("query must be read-only"));
    }
    let headers = statement
        .column_names()
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut rows = statement.query([])?;
    let mut csv_writer = WriterBuilder::new().delimiter(b'\t').from_writer(writer);
    csv_writer.write_record(&headers)?;
    while let Some(row) = rows.next()? {
        let mut record = Vec::with_capacity(headers.len());
        for index in 0..headers.len() {
            let value = match row.get_ref(index)? {
                ValueRef::Null => String::new(),
                ValueRef::Integer(value) => value.to_string(),
                ValueRef::Real(value) => value.to_string(),
                ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
                ValueRef::Blob(value) => hex::encode(value),
            };
            record.push(value);
        }
        csv_writer.write_record(&record)?;
    }
    csv_writer.flush()?;
    Ok(())
}

/// Return the schema DDL used by `mzmlwatcher`.
pub fn schema_sql() -> String {
    format!("{SCHEMA_SQL}\n{DROP_VIEW_SQL};\n{CREATE_VIEW_SQL}")
}

fn validate_query(sql: &str) -> Result<()> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(anyhow!("query must not be empty"));
    }
    if trimmed.matches(';').count() > 1 || trimmed[..trimmed.len().saturating_sub(1)].contains(';') {
        return Err(anyhow!("multiple SQL statements are not allowed"));
    }
    if !(upper.starts_with("SELECT") || upper.starts_with("WITH")) {
        return Err(anyhow!("only SELECT or WITH queries are allowed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::db::Database;

    use super::export_query_to_writer;

    #[test]
    fn rejects_non_select_queries() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.sqlite")).unwrap();
        let result = export_query_to_writer(db.connection(), "DELETE FROM files", Vec::new());
        assert!(result.is_err());
    }
}
