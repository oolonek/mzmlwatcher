//! SQLite storage and schema management.

use std::path::Path;

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::model::{
    DataProcessingRecord, FileIdentity, InstrumentConfigRecord, ParsedMetadata, SampleRecord,
    SoftwareRecord, SourceFileRecord,
};

/// SQLite wrapper for schema management and upserts.
#[derive(Debug)]
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open or create the SQLite database and ensure the schema exists.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(Self { conn })
    }

    /// Borrow the underlying SQLite connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Return `true` when the stored fingerprint matches the current file.
    pub fn is_unchanged(&self, identity: &FileIdentity) -> Result<bool> {
        let row = self
            .conn
            .query_row(
                "SELECT file_size_bytes, modified_time, checksum FROM files WHERE canonical_path = ?1",
                params![identity.canonical_path],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(match row {
            Some((file_size_bytes, modified_time, checksum)) => {
                file_size_bytes == identity.file_size_bytes
                    && modified_time == identity.modified_time
                    && checksum == identity.checksum
            }
            None => false,
        })
    }

    /// Insert or replace metadata for a file and its child records.
    pub fn upsert_metadata(&mut self, metadata: &ParsedMetadata) -> Result<()> {
        let tx = self.conn.transaction()?;
        let file_id = upsert_file(&tx, metadata)?;
        clear_children(&tx, file_id)?;
        insert_run(&tx, file_id, metadata)?;
        insert_instrument_configs(&tx, file_id, &metadata.instrument_configs)?;
        insert_software(&tx, file_id, &metadata.software)?;
        insert_samples(&tx, file_id, &metadata.samples)?;
        insert_data_processings(&tx, file_id, &metadata.data_processings)?;
        insert_source_files(&tx, file_id, &metadata.source_files)?;
        tx.commit()?;
        Ok(())
    }
}

pub(crate) const SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    canonical_path TEXT NOT NULL UNIQUE,
    discovered_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    modified_time TEXT NOT NULL,
    checksum TEXT,
    parse_timestamp TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    mzml_version TEXT,
    parse_status TEXT NOT NULL,
    parse_error TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL UNIQUE REFERENCES files(id) ON DELETE CASCADE,
    run_id TEXT,
    start_time_stamp TEXT,
    default_instrument_configuration_ref TEXT,
    default_source_file_ref TEXT,
    sample_ref TEXT,
    native_id_format TEXT,
    polarity TEXT,
    ms_level_coverage TEXT,
    spectrum_count INTEGER,
    chromatogram_count INTEGER,
    signal_continuity TEXT
);

CREATE TABLE IF NOT EXISTS instrument_configs (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    instrument_configuration_id TEXT NOT NULL,
    instrument_model TEXT,
    ionization_source TEXT,
    analyzer TEXT,
    detector TEXT,
    important_cv_terms TEXT
);

CREATE TABLE IF NOT EXISTS software (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    software_id TEXT NOT NULL,
    software_name TEXT,
    software_version TEXT,
    important_cv_terms TEXT
);

CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    sample_id TEXT NOT NULL,
    sample_name TEXT,
    important_cv_terms TEXT
);

CREATE TABLE IF NOT EXISTS data_processings (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    data_processing_id TEXT NOT NULL,
    software_ref TEXT,
    processing_actions TEXT
);

CREATE TABLE IF NOT EXISTS source_files (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    source_file_id TEXT NOT NULL,
    source_file_name TEXT,
    source_file_location TEXT,
    native_id_format TEXT,
    file_format TEXT,
    checksum_sha1 TEXT,
    important_cv_terms TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_path ON files(canonical_path);
CREATE INDEX IF NOT EXISTS idx_runs_run_id ON runs(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_start_time_stamp ON runs(start_time_stamp);
CREATE INDEX IF NOT EXISTS idx_samples_name ON samples(sample_name);
CREATE INDEX IF NOT EXISTS idx_instrument_model ON instrument_configs(instrument_model);
CREATE INDEX IF NOT EXISTS idx_source_files_name ON source_files(source_file_name);

CREATE VIEW IF NOT EXISTS v_metadata_flat AS
SELECT
    f.id AS file_id,
    f.canonical_path AS file_path,
    f.file_name,
    f.file_size_bytes,
    f.modified_time,
    COALESCE(f.checksum, '') AS checksum,
    f.parse_timestamp,
    f.parser_version,
    COALESCE(f.mzml_version, '') AS mzml_version,
    f.parse_status,
    COALESCE(f.parse_error, '') AS parse_error,
    COALESCE(r.run_id, '') AS run_id,
    COALESCE(r.start_time_stamp, '') AS acquisition_date,
    COALESCE(r.default_instrument_configuration_ref, '') AS default_instrument_configuration_ref,
    COALESCE(r.default_source_file_ref, '') AS default_source_file_ref,
    COALESCE(r.sample_ref, '') AS sample_ref,
    COALESCE(r.native_id_format, '') AS native_id_format,
    COALESCE(r.polarity, '') AS polarity,
    COALESCE(r.ms_level_coverage, '') AS ms_level_coverage,
    COALESCE(r.spectrum_count, '') AS spectrum_count,
    COALESCE(r.chromatogram_count, '') AS chromatogram_count,
    COALESCE(r.signal_continuity, '') AS signal_continuity,
    COALESCE((SELECT GROUP_CONCAT(sample_name, '; ') FROM samples s WHERE s.file_id = f.id), '') AS sample_name,
    COALESCE((SELECT GROUP_CONCAT(instrument_model, '; ') FROM instrument_configs i WHERE i.file_id = f.id), '') AS instrument_model,
    COALESCE((SELECT GROUP_CONCAT(ionization_source, '; ') FROM instrument_configs i WHERE i.file_id = f.id), '') AS ionization_source,
    COALESCE((SELECT GROUP_CONCAT(analyzer, '; ') FROM instrument_configs i WHERE i.file_id = f.id), '') AS analyzer,
    COALESCE((SELECT GROUP_CONCAT(detector, '; ') FROM instrument_configs i WHERE i.file_id = f.id), '') AS detector,
    COALESCE((SELECT GROUP_CONCAT(software_name, '; ') FROM software sw WHERE sw.file_id = f.id), '') AS software_names,
    COALESCE((SELECT GROUP_CONCAT(software_version, '; ') FROM software sw WHERE sw.file_id = f.id), '') AS software_versions,
    COALESCE((SELECT GROUP_CONCAT(data_processing_id, '; ') FROM data_processings dp WHERE dp.file_id = f.id), '') AS data_processing_ids,
    COALESCE((SELECT GROUP_CONCAT(processing_actions, '; ') FROM data_processings dp WHERE dp.file_id = f.id), '') AS processing_actions,
    COALESCE((SELECT GROUP_CONCAT(source_file_name, '; ') FROM source_files sf WHERE sf.file_id = f.id), '') AS source_file_names,
    COALESCE((SELECT GROUP_CONCAT(source_file_location, '; ') FROM source_files sf WHERE sf.file_id = f.id), '') AS source_file_paths
FROM files f
LEFT JOIN runs r ON r.file_id = f.id;
"#;

fn upsert_file(tx: &Transaction<'_>, metadata: &ParsedMetadata) -> Result<i64> {
    tx.execute(
        r#"
        INSERT INTO files (
            canonical_path,
            discovered_path,
            file_name,
            file_size_bytes,
            modified_time,
            checksum,
            parse_timestamp,
            parser_version,
            mzml_version,
            parse_status,
            parse_error
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ON CONFLICT(canonical_path) DO UPDATE SET
            discovered_path = excluded.discovered_path,
            file_name = excluded.file_name,
            file_size_bytes = excluded.file_size_bytes,
            modified_time = excluded.modified_time,
            checksum = excluded.checksum,
            parse_timestamp = excluded.parse_timestamp,
            parser_version = excluded.parser_version,
            mzml_version = excluded.mzml_version,
            parse_status = excluded.parse_status,
            parse_error = excluded.parse_error
        "#,
        params![
            metadata.file.identity.canonical_path,
            metadata.file.identity.path.to_string_lossy().into_owned(),
            metadata.file.identity.file_name,
            metadata.file.identity.file_size_bytes,
            metadata.file.identity.modified_time,
            metadata.file.identity.checksum,
            metadata.file.parse_timestamp,
            metadata.file.parser_version,
            metadata.file.mzml_version,
            metadata.file.status.as_str(),
            metadata.file.parse_error,
        ],
    )?;

    let file_id = tx.query_row(
        "SELECT id FROM files WHERE canonical_path = ?1",
        params![metadata.file.identity.canonical_path],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(file_id)
}

fn clear_children(tx: &Transaction<'_>, file_id: i64) -> Result<()> {
    tx.execute("DELETE FROM runs WHERE file_id = ?1", params![file_id])?;
    tx.execute(
        "DELETE FROM instrument_configs WHERE file_id = ?1",
        params![file_id],
    )?;
    tx.execute("DELETE FROM software WHERE file_id = ?1", params![file_id])?;
    tx.execute("DELETE FROM samples WHERE file_id = ?1", params![file_id])?;
    tx.execute(
        "DELETE FROM data_processings WHERE file_id = ?1",
        params![file_id],
    )?;
    tx.execute("DELETE FROM source_files WHERE file_id = ?1", params![file_id])?;
    Ok(())
}

fn insert_run(tx: &Transaction<'_>, file_id: i64, metadata: &ParsedMetadata) -> Result<()> {
    if metadata.run == crate::model::RunRecord::default() {
        return Ok(());
    }
    tx.execute(
        r#"
        INSERT INTO runs (
            file_id,
            run_id,
            start_time_stamp,
            default_instrument_configuration_ref,
            default_source_file_ref,
            sample_ref,
            native_id_format,
            polarity,
            ms_level_coverage,
            spectrum_count,
            chromatogram_count,
            signal_continuity
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            file_id,
            metadata.run.run_id,
            metadata.run.start_time_stamp,
            metadata.run.default_instrument_configuration_ref,
            metadata.run.default_source_file_ref,
            metadata.run.sample_ref,
            metadata.run.native_id_format,
            metadata.run.polarity,
            metadata.run.ms_level_coverage,
            metadata.run.spectrum_count,
            metadata.run.chromatogram_count,
            metadata.run.signal_continuity,
        ],
    )?;
    Ok(())
}

fn insert_instrument_configs(
    tx: &Transaction<'_>,
    file_id: i64,
    records: &[InstrumentConfigRecord],
) -> Result<()> {
    let mut statement = tx.prepare(
        r#"
        INSERT INTO instrument_configs (
            file_id,
            instrument_configuration_id,
            instrument_model,
            ionization_source,
            analyzer,
            detector,
            important_cv_terms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
    )?;
    for record in records {
        statement.execute(params![
            file_id,
            record.id,
            record.model,
            join(&record.ionization_sources),
            join(&record.analyzers),
            join(&record.detectors),
            join(&record.important_cv_terms),
        ])?;
    }
    Ok(())
}

fn insert_software(tx: &Transaction<'_>, file_id: i64, records: &[SoftwareRecord]) -> Result<()> {
    let mut statement = tx.prepare(
        r#"
        INSERT INTO software (
            file_id,
            software_id,
            software_name,
            software_version,
            important_cv_terms
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
    )?;
    for record in records {
        statement.execute(params![
            file_id,
            record.id,
            record.name,
            record.version,
            join(&record.important_cv_terms),
        ])?;
    }
    Ok(())
}

fn insert_samples(tx: &Transaction<'_>, file_id: i64, records: &[SampleRecord]) -> Result<()> {
    let mut statement = tx.prepare(
        r#"
        INSERT INTO samples (
            file_id,
            sample_id,
            sample_name,
            important_cv_terms
        ) VALUES (?1, ?2, ?3, ?4)
        "#,
    )?;
    for record in records {
        statement.execute(params![
            file_id,
            record.id,
            record.name,
            join(&record.important_cv_terms),
        ])?;
    }
    Ok(())
}

fn insert_data_processings(
    tx: &Transaction<'_>,
    file_id: i64,
    records: &[DataProcessingRecord],
) -> Result<()> {
    let mut statement = tx.prepare(
        r#"
        INSERT INTO data_processings (
            file_id,
            data_processing_id,
            software_ref,
            processing_actions
        ) VALUES (?1, ?2, ?3, ?4)
        "#,
    )?;
    for record in records {
        statement.execute(params![
            file_id,
            record.id,
            record.software_ref,
            join(&record.processing_actions),
        ])?;
    }
    Ok(())
}

fn insert_source_files(
    tx: &Transaction<'_>,
    file_id: i64,
    records: &[SourceFileRecord],
) -> Result<()> {
    let mut statement = tx.prepare(
        r#"
        INSERT INTO source_files (
            file_id,
            source_file_id,
            source_file_name,
            source_file_location,
            native_id_format,
            file_format,
            checksum_sha1,
            important_cv_terms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
    )?;
    for record in records {
        statement.execute(params![
            file_id,
            record.id,
            record.name,
            record.location,
            record.native_id_format,
            record.file_format,
            record.checksum_sha1,
            join(&record.important_cv_terms),
        ])?;
    }
    Ok(())
}

fn join(values: &[String]) -> String {
    values.join("; ")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{Database, SCHEMA_SQL};
    use crate::fs::now_rfc3339;
    use crate::model::{
        FileIdentity, FileRecord, ParseStatus, ParsedMetadata, RunRecord, SpectrumSummary,
    };

    #[test]
    fn schema_contains_flat_view() {
        assert!(SCHEMA_SQL.contains("CREATE VIEW IF NOT EXISTS v_metadata_flat"));
    }

    #[test]
    fn creates_database_schema() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let db = Database::open(&db_path).unwrap();
        let count: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'files'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn dedup_detects_unchanged_identity() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let mut db = Database::open(&db_path).unwrap();
        let metadata = ParsedMetadata {
            file: FileRecord {
                identity: FileIdentity {
                    path: PathBuf::from("/tmp/test.mzML"),
                    canonical_path: "/tmp/test.mzML".to_string(),
                    file_name: "test.mzML".to_string(),
                    file_size_bytes: 123,
                    modified_time: "2024-01-01T00:00:00.000Z".to_string(),
                    checksum: Some("abc".to_string()),
                },
                parse_timestamp: now_rfc3339(),
                parser_version: "0.1.0".to_string(),
                mzml_version: Some("1.1.0".to_string()),
                status: ParseStatus::Success,
                parse_error: None,
            },
            run: RunRecord {
                spectrum_count: Some(1),
                ms_level_coverage: SpectrumSummary::default().ms_level_coverage(),
                ..RunRecord::default()
            },
            instrument_configs: Vec::new(),
            software: Vec::new(),
            samples: Vec::new(),
            data_processings: Vec::new(),
            source_files: Vec::new(),
        };
        db.upsert_metadata(&metadata).unwrap();
        assert!(db.is_unchanged(&metadata.file.identity).unwrap());
    }
}
