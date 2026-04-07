mod common;

use std::fs;
use std::path::Path;

use predicates::str::contains;
use rusqlite::Connection;
use tempfile::tempdir;

use common::{cargo_bin, fixture_path, install_fixture};

#[test]
fn scan_populates_sqlite_metadata() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "scan",
            tempdir.path().to_str().unwrap(),
            "--sqlite",
            db_path.to_str().unwrap(),
            "--settle-seconds",
            "0",
        ])
        .assert()
        .success()
        .stdout(contains("scanned=1 changed=1 skipped=0 failed=0"));

    let conn = Connection::open(&db_path).unwrap();
    let row: (String, String, String, String, String, String) = conn
        .query_row(
            "SELECT file_name, instrument_model, sample_name, raw_file_sha1, ontology_links, ontology_curies FROM v_metadata_flat",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .unwrap();
    assert_eq!(row.0, "fixture.mzML");
    assert_eq!(row.1, "MS:1002634|Q Exactive Plus");
    assert_eq!(row.2, "Fixture Sample");
    assert_eq!(row.3, "0123456789abcdef0123456789abcdef01234567");
    assert!(row.4.contains("MS=https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"));
    assert!(row.5.contains("MS:1000569"));
}

#[test]
fn export_tsv_writes_stable_header() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");
    let tsv_path = tempdir.path().join("metadata.tsv");

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "scan",
            tempdir.path().to_str().unwrap(),
            "--sqlite",
            db_path.to_str().unwrap(),
            "--settle-seconds",
            "0",
        ])
        .assert()
        .success();

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "export-tsv",
            db_path.to_str().unwrap(),
            tsv_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let expected_header = concat!(
        "file_path\tfile_name\tfile_size_bytes\tmodified_time\tchecksum\tconverted_file_sha1\t",
        "parse_timestamp\tparser_version\tmzml_version\tparse_status\tparse_error\trun_id\t",
        "acquisition_date\tdefault_instrument_configuration_ref\tdefault_source_file_ref\t",
        "sample_ref\tnative_id_format\tpolarity\tms_level_coverage\tspectrum_count\t",
        "chromatogram_count\tsignal_continuity\tsample_name\tinstrument_model\t",
        "ionization_source\tanalyzer\tdetector\tsoftware_names\tsoftware_versions\t",
        "data_processing_ids\tprocessing_actions\tsource_file_names\tsource_file_paths\t",
        "raw_file_sha1\tontology_links\n"
    );
    let exported = fs::read_to_string(tsv_path).unwrap();
    let actual_header = exported.lines().next().unwrap().to_string() + "\n";
    assert_eq!(actual_header, expected_header);
}

#[test]
fn export_tsv_works_on_existing_database() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");
    let tsv_path = tempdir.path().join("metadata.tsv");

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "scan",
            tempdir.path().to_str().unwrap(),
            "--sqlite",
            db_path.to_str().unwrap(),
            "--settle-seconds",
            "0",
        ])
        .assert()
        .success();

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "export-tsv",
            db_path.to_str().unwrap(),
            tsv_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert!(tsv_path.exists());
}

#[test]
fn query_prints_tabular_output() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "scan",
            tempdir.path().to_str().unwrap(),
            "--sqlite",
            db_path.to_str().unwrap(),
            "--settle-seconds",
            "0",
        ])
        .assert()
        .success();

    cargo_bin()
        .current_dir(tempdir.path())
        .args([
            "query",
            db_path.to_str().unwrap(),
            "--sql",
            "SELECT file_name, instrument_model FROM v_metadata_flat ORDER BY file_name",
        ])
        .assert()
        .success()
        .stdout(contains("file_name\tinstrument_model"))
        .stdout(contains("fixture.mzML\tMS:1002634|Q Exactive Plus"));
}

#[test]
fn scan_and_export_use_dotenv_defaults() {
    let tempdir = tempdir().unwrap();
    let scan_dir = tempdir.path().join("input");
    let output_dir = tempdir.path().join("output");
    fs::create_dir_all(&scan_dir).unwrap();
    install_fixture_at(&scan_dir, "fixture.mzML");
    fs::write(
        tempdir.path().join(".env"),
        format!(
            "MZMLWATCHER_SCAN_DIR={}\nMZMLWATCHER_OUTPUT_DIR={}\n",
            scan_dir.display(),
            output_dir.display()
        ),
    )
    .unwrap();

    cargo_bin()
        .current_dir(tempdir.path())
        .args(["scan", "--settle-seconds", "0"])
        .assert()
        .success()
        .stdout(contains("scanned=1 changed=1 skipped=0 failed=0"));

    let db_path = output_dir.join("mzmlwatcher.sqlite");
    let tsv_path = output_dir.join("mzmlwatcher.tsv");
    assert!(db_path.exists());
    assert!(tsv_path.exists());

    cargo_bin()
        .current_dir(tempdir.path())
        .args(["export-tsv"])
        .assert()
        .success();

    let exported = fs::read_to_string(tsv_path).unwrap();
    assert!(exported.contains("fixture.mzML"));
}

fn install_fixture_at(directory: &Path, file_name: &str) {
    let source = fixture_path("minimal.mzML");
    let target = directory.join(file_name);
    fs::copy(source, target).unwrap();
}
