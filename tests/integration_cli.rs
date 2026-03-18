mod common;

use std::fs;

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
    let row: (String, String, String) = conn
        .query_row(
            "SELECT file_name, instrument_model, sample_name FROM v_metadata_flat",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(row.0, "fixture.mzML");
    assert_eq!(row.1, "Q Exactive Plus");
    assert_eq!(row.2, "Fixture Sample");
}

#[test]
fn export_tsv_writes_stable_header() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");
    let tsv_path = tempdir.path().join("metadata.tsv");

    cargo_bin()
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
        .args([
            "export-tsv",
            db_path.to_str().unwrap(),
            tsv_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let expected_header = fs::read_to_string(fixture_path("expected_header.tsv")).unwrap();
    let exported = fs::read_to_string(tsv_path).unwrap();
    let actual_header = exported.lines().next().unwrap().to_string() + "\n";
    assert_eq!(actual_header, expected_header);
}

#[test]
fn query_prints_tabular_output() {
    let tempdir = tempdir().unwrap();
    install_fixture(&tempdir, "fixture.mzML");
    let db_path = tempdir.path().join("metadata.sqlite");

    cargo_bin()
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
        .args([
            "query",
            db_path.to_str().unwrap(),
            "--sql",
            "SELECT file_name, instrument_model FROM v_metadata_flat ORDER BY file_name",
        ])
        .assert()
        .success()
        .stdout(contains("file_name\tinstrument_model"))
        .stdout(contains("fixture.mzML\tQ Exactive Plus"));
}
