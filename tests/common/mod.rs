use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use tempfile::TempDir;

pub fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

pub fn install_fixture(tempdir: &TempDir, file_name: &str) -> PathBuf {
    let source = fixture_path("minimal.mzML");
    let target = tempdir.path().join(file_name);
    fs::copy(source, &target).unwrap();
    target
}

pub fn cargo_bin() -> Command {
    Command::cargo_bin("mzmlwatcher").unwrap()
}
