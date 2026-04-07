use std::io;

use anyhow::Result;
use clap::Parser;
use mzmlwatcher::cli::{Cli, Command};
use mzmlwatcher::config::{DatabasePathSettings, ExportTsvSettings, Settings, ensure_parent_dir};
use mzmlwatcher::db::Database;
use mzmlwatcher::export::{export_query_to_writer, export_view_to_tsv, schema_sql};
use mzmlwatcher::watch::watch_directory;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();
    init_tracing(cli.verbose)?;

    match cli.command {
        Command::Scan(args) => {
            let settings = Settings::from_scan_args(args)?;
            ensure_parent_dir(&settings.sqlite_path)?;
            if let Some(path) = &settings.tsv_path {
                ensure_parent_dir(path)?;
            }
            let mut db = Database::open(&settings.sqlite_path)?;
            let summary = mzmlwatcher::watch::run_scan(&settings, &mut db)?;
            if let Some(path) = &settings.tsv_path {
                export_view_to_tsv(db.connection(), path, settings.include_failed)?;
            }
            println!(
                "scanned={} changed={} skipped={} failed={}",
                summary.scanned, summary.changed, summary.skipped, summary.failed
            );
        }
        Command::Watch(args) => {
            let settings = Settings::from_watch_args(args)?;
            ensure_parent_dir(&settings.sqlite_path)?;
            if let Some(path) = &settings.tsv_path {
                ensure_parent_dir(path)?;
            }
            let mut db = Database::open(&settings.sqlite_path)?;
            watch_directory(&settings, &mut db)?;
        }
        Command::ExportTsv(args) => {
            let settings = ExportTsvSettings::from_args(args);
            ensure_parent_dir(&settings.sqlite_path)?;
            ensure_parent_dir(&settings.output_tsv)?;
            let db = Database::open(&settings.sqlite_path)?;
            export_view_to_tsv(
                db.connection(),
                &settings.output_tsv,
                settings.include_failed,
            )?;
        }
        Command::Query(args) => {
            let sql = args
                .sql
                .clone()
                .unwrap_or_else(|| "SELECT * FROM v_metadata_flat ORDER BY file_path".to_string());
            let settings = DatabasePathSettings::from_query_args(args);
            let db = Database::open(&settings.sqlite_path)?;
            export_query_to_writer(db.connection(), &sql, io::stdout())?;
        }
        Command::Schema(args) => {
            let settings = DatabasePathSettings::from_schema_args(args);
            let _db = Database::open(&settings.sqlite_path)?;
            println!("{}", schema_sql());
        }
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}

fn init_tracing(verbose: u8) -> Result<()> {
    let directive = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(directive)),
        )
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(())
}
