use std::io;

use anyhow::Result;
use clap::Parser;
use mzmlwatcher::cli::{Cli, Command};
use mzmlwatcher::config::Settings;
use mzmlwatcher::db::Database;
use mzmlwatcher::export::{export_query_to_writer, export_view_to_tsv, schema_sql};
use mzmlwatcher::watch::watch_directory;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose)?;

    match cli.command {
        Command::Scan(args) => {
            let settings = Settings::from_scan_args(args)?;
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
            let mut db = Database::open(&settings.sqlite_path)?;
            watch_directory(&settings, &mut db)?;
        }
        Command::ExportTsv(args) => {
            let db = Database::open(&args.sqlite_path)?;
            export_view_to_tsv(db.connection(), &args.output_tsv, args.include_failed)?;
        }
        Command::Query(args) => {
            let db = Database::open(&args.sqlite_path)?;
            let sql = args
                .sql
                .unwrap_or_else(|| "SELECT * FROM v_metadata_flat ORDER BY file_path".to_string());
            export_query_to_writer(db.connection(), &sql, io::stdout())?;
        }
        Command::Schema(args) => {
            let _db = Database::open(&args.sqlite_path)?;
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
