# mzmlwatcher

`mzmlwatcher` is a Rust CLI for compact mzML provenance indexing. It scans or watches a directory of `.mzML` / `.mzml` files, extracts header-level metadata, stores the results in SQLite, and exports a stable denormalized TSV for downstream inspection.

The project is intentionally limited to metadata and provenance. It does not store spectra or chromatogram peak arrays.

## Features

- `scan <directory>` for one-shot ingestion
- `watch <directory>` for continuous ingestion of new or changed files
- `export-tsv <sqlite_path> <output_tsv>` for stable TSV export
- `query <sqlite_path> [--sql "..."]` for read-only ad hoc SQL
- `schema <sqlite_path>` to print the schema DDL
- `version`

## Installation

```bash
cargo install --path .
```

Or run directly from the workspace:

```bash
cargo run -- scan ./data --sqlite mzmlwatcher.sqlite --settle-seconds 0
```

## Example Commands

One-shot scan:

```bash
mzmlwatcher scan ./data \
  --sqlite mzmlwatcher.sqlite \
  --recursive \
  --checksum sha256
```

Continuous watch with polling fallback:

```bash
mzmlwatcher watch ./data \
  --sqlite mzmlwatcher.sqlite \
  --poll-interval 10 \
  --settle-seconds 5
```

Export TSV:

```bash
mzmlwatcher export-tsv mzmlwatcher.sqlite mzmlwatcher.tsv
```

Read-only query:

```bash
mzmlwatcher query mzmlwatcher.sqlite \
  --sql "SELECT file_path, acquisition_date, instrument_model FROM v_metadata_flat ORDER BY acquisition_date"
```

Print schema:

```bash
mzmlwatcher schema mzmlwatcher.sqlite
```

## What Gets Extracted

At minimum the CLI captures:

- File identity: path, file name, size, modified time, optional SHA-256, converted mzML SHA-1 from `<fileChecksum>`, parse status, parse error
- Run metadata: run id, acquisition timestamp, default refs, polarity, ms-level coverage, spectrum/chromatogram counts, continuity
- Instrument metadata: instrument configuration id, model, source, analyzer, detector
- Software and processing provenance
- Samples
- Source files, native ID format, and embedded source RAW SHA-1 values
- Ontology declarations from `<cvList>` and distinct CURIEs referenced in parsed metadata

## SQLite Schema Overview

Core tables:

- `files`
- `runs`
- `instrument_configs`
- `software`
- `samples`
- `data_processings`
- `source_files`

Convenience view:

- `v_metadata_flat`

The view is designed for quick queries and TSV export. It emits one row per file with denormalized aggregate columns such as `instrument_model`, `sample_name`, `software_names`, `processing_actions`, `source_file_names`, `ontology_links`, and `ontology_curies`.

## Sample Queries

See `examples/queries.sql`.

Examples:

```sql
SELECT file_path, acquisition_date
FROM v_metadata_flat
ORDER BY acquisition_date;

SELECT instrument_model, COUNT(*) AS file_count
FROM v_metadata_flat
GROUP BY instrument_model
ORDER BY file_count DESC;

SELECT file_path
FROM v_metadata_flat
WHERE sample_name = '';

SELECT DISTINCT software_names, software_versions
FROM v_metadata_flat
ORDER BY software_names;

SELECT file_path, source_file_names
FROM v_metadata_flat
WHERE source_file_names LIKE '%RAW%';
```

## Watcher Caveats

`mzmlwatcher` prefers `notify` native backends. Some filesystems and very large directories can behave inconsistently with native event delivery, so `--poll-interval <secs>` enables an explicit polling mode. Polling is slower, but usually more predictable across network mounts, containerized environments, and unusual host filesystems.

Both `scan` and `watch` apply a settle/debounce delay before parsing. Files modified too recently are deferred so partially written mzML files are not parsed prematurely.

## Architecture

- `src/parser.rs`: isolates mzML-specific parsing
- `src/db.rs`: schema creation, dedup checks, and transactional upserts
- `src/watch.rs`: one-shot scan and long-running watcher orchestration
- `src/export.rs`: TSV export and read-only query output
- `src/fs.rs`: traversal, path filtering, settle logic, checksum support

## mzdata Notes

The CLI uses `mzdata` to open and validate mzML files, which keeps mzML compatibility anchored to the library required by the project brief.

For broad header/provenance extraction, the implementation supplements `mzdata` with a streaming XML pass. The reason is practical: the current `mzdata` quickstart and reader APIs are optimized for spectrum iteration, while this tool needs a compact header-first projection across software lists, source files, data processing blocks, instrument configuration components, and stable TSV-ready aggregates without storing peak data.

As a result:

- mzML compatibility is still checked through `mzdata`
- Header/provenance extraction stays lightweight and deterministic
- No full-spectrum storage engine is introduced

## Development

Verification targets:

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```
