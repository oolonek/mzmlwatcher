-- List all files and acquisition timestamps
SELECT file_path, acquisition_date
FROM v_metadata_flat
ORDER BY acquisition_date;

-- Count files by instrument model
SELECT instrument_model, COUNT(*) AS file_count
FROM v_metadata_flat
GROUP BY instrument_model
ORDER BY file_count DESC;

-- Find files with missing sample metadata
SELECT file_path
FROM v_metadata_flat
WHERE sample_name = ''
ORDER BY file_path;

-- List software versions observed
SELECT DISTINCT software_names, software_versions
FROM v_metadata_flat
ORDER BY software_names, software_versions;

-- Count files acquired in positive ion mode during calendar year 2025.
-- Polarity is stored as a CURIE-prefixed mzML CV label.
SELECT COUNT(*) AS positive_2025_file_count
FROM v_metadata_flat
WHERE polarity = 'MS:1000130|positive scan'
  AND acquisition_date >= '2025-01-01'
  AND acquisition_date < '2026-01-01';

-- Search by source file name
SELECT file_path, source_file_names
FROM v_metadata_flat
WHERE source_file_names LIKE '%RAW%'
ORDER BY file_path;
