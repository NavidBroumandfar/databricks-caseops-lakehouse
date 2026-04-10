-- ============================================================
-- 02_bronze_bootstrap_v1.sql
-- Bronze full bootstrap: validated in personal Databricks workspace (A-3B)
--
-- Purpose: Ingest all raw PDFs from the managed volume into the production
-- Bronze table with full provenance fields: document UUID, file hash,
-- ingestion timestamp, and schema version. This is the authoritative
-- Bronze layer for the bootstrap pass.
--
-- Validated outputs: 4 documents ingested; parse_status = 'success' for all 4.
-- Note: ai_parse_document result is stored in parsed_content (VARIANT type).
-- ============================================================

CREATE OR REPLACE TABLE caseops.bronze.parsed_documents AS
SELECT
  uuid() AS document_id,
  path AS source_path,
  regexp_extract(path, '[^/]+$', 0) AS file_name,
  length AS file_size_bytes,
  sha2(content, 256) AS file_hash,
  current_timestamp() AS ingested_at,
  current_timestamp() AS parsed_at,
  'success' AS parse_status,
  ai_parse_document(
    content,
    MAP('version', '2.0')
  ) AS parsed_content,
  'bootstrap_sql_v1' AS pipeline_run_id,
  'v0.1.0' AS schema_version
FROM READ_FILES(
  '/Volumes/caseops/raw/documents/fda_warning_letters/',
  format => 'binaryFile'
);

SELECT
  document_id,
  file_name,
  file_size_bytes,
  file_hash,
  parse_status,
  parsed_at,
  schema_version
FROM caseops.bronze.parsed_documents
LIMIT 10;
