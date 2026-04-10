-- ============================================================
-- 01_bronze_parse_smoke.sql
-- Bronze parse smoke test: validated in personal Databricks workspace (A-3B)
--
-- Purpose: Confirm that ai_parse_document processes raw PDF files from the
-- managed volume and writes results to a Bronze smoke table. Run against
-- a small batch of public FDA warning letter PDFs before running the full
-- bootstrap.
--
-- Validated outputs: 4/4 documents parsed successfully.
-- ============================================================

CREATE OR REPLACE TABLE caseops.bronze.parsed_documents_smoke AS
SELECT
  path AS source_path,
  current_timestamp() AS parsed_at,
  ai_parse_document(
    content,
    MAP('version', '2.0')
  ) AS parsed_content
FROM READ_FILES(
  '/Volumes/caseops/raw/documents/fda_warning_letters/',
  format => 'binaryFile'
);

SELECT *
FROM caseops.bronze.parsed_documents_smoke
LIMIT 5;
