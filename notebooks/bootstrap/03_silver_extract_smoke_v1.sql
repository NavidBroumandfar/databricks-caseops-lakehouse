-- ============================================================
-- 03_silver_extract_smoke_v1.sql
-- Silver extraction smoke test: validated in personal Databricks workspace (A-3B)
--
-- Purpose: Extract structured fields from parsed Bronze records using
-- ai_extract with the FDA warning letter prompt schema. Reads from the
-- production Bronze table created in step 02.
--
-- Extraction prompt targets: issuing_office, recipient_company,
-- recipient_name, issue_date, corrective_action_requested, summary.
--
-- Validated outputs: 4/4 Bronze records produced Silver extraction records.
-- ============================================================

CREATE OR REPLACE TABLE caseops.silver.extracted_records_smoke AS
SELECT
  document_id,
  source_path,
  file_name,
  parsed_at,
  ai_extract(
    parsed_content,
    '{
      "issuing_office": {"type": "string", "description": "FDA issuing office or center"},
      "recipient_company": {"type": "string", "description": "Company receiving the warning letter"},
      "recipient_name": {"type": "string", "description": "Named recipient if present"},
      "issue_date": {"type": "string", "description": "Letter issue date in YYYY-MM-DD format if present"},
      "corrective_action_requested": {"type": "boolean", "description": "Whether the letter asks for corrective action or remediation"},
      "summary": {"type": "string", "description": "Short summary of the main findings in the warning letter"}
    }',
    MAP(
      'version', '2.0',
      'instructions', 'These are FDA warning letters. Extract the requested fields carefully from the parsed document.'
    )
  ) AS extraction_result,
  current_timestamp() AS extracted_at,
  'fda_warning_letter_v1' AS extraction_prompt_id,
  'bootstrap_sql_v1' AS pipeline_run_id,
  'v0.1.0' AS schema_version
FROM caseops.bronze.parsed_documents;

SELECT
  document_id,
  file_name,
  extraction_result,
  extracted_at,
  extraction_prompt_id,
  schema_version
FROM caseops.silver.extracted_records_smoke
LIMIT 10;
