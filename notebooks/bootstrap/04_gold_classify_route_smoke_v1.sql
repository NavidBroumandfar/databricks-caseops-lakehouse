-- ============================================================
-- 04_gold_classify_route_smoke_v1.sql
-- Gold classification and routing smoke test: validated in personal Databricks workspace (A-3B)
--
-- Purpose: Classify Silver extraction records using ai_classify with the
-- FDA warning letter label taxonomy, then apply rule-based routing to
-- assign routing_label. Export-ready records are labeled 'regulatory_review';
-- records that do not match the expected label are routed to 'quarantine'.
--
-- Validated outputs:
--   3 records → routing_label = 'regulatory_review', export_ready = true
--   1 record  → routing_label = 'quarantine', export_ready = false
--
-- Implementation note: classification_confidence is stored as NULL in this
-- bootstrap-stage SQL implementation. ai_classify in the Databricks SQL AI
-- Functions API (as used here) does not return a scalar confidence score in
-- the response variant at this bootstrap stage. This is an explicit,
-- documented bootstrap-stage limitation — not a hidden gap. A-4 will
-- address confidence extraction and evaluation instrumentation.
-- ============================================================

CREATE OR REPLACE TABLE caseops.gold.ai_ready_assets_smoke AS
WITH classified AS (
  SELECT
    document_id,
    source_path,
    file_name,
    extraction_result,
    extracted_at,
    extraction_prompt_id,
    ai_classify(
      extraction_result,
      '{
        "fda_warning_letter": "FDA-issued warning letter to a regulated company",
        "unknown": "Document does not clearly match the FDA warning letter class"
      }',
      MAP(
        'version', '2.0',
        'instructions', 'Classify the document type for a regulated-document pipeline. These records are expected to be FDA warning letters unless the extracted content clearly indicates otherwise.'
      )
    ) AS document_type_result
  FROM caseops.silver.extracted_records_smoke
)
SELECT
  document_id,
  source_path,
  file_name,
  current_timestamp() AS classified_at,
  document_type_result,
  try_variant_get(document_type_result, '$.response[0]', 'string') AS document_type_label,
  CASE
    WHEN try_variant_get(document_type_result, '$.response[0]', 'string') = 'fda_warning_letter'
      THEN 'regulatory_review'
    ELSE 'quarantine'
  END AS routing_label,
  CAST(NULL AS DOUBLE) AS classification_confidence,
  extraction_result AS export_payload,
  CASE
    WHEN try_variant_get(document_type_result, '$.response[0]', 'string') = 'fda_warning_letter'
      THEN true
    ELSE false
  END AS export_ready,
  'bootstrap_sql_v1' AS pipeline_run_id,
  'v0.1.0' AS schema_version
FROM classified;

SELECT
  document_id,
  file_name,
  document_type_label,
  routing_label,
  classification_confidence,
  export_ready,
  schema_version
FROM caseops.gold.ai_ready_assets_smoke
LIMIT 10;
