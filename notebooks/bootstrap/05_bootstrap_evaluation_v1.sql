-- ============================================================
-- 05_bootstrap_evaluation_v1.sql
-- Bootstrap cross-layer evaluation: validated in personal Databricks workspace (A-3B)
--
-- Purpose: Join Bronze, Silver, and Gold smoke tables to validate full
-- lineage and produce a summary of the bootstrap pass. This is a manual
-- SQL evaluation proxy for the MLflow evaluation layer that will be
-- implemented in A-4.
--
-- Validated summary counts:
--   total_documents         = 4
--   bronze_success_count    = 4
--   silver_record_count     = 4
--   gold_export_ready_count = 3
--   quarantine_count        = 1
--   full_lineage_count      = 4
--
-- The quarantine record is a governance/evaluation signal, not a failure.
-- It confirms the routing logic is operating correctly.
-- ============================================================

-- Part 1: Per-document lineage trace
WITH bronze AS (
  SELECT
    document_id,
    file_name,
    parse_status,
    parsed_at,
    schema_version
  FROM caseops.bronze.parsed_documents
),
silver AS (
  SELECT
    document_id,
    file_name,
    extracted_at,
    extraction_result,
    schema_version
  FROM caseops.silver.extracted_records_smoke
),
gold AS (
  SELECT
    document_id,
    file_name,
    document_type_label,
    routing_label,
    export_ready,
    schema_version
  FROM caseops.gold.ai_ready_assets_smoke
)
SELECT
  b.document_id,
  b.file_name,
  b.parse_status,
  s.extracted_at IS NOT NULL AS has_silver_record,
  g.document_type_label,
  g.routing_label,
  g.export_ready,
  CASE
    WHEN b.document_id IS NOT NULL
     AND s.document_id IS NOT NULL
     AND g.document_id IS NOT NULL
    THEN true
    ELSE false
  END AS full_lineage_present,
  b.schema_version AS bronze_schema_version,
  s.schema_version AS silver_schema_version,
  g.schema_version AS gold_schema_version
FROM bronze b
LEFT JOIN silver s
  ON b.document_id = s.document_id
LEFT JOIN gold g
  ON b.document_id = g.document_id
ORDER BY b.file_name;

-- Part 2: Aggregate summary
SELECT
  COUNT(*) AS total_documents,
  SUM(CASE WHEN parse_status = 'success' THEN 1 ELSE 0 END) AS bronze_success_count,
  SUM(CASE WHEN has_silver_record THEN 1 ELSE 0 END) AS silver_record_count,
  SUM(CASE WHEN export_ready THEN 1 ELSE 0 END) AS gold_export_ready_count,
  SUM(CASE WHEN routing_label = 'quarantine' THEN 1 ELSE 0 END) AS quarantine_count,
  SUM(CASE WHEN full_lineage_present THEN 1 ELSE 0 END) AS full_lineage_count
FROM (
  WITH bronze AS (
    SELECT document_id, file_name, parse_status FROM caseops.bronze.parsed_documents
  ),
  silver AS (
    SELECT document_id FROM caseops.silver.extracted_records_smoke
  ),
  gold AS (
    SELECT document_id, routing_label, export_ready FROM caseops.gold.ai_ready_assets_smoke
  )
  SELECT
    b.document_id,
    b.file_name,
    b.parse_status,
    s.document_id IS NOT NULL AS has_silver_record,
    g.routing_label,
    g.export_ready,
    CASE
      WHEN b.document_id IS NOT NULL
       AND s.document_id IS NOT NULL
       AND g.document_id IS NOT NULL
      THEN true
      ELSE false
    END AS full_lineage_present
  FROM bronze b
  LEFT JOIN silver s ON b.document_id = s.document_id
  LEFT JOIN gold g ON b.document_id = g.document_id
) x;
