# Data Contracts

> Field-level contracts for all layers of the Databricks CaseOps Lakehouse pipeline.
> These contracts define the expected shape, types, and constraints for data moving through the Bronze → Silver → Gold flow.
> Authoritative technical design is in [`ARCHITECTURE.md`](../ARCHITECTURE.md).

---

## Contract Versioning

All contracts carry a `schema_version` field. Breaking changes increment the major version. Additive changes increment the minor version. Deprecated fields are marked but not removed until the next major version.

Current version: **v0.1.0** (foundation draft, subject to revision during A-1/A-2 implementation)

---

## 1. Source Document Metadata

Captured at ingest before any parsing occurs. This is the provenance anchor for all downstream records.

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `document_id` | string (UUID v4) | Yes | Unique, immutable | Assigned at ingest; stable across all layers |
| `source_path` | string | Yes | Non-empty | Full path within Unity Catalog Volume |
| `file_name` | string | Yes | Non-empty | Original filename as uploaded |
| `file_extension` | string | Yes | `.pdf`, `.docx`, `.txt` | Lowercased |
| `file_size_bytes` | long | Yes | > 0 | File size at ingestion time |
| `file_hash` | string | Yes | SHA-256, 64 hex chars | Hash of raw file bytes; used for deduplication |
| `mime_type` | string | Yes | RFC 2045 format | Detected MIME type |
| `ingested_at` | timestamp | Yes | UTC | Time the file was registered in the pipeline |
| `ingested_by` | string | No | | Principal or job identity that triggered ingest |
| `document_class_hint` | string | No | From defined taxonomy | Operator-supplied class hint; not validated at ingest |
| `source_system` | string | No | | Originating system label (e.g., `fda_portal`, `cisa_feed`) |
| `schema_version` | string | Yes | Semantic version | Contract version this record was written against |

---

## 2. Bronze: Parse Output Contract

Written after `ai_parse_document` completes. One record per document per pipeline run.

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `document_id` | string (UUID v4) | Yes | FK to source metadata | Matches source document |
| `bronze_record_id` | string (UUID v4) | Yes | Unique | Unique identifier for this parse record |
| `pipeline_run_id` | string | Yes | MLflow run ID | Traceability to the MLflow run that produced this record |
| `parsed_at` | timestamp | Yes | UTC | Time `ai_parse_document` completed |
| `parse_status` | string | Yes | `success`, `partial`, `failed` | Outcome of the parse attempt |
| `parse_failure_reason` | string | If failed | | Human-readable failure description |
| `parsed_text` | string | If success/partial | Non-empty if success | Full extracted text content |
| `page_count` | int | No | >= 0 | Number of pages parsed; null for non-paginated formats |
| `char_count` | int | If success/partial | >= 0 | Character count of `parsed_text` |
| `parse_model` | string | Yes | | Model or function version used (e.g., `ai_parse_document/v1`) |
| `schema_version` | string | Yes | Semantic version | Contract version this record was written against |

**Constraints**:
- `parse_status = 'failed'` records must have `parse_failure_reason` and `parsed_text = null`
- `parse_status = 'partial'` records have non-null `parsed_text` but `char_count` may be below expected threshold
- Bronze records are append-only; reprocessing a document creates a new `bronze_record_id`

---

## 3. Silver: Extraction Schema Contract

Written after `ai_extract` completes and Pydantic validation runs. One record per Bronze record that had `parse_status != 'failed'`.

### Core Fields

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `document_id` | string (UUID v4) | Yes | FK to Bronze | Matches source document |
| `bronze_record_id` | string (UUID v4) | Yes | FK to Bronze record | Matches the parse record used for extraction |
| `extraction_id` | string (UUID v4) | Yes | Unique | Unique identifier for this extraction pass |
| `pipeline_run_id` | string | Yes | MLflow run ID | Traceability to the MLflow run |
| `extracted_at` | timestamp | Yes | UTC | Time extraction completed |
| `document_class_hint` | string | No | | Class hint used to select the extraction prompt |
| `extraction_prompt_id` | string | Yes | | Identifier for the prompt template used |
| `extraction_model` | string | Yes | | Model identifier used by `ai_extract` |
| `extracted_fields` | struct | If valid/partial | | Domain-specific extracted fields (see domain schemas below) |
| `field_coverage_pct` | float | Yes | 0.0 – 1.0 | Fraction of expected fields that have non-null values |
| `validation_status` | string | Yes | `valid`, `partial`, `invalid` | Outcome of Pydantic validation |
| `validation_errors` | array[string] | If partial/invalid | | Field-level validation error messages |
| `schema_version` | string | Yes | Semantic version | Contract version |

### Domain-Specific Extracted Fields

V1 implements the FDA warning letter field set only. CISA advisory and incident report schemas are drafted below as planned extensions (V2+) to establish contract structure early. They are not part of the V1 executable pipeline.

#### FDA Warning Letter Fields — V1

| Field | Type | Required |
|---|---|---|
| `issuing_office` | string | Yes |
| `recipient_company` | string | Yes |
| `recipient_name` | string | No |
| `issue_date` | date | Yes |
| `violation_type` | array[string] | Yes |
| `cited_regulations` | array[string] | No |
| `corrective_action_requested` | boolean | Yes |
| `response_deadline_days` | int | No |
| `product_involved` | string | No |
| `summary` | string | No |

#### CISA Advisory Fields — Planned (V2+)

| Field | Type | Required |
|---|---|---|
| `advisory_id` | string | Yes |
| `title` | string | Yes |
| `published_date` | date | Yes |
| `severity_level` | string | Yes (`Critical`, `High`, `Medium`, `Low`) |
| `affected_products` | array[string] | No |
| `cve_ids` | array[string] | No |
| `remediation_available` | boolean | Yes |
| `remediation_summary` | string | No |
| `summary` | string | No |

#### Incident Report Fields — Planned (V2+)

| Field | Type | Required |
|---|---|---|
| `incident_id` | string | No |
| `incident_date` | date | Yes |
| `incident_type` | string | Yes |
| `severity` | string | Yes |
| `affected_systems` | array[string] | No |
| `root_cause` | string | No |
| `resolution_summary` | string | No |
| `status` | string | Yes (`open`, `resolved`, `under_review`) |
| `reported_by` | string | No |

---

## 4. Classification Labels

Used by `ai_classify` in the Gold stage. Labels are closed-set and versioned.

### Document Type Labels

| Label | Description |
|---|---|
| `fda_warning_letter` | FDA-issued warning letter to a regulated company |
| `cisa_advisory` | CISA-issued cybersecurity advisory or bulletin |
| `incident_report` | Internal or regulatory incident report |
| `standard_operating_procedure` | Procedural document (SOP, work instruction) |
| `quality_audit_record` | Quality management audit or review record |
| `technical_case_record` | Technical support or case review document |
| `unknown` | Document does not match any defined class |

### Routing Labels

Routing labels determine which Bedrock downstream system receives the Gold export.

| Routing Label | Downstream Target |
|---|---|
| `regulatory_review` | Bedrock regulatory intelligence index |
| `security_ops` | Bedrock security operations index |
| `incident_management` | Bedrock incident management workflow |
| `quality_management` | Bedrock quality assurance workflow |
| `knowledge_base` | General Bedrock knowledge base index |
| `quarantine` | Held for human review; not forwarded |

---

## 5. Gold: AI-Ready Asset Contract

Written after classification and routing are complete.

> **Two states of this contract are in effect.** The target-state contract below reflects the full intended schema including scalar confidence. The A-3B bootstrap-stage implementation note below documents where the current validated Databricks execution deviates from this target. Both are explicit and honest.

### Target-State Contract

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `document_id` | string (UUID v4) | Yes | FK to Bronze | Matches source document |
| `bronze_record_id` | string (UUID v4) | Yes | FK to Bronze record | Matches the parse record that fed this document through the pipeline |
| `extraction_id` | string (UUID v4) | Yes | FK to Silver | Matches the extraction record |
| `gold_record_id` | string (UUID v4) | Yes | Unique | Unique identifier for this Gold record |
| `pipeline_run_id` | string | Yes | MLflow run ID | Traceability to the pipeline batch that produced this record |
| `classified_at` | timestamp | Yes | UTC | Time classification completed |
| `document_type_label` | string | Yes | From defined taxonomy | Primary classification label |
| `routing_label` | string | Yes | From defined taxonomy | Target downstream system |
| `classification_confidence` | float | **Target: Yes** | 0.0 – 1.0 | `ai_classify` confidence score. **Nullable in bootstrap stage — see note below.** |
| `classification_model` | string | Yes | | Model used for classification |
| `export_payload` | struct | Yes | | Full AI-ready payload (see below) |
| `export_ready` | boolean | Yes | | True if record meets export quality threshold |
| `export_path` | string | If export_ready | Volume path | Path to the materialized JSON export file |
| `schema_version` | string | Yes | Semantic version | Contract version |

### A-3B Bootstrap-Stage Implementation Note

The validated A-3B Databricks bootstrap implementation deviates from the target-state contract in two documented ways:

**`classification_confidence` is NULL:**
The `ai_classify` SQL AI Function response variant at the A-3B bootstrap stage does not expose a scalar confidence score via `try_variant_get`. `classification_confidence` is stored as `CAST(NULL AS DOUBLE)`. This is a known implementation gap in the bootstrap SQL path, not a hidden defect. Evaluation logic in `src/evaluation/eval_gold.py` handles null confidence explicitly (see `docs/evaluation-plan.md` § A-3B Bootstrap Path for details).

**`pipeline_run_id = 'bootstrap_sql_v1'`:**
The A-3B bootstrap uses a static placeholder string rather than a real MLflow run ID. Lineage via `document_id` is intact. Traceability evaluation surfaces these records explicitly via `placeholder_run_id_count` rather than treating them as orphans.

**Export quality threshold in the bootstrap path:**
The export-ready determination in the A-3B bootstrap SQL uses rule-based routing (`document_type_label = 'fda_warning_letter'` → `regulatory_review`), not the confidence threshold defined below. The confidence-based threshold is the target-state standard.

### Export Payload Structure

The `export_payload` is the Bedrock handoff unit. It is also materialized as a standalone JSON file.

```json
{
  "document_id": "<uuid>",
  "source_file": "<original filename>",
  "document_type": "<label>",
  "routing_label": "<routing label>",
  "extracted_fields": { "...domain-specific fields..." },
  "parsed_text_excerpt": "<first 2000 characters of parsed text>",
  "provenance": {
    "ingested_at": "<UTC timestamp>",
    "pipeline_run_id": "<mlflow run id>",
    "extraction_model": "<model id>",
    "classification_model": "<model id>",
    "classification_confidence": 0.92,
    "schema_version": "v0.1.0"
  }
}
```

---

## 6. Traceability Fields Summary

Every record at every layer carries the following traceability fields:

| Field | Layers Present | Purpose |
|---|---|---|
| `document_id` | Bronze, Silver, Gold | Stable cross-layer document identifier |
| `pipeline_run_id` | Bronze, Silver, Gold | MLflow run that produced this record |
| `schema_version` | Bronze, Silver, Gold | Contract version for schema drift detection |
| `bronze_record_id` | Silver, Gold | FK to Bronze parse record |
| `extraction_id` | Gold | FK to Silver extraction record |

Given any `document_id`, the full Bronze → Silver → Gold lineage is reconstructable via direct joins on these foreign keys.

---

## 7. Downstream AI-Ready Asset Requirements

### Target-State Export Quality Threshold

For a Gold record to be marked `export_ready = true` in the target-state pipeline, it must satisfy all of the following:

- `document_type_label != 'unknown'`
- `routing_label != 'quarantine'`
- `classification_confidence >= 0.7`
- `validation_status` on the corresponding Silver record is `'valid'` or `'partial'`
- `field_coverage_pct` on the corresponding Silver record is `>= 0.5`
- `export_payload` is structurally valid (all required payload fields present)

Records that fail these criteria are written to Gold with `export_ready = false` and `routing_label = 'quarantine'`.

### Bootstrap-Stage Export Quality (A-3B)

The A-3B bootstrap SQL does not apply the `classification_confidence >= 0.7` threshold because `classification_confidence` is NULL in the bootstrap implementation (see § 5 above). Export readiness in the bootstrap path is determined by rule-based routing:

- Records classified as `fda_warning_letter` → `routing_label = 'regulatory_review'` → `export_ready = true`
- All other classification results → `routing_label = 'quarantine'` → `export_ready = false`

This is a documented implementation constraint of the bootstrap stage, not the intended long-term behavior. The confidence-based threshold is the target for when `ai_classify` confidence extraction is resolved.
