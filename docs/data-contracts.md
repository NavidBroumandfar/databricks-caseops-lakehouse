# Data Contracts

> Field-level contracts for all layers of the Databricks CaseOps Lakehouse pipeline.
> These contracts define the expected shape, types, and constraints for data moving through the Bronze → Silver → Gold flow.
> Authoritative technical design is in [`ARCHITECTURE.md`](../ARCHITECTURE.md).
> The Gold → Bedrock interface contract is in [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md).

---

## Contract Versioning

All contracts carry a `schema_version` field. Breaking changes increment the major version. Additive changes increment the minor version. Deprecated fields are marked but not removed until the next major version.

Current version: **v0.1.0**

| Version Component | Trigger |
|---|---|
| Patch (0.1.**x**) | Documentation-only; no schema field changes |
| Minor (0.**x**.0) | Additive: new optional fields, new routing labels, new document type labels |
| Major (**x**.0.0) | Breaking: field removal, type changes, renamed required fields |

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

Used by `ai_classify` in the Gold stage. Both label sets are closed-set and versioned. New labels require a minor version increment.

### Document Type Labels

| Label | Description | V1 Pipeline Status |
|---|---|---|
| `fda_warning_letter` | FDA-issued warning letter to a regulated company | **V1 — executable** |
| `cisa_advisory` | CISA-issued cybersecurity advisory or bulletin | Planned V2+ |
| `incident_report` | Internal or regulatory incident report | Planned V2+ |
| `standard_operating_procedure` | Procedural document (SOP, work instruction) | Planned V2+ |
| `quality_audit_record` | Quality management audit or review record | Planned V2+ |
| `technical_case_record` | Technical support or case review document | Planned V2+ |
| `unknown` | Document does not match any defined class | Active — triggers quarantine routing |

### Routing Labels

Routing labels determine which Bedrock CaseOps downstream consumer receives the Gold export. The full routing label → consumer mapping is the Bedrock handoff routing contract — see [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md) § 5.

| Routing Label | Downstream Consumer | V1 Execution Status |
|---|---|---|
| `regulatory_review` | Bedrock regulatory intelligence index | **V1 active** — FDA warning letters only |
| `security_ops` | Bedrock security operations index | Planned V2+ |
| `incident_management` | Bedrock incident management workflow | Planned V2+ |
| `quality_management` | Bedrock quality assurance workflow | Planned V2+ |
| `knowledge_base` | General Bedrock knowledge base index | Planned V2+ |
| `quarantine` | Human review queue — record NOT forwarded to Bedrock | Active (governance path) |

**Routing label semantics**: `quarantine` is always assigned alongside `export_ready = false`. No `quarantine`-labeled record produces an export payload file. All other routing labels require `export_ready = true` and correspond to a defined downstream Bedrock consumer.

---

## 5. Gold: AI-Ready Asset Contract

Written after classification and routing are complete.

> **Two states of this contract are in effect.** The target-state contract below reflects the full intended schema including scalar confidence. The A-3B bootstrap-stage implementation note below documents where the current validated Databricks execution deviates from this target. Both are explicit and honest.
>
> The complete Gold → Bedrock handoff contract — including full `export_payload` field definitions, routing semantics, `export_ready` criteria, and delivery mechanism — is in [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md). This section defines the Gold table schema. The handoff contract document defines the interface boundary.

### Target-State Gold Table Contract

| Field | Type | Required | Constraints | Description |
|---|---|---|---|---|
| `document_id` | string (UUID v4) | Yes | FK to Bronze | Matches source document; stable and immutable |
| `bronze_record_id` | string (UUID v4) | Yes | FK to Bronze record | Matches the parse record that fed this document |
| `extraction_id` | string (UUID v4) | Yes | FK to Silver | Matches the Silver extraction record |
| `gold_record_id` | string (UUID v4) | Yes | Unique | Unique identifier for this Gold record |
| `pipeline_run_id` | string | Yes | MLflow run ID or placeholder | Traceability to the pipeline batch. `bootstrap_sql_v1` in bootstrap path. |
| `classified_at` | timestamp | Yes | UTC | Time classification completed |
| `document_type_label` | string | Yes | From defined taxonomy | Primary classification label |
| `routing_label` | string | Yes | From defined taxonomy | Target downstream consumer; never null |
| `classification_confidence` | float | **Target: Yes; Bootstrap: Null** | 0.0 – 1.0 | `ai_classify` confidence score. **Null in the A-3B bootstrap path** — see note below. |
| `classification_model` | string | Yes | | Model identifier used for classification |
| `export_payload` | struct | Yes (if export_ready) | See § 5.2 | Full AI-ready payload for Bedrock consumption |
| `export_ready` | boolean | Yes | | True if record satisfies all export quality criteria |
| `export_path` | string | If export_ready | Volume path | Path to the materialized JSON export file |
| `schema_version` | string | Yes | Semantic version | Contract version this record was written against |

### A-3B Bootstrap-Stage Implementation Note

The validated A-3B Databricks bootstrap implementation deviates from the target-state contract in two explicitly documented ways:

**`classification_confidence` is NULL:**
The `ai_classify` SQL AI Function response variant at the A-3B bootstrap stage does not expose a scalar confidence score via `try_variant_get`. `classification_confidence` is stored as `CAST(NULL AS DOUBLE)`. This is a known, documented bootstrap-path implementation detail, not a hidden defect. Phase A-4.1 runtime inspection confirmed the absence of a scalar confidence key in the `ai_classify` response variant for this path. Evaluation logic in `src/evaluation/eval_gold.py` handles null confidence explicitly — see `docs/evaluation-plan.md` § A-3B Bootstrap Path.

**`pipeline_run_id = 'bootstrap_sql_v1'`:**
The A-3B bootstrap uses a static placeholder string rather than a real MLflow run ID. Lineage via `document_id` is intact. Traceability evaluation surfaces these records explicitly via `placeholder_run_id_count` rather than treating them as orphans.

**Export quality threshold in the bootstrap path:**
The export-ready determination in the A-3B bootstrap SQL uses rule-based routing (`document_type_label = 'fda_warning_letter'` → `regulatory_review`), not the confidence threshold defined in § 7. The confidence-based threshold applies to the target-state pipeline only.

### 5.2 Export Payload Structure

The `export_payload` is the Bedrock handoff unit. It is embedded in the Gold table record and also materialized as a standalone JSON file at:
`/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json`

The authoritative field-level definition is in [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md) § 4. The canonical structure is:

```json
{
  "document_id": "<uuid>",
  "source_file": "<original filename>",
  "document_type": "<classification label>",
  "routing_label": "<routing label>",
  "extracted_fields": { "...domain-specific fields per document_type..." },
  "parsed_text_excerpt": "<first 2000 characters of parsed text>",
  "provenance": {
    "ingested_at": "<UTC ISO 8601 timestamp>",
    "pipeline_run_id": "<mlflow run id or 'bootstrap_sql_v1'>",
    "extraction_model": "<model identifier>",
    "classification_model": "<model identifier>",
    "classification_confidence": 0.92,
    "schema_version": "v0.1.0"
  }
}
```

**Required export payload fields**: `document_id`, `source_file`, `document_type`, `routing_label`, `extracted_fields`, `parsed_text_excerpt`, `provenance` (all sub-fields required except `classification_confidence` which is null in bootstrap path).

**Optional export payload fields**: `page_count`, `char_count`, `extraction_prompt_id`.

Records with `export_ready = false` do not produce an export payload file. No `quarantine`-labeled record is materialized as an export file.

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

> The complete `export_ready` and `quarantine` semantics, including the distinction between target-state and bootstrap-path behavior, are defined in [`docs/bedrock-handoff-contract.md`](./bedrock-handoff-contract.md) § 6. This section mirrors those requirements for in-context reference.

### Target-State Export Quality Threshold

For a Gold record to be marked `export_ready = true` in the target-state pipeline, it must satisfy **all** of the following:

| Condition | Layer |
|---|---|
| `document_type_label != 'unknown'` | Gold |
| `routing_label != 'quarantine'` | Gold |
| `classification_confidence >= 0.7` | Gold |
| `validation_status` ∈ `{'valid', 'partial'}` on the corresponding Silver record | Silver |
| `field_coverage_pct >= 0.5` on the corresponding Silver record | Silver |
| `export_payload` is structurally valid (all required payload fields present) | Gold |

Records failing any criterion are written to Gold with `export_ready = false` and `routing_label = 'quarantine'`. They are not materialized as export files.

### Bootstrap-Stage Export Quality (A-3B)

The A-3B bootstrap SQL does not apply the `classification_confidence >= 0.7` threshold because `classification_confidence` is NULL in the bootstrap implementation (see § 5 above). Export readiness in the bootstrap path is determined by rule-based routing only:

- Records classified as `fda_warning_letter` → `routing_label = 'regulatory_review'` → `export_ready = true`
- All other classification results → `routing_label = 'quarantine'` → `export_ready = false`

This is a documented implementation constraint of the bootstrap path, not the intended long-term behavior. The confidence-based threshold is the target-state standard and will apply when `ai_classify` confidence extraction is resolved.

### Provenance Requirements for Export-Ready Records

Every export-ready record's `export_payload.provenance` must carry:

| Field | Requirement |
|---|---|
| `ingested_at` | UTC ISO 8601 timestamp; must be non-null |
| `pipeline_run_id` | MLflow run ID (target-state) or `bootstrap_sql_v1` (bootstrap path); must be non-null |
| `extraction_model` | Non-null model identifier |
| `classification_model` | Non-null model identifier |
| `classification_confidence` | Float 0.0–1.0 (target-state); null acceptable in bootstrap path with explicit documentation |
| `schema_version` | Non-null semantic version string |

Missing provenance fields on an `export_ready = true` record is a pipeline defect, not a data quality issue, and must be investigated and resolved before the pipeline run is considered complete.
