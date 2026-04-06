# ARCHITECTURE.md — Technical Design Source of Truth

> This file defines the technical architecture of the Databricks CaseOps Lakehouse pipeline.
> When in conflict with any other document, this file governs technical design decisions.

---

## System Purpose

The Databricks CaseOps Lakehouse pipeline is responsible for the **structured transformation of unstructured enterprise documents** into governed, schema-validated, evaluation-ready records. It operates entirely within Databricks and Unity Catalog, and produces outputs consumable by downstream Bedrock retrieval and agent systems.

The pipeline does not own retrieval, generation, or agent orchestration. Its contract is: **raw document in, structured AI-ready record out**.

---

## Target Inputs

| Input Type | Format | Source |
|---|---|---|
| Regulatory notices | PDF, DOCX | Uploaded to Unity Catalog Volume |
| Security advisories | PDF, TXT | Uploaded to Unity Catalog Volume |
| Incident reports | PDF, DOCX, TXT | Uploaded to Unity Catalog Volume |
| SOPs | DOCX, PDF | Uploaded to Unity Catalog Volume |
| Quality / audit records | PDF, DOCX | Uploaded to Unity Catalog Volume |
| Technical case records | TXT, DOCX | Uploaded to Unity Catalog Volume |

Accepted formats are PDF, DOCX, and TXT. HTML-sourced content (e.g., web-scraped advisories) must be normalized to `.txt` before being placed in the Volume; HTML normalization is a pre-ingest step outside the pipeline boundary.

All inputs are treated as **immutable once ingested**. Original files are never modified. Processing state is recorded separately in the Bronze table.

---

## Unity Catalog Governance Layout

```
Unity Catalog
└── catalog: caseops
    ├── schema: raw
    │   └── Volume: documents          ← raw uploaded files (immutable)
    ├── schema: bronze
    │   └── Table: parsed_documents    ← parse output + source metadata
    ├── schema: silver
    │   └── Table: extracted_records   ← structured field extraction
    └── schema: gold
        └── Table: ai_ready_assets     ← classified, routed, export-ready
```

All tables are Delta format. All schemas are planned for implementation in `src/schemas/`. All lineage is captured at the row level via `document_id` and `pipeline_run_id` fields present in every table.

---

## Bronze Layer — Parse and Provenance

### Purpose
Capture the full parse output of each source document along with provenance metadata. Bronze records are append-only and represent a direct transformation of the raw input.

### Processing Steps
1. File is detected in the Unity Catalog Volume
2. Source metadata is extracted: filename, file hash (SHA-256), MIME type, ingestion timestamp, operator-supplied document class hint (optional)
3. `ai_parse_document` is called on the file path
4. Parse output (text content, page structure, parse status) is written to the Bronze Delta table
5. The pipeline MLflow run logs operational metadata: batch size, document count, and the pipeline run ID written to each record. Parse quality metrics (success rate, character yield) are computed by a separate evaluation run against the Bronze snapshot — not inline during pipeline execution.

### Bronze Schema (abbreviated)

| Field | Type | Description |
|---|---|---|
| `document_id` | string (UUID) | Stable identifier assigned at ingest |
| `source_path` | string | Volume path to the original file |
| `file_hash` | string | SHA-256 of original file bytes |
| `file_name` | string | Original filename |
| `mime_type` | string | Detected or declared MIME type |
| `ingested_at` | timestamp | UTC ingestion time |
| `parsed_at` | timestamp | UTC parse completion time |
| `parse_status` | string | `success`, `partial`, `failed` |
| `parsed_text` | string | Full extracted text content |
| `page_count` | int | Number of pages parsed (if applicable) |
| `char_count` | int | Character count of parsed text |
| `pipeline_run_id` | string | MLflow run ID for traceability |

Planned implementation: `src/schemas/bronze_schema.py`

---

## Silver Layer — Structured Field Extraction

### Purpose
Transform parsed text into a validated, schema-conformant record of structured fields using `ai_extract`. Silver records represent the operational intelligence extracted from the document.

### Processing Steps
1. Bronze record is selected for extraction based on `parse_status = 'success'` or `'partial'`
2. A domain-appropriate prompt template is selected based on document class hint or a classification pre-pass
3. `ai_extract` is called with the prompt template and parsed text
4. Extracted fields are validated against the Silver Pydantic schema
5. Validation failures are recorded with a `validation_status` field; records are not dropped
6. The pipeline MLflow run logs operational metadata: batch size, document class processed, and the pipeline run ID written to each record. Extraction quality metrics (field coverage, schema validity rate) are computed by a separate evaluation run against the Silver snapshot — not inline during pipeline execution.

### Silver Schema (abbreviated)

| Field | Type | Description |
|---|---|---|
| `document_id` | string | Foreign key to Bronze `document_id` |
| `extraction_id` | string (UUID) | Unique ID for this extraction pass |
| `extracted_at` | timestamp | UTC extraction time |
| `document_class_hint` | string | Input class hint used for prompt selection |
| `extracted_fields` | map / struct | Domain-specific extracted fields (see domain schemas) |
| `field_coverage_pct` | float | Fraction of expected fields successfully extracted |
| `validation_status` | string | `valid`, `partial`, `invalid` |
| `validation_errors` | array[string] | List of field-level validation failures |
| `extraction_model` | string | Model identifier used by `ai_extract` |
| `pipeline_run_id` | string | MLflow run ID for traceability |

Domain-specific field sets (e.g., `fda_warning_letter_fields`) are planned for implementation in `src/schemas/`. V1 implements the FDA warning letter field set only.

Planned implementation: `src/schemas/silver_schema.py`

---

## Gold Layer — Classification, Routing, and Export

### Purpose
Assign a verified document type label and routing label to each Silver record, and construct the AI-ready export payload for downstream Bedrock consumption.

### Processing Steps
1. Silver record with `validation_status != 'invalid'` is selected for classification
2. `ai_classify` is called with the parsed text and a defined label taxonomy
3. Document type label and routing label are assigned
4. Routing label determines which Bedrock index or agent context the document belongs to
5. An export payload is constructed: a structured JSON object combining classification metadata, extracted fields, and source provenance
6. Gold record is written to the Delta table; export payload is also materialized as a JSON artifact in the Volume

### Gold Schema (abbreviated)

| Field | Type | Description |
|---|---|---|
| `document_id` | string | Foreign key to Bronze `document_id` |
| `gold_record_id` | string (UUID) | Unique ID for this Gold record |
| `classified_at` | timestamp | UTC classification time |
| `document_type_label` | string | Primary classification label |
| `routing_label` | string | Target downstream index or workflow |
| `classification_confidence` | float | Confidence score from `ai_classify` |
| `export_payload` | map / struct | AI-ready structured record for Bedrock |
| `export_ready` | boolean | Whether record meets export quality threshold |
| `pipeline_run_id` | string | MLflow run ID for traceability |

Planned implementation: `src/schemas/gold_schema.py`

---

## Evaluation and Observability Layer

### Design Principle
Pipeline runs and evaluation runs are distinct MLflow runs with separate concerns:

- **Pipeline runs** log operational metadata (batch size, document counts, run ID) and write `pipeline_run_id` to every output record.
- **Evaluation runs** consume Delta table snapshots and compute quality metrics (parse success rate, field coverage, classification confidence, traceability completeness). Evaluation is never performed inline during pipeline execution.

Metrics are recorded at the **run level** (batch summary) and linked at the **record level** via `pipeline_run_id`.

### MLflow Experiment Structure

| Experiment | Stage | Key Metrics |
|---|---|---|
| `caseops/bronze/parse_quality` | Bronze | parse success rate, char yield, parse latency |
| `caseops/silver/extraction_quality` | Silver | field coverage %, schema validity rate, extraction model used |
| `caseops/gold/classification_quality` | Gold | label distribution, confidence distribution, export readiness rate |
| `caseops/pipeline/end_to_end` | All | document drop rate by stage, total pipeline latency |

### Traceability

Every Bronze, Silver, and Gold record carries:
- `document_id` — stable identifier from ingest
- `pipeline_run_id` — MLflow run ID of the batch that produced this record

This enables full lineage reconstruction: given any Gold record, the Silver extraction, Bronze parse, and source file can be retrieved deterministically.

### Evaluation Runs

Evaluation is performed as **explicit MLflow runs**, not inline during pipeline execution. Evaluation scripts in `src/evaluation/` consume Delta table snapshots and emit evaluation metrics to the appropriate experiment. This keeps the pipeline logic clean and allows re-evaluation without re-running the pipeline.

---

## Bedrock Handoff Design

### Contract
The Gold layer produces an `export_payload` per document. This payload is the handoff unit. It contains:

```json
{
  "document_id": "<uuid>",
  "source_file": "<original filename>",
  "document_type": "<label>",
  "routing_label": "<bedrock index or workflow>",
  "extracted_fields": { ... },
  "parsed_text_excerpt": "<first N characters or key section>",
  "provenance": {
    "ingested_at": "<timestamp>",
    "pipeline_run_id": "<mlflow run id>",
    "extraction_model": "<model id>",
    "classification_confidence": 0.92
  }
}
```

### Delivery Mechanism (V1)
In V1, Gold records are exported as JSON files written to a Unity Catalog Volume path: `caseops/gold/exports/<routing_label>/<document_id>.json`. The consuming Bedrock system reads from this path.

In V2, this will be replaced by a live Delta table subscription or a structured API call to the Bedrock ingestion endpoint.

---

## Governance and Traceability Principles

1. **Immutability at source**: raw files in the Volume are never modified or deleted by the pipeline
2. **Append-only Bronze**: Bronze records are append-only; reprocessing generates a new record, not an update
3. **Schema enforcement**: all Silver records must conform to the declared schema; violations are surfaced, not silently dropped
4. **Full lineage via `document_id`**: every record in every layer can be traced back to its source file
5. **MLflow as the audit log**: every pipeline run is a named MLflow run with parameters, metrics, and artifact references
6. **No silent failures**: parse failures, extraction failures, and validation failures are all written as records with a status field, never silently discarded

---

## Future Evolution

| Capability | Current State | Future Direction |
|---|---|---|
| Multi-domain extraction | Single domain per batch | Dynamic prompt routing by detected document class |
| Streaming ingestion | Batch only | Databricks Auto Loader on Volume |
| Human review loop | Not implemented | Disagreement queue surfaced to a review tool |
| Model-based routing | Rule-based V1 | Classification model trained on Gold labels |
| Live Bedrock integration | File export V1 | Delta Sharing or API push |
| Extraction model selection | Default `ai_extract` | Per-class model selection with A/B evaluation |

No future evolution item should be treated as in-scope until explicitly added to `PROJECT_SPEC.md`.
