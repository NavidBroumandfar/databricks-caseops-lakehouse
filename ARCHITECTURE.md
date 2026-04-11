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

### Validated Bootstrap State (A-3B)

The above layout was validated in a personal Databricks workspace during Phase A-3B. The following objects were confirmed operational:

- Catalog `caseops` with schemas `raw`, `bronze`, `silver`, `gold`
- Managed volume `caseops.raw.documents` with subdirectory `fda_warning_letters/`
- Tables `parsed_documents` (Bronze), `extracted_records_smoke` (Silver), `ai_ready_assets_smoke` (Gold) created and populated via SQL AI Functions
- Full `document_id`-based lineage confirmed across all three layers for 4 documents

This bootstrap used a personal Free Edition workspace with a serverless SQL warehouse. It does not represent enterprise deployment. See [`docs/databricks-bootstrap.md`](./docs/databricks-bootstrap.md) for full details.

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

Implementation: `src/schemas/bronze_schema.py`

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

Domain-specific field sets (e.g., `fda_warning_letter_fields`) are implemented in `src/schemas/`. V1 implements the FDA warning letter field set only.

Implementation: `src/schemas/silver_schema.py`

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
| `classification_confidence` | float or null | Confidence score from `ai_classify`; null in bootstrap path — see A-3B notes and B-1 |
| `export_payload` | map / struct | AI-ready structured record for Bedrock |
| `export_ready` | boolean | Whether record meets export quality threshold |
| `pipeline_run_id` | string | MLflow run ID for traceability |

Implementation: `src/schemas/gold_schema.py`

### Gold Bootstrap Implementation Notes (A-3B)

The validated A-3B bootstrap SQL implements rule-based routing: the `routing_label` is derived directly from the `ai_classify` response label rather than from a confidence threshold. Records classified as `fda_warning_letter` are routed to `regulatory_review`; all other results are routed to `quarantine`.

`classification_confidence` is stored as `CAST(NULL AS DOUBLE)` in the A-3B bootstrap implementation. The `ai_classify` SQL AI Function response variant at this bootstrap stage does not expose a scalar confidence score via `try_variant_get`. This is an explicitly documented implementation detail, not a hidden gap. Phase A-4.1 runtime inspection confirmed that scalar confidence is not available from this bootstrap SQL path; see A-4.1 Runtime Inspection Findings below.

The export quality threshold defined in `docs/data-contracts.md` (requiring `classification_confidence >= 0.7`) applies to the target-state pipeline, not to this bootstrap pass.

---

## Evaluation and Observability Layer

### Design Principle
Pipeline runs and evaluation runs are distinct MLflow runs with separate concerns:

- **Pipeline runs** log operational metadata (batch size, document counts, run ID) and write `pipeline_run_id` to every output record.
- **Evaluation runs** consume artifact snapshots and compute quality metrics (parse success rate, field coverage, classification confidence, traceability completeness). Evaluation is never performed inline during pipeline execution.

Metrics are recorded at the **run level** (batch summary) and linked at the **record level** via `pipeline_run_id`.

### A-4 Implementation Status

Phase A-4 is implemented. The following evaluation assets are now first-class repo components:

| Script | Purpose | Status |
|---|---|---|
| `src/evaluation/eval_bronze.py` | Bronze parse quality | ✅ Implemented |
| `src/evaluation/eval_silver.py` | Silver extraction quality | ✅ Implemented |
| `src/evaluation/eval_gold.py` | Gold classification quality (null-confidence safe) | ✅ Updated A-4 |
| `src/evaluation/eval_traceability.py` | Cross-layer traceability | ✅ Implemented A-4 |
| `src/evaluation/run_evaluation.py` | Full-pipeline orchestrator | ✅ Implemented A-4 |
| `src/evaluation/report_models.py` | Structured report dataclasses | ✅ Implemented A-4 |
| `src/evaluation/report_writer.py` | JSON + text report writer | ✅ Implemented A-4 |

All evaluators are locally executable. MLflow logging is optional and does not require a live Databricks workspace.

### MLflow Experiment Structure

| Experiment | Stage | Key Metrics |
|---|---|---|
| `caseops/bronze/parse_quality` | Bronze | parse success rate, char yield, parse latency |
| `caseops/silver/extraction_quality` | Silver | field coverage %, schema validity rate, extraction model used |
| `caseops/gold/classification_quality` | Gold | label distribution, confidence distribution, export readiness rate |
| `caseops/pipeline/traceability` | All | link rates, orphan counts, placeholder run ID count |
| `caseops/pipeline/end_to_end` | All | document drop rate by stage, cross-layer summary |

### Traceability

Every Bronze, Silver, and Gold record carries:
- `document_id` — stable identifier from ingest
- `pipeline_run_id` — MLflow run ID of the batch that produced this record (or a placeholder for bootstrap-origin records)

This enables full lineage reconstruction: given any Gold record, the Silver extraction, Bronze parse, and source file can be retrieved deterministically.

### Evaluation Runs

Evaluation is performed as **explicit, rerunnable passes**, not inline during pipeline execution. Evaluation scripts in `src/evaluation/` consume local JSON artifact snapshots and emit evaluation metrics. MLflow logging is available when configured. This keeps the pipeline logic clean and allows re-evaluation without re-running the pipeline.

### A-3B Bootstrap Evaluation Context

The validated A-3B Databricks bootstrap execution is tracked as an explicit context in the evaluation layer:

- **`classification_confidence` is NULL in bootstrap Gold records.** The evaluator surfaces this via `confidence_null_rate` and `observations` rather than hiding it. Confidence-based thresholds are not fired when confidence is null.
- **`pipeline_run_id = 'bootstrap_sql_v1'` in bootstrap records.** This is a placeholder, not a real MLflow run ID. The traceability evaluator reports `placeholder_run_id_count` explicitly. Document lineage via `document_id` remains intact.
- **Evaluation still produces meaningful signals from bootstrap records:** classification success rate, export-ready rate, quarantine rate, and cross-layer link completeness are all computable even when confidence is null.

### A-4.1 Runtime Inspection Findings

The validated A-3B bootstrap path was directly runtime-inspected in the personal Databricks workspace during Phase A-4.1:

- The `ai_classify` output variant observed at runtime contained only `error_message` and `response`. No scalar confidence or score key was present at any tested extraction path.
- Gold routing and quarantine behavior were confirmed against real workspace tables: 3 records classified as `fda_warning_letter` routed to `regulatory_review` (`export_ready = true`); 1 record classified as `unknown` routed to `quarantine` (`export_ready = false`).
- This confirms the null-confidence handling in `eval_gold.py` is correct for this bootstrap path.
- The distinction between **target-state contract aspirations** (which include a `classification_confidence` field) and **current validated bootstrap behavior** (where that field is null) is intentional and explicitly tracked.

This observation is specific to the validated personal workspace bootstrap SQL implementation. It does not assert that scalar confidence is unavailable in all Databricks `ai_classify` contexts.

See `docs/databricks-bootstrap.md` § A-4.1 Runtime Validation Findings for full detail.

See `docs/evaluation-plan.md` § A-3B Bootstrap Path for full details on evaluator handling.

---

## Bedrock Handoff Design

### Boundary

This repo's responsibility ends at the Gold export. Everything upstream of the handoff — ingestion, parsing, extraction, schema validation, classification, routing, traceability, and evaluation — is owned here. Everything downstream — retrieval index population, vector search, agentic reasoning, escalation, and case-support workflow orchestration — is owned by Bedrock CaseOps.

**The Gold `export_payload` is the interface contract between the two systems.** This repo prepares it; Bedrock CaseOps consumes it.

The complete contract is defined in [`docs/bedrock-handoff-contract.md`](./docs/bedrock-handoff-contract.md). That document is the single authoritative artifact for the Gold → Bedrock handoff and supersedes any partial description of the interface elsewhere. This section provides the architectural framing; the contract document provides the operative field-level detail.

### B-0 Phase Context

The Bedrock Handoff Design section reflects the output of **Phase B-0 — Bedrock Handoff Contract Preparation**. B-0 is a contract-hardening phase. Its purpose is to establish the Gold → Bedrock interface boundary clearly enough that live integration work (Phase B proper) can begin without ambiguity.

B-0 does **not** deliver:
- Live AWS or Bedrock integration
- S3 plumbing or Bedrock SDK code
- Vector index configuration or retrieval logic
- Event-driven delivery mechanisms

The V1 delivery mechanism remains file-based (structured JSON export to a Unity Catalog Volume path). Live integration is Phase B proper.

### Contract

The Gold layer produces an `export_payload` per export-ready document. This payload is the handoff unit. The `export_payload` is both embedded in the Gold Delta table record and materialized as a standalone JSON file at a deterministic Volume path.

**Required payload fields:**

| Field | Type | Description |
|---|---|---|
| `document_id` | string (UUID v4) | Stable cross-layer document identifier |
| `source_file` | string | Original filename as uploaded |
| `document_type` | string | Classification label from the closed taxonomy |
| `routing_label` | string | Target downstream Bedrock consumer or workflow |
| `extracted_fields` | object | Domain-specific structured fields (shape varies by `document_type`) |
| `parsed_text_excerpt` | string | First 2000 characters of parsed text |
| `provenance` | object | Traceability metadata including `pipeline_run_id`, model identifiers, `classification_confidence`, and `schema_version` |

Full field definitions, optional fields, the complete provenance sub-object, and a canonical payload example are in `docs/bedrock-handoff-contract.md` § 4.

**Bootstrap-path note**: In the A-3B bootstrap implementation, `provenance.classification_confidence` is `null` and `provenance.pipeline_run_id` is the placeholder `"bootstrap_sql_v1"`. Both are explicitly documented limitations — see `docs/bedrock-handoff-contract.md` § 9 and `docs/data-contracts.md` § 5.

### Routing Label → Bedrock Consumer Mapping

The `routing_label` field determines which downstream Bedrock system is the intended consumer.

| Routing Label | Bedrock Consumer | V1 Status |
|---|---|---|
| `regulatory_review` | Bedrock regulatory intelligence index | **V1 active** — FDA warning letters only |
| `security_ops` | Bedrock security operations index | Planned V2+ |
| `incident_management` | Bedrock incident management workflow | Planned V2+ |
| `quality_management` | Bedrock quality assurance workflow | Planned V2+ |
| `knowledge_base` | General Bedrock knowledge base index | Planned V2+ |
| `quarantine` | Human review queue — not forwarded | Active (governance path) |

Full routing label semantics are in `docs/bedrock-handoff-contract.md` § 5.

### Delivery Mechanism (V1)

In V1, export-ready Gold records are materialized as individual JSON files at:

```
/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json
```

One file per `export_ready = true` record. File content is the `export_payload` object exactly as defined in the contract. Records with `export_ready = false` (routed to `quarantine`) produce no export file.

In V2, this will evolve to Delta Sharing, a structured API push, or an event-driven notification mechanism. No V2 delivery work is in scope until explicitly added to `PROJECT_SPEC.md`.

### Phase B Handoff Layer — Implementation Status

Phases B-1 through B-6 converted the B-0 contract from documentation into a fully implemented, tested, and locally-safe upstream handoff preparation layer. The following modules are complete:

| Module | Path | Role |
|---|---|---|
| Contract validator | `src/schemas/bedrock_contract.py` | Enforces B-0 §4 required/optional field rules for every export payload (B-1) |
| Export/handoff service | `src/pipelines/export_handoff.py` | Validates contract, writes export artifact, returns `ExportResult`; clean service boundary (B-3) |
| Handoff reporting | `src/pipelines/handoff_report.py` | Derives outcome categories and reason codes; builds `HandoffBatchReport` per pipeline run (B-4) |
| Batch bundle packaging | `src/pipelines/handoff_bundle.py` | Packages per-record artifact references and B-4 report into a single `HandoffBatchManifest` (B-5) |
| Bundle integrity validation | `src/pipelines/handoff_bundle_validation.py` | 24 explicit checks: structural, count consistency, reference integrity, identifier uniqueness, path existence (B-6) |

**Module boundary:**

```
classify_gold.py              → assembles GoldRecord → calls execute_export → derives outcome → writes Gold artifact → builds bundle
export_handoff.py             → validates contract → writes export artifact → returns ExportResult
handoff_report.py             → derives outcome categories → aggregates batch report → writes report artifacts
handoff_bundle.py             → packages batch into manifest/review bundle → writes bundle artifacts (JSON + text)
handoff_bundle_validation.py  → validates the bundle is internally consistent and trustworthy
```

**What this layer does not include:** No live Bedrock/AWS integration, no Bedrock SDK, no S3 wiring, no vector index, no agent workflows. The repo remains the upstream-only governed document intelligence and handoff preparation layer. Live integration is a future phase beyond B-6.

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
