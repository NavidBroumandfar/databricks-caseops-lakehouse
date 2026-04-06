# Roadmap

> Practical delivery plan for the Databricks CaseOps Lakehouse pipeline.
> Authoritative scope is in [`PROJECT_SPEC.md`](../PROJECT_SPEC.md).
> This file provides phase-level detail and milestone tracking.

---

## Phase Summary

| Phase | Name | Status | Goal |
|---|---|---|---|
| A-0 | Repo Foundation | ✅ Complete | Docs, scaffold, project identity |
| A-1 | Bronze: Ingest and Parse | 🔲 Next | Raw file → parsed text in Bronze Delta table |
| A-2 | Silver: Extraction and Validation | 🔲 Planned | Parsed text → structured fields in Silver table |
| A-3 | Gold: Classification and Routing | 🔲 Planned | Structured fields → classified, routed Gold records |
| A-4 | Evaluation and Observability | 🔲 Planned | MLflow evaluation across all stages |
| B | Bedrock Handoff Integration | 🔲 Future | Gold export → live Bedrock consumption |

---

## Phase A-0 — Repo Foundation

**Status**: Complete

**Goal**: Establish project identity, documentation, and directory structure. No pipeline code.

**Deliverables**:
- `README.md` — public-facing overview
- `PROJECT_SPEC.md` — scope and roadmap source of truth
- `ARCHITECTURE.md` — technical design source of truth
- `docs/CURSOR_CONTEXT.md` — agent orientation guide
- `docs/roadmap.md` — this file
- `docs/data-contracts.md` — layer schema contracts
- `docs/evaluation-plan.md` — evaluation quality plan
- `.gitignore` — security-aware exclusions
- Directory scaffold: `src/`, `docs/`, `notebooks/`, `examples/`

---

## Phase A-1 — Bronze: Ingest and Parse

**Status**: Not started

**Goal**: Build the ingestion and parsing pipeline. A document dropped into the Unity Catalog Volume should produce a Bronze Delta record with parsed text and provenance metadata.

**Deliverables**:

| Artifact | Path | Description |
|---|---|---|
| Unity Catalog config | `src/pipelines/catalog_config.yaml` | Catalog, schema, Volume definitions |
| Ingestion script | `src/pipelines/ingest.py` | File detection, hash, metadata extraction |
| Bronze writer | `src/pipelines/bronze_writer.py` | `ai_parse_document` call and Bronze table write |
| Bronze schema | `src/schemas/bronze_schema.py` | Pydantic model for Bronze records |
| Parse eval notebook | `notebooks/eval_bronze_parse.ipynb` | MLflow run for parse quality metrics |

**Completion criteria**:
- A real PDF can be dropped into the Volume and produces a valid Bronze record
- Parse failures are captured as records with `parse_status = 'failed'`, not silent drops
- MLflow run exists with parse success rate and character yield metrics
- Bronze schema is validated against at least one real document

---

## Phase A-2 — Silver: Extraction and Validation

**Status**: Not started

**Goal**: Extract structured fields from Bronze parsed text using `ai_extract`. Validate all extracted records against the Silver schema. Surface validation failures as records, not errors.

**Deliverables**:

| Artifact | Path | Description |
|---|---|---|
| Extraction pipeline | `src/pipelines/silver_extractor.py` | `ai_extract` call with prompt selection |
| Prompt templates | `docs/prompts/` (gitignored) | Per-domain extraction prompts |
| Silver schema | `src/schemas/silver_schema.py` | Pydantic models per document class |
| Validation logic | `src/utils/schema_validator.py` | Shared validation helpers |
| Extraction eval notebook | `notebooks/eval_silver_extraction.ipynb` | MLflow run for extraction quality |

**Completion criteria**:
- At least one document class (e.g., FDA warning letter) has a defined extraction schema
- Extracted records pass Pydantic validation at ≥ 80% field coverage on test documents
- Validation failures are recorded with `validation_status = 'partial'` or `'invalid'`
- MLflow run reports field coverage %, schema validity rate, and model used

---

## Phase A-3 — Gold: Classification and Routing

**Status**: Not started

**Goal**: Classify Silver records into document type labels and routing labels. Construct the export payload for downstream Bedrock consumption.

**Deliverables**:

| Artifact | Path | Description |
|---|---|---|
| Classification pipeline | `src/pipelines/gold_classifier.py` | `ai_classify` call with label taxonomy |
| Label taxonomy | `src/schemas/label_taxonomy.yaml` | Defined document type and routing labels |
| Gold schema | `src/schemas/gold_schema.py` | Pydantic model for Gold records |
| Export formatter | `src/utils/export_formatter.py` | Builds the Bedrock handoff payload |
| Classification eval notebook | `notebooks/eval_gold_classification.ipynb` | MLflow run for classification quality |

**Completion criteria**:
- All Silver records with `validation_status != 'invalid'` are classified
- Each Gold record has a `document_type_label`, `routing_label`, and `export_payload`
- Export payloads are written as JSON to the Gold Volume path
- MLflow run reports label distribution, confidence distribution, and export readiness rate

---

## Phase A-4 — Evaluation and Observability Layer

**Status**: Not started

**Goal**: Formalize evaluation as explicit, rerunnable MLflow experiments across all pipeline stages. Enable per-document traceability from Gold back to source.

**Deliverables**:

| Artifact | Path | Description |
|---|---|---|
| Evaluation runner | `src/evaluation/run_evaluation.py` | Batch evaluation across all stages |
| Trace builder | `src/evaluation/trace_builder.py` | Reconstruct full document lineage |
| Evaluation summary | `notebooks/eval_pipeline_summary.ipynb` | Cross-stage summary report |

**Completion criteria**:
- Given any Gold `document_id`, the full Bronze → Silver → Gold lineage is retrievable
- Re-running evaluation on a Delta snapshot produces stable metrics
- Evaluation summary notebook renders a clean per-document trace for at least five test documents

---

## Phase B — Bedrock Handoff Integration

**Status**: Future (scope pending)

**Goal**: Replace the V1 file-based export with a live integration that delivers Gold payloads to a Bedrock retrieval index or agent workflow.

**Scope**: To be defined when Bedrock CaseOps interface is stabilized. This phase depends on the export contract defined in `ARCHITECTURE.md` being accepted by the consuming system.

---

## Milestone Markers

These are the checkpoints that determine when the project is V1-complete:

- [ ] A-1: One real document processed end-to-end to Bronze
- [ ] A-2: One document class with validated Silver extraction
- [ ] A-3: Gold classification and routing label assigned for one document
- [ ] A-3: Export payload written and structurally valid
- [ ] A-4: Full lineage trace demonstrated for five documents
- [ ] A-4: MLflow experiments populated with real metrics across all three layers
