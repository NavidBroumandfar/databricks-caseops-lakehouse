# Roadmap

> Practical delivery plan for the Databricks CaseOps Lakehouse pipeline.
> Authoritative scope is in [`PROJECT_SPEC.md`](../PROJECT_SPEC.md).
> This file provides phase-level detail and milestone tracking.

---

## Phase Summary

| Phase | Name | Status | Goal |
|---|---|---|---|
| A-0 | Repo Foundation | ✅ Complete | Docs, scaffold, project identity |
| A-1 | Bronze: Ingest and Parse | ✅ Complete | Raw file → parsed text in Bronze Delta table |
| A-2 | Silver: Extraction and Validation | ✅ Complete | Parsed text → structured fields in Silver table |
| A-3 | Gold: Classification and Routing | ✅ Complete | Structured fields → classified, routed Gold records (local-safe) |
| A-3B | Personal Databricks Bootstrap Consolidation | 🔄 In Progress | Validated personal-workspace SQL execution; repo consolidation |
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

**Status**: Complete

**Goal**: Build the ingestion and parsing pipeline. A document dropped into the Unity Catalog Volume should produce a Bronze Delta record with parsed text and provenance metadata. For local development and demonstration, a Bronze JSON artifact is written instead of a Delta table write.

**Deliverables**:

| Artifact | Path | Status | Description |
|---|---|---|---|
| Unity Catalog config | `src/pipelines/catalog_config.yaml` | ✅ Complete | Catalog, schema, Volume definitions (placeholder values) |
| Bronze ingestion script | `src/pipelines/ingest_bronze.py` | ✅ Complete | File validation, hash, metadata, parser abstraction, artifact writer |
| Bronze schema | `src/schemas/bronze_schema.py` | ✅ Complete | Pydantic v2 model for Bronze records |
| Bronze evaluation script | `src/evaluation/eval_bronze.py` | ✅ Complete | Separate parse quality evaluation; optional MLflow logging |
| Sample fixture | `examples/fda_warning_letter_sample.md` | ✅ Complete | Synthetic FDA warning letter for local demonstration |
| Delta table write | `src/pipelines/ingest_bronze.py` | 🔲 Deferred | Requires live Databricks runtime; local JSON artifact written instead |

**Completion criteria**:
- ✅ A local sample file can be processed into a Bronze JSON artifact
- ✅ Parse failures are captured as records with `parse_status = 'failed'`, not silent drops
- ✅ Bronze schema is validated against at least one real document
- ✅ A separate evaluation script computes parse quality metrics from Bronze artifacts
- 🔲 Delta table write validated on Databricks cluster (deferred to Databricks execution phase)
- 🔲 MLflow evaluation run populated with real metrics on Databricks (deferred)

---

## Phase A-2 — Silver: Extraction and Validation

**Status**: Complete

**Goal**: Extract structured fields from Bronze parsed text. Validate all extracted records against the Silver schema. Surface validation failures as records, not silent drops. Local-safe implementation uses a deterministic rule-based extractor; Databricks `ai_extract` integration is stubbed as an adapter for future enablement.

**Deliverables**:

| Artifact | Path | Status | Description |
|---|---|---|---|
| Silver schema | `src/schemas/silver_schema.py` | ✅ Complete | Pydantic v2 model; FDA warning letter field set; coverage calculator |
| Extraction config | `src/pipelines/extraction_config.yaml` | ✅ Complete | Domain, paths, field lists, model identifiers — no secrets |
| Extraction prompt templates | `src/utils/extraction_prompts.py` | ✅ Complete | Versioned prompt for future `ai_extract`; not used in local execution |
| Silver extraction pipeline | `src/pipelines/extract_silver.py` | ✅ Complete | Extractor abstraction; local rule-based FDA extractor; Databricks adapter stub |
| Silver evaluation script | `src/evaluation/eval_silver.py` | ✅ Complete | Separate evaluation; validity rates, coverage, required-field null rate |
| Expected Silver fixture | `examples/expected_silver_fda_warning_letter.json` | ✅ Complete | Reference extraction result for the sample FDA warning letter |
| Delta table write | `src/pipelines/extract_silver.py` | 🔲 Deferred | Requires live Databricks runtime; local JSON artifact written instead |

**Completion criteria**:
- ✅ Bronze JSON artifact can be processed into a Silver JSON artifact locally
- ✅ Silver artifact conforms to the Silver schema (Pydantic-validated)
- ✅ Lineage fields (`document_id`, `bronze_record_id`, `pipeline_run_id`) are preserved
- ✅ A separate evaluation script computes required Silver metrics
- ✅ FDA warning letter field set is the sole V1 domain — no multi-domain execution
- 🔲 Delta table write validated on Databricks cluster (deferred)
- 🔲 MLflow evaluation run with real metrics on Databricks (deferred)

---

## Phase A-3 — Gold: Classification and Routing

**Status**: Complete (local-safe implementation)

**Goal**: Classify Silver records into document type labels and routing labels. Construct the export payload for downstream Bedrock consumption. Local-safe implementation uses a deterministic rule-based classifier; Databricks `ai_classify` integration is stubbed as an adapter for future enablement.

**Deliverables**:

| Artifact | Path | Status | Description |
|---|---|---|---|
| Gold schema | `src/schemas/gold_schema.py` | ✅ Complete | Pydantic v2 model; ExportPayload + ExportProvenance sub-structs; lineage fields |
| Classification taxonomy | `src/utils/classification_taxonomy.py` | ✅ Complete | Closed label sets for document types and routing; V1 routing map; helpers |
| Classification config | `src/pipelines/classification_config.yaml` | ✅ Complete | Domain, paths, thresholds, model identifiers — no secrets |
| Gold classification pipeline | `src/pipelines/classify_gold.py` | ✅ Complete | Classifier abstraction; local rule-based FDA classifier; Databricks adapter stub; export artifact writer |
| Gold evaluation script | `src/evaluation/eval_gold.py` | ✅ Complete | Separate evaluation; classification success rate, export-ready rate, confidence metrics, label distribution |
| Expected Gold fixture | `examples/expected_gold_fda_warning_letter.json` | ✅ Complete | Reference Gold record for the sample FDA warning letter |
| Delta table write | `src/pipelines/classify_gold.py` | 🔲 Deferred | Requires live Databricks runtime; local JSON artifact written instead |

**Completion criteria**:
- ✅ Silver JSON artifact can be processed into a Gold JSON artifact locally
- ✅ Gold artifact conforms to the Gold schema (Pydantic-validated)
- ✅ Gold pipeline preserves lineage fields (`document_id`, `bronze_record_id`, `extraction_id`, `pipeline_run_id`)
- ✅ Export payload JSON artifact is materialized for export-ready records
- ✅ A separate evaluation script computes required Gold metrics
- ✅ FDA warning letter is the sole V1 domain — no multi-domain execution
- 🔲 Delta table write validated on Databricks cluster (deferred)
- 🔲 MLflow evaluation run with real metrics on Databricks (deferred)

---

---

## Phase A-3B — Personal Databricks Bootstrap Consolidation

**Status**: In Progress

**Goal**: Capture and consolidate a validated personal-workspace end-to-end SQL execution pass using real Databricks AI Functions (`ai_parse_document`, `ai_extract`, `ai_classify`) and public FDA sample documents. This is a bridging phase that reduces placeholder-only architecture, records the real platform validation that was completed, and prepares the repo cleanly for the formal A-4 evaluation phase.

**Scope constraints**:
- Personal Databricks Free Edition workspace only
- Non-production; no enterprise deployment, Jobs, Workflows, or Asset Bundles
- Public FDA warning letter PDFs only — no proprietary data
- No MLflow automation (manual SQL evaluation proxy used)
- No service principals, external locations, or organizational credentials

**Deliverables**:

| Artifact | Path | Status | Description |
|---|---|---|---|
| Bronze parse smoke SQL | `notebooks/bootstrap/01_bronze_parse_smoke.sql` | ✅ Complete | Smoke test confirming ai_parse_document reads from managed volume |
| Bronze bootstrap v1 SQL | `notebooks/bootstrap/02_bronze_bootstrap_v1.sql` | ✅ Complete | Full Bronze ingest with UUID, hash, provenance fields |
| Silver extraction smoke SQL | `notebooks/bootstrap/03_silver_extract_smoke_v1.sql` | ✅ Complete | ai_extract with FDA warning letter prompt schema |
| Gold classification smoke SQL | `notebooks/bootstrap/04_gold_classify_route_smoke_v1.sql` | ✅ Complete | ai_classify + rule-based routing; quarantine path confirmed |
| Bootstrap evaluation SQL | `notebooks/bootstrap/05_bootstrap_evaluation_v1.sql` | ✅ Complete | Cross-layer lineage join and summary counts |
| Bootstrap documentation | `docs/databricks-bootstrap.md` | ✅ Complete | Validated results, constraints, what this proves and does not prove |
| Resource layout example | `config/databricks.resources.example.yml` | ✅ Complete | Unity Catalog layout reference (no credentials) |

**Validated results**:

| Metric | Value |
|---|---|
| total_documents | 4 |
| bronze_success_count | 4 |
| silver_record_count | 4 |
| gold_export_ready_count | 3 |
| quarantine_count | 1 |
| full_lineage_count | 4 |

**Known implementation detail**: `classification_confidence` is `NULL` in the A-3B Gold smoke table. The `ai_classify` response variant at this bootstrap stage does not expose a scalar confidence score via `try_variant_get`. This is documented explicitly. Resolving confidence extraction is A-4 scope.

**Completion criteria**:
- ✅ Five SQL bootstrap files committed under `notebooks/bootstrap/`
- ✅ `config/databricks.resources.example.yml` committed
- ✅ `docs/databricks-bootstrap.md` committed
- ✅ `PROJECT_SPEC.md`, `ARCHITECTURE.md`, `README.md`, `docs/roadmap.md`, `docs/CURSOR_CONTEXT.md` updated to reflect A-3B

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
