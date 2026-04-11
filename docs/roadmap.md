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
| A-3B | Personal Databricks Bootstrap Consolidation | ✅ Complete | Validated personal-workspace SQL execution; repo consolidation |
| A-4 | Evaluation and Observability | ✅ Complete | Formal evaluation layer across all stages; structured reports; 84 tests |
| A-4.1 | Runtime Validation Checkpoint | ✅ Complete | Runtime inspection of A-3B bootstrap tables; confirmed null confidence and routing behavior |
| B-0 | Bedrock Handoff Contract Preparation | ✅ Complete | Explicit Gold → Bedrock interface contract; payload definitions; routing map; delivery semantics |
| B-1 | Handoff Contract Materialization and Validation | ✅ Complete | B-0 contract converted from docs into repo-enforced schema, fixtures, and tests |

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

**Status**: Complete

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

**Status**: Complete

**Goal**: Formalize evaluation as explicit, rerunnable evaluation passes across all pipeline stages. Enable per-document traceability from Gold back to source. Resolve A-3B architectural tensions honestly in code and docs.

**Deliverables**:

| Artifact | Path | Status | Description |
|---|---|---|---|
| Bronze evaluator | `src/evaluation/eval_bronze.py` | ✅ Implemented | Parse quality metrics, flagged records |
| Silver evaluator | `src/evaluation/eval_silver.py` | ✅ Implemented | Extraction quality, field coverage, required null rate |
| Gold evaluator | `src/evaluation/eval_gold.py` | ✅ Updated A-4 | Classification quality; explicit null-confidence handling |
| Traceability evaluator | `src/evaluation/eval_traceability.py` | ✅ New A-4 | Cross-layer link rates, orphan detection, placeholder run IDs |
| Full-pipeline orchestrator | `src/evaluation/run_evaluation.py` | ✅ New A-4 | Runs all evaluators, assembles EvaluationReport |
| Report models | `src/evaluation/report_models.py` | ✅ New A-4 | Structured dataclasses for evaluation reports |
| Report writer | `src/evaluation/report_writer.py` | ✅ New A-4 | JSON + text report output |
| Tests | `tests/` (4 files, 84 tests) | ✅ New A-4 | Covers all evaluators including null-confidence path |
| Examples | `examples/evaluation/README.md` | ✅ New A-4 | Usage guide for evaluation layer |

**A-3B tensions resolved in A-4**:
- `classification_confidence` NULL in bootstrap Gold → evaluator explicit, non-breaking
- `pipeline_run_id = 'bootstrap_sql_v1'` placeholder → traceability evaluator reports it; not treated as broken provenance
- Contract contradiction between required confidence threshold and bootstrap NULL → resolved in `docs/data-contracts.md`

**Completion criteria met**:
- ✅ Bronze, Silver, Gold, and Traceability evaluators exist as real code
- ✅ Evaluators run locally on representative structured inputs without Databricks
- ✅ Evaluators produce explicit metrics and structured JSON artifacts
- ✅ Gold evaluation handles missing/null confidence without breaking
- ✅ Traceability evaluation detects missing links / orphaned records
- ✅ Docs no longer contain a misleading contradiction about Gold confidence
- ✅ Roadmap/docs reflect A-3B complete and A-4 complete
- ✅ No secrets, workspace identifiers, or fake production claims introduced

---

## Phase A-4.1 — Runtime Validation Checkpoint

**Status**: Complete

**Goal**: Perform a direct runtime inspection of the A-3B bootstrap tables in the personal Databricks workspace to confirm evaluator assumptions and close the loop between platform behavior and repo documentation. This is not a separate major implementation phase; it is a validation sub-step that follows A-4.

**Findings**:
- The `ai_classify` output variant observed at runtime contained only `error_message` and `response`. No scalar confidence or score key was present at any tested extraction path (`$.confidence`, `$.score`, `$.response[0].confidence`, etc.).
- Gold routing confirmed: 3 records `fda_warning_letter` → `regulatory_review` (`export_ready = true`); 1 record `unknown` → `quarantine` (`export_ready = false`). Matched expected behavior.
- Null-confidence handling in `eval_gold.py` is correct for this bootstrap path.

**Documented in**: `docs/databricks-bootstrap.md` § A-4.1 and `ARCHITECTURE.md` § A-4.1 Runtime Inspection Findings.

---

## Phase B-0 — Bedrock Handoff Contract Preparation

**Status**: Complete

**Goal**: Establish the explicit, versioned Gold → Bedrock CaseOps interface contract before any live integration work begins. Harden the handoff boundary so that Phase B (live Bedrock integration) can proceed without interface ambiguity.

**Scope boundary**: B-0 is a documentation and contract discipline phase. It produces no AWS infrastructure, no Bedrock SDK code, no S3 plumbing, no live integration, and no production credentials. It defines what gets built; Phase B proper builds it.

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Bedrock handoff contract | `docs/bedrock-handoff-contract.md` | ✅ Complete | Single authoritative contract for Gold → Bedrock handoff; required vs optional fields; routing map; delivery semantics; versioning; known limitations; acceptance criteria |
| ARCHITECTURE.md update | `ARCHITECTURE.md` | ✅ Complete | Bedrock Handoff Design section strengthened; references contract doc; B-0 phase context added; routing table aligned |
| data-contracts.md update | `docs/data-contracts.md` | ✅ Complete | Export payload field definitions tightened; routing label table expanded with V1 status; provenance requirements added; contract doc cross-referenced |
| PROJECT_SPEC.md update | `PROJECT_SPEC.md` | ✅ Complete | B-0 entry replaced with concrete deliverables, exclusions, and rationale |
| roadmap.md update | `docs/roadmap.md` | ✅ Complete | B-0 section (this section) updated with concrete completion criteria |

**Completion criteria met:**

- ✅ One dedicated contract document for Gold → Bedrock handoff exists and is committed
- ✅ Required vs optional payload field expectations are explicit in the contract
- ✅ Every routing label maps to a named downstream Bedrock consumer
- ✅ `export_ready`, `quarantine`, and delivery semantics are unambiguous
- ✅ The Databricks / Bedrock ownership boundary is stated explicitly and consistently
- ✅ B-0 is defined as contract preparation, not live integration
- ✅ All updated documents are consistent with each other
- ✅ No document claims Bedrock CaseOps integration is live
- ✅ Bootstrap limitations (null confidence, placeholder run IDs) remain explicitly documented
- ✅ V1 executable domain remains FDA warning letters only

This is the first sub-phase of the broader Phase B (Bedrock Handoff Integration). Phase B proper begins live integration work against this contract.

---

## Phase B-1 — Handoff Contract Materialization and Validation

**Status**: Complete

**Goal**: Convert the B-0 Gold → Bedrock handoff contract from documentation into repo-enforced schema behavior, fixtures, and tests. B-1 makes the contract enforceable — not just documented.

**Scope boundary**: B-1 is contract materialization only. It introduces no AWS/Bedrock SDK code, no live integration, no S3 plumbing, no vector search, and no agent workflows. The repo remains the upstream-only governed document intelligence layer.

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Contract validator | `src/schemas/bedrock_contract.py` | ✅ Complete | `validate_export_payload()` and `validate_quarantine_record()` enforce B-0 §4, §6 |
| Contract-valid fixture | `examples/contract_valid_fda_export_payload.json` | ✅ Complete | Standalone V1 FDA export payload matching B-0 §4.5 exactly |
| B-1 test suite | `tests/test_bedrock_contract_validation.py` | ✅ Complete | 53 tests covering valid, invalid, quarantine, bootstrap-path, and schema alignment cases |
| Gold schema alignment | `src/schemas/gold_schema.py` | ✅ Updated | `classification_confidence` made `Optional[float]` in `ExportProvenance` and `GoldRecord` per B-0 §4.3 |
| Pipeline alignment | `src/pipelines/classify_gold.py` | ✅ Updated | Routing/readiness logic handles `None` confidence correctly per B-0 §6 |

**What B-1 enforces:**

- Required vs optional payload field expectations (B-0 §4.1, §4.2)
- Required provenance fields, with `classification_confidence` explicitly nullable (B-0 §4.3)
- Required FDA warning letter extracted_fields for the V1 executable slice (B-0 §4.4)
- `document_type != 'unknown'` and `routing_label != 'quarantine'` for valid handoff units (B-0 §3)
- Quarantine record shape: `export_ready=False`, `routing_label='quarantine'`, `export_path=None` (B-0 §6)
- Bootstrap-path: `classification_confidence=None` accepted without rejection (B-0 §9)

**Inconsistencies found and resolved:**

- `ExportProvenance.classification_confidence` was `float` (non-optional) but B-0 §4.3 and §9 explicitly allow null for bootstrap-origin records. Fixed to `Optional[float]`.
- `GoldRecord.classification_confidence` had the same issue. Fixed.
- `classify_gold.py` routing and readiness logic compared `None < threshold` (would raise TypeError). Fixed with explicit None guard per B-0 §6.
- The existing Gold fixture `expected_gold_fda_warning_letter.json` had `classification_confidence=0.9` (valid target-state shape — no change needed). A separate bootstrap-path fixture is not required since null confidence is tested in the test suite.

**Completion criteria met:**

- ✅ Gold export payload validation is explicit in the repo (`src/schemas/bedrock_contract.py`)
- ✅ Required vs optional handoff fields are enforced by `validate_export_payload()`
- ✅ At least one V1 FDA Gold fixture is contract-valid (`examples/contract_valid_fda_export_payload.json`)
- ✅ Tests cover valid, invalid, quarantine, null-confidence (bootstrap path), and optional-field cases
- ✅ Gold schema / pipeline output / fixture shape are aligned (Optional confidence)
- ✅ Repo docs remain consistent with B-0 (no rewrites of the handoff contract)
- ✅ No AWS/Bedrock SDK code introduced; no fake live integration added
- ✅ 137 tests pass (84 A-4 existing + 53 B-1 new); no regressions

---

## Milestone Markers

These are the checkpoints that determine when the project is V1-complete:

- [x] A-1: One real document processed end-to-end to Bronze
- [x] A-2: One document class with validated Silver extraction
- [x] A-3: Gold classification and routing label assigned for one document
- [x] A-3: Export payload written and structurally valid
- [x] A-3B: End-to-end validated in a real personal Databricks workspace with 4 FDA documents
- [x] A-4: Full lineage trace evaluator implemented (eval_traceability.py)
- [x] A-4: Evaluation runners implemented for all four quality dimensions
- [x] A-4.1: Runtime inspection confirmed null confidence and routing behavior for bootstrap path
- [x] B-0: Gold → Bedrock handoff contract established (`docs/bedrock-handoff-contract.md`)
- [x] B-1: B-0 contract materialized into repo-enforced validator, contract-valid fixture, and 53 tests (`src/schemas/bedrock_contract.py`, `tests/test_bedrock_contract_validation.py`)
- [ ] MLflow experiments populated with real metrics from a live Databricks workspace (deferred — requires live execution)
