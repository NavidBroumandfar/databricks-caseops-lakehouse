# Roadmap

> Practical delivery plan for the Databricks CaseOps Lakehouse pipeline.
> Authoritative scope is in [`PROJECT_SPEC.md`](../PROJECT_SPEC.md).
> This file provides phase-level detail and milestone tracking.

---

## V1 Phase Summary

> V2 phases are defined in the [V2 — Future Work](#v2--future-work) section below.

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
| B-2 | Contract-Enforced Export Materialization | ✅ Complete | Pipeline obeys B-1 contract during real export write; invalid payloads blocked; quarantine explicit |
| B-3 | Export Packaging Refactor and Handoff Service Boundary | ✅ Complete | Export/handoff logic extracted into `export_handoff.py`; `classify_gold.py` delegates cleanly |
| B-4 | Export Outcome Observability and Handoff Reporting | ✅ Complete | Explicit outcome categories, reason codes, and batch-level handoff summary reporting |
| B-5 | Handoff Batch Manifest and Review Bundle | ✅ Complete | Single reviewable batch bundle with per-record artifact references, linked to B-4 report |
| B-6 | Handoff Bundle Integrity and Consistency Validation | ✅ Complete | Local-safe validator that proves the B-5 bundle is internally trustworthy and review-safe |
| C-0 | Live Handoff: Delivery Mechanism Design | ✅ Complete (V2) | Delta Sharing selected as primary; delivery events as secondary; V1 file path retained |
| C-1 | Live Handoff: Export Delivery Implementation | ✅ Complete (V2) | Producer-side delivery augmentation: DeliveryEvent schema + Delta Share prep layer |
| C-2 | Live Handoff: Runtime Integration Validation | ✅ Complete (V2, producer-side) | 15-check delivery-layer validation; honest `not_provisioned` baseline; runbook for workspace validation |

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

## Phase B-2 — Contract-Enforced Export Materialization

**Status**: Complete

**Goal**: Make the real Gold export-writing path obey the B-1 contract during actual materialization. B-2 converts contract enforcement from a testable schema layer into a pipeline-level gate: invalid export payloads cannot be written as Bedrock handoff artifacts. Quarantine behavior is explicit, deterministic, and correctly separated from export-ready behavior.

**Scope boundary**: B-2 strengthens upstream export behavior only. No AWS/Bedrock SDK, no S3, no live integration, no vector search, no agent workflows, no orchestration are introduced. The repo remains the upstream-only governed document intelligence layer.

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Contract-enforced pipeline | `src/pipelines/classify_gold.py` | ✅ Updated B-2 | Validates export payload with B-1 validator before every write; blocks invalid payloads; validates quarantine shape |
| B-2 materialization test suite | `tests/test_b2_export_materialization.py` | ✅ New B-2 | 18 focused tests covering valid write, contract block, quarantine separation, path generation, error surfacing, no-SDK guard |
| Invalid payload fixture | `examples/invalid_export_payload_missing_fields.json` | ✅ New B-2 | FDA payload that passes routing but fails contract validation (empty violation_type, null corrective_action_requested) |
| Quarantine record fixture | `examples/quarantine_gold_record.json` | ✅ New B-2 | Correctly shaped quarantine Gold record per B-0 §6; export_ready=False, export_path=null |

**What B-2 enforces in the pipeline:**

- Before any export-ready payload is written to disk, `validate_export_payload()` from `bedrock_contract.py` is called
- If validation fails: the export artifact is NOT written; the Gold record is updated to `export_ready=False`; `contract_validation_errors` is populated in the summary; the failure is logged explicitly
- If validation passes: the export artifact is written at the deterministic path `<export_base>/<routing_label>/<document_id>.json`
- Quarantine records (`routing_label='quarantine'`) are validated with `validate_quarantine_record()` post-assembly for governance shape correctness
- The Gold record is written once with its final resolved state (export_path and export_ready reflect actual outcome)

**What B-2 makes explicit:**

| Behavior | Pre-B-2 | Post-B-2 |
|---|---|---|
| Export payload contract validation in pipeline | Not called | Called before every write |
| Invalid payload write behavior | Possible (no gate) | Blocked; errors in summary |
| Quarantine shape validation | Not called | Called post-assembly |
| Gold record written once vs twice | Double-write (then update) | Written once with final state |
| Summary contract error field | Not present | Always present (`contract_validation_errors`) |

**Inconsistencies found and resolved:**

- The pre-B-2 pipeline called `compute_export_ready()` for routing, which did not use the B-1 contract validator (`validate_export_payload()`). A payload could be marked export_ready=True and written without ever being checked against B-0 §4 requirements. Fixed: contract validation now gates every write.
- The Gold artifact was written twice (once without export_path, once with it after the export write). Restructured: export path is determined first, then the Gold record is written once with its final state.

**Completion criteria met:**

- ✅ Export payload validation is invoked in the real export-writing flow (`src/pipelines/classify_gold.py`)
- ✅ Invalid export-ready payloads are not written as valid Bedrock handoff exports
- ✅ Quarantine behavior is explicit and correctly separated from export-ready behavior
- ✅ Export path generation/materialization behavior is deterministic and tested
- ✅ Pipeline behavior, validator, and tests are aligned
- ✅ Docs updated only where necessary and remain consistent
- ✅ No AWS/Bedrock SDK code or fake live integration added

---

## Phase B-3 — Export Packaging Refactor and Handoff Service Boundary

**Status**: Complete

**Goal**: Extract export/handoff materialization behavior from `classify_gold.py` into a dedicated internal module (`src/pipelines/export_handoff.py`), giving the export/handoff slice a clean service boundary while preserving B-2 behavior exactly.

**Scope boundary**: B-3 is a structural refactor only. No AWS/Bedrock SDK, no live integration, no contract semantic changes, no new downstream assumptions. The repo remains the upstream-only governed document intelligence layer.

**What B-3 extracts:**

| Behavior | Pre-B-3 location | Post-B-3 location |
|---|---|---|
| Export artifact path computation | Inline in `write_export_artifact` | `compute_export_path()` in `export_handoff.py` |
| Export artifact write | `write_export_artifact()` in `classify_gold.py` | `write_export_artifact()` in `export_handoff.py` |
| Contract-gated export validation + write | Inline block in `run_classify_gold` | `execute_export()` in `export_handoff.py` |
| Quarantine governance shape assertion | Inline block in `run_classify_gold` | `execute_export()` in `export_handoff.py` |

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Export/handoff module | `src/pipelines/export_handoff.py` | ✅ New B-3 | `compute_export_path`, `write_export_artifact`, `execute_export`, `ExportResult` |
| Simplified pipeline | `src/pipelines/classify_gold.py` | ✅ Updated B-3 | Delegates to `execute_export`; no inline contract/export logic |
| B-3 test suite | `tests/test_b3_export_handoff.py` | ✅ New B-3 | 28 tests: path computation, execute_export all cases, module boundary, integration |
| B-2 test update | `tests/test_b2_export_materialization.py` | ✅ Updated B-3 | Import updated; no-SDK guard updated to reflect new module boundary |

**Module boundary established:**

```
classify_gold.py   → assembles GoldRecord → calls execute_export → writes Gold artifact
export_handoff.py  → validates contract  → writes export artifact → returns ExportResult
```

**Inconsistencies found and resolved:**
None. B-2 behavior was preserved exactly. The `execute_export` function encapsulates the identical contract gating, quarantine shape validation, and write logic that was previously inline. All 183 tests pass (155 pre-B-3 + 28 new B-3).

**Completion criteria met:**

- ✅ Export/handoff materialization logic extracted into `src/pipelines/export_handoff.py`
- ✅ `classify_gold.py` is materially cleaner — no inline contract/export logic
- ✅ B-2 export behavior preserved exactly (same contract enforcement rules, same paths)
- ✅ Contract enforcement still happens in the real export path (via `execute_export`)
- ✅ Quarantine and export-ready behavior remain explicit and tested
- ✅ Path generation is deterministic and tested via `compute_export_path`
- ✅ No AWS/Bedrock SDK code or fake live integration introduced
- ✅ 183 tests pass; zero regressions

---

## Phase B-4 — Export Outcome Observability and Handoff Reporting

**Status**: Complete

**Goal**: Make Gold → Bedrock handoff outcomes operationally visible, structured, and reviewable at batch level. Each pipeline run now produces a clear handoff outcome summary showing what was exported, quarantined, contract-blocked, or skipped, and why.

**Scope boundary**: B-4 is reporting and observability only. No AWS/Bedrock SDK, no live integration, no S3, no contract semantic changes, no new downstream assumptions. The repo remains the upstream-only governed document intelligence layer.

**Design constraint (B-4 vs A-4)**: A-4 covers generic pipeline evaluation quality (parse, extraction, classification, traceability). B-4 is narrower: what happened specifically in the handoff/export path — what was written vs not written, why records were blocked or quarantined, and what a downstream readiness review would need to inspect.

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Handoff reporting module | `src/pipelines/handoff_report.py` | ✅ New B-4 | Outcome categories, reason codes, `HandoffBatchReport`, `build_handoff_batch_report`, `write_handoff_report`, `format_handoff_report_text` |
| Pipeline integration | `src/pipelines/classify_gold.py` | ✅ Updated B-4 | Per-record `outcome_category` + `outcome_reason` in summaries; `report_dir` parameter; batch report written when requested |
| B-4 test suite | `tests/test_b4_handoff_report.py` | ✅ New B-4 | 68 tests covering outcome constants, derivation, batch report, write, integration, module boundary |

**Outcome categories defined:**

| Category | Meaning |
|---|---|
| `exported` | Record successfully written as a Bedrock handoff artifact |
| `quarantined` | Record routed to quarantine — governance path, no export file |
| `contract_blocked` | Export-ready record rejected by B-1 contract validator — not written |
| `skipped_not_export_ready` | Not export-ready and not quarantined — no export attempted |

**Reason codes defined:**

| Reason Code | Meaning |
|---|---|
| `none` | Successful export — no blocking reason |
| `routing_quarantine` | Document routed to quarantine by classification/routing logic |
| `contract_validation_failed` | B-1 validator rejected the payload; field-level detail in `contract_validation_errors` |
| `export_not_attempted` | Not export-ready and not quarantined (edge case path) |

**Batch report structure:**

Each `HandoffBatchReport` includes: `pipeline_run_id`, `batch_processed_at`, `total_records_processed`, `total_ineligible_skipped`, `total_eligible`, `total_export_attempts`, `total_exported`, `total_quarantined`, `total_contract_blocked`, `total_skipped_not_export_ready`, `outcome_distribution`, `reason_code_distribution`, `contract_blocked_document_ids`, `quarantined_document_ids`.

Written as both `.json` (machine-readable) and `.txt` (human-readable) artifacts.

**Module boundary preserved:**

```
classify_gold.py   → assembles GoldRecord → calls execute_export → derives outcome → builds batch report
export_handoff.py  → validates contract  → writes export artifact → returns ExportResult
handoff_report.py  → derives outcome categories → aggregates batch report → writes report artifacts
```

**Completion criteria met:**

- ✅ Export/handoff outcomes have explicit, stable outcome categories
- ✅ Non-export paths have structured reason codes
- ✅ The real pipeline flow produces a structured batch-level handoff summary
- ✅ Exported / quarantined / contract-blocked / skipped outcomes are clearly distinguishable
- ✅ Tests cover both helper-level and integration-level reporting behavior (68 tests)
- ✅ Module boundaries remain clean (`handoff_report.py` distinct from `export_handoff.py` and `classify_gold.py`)
- ✅ No AWS/Bedrock SDK code or fake live integration introduced
- ✅ 251 tests pass; zero regressions

---

## Phase B-5 — Handoff Batch Manifest and Review Bundle

**Status**: Complete

**Goal**: Package the Gold → Bedrock export batch outputs into a single, coherent, reviewable batch handoff bundle. Each pipeline run that produces a B-5 bundle has a single manifest artifact that links batch metadata, aggregate outcome counts, per-record artifact references organized by outcome category, and paths to B-4 report artifacts into one reviewable unit.

**Scope boundary**: B-5 is upstream handoff packaging only. No AWS/Bedrock SDK, no live integration, no S3, no contract semantic changes, no new downstream assumptions. The repo remains the upstream-only governed document intelligence layer.

**Design constraint (B-5 vs B-4)**: B-4 tells us what happened in the handoff path: outcome categories, reason codes, counts, and affected document ID lists. B-5 packages that batch into a clean manifest with full per-record artifact references — the single artifact a reviewer opens to understand and navigate the full state of a batch handoff run.

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Batch manifest/review bundle module | `src/pipelines/handoff_bundle.py` | ✅ New B-5 | `MANIFEST_VERSION`, `RecordArtifactRef`, `HandoffBatchManifest`, `build_handoff_batch_manifest`, `compute_bundle_path`, `format_bundle_text`, `write_handoff_bundle` |
| Pipeline integration | `src/pipelines/classify_gold.py` | ✅ Updated B-5 | `bundle_dir` parameter; `--bundle-dir` CLI arg; B-5 bundle written after B-4 report when requested |
| B-5 test suite | `tests/test_b5_handoff_bundle.py` | ✅ New B-5 | 84 tests covering manifest structure, builder, path computation, text formatter, write, integration, module boundary |
| Expected manifest fixture | `examples/expected_handoff_batch_manifest.json` | ✅ New B-5 | Reference manifest showing a batch with 3 eligible records: 2 exported, 1 quarantined, with report_artifacts attached |

**Manifest structure (per-run):**

Each `HandoffBatchManifest` includes: `manifest_version`, `batch_id` (= `pipeline_run_id`), `pipeline_run_id`, `generated_at`, `total_records_processed`, `total_ineligible_skipped`, `total_eligible`, `total_exported`, `total_quarantined`, `total_contract_blocked`, `total_skipped_not_export_ready`, `outcome_distribution`, `exported_records` (with `export_artifact_path`), `quarantined_records`, `contract_blocked_records`, `skipped_records`, `report_artifacts` (B-4 JSON + text paths when available), `review_notes`.

Written as both `.json` (machine-readable) and `.txt` (human-readable review summary).

**Module boundary preserved:**

```
classify_gold.py   → assembles GoldRecord → calls execute_export → derives outcome → builds batch report → builds bundle
export_handoff.py  → validates contract  → writes export artifact → returns ExportResult
handoff_report.py  → derives outcome categories → aggregates batch report → writes report artifacts
handoff_bundle.py  → packages batch into manifest → writes bundle artifacts (JSON + text)
```

**Completion criteria met:**

- ✅ A stable batch manifest/review bundle exists (`src/pipelines/handoff_bundle.py`)
- ✅ The bundle clearly references exported / quarantined / contract-blocked / skipped records with artifact paths
- ✅ The bundle references B-4 handoff report artifacts when available
- ✅ The real pipeline flow produces the bundle via `--bundle-dir` (`classify_gold.py`)
- ✅ Bundle path/materialization behavior is deterministic and tested (`compute_bundle_path`)
- ✅ Module boundaries remain clean (4 distinct modules, no collapsed responsibilities)
- ✅ No AWS/Bedrock SDK code or fake live integration added
- ✅ 335 tests pass; zero regressions (84 A-4 + 53 B-1 + 18 B-2 + 28 B-3 + 68 B-4 + 84 B-5)

---

## Phase B-6 — Handoff Bundle Integrity and Consistency Validation

**Status**: Complete

**Goal**: Prove that the B-5 handoff batch bundle is internally trustworthy and review-safe. Implement a dedicated local-safe validation layer that checks the bundle for structural correctness, count consistency, reference integrity, identifier uniqueness, and filesystem path existence.

**Scope boundary**: B-6 is upstream bundle validation only. No AWS/Bedrock SDK, no live integration, no S3, no contract semantic changes, no new downstream assumptions. The manifest remains a derived packaging artifact — B-6 confirms it is coherent with the artifacts it references.

**What B-6 validates:**

| Check Category | Checks |
|---|---|
| Structural | manifest_version known; batch_id == pipeline_run_id |
| Count consistency | total_exported/quarantined/blocked/skipped match record list lengths; total_eligible = sum of lists; total_records_processed = total_eligible + ineligible; outcome_distribution matches lists |
| Reference consistency | exported records have export paths; non-exported records don't; outcome_category correct per list; quarantined records have quarantine routing; exported records don't have quarantine routing |
| Identifier uniqueness | no duplicate document_ids; no duplicate gold_record_ids across all record lists |
| Filesystem paths | gold_artifact_paths exist; export_artifact_paths exist; report_artifact paths exist (all optional, on by default) |

**Deliverables:**

| Artifact | Path | Status | Description |
|---|---|---|---|
| Bundle validation module | `src/pipelines/handoff_bundle_validation.py` | ✅ New B-6 | `CHECK_*` constants, `CheckResult`, `BundleValidationResult`, `validate_handoff_bundle`, `validate_handoff_bundle_from_manifest`, `write_validation_result`, `format_validation_result_text` |
| B-6 test suite | `tests/test_b6_bundle_validation.py` | ✅ New B-6 | 92 tests covering constants, models, valid bundle, structural failures, count mismatches, reference contradictions, identifier uniqueness, path checks, file-based entry point, write/format, module boundary, integration |

**Module boundary preserved:**

```
classify_gold.py               → assembles GoldRecord → calls execute_export → writes Gold artifact → builds bundle
export_handoff.py              → validates contract → writes export artifact → returns ExportResult
handoff_report.py              → derives outcome categories → aggregates batch report → writes report
handoff_bundle.py              → packages batch into manifest/review bundle → writes bundle artifacts
handoff_bundle_validation.py   → validates the bundle is internally consistent and trustworthy
```

**Completion criteria met:**

- ✅ Dedicated bundle integrity/consistency validation module exists (`src/pipelines/handoff_bundle_validation.py`)
- ✅ Validator checks referenced artifact existence and count consistency
- ✅ Validator detects broken or contradictory bundle state
- ✅ Validator produces a structured validation result (`BundleValidationResult`)
- ✅ Tests cover valid and broken bundle scenarios (92 tests)
- ✅ Module boundaries remain clean after the change
- ✅ No AWS/Bedrock SDK code or fake live integration added
- ✅ 427 tests pass; zero regressions (335 pre-B-6 + 92 new B-6)

---

## Milestone Markers

These are the checkpoints that determine when the project is V1-complete.

**V1 IS COMPLETE** — all milestones below are checked. The final MLflow live-workspace milestone was completed in April 2026.

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
- [x] B-2: Contract-enforced export materialization — pipeline validates before write; invalid payloads blocked; quarantine explicit; 22 new tests (`src/pipelines/classify_gold.py`, `tests/test_b2_export_materialization.py`)
- [x] B-3: Export/handoff slice extracted into `src/pipelines/export_handoff.py`; `classify_gold.py` delegates to `execute_export`; 28 new tests; 183 total tests pass
- [x] B-4: Explicit handoff outcome categories, reason codes, and batch-level `HandoffBatchReport`; `handoff_report.py` module; 68 new tests; 251 total tests pass
- [x] B-5: Single reviewable batch manifest/review bundle; `handoff_bundle.py` module; per-record artifact references; B-4 report linking; `--bundle-dir` CLI arg; 84 new tests; 335 total tests pass
- [x] B-6: Bundle integrity and consistency validation; `handoff_bundle_validation.py` module; 24 explicit checks across structural, count, reference, uniqueness, and path categories; 92 new tests; 427 total tests pass
- [x] MLflow experiments populated with real metrics from a live Databricks workspace — completed April 2026; all four evaluation stages (bronze parse quality, silver extraction quality, gold classification quality, pipeline traceability) logged to Databricks-hosted MLflow experiments via `CASEOPS_MLFLOW_EXPERIMENT_ROOT`-qualified experiment paths; personal non-production workspace; see `docs/v1-closeout-mlflow-checkpoint.md` for runbook details

---

## V1 Closeout Statement

V1 is complete as of April 2026. This means:

- End-to-end single-domain (FDA warning letters) pipeline implemented, validated in a real Databricks personal workspace, and closed with live MLflow experiment population
- Bronze / Silver / Gold contracts implemented, contract-enforced, and tested (427 tests)
- Evaluation layer implemented for all four quality dimensions; all four stages logged to Databricks MLflow
- Gold → Bedrock handoff preparation layer complete: contract, repo-enforced validator, materialization gate, outcome reporting, batch bundle, bundle integrity validation
- Databricks-safe MLflow experiment path resolution implemented (`src/evaluation/mlflow_experiment_paths.py`)
- V1 remains single-domain and controlled — no enterprise deployment, no production credentials, no live Bedrock integration
- Downstream Bedrock live integration is future work (V2+)
- V2 is formally defined and has not started

---

## V2 — Future Work

**V2 is not started.** V2 is formally defined after V1 closeout (April 2026). The phases below are planned and not yet implemented. No V2 code, infrastructure, or live integration exists.

### V2 Boundary Rules

- V2 may deepen the Bedrock handoff and operational readiness of this repo
- V2 must not turn this repo into Bedrock CaseOps
- The Databricks / Bedrock ownership split remains explicit in all V2 phases
- V2 introduces no production credentials, no enterprise deployment, and no implementation work until a phase is formally started with an explicit spec
- V2 begins when Phase C-0 is formally started

### V2 Phase Summary

| Phase | Name | Status | Goal |
|---|---|---|---|
| C-0 | Integration Delivery Mechanism Design | ✅ Design Complete | Delta Sharing selected; file export augmented not replaced; v0.2.0 contract planned |
| C-1 | Export Delivery Implementation | ✅ Complete | Producer-side delivery layer: `DeliveryEvent` schema, delivery event materialization, Delta Share prep layer, `--delivery-dir` integration; 613 total tests |
| C-2 | Runtime Integration Validation | 🔲 Not Started | End-to-end delivery validation: share query, event row, manifest integrity, payload fetch |
| D-0 | Multi-Domain Framework | 🔲 Not Started | Per-domain prompt routing; domain registry; multi-domain classification and routing |
| D-1 | CISA Advisory Domain | 🔲 Not Started | CISA advisory schema, extraction, classification, routing; activate `security_ops` label |
| D-2 | Incident Report Domain | 🔲 Not Started | Incident report schema, extraction, classification, routing; activate `incident_management` label |
| E-0 | Human Review and Reprocessing | 🔲 Not Started | Human review queue for quarantined records; reprocessing-on-failure path |
| E-1 | Environment Separation | 🔲 Not Started | Dev/staging/prod Databricks environment structure; environment-aware configuration |
| E-2 | Governance Monitoring | 🔲 Not Started | Pipeline health views; quality trend artifacts; schema drift detection |

---

## Phase C — Live Handoff Integration and Export Delivery

**Status**: C-0 design complete. C-1 complete. C-2 not started.

**Goal**: Move beyond file-only export preparation to a real, validated delivery slice connecting Gold exports to Bedrock CaseOps consumption. V1 B-phases established and hardened the export boundary — contract, validator, materialization, bundle, and integrity validation. V2-C executes across that boundary using a selected delivery protocol.

**What is different from V1**: V1 B-phases prepared the handoff (file export to a Unity Catalog Volume path). V2-C delivers that handoff to a live Bedrock consumer. This is the first point in the project where output leaves this repo's infrastructure boundary and arrives at a Bedrock CaseOps consumer.

**Scope boundary**: V2-C delivers from this repo to a Bedrock consumer endpoint. It does not implement retrieval indexes, vector search, RAG, agent reasoning, or escalation logic — those remain Bedrock CaseOps. The furthest this repo reaches toward Bedrock is Delta Share provisioning.

**C-0 decision (final)**: Delta Sharing is the selected primary delivery mechanism. The V1 file export path is **augmented, not replaced**. A `delivery_events` Delta table is added as the per-batch notification record. The contract version bumps from v0.1.0 to v0.2.0 (minor, additive). See [`docs/live-handoff-design.md`](./live-handoff-design.md) for the full design record.

---

### Phase C-0 — Integration Delivery Mechanism Design

**Status**: Design complete.

**Goal**: Evaluate and select the V2 export delivery mechanism. Define the enhanced interface contract that supports live delivery. Document the integration architecture and consumer-side prerequisites.

**Decisions made**:
- **Primary delivery mechanism**: Delta Sharing (`caseops_handoff` share; shares `caseops.gold.ai_ready_assets`)
- **Secondary delivery artifact**: `caseops.gold.delivery_events` Delta table (per-batch notification log)
- **Rejected**: Structured API push (tight coupling, not Databricks-native, imports consumer concerns)
- **File export path**: Retained and augmented — not replaced
- **Contract version**: v0.1.0 → v0.2.0 (minor bump; three new optional `provenance` fields)
- **Runtime boundary**: Delta Share provisioning is this repo's V2 boundary; consumption is Bedrock CaseOps

**Deliverables**:
- `docs/live-handoff-design.md` — complete C-0 design record (purpose, V1 baseline, option comparison, recommendation, boundary, contract versioning, C-1 target, C-2 target, non-goals, open questions)
- `PROJECT_SPEC.md` — Phase C subphase entries updated with C-0 decisions
- `ARCHITECTURE.md` — V2 delivery mechanism section added; runtime boundary table added
- `docs/roadmap.md` — C-0/C-1/C-2 entries updated; Phase C status updated
- `docs/data-contracts.md` — V2 contract versioning note added

---

### Phase C-1 — Export Delivery Implementation

**Status**: Complete (April 2026).

**Goal**: Implement the upstream producer-side delivery augmentation. First bounded, testable delivery slice — implementing the producer side of the Delta Sharing delivery channel chosen in C-0. The V1 file export path is preserved and augmented, not replaced.

**What C-1 delivers (implemented)**:

| Deliverable | Path | Status |
|---|---|---|
| Delivery event schema | `src/schemas/delivery_event.py` | ✅ New C-1 |
| Delivery event materialization | `src/pipelines/delivery_events.py` | ✅ New C-1 |
| Delta Share prep layer | `src/pipelines/delta_share_handoff.py` | ✅ New C-1 |
| v0.2.0 provenance fields | `src/schemas/gold_schema.py` (3 new optional fields) | ✅ Updated C-1 |
| Pipeline delivery integration | `src/pipelines/classify_gold.py` (`--delivery-dir`) | ✅ Updated C-1 |
| Delivery event fixture | `examples/expected_delivery_event.json` | ✅ New C-1 |
| Delivery event tests | `tests/test_delivery_events.py` | ✅ New C-1 |
| Delta Share handoff tests | `tests/test_delta_share_handoff.py` | ✅ New C-1 |

**Total test count**: 613 (427 V1 + 186 C-1 across delivery event and Delta Share handoff modules).

**C-1 implementation stance**:
- All C-1 delivery events carry `status = 'prepared'` — producer-side preparation is complete
- No live Unity Catalog provisioning has been executed — that is manual or C-2 automation
- No Delta Sharing SDK calls, no credentials, no real share created
- SQL DDL templates and share configuration are documented in `DeltaShareConfig` and `SharePreparationManifest`
- Runtime end-to-end validation (share query, consumer receipt, payload conformance) is Phase C-2

**What C-1 does NOT deliver** (deferred to C-2 or later):
- Live Unity Catalog Delta Share provisioning
- Bedrock CaseOps consumer implementation
- End-to-end delivery receipt confirmation
- Retrieval index, vector search, or agent logic

---

### Phase C-2 — Runtime Integration Validation

**Status**: Complete (producer-side validation layer). Live workspace validation pending manual share provisioning.

**Goal**: Implement a bounded delivery-layer validation and observability layer for the C-1 delivery artifacts. Provide explicit, honest integration health signals. Define the runbook for live workspace validation.

**What C-2 delivered**:
- `src/schemas/delivery_validation.py` — `DeliveryValidationResult` schema with 4-state status vocabulary (`validated`, `partially_validated`, `not_provisioned`, `failed`), scope vocabulary, workspace mode vocabulary, and 15 check name constants
- `src/pipelines/delivery_validation.py` — 15 named check functions + `validate_delivery_layer()` main entry point. Locally executable, credential-free, no Databricks workspace required
- `examples/expected_delivery_validation_result.json` — Reference C-2 validation result fixture
- `docs/delivery-runtime-validation.md` — C-2 design record, check catalogue, and personal Databricks runtime validation runbook
- `tests/test_delivery_validation.py` — 134 focused tests. 747 total tests pass
- Honest default: `status = 'not_provisioned'` for local runs (share designed, not yet provisioned in Unity Catalog)

**What C-2 does NOT deliver** (honest scope boundary):
- Live Delta Share provisioning (requires Databricks workspace with CREATE SHARE privilege)
- `validated` status without actual workspace evidence (evidence_sufficiency check enforces this)
- Bedrock CaseOps consumer simulation
- Any live API calls, SDK calls, or credentials

**Remaining for runtime `validated` status**:
- Execute `setup_sql` from the share manifest in a Databricks SQL notebook
- Create the `delivery_events` table using the `delivery_events_ddl` from the manifest
- Run the C-2 validation queries from `c2_validation_queries` in the manifest
- Re-run `validate_delivery_layer(..., workspace_mode='personal_databricks')` to capture `validated` status

See `docs/delivery-runtime-validation.md` § 7 for the step-by-step runbook.

---

## Phase D — Multi-Domain Pipeline Expansion

**Status**: Not started.

**Goal**: Extend the pipeline beyond FDA warning letters. V1 is single-domain by design. V2-D adds CISA cybersecurity advisories and incident reports — both have draft schemas in `docs/data-contracts.md` § 3 and routing labels already defined in `ARCHITECTURE.md` and `docs/data-contracts.md` § 4.

**Domain expansion priority**: CISA advisories are the first V2-D domain because:
- Draft extraction schema exists in `docs/data-contracts.md` § 3 (CISA Advisory Fields)
- `cisa_advisory` classification label and `security_ops` routing label are already in the defined taxonomy
- Public CISA advisory PDFs are freely accessible for non-commercial use

**Scope boundary**: Domain expansion adds extraction schemas, prompts, classification labels, and routing labels to this repo. It does not add retrieval indexes, vector stores, or agent workflows — those are Bedrock CaseOps concerns.

---

### Phase D-0 — Multi-Domain Framework

**Status**: Not started.

**Goal**: Extend the extraction, classification, and routing framework to support multiple document domains in a single pipeline run. Includes: domain registry, per-domain prompt selection, per-domain schema validation, and multi-domain routing table.

---

### Phase D-1 — CISA Advisory Domain

**Status**: Not started.

**Goal**: Implement the CISA advisory domain end-to-end: extraction schema (from `docs/data-contracts.md` § 3 draft), `ai_extract` prompt template, `cisa_advisory` classification label, `security_ops` routing, and evaluation pass. This activates the `security_ops` routing label for the first time.

---

### Phase D-2 — Incident Report Domain

**Status**: Not started.

**Goal**: Implement the incident report domain end-to-end: extraction schema (from `docs/data-contracts.md` § 3 draft), `ai_extract` prompt template, `incident_report` classification label, `incident_management` routing, and evaluation pass. This activates the `incident_management` routing label for the first time.

---

## Phase E — Enterprise Operational Hardening

**Status**: Not started.

**Goal**: Add selected operational hardening that makes the pipeline more robust for non-trivial batch operations: structured human review for quarantined and low-confidence records, environment separation for safe multi-environment iteration, and governance monitoring for pipeline health visibility.

**Scope constraint**: E-phase hardening is upstream only. This repo does not own human case management tooling, production orchestration, or downstream operational dashboards.

---

### Phase E-0 — Human Review and Reprocessing

**Status**: Not started.

**Goal**: Design and implement a structured human review queue for quarantined or low-confidence records. Define the reprocessing path — how a reviewed record re-enters the pipeline with an updated classification or routing decision.

---

### Phase E-1 — Environment Separation

**Status**: Not started.

**Goal**: Define and implement dev/staging/prod environment separation for the Databricks deployment. Includes Unity Catalog catalog-level separation, environment-aware configuration, and deployment patterns.

**Scope boundary**: Environment separation design only. No production credentials, no enterprise organizational deployment configuration committed to the repo.

---

### Phase E-2 — Governance Monitoring

**Status**: Not started.

**Goal**: Implement governance monitoring views: pipeline health metrics, batch-level quality trend artifacts, schema drift detection signals, and governance reporting outputs — surfaced as Databricks notebooks or structured export artifacts.

**Scope boundary**: Pipeline health visibility only. Not cross-case analytics, not KPI dashboards, not downstream operational intelligence.

---

## V2 Closeout Criteria

V2 is complete when all of the following are true:

- A live delivery mechanism exists and is validated: Gold export payloads delivered to a Bedrock CaseOps consumer without a manual copy step (Phase C)
- CISA advisories and incident reports processable end-to-end through the pipeline alongside FDA warning letters (Phase D)
- Quarantined and low-confidence records have a defined human review path and reprocessing mechanism (Phase E-0)
- Pipeline deployable in at least two distinct Databricks environments without configuration collision (Phase E-1)
- The Databricks / Bedrock boundary remains explicit throughout — no retrieval, RAG, agent, or escalation logic enters this repo
- No production credentials or enterprise configuration committed at any V2 phase
