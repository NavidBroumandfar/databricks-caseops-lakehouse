# CURSOR_CONTEXT.md — Agent Orientation Guide

> This file is a quick orientation reference for AI agents working in this repository.
> It is NOT the source of truth for scope or technical design. Read this file last, after completing the required reading order below.

---

## Required Reading Order

When starting any task in this repository, always read files in this exact order:

1. [`README.md`](../README.md) — Project identity and overview
2. [`PROJECT_SPEC.md`](../PROJECT_SPEC.md) — Scope, non-goals, roadmap **(scope source of truth)**
3. [`ARCHITECTURE.md`](../ARCHITECTURE.md) — Technical design **(technical source of truth)**
4. [`docs/CURSOR_CONTEXT.md`](./CURSOR_CONTEXT.md) — This file (quick orientation only)

---

## Authority Rules

| File | Authority |
|---|---|
| `PROJECT_SPEC.md` | **Scope and roadmap source of truth.** When in doubt about what is in or out of scope, this file governs. |
| `ARCHITECTURE.md` | **Technical design source of truth.** When in doubt about schema, layer design, or system contracts, this file governs. |
| `docs/CURSOR_CONTEXT.md` | **Orientation only.** Does not override the above files on any matter. |

---

## Project Identity (Quick Summary)

**Databricks CaseOps Lakehouse** is a governed, Databricks-native document intelligence pipeline.

- **Input**: unstructured enterprise documents (PDF, DOCX, TXT) stored in Unity Catalog Volumes
- **Output**: structured, schema-validated, classified, AI-ready records in a Gold Delta table
- **Downstream**: Bedrock CaseOps retrieval and agent workflows
- **Platform**: Databricks (Unity Catalog, AI Functions, MLflow)

---

## What This Repo Is NOT

- Not a generic Databricks demo
- Not a chatbot or conversational AI system
- Not a retrieval or RAG pipeline (that is Bedrock CaseOps)
- Not a frontend project
- Not a collection of ML experiments
- Not a cross-case analytics or trend reporting platform
- Not a KPI dashboard or historical operational intelligence system
- Not a downstream agent reasoning or orchestration runtime (that is Bedrock CaseOps)
- Not a full analytics backbone — it is the governed upstream document intelligence and AI-ready asset preparation layer

**Positioning guard**: When summarizing this repo, do not use language that implies it is an analytics platform, a KPI system, or a mature intelligence backbone. It structures and prepares documents for downstream use. Bedrock CaseOps handles retrieval, reasoning, and decisioning.

---

## Current Status

**V1 IS COMPLETE.** Do not treat any V1 milestone as pending. Do not rewrite V1 history.

**V2 HAS STARTED. PHASES C-1 AND C-2 ARE COMPLETE.** C-0 (design), C-1 (producer-side implementation), and C-2 (runtime validation layer) are complete. C-2 added a bounded 15-check delivery-layer validation layer with honest `not_provisioned` / `partially_validated` / `validated` / `failed` status vocabulary. The producer-side validation is implemented; live Delta Share provisioning in a Databricks workspace and the `validated` status are achievable after running the C-2 runbook in `docs/delivery-runtime-validation.md`. Do not claim live Delta Sharing is provisioned — it is `not_provisioned` by default. Do not reopen V1.

**Phases A-0 through B-6, C-1, and C-2 are complete**, and the final V1 MLflow live-workspace evaluation checkpoint has been executed. The repo has:
- A validated pipeline (A-0 through A-4.1) with A-3B personal Databricks bootstrap and A-4.1 runtime inspection
- Real Databricks MLflow experiments populated for all four evaluation stages: bronze parse quality, silver extraction quality, gold classification quality, pipeline traceability — logged April 2026 via `CASEOPS_MLFLOW_EXPERIMENT_ROOT`-qualified paths using `src/evaluation/mlflow_experiment_paths.py`
- An explicit Gold → Bedrock handoff contract (B-0), repo-enforced contract validator (B-1), contract-enforced export materialization path (B-2), clean export/handoff module boundary (B-3), structured handoff outcome observability (B-4), a single reviewable batch handoff bundle/manifest (B-5), and a local-safe bundle integrity validation layer (B-6)
- A Delta Sharing-oriented producer-side delivery augmentation (C-1) with delivery events and share preparation manifests
- A bounded 15-check delivery-layer runtime validation layer (C-2) with honest `not_provisioned` / `partially_validated` / `validated` / `failed` status vocabulary
- 747 tests passing across all pipeline stages, contract enforcement layers, delivery event materialization, Delta Share preparation, and delivery-layer runtime validation

**V2 has started. Phase C-1 (export delivery implementation) is complete. Phase C-2 (runtime validation layer) is complete.** Live Delta Share provisioning in a Databricks workspace completes the runtime `validated` state — see `docs/delivery-runtime-validation.md`.

Key V1 completion boundaries:
- No live Bedrock integration exists — downstream integration is V2+
- No enterprise deployment or production credentials
- Single domain only: FDA warning letters
- Personal Databricks workspace was used for bootstrap (A-3B) and MLflow closeout — not an enterprise or production environment

**A-0 through A-3** (local-safe implementation) are complete:
- A-0: Repo foundation and documentation
- A-1: Bronze ingestion and parsing pipeline (local-safe)
- A-2: Silver extraction and validation (local-safe)
- A-3: Gold classification and routing (local-safe)

**Phase A-3B — Personal Databricks Bootstrap Consolidation** is complete.
This phase captured a validated personal-workspace Databricks SQL execution pass (Bronze → Silver → Gold)
using real AI Functions (`ai_parse_document`, `ai_extract`, `ai_classify`) and public FDA sample documents.
Key outcome: 4 documents processed end-to-end, full document_id lineage confirmed, 3 export-ready,
1 quarantined. Known implementation detail: `classification_confidence` is NULL in the bootstrap SQL path.
See [`docs/databricks-bootstrap.md`](./databricks-bootstrap.md) for full details.

**Phase A-4 — Evaluation and Observability** is complete.
Deliverables: evaluation runners for all four quality dimensions (Bronze, Silver, Gold, Traceability),
a full-pipeline orchestrator, structured report models, JSON + text report writer, 84 focused tests,
and explicit handling of A-3B bootstrap tensions (null confidence, placeholder run IDs,
bootstrap vs. target-state contract distinction).

**Phase A-4.1 — Runtime Validation Checkpoint** is complete.
A direct runtime inspection of the A-3B bootstrap tables confirmed: scalar `classification_confidence`
is not available from the validated `ai_classify` bootstrap SQL path (no confidence key present in the
response variant at any tested path); Gold routing behavior matched expectations (3 export-ready,
1 quarantined). This is a validation sub-step, not a separate major phase.
See [`docs/databricks-bootstrap.md`](./databricks-bootstrap.md) § A-4.1 for full findings.

**Phase B-0 — Bedrock Handoff Contract Preparation** is complete.
B-0 established the explicit Gold → Bedrock CaseOps interface contract. The single authoritative
contract artifact is `docs/bedrock-handoff-contract.md`. It defines the `export_payload` structure,
required vs optional fields, routing label → Bedrock consumer mapping, `export_ready` / `quarantine`
semantics, V1 delivery mechanism (JSON file export to Unity Catalog Volume), and known current
limitations. `ARCHITECTURE.md`, `docs/data-contracts.md`, and `docs/roadmap.md` are updated for
consistency. B-0 is a contract-hardening phase only — no live AWS/Bedrock integration was delivered.
Live integration is Phase B proper (not started).

**Phase B-1 — Handoff Contract Materialization and Validation** is complete.
B-1 converted the B-0 documentation contract into repo-enforced behavior. Key deliverables:
`src/schemas/bedrock_contract.py` (validates export payloads against B-0 §4 requirements),
`examples/contract_valid_fda_export_payload.json` (contract-valid V1 FDA fixture),
`tests/test_bedrock_contract_validation.py` (53 tests: valid, invalid, quarantine, bootstrap-path).
Also fixed: `classification_confidence` in `gold_schema.py` is now `Optional[float]` (per B-0 §4.3),
and `classify_gold.py` routing logic correctly handles null confidence (per B-0 §6).
No live Bedrock/AWS integration was introduced.

**Phase B-2 — Contract-Enforced Export Materialization** is complete.
B-2 makes the pipeline obey the B-1 contract during real export materialization. Key deliverables:
`src/pipelines/classify_gold.py` updated (B-1 contract validation gates every export write;
invalid payloads blocked; quarantine shape validated; Gold record written once with final state),
`tests/test_b2_export_materialization.py` (18 tests covering valid write, contract block, quarantine
separation, deterministic path, error surfacing, no-SDK guard),
`examples/invalid_export_payload_missing_fields.json` and `examples/quarantine_gold_record.json`
(B-2 fixtures). No live Bedrock/AWS integration was introduced.

**Phase B-3 — Export Packaging Refactor and Handoff Service Boundary** is complete.
B-3 extracts export/handoff materialization logic from the classification pipeline loop into a
dedicated module (`src/pipelines/export_handoff.py`). Key deliverables: `export_handoff.py`
with `ExportResult`, `compute_export_path`, `write_export_artifact`, and `execute_export`;
`classify_gold.py` simplified to delegate all export packaging to `execute_export`;
`tests/test_b3_export_handoff.py` (28 focused tests). B-2 behavior is preserved exactly.
183 tests pass total. No live Bedrock/AWS integration was introduced.

**Phase B-4 — Export Outcome Observability and Handoff Reporting** is complete.
B-4 makes Gold → Bedrock handoff outcomes operationally visible and reviewable at batch level.
Key deliverables: `src/pipelines/handoff_report.py` with explicit outcome categories
(`exported`, `quarantined`, `contract_blocked`, `skipped_not_export_ready`), reason codes
(`none`, `routing_quarantine`, `contract_validation_failed`, `export_not_attempted`),
`HandoffBatchReport` dataclass, `build_handoff_batch_report`, and `write_handoff_report`;
`classify_gold.py` updated — per-record `outcome_category` + `outcome_reason` in summaries,
`report_dir` parameter for batch report output; `tests/test_b4_handoff_report.py` (68 tests).
251 tests pass total. No live Bedrock/AWS integration was introduced.

**Phase B-5 — Handoff Batch Manifest and Review Bundle** is complete.
B-5 packages the full Gold/export batch into a single, coherent, reviewable batch handoff
bundle. Key deliverables: `src/pipelines/handoff_bundle.py` with `RecordArtifactRef`,
`HandoffBatchManifest`, `build_handoff_batch_manifest`, `compute_bundle_path`,
`format_bundle_text`, `write_handoff_bundle`; `classify_gold.py` updated — `bundle_dir`
parameter and `--bundle-dir` CLI arg; bundle references all per-record artifact paths
(Gold + export) by outcome category and links B-4 report artifacts when available;
`tests/test_b5_handoff_bundle.py` (84 tests). 335 tests pass total. No live Bedrock/AWS
integration was introduced.

**Phase B-6 — Handoff Bundle Integrity and Consistency Validation** is complete.
B-6 proves the B-5 bundle is internally trustworthy and review-safe. Key deliverables:
`src/pipelines/handoff_bundle_validation.py` with 24 explicit integrity checks across
structural correctness, count consistency, reference integrity, identifier uniqueness, and
filesystem path existence; `validate_handoff_bundle` (file-based entry point),
`validate_handoff_bundle_from_manifest` (in-memory), `BundleValidationResult` with full
check detail; `tests/test_b6_bundle_validation.py` (92 tests). 427 tests pass at B-6 closeout.

**Phase C-1 — Export Delivery Implementation** is complete.
C-1 implements the upstream producer-side delivery augmentation chosen in C-0. Key deliverables:
`src/schemas/delivery_event.py` (`DeliveryEvent` Pydantic schema; `DELIVERY_SCHEMA_VERSION = 'v0.2.0'`);
`src/pipelines/delivery_events.py` (build, write, load delivery event artifacts);
`src/pipelines/delta_share_handoff.py` (`DeltaShareConfig`, `SharePreparationManifest`, SQL DDL
templates for Unity Catalog, C-2 validation query set);
`src/schemas/gold_schema.py` updated with `SCHEMA_VERSION_V2 = 'v0.2.0'` and three new optional
`ExportProvenance` fields (`delivery_mechanism`, `delta_share_name`, `delivery_event_id`);
`src/pipelines/classify_gold.py` updated with `--delivery-dir` flag (activates C-1 path);
`examples/expected_delivery_event.json`; `tests/test_delivery_events.py` (88 tests);
`tests/test_delta_share_handoff.py` (67 tests). 613 tests at C-1 closeout.
All C-1 delivery events carry `status = 'prepared'` — producer-side is complete; runtime validation
is C-2. No live Unity Catalog provisioning, no Bedrock SDK, no real Delta Share created.

**Phase C-2 — Runtime Integration Validation** is complete (producer-side validation layer).
C-2 adds a bounded, credential-free, locally executable delivery-layer validation layer. Key deliverables:
`src/schemas/delivery_validation.py` (`DeliveryValidationResult`, `CheckResult`; status/scope/workspace
vocabulary with 4 statuses, 2 scopes, 2 workspace modes, 15 check name constants);
`src/pipelines/delivery_validation.py` (15 named check functions + `validate_delivery_layer()` entry
point + `format_validation_result_text()` + `write_validation_result()` + `load_validation_result()`);
`examples/expected_delivery_validation_result.json` (C-2 reference fixture; status `not_provisioned`);
`docs/delivery-runtime-validation.md` (C-2 design record, check catalogue, and runtime validation runbook);
`tests/test_delivery_validation.py` (134 tests). **747 tests pass total.**
Default local run produces `status = 'not_provisioned'` (honest baseline). `validated` requires
`workspace_mode='personal_databricks'` and execution of share setup SQL in a Databricks workspace.

The module boundary (through C-2) is:
  `classify_gold.py`              → assembles GoldRecord → calls `execute_export` → derives outcome → writes Gold artifact → builds B-5 bundle → writes C-1 delivery event
  `export_handoff.py`             → validates contract → writes export artifact → returns `ExportResult`
  `handoff_report.py`             → derives outcome categories → aggregates batch report → writes report artifacts
  `handoff_bundle.py`             → packages batch into manifest/review bundle → writes bundle artifacts (JSON + text)
  `handoff_bundle_validation.py`  → validates the bundle is internally consistent and trustworthy
  `delivery_events.py`            → builds DeliveryEvent from summaries → writes event artifacts
  `delta_share_handoff.py`        → defines share config → generates SQL templates → writes share prep manifest
  `delivery_validation.py`        → runs 15 checks on C-1 delivery artifacts → produces DeliveryValidationResult

See [`PROJECT_SPEC.md`](../PROJECT_SPEC.md) for the full roadmap and phase status.

## Known A-3B / A-4 Tensions (Resolved in A-4)

| Tension | Resolution |
|---|---|
| `classification_confidence` NULL in bootstrap Gold | Evaluator surfaces `confidence_null_rate`, skips confidence thresholds when null, adds observations |
| `pipeline_run_id = 'bootstrap_sql_v1'` (placeholder) | Traceability evaluator reports `placeholder_run_id_count`; not treated as broken provenance |
| `_smoke` table naming in Databricks | Documented as bootstrap-specific; local evaluation artifacts use target-state naming |
| `export_ready` threshold includes `confidence >= 0.7` | Target-state only; bootstrap path uses rule-based routing without confidence threshold |

---

## Key Conventions

- Primary language: **Python**
- Secondary language: **SQL**
- Config: **YAML**
- No Scala
- No UI code
- All schemas defined in `src/schemas/`
- All pipeline logic in `src/pipelines/`
- All evaluation logic in `src/evaluation/`
- `docs/prompts/` is excluded from version control (see `.gitignore`)
- No credentials, tokens, workspace URLs, or personal config values anywhere in the repo
