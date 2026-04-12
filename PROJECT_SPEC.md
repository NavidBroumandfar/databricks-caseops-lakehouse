# PROJECT_SPEC.md — Scope and Roadmap Source of Truth

> This file defines the project objective, scope boundaries, default architecture flow, and phased delivery plan.
> When in conflict with any other document, this file governs scope decisions.

---

## Project Objective

Build a governed, Databricks-native document intelligence pipeline that transforms unstructured enterprise documents into structured, traceable, evaluation-ready AI assets — suitable for downstream consumption by Bedrock retrieval systems and agent workflows.

The pipeline must be:

- **Governed**: all data and metadata managed via Unity Catalog
- **Traceable**: every parsing and extraction decision recorded and replayable
- **Evaluated**: extraction quality and schema validity measured at every layer
- **Platform-native**: built on Databricks AI Functions, not external APIs where avoidable
- **Production-aware**: designed to run on real document corpora, not synthetic demos

---

## Explicit Non-Goals

The following are out of scope for this project at all phases:

| Out of Scope | Reason |
|---|---|
| Frontend / UI of any kind | Outside platform boundary |
| Conversational agent or chatbot | Owned by Bedrock CaseOps |
| Retrieval-augmented generation (RAG) | Owned by Bedrock CaseOps |
| Agent reasoning, orchestration, or escalation workflows | Owned by Bedrock CaseOps |
| Cross-case analytics or trend reporting | Not the purpose of this repo; downstream concern |
| KPI dashboards or historical operational intelligence | Not the purpose of this repo; downstream concern |
| Generic ML or data science notebooks | Not the project identity |
| Real-time streaming ingestion | V1 targets batch; streaming is a future evolution |
| Fine-tuned model training | Evaluation and prompt engineering only in V1 |
| Scala code | Python and SQL are the sole implementation languages |
| External LLM API calls (OpenAI, Anthropic) | Databricks AI Functions are preferred; external calls require explicit justification |
| Production secrets or credentials | Never included in this repo |

**Scope positioning note**: This repo is the governed upstream document intelligence and AI-ready asset preparation layer. It is not an analytics backbone, a KPI platform, or a downstream decisioning system. Those roles belong to Bedrock CaseOps or to future platform evolution well beyond V1.

---

## Default Architecture Flow

```
Raw Documents (PDF, DOCX, TXT)
        │
        ▼
[A] Ingest → Unity Catalog Volume (raw)
        │
        ▼
[B] Parse → Bronze Table
        │   ai_parse_document, source metadata, parse timestamp, document hash
        │
        ▼
[C] Extract → Silver Table
        │   ai_extract, schema-validated fields, extraction confidence
        │
        ▼
[D] Classify + Route → Gold Table
        │   ai_classify, document type label, routing label, AI-ready payload
        │
        ▼
[E] Export → Downstream Bedrock handoff
            Structured JSON or Delta table slice per document class
```

---

## Default Document Domain

The pipeline is designed for document-heavy operational and regulatory workflows. The table below lists the intended long-term domain coverage. **V1 implements a single domain: FDA warning letters.** All other document types are planned extensions (V2+).

| Document Type | Source Domain | V1 Status |
|---|---|---|
| FDA warning letters | Regulatory / compliance | **V1 — single executable domain** |
| CISA cybersecurity advisories | Security operations | Planned (V2+) |
| Incident reports | IT / quality operations | Planned (V2+) |
| Standard operating procedures | Operations / quality | Planned (V2+) |
| Quality audit records | Quality management | Planned (V2+) |
| Technical support and case review records | Enterprise support | Planned (V2+) |

These document types are chosen because they:

- Contain operationally significant, extractable structured fields
- Are representative of real enterprise document operations
- Are publicly available in redacted or sample form for demonstration
- Are document-heavy enough to justify a pipeline rather than manual review

FDA warning letters are the V1 implementation domain because they have a well-defined, publicly available structure with clearly extractable fields, and published samples are freely accessible for non-commercial use.

---

## V1 Scope

V1 delivers a functional, evaluated, single-domain pipeline end-to-end. Specifically:

### In Scope for V1

- Unity Catalog catalog, schema, and Volume configuration (YAML-defined)
- Bronze ingestion: raw document ingest with source metadata and parse provenance
- Silver extraction: structured field extraction via `ai_extract` with schema validation
- Gold classification: document type classification and routing label assignment via `ai_classify`
- MLflow evaluation run for Bronze parse quality and Silver extraction quality
- Export schema for downstream Bedrock handoff (JSON structure, not live integration)
- Data contracts documented for all three layers
- Evaluation plan documented and partially implemented

### Not in V1

- Multi-domain pipelines (V2+)
- Live Bedrock integration endpoint (V2+)
- Human-in-the-loop review workflow (V2+)
- Automated reprocessing on failure (V2+)
- Streaming or near-real-time ingestion (V3+)
- Cross-case analytics, KPI reporting, or trend dashboards (not a V1 concern; not a repo concern)
- Downstream agent reasoning, escalation logic, or case-support orchestration (Bedrock CaseOps, not this repo)

---

## Phased Roadmap

### Phase A-0 — Repo Foundation
**Goal**: Establish repo identity, documentation, and project structure.

Deliverables:
- `README.md`, `PROJECT_SPEC.md`, `ARCHITECTURE.md`
- `docs/CURSOR_CONTEXT.md`, `docs/roadmap.md`, `docs/data-contracts.md`, `docs/evaluation-plan.md`
- Directory scaffold: `src/`, `notebooks/`, `examples/`

### Phase A-1 — Bronze: Ingest and Parse
**Goal**: Build the ingestion and parsing pipeline from raw documents to Bronze Delta table.

Deliverables:
- Unity Catalog config YAML (catalog, schema, Volume)
- Python ingestion script with source metadata extraction
- `ai_parse_document` integration and Bronze table writer
- Bronze schema definition (Pydantic + JSON Schema)
- Separate Bronze evaluation script that computes parse quality metrics (success rate, character
  yield) against Bronze artifacts or table snapshots. The pipeline run itself logs only
  operational metadata (batch size, document count, pipeline run ID); parse quality metrics
  are produced by an independent evaluation run in `src/evaluation/eval_bronze.py`.

### Phase A-2 — Silver: Extraction and Validation
**Goal**: Extract structured fields from parsed text into a validated Silver layer.

Deliverables:
- `ai_extract` prompt templates per document class
- Silver schema definitions with field-level types and constraints
- Pydantic validation pass on extracted records
- Extraction confidence scoring and schema conformance metrics
- MLflow evaluation: field coverage, schema validity rate, extraction drift

### Phase A-3 — Gold: Classification and Routing
**Goal**: Classify documents and generate routing labels and AI-ready downstream assets.

Deliverables:
- `ai_classify` prompt templates with defined label taxonomy
- Gold table schema with classification, routing label, and export payload fields
- Routing logic (rule-based in V1, model-based in V2)
- Gold export format specification for Bedrock handoff
- Classification evaluation: label precision, coverage, disagreement rate

**Status**: Complete (local-safe implementation).

### Phase A-3B — Personal Databricks Bootstrap Consolidation
**Goal**: Capture and consolidate a validated personal-workspace end-to-end SQL execution pass using real Databricks AI Functions and public FDA sample documents. This is a bridging phase between local-safe A-3 and the formal evaluation phase A-4.

**Scope**: Non-production. Personal Databricks Free Edition workspace. Public FDA warning letter PDFs only. No enterprise deployment, no service principals, no external locations, no MLflow automation.

Deliverables:
- Bootstrap SQL assets under `notebooks/bootstrap/` reflecting the validated Bronze → Silver → Gold SQL flow
- `config/databricks.resources.example.yml` documenting the Unity Catalog resource layout
- `docs/databricks-bootstrap.md` recording validated results, constraints, and next steps

**Validated bootstrap results**:
- 4 documents processed end-to-end from raw PDF to Gold routing record
- Full `document_id`-based lineage confirmed across all three layers
- 3 records export-ready; 1 record quarantined (correct governance behavior)
- `classification_confidence` stored as NULL in this bootstrap implementation (documented; A-4 evaluation layer handles this explicitly)

**Status**: Complete.

### Phase A-4 — Evaluation and Observability Layer
**Goal**: Formalize evaluation across all layers as explicit, rerunnable evaluation passes with structured artifact output and optional MLflow logging.

**Status**: Complete.

Deliverables:
- `src/evaluation/eval_bronze.py` — Bronze parse quality evaluation ✅
- `src/evaluation/eval_silver.py` — Silver extraction quality evaluation ✅
- `src/evaluation/eval_gold.py` — Gold classification evaluation (null-confidence safe) ✅ Updated A-4
- `src/evaluation/eval_traceability.py` — Cross-layer traceability evaluation ✅ New A-4
- `src/evaluation/run_evaluation.py` — Full-pipeline evaluation orchestrator ✅ New A-4
- `src/evaluation/report_models.py` — Structured evaluation report dataclasses ✅ New A-4
- `src/evaluation/report_writer.py` — JSON + text report writer ✅ New A-4
- `tests/` — 84 focused tests covering all evaluators ✅ New A-4
- `examples/evaluation/README.md` — Usage documentation ✅ New A-4
- Honest resolution of A-3B tensions: null confidence, placeholder run IDs, bootstrap vs. target-state distinction ✅

**A-4 scope boundaries** (what this phase is and is not):
- Is: first formal evaluation hardening; explicit metrics; structured artifact output; honest accounting of bootstrap-origin records
- Is not: full MLflow automation; production orchestration; live Databricks experiment population; confidence extraction fix

### Phase A-4.1 — Runtime Validation Checkpoint
**Goal**: Direct runtime inspection of the A-3B bootstrap tables in the personal Databricks workspace to confirm evaluator assumptions and close the loop between platform behavior and repo documentation.

**Status**: Complete (documented validation sub-step following A-4).

Key findings:
- `ai_classify` output variant confirmed to contain only `error_message` and `response` — no scalar confidence key at any tested extraction path
- Gold routing confirmed: 3 records `fda_warning_letter` → `regulatory_review` (`export_ready = true`); 1 record `unknown` → `quarantine` (`export_ready = false`)
- Null-confidence handling in `eval_gold.py` confirmed correct for this bootstrap path

This is not a separate major phase. It is a runtime validation checkpoint that closes the A-4 evaluation loop. Full findings are recorded in `docs/databricks-bootstrap.md` § A-4.1 and `ARCHITECTURE.md` § A-4.1 Runtime Inspection Findings.

### Phase B-0 — Bedrock Handoff Contract Preparation
**Goal**: Establish the explicit, testable, versioned Gold → Bedrock CaseOps interface contract before any live integration work begins. This phase hardens the contract boundary so that Phase B (live Bedrock integration) can proceed without ambiguity about what the upstream pipeline produces and what the downstream system must consume.

**Status**: Complete.

### Phase B-1 — Handoff Contract Materialization and Validation
**Goal**: Convert the B-0 documentation contract into repo-enforced schema behavior, fixtures, and tests. Make the Gold → Bedrock handoff contract enforceable in code, not just documented.

**Status**: Complete.

Deliverables:
- `src/schemas/bedrock_contract.py` — explicit contract validator for Gold export payloads
- `examples/contract_valid_fda_export_payload.json` — contract-valid V1 FDA export payload fixture
- `tests/test_bedrock_contract_validation.py` — 53 focused tests (valid, invalid, quarantine, bootstrap-path)
- `src/schemas/gold_schema.py` updated — `classification_confidence` made `Optional[float]` per B-0 §4.3
- `src/pipelines/classify_gold.py` updated — routing and readiness logic handles `None` confidence per B-0 §6

**Scope boundary**: B-1 introduces no AWS/Bedrock SDK, no live integration, no S3, and no agent workflows. The repo remains the upstream-only governed document intelligence layer.

**What B-0 delivers:**
- `docs/bedrock-handoff-contract.md` — the single authoritative contract artifact for the Gold → Bedrock handoff
- Explicit required vs optional payload field differentiation in the contract and in `docs/data-contracts.md`
- Routing label → Bedrock consumer/workflow mapping defined and stable for V1
- `export_ready`, `quarantine`, and delivery semantics formally defined
- V1 delivery mechanism specified as structured JSON file export to a Unity Catalog Volume path
- Versioning expectations established (v0.1.0 contract, semantic versioning rules)
- Known current limitations documented honestly (null confidence, bootstrap placeholder run IDs, V1-only routing)
- Acceptance criteria defined and verifiable
- `ARCHITECTURE.md`, `docs/data-contracts.md`, and `docs/roadmap.md` updated for consistency

**What B-0 explicitly excludes:**
- AWS credentials, IAM roles, or S3 plumbing
- Bedrock SDK code or API client implementation
- Live ingestion endpoint on the Bedrock side
- Vector index or retrieval configuration
- Agent reasoning or escalation logic
- Multi-domain pipeline execution (V2+ document types)
- Streaming or event-driven delivery
- Human-in-the-loop review tooling
- Delta Sharing configuration
- Production Databricks deployment

### Phase B-2 — Contract-Enforced Export Materialization
**Goal**: Make the real Gold export-writing path obey the B-1 contract during actual materialization. Before any export-ready payload is written as a Bedrock handoff artifact, it is validated against the B-1 contract enforcement layer. Invalid export payloads are explicitly blocked. Quarantine behavior is made explicit and deterministic. Export path semantics are hardened and tested.

**Status**: Complete.

Deliverables:
- `src/pipelines/classify_gold.py` updated — B-1 contract validation gating every export write; quarantine shape validation; Gold record written once with final state
- `tests/test_b2_export_materialization.py` — 18 focused tests covering real materialization behavior (not just isolated validation helpers)
- `examples/invalid_export_payload_missing_fields.json` — B-2 invalid payload fixture
- `examples/quarantine_gold_record.json` — B-2 quarantine record fixture

**Scope boundary**: B-2 strengthens upstream export behavior only. No AWS/Bedrock SDK, no live integration. The repo remains the upstream-only governed document intelligence layer.

### Phase B-3 — Export Packaging Refactor and Handoff Service Boundary
**Goal**: Extract export/handoff materialization behavior from `classify_gold.py` into a dedicated internal module (`src/pipelines/export_handoff.py`), creating a clean service boundary for the export/handoff slice while preserving B-2 behavior exactly.

**Status**: Complete.

Deliverables:
- `src/pipelines/export_handoff.py` — `ExportResult`, `compute_export_path`, `write_export_artifact`, `execute_export`
- `src/pipelines/classify_gold.py` updated — delegates all export/handoff materialization to `execute_export`
- `tests/test_b3_export_handoff.py` — 28 focused tests (path computation, `execute_export` all cases, module boundary, integration)
- `tests/test_b2_export_materialization.py` updated — import updated; no-SDK guard reflects new boundary

**Scope boundary**: B-3 is a structural refactor only. No contract semantic changes, no AWS/Bedrock SDK, no live integration, no new downstream assumptions. The repo remains the upstream-only governed document intelligence layer.

### Phase B-4 — Export Outcome Observability and Handoff Reporting
**Goal**: Make Gold → Bedrock handoff outcomes operationally visible, structured, and reviewable at batch level. Each pipeline run produces a clear handoff outcome summary with explicit outcome categories, reason codes, and batch-level counts.

**Status**: Complete.

Deliverables:
- `src/pipelines/handoff_report.py` — `OUTCOME_*` / `REASON_*` constants, `HandoffBatchReport`, `derive_outcome`, `build_handoff_batch_report`, `write_handoff_report`, `format_handoff_report_text`
- `src/pipelines/classify_gold.py` updated — per-record `outcome_category` + `outcome_reason` in summaries; `report_dir` parameter; batch report written when requested
- `tests/test_b4_handoff_report.py` — 68 focused tests (constants, derivation, batch report, write, integration, module boundary)

**Outcome categories**: `exported`, `quarantined`, `contract_blocked`, `skipped_not_export_ready`

**Reason codes**: `none`, `routing_quarantine`, `contract_validation_failed`, `export_not_attempted`

**Scope boundary**: B-4 is reporting and observability only. No AWS/Bedrock SDK, no live integration, no contract semantic changes. The repo remains the upstream-only governed document intelligence layer.

### Phase B-5 — Handoff Batch Manifest and Review Bundle
**Goal**: Package the Gold → Bedrock export batch outputs into a single, coherent, reviewable batch handoff bundle — a single manifest artifact per pipeline run that links batch metadata, aggregate outcome counts, per-record artifact references (by outcome category), and paths to B-4 report artifacts.

**Status**: Complete.

Deliverables:
- `src/pipelines/handoff_bundle.py` — `MANIFEST_VERSION`, `RecordArtifactRef`, `HandoffBatchManifest`, `build_handoff_batch_manifest`, `compute_bundle_path`, `format_bundle_text`, `write_handoff_bundle`
- `src/pipelines/classify_gold.py` updated — `bundle_dir` parameter; `--bundle-dir` CLI arg; B-5 bundle written when `bundle_dir` is provided; bundle references B-4 report artifacts when `report_dir` was also provided
- `tests/test_b5_handoff_bundle.py` — 84 focused tests (manifest structure, builder, path computation, text formatter, write, integration, module boundary)
- `examples/expected_handoff_batch_manifest.json` — reference manifest fixture

**Design boundary (B-5 vs B-4)**: B-4 produces counts, reason codes, and a batch summary. B-5 packages that batch into a clean manifest with full per-record artifact references — the single artifact a reviewer opens to understand the full state of a batch run.

**Scope boundary**: B-5 is upstream handoff packaging only. No AWS/Bedrock SDK, no live integration, no contract semantic changes. The repo remains the upstream-only governed document intelligence layer.

### Phase B-6 — Handoff Bundle Integrity and Consistency Validation
**Goal**: Prove that the B-5 handoff batch bundle is internally trustworthy and review-safe by implementing a dedicated local-safe validation layer. B-6 checks the bundle for structural correctness, count consistency, reference integrity, identifier uniqueness, and (optionally) filesystem path existence.

**Status**: Complete.

Deliverables:
- `src/pipelines/handoff_bundle_validation.py` — dedicated B-6 validation module with `CHECK_*` constants, `CheckResult`, `BundleValidationResult`, `validate_handoff_bundle` (file-based entry point), `validate_handoff_bundle_from_manifest` (in-memory entry point), `write_validation_result`, `format_validation_result_text`
- `tests/test_b6_bundle_validation.py` — 92 focused tests covering valid bundle, structural failures, count mismatches, reference contradictions, identifier uniqueness, path checks, file-based entry point, module boundary, integration with real pipeline output

**What B-6 validates (24 explicit checks):**
- Structural: manifest_version known; batch_id == pipeline_run_id
- Count consistency: all totals (exported, quarantined, contract_blocked, skipped, eligible, records_processed) match record list lengths; outcome_distribution sums match lists
- Reference consistency: exported records have export paths; non-exported records don't; outcome_category correct per list; quarantined records carry quarantine routing; exported records don't carry quarantine routing
- Identifier uniqueness: no duplicate document_ids or gold_record_ids across all record lists
- Filesystem paths: gold_artifact_paths, export_artifact_paths, and report_artifact paths exist on disk (optional, on by default)

**Scope boundary**: B-6 is upstream bundle validation only. No AWS/Bedrock SDK, no live integration, no contract semantic changes. The manifest remains a derived packaging artifact. B-6 confirms it is coherent with the artifacts it references — not a new source of truth.

**Why B-0 exists before live integration:**
Live integration (Phase B proper) requires both sides of the interface to have a shared, explicit understanding of the contract. Without B-0, the handoff structure is implicit, versionless, and subject to misalignment. B-0 makes the contract explicit, honest about current limitations, and testable before any infrastructure work begins.

**How B-0 preserves the Databricks / Bedrock split:**
B-0 documents what this repo guarantees to produce (Gold `export_payload` at a deterministic path) and what Bedrock CaseOps is responsible for consuming. It does not cross into Bedrock's implementation domain. The governance boundary remains at the materialized export file.

This is the first sub-phase of the broader Phase B (Bedrock Handoff Integration). It does not include live endpoint work — that is Phase B proper.

---

## Success Criteria

**V1 is complete.** All criteria below are met as of the V1 closeout MLflow workspace checkpoint (April 2026).

1. ✅ A real document (e.g., a publicly available FDA warning letter) can be dropped into a Unity Catalog Volume and processed end-to-end to a Gold record without manual intervention — confirmed in personal Databricks workspace during A-3B bootstrap (4 documents, full lineage)
2. ✅ All three layer schemas (Bronze, Silver, Gold) are defined, documented, and validated — Pydantic v2 schemas with contract enforcement (B-1)
3. ✅ An MLflow evaluation run exists with meaningful metrics for at least two pipeline stages — all four stages logged to real Databricks MLflow experiments during V1 closeout checkpoint
4. ✅ The data contract is documented and consistent with the actual schema definitions — `docs/data-contracts.md` and `docs/bedrock-handoff-contract.md` are consistent with all schemas
5. ✅ The downstream export format is specified and could be consumed by a Bedrock retrieval system without modification — B-0 contract, B-1 enforced validator, B-2 materialization gate; no live Bedrock integration exists (V2+)
6. ✅ The evaluation plan covers all four quality dimensions — parse quality, extraction quality, classification quality, and traceability completeness — and all four have implemented MLflow evaluation runs with real metrics logged to a Databricks-hosted MLflow tracking server during the V1 closeout checkpoint

**What V1 completion means precisely:**
- End-to-end single-domain (FDA warning letters) pipeline implemented and validated in a real Databricks personal workspace
- Bronze / Silver / Gold layer contracts implemented, enforced, and tested (427 tests)
- Evaluation layer implemented for all four quality dimensions with local and live Databricks MLflow logging
- Gold → Bedrock handoff preparation layer implemented (contract, validator, export materialization, batch bundle, integrity validation)
- Live Databricks MLflow experiments successfully populated for all four pipeline evaluation stages
- V1 remains single-domain, controlled, and non-production — no enterprise deployment, no production credentials
- Downstream Bedrock live integration is explicitly future work (V2+)

---

## V2 Scope

**V2 has started. Phase C is complete.** V2 is formally defined after V1 closeout (April 2026). Phase C (C-0: design, C-1: implementation, C-2: producer-side validation layer) is complete as of April 2026. Phase D-0 is the next phase not yet started. The phases below reflect the current delivery state.

### V2 Objective

V2 deepens the operational readiness and integration capability of this repo across three themes:

1. **Live downstream integration** — Move from contract-only handoff preparation (V1) to a real, validated delivery slice connecting Gold exports to Bedrock CaseOps consumption.
2. **Multi-domain coverage** — Expand beyond FDA warning letters to CISA advisories and incident reports, both of which have draft schemas in `docs/data-contracts.md` and routing labels already defined.
3. **Enterprise operational hardening** — Add human review workflow, environment separation, and governance monitoring.

### V2 Explicit Non-Goals

The following are explicitly out of scope for V2:

| Out of Scope for V2 | Reason |
|---|---|
| Streaming or near-real-time ingestion | V3+ |
| Cross-case analytics or KPI reporting | Not a repo concern at any phase |
| Agent reasoning, orchestration, or escalation | Bedrock CaseOps, not this repo |
| Production enterprise deployment | Requires explicit organizational scope change |
| Turning this repo into Bedrock CaseOps | Violates the Databricks / Bedrock boundary |

### V2 Databricks / Bedrock Boundary

This boundary remains explicit and non-negotiable in V2:

| Concern | This Repo (V2) | Bedrock CaseOps |
|---|---|---|
| Document ingestion, parsing, extraction | Yes | No |
| Schema validation and traceability | Yes | No |
| Classification, routing, and export | Yes | No |
| Live export delivery to Bedrock | Yes — V2-C (Delta Sharing + delivery events) | Receives via Delta Share |
| Multi-domain extraction and classification | Yes — V2-D (planned) | No |
| Human review queue (upstream intake side) | Yes — V2-E (planned) | Downstream review tools: No |
| Retrieval, RAG, and agent reasoning | No | Yes |
| Escalation and case-support workflows | No | Yes |

### V2 Phased Roadmap

#### Phase C — Live Handoff Integration and Export Delivery

**Goal**: Move beyond file-only export preparation to a real, validated delivery slice connecting Gold exports to Bedrock CaseOps. V1 B-phases prepared and hardened the export boundary. V2-C executes across that boundary — delivering to a real Bedrock consumer using a selected delivery protocol.

**Status**: Complete. C-0 (design), C-1 (implementation), and C-2 (producer-side validation layer) are all complete. Phase D-0 is next.

Subphases:
- **C-0** — Integration delivery mechanism design and selection. **Complete.** Decision: Delta Sharing as primary mechanism, augmenting (not replacing) the V1 file export path. See [`docs/live-handoff-design.md`](./docs/live-handoff-design.md) for the full design record.
- **C-1** — Export delivery implementation. **Complete.** Implements the upstream producer-side delivery augmentation: `DeliveryEvent` schema (`src/schemas/delivery_event.py`), delivery event materialization (`src/pipelines/delivery_events.py`), Delta Sharing producer-side preparation layer (`src/pipelines/delta_share_handoff.py`), and integration into `classify_gold.py` via `--delivery-dir`. Export payloads written with `--delivery-dir` carry `schema_version: v0.2.0` and three new optional provenance fields (`delivery_mechanism`, `delta_share_name`, `delivery_event_id`). V1 file export path fully preserved. 155 new tests. C-2 runtime validation not yet performed — `status = 'prepared'` in delivery events.
- **C-2** — Runtime integration validation. **Complete (producer-side validation layer).** Implements a bounded, 15-check delivery-layer validation layer: `DeliveryValidationResult` schema (`src/schemas/delivery_validation.py`), `validate_delivery_layer()` entry point (`src/pipelines/delivery_validation.py`), 134 new tests. Honest status vocabulary: `validated`, `partially_validated`, `not_provisioned`, `failed`. Default local run produces `not_provisioned` — correct and honest. Live end-to-end validation (Delta Share query + delivery event table row + payload conformance) requires manual workspace provisioning; the runbook is in `docs/delivery-runtime-validation.md`.

**Scope boundary**: V2-C delivers from this repo to a Bedrock consumer endpoint. It does not implement retrieval indexes, vector search, agent reasoning, or escalation logic — those remain Bedrock CaseOps. The furthest this repo reaches toward Bedrock is Delta Share provisioning — making data available for Bedrock to consume.

#### Phase D — Multi-Domain Pipeline Expansion

**Goal**: Extend the pipeline beyond FDA warning letters. CISA cybersecurity advisories are the first V2-D domain (draft schema in `docs/data-contracts.md`, `security_ops` routing label already defined). Incident reports are the second.

**Status**: Not started.

Subphases:
- **D-0** — Multi-domain framework: per-domain prompt routing, domain registry, multi-domain classification and routing table extension
- **D-1** — CISA advisory domain: extraction schema (from `docs/data-contracts.md` § 3 draft), `ai_extract` prompt template, `cisa_advisory` classification label, `security_ops` routing, evaluation pass
- **D-2** — Incident report domain: extraction schema (from `docs/data-contracts.md` § 3 draft), `ai_extract` prompt template, `incident_report` classification label, `incident_management` routing, evaluation pass

**Scope boundary**: Domain expansion adds schemas, prompts, labels, and routing to this repo. It does not add retrieval indexes or agent workflows.

#### Phase E — Enterprise Operational Hardening

**Goal**: Add selected operational hardening: structured human review for quarantined and low-confidence records, environment separation for safe multi-environment iteration, and governance monitoring views for pipeline health visibility.

**Status**: Not started.

Subphases:
- **E-0** — Human review queue and reprocessing design: review queue structure for quarantined records; reprocessing-on-failure path design; review outcome recording
- **E-1** — Environment separation: dev/staging/prod Databricks environment structure; environment-aware Unity Catalog configuration; deployment patterns without production credentials
- **E-2** — Governance monitoring: pipeline health views; batch-level quality trend artifacts; schema drift detection; governance reporting outputs

**Scope boundary**: E-phase hardening is upstream only. This repo does not own human case management tooling, production orchestration, or downstream operational dashboards.

### V2 Success Criteria

V2 is complete when all of the following are true:

1. A live delivery mechanism exists and is validated: Gold export payloads can be delivered to a Bedrock CaseOps consumer endpoint without a manual copy step (Phase C)
2. At least two additional document domains (CISA advisories, incident reports) can be processed end-to-end through the pipeline alongside FDA warning letters (Phase D)
3. Quarantined and low-confidence records have a defined human review path and reprocessing mechanism (Phase E-0)
4. The pipeline can be deployed in at least two distinct Databricks environments without configuration collision (Phase E-1)
5. The Databricks / Bedrock ownership boundary remains explicit throughout V2 — no retrieval, RAG, agent, or escalation logic enters this repo
6. No production credentials or enterprise organizational configuration is committed to this repo at any V2 phase
