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

**Why B-0 exists before live integration:**
Live integration (Phase B proper) requires both sides of the interface to have a shared, explicit understanding of the contract. Without B-0, the handoff structure is implicit, versionless, and subject to misalignment. B-0 makes the contract explicit, honest about current limitations, and testable before any infrastructure work begins.

**How B-0 preserves the Databricks / Bedrock split:**
B-0 documents what this repo guarantees to produce (Gold `export_payload` at a deterministic path) and what Bedrock CaseOps is responsible for consuming. It does not cross into Bedrock's implementation domain. The governance boundary remains at the materialized export file.

This is the first sub-phase of the broader Phase B (Bedrock Handoff Integration). It does not include live endpoint work — that is Phase B proper.

---

## Success Criteria

The project is considered complete for V1 when:

1. A real document (e.g., a publicly available FDA warning letter) can be dropped into a Unity Catalog Volume and processed end-to-end to a Gold record without manual intervention
2. All three layer schemas (Bronze, Silver, Gold) are defined, documented, and validated
3. An MLflow evaluation run exists with meaningful metrics for at least two pipeline stages
4. The data contract is documented and consistent with the actual schema definitions
5. The downstream export format is specified and could be consumed by a Bedrock retrieval system without modification
6. The evaluation plan covers all four quality dimensions — parse quality, extraction quality, classification quality, and traceability completeness — and at least two of the four have implemented MLflow evaluation runs with real metrics
