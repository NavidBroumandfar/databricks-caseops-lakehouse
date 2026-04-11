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

**Phases A-0 through B-3 are complete.** The repo has a validated pipeline (A-0 through A-4.1), an explicit Gold → Bedrock handoff contract (B-0), a repo-enforced contract validator (B-1), a contract-enforced export materialization path (B-2), and a clean export/handoff module boundary (B-3). Phase B proper (live Bedrock integration) has not started.

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

The module boundary is:
  `classify_gold.py` → assembles GoldRecord → calls `execute_export` → writes Gold artifact
  `export_handoff.py` → validates contract → writes export artifact → returns `ExportResult`

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
