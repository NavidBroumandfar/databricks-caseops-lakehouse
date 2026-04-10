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

---

## Current Status

**Phases A-0 through A-4 are complete.** The repo is in a post-A-4.1 documentation alignment state, positioned for Phase B-0.

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

**Phase B-0 — Bedrock Handoff Contract Preparation** is the next phase and has not started.
Do not treat B-0 as active or underway. Scope will be defined when the Bedrock CaseOps interface
is stabilized.

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
