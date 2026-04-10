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

## Active Phase

**Phases A-0 through A-3** (local-safe implementation) are complete:
- A-0: Repo foundation and documentation
- A-1: Bronze ingestion and parsing pipeline (local-safe)
- A-2: Silver extraction and validation (local-safe)
- A-3: Gold classification and routing (local-safe)

**Phase A-3B — Personal Databricks Bootstrap Consolidation** is the current integration task.
This phase captures a validated personal-workspace Databricks SQL execution pass (Bronze → Silver → Gold)
using real AI Functions and public FDA sample documents. It is non-production. See
[`docs/databricks-bootstrap.md`](./databricks-bootstrap.md) for full details.

**Phase A-4 — Evaluation and Observability** is planned next after A-3B consolidation is complete.
A-4 scope: MLflow experiment structure, per-document trace records, evaluation runner scripts.

See [`PROJECT_SPEC.md`](../PROJECT_SPEC.md) for the full roadmap.

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
