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
| Generic ML or data science notebooks | Not the project identity |
| Real-time streaming ingestion | V1 targets batch; streaming is a future evolution |
| Fine-tuned model training | Evaluation and prompt engineering only in V1 |
| Scala code | Python and SQL are the sole implementation languages |
| External LLM API calls (OpenAI, Anthropic) | Databricks AI Functions are preferred; external calls require explicit justification |
| Production secrets or credentials | Never included in this repo |

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

---

## Phased Roadmap

### Phase A-0 — Repo Foundation (current)
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

### Phase A-4 — Evaluation and Observability Layer
**Goal**: Formalize evaluation across all layers with MLflow experiments and traceable runs.

Deliverables:
- MLflow experiment structure (one experiment per pipeline stage)
- Per-document trace records linking Bronze → Silver → Gold
- Evaluation runner scripts for batch re-evaluation
- Evaluation summary report template

### Phase B (future) — Bedrock Handoff Integration
**Goal**: Deliver structured Gold assets to a live Bedrock retrieval index or agent workflow.

Scope to be defined when Bedrock CaseOps interface is stabilized.

---

## Success Criteria

The project is considered complete for V1 when:

1. A real document (e.g., a publicly available FDA warning letter) can be dropped into a Unity Catalog Volume and processed end-to-end to a Gold record without manual intervention
2. All three layer schemas (Bronze, Silver, Gold) are defined, documented, and validated
3. An MLflow evaluation run exists with meaningful metrics for at least two pipeline stages
4. The data contract is documented and consistent with the actual schema definitions
5. The downstream export format is specified and could be consumed by a Bedrock retrieval system without modification
6. The evaluation plan covers all four quality dimensions — parse quality, extraction quality, classification quality, and traceability completeness — and at least two of the four have implemented MLflow evaluation runs with real metrics
