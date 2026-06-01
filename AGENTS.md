# AGENTS.md - Repository Working Guide

This file is for AI coding agents and future maintainers working in this repository.
It summarizes how to operate safely in the project after the June 2026 audit.

## Repository Identity

`databricks-caseops-lakehouse` is an upstream governed document intelligence and
handoff layer. It prepares unstructured documents for downstream AI systems by
producing structured, traceable, contract-validated Gold records.

This repository is not the multi-agent reasoning layer. Retrieval, RAG, agent
orchestration, escalation, case-support workflows, and Bedrock runtime logic
belong in the downstream Bedrock CaseOps/control-tower project.

## Required Reading Order

Before making non-trivial changes, read these in order:

1. `README.md` - public project identity and current positioning
2. `PROJECT_SPEC.md` - scope and non-goals
3. `ARCHITECTURE.md` - technical design and layer contracts
4. `ROADMAP.md` - forward improvement roadmap from the audit
5. `docs/data-contracts.md` - field-level contracts
6. `docs/bedrock-handoff-contract.md` - Gold to Bedrock boundary
7. `docs/CURSOR_CONTEXT.md` - existing agent orientation and phase history
8. `docs/roadmap.md` - historical phase roadmap

When files disagree, use this authority order:

1. `PROJECT_SPEC.md` for scope
2. `ARCHITECTURE.md` for technical design
3. `docs/data-contracts.md` and `docs/bedrock-handoff-contract.md` for schema and handoff behavior
4. `ROADMAP.md` for next-step prioritization
5. `docs/roadmap.md` for historical phase tracking

## Audit Baseline

The June 2026 audit found:

- The repo is coherent as an upstream Databricks-style preparation layer.
- It does not implement a multi-agent runtime.
- Local execution uses deterministic Python extractors/classifiers and Pydantic validation.
- Databricks AI Function execution exists as bootstrap SQL and design stubs, not reusable production Python adapters.
- Delta Sharing delivery is producer-side prepared, not live-provisioned.
- Tests pass locally once `pytest` is installed: 1,425 collected, 1,425 passed.

## Scope Guardrails

Do not add these to this repository unless the project scope is deliberately changed:

- Bedrock agent reasoning loops
- Chatbot or conversational UI
- RAG orchestration
- Vector index population
- AWS Bedrock SDK runtime code
- Multi-agent frameworks such as CrewAI, AutoGen, LangGraph, or LangChain agent graphs
- Production credentials, workspace URLs, PATs, account IDs, or customer data

Acceptable additions here:

- Databricks Jobs / Asset Bundle deployment for this pipeline
- Real Databricks runtime adapters for `ai_parse_document`, `ai_extract`, and `ai_classify`
- Delta table read/write implementations behind existing interfaces
- Stronger evaluation, governance monitoring, and contract validation
- Better fixtures and reproducible local developer setup
- Documentation that keeps the upstream/downstream boundary explicit

## Development Notes

The local shell may not expose `python`; prefer:

```bash
.venv/bin/python
```

or:

```bash
python3
```

Minimum runtime dependency is currently `pydantic`. The test runner is not in
`requirements.txt`; install it explicitly until a dev dependency file exists:

```bash
.venv/bin/python -m pip install pytest
.venv/bin/python -m pytest -q
```

Recommended verification after pipeline changes:

```bash
.venv/bin/python -m pytest -q
```

For docs-only changes, no test run is required, but inspect the diff carefully.

## Change Discipline

- Preserve the repo boundary: raw document in, structured AI-ready record out.
- Do not claim live Bedrock or production Databricks deployment unless actually validated.
- Keep local-safe behavior working without credentials.
- Do not commit generated local outputs, secrets, or virtualenv artifacts.
- Keep examples public, synthetic, or redacted.
- Prefer small, contract-preserving changes over broad rewrites.
- When changing schemas, update tests, examples, docs, and validators together.
