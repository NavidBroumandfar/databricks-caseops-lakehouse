# Databricks CaseOps Lakehouse

A governed, Databricks-native document intelligence pipeline that converts unstructured enterprise documents into structured, traceable, evaluation-ready AI assets for downstream retrieval and agent workflows.

---

## What This Project Does

Enterprise operations generate large volumes of unstructured documents — regulatory notices, incident reports, standard operating procedures, quality reviews, and technical advisories. These documents contain operationally significant information that is difficult to query, route, or reason over at scale without structured transformation.

This project builds a **production-aware document intelligence pipeline** on Databricks that:

1. **Ingests** unstructured documents into governed Unity Catalog Volumes
2. **Parses** raw document content using `ai_parse_document` and normalizes it into a Bronze layer
3. **Extracts** structured fields from parsed content using `ai_extract` into a Silver layer
4. **Classifies and routes** documents using `ai_classify` into a Gold layer of AI-ready assets
5. **Evaluates** every stage for extraction quality, schema validity, and traceability completeness using MLflow
6. **Exports** Gold-tier structured assets to downstream Bedrock retrieval and agent systems

---

## Positioning

This is the Databricks-native upstream layer of the **Bedrock CaseOps** system. This repo owns **governed document transformation and AI-ready asset preparation**. Bedrock CaseOps owns what happens after the handoff: retrieval, agentic reasoning, escalation, and case-support workflows.

| Concern | This Repo | Bedrock CaseOps |
|---|---|---|
| Raw document ingestion | Yes | No |
| Parsing and extraction | Yes | No |
| Schema validation and traceability | Yes | No |
| Classification and routing | Yes | No |
| Governed AI-ready asset preparation | Yes | No |
| Gold export payload delivery | Yes (file/Delta) | Consumes |
| Retrieval and RAG | No | Yes |
| Agent reasoning and orchestration | No | Yes |
| Escalation and case-support workflows | No | Yes |
| KPI reporting or cross-case analytics | No | Out of scope for both in V1 |

This repo is the **governed upstream structuring layer**. It does not reason over documents, orchestrate decisions, or produce operational dashboards. Its contract is: raw document in, structured AI-ready record out.

---

## Default Document Domain

The pipeline is designed for document-heavy operational and regulatory workflows:

- FDA warning letters and safety notices
- CISA advisories and cybersecurity bulletins
- Incident reports and post-mortems
- Standard operating procedures (SOPs)
- Quality review and audit records
- Technical support and case review documents

---

## Architecture Overview

```
Unity Catalog Volumes (raw)
        │
        ▼
  Bronze Layer  ─── raw parsed text, source metadata, parse provenance
        │
        ▼
  Silver Layer  ─── structured field extraction, schema-validated records
        │
        ▼
  Gold Layer    ─── classified, routed, AI-ready assets
        │
        ▼
  Downstream    ─── Bedrock retrieval index / agent context payloads
```

All layers are governed by Unity Catalog. All transformations are traceable via MLflow. See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for full design detail.

---

## Tech Stack

| Component | Technology |
|---|---|
| Platform | Databricks |
| Storage governance | Unity Catalog, Volumes |
| Parsing | `ai_parse_document` |
| Extraction | `ai_extract` |
| Classification | `ai_classify` |
| Evaluation & tracing | MLflow |
| Language (pipelines) | Python, SQL |
| Config | YAML |
| Docs | Markdown |

---

## Project Status

**Phases A-0 through B-1** are complete. This repo now includes a validated personal Databricks bootstrap pass (A-3B), a full evaluation and observability layer (A-4), an explicit Gold → Bedrock handoff contract (B-0), and a repo-enforced contract validator with tests (B-1). This remains a controlled, portfolio-safe, non-production project — no enterprise deployment, no production credentials, no live orchestration.

**Phase A-0 — Repo foundation and core documentation** is complete.

**Phase A-1 — Bronze ingestion and parsing pipeline** is complete:

| Deliverable | Path | Status |
|---|---|---|
| Bronze schema (Pydantic v2) | `src/schemas/bronze_schema.py` | ✅ Complete |
| Unity Catalog config | `src/pipelines/catalog_config.yaml` | ✅ Complete |
| Bronze ingestion script | `src/pipelines/ingest_bronze.py` | ✅ Complete |
| Bronze evaluation script | `src/evaluation/eval_bronze.py` | ✅ Complete |
| Sample FDA warning letter fixture | `examples/fda_warning_letter_sample.md` | ✅ Complete |

**Phase A-2 — Silver extraction and validation** is complete. The local-safe implementation slice is delivered:

| Deliverable | Path | Status |
|---|---|---|
| Silver schema (Pydantic v2) | `src/schemas/silver_schema.py` | ✅ Complete |
| Extraction config | `src/pipelines/extraction_config.yaml` | ✅ Complete |
| Extraction prompt templates | `src/utils/extraction_prompts.py` | ✅ Complete |
| Silver extraction pipeline | `src/pipelines/extract_silver.py` | ✅ Complete |
| Silver evaluation script | `src/evaluation/eval_silver.py` | ✅ Complete |
| Expected Silver fixture | `examples/expected_silver_fda_warning_letter.json` | ✅ Complete |

**Phase A-3 — Gold classification and routing** is complete. The local-safe implementation slice is delivered:

| Deliverable | Path | Status |
|---|---|---|
| Gold schema (Pydantic v2) | `src/schemas/gold_schema.py` | ✅ Complete |
| Classification taxonomy | `src/utils/classification_taxonomy.py` | ✅ Complete |
| Classification config | `src/pipelines/classification_config.yaml` | ✅ Complete |
| Gold classification pipeline | `src/pipelines/classify_gold.py` | ✅ Complete |
| Gold evaluation script | `src/evaluation/eval_gold.py` | ✅ Complete |
| Expected Gold fixture | `examples/expected_gold_fda_warning_letter.json` | ✅ Complete |

**Phase A-3B — Personal Databricks Bootstrap Consolidation** is complete. This bridging phase captured a validated personal-workspace end-to-end SQL execution pass against real Databricks AI Functions and public FDA sample documents. It is non-production and does not imply enterprise deployment or MLflow automation.

| Deliverable | Path | Status |
|---|---|---|
| Bronze parse smoke SQL | `notebooks/bootstrap/01_bronze_parse_smoke.sql` | ✅ Complete |
| Bronze bootstrap v1 SQL | `notebooks/bootstrap/02_bronze_bootstrap_v1.sql` | ✅ Complete |
| Silver extraction smoke SQL | `notebooks/bootstrap/03_silver_extract_smoke_v1.sql` | ✅ Complete |
| Gold classification smoke SQL | `notebooks/bootstrap/04_gold_classify_route_smoke_v1.sql` | ✅ Complete |
| Bootstrap evaluation SQL | `notebooks/bootstrap/05_bootstrap_evaluation_v1.sql` | ✅ Complete |
| Bootstrap documentation | `docs/databricks-bootstrap.md` | ✅ Complete |
| Resource layout example | `config/databricks.resources.example.yml` | ✅ Complete |

**Validated bootstrap results** (4 public FDA sample PDFs, personal serverless SQL warehouse):

| Metric | Value |
|---|---|
| Total documents | 4 |
| Bronze parse success | 4 / 4 |
| Silver extraction records | 4 / 4 |
| Gold export-ready | 3 / 4 |
| Quarantine | 1 / 4 |
| Full lineage present | 4 / 4 |

The quarantine record is a governance signal, not a failure — it confirms rule-based routing is functioning correctly. See [`docs/databricks-bootstrap.md`](./docs/databricks-bootstrap.md) for full details, constraints, and next steps.

**Phase A-4 — Evaluation and Observability** is complete. This phase implements the formal evaluation layer across all pipeline stages:

| Deliverable | Path | Status |
|---|---|---|
| Traceability evaluator | `src/evaluation/eval_traceability.py` | ✅ Complete |
| Full-pipeline orchestrator | `src/evaluation/run_evaluation.py` | ✅ Complete |
| Report models | `src/evaluation/report_models.py` | ✅ Complete |
| Report writer | `src/evaluation/report_writer.py` | ✅ Complete |
| A-4 test suite | `tests/` (84 tests) | ✅ Complete | Evaluation layer tests |
| Evaluation usage guide | `examples/evaluation/README.md` | ✅ Complete |

The evaluation layer explicitly handles the A-3B bootstrap path: null `classification_confidence`, placeholder `pipeline_run_id` values, and the distinction between bootstrap-origin records and target-state MLflow pipeline records. A-4.1 runtime inspection confirmed that scalar confidence is not available from `ai_classify` in the validated bootstrap path, and that the conservative quarantine behavior (1 of 4 records quarantined) matched expectations. See [`docs/evaluation-plan.md`](./docs/evaluation-plan.md) for the full approach.

To run the local Gold demo, see the [Running the Gold Demo](#running-the-gold-demo) section below.

**Phase B-0 — Bedrock Handoff Contract Preparation** is complete. The single authoritative contract artifact is [`docs/bedrock-handoff-contract.md`](./docs/bedrock-handoff-contract.md). It defines the `export_payload` structure, required vs optional fields, routing label → Bedrock consumer mapping, and `export_ready` / `quarantine` semantics for the Gold → Bedrock handoff. No live AWS/Bedrock integration was delivered.

**Phase B-1 — Handoff Contract Materialization and Validation** is complete. B-1 converts the B-0 documentation contract into repo-enforced, testable behavior:

| Deliverable | Path | Status |
|---|---|---|
| Contract validator | `src/schemas/bedrock_contract.py` | ✅ Complete |
| Contract-valid fixture | `examples/contract_valid_fda_export_payload.json` | ✅ Complete |
| B-1 test suite | `tests/test_bedrock_contract_validation.py` (53 tests) | ✅ Complete |
| Gold schema alignment | `src/schemas/gold_schema.py` (`classification_confidence` Optional) | ✅ Updated |
| Pipeline alignment | `src/pipelines/classify_gold.py` (null-confidence safe routing) | ✅ Updated |

Total test count: **137 tests** across all pipeline stages and contract validation.

See [`PROJECT_SPEC.md`](./PROJECT_SPEC.md) for the full roadmap and [`docs/roadmap.md`](./docs/roadmap.md) for phase detail.

---

## Running the Bronze Demo

Requires Python 3.9+ and `pydantic` (v2). No Databricks workspace needed.

```bash
# 1. Install the only required dependency
pip install pydantic

# 2. Ingest the sample FDA warning letter → produces a Bronze JSON artifact
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev

# Artifact is written to output/bronze/<bronze_record_id>.json

# 3. Run Bronze evaluation against all artifacts in the output directory
python src/evaluation/eval_bronze.py --input-dir output/bronze

# Optional: evaluate a single artifact
python src/evaluation/eval_bronze.py --input output/bronze/<bronze_record_id>.json
```

The evaluation script prints a parse quality summary and writes a JSON evaluation artifact to `output/eval/`.

---

## Running the Silver Demo

Requires Python 3.9+ and `pydantic` (v2). No Databricks workspace needed.
If you have already run the Bronze demo, skip step 2.

```bash
# 1. Install the only required dependency
pip install pydantic

# 2. Ingest the sample FDA warning letter → produces a Bronze JSON artifact
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev

# Artifact is written to output/bronze/<bronze_record_id>.json

# 3. Extract structured fields from Bronze → produces a Silver JSON artifact
python src/pipelines/extract_silver.py --input-dir output/bronze

# Artifact is written to output/silver/<extraction_id>.json

# 4. Run Silver evaluation against all artifacts in the output directory
python src/evaluation/eval_silver.py --input-dir output/silver

# Optional: evaluate a single artifact
python src/evaluation/eval_silver.py --input output/silver/<extraction_id>.json
```

The evaluation script prints an extraction quality summary (validity rate, field
coverage, required-field null rate) and writes a JSON evaluation artifact to
`output/eval/`. See `examples/expected_silver_fda_warning_letter.json` for a
reference fixture showing a successful extraction result.

---

## Running the Gold Demo

Requires Python 3.9+ and `pydantic` (v2). No Databricks workspace needed.
If you have already run the Bronze and Silver demos, skip steps 2–3.

```bash
# 1. Install the only required dependency
pip install pydantic

# 2. Ingest the sample FDA warning letter → produces a Bronze JSON artifact
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev

# Artifact is written to output/bronze/<bronze_record_id>.json

# 3. Extract structured fields from Bronze → produces a Silver JSON artifact
python src/pipelines/extract_silver.py --input-dir output/bronze

# Artifact is written to output/silver/<extraction_id>.json

# 4. Classify Silver records → produces Gold artifacts and export payloads
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze

# Gold record: output/gold/<gold_record_id>.json
# Export payload (if export-ready): output/gold/exports/regulatory_review/<document_id>.json

# 5. Run Gold evaluation against all artifacts in the output directory
python src/evaluation/eval_gold.py --input-dir output/gold

# Optional: evaluate a single artifact
python src/evaluation/eval_gold.py --input output/gold/<gold_record_id>.json
```

The evaluation script prints a classification quality summary (success rate,
export-ready rate, confidence distribution, label distribution) and writes a
JSON evaluation artifact to `output/eval/`. See
`examples/expected_gold_fda_warning_letter.json` for a reference fixture showing
a successful classification and export-ready result.

---

## Running the A-4 Evaluation Layer

Requires Python 3.9+ and `pydantic` (v2). Run the pipeline demos first to generate artifacts.

```bash
# Run the full evaluation pass across all three layers
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold

# Reports are written to output/eval/:
#   report_<id>.json  — machine-readable full report
#   report_<id>.txt   — human-readable summary
```

Or run individual evaluators:

```bash
# Bronze: parse quality
python src/evaluation/eval_bronze.py --input-dir output/bronze

# Silver: extraction quality
python src/evaluation/eval_silver.py --input-dir output/silver

# Gold: classification quality (null-confidence safe)
python src/evaluation/eval_gold.py --input-dir output/gold

# Cross-layer traceability
python src/evaluation/eval_traceability.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold
```

Optional MLflow logging (requires `mlflow` installed):

```bash
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold \
  --mlflow
```

See [`examples/evaluation/README.md`](./examples/evaluation/README.md) for the full evaluation usage guide, including bootstrap-path context (null confidence, placeholder run IDs).

---

## Repository Structure

```
databricks-caseops-lakehouse/
├── README.md
├── PROJECT_SPEC.md          # Scope and roadmap source of truth
├── ARCHITECTURE.md          # Technical design source of truth
├── config/
│   └── databricks.resources.example.yml   # Unity Catalog layout reference (no credentials)
├── docs/
│   ├── CURSOR_CONTEXT.md    # Agent orientation guide
│   ├── roadmap.md
│   ├── data-contracts.md
│   ├── evaluation-plan.md
│   ├── databricks-bootstrap.md   # A-3B personal bootstrap validation record
│   └── prompts/             # Excluded from version control
├── src/
│   ├── schemas/             # Pydantic / JSON Schema definitions
│   │   └── bedrock_contract.py   # B-1: Gold export payload contract validator
│   ├── pipelines/           # Bronze → Silver → Gold pipeline logic
│   ├── evaluation/          # A-4 evaluation runners and report infrastructure
│   │   ├── eval_bronze.py
│   │   ├── eval_silver.py
│   │   ├── eval_gold.py          # Null-confidence safe (A-3B / A-4)
│   │   ├── eval_traceability.py  # Cross-layer traceability (A-4)
│   │   ├── run_evaluation.py     # Full-pipeline orchestrator (A-4)
│   │   ├── report_models.py      # Structured report dataclasses (A-4)
│   │   └── report_writer.py      # JSON + text report writer (A-4)
│   └── utils/               # Shared helpers
├── notebooks/
│   └── bootstrap/           # Validated Databricks bootstrap SQL (A-3B)
├── tests/                   # 137 tests: A-4 evaluators (84) + B-1 contract validation (53)
└── examples/
    ├── evaluation/          # A-4 usage guide
    └── ...                  # Sample documents and expected outputs
```

---

## Non-Goals

This repo does **not** include:

- A frontend or UI of any kind
- A standalone chatbot or conversational interface
- Generic Databricks demo notebooks
- Scala code
- Anything that requires production credentials to demonstrate
- Cross-case analytics, trend reporting, or KPI dashboards
- Historical operational intelligence or aggregate performance metrics
- Downstream agent orchestration, reasoning, or decision runtime (that is Bedrock CaseOps)

This repo is not yet, and does not aim to be, a mature analytics backbone or operational intelligence platform. It is the governed upstream document intelligence and AI-ready asset preparation layer.

---

## Related

- `bedrock-caseops` — downstream retrieval and agent orchestration layer
