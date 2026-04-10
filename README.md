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

This is the Databricks-native sibling of **Bedrock CaseOps**. Where Bedrock CaseOps handles retrieval augmentation and agent orchestration, this repo owns the **document transformation and structuring layer** upstream of it.

| Concern | This Repo | Bedrock CaseOps |
|---|---|---|
| Raw document ingestion | Yes | No |
| Parsing and extraction | Yes | No |
| Structured schema output | Yes | Consumes |
| Classification and routing | Yes | Consumes |
| Agent orchestration | No | Yes |
| Retrieval (RAG) | No | Yes |

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

**Phase A-3 — Gold classification and routing** is in progress. The local-safe implementation slice is delivered:

| Deliverable | Path | Status |
|---|---|---|
| Gold schema (Pydantic v2) | `src/schemas/gold_schema.py` | ✅ Complete |
| Classification taxonomy | `src/utils/classification_taxonomy.py` | ✅ Complete |
| Classification config | `src/pipelines/classification_config.yaml` | ✅ Complete |
| Gold classification pipeline | `src/pipelines/classify_gold.py` | ✅ Complete |
| Gold evaluation script | `src/evaluation/eval_gold.py` | ✅ Complete |
| Expected Gold fixture | `examples/expected_gold_fda_warning_letter.json` | ✅ Complete |

To run the local Gold demo, see the [Running the Gold Demo](#running-the-gold-demo) section below.

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

## Repository Structure

```
databricks-caseops-lakehouse/
├── README.md
├── PROJECT_SPEC.md          # Scope and roadmap source of truth
├── ARCHITECTURE.md          # Technical design source of truth
├── docs/
│   ├── CURSOR_CONTEXT.md    # Agent orientation guide
│   ├── roadmap.md
│   ├── data-contracts.md
│   ├── evaluation-plan.md
│   └── prompts/             # Excluded from version control
├── src/
│   ├── schemas/             # Pydantic / JSON Schema definitions
│   ├── pipelines/           # Bronze → Silver → Gold pipeline logic
│   ├── evaluation/          # MLflow evaluation runners
│   └── utils/               # Shared helpers
├── notebooks/               # Databricks notebooks (exploration, demos)
└── examples/                # Sample documents and expected outputs
```

---

## Non-Goals

This repo does **not** include:

- A frontend or UI of any kind
- A standalone chatbot or conversational interface
- Generic Databricks demo notebooks
- Scala code
- Anything that requires production credentials to demonstrate

---

## Related

- `bedrock-caseops` — downstream retrieval and agent orchestration layer
