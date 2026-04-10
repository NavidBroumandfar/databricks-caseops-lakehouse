# Evaluation Layer — Usage Guide (Phase A-4)

This directory documents the A-4 evaluation layer for the Databricks CaseOps Lakehouse pipeline. The evaluation layer is a first-class component of the repo — not an afterthought — and is designed to give any pipeline run a complete, honest quality picture.

---

## What the evaluation layer does

The evaluation layer sits **after** the pipeline, not inside it. Pipeline runs produce data; evaluation runs measure that data. This separation keeps the pipeline logic clean and allows re-evaluation without re-running ingestion.

The A-4 evaluation layer covers four quality dimensions:

| Dimension | Script | Experiment |
|---|---|---|
| Bronze parse quality | `src/evaluation/eval_bronze.py` | `caseops/bronze/parse_quality` |
| Silver extraction quality | `src/evaluation/eval_silver.py` | `caseops/silver/extraction_quality` |
| Gold classification quality | `src/evaluation/eval_gold.py` | `caseops/gold/classification_quality` |
| Cross-layer traceability | `src/evaluation/eval_traceability.py` | `caseops/pipeline/traceability` |
| End-to-end summary | `src/evaluation/run_evaluation.py` | `caseops/pipeline/end_to_end` |

Each evaluator:
- reads JSON artifacts from the local `output/` directory
- computes explicit metrics aligned with [`docs/evaluation-plan.md`](../../docs/evaluation-plan.md)
- writes a JSON evaluation artifact to `output/eval/`
- optionally logs metrics to MLflow when `--mlflow` is set

---

## Prerequisites

Python 3.9+, `pydantic` (v2). No Databricks workspace required for local evaluation.

```bash
pip install pydantic
```

MLflow logging is optional:

```bash
pip install mlflow
```

---

## Running the full pipeline first

The evaluation layer reads artifacts produced by the pipeline scripts. Run the full demo pipeline to generate artifacts:

```bash
# 1. Ingest → Bronze artifacts
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev

# 2. Extract → Silver artifacts
python src/pipelines/extract_silver.py --input-dir output/bronze

# 3. Classify → Gold artifacts
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze
```

After these three steps, `output/bronze/`, `output/silver/`, and `output/gold/` will contain JSON artifacts ready for evaluation.

---

## Running individual evaluators

### Bronze parse quality

```bash
python src/evaluation/eval_bronze.py --input-dir output/bronze
```

Metrics produced:
- `parse_success_rate` (target ≥ 0.95)
- `parse_failure_rate` (target ≤ 0.05)
- `median_char_count` (target > 500)
- `p10_char_count` (target > 100)
- `zero_char_count_rate` (target 0.0)
- Flagged records: any with `parse_status=failed` or `char_count < 100`

### Silver extraction quality

```bash
python src/evaluation/eval_silver.py --input-dir output/silver
```

Metrics produced:
- `schema_validity_rate` (target ≥ 0.80)
- `invalid_rate` (target ≤ 0.10)
- `mean_field_coverage_pct` (target ≥ 0.75)
- `p25_field_coverage_pct` (target ≥ 0.50)
- `required_field_null_rate` (target ≤ 0.05)
- Flagged records: any with `validation_status=invalid` or `field_coverage_pct < 0.40`

### Gold classification quality

```bash
python src/evaluation/eval_gold.py --input-dir output/gold
```

Metrics produced:
- `classification_success_rate` (target ≥ 0.85)
- `export_ready_rate` (target ≥ 0.70)
- `quarantine_rate` (target ≤ 0.15)
- `confidence_null_rate` — explicitly reported; > 0 means bootstrap or partial path
- `mean_classification_confidence` — `None` when all confidence values are null
- `low_confidence_rate` — computed only over records with non-null confidence
- Label distribution table
- Observations: documented when null confidence or placeholder run IDs are present

**Note on null confidence (A-3B bootstrap path):** When records originate from the A-3B Databricks bootstrap SQL path, `classification_confidence` is `NULL`. The evaluator surfaces this explicitly via `confidence_null_rate` and `observations` rather than hiding it or failing. See [ARCHITECTURE.md](../../ARCHITECTURE.md) § Gold Bootstrap Implementation Notes for context.

### Cross-layer traceability

```bash
python src/evaluation/eval_traceability.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold
```

Metrics produced:
- `gold_to_silver_link_rate` (target 1.0)
- `silver_to_bronze_link_rate` (target 1.0)
- `pipeline_run_id_coverage` (target 1.0)
- `schema_version_coverage` (target 1.0)
- `orphaned_silver_count` (target 0)
- `orphaned_gold_count` (target 0)
- `placeholder_run_id_count` — counts records with `pipeline_run_id = 'bootstrap_sql_v1'`

**Note on placeholder run IDs:** The A-3B bootstrap uses `pipeline_run_id = 'bootstrap_sql_v1'` as a placeholder string rather than a real MLflow run ID. These records are not orphaned — their `document_id`-based lineage is intact — but they are not linked to a tracked MLflow pipeline run. The evaluator surfaces this explicitly in its `placeholder_run_id_count` metric and `observations` list.

---

## Running the full evaluation in one command

```bash
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold
```

This runs all four evaluators, assembles an `EvaluationReport`, and writes:
- `output/eval/report_<id>.json` — machine-readable full report
- `output/eval/report_<id>.txt` — human-readable summary

### Skip individual layers

```bash
# Skip Gold if no Gold artifacts exist yet
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --skip-gold --skip-traceability
```

### With MLflow logging (optional)

```bash
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold \
  --mlflow
```

Requires `mlflow` to be installed. Set `MLFLOW_TRACKING_URI` to point at a live tracking server, or leave unset to use the local file store.

---

## Understanding the evaluation report

The JSON report has this structure:

```json
{
  "report_id": "<uuid>",
  "generated_at": "<UTC timestamp>",
  "pipeline_run_id_filter": null,
  "bootstrap_path_detected": true,
  "review_queue_size": 1,
  "all_warnings": ["...threshold violations..."],
  "all_observations": ["...non-error notes..."],
  "bronze": { "layer": "bronze", "metrics": {...}, "flagged_records": [...] },
  "silver": { "layer": "silver", "metrics": {...}, "flagged_records": [...] },
  "gold":   { "layer": "gold",   "metrics": {...}, "flagged_records": [...] },
  "traceability": {
    "gold_to_silver_link_rate": 1.0,
    "silver_to_bronze_link_rate": 1.0,
    "orphaned_silver_count": 0,
    "orphaned_gold_count": 0,
    "placeholder_run_id_count": 4,
    ...
  }
}
```

`bootstrap_path_detected: true` means at least one record carries a null `classification_confidence` or a placeholder `pipeline_run_id`. This is expected for records from the A-3B bootstrap and is documented as an observation, not a defect.

---

## Report models

`src/evaluation/report_models.py` defines the dataclass schema for evaluation reports:

- `LayerEvalResult` — metrics, warnings, observations, and flagged records for one layer
- `TraceabilityResult` — cross-layer link rates, orphan counts, placeholder run ID count
- `EvaluationReport` — top-level report aggregating all layer results

`src/evaluation/report_writer.py` writes both JSON and text formats from an `EvaluationReport`.

---

## Running the tests

```bash
python -m pytest tests/ -v
```

Tests cover:
- Bronze metric calculation and threshold checks
- Silver schema validity and field coverage aggregation
- Gold metric calculation with null confidence (bootstrap path)
- Gold threshold checks: null confidence warnings emitted but confidence thresholds not fired
- Traceability join logic and orphan detection
- Placeholder run ID detection
- End-to-end report assembly and writer output

---

## Architecture references

- [`ARCHITECTURE.md`](../../ARCHITECTURE.md) § Evaluation and Observability Layer
- [`docs/evaluation-plan.md`](../../docs/evaluation-plan.md)
- [`docs/data-contracts.md`](../../docs/data-contracts.md)
- [`docs/databricks-bootstrap.md`](../../docs/databricks-bootstrap.md) — A-3B context
