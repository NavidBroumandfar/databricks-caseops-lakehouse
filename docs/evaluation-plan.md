# Evaluation Plan

> This document defines the evaluation strategy for all stages of the Databricks CaseOps Lakehouse pipeline.
> Evaluation is implemented as explicit MLflow runs, not inline assertions, to keep pipeline logic clean and allow independent re-evaluation.
> Authoritative design context is in [`ARCHITECTURE.md`](../ARCHITECTURE.md).

---

## Evaluation Philosophy

1. **Separation of concerns**: pipeline runs produce data; evaluation runs measure that data. These are independent MLflow runs.
2. **Explicit over implicit**: every quality dimension is named, defined, and measured with a specific metric. No vague "quality checks".
3. **Records, not exceptions**: failures and low-quality records are written as records with status fields. Evaluation counts them; it does not silently discard them.
4. **Reproducibility**: evaluation runs consume Delta table snapshots. The same run against the same snapshot produces the same metrics.
5. **Human review readiness**: evaluation surfaces records that require human attention, rather than blocking the pipeline.

---

## Evaluation Dimensions

The pipeline is evaluated across four quality dimensions:

| Dimension | Stage | Primary Question |
|---|---|---|
| **Parse quality** | Bronze | Did `ai_parse_document` produce usable text output? |
| **Extraction quality** | Silver | Did `ai_extract` produce accurate, complete structured fields? |
| **Classification quality** | Gold | Did `ai_classify` assign correct, confident labels? |
| **Traceability completeness** | All | Is every record fully traceable from Gold back to source? |

A fifth dimension, **schema validity**, is applied as a gate within Extraction quality.

---

## 1. Parse Quality (Bronze)

### Goal
Confirm that `ai_parse_document` is producing usable text from source documents. Detect documents that silently produce short or empty output.

### Metrics

| Metric | Definition | Target |
|---|---|---|
| `parse_success_rate` | Count of `parse_status = 'success'` / total documents in batch | ≥ 0.95 |
| `parse_partial_rate` | Count of `parse_status = 'partial'` / total | Monitor; no hard threshold in V1 |
| `parse_failure_rate` | Count of `parse_status = 'failed'` / total | ≤ 0.05 |
| `median_char_count` | Median `char_count` across successful parse records | > 500 chars (sanity check) |
| `p10_char_count` | 10th percentile `char_count` | > 100 chars (detect near-empty parses) |
| `zero_char_count_rate` | Fraction of records with `char_count = 0` despite `parse_status = 'success'` | 0.0 (should never occur) |

### Evaluation Method
- MLflow run consuming Bronze Delta table snapshot
- Logged as experiment: `caseops/bronze/parse_quality`
- Logged per batch: aggregated metrics above
- Flagged records: any with `parse_status = 'failed'` or `char_count < 100` logged as MLflow artifact (`flagged_records.json`)

### Human Review Triggers
- `parse_failure_rate > 0.10` — review batch for format issues or Volume path errors
- Any record with `zero_char_count_rate > 0` — investigate `ai_parse_document` call immediately
- `median_char_count < 200` for a batch — likely PDF parsing issue (scanned documents, image-only PDFs)

---

## 2. Extraction Quality (Silver)

### Goal
Measure how accurately and completely `ai_extract` populates structured fields from parsed text. Distinguish between schema failures (wrong shape) and coverage failures (missing values).

### Metrics

| Metric | Definition | Target |
|---|---|---|
| `schema_validity_rate` | Count of `validation_status = 'valid'` / total extraction records | ≥ 0.80 |
| `partial_validity_rate` | Count of `validation_status = 'partial'` / total | Monitor |
| `invalid_rate` | Count of `validation_status = 'invalid'` / total | ≤ 0.10 |
| `mean_field_coverage_pct` | Mean `field_coverage_pct` across all extraction records | ≥ 0.75 |
| `p25_field_coverage_pct` | 25th percentile field coverage | ≥ 0.50 |
| `required_field_null_rate` | Fraction of required fields that are null across all records | ≤ 0.05 |
| `validation_error_frequency` | Top-N most frequent `validation_errors` by field name | Informational |

### Schema Validity Definition
A record is `valid` if all required fields are non-null and all field types conform to the Silver schema for the declared document class. A record is `partial` if non-required fields are missing or confidence is low but required fields are present. A record is `invalid` if one or more required fields are null or have a type mismatch.

### Evaluation Method
- MLflow run consuming Silver Delta table snapshot
- Logged as experiment: `caseops/silver/extraction_quality`
- Logged per batch: aggregated metrics above
- Logged per run: model identifier used (`extraction_model`), prompt template ID used (`extraction_prompt_id`)
- Flagged records: any with `validation_status = 'invalid'` logged as artifact

### Human Review Triggers
- `invalid_rate > 0.10` — review prompt template for the failing document class
- `required_field_null_rate > 0.05` for a specific field — field may not exist in this document domain; update schema or prompt
- A specific `validation_error` appearing in > 20% of records — likely a systematic extraction failure

### Extraction Drift Detection
When the pipeline runs across multiple batches, the evaluation run tracks:
- `extraction_model` version changes between runs
- Mean `field_coverage_pct` trend over time
- `schema_validity_rate` trend over time

A significant drop (> 10 percentage points) in `schema_validity_rate` between runs triggers a human review flag.

---

## 3. Classification Quality (Gold)

### Goal
Confirm that `ai_classify` is assigning document type labels and routing labels with sufficient confidence and without excessive concentration in the `unknown` or `quarantine` categories.

### Metrics

| Metric | Definition | Target |
|---|---|---|
| `classification_success_rate` | Count of `document_type_label != 'unknown'` / total | ≥ 0.85 |
| `unknown_label_rate` | Count of `document_type_label = 'unknown'` / total | ≤ 0.15 |
| `quarantine_rate` | Count of `routing_label = 'quarantine'` / total | ≤ 0.15 |
| `export_ready_rate` | Count of `export_ready = true` / total | ≥ 0.70 |
| `mean_classification_confidence` | Mean `classification_confidence` across all records | ≥ 0.75 |
| `low_confidence_rate` | Count of `classification_confidence < 0.70` / total | ≤ 0.20 |
| `label_distribution` | Count per `document_type_label` | Informational; flag extreme imbalance |

### Evaluation Method
- MLflow run consuming Gold Delta table snapshot
- Logged as experiment: `caseops/gold/classification_quality`
- Logged per batch: aggregated metrics above
- Logged: label distribution as a table artifact
- Flagged records: any with `document_type_label = 'unknown'` or `classification_confidence < 0.50`

### Human Review Triggers
- `unknown_label_rate > 0.20` — taxonomy may need expansion, or prompt needs revision
- `export_ready_rate < 0.60` — systematic quality issue upstream (extraction or classification)
- `mean_classification_confidence < 0.65` — classifier is uncertain across the board; review prompt or label taxonomy

---

## 4. Traceability Completeness

### Goal
Confirm that every Gold record can be traced back through Silver to Bronze to the source document. Detect orphaned or broken lineage records.

### Metrics

| Metric | Definition | Target |
|---|---|---|
| `gold_to_silver_link_rate` | Fraction of Gold records with a matching Silver `extraction_id` | 1.0 |
| `silver_to_bronze_link_rate` | Fraction of Silver records with a matching Bronze `bronze_record_id` | 1.0 |
| `bronze_to_source_link_rate` | Fraction of Bronze records with a matching `source_path` in the Volume | 1.0 |
| `pipeline_run_id_coverage` | Fraction of records with a non-null `pipeline_run_id` | 1.0 |
| `schema_version_coverage` | Fraction of records with a non-null `schema_version` | 1.0 |
| `orphaned_silver_records` | Silver records with no corresponding Bronze record | 0 |
| `orphaned_gold_records` | Gold records with no corresponding Silver record | 0 |

### Evaluation Method
- MLflow run consuming all three layer Delta table snapshots
- Logged as experiment: `caseops/pipeline/traceability`
- Cross-layer join to detect orphaned records
- Any orphaned record is logged as a critical artifact (`orphaned_records.json`)

### Failure Response
- Any `gold_to_silver_link_rate < 1.0` or `silver_to_bronze_link_rate < 1.0` is treated as a pipeline defect, not a data quality issue
- Orphaned records must be investigated and resolved before the pipeline run is considered complete

---

## 5. Human Review Readiness

### Goal
Ensure the pipeline produces a clear, actionable set of records that require human attention — without overwhelming reviewers or being so permissive that bad records reach downstream systems.

### Review Queue Definition
A record enters the human review queue if any of the following are true:

| Condition | Layer | Reason |
|---|---|---|
| `parse_status = 'failed'` | Bronze | Could not extract usable text |
| `char_count < 100` and `parse_status = 'success'` | Bronze | Suspiciously short parse output |
| `validation_status = 'invalid'` | Silver | Required fields missing or wrong type |
| `field_coverage_pct < 0.40` | Silver | Extraction missed more than 60% of fields |
| `document_type_label = 'unknown'` | Gold | Classifier could not identify document type |
| `classification_confidence < 0.50` | Gold | Very low classification confidence |
| `export_ready = false` | Gold | Did not meet export quality threshold |

### Review Queue Metrics (tracked per batch)

| Metric | Definition | Alert Threshold |
|---|---|---|
| `review_queue_rate` | Fraction of total documents entering the review queue | > 0.30 triggers investigation |
| `review_queue_size` | Absolute count of documents in the review queue per batch | Informational |
| `review_queue_reason_distribution` | Count per queue entry reason | Informational; identify dominant failure mode |

---

## MLflow Experiment Structure Summary

| Experiment Path | Stage | Frequency |
|---|---|---|
| `caseops/bronze/parse_quality` | Bronze | Per pipeline batch |
| `caseops/silver/extraction_quality` | Silver | Per pipeline batch |
| `caseops/gold/classification_quality` | Gold | Per pipeline batch |
| `caseops/pipeline/traceability` | All | Per pipeline batch |
| `caseops/pipeline/end_to_end` | All | Periodic summary run |

All evaluation runs log:
- Run parameters: batch size, document class filter, pipeline run ID being evaluated, schema version
- Run metrics: all metrics defined above
- Run artifacts: flagged record lists as JSON, label distribution tables

---

## Evaluation Implementation Status

| Evaluator | Implementation Path | Status |
|---|---|---|
| Bronze parse quality | `src/evaluation/eval_bronze.py` | Planned (Phase A-1) |
| Silver extraction quality | `src/evaluation/eval_silver.py` | Planned (Phase A-2) |
| Gold classification quality | `src/evaluation/eval_gold.py` | Planned (Phase A-3) |
| Traceability completeness | `src/evaluation/eval_traceability.py` | Planned (Phase A-4) |
| End-to-end summary | `src/evaluation/run_evaluation.py` | Planned (Phase A-4) |
