# V1 Closeout — MLflow Workspace Evaluation Checkpoint

> **Runbook status**: Ready to execute. No code blockers.
> This document is the single practical runbook for completing the final V1 closeout milestone.

---

## Purpose

All implementation phases A-0 through B-6 are complete. The single remaining V1 closeout
milestone is:

> **Populate MLflow experiments with real metrics from a live Databricks workspace.**

This milestone closes the gap between "evaluation layer implemented locally" and "evaluation
run logged to a real MLflow tracking server." The V1 success criteria in `PROJECT_SPEC.md` §
Success Criteria #3 and #6 require at least two pipeline stages to have MLflow evaluation runs
with real metrics.

This runbook defines exactly what must be run, in what order, what evidence to capture, and
what to update in the repo afterward.

---

## Scope of This Checkpoint

**In scope:**
- One end-to-end local pipeline run against the sample FDA warning letter fixture
- MLflow logging of evaluation metrics for at least Bronze + Silver (minimum), preferably all four stages
- Evidence capture: MLflow experiment names, run IDs, key metrics

**Not in scope — even if the run succeeds:**
- Enterprise deployment
- Production credentials or workspace configuration
- Live Bedrock integration
- Multi-domain pipeline execution (V2+)
- Automated orchestration or Jobs/Workflows
- Retroactively claiming the A-3B bootstrap SQL results as MLflow-logged (they are not)

---

## Prerequisites

### Software

| Requirement | Check |
|---|---|
| Python 3.9+ | `python --version` |
| `pydantic` v2 | `pip show pydantic` |
| `mlflow` | `pip show mlflow` |

Install if needed:

```bash
pip install pydantic mlflow
```

### Databricks Workspace

The `--mlflow` flag in the evaluation scripts uses the standard MLflow client. To log to a
Databricks-hosted MLflow tracking server, set these environment variables **ephemerally in your
shell session** before running — do not write these values to any file in this repo:

```bash
export MLFLOW_TRACKING_URI=databricks
export DATABRICKS_HOST=<your-workspace-url>   # e.g. https://adb-XXXX.azuredatabricks.net
export DATABRICKS_TOKEN=<your-personal-access-token>
```

> **No-secrets rule**: These values must never be committed to this repo. Set them only in
> your terminal session. The `.gitignore` already excludes `databricks_host`, `databricks_token`,
> `.env`, and `databricks.cfg`. Verify before committing with `git status`.

If you want to run a purely local MLflow evaluation first (no Databricks tracking server),
omit the environment variables above. MLflow will log to a local `mlruns/` directory (also
git-ignored).

---

## What Is Already Implemented vs What Is Manual

| Item | Status |
|---|---|
| Full Bronze → Silver → Gold pipeline | ✅ Implemented — runs locally |
| Evaluation scripts for all four layers | ✅ Implemented — run locally |
| `--mlflow` flag in `run_evaluation.py` | ✅ Implemented — logs to configured tracking URI |
| Sample FDA warning letter fixture | ✅ Present at `examples/fda_warning_letter_sample.md` |
| MLflow experiment names defined in code | ✅ `caseops/pipeline/end_to_end` and per-layer experiments |
| Databricks workspace available | **Manual** — requires your personal workspace |
| MLflow tracking URI configured in shell | **Manual** — set as environment variables, not committed |
| Running the commands in this runbook | **Manual** |
| Capturing and recording evidence | **Manual** |
| Updating repo docs after successful run | **Manual** — see post-run section below |

**No code changes are needed before running.** The evaluation layer is complete.

---

## Pre-Flight Checklist

Before executing, confirm each of the following:

- [ ] `python --version` returns 3.9 or higher
- [ ] `pip show mlflow` shows mlflow is installed
- [ ] `pip show pydantic` shows pydantic v2 is installed
- [ ] `MLFLOW_TRACKING_URI`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN` are set in the current shell (if targeting Databricks MLflow)
- [ ] `examples/fda_warning_letter_sample.md` exists in the repo
- [ ] No credentials or workspace URLs are present in any tracked file (`git status` is clean)
- [ ] Working directory is the repo root: `pwd` returns the `databricks-caseops-lakehouse/` path

---

## Commands — Exact Execution Order

Run all commands from the repo root.

### Step 1 — Generate local pipeline artifacts

```bash
# Bronze: ingest the sample FDA warning letter
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev
```

Expected: `output/bronze/<bronze_record_id>.json` written.

```bash
# Silver: extract structured fields from Bronze
python src/pipelines/extract_silver.py --input-dir output/bronze
```

Expected: `output/silver/<extraction_id>.json` written.

```bash
# Gold: classify and route Silver records
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze
```

Expected: `output/gold/<gold_record_id>.json` written. Export payload at
`output/gold/exports/regulatory_review/<document_id>.json` if export-ready.

### Step 2 — Run the full evaluation pass with MLflow logging

```bash
python src/evaluation/run_evaluation.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold \
  --mlflow
```

Expected output:
- Console: metrics for Bronze, Silver, Gold, and Traceability layers
- `output/eval/report_<id>.json` — machine-readable full report
- `output/eval/report_<id>.txt` — human-readable summary
- MLflow run logged to `caseops/pipeline/end_to_end` experiment

### Step 3 — Verify the MLflow run was logged

If using Databricks MLflow: open your workspace UI, navigate to
**Experiments → caseops/pipeline/end_to_end**, and confirm the run appears with metrics.

If using local MLflow:
```bash
mlflow ui
```
Then open `http://localhost:5000` and verify the `caseops/pipeline/end_to_end` experiment.

### Step 4 (optional) — Run per-layer evaluators with MLflow logging

For explicit per-stage MLflow runs (separate experiments per layer):

```bash
python src/evaluation/eval_bronze.py --input-dir output/bronze --mlflow
python src/evaluation/eval_silver.py --input-dir output/silver --mlflow
python src/evaluation/eval_gold.py --input-dir output/gold --mlflow
python src/evaluation/eval_traceability.py \
  --bronze-dir output/bronze \
  --silver-dir output/silver \
  --gold-dir output/gold \
  --mlflow
```

Each logs to its own experiment:
- `caseops/bronze/parse_quality`
- `caseops/silver/extraction_quality`
- `caseops/gold/classification_quality`
- `caseops/pipeline/traceability`

---

## What Success Looks Like

The checkpoint is complete when all of the following are true:

1. All four pipeline artifacts exist locally (`output/bronze/`, `output/silver/`, `output/gold/`)
2. An MLflow run exists in the configured tracking server (Databricks or local) for at least
   `caseops/pipeline/end_to_end`
3. The run shows scalar metrics for Bronze parse quality, Silver extraction quality, Gold
   classification quality, and Traceability completeness
4. `report_<id>.json` and `report_<id>.txt` are present in `output/eval/`
5. No threshold warnings appear that indicate a broken pipeline state (warnings about null
   confidence or placeholder run IDs from the A-3B bootstrap path are expected and non-blocking)

**Minimum acceptable**: Bronze + Silver metrics logged to MLflow (2 of 4 stages).
**Full closeout**: All four stages logged.

---

## What Does NOT Get Claimed Even If This Succeeds

| Claim | Accurate? |
|---|---|
| "Pipeline runs on real Databricks AI Functions" | No — local run uses rule-based extractors, not `ai_extract`/`ai_classify` |
| "A-3B bootstrap results are MLflow-logged" | No — A-3B used manual SQL evaluation; those results are not in MLflow |
| "Production deployment validated" | No — this is a personal, non-production checkpoint |
| "Multi-domain pipeline validated" | No — FDA warning letters only |
| "Live Bedrock integration complete" | No — downstream integration is not part of V1 |
| "Enterprise Databricks credentials tested" | No — personal workspace only |

The checkpoint proves: the evaluation layer can log real metrics to a real MLflow tracking
server, closing the local-only gap for V1. It does not prove production readiness.

---

## Post-Run Evidence Checklist

After a successful run, capture the following before updating the repo:

### MLflow Evidence
- [ ] Experiment name(s) where runs were logged (e.g. `caseops/pipeline/end_to_end`)
- [ ] Run ID(s) — copy from MLflow UI or CLI output
- [ ] Key metrics observed (copy from console output or MLflow UI):
  - `bronze_parse_success_rate`
  - `silver_schema_validity_rate`
  - `silver_mean_field_coverage_pct`
  - `gold_classification_success_rate`
  - `gold_export_ready_rate`
  - `traceability_gold_to_silver_link_rate`
  - `traceability_silver_to_bronze_link_rate`
- [ ] Whether any threshold warnings fired
- [ ] Whether bootstrap path was detected (`bootstrap_path_detected` flag in report)

### Artifact Evidence
- [ ] Path of `report_<id>.json` generated
- [ ] Path of `report_<id>.txt` generated
- [ ] Screenshot of MLflow experiment UI (optional but recommended for portfolio record)

### Environment Evidence
- [ ] MLflow tracking URI used (Databricks or local)
- [ ] Date of run

---

## Repo Updates Required After Successful Run

Update these files **after** the run is confirmed successful. Do not mark anything complete
before the run has actually been executed.

### `docs/roadmap.md` — Milestone Markers section

Change the final unchecked milestone from:

```
- [ ] MLflow experiments populated with real metrics from a live Databricks workspace (deferred — requires live execution)
```

To:

```
- [x] MLflow experiments populated with real metrics from a live Databricks workspace (completed <date>; experiment: <experiment_name>; run_id: <run_id>)
```

### `README.md` — Project Status section

Add a brief note under the A-4 section that the MLflow evaluation checkpoint was completed,
with the experiment name and run date. Keep it factual — no exaggeration of what was validated.

### `docs/databricks-bootstrap.md` (optional)

If the run uses a Databricks-hosted MLflow tracking server from the same personal workspace
as A-3B, add a note under a new § MLflow Evaluation Checkpoint section recording the
experiment name, run ID, and date. Not required if using local MLflow only.

---

## What Remains for V2

The following are explicitly **not** V1 concerns and should not be started during or after
this checkpoint:

- Multi-domain pipelines (CISA advisories, incident reports, SOPs)
- Live Bedrock integration endpoint
- Human-in-the-loop review workflow
- Automated reprocessing on failure
- Streaming or near-real-time ingestion
- Cross-case analytics or KPI reporting
- Enterprise deployment or production Jobs/Workflows

---

## No-Secrets Confirmation

This runbook does not contain any:
- Workspace URLs
- Personal access tokens
- Databricks host values
- Service principal credentials
- Catalog or Volume paths specific to any user's personal workspace

All environment-specific values are set ephemerally in the shell session only.
