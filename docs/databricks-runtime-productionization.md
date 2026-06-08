# Databricks Runtime Productionization (Phase 4)

> **Status**: Active (phase 4 implementation scaffold in place; runtime smoke workflow still requires workspace execution)
> **Scope**: Reproducible Databricks runtime execution for already implemented runtime adapters and Delta write/read paths
> **Source authority**: [`README.md`](../README.md), [`PROJECT_SPEC.md`](../PROJECT_SPEC.md), [`ARCHITECTURE.md`](../ARCHITECTURE.md)
> **No-credentials rule**: This repository keeps workspace URLs, account IDs, tokens, and personal identifiers out of source control.

---

## 1) Purpose and boundary

Phase 4 converts the existing adapter-ready code into a reproducible Databricks execution path.

The boundary remains unchanged:

- This repo controls the upstream preparation, schema-constrained output, validation, and handoff artifact behavior.
- It does **not** implement Bedrock/RAG/agent runtime logic.
- Local operation remains workspace-credential-free and deterministic.
- Runtime operation requires an external Databricks runtime context provided by operator deployment.

## 2) Current implementation status

- `src/pipelines/databricks_runtime.py`
  - Runtime adapters implemented for:
    - `DatabricksAiParseRuntimeAdapter`
    - `DatabricksAiExtractRuntimeAdapter`
    - `DatabricksAiClassifyRuntimeAdapter`
  - `DeltaTableIO` and `DeltaTableTargets` for table read/write
  - Local-safe, non-PySpark import path
- `src/utils/environment_config.py`
  - Environment-derived table naming via `get_environment_config()`
  - `caseops_<env>.{bronze,silver,gold}` targets
- `src/pipelines/run_databricks_pipeline.py`
  - Workspace-first stage orchestrator for:
    - `bronze`
    - `silver`
    - `gold`
    - `all`
    - `delivery_validation`
  - Stage preflight checks for table readability and table-name validation
  - Runtime Bronze `file_hash` uses SHA-256 of raw source bytes from Spark
    `binaryFile` content metadata; path-derived hashes are not accepted for
    runtime Bronze writes.
  - Silver and Gold runtime reads are scoped by `pipeline_run_id` before
    collection so a run does not unintentionally reprocess historical rows.
- `config/databricks-runtime-bundle/databricks.yml`
  - Job scaffold for stage-level Databricks execution using the same runner
  - No secrets or workspace URLs committed
- `src/pipelines/runtime_smoke_validation.py`
  - Local-safe validator for a complete sanitized Phase 4 smoke evidence
    package
  - Does not call Databricks APIs; it validates captured artifacts only
- `src/pipelines/runtime_smoke_plan.py`
  - Local-safe generator for a run-scoped smoke capture plan before workspace
    execution
  - Produces expected artifact paths, commands, and sanitization rules; it is
    not runtime evidence

## 3) Runtime path target (environment-aware)

Runtime stages execute against:

- Bronze: `caseops_<env>.bronze.parsed_documents`
- Silver: `caseops_<env>.silver.extracted_records`
- Gold: `caseops_<env>.gold.ai_ready_assets`

`raw` volume input for bronze comes from `EnvironmentConfig.raw_volume_path`, derived from `CASEOPS_ENV`:

- `caseops_dev.raw.documents`
- `caseops_staging.raw.documents`
- `caseops_prod.raw.documents`

## 4) Runtime execution contract (implemented)

### 4.1 Stage entrypoints

Use:

```bash
.venv/bin/python src/pipelines/run_databricks_pipeline.py \
  --stage bronze \
  --environment dev \
  --output-mode append
```

Equivalent for other stages:

- `--stage silver`
- `--stage gold`
- `--stage all`
- `--stage delivery_validation`
- `--check-only` (preflight only; no writes)
- `--check-write-permissions` (optional write-permission preflight on output tables)

Example:

```bash
.venv/bin/python src/pipelines/run_databricks_pipeline.py \
  --stage all \
  --environment dev \
  --check-only
```

### 4.2 Required runtime resources per stage

- `bronze`: readable `raw_volume_path` via Spark `binaryFile` scan
- `silver`: readable `bronze_table`
- `gold`: readable `silver_table`
- `all`: readable raw input via Spark `binaryFile` scan for bronze source volume (bronze/silver/gold are produced in one run)
- `--check-write-permissions`: additionally validates output-table write permissions for the requested stage targets.

### 4.2.1 Deployment/scaffold execution

Deploy and run the corresponding Databricks job through `databricks.yml`:

```bash
export CASEOPS_DATABRICKS_HOST=...
export CASEOPS_CLUSTER_ID=...
export CASEOPS_WORKSPACE_ROOT=/Workspace/Shared/caseops-caseops-lakehouse
export CASEOPS_RUNNER_SCRIPT_PATH=/Workspace/Shared/caseops-caseops-lakehouse/src/pipelines/run_databricks_pipeline.py
export CASEOPS_ENV=dev

databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run caseops-runtime-all --target dev
```

To run an individual stage, map to the matching job key:

- bronze: `caseops-runtime-bronze`
- silver: `caseops-runtime-silver`
- gold: `caseops-runtime-gold`
- all: `caseops-runtime-all`
- delivery_validation local-only baseline: `caseops-runtime-delivery-validation-local`
- delivery_validation provisioned personal-workspace evidence: `caseops-runtime-delivery-validation-provisioned`
- check-only: `caseops-runtime-check-only`
- check-write-permissions: `caseops-runtime-check-write-permissions`

Keep Databricks CLI syntax aligned to your installed CLI version and target naming.

### 4.3 Environment and safety knobs

- `--environment` (`dev`, `staging`, `prod`); defaults to `CASEOPS_ENV` then `dev`
- Explicitly pass model versions: `--parse-version`, `--extract-version`, `--classify-version`
- Reproducible writes through explicit `--output-mode`
- One `pipeline-run-id` can be reused across multi-stage `--stage all` execution
- `--source-pipeline-run-id` scopes Silver/Gold reads to an explicit upstream
  batch. It defaults to `--pipeline-run-id`. Use the same run ID across
  separated Bronze, Silver, and Gold jobs when running stages independently.
- `--check-only` validates preconditions for the requested stage and returns without writing output tables
- `--check-write-permissions` performs stage-aware output permission probes without mutating rows.
- For append/error/ignore/errorifexists modes, output tables are validated as pre-existing targets; overwrite mode allows missing targets to be created.

### 4.4 Delivery validation from runtime runs

Validate produced delivery artifacts after Databricks provisioning/verification with:

```bash
.venv/bin/python src/pipelines/run_databricks_pipeline.py \
  --stage delivery_validation \
  --pipeline-run-id <pipeline-run-id> \
  --delivery-event-path output/delivery/delivery_event_<pipeline-run-id>.json \
  --share-manifest-path output/delivery/delta_share_preparation_manifest.json \
  --runtime-evidence-path output/validation/runtime_evidence/runtime_evidence.json \
  --workspace-mode personal_databricks \
  --validation-output-dir output/validation
```

## 5) Deployment scaffold

`config/databricks-runtime-bundle/databricks.yml` wires stage tasks to
`src/pipelines/run_databricks_pipeline.py` for:

- bronze
- silver
- gold
- all
- delivery_validation local-only baseline
- delivery_validation provisioned personal-workspace evidence

This file is intentionally environment-agnostic and contains placeholders for cluster/workspace paths.
Operators must replace placeholders in their deployment environment. Both
delivery validation jobs require:

- `CASEOPS_DELIVERY_PIPELINE_RUN_ID`
- `CASEOPS_DELIVERY_EVENT_PATH`
- `CASEOPS_SHARE_MANIFEST_PATH`

The provisioned personal-workspace validation job also requires:

- `CASEOPS_RUNTIME_EVIDENCE_PATH`

## 6) Smoke-test evidence checklist (repeatable)

Before a workspace smoke run, generate a run-specific capture plan:

```bash
.venv/bin/python src/pipelines/runtime_smoke_plan.py \
  --pipeline-run-id <pipeline-run-id> \
  --environment dev \
  --output-root output \
  --output-dir output/validation
```

This writes:

- `output/validation/runtime_smoke_capture_plan_<pipeline_run_id>.json`
- `output/validation/runtime_smoke_capture_plan_<pipeline_run_id>.txt`

The capture plan is an operator checklist only. It does not prove Databricks
execution, provisioning, or validation.

For each smoke run, produce the minimum artifacts below and keep sensitive values stripped:

1. Runtime run metadata:
   - `pipeline_run_id`
   - `--environment`
2. Delivery artifacts:
   - `output/delivery/delivery_event_<pipeline_run_id>.json`
   - `output/delivery/delta_share_preparation_manifest.json`
3. Workspace checks:
   - share/table visibility check (`SHOW ALL IN SHARE` equivalent)
   - Gold source-table row checks in target catalog schema
   - routing-label visibility check
4. Evidence artifact:
   - `output/validation/runtime_evidence/runtime_evidence_<pipeline_run_id>.json` (sanitized)
5. C-2 result artifact from runner:
   - `output/validation/delivery_validation_<pipeline_run_id>.json`
6. Phase 4 smoke package validation result:
   - `output/validation/runtime_smoke_validation_<pipeline_run_id>.json`

Sanitize all evidence:

- no workspace URLs
- no account IDs
- no recipient IDs
- no tokens
- no personal identifiers

Validate the complete smoke package locally after the workspace run:

```bash
.venv/bin/python src/pipelines/runtime_smoke_validation.py \
  --pipeline-run-id <pipeline-run-id> \
  --environment dev \
  --delivery-event-path output/delivery/delivery_event_<pipeline-run-id>.json \
  --share-manifest-path output/delivery/delta_share_preparation_manifest.json \
  --runtime-evidence-path output/validation/runtime_evidence/runtime_evidence_<pipeline-run-id>.json \
  --delivery-validation-result-path output/validation/delivery_validation_<pipeline-run-id>.json \
  --output-dir output/validation
```

The result reaches `smoke_status = ready_for_phase4_closeout` only when:

- delivery event, share manifest, runtime evidence, and C-2 validation result
  all parse successfully
- all artifacts agree on the same `pipeline_run_id`
- share manifest status is `provisioned`
- runtime evidence contains all required query and assertion checks
- C-2 delivery validation status is `validated` with `end_to_end` scope in
  `personal_databricks` mode
- checked artifacts do not contain URL, token, activation-link, or credential
  patterns

## 7) Acceptance criteria (Phase 4)

- [x] Runtime adapters (`ai_parse_document`, `ai_extract`, `ai_classify`) wired through a single Databricks entrypoint.
- [x] Delta table write/read execution path present (`DeltaTableIO`) and environment-derived.
- [x] Deployment scaffold added under `config/databricks-runtime-bundle/`.
- [x] Runtime validation stage implemented (`delivery_validation`) with local-safe evidence path.
- [x] Deterministic preflight/check-only path added for workspace runs (`--check-only`).
- [x] Run-scoped smoke capture plan generator added for repeatable evidence packaging.
- [x] Local-safe Phase 4 smoke package validator added for captured workspace evidence.
- [ ] End-to-end workspace repeatable smoke check has been executed against staging/prod (pipeline remains scoped to dev/staging/prod by environment).
- [x] No secrets or workspace-identifying values committed in scoped files.

This document remains the single source of truth for Phase 4 execution detail.
