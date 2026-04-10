# Databricks Bootstrap — A-3B Personal Platform Validation

> This document records the personal Databricks workspace bootstrap completed as Phase A-3B.
> It is a factual record of what was validated, under what conditions, and what it does and does not prove.
> It is not a deployment guide and does not imply production readiness.

---

## Purpose

Phases A-0 through A-3 established the repo's local-safe implementation: Python schemas, pipeline scripts, evaluation runners, and a local fixture-based demo. These phases validated project structure and design intent but deferred all Databricks AI Function execution to a later platform pass.

A-3B is that platform pass. It documents a controlled, personal-workspace bootstrap that confirmed the pipeline's core SQL flow executes correctly against real documents using real Databricks AI Functions.

This phase exists to:

- Reduce the gap between placeholder architecture and validated execution
- Record the real platform-level behavior observed in a non-production environment
- Add safe, reusable SQL assets to the repo reflecting the validated flow
- Keep the public repo honest and portfolio-worthy
- Prepare cleanly for the A-4 formal evaluation and observability phase

---

## Environment

| Property | Value |
|---|---|
| Platform | Databricks (personal workspace) |
| Edition | Free / personal learning environment |
| SQL warehouse type | Serverless SQL warehouse |
| Catalog | `caseops` |
| Schemas | `raw`, `bronze`, `silver`, `gold` |
| Volume | `caseops.raw.documents` (managed) |
| Document corpus | Public FDA warning letter PDFs (sample batch) |

**This is a personal, non-production, non-enterprise environment.**

It does not use service principals, external locations, enterprise networking, or any organizational Databricks account. No organizational data was used.

---

## Public Data Boundary

Only publicly available, freely downloadable FDA warning letter PDFs were used in this bootstrap. These are published by the U.S. Food and Drug Administration and are in the public domain. No proprietary, confidential, or personally identifiable information was used at any stage.

The documents were uploaded manually to the managed volume at:

```
/Volumes/caseops/raw/documents/fda_warning_letters/
```

This path is defined in `config/databricks.resources.example.yml` and is the only path referenced by the bootstrap SQL assets.

---

## Governed Object Layout

The following Unity Catalog objects were created and used during the bootstrap:

```
Unity Catalog
└── catalog: caseops
    ├── schema: raw
    │   └── Volume: documents
    │       └── fda_warning_letters/   ← 4 public FDA sample PDFs uploaded here
    ├── schema: bronze
    │   ├── Table: parsed_documents_smoke   ← smoke test output
    │   └── Table: parsed_documents         ← full bootstrap Bronze table
    ├── schema: silver
    │   └── Table: extracted_records_smoke  ← Silver extraction output
    └── schema: gold
        └── Table: ai_ready_assets_smoke    ← Gold classification and routing output
```

---

## Validated SQL Steps

The bootstrap was executed as five sequential SQL statements in the serverless SQL warehouse. Source files are in `notebooks/bootstrap/`.

### Step 1 — Bronze Parse Smoke (`01_bronze_parse_smoke.sql`)

Ran `ai_parse_document` against the raw volume using `READ_FILES` with `format => 'binaryFile'`. Confirmed that all 4 PDFs could be read and parsed. Output written to `caseops.bronze.parsed_documents_smoke`.

### Step 2 — Bronze Bootstrap v1 (`02_bronze_bootstrap_v1.sql`)

Full Bronze ingest with provenance fields: `document_id` (UUID), `file_name`, `file_size_bytes`, `file_hash` (SHA-256), `ingested_at`, `parsed_at`, `parse_status`, `parsed_content`, `pipeline_run_id`, `schema_version`. Output written to `caseops.bronze.parsed_documents`.

### Step 3 — Silver Extraction Smoke v1 (`03_silver_extract_smoke_v1.sql`)

Ran `ai_extract` against `parsed_content` from the Bronze table using the FDA warning letter prompt schema. Fields extracted: `issuing_office`, `recipient_company`, `recipient_name`, `issue_date`, `corrective_action_requested`, `summary`. Output written to `caseops.silver.extracted_records_smoke`.

### Step 4 — Gold Classification and Routing Smoke v1 (`04_gold_classify_route_smoke_v1.sql`)

Ran `ai_classify` against Silver extraction results using a two-class taxonomy: `fda_warning_letter` and `unknown`. Applied rule-based routing: documents classified as `fda_warning_letter` are routed to `regulatory_review`; all others to `quarantine`. Output written to `caseops.gold.ai_ready_assets_smoke`.

### Step 5 — Bootstrap Evaluation v1 (`05_bootstrap_evaluation_v1.sql`)

Joined all three smoke tables on `document_id` to validate full lineage and produce summary counts. This is a manual SQL evaluation proxy for the MLflow evaluation runs planned in A-4.

---

## Observed Results

All five SQL steps completed successfully in the personal serverless SQL warehouse.

| Metric | Observed Value |
|---|---|
| `total_documents` | 4 |
| `bronze_success_count` | 4 |
| `silver_record_count` | 4 |
| `gold_export_ready_count` | 3 |
| `quarantine_count` | 1 |
| `full_lineage_count` | 4 |

### Quarantine record

One document was routed to `quarantine` with `export_ready = false`. This is correct behavior: the routing logic requires `ai_classify` to return the label `fda_warning_letter` for a record to be export-ready. If the classifier returns `unknown` for any record in the batch, that record is quarantined.

This is an intended governance signal, not a pipeline failure. It confirms the routing condition is functioning as designed.

### `classification_confidence` is NULL

The `classification_confidence` field is stored as `CAST(NULL AS DOUBLE)` in the Gold smoke table. The `ai_classify` SQL AI Function, as called in this bootstrap implementation, does not return a scalar confidence score in the response variant at this stage. This is a known, explicitly documented bootstrap-stage implementation detail.

This does not indicate a bug or hidden failure. The A-4 evaluation phase will address:
- Whether a confidence score is extractable from the `document_type_result` variant
- How to instrument classification quality evaluation in MLflow without a scalar confidence value
- Whether to use a proxy confidence measure or a different evaluation approach

---

## What This Proves

- The `caseops` Unity Catalog layout (catalog, schemas, managed volume) is functional in a personal Databricks workspace
- `ai_parse_document`, `ai_extract`, and `ai_classify` can be called as SQL AI Functions using `READ_FILES` from a managed volume
- A four-document FDA warning letter batch can be processed end-to-end from raw PDFs to Gold routing records using only SQL
- Full `document_id`-based lineage is present across all three layers
- Rule-based routing (classification result → routing label) works correctly
- The quarantine path is functional: records that do not match the expected class are quarantined, not silently passed through

---

## What This Does Not Prove Yet

- **MLflow evaluation is not implemented.** No MLflow experiments were run. The bootstrap evaluation SQL in step 5 is a manual proxy, not a rerunnable, logged MLflow evaluation run. This is A-4 scope.
- **Production orchestration is not implemented.** No Databricks Jobs, Workflows, or Asset Bundles were configured. This was a series of manually executed SQL statements.
- **No service principal or enterprise auth was used.** The bootstrap used personal workspace credentials and a personal serverless warehouse.
- **No external location was used.** The managed volume is sufficient for personal bootstrap but is not equivalent to an enterprise external storage configuration.
- **Classification confidence scoring is not complete.** `classification_confidence` is NULL and requires follow-up investigation in A-4.
- **Multi-document class routing is not tested.** The batch contained only FDA warning letters. CISA advisories, incident reports, and other document classes are planned for V2+.
- **Scale is not validated.** Four documents is a smoke-test batch. Production throughput, cost, and latency characteristics are not known from this bootstrap.

---

## Security and Public-Repo Boundary

This repository does not contain and will never contain:

- Databricks workspace URLs
- Account IDs or organization IDs
- Personal access tokens or service principal credentials
- Databricks CLI configuration files (`.databrickscfg`)
- Any authentication artifacts from the personal workspace

The `.gitignore` excludes `.databricks/`, `.databrickscfg`, `.env`, `.env.*`, `*.local.yml`, and `*.local.yaml`. These exclusions are enforced at the repo level.

The only Databricks-specific artifact committed to this repo is the example resource layout YAML at `config/databricks.resources.example.yml`, which contains no credentials or environment-specific values.

---

## Next Steps

A-3B consolidation is complete when:

- [ ] All five SQL bootstrap files are committed under `notebooks/bootstrap/`
- [ ] `config/databricks.resources.example.yml` is committed
- [ ] This documentation file is committed
- [ ] `PROJECT_SPEC.md`, `ARCHITECTURE.md`, `README.md`, `docs/roadmap.md`, and `docs/CURSOR_CONTEXT.md` are updated to reflect A-3B

A-4 begins after this consolidation. A-4 scope: MLflow experiment structure, per-document trace records, evaluation runner scripts, and evaluation summary reporting across all three layers.
