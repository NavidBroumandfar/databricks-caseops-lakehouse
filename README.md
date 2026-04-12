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

**V1 is complete. V2 Phase C is complete. V2 Phases D-0, D-1, and D-2 are complete. V2 Phase E-0 is complete.** Phases A-0 through B-6 are complete, and the final V1 MLflow live-workspace evaluation checkpoint has been successfully executed. Phase C-1 (Export Delivery Implementation) and Phase C-2 (Runtime Integration Validation) have been implemented. Phase D-0 (Multi-Domain Framework) is complete. Phase D-1 (CISA Advisory Domain) is complete. Phase D-2 (Incident Report Domain) is complete — the pipeline now executes **three active domains**: FDA warning letters, CISA cybersecurity advisories, and incident reports. Phase E-0 (Human Review and Reprocessing) is complete — the pipeline now produces a structured human review queue for quarantined and contract-blocked records, and the repo gains a governed review decision and reprocessing request artifact layer. Phases E-1 and E-2 are not yet started.

This remains a controlled, portfolio-safe, non-production project — no enterprise deployment, no production credentials, no live Bedrock integration, no live orchestration. **V2 has started. Phase C is complete. Phases D-0, D-1, and D-2 are complete. Phase E-0 is complete.** V2 phases (C: live handoff integration; D: multi-domain expansion; E: enterprise operational hardening) are documented in [`PROJECT_SPEC.md`](./PROJECT_SPEC.md) § V2 Scope and [`docs/roadmap.md`](./docs/roadmap.md) § V2 — Future Work.

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

**V1 MLflow Live-Workspace Checkpoint** — The final V1 closeout milestone has been completed: all four evaluation stages (Bronze parse quality, Silver extraction quality, Gold classification quality, pipeline traceability) were executed end-to-end and logged to real Databricks MLflow experiments in a personal, non-production workspace. This closes the gap between "evaluation layer implemented locally" and "evaluation run logged to a real MLflow tracking server." The experiments were populated using the `caseops/` experiment root path via `Databricks-safe MLflow experiment path resolution` (`src/evaluation/mlflow_experiment_paths.py`). This is a personal workspace validation only — not an enterprise deployment and not connected to live Bedrock integration.

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

**Phase B-2 — Contract-Enforced Export Materialization** is complete. B-2 makes the pipeline obey the B-1 contract during real export materialization — invalid downstream payloads cannot silently pass as valid exports:

| Deliverable | Path | Status |
|---|---|---|
| Contract-enforced export path | `src/pipelines/classify_gold.py` (B-1 validation gates every write) | ✅ Updated B-2 |
| B-2 materialization test suite | `tests/test_b2_export_materialization.py` (18 tests) | ✅ New B-2 |
| Invalid payload fixture | `examples/invalid_export_payload_missing_fields.json` | ✅ New B-2 |
| Quarantine record fixture | `examples/quarantine_gold_record.json` | ✅ New B-2 |

**Phase B-3 — Export Packaging Refactor and Handoff Service Boundary** is complete. B-3 extracts export/handoff materialization into a dedicated module with a clean internal service boundary:

| Deliverable | Path | Status |
|---|---|---|
| Export/handoff module | `src/pipelines/export_handoff.py` | ✅ New B-3 |
| Simplified pipeline | `src/pipelines/classify_gold.py` (delegates to `execute_export`) | ✅ Updated B-3 |
| B-3 test suite | `tests/test_b3_export_handoff.py` (28 tests) | ✅ New B-3 |

Module boundary: `classify_gold.py` assembles the Gold record and delegates all export packaging to `export_handoff.py`. B-2 behavior is preserved exactly.

**Phase B-4 — Export Outcome Observability and Handoff Reporting** is complete. B-4 makes Gold → Bedrock handoff outcomes operationally visible and reviewable at batch level. Each pipeline run now produces a structured `HandoffBatchReport` showing what was exported, quarantined, contract-blocked, or skipped, and why.

| Deliverable | Path | Status |
|---|---|---|
| Handoff reporting module | `src/pipelines/handoff_report.py` | ✅ New B-4 |
| Pipeline integration | `src/pipelines/classify_gold.py` (outcome fields + `report_dir`) | ✅ Updated B-4 |
| B-4 test suite | `tests/test_b4_handoff_report.py` (68 tests) | ✅ New B-4 |

Outcome categories: `exported`, `quarantined`, `contract_blocked`, `skipped_not_export_ready`. Reason codes: `none`, `routing_quarantine`, `contract_validation_failed`, `export_not_attempted`. The batch report is written as JSON + text artifacts when `--report-dir` is provided.

**Phase B-5 — Handoff Batch Manifest and Review Bundle** is complete. B-5 packages the full Gold/export batch into a single, coherent, reviewable batch handoff bundle. A `HandoffBatchManifest` links batch metadata, aggregate outcome counts, per-record artifact references (Gold + export paths by outcome category), and B-4 report artifacts into one reviewable unit.

| Deliverable | Path | Status |
|---|---|---|
| Batch manifest/review bundle module | `src/pipelines/handoff_bundle.py` | ✅ New B-5 |
| Pipeline integration | `src/pipelines/classify_gold.py` (`bundle_dir` + `--bundle-dir`) | ✅ Updated B-5 |
| B-5 test suite | `tests/test_b5_handoff_bundle.py` (84 tests) | ✅ New B-5 |
| Expected manifest fixture | `examples/expected_handoff_batch_manifest.json` | ✅ New B-5 |

The bundle is written as JSON + text artifacts when `--bundle-dir` is provided. When `--report-dir` is also provided, the bundle references the B-4 report artifacts. The manifest captures exported_records (with export_artifact_path), quarantined_records, contract_blocked_records, and skipped_records.

**Phase B-6 — Handoff Bundle Integrity and Consistency Validation** is complete. B-6 proves the B-5 bundle is internally trustworthy and review-safe. A dedicated validator checks the bundle for structural correctness, count consistency, reference integrity, identifier uniqueness, and filesystem path existence — locally and deterministically, with no live dependencies.

| Deliverable | Path | Status |
|---|---|---|
| Bundle validation module | `src/pipelines/handoff_bundle_validation.py` | ✅ New B-6 |
| B-6 test suite | `tests/test_b6_bundle_validation.py` (92 tests) | ✅ New B-6 |

The validator exposes `validate_handoff_bundle(bundle_json_path)` for file-based validation and `validate_handoff_bundle_from_manifest(manifest, check_paths=False)` for in-memory validation. It produces a `BundleValidationResult` with `bundle_valid`, `failed_checks`, `count_mismatches`, `missing_paths`, `duplicate_identifiers`, `contradictions`, and full per-check detail. 24 explicit checks across 5 categories.

**Phase C-1 — Export Delivery Implementation** is complete. C-1 implements the upstream producer-side delivery augmentation, adding the Delta Sharing-oriented delivery layer on top of the existing B-phase handoff preparation. The V1 file export path is fully preserved. C-1 is additive and honest: all delivery events carry `status = 'prepared'`; runtime validation (live share query, consumer receipt) is Phase C-2.

| Deliverable | Path | Status |
|---|---|---|
| Delivery event schema | `src/schemas/delivery_event.py` | ✅ New C-1 |
| Delivery event materialization | `src/pipelines/delivery_events.py` | ✅ New C-1 |
| Delta Share prep layer | `src/pipelines/delta_share_handoff.py` | ✅ New C-1 |
| v0.2.0 provenance fields | `src/schemas/gold_schema.py` (3 optional fields + `SCHEMA_VERSION_V2`) | ✅ Updated C-1 |
| Pipeline delivery integration | `src/pipelines/classify_gold.py` (`--delivery-dir`) | ✅ Updated C-1 |
| Delivery event fixture | `examples/expected_delivery_event.json` | ✅ New C-1 |
| Delivery event test suite | `tests/test_delivery_events.py` | ✅ New C-1 |
| Delta Share handoff test suite | `tests/test_delta_share_handoff.py` | ✅ New C-1 |

The `--delivery-dir` flag activates C-1 delivery augmentation: export payloads are written at `schema_version: v0.2.0` with delivery provenance fields populated; a `DeliveryEvent` artifact (JSON + text) is written per batch; a `SharePreparationManifest` with Unity Catalog SQL DDL templates is written alongside. When `--delivery-dir` is omitted, V1 behavior is fully preserved.

**Phase C-2 — Runtime Integration Validation** is complete (producer-side validation layer). C-2 adds a bounded, honest, 15-check delivery-layer validation layer that validates the C-1 artifacts locally without requiring a live Databricks workspace. The producer-side validation produces an explicit integration health status (`not_provisioned` by default — the honest baseline before Unity Catalog share provisioning). Full runtime end-to-end validation (`validated` status) is achievable after executing the setup SQL in a personal Databricks workspace.

| Deliverable | Path | Status |
|---|---|---|
| Delivery validation schema | `src/schemas/delivery_validation.py` | ✅ New C-2 |
| Delivery validation logic | `src/pipelines/delivery_validation.py` | ✅ New C-2 |
| Validation result fixture | `examples/expected_delivery_validation_result.json` | ✅ New C-2 |
| C-2 runtime validation design and runbook | `docs/delivery-runtime-validation.md` | ✅ New C-2 |
| C-2 test suite | `tests/test_delivery_validation.py` (134 tests) | ✅ New C-2 |

Integration health states (C-2): `not_provisioned` (share designed, not yet in Unity Catalog — honest default), `partially_validated` (producer-side correct, share provisioned, no live queries run), `validated` (confirmed in personal Databricks workspace), `failed` (schema error, ID mismatch, or parse failure).

**Phase D-0 — Multi-Domain Framework** is complete. D-0 establishes the multi-domain framework layer as the architectural foundation for D-1 (CISA advisories) and D-2 (incident reports). FDA warning letters remain the only fully executable domain after D-0.

| Deliverable | Path | Status |
|---|---|---|
| Domain registry | `src/utils/domain_registry.py` | ✅ New D-0 |
| Domain schema registry | `src/schemas/domain_schema_registry.py` | ✅ New D-0 |
| Prompt routing framework | `src/utils/extraction_prompts.py` (`get_prompt_for_domain`) | ✅ Updated D-0 |
| Taxonomy D-0 extensions | `src/utils/classification_taxonomy.py` (`DOMAIN_ROUTING_MAP`, `is_domain_executable`, `resolve_routing_label_for_domain`) | ✅ Updated D-0 |
| Pipeline domain routing | `extract_silver.py`, `classify_gold.py` (`select_extractor`, `select_classifier`) | ✅ Updated D-0 |
| D-0 test suite | `tests/test_domain_registry.py` | ✅ New D-0 |

D-0 domain state post-D-0: `fda_warning_letter` → `active`; `cisa_advisory` → `planned` (D-1); `incident_report` → `planned` (D-2). Both are now `active` after D-1 and D-2.

**Phase D-1 — CISA Advisory Domain** is complete. D-1 is the first real multi-domain expansion built on the D-0 framework. CISA advisory records are now fully executable through the entire pipeline.

| Deliverable | Path | Status |
|---|---|---|
| `CISAAdvisoryFields` Pydantic schema + coverage helper | `src/schemas/silver_schema.py` | ✅ New D-1 |
| CISA schema registry activation | `src/schemas/domain_schema_registry.py` | ✅ Updated D-1 |
| CISA extraction prompt (`cisa_advisory_extract_v1`) | `src/utils/extraction_prompts.py` | ✅ New D-1 |
| CISA domain activation (status: ACTIVE) | `src/utils/domain_registry.py` | ✅ Updated D-1 |
| `security_ops` routing activation in V1_ROUTING_MAP | `src/utils/classification_taxonomy.py` | ✅ Updated D-1 |
| `LocalCISAAdvisoryExtractor` + `validate_cisa_extracted_fields` | `src/pipelines/extract_silver.py` | ✅ New D-1 |
| `LocalCISAAdvisoryClassifier` | `src/pipelines/classify_gold.py` | ✅ New D-1 |
| CISA Bedrock contract validation (`REQUIRED_CISA_EXTRACTED_FIELDS`) | `src/schemas/bedrock_contract.py` | ✅ Updated D-1 |
| CISA sample advisory fixture | `examples/cisa_advisory_sample.md` | ✅ New D-1 |
| Expected Silver/Gold CISA output fixtures | `examples/expected_silver_cisa_advisory.json`, `examples/expected_gold_cisa_advisory.json` | ✅ New D-1 |
| D-1 test suite (123 tests) | `tests/test_d1_cisa_domain.py` | ✅ New D-1 |

D-1 domain state: `fda_warning_letter` → `active` (V1); `cisa_advisory` → `active` (D-1 ✅); `incident_report` → `planned` (D-2 pending at time of D-1). `security_ops` is now an active routing label alongside `regulatory_review`.

**Phase D-2 — Incident Report Domain** is complete. D-2 is the second real multi-domain expansion built on the D-0 framework. Incident report records are now fully executable through the entire pipeline.

| Deliverable | Path | Status |
|---|---|---|
| `IncidentReportFields` Pydantic schema + coverage helper | `src/schemas/silver_schema.py` | ✅ New D-2 |
| Incident schema registry activation | `src/schemas/domain_schema_registry.py` | ✅ Updated D-2 |
| Incident extraction prompt (`incident_report_extract_v1`) | `src/utils/extraction_prompts.py` | ✅ New D-2 |
| Incident domain activation (status: ACTIVE) | `src/utils/domain_registry.py` | ✅ Updated D-2 |
| `incident_management` routing activation in V1_ROUTING_MAP | `src/utils/classification_taxonomy.py` | ✅ Updated D-2 |
| `LocalIncidentReportExtractor` + `validate_incident_extracted_fields` | `src/pipelines/extract_silver.py` | ✅ New D-2 |
| `LocalIncidentReportClassifier` | `src/pipelines/classify_gold.py` | ✅ New D-2 |
| Incident Bedrock contract validation (`REQUIRED_INCIDENT_EXTRACTED_FIELDS`) | `src/schemas/bedrock_contract.py` | ✅ Updated D-2 |
| Incident sample report fixture | `examples/incident_report_sample.md` | ✅ New D-2 |
| Expected Silver/Gold incident output fixtures | `examples/expected_silver_incident_report.json`, `examples/expected_gold_incident_report.json` | ✅ New D-2 |
| D-2 test suite (125 tests) | `tests/test_d2_incident_domain.py` | ✅ New D-2 |

D-2 domain state: `fda_warning_letter` → `active` (V1); `cisa_advisory` → `active` (D-1 ✅); `incident_report` → `active` (D-2 ✅). All three reference domains are now executable. `incident_management` is now an active routing label alongside `regulatory_review` and `security_ops`. No planned domains remain.

**Phase E-0 — Human Review and Reprocessing** is complete. E-0 adds a structured, upstream human review queue layer for records that should not flow cleanly through the automated path without human attention.

| Deliverable | Path | Status |
|---|---|---|
| Review queue schema | `src/schemas/review_queue.py` | ✅ New E-0 |
| Review decision schema | `src/schemas/review_decision.py` | ✅ New E-0 |
| Review queue derivation pipeline | `src/pipelines/review_queue.py` | ✅ New E-0 |
| Pipeline integration | `src/pipelines/classify_gold.py` (`--review-queue-dir`) | ✅ Updated E-0 |
| Review queue fixture | `examples/expected_review_queue.json` | ✅ New E-0 |
| Review decision fixture | `examples/expected_review_decision.json` | ✅ New E-0 |
| Reprocessing request fixture | `examples/expected_reprocessing_request.json` | ✅ New E-0 |
| E-0 test suite | `tests/test_e0_review_queue.py` (111 tests) | ✅ New E-0 |

The review queue is derived deterministically from pipeline summaries. Records with `outcome_category` == `quarantined`, `contract_blocked`, or `skipped_not_export_ready` with `unknown` document type enter the queue. Review reason categories: `quarantined`, `contract_blocked`, `extraction_failed`. Review decisions: `approve_for_export`, `confirm_quarantine`, `request_reprocessing`, `reject_unresolved`. The automated pipeline path is fully preserved — the review queue is additive and optional via `--review-queue-dir`. Phases E-1 (environment separation) and E-2 (governance monitoring) are not yet started.

Total test count: **1215 tests** across all pipeline stages, contract validation, export materialization, export handoff boundary, handoff outcome observability, batch handoff bundle packaging, bundle integrity validation, delivery event materialization, Delta Share preparation layer, delivery-layer runtime validation, D-0 multi-domain framework, D-1 CISA advisory domain, D-2 incident report domain, and E-0 human review queue and reprocessing layer.

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
# Export payload (if export-ready and contract-valid): output/gold/exports/regulatory_review/<document_id>.json
# Invalid payloads are blocked before write (B-2) — see contract_validation_errors in pipeline output

# Optional: produce a B-4 handoff batch report
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports

# Report artifacts: output/reports/handoff_report_<run_id>.json  (machine-readable)
#                   output/reports/handoff_report_<run_id>.txt   (human-readable)

# Optional: produce a B-5 batch manifest/review bundle (may be combined with --report-dir)
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports \
  --bundle-dir output/reports

# Bundle artifacts: output/reports/handoff_bundle_<run_id>.json  (machine-readable manifest)
#                   output/reports/handoff_bundle_<run_id>.txt   (human-readable review summary)
# The bundle references all per-record artifact paths and the B-4 report when both flags are used.

# Optional: run B-6 bundle integrity validation against the generated bundle
python -c "
from pathlib import Path
from src.pipelines.handoff_bundle_validation import validate_handoff_bundle, format_validation_result_text
import glob
bundles = sorted(glob.glob('output/reports/handoff_bundle_*.json'))
if bundles:
    result = validate_handoff_bundle(Path(bundles[-1]))
    print(format_validation_result_text(result))
"

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

## Running the C-1 Delivery Demo

Requires Python 3.9+ and `pydantic` (v2). Run the full Gold Demo first to generate artifacts.

```bash
# 1–3. Run the Bronze, Silver, Gold demos (if not already done)
python src/pipelines/ingest_bronze.py \
  --input examples/fda_warning_letter_sample.md \
  --document-class-hint fda_warning_letter \
  --source-system local_dev

python src/pipelines/extract_silver.py --input-dir output/bronze

# 4. Classify Gold with full delivery augmentation (C-1)
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports \
  --bundle-dir output/reports \
  --delivery-dir output/delivery

# New C-1 artifacts:
#   output/delivery/delivery_event_<run_id>.json   — DeliveryEvent record (v0.2.0)
#   output/delivery/delivery_event_<run_id>.txt    — Human-readable delivery event summary
#   output/delivery/delta_share_preparation_manifest.json — Share config + SQL DDL templates
```

The delivery event JSON carries `status: "prepared"` — the producer-side layer is complete.
The Delta Share preparation manifest contains the Unity Catalog SQL DDL to provision the share
in a Databricks workspace. Runtime end-to-end validation is Phase C-2.

When `--delivery-dir` is active, export payloads are written at `schema_version: v0.2.0` with
three new optional provenance fields: `delivery_mechanism`, `delta_share_name`, `delivery_event_id`.
When `--delivery-dir` is omitted, V1 export behavior (v0.1.0) is fully preserved.

---

## Running the E-0 Review Queue

Requires Python 3.9+ and `pydantic` (v2). Run the Gold Demo first to generate pipeline artifacts.

```bash
# Run the Gold pipeline with full report, bundle, and review queue output
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports \
  --bundle-dir output/reports \
  --review-queue-dir output/review_queue

# E-0 artifacts:
#   output/review_queue/review_queue_<run_id>.json  — ReviewQueueArtifact (machine-readable)
#   output/review_queue/review_queue_<run_id>.txt   — Human-readable review queue summary
```

The review queue collects records with:
- `outcome_category == 'quarantined'` → reason: `quarantined`
- `outcome_category == 'contract_blocked'` → reason: `contract_blocked`
- `outcome_category == 'skipped_not_export_ready'` AND `document_type_label == 'unknown'` → reason: `extraction_failed`

To record a review decision and produce a reprocessing request:

```python
from src.schemas.review_decision import (
    ReviewDecision,
    DECISION_REQUEST_REPROCESSING,
    REVIEW_DECISION_SCHEMA_VERSION,
    build_reprocessing_request,
    make_decision_id,
    validate_review_decision,
    validate_reprocessing_request,
)
from datetime import datetime, timezone

# Load a queue entry (from the review queue JSON)
# ... queue_entry = loaded_queue["queue_entries"][0] ...

decision = ReviewDecision(
    decision_id=make_decision_id(),
    queue_entry_id=queue_entry["queue_entry_id"],
    document_id=queue_entry["document_id"],
    gold_record_id=queue_entry["gold_record_id"],
    pipeline_run_id=queue_entry["pipeline_run_id"],
    decided_at=datetime.now(tz=timezone.utc).isoformat(),
    schema_version=REVIEW_DECISION_SCHEMA_VERSION,
    decision=DECISION_REQUEST_REPROCESSING,
    decision_rationale="Reviewer identified this as an FDA warning letter. Re-extract with explicit class hint.",
    reprocessing_request_id="placeholder-to-be-replaced",
)

reprocessing_req = build_reprocessing_request(
    decision=decision,
    reprocessing_reason="Document classified as unknown but contains FDA warning letter structure.",
    suggested_document_class_hint="fda_warning_letter",
)

# Update decision with the real reprocessing_request_id
decision.reprocessing_request_id = reprocessing_req.reprocessing_request_id

errors = validate_review_decision(decision)
assert not errors, errors
errors = validate_reprocessing_request(reprocessing_req)
assert not errors, errors
```

See `examples/expected_review_queue.json`, `examples/expected_review_decision.json`, and `examples/expected_reprocessing_request.json` for reference artifacts.

---

## Running the C-2 Delivery Validation

Requires Python 3.9+ and `pydantic` (v2). Run the C-1 Delivery Demo first to generate delivery artifacts.

```bash
# Run the delivery validation against the C-1 artifacts
python -c "
from pathlib import Path
import glob
from src.pipelines.delivery_validation import (
    validate_delivery_layer,
    format_validation_result_text,
    write_validation_result,
)

# Locate delivery event (adjust run_id to match your run)
events = sorted(glob.glob('output/delivery/delivery_event_*.json'))
if not events:
    print('No delivery event found. Run the C-1 delivery demo first.')
else:
    # Extract run_id from filename
    import re
    run_id = re.sub(r'^delivery_event_|\.json$', '', Path(events[-1]).name)
    result = validate_delivery_layer(
        pipeline_run_id=run_id,
        delivery_event_path=Path(events[-1]),
        share_manifest_path=Path('output/delivery/delta_share_preparation_manifest.json'),
        workspace_mode='local_repo_only',
    )
    print(format_validation_result_text(result))
    json_path, text_path = write_validation_result(result, Path('output/validation'))
    print(f'Written: {json_path}')
"

# Expected: status = 'not_provisioned' — correct and honest.
# This means: delivery artifacts are correct producer-side; Delta Share not yet
# executed in Unity Catalog. Run setup_sql from the manifest in Databricks SQL
# to proceed toward 'validated' status (see docs/delivery-runtime-validation.md).
```

C-2 integration health states:
- `not_provisioned` — Share designed in repo, not yet executed in Unity Catalog (honest default)
- `partially_validated` — Producer-side correct; share provisioned; no live queries run
- `validated` — Confirmed in personal Databricks workspace
- `failed` — Schema error, ID mismatch, or parse failure

See [`docs/delivery-runtime-validation.md`](./docs/delivery-runtime-validation.md) for the full C-2 design, check catalogue, and personal Databricks runtime validation runbook.

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
│   │   ├── bedrock_contract.py   # B-1: Gold export payload contract validator
│   │   ├── delivery_event.py     # C-1: Delivery event schema (v0.2.0)
│   │   ├── review_queue.py       # E-0: Human review queue schema
│   │   └── review_decision.py    # E-0: Review decision and reprocessing request schemas
│   ├── pipelines/           # Bronze → Silver → Gold pipeline logic
│   │   ├── export_handoff.py             # B-3: Export packaging and handoff service boundary
│   │   ├── handoff_report.py             # B-4: Export outcome observability and handoff reporting
│   │   ├── handoff_bundle.py             # B-5: Batch manifest and review bundle packaging
│   │   ├── handoff_bundle_validation.py  # B-6: Bundle integrity and consistency validation
│   │   ├── delivery_events.py            # C-1: Delivery event materialization
│   │   ├── delta_share_handoff.py        # C-1: Delta Sharing producer-side preparation layer
│   │   └── review_queue.py               # E-0: Human review queue derivation and materialization
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
├── tests/                   # 1215 tests across all phases: A-4 through B-6, C-1, C-2, D-0, D-1, D-2, E-0
└── examples/
    ├── evaluation/                       # A-4 usage guide
    ├── expected_delivery_event.json      # C-1: Reference delivery event fixture
    └── ...                              # Sample documents and expected outputs
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
