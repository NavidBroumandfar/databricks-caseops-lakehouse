# Forward Roadmap - Audit-Based Improvement Plan

This roadmap is forward-looking. It is based on the June 2026 repository audit
and is separate from `docs/roadmap.md`, which records historical delivery phases.

## Current Position

The repository is a strong upstream Databricks-style document intelligence layer.
It already has Bronze, Silver, and Gold artifacts; schema contracts; evaluation;
contract-gated export; handoff reports; bundle validation; delivery-event
preparation; Delta Share setup manifests; review queues; and governance monitoring.
Runtime execution has moved beyond local-first behavior: Phase 2 adapter
integration is complete, and Phase 4 productionization is active.

The key strategic conclusion from the audit:

This repo should remain the governed Databricks preparation layer. The
multi-agent reasoning system should be built downstream, consuming this repo's
Gold export contract or Delta Share surface.

## Phase 0 - Stabilize Developer Reproducibility

Goal: make the project easy to clone, install, test, and audit.

Status: implemented for the June 2026 audit cleanup.

1. Add a dev dependency file.
   - `requirements-dev.txt` extends `requirements.txt`.
   - Includes `pytest`.
   - Keep runtime dependencies minimal.

2. Add a single verification command.
   - `make test` runs the full local test suite.
   - `make audit` currently aliases the full local test suite.

3. Document the local Python command.
   - The audit environment did not expose `python`.
   - Use `.venv/bin/python` or `python3` consistently in docs.

4. Keep generated artifacts out of commits.
   - `output/` is ignored for generated local run artifacts.
   - Canonical public fixtures belong under `examples/`.

Acceptance criteria:

- Fresh setup instructions install both runtime and test dependencies.
- `pytest` runs without manual dependency discovery.
- The README and this roadmap agree on local commands.

## Phase 1 - Fix Documentation Drift

Goal: make the public story internally consistent.

Status: implemented for the June 2026 audit cleanup.

1. Reconcile V2 domain status.
   - FDA warning letters, CISA advisories, and incident reports are the active reference domains.
   - `docs/data-contracts.md` and `docs/bedrock-handoff-contract.md` should both reflect that status.

2. Clarify "live handoff" wording.
   - Producer-side delivery is implemented.
   - Live Delta Share provisioning is not validated unless the share is actually created and queried.
   - Use "producer-side prepared" vs. "runtime validated" consistently.

3. Update project status language.
   - Avoid saying this repo is a multi-agent or reasoning runtime.
   - Use "upstream AI-ready asset preparation layer."

4. Add an audit summary section.
   - Short, honest summary of what exists, what does not, and the next milestone.

Acceptance criteria:

- No docs imply agent reasoning exists in this repo.
- Domain status is consistent across README, PROJECT_SPEC, data contracts, handoff contract, and roadmap docs.
- Delivery status never overclaims live runtime validation.

## Phase 2 - Productionize Databricks Runtime Execution

Goal: move from local-safe and bootstrap SQL to a repeatable Databricks runtime.

Status: complete. Phase 2 provides runtime injection points in this repo and no
longer requires SQL-only execution.

1. Implement real Databricks adapters.
   - Replace placeholder-only paths for `ai_parse_document`, `ai_extract`, and `ai_classify` with injectable Spark-backed implementations.
   - Preserve local deterministic implementations for tests and demos.
   - Current slice: `src/pipelines/databricks_runtime.py` provides Spark-backed adapter implementations; the existing parser/extractor/classifier adapter classes delegate to them only when a Spark session is injected.

2. Add Delta table readers/writers.
   - Bronze writer: raw parse outputs to `caseops_<env>.bronze.parsed_documents`.
   - Silver writer: extracted records to `caseops_<env>.silver.extracted_records`.
   - Gold writer: AI-ready assets to `caseops_<env>.gold.ai_ready_assets`.
   - Current slice: reusable `DeltaTableIO` and `DeltaTableTargets` helpers exist for injected Spark sessions.

3. Add runtime configuration validation.
   - Validate catalog/schema/table/Volume paths before a run starts.
   - Fail fast on missing Unity Catalog resources.

4. Complete deployment execution path.
   - Databricks Jobs/Workflows or Asset Bundle equivalent for Bronze, Silver,
     Gold, delivery_validation, and delivery verification.
   - The entrypoint is `src/pipelines/run_databricks_pipeline.py`.
   - Scaffold is provided in `config/databricks-runtime-bundle/databricks.yml` (single shared runner for all stages).

Acceptance criteria:

 - A Databricks workspace can run the pipeline from repository-provided runtime entrypoints through the Phase 4 runbook/scaffold.
- Local tests still pass without credentials.
- Runtime adapters are covered by unit tests using mocks.

Current status: runtime productionization scaffolding is in place for this phase
(runner + bundle). Remaining work is repeatable workspace smoke evidence and periodic evidence package refresh.

## Phase 3 - Complete Runtime Handoff Validation

Goal: promote delivery status from `not_provisioned` to `validated` in a real workspace.

Status: complete for personal-workspace producer-side runtime validation as of
2026-06-07. The delivery validator reached `validated` with sanitized evidence
from a real personal Databricks workspace. This is not an enterprise deployment
and does not include a Bedrock consumer.

1. Provision the Delta Share from the generated manifest.
   - Run `delivery_events_ddl` first, then `setup_sql` in Databricks SQL with the required Unity Catalog privileges.
   - Completed in a personal Databricks workspace on 2026-06-07.

2. Create and populate `caseops.gold.delivery_events`.
   - Confirm the delivery event table is queryable.

3. Query the shared Gold table.
   - Validate the Gold table is exposed through the share.
   - Validate routing labels and schema versions are visible.
   - Completed with `SHOW ALL IN SHARE` for shared-object visibility and provider-side source table queries for row-level checks.

4. Capture runtime evidence.
   - Store sanitized evidence artifacts under an ignored or clearly documented path.
   - Do not commit tokens, activation links, workspace URLs, or personal identifiers.
   - Completed: sanitized evidence stored under `output/validation/runtime_evidence/`.

5. Run the C-2 validator in `personal_databricks` mode.
   - Completed: final status reached `validated` with 21/21 checks passing.
   - `validate_delivery_layer(..., runtime_evidence_path=..., workspace_mode='personal_databricks')` reaches `validated` only when the manifest is `provisioned` and the sanitized runtime evidence passes all checks.

Acceptance criteria:

- ✅ Delivery validation result reaches `validated` with real workspace evidence.
- ✅ Documentation includes exact validation date, scope, limitations, and sanitized evidence reference.
- ✅ The repo still does not include live Bedrock runtime logic.

Runtime finding addressed during Phase 3: current Databricks SQL requires shared
aliases in `schema.name` form. The manifest generator now uses
`gold.gold_ai_ready_assets` and `gold.delivery_events`, and row-level provider
checks query source tables while `SHOW ALL IN SHARE` confirms shared-object
exposure.

## Phase 4 - Databricks Productionization and Runtime Path

Goal: convert runtime capabilities into a reproducible deployment execution path
that is deterministic, secret-free in source control, and evidence-driven.

Status: active.

1. Add runtime orchestration for Databricks execution.
   - Add Jobs/Workflows (or Asset Bundle equivalent) for Bronze, Silver,
     Gold, delivery validation, and delivery verification.
   - Keep credentials and workspace URLs out of the repository.

2. Wire environment-aware Delta I/O execution.
   - Use `src/utils/environment_config.py` and
     `src/pipelines/databricks_runtime.py::DeltaTableTargets` for target tables.
   - Ensure runtime paths resolve as:
     - `caseops_<env>.bronze.parsed_documents`
     - `caseops_<env>.silver.extracted_records`
     - `caseops_<env>.gold.ai_ready_assets`

3. Formalize a repeatable smoke-test runbook.
   - Define deterministic workspace commands for end-to-end execution.
   - Capture sanitized evidence for each run using
     `examples/runtime_evidence_personal_databricks_template.json`.
   - Keep evidence artifacts out of committed outputs.
   - Current slice: `src/pipelines/runtime_smoke_validation.py` validates a
     captured smoke evidence package without Databricks credentials and only
     reports `ready_for_phase4_closeout` when runtime evidence and C-2 delivery
     validation agree on the same provisioned `pipeline_run_id`.
   - Current support slice: `src/pipelines/runtime_smoke_plan.py` generates a
     run-scoped capture plan with expected artifact paths, validation commands,
     and sanitization rules before the workspace smoke run. This plan is not
     runtime evidence.

4. Add production-grade pre-flight validation.
   - Validate required tables, schemas, and manifest presence before runtime runs.
   - Validate run-mode-specific evidence requirements before declaring runtime status.

Acceptance criteria:

- Runtime pipeline execution path is documented in repo with concrete entrypoints.
- Workspace runtime smoke test is repeatable and evidence-based; captured
  evidence can be checked locally with `runtime_smoke_validation.py`.
- No secret or workspace-identifying value is added to tracked files.
- `docs/databricks-runtime-productionization.md` serves as the single source of truth for Phase 4 execution details.

## Phase 5 - Strengthen Evaluation and Governance

Status: implemented.

Goal: make quality measurable beyond synthetic/sample fixtures.

1. Add a richer evaluation fixture set.
   - At least 5 FDA, 5 CISA, and 5 incident examples.
   - Include partial, malformed, and quarantine-triggering records.

2. Add regression baselines.
   - Store expected metric ranges for each domain.
   - Fail tests when schema validity, coverage, or traceability regresses.

3. Improve confidence handling.
  - If Databricks `ai_classify` still lacks scalar confidence, document the workaround and add regression-safe guards for null/missing confidence.
  - Keep confidence threshold logic constrained to numeric samples only.

4. Add governance report examples.
  - Include clean, warning, and failure cases.

Artifacts:

- `examples/evaluation/phase5_regression_fixtures.json` (multi-domain silver baseline + traceability fixtures)
- `tests/test_phase5_evaluation_regression_baselines.py` (range checks + regression detection)
- `examples/expected_governance_report_warning.json`
- `examples/expected_governance_report_failure.json`

Acceptance criteria:

- Regression baselines are defined per domain and checked in one dedicated test file.
- Quarantine-triggering and malformed/cross-domain examples are explicitly represented in the fixtures.
- Confidence null handling is documented and tested as a guarded fallback.
- Governance report examples include all three states: healthy, warning, critical.

Acceptance criteria:

- Evaluation covers all active domains.
- Quality gates are explicit and repeatable.
- Governance reports are useful for a reviewer, not just schema-valid.

## Phase 6 - Define the Downstream Agent Contract

Status: implemented.

Goal: prepare for the actual multi-agent project without moving that runtime into this repo.

1. Create a downstream consumer contract.
   - Define exactly what Bedrock CaseOps reads from the Gold payload or Delta Share.
   - Include routing labels, required fields, and expected failure handling.

2. Add a consumer simulation test.
   - Local script reads export payloads and verifies a downstream system could ingest them.
   - This is not a Bedrock SDK integration.

3. Define agent responsibilities outside this repo.
   - Regulatory review agent
   - Security operations agent
   - Incident management agent
   - Human escalation/review agent

4. Document the split between repositories.
   - This repo: governed preparation and handoff.
   - Downstream repo: retrieval, agent reasoning, workflow execution, escalation.

Acceptance criteria:

- A downstream team can implement agents from the contract without guessing payload semantics.
- This repo still has no agent orchestration runtime.
- Handoff tests prove payloads are consumable by contract and the local simulation scaffold.

## Phase 7 - Optional Portfolio Polish

Goal: make the project easier to understand for reviewers.

1. Add an architecture decision record folder.
   - Capture decisions such as Delta Sharing over API push and local-safe deterministic baselines.

2. Add a short audit report.
   - Summarize architecture, strengths, gaps, and next steps.

3. Add one diagram per boundary.
   - Bronze/Silver/Gold flow.
   - Gold to Bedrock handoff.
   - Databricks vs. Bedrock ownership split.

4. Add a "What this is not" section to the README if it is not prominent enough.

Acceptance criteria:

- A reviewer can understand the project in under 10 minutes.
- The distinction between data preparation and agent reasoning is obvious.
- Claims remain honest and verifiable.

## Recommended Immediate Next Steps

Start with these in order:

1. Add Databricks runtime orchestration scaffolding (Jobs/Asset Bundle equivalent).
2. Publish and run one non-production end-to-end workspace smoke test using the new runbook.
3. Capture and archive sanitized evidence artifacts and evidence checks.
4. Add hardening checks (resource existence + evidence integrity) to the runtime entrypoints.
