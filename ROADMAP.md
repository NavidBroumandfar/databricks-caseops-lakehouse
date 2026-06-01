# Forward Roadmap - Audit-Based Improvement Plan

This roadmap is forward-looking. It is based on the June 2026 repository audit
and is separate from `docs/roadmap.md`, which records historical delivery phases.

## Current Position

The repository is a strong upstream Databricks-style document intelligence layer.
It already has Bronze, Silver, and Gold artifacts; schema contracts; evaluation;
contract-gated export; handoff reports; bundle validation; delivery-event
preparation; Delta Share setup manifests; review queues; and governance monitoring.

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

Status: started. The first Phase 2 slice adds injectable Spark-backed AI
Function adapters and a reusable Delta table I/O helper while preserving the
local deterministic implementations. Databricks Asset Bundles, job wiring,
runtime resource validation, and live workspace smoke validation remain pending.

1. Implement real Databricks adapters.
   - Replace placeholder-only paths for `ai_parse_document`, `ai_extract`, and `ai_classify` with injectable Spark-backed implementations.
   - Preserve local deterministic implementations for tests and demos.
   - Current slice: `src/pipelines/databricks_runtime.py` provides Spark-backed adapter implementations; the existing parser/extractor/classifier adapter classes delegate to them only when a Spark session is injected.

2. Add Delta table readers/writers.
   - Bronze writer: raw parse outputs to `caseops.<env>.bronze.parsed_documents`.
   - Silver writer: extracted records to `caseops.<env>.silver.extracted_records`.
   - Gold writer: AI-ready assets to `caseops.<env>.gold.ai_ready_assets`.
   - Current slice: reusable `DeltaTableIO` and `DeltaTableTargets` helpers exist for injected Spark sessions. Pipeline job wiring is still pending.

3. Add Databricks Asset Bundle support.
   - Define dev/staging/prod resources without secrets.
   - Add Jobs or Workflows for Bronze, Silver, Gold, evaluation, and delivery validation.

4. Add runtime configuration validation.
   - Validate catalog/schema/table/Volume paths before a run starts.
   - Fail fast on missing Unity Catalog resources.

Acceptance criteria:

- A Databricks workspace can run the pipeline without manually copying SQL snippets.
- Local tests still pass without credentials.
- Runtime adapters are covered by unit tests using mocks and by a documented Databricks smoke test.

Current gap: local mock coverage exists for the adapter and Delta I/O surface.
The documented Databricks smoke test and workspace execution evidence are still
required before Phase 2 is complete.

## Phase 3 - Complete Runtime Handoff Validation

Goal: promote delivery status from `not_provisioned` to `validated` in a real workspace.

1. Provision the Delta Share from the generated manifest.
   - Run `setup_sql` in Databricks SQL with the required Unity Catalog privileges.

2. Create and populate `caseops.gold.delivery_events`.
   - Confirm the delivery event table is queryable.

3. Query the shared Gold table.
   - Validate export-ready records are visible through the share.
   - Validate routing labels and schema versions are visible.

4. Capture runtime evidence.
   - Store sanitized evidence artifacts under an ignored or clearly documented path.
   - Do not commit tokens, activation links, workspace URLs, or personal identifiers.

5. Run the C-2 validator in `personal_databricks` mode.
   - The expected final status is `validated`.

Acceptance criteria:

- Delivery validation result reaches `validated` with real workspace evidence.
- Documentation includes exact validation date, scope, limitations, and sanitized evidence reference.
- The repo still does not include live Bedrock runtime logic.

## Phase 4 - Strengthen Evaluation and Governance

Goal: make quality measurable beyond synthetic/sample fixtures.

1. Add a richer evaluation fixture set.
   - At least 5 FDA, 5 CISA, and 5 incident examples.
   - Include partial, malformed, and quarantine-triggering records.

2. Add regression baselines.
   - Store expected metric ranges for each domain.
   - Fail tests when schema validity, coverage, or traceability regresses.

3. Improve confidence handling.
   - If Databricks `ai_classify` still lacks scalar confidence, document the workaround.
   - If confidence becomes available, add extraction logic and migration tests.

4. Add governance report examples.
   - Include clean, warning, and failure cases.

Acceptance criteria:

- Evaluation covers all active domains.
- Quality gates are explicit and repeatable.
- Governance reports are useful for a reviewer, not just schema-valid.

## Phase 5 - Define the Downstream Agent Contract

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
- Handoff tests prove payloads are consumable.

## Phase 6 - Optional Portfolio Polish

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

1. Add dev dependency management so tests are reproducible.
2. Fix documentation drift around active domains and live delivery status.
3. Implement or scaffold Databricks runtime adapters behind the existing local-safe interfaces.
4. Run and document live Delta Share validation in a personal Databricks workspace.
5. Create the downstream agent contract before writing any agent runtime code.
