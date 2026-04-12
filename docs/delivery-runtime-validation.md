# Delivery Runtime Validation — Phase C-2

> **Phase**: C-2 — Runtime Integration Validation
> **Status**: C-2 implemented (producer-side validation layer complete).
> Runtime end-to-end validation (live Delta Share query) is pending manual provisioning.
> **Authoritative scope**: [`PROJECT_SPEC.md`](../PROJECT_SPEC.md) § Phase C-2
> **Technical design**: [`ARCHITECTURE.md`](../ARCHITECTURE.md) § Phase C-1 Delivery Layer
> **C-0 design record**: [`docs/live-handoff-design.md`](./live-handoff-design.md)

---

## 1. What C-2 Is

C-2 is the **runtime integration validation and observability phase** for the delivery layer chosen in C-0 (Delta Sharing) and implemented producer-side in C-1.

C-2 is about:

- Validating that the **delivery layer is runtime-checkable** from the producer side
- Confirming the **C-1 artifacts are internally correct and cross-artifact consistent**
- Making the **integration health state explicit**: prepared / not_provisioned / partially_validated / validated / failed
- Recording validation results in a **structured, honest, and reviewable way**
- Defining the **runbook for live workspace validation** without executing it unconditionally

C-2 is **not** about:

- Implementing Bedrock consumer logic (retrieval, RAG, agent workflows)
- Pretending the Delta Share is live when it has not been executed in Unity Catalog
- Replacing the V1 file export path
- Drifting into Phase D (multi-domain) or Phase E (operational hardening)

---

## 2. What C-1 Produced (C-2 Input State)

C-1 implemented the upstream producer-side delivery augmentation. At the end of C-1, this repo can produce:

| Artifact | Path | C-1 Status |
|---|---|---|
| Delivery event JSON | `output/delivery/delivery_event_<run_id>.json` | `status = 'prepared'` |
| Delivery event text | `output/delivery/delivery_event_<run_id>.txt` | Human-readable summary |
| Share prep manifest JSON | `output/delivery/delta_share_preparation_manifest.json` | `status = 'designed'` |
| Export payloads (v0.2.0) | `output/gold/exports/regulatory_review/<doc_id>.json` | `schema_version: v0.2.0` |
| B-5 batch manifest | `output/reports/handoff_bundle_<run_id>.json` | Referenced in delivery event |

**C-1 delivery event status = 'prepared'** means: the producer-side layer is complete. The SQL DDL templates are ready. No live Unity Catalog provisioning has been executed.

**C-1 share manifest status = 'designed'** means: the share configuration is documented in this repo. The share does not yet exist in a Databricks workspace.

---

## 3. C-2 Validation Architecture

C-2 adds a bounded, producer-side validation layer that checks the C-1 artifacts without requiring a live Databricks workspace.

### New C-2 files

| File | Role |
|---|---|
| `src/schemas/delivery_validation.py` | Pydantic schema for `DeliveryValidationResult` and `CheckResult`; validation status/scope/workspace vocabulary |
| `src/pipelines/delivery_validation.py` | 15 named check functions + `validate_delivery_layer()` entry point |
| `examples/expected_delivery_validation_result.json` | Reference fixture for the expected validation result shape |
| `docs/delivery-runtime-validation.md` | This document (C-2 design record and runbook) |
| `tests/test_delivery_validation.py` | Test suite for the C-2 validation layer |

---

## 4. Validation Status Vocabulary

The C-2 validation layer uses four explicit statuses. These must not be collapsed.

| Status | Meaning | Conditions |
|---|---|---|
| `validated` | All checks passed with runtime workspace evidence | `workspace_mode = 'personal_databricks'` AND `validation_scope = 'end_to_end'` AND zero failed checks |
| `partially_validated` | Producer-side artifacts correct; runtime evidence not collected | `workspace_mode = 'local_repo_only'` AND zero failed checks AND share is provisioned |
| `not_provisioned` | Delta Share not yet executed in Unity Catalog | Share manifest `status = 'designed'` AND `workspace_mode = 'local_repo_only'` |
| `failed` | One or more checks explicitly failed | Schema mismatch, ID inconsistency, parse failure, evidence sufficiency violation |

**Honesty rule**: `validated` MUST NOT be assigned for local-only runs or producer-side-only scope. The `check_evidence_sufficiency` check enforces this and will downgrade `validated` to `failed` if the workspace_mode or scope is insufficient.

---

## 5. The 15 Validation Checks

| Check Name | What It Validates |
|---|---|
| `delivery_event_exists` | Delivery event JSON file exists on disk |
| `delivery_event_parseable` | File parses as a valid `DeliveryEvent` (Pydantic v2) |
| `delivery_event_schema_version` | `schema_version == 'v0.2.0'` |
| `delivery_event_status_known` | `status` is a known `DELIVERY_STATUS_*` constant |
| `delivery_mechanism_known` | `delivery_mechanism == 'delta_sharing'` (C-0 decision) |
| `cross_id_consistency` | `delivery_event.pipeline_run_id` matches expected; `batch_id == pipeline_run_id` |
| `bundle_path_referenced` | `delivery_event.bundle_artifact_path` is set |
| `bundle_path_exists` | Referenced bundle file exists on disk |
| `routing_labels_present` | When `exported_record_count > 0`, `routing_labels` is non-empty |
| `share_manifest_exists` | Share preparation manifest JSON exists on disk |
| `share_manifest_parseable` | Manifest parses as valid JSON object |
| `share_manifest_has_setup_sql` | `setup_sql` is present and non-empty |
| `share_manifest_has_c2_queries` | `c2_validation_queries` list is non-empty |
| `share_provisioning_acknowledged` | Manifest `status` is `'designed'` or `'provisioned'` |
| `evidence_sufficiency` | Claimed `validation_status` is honest given `workspace_mode` and `validation_scope` |

---

## 6. Running C-2 Producer-Side Validation (Local)

This validation is locally executable. No Databricks workspace is required.

**Step 1**: Run the C-1 delivery demo to generate artifacts:

```bash
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports \
  --bundle-dir output/reports \
  --delivery-dir output/delivery
```

**Step 2**: Run the C-2 validation against those artifacts:

```python
from pathlib import Path
from src.pipelines.delivery_validation import (
    validate_delivery_layer,
    format_validation_result_text,
    write_validation_result,
)

# Locate the delivery event (adjust run_id to match your run)
run_id = "your-pipeline-run-id"
result = validate_delivery_layer(
    pipeline_run_id=run_id,
    delivery_event_path=Path(f"output/delivery/delivery_event_{run_id}.json"),
    share_manifest_path=Path("output/delivery/delta_share_preparation_manifest.json"),
    workspace_mode="local_repo_only",
)

print(format_validation_result_text(result))
print(f"\nStatus: {result.validation_status}")
print(f"Checks passed: {len(result.checks_passed)}")
print(f"Checks failed: {len(result.checks_failed)}")

# Optionally write artifacts
json_path, text_path = write_validation_result(result, Path("output/validation"))
print(f"Written: {json_path}, {text_path}")
```

**Expected result for a fresh local run**: `status = 'not_provisioned'`

This is correct and honest. It means: the delivery artifacts are correct on the producer side; the Delta Share has not been executed in Unity Catalog yet.

---

## 7. Runbook for Personal Databricks Runtime Validation

This runbook describes the steps to achieve `status = 'validated'` in a personal Databricks workspace. These steps require a live personal Databricks workspace and must be executed manually.

**Prerequisites**:
- Personal Databricks workspace (Free Edition or above)
- Unity Catalog enabled
- Serverless SQL warehouse available
- `CREATE SHARE` privilege on the `caseops` catalog

**Step 1** — Generate C-1 delivery artifacts (local):
```bash
python src/pipelines/classify_gold.py \
  --input-dir output/silver \
  --bronze-dir output/bronze \
  --report-dir output/reports \
  --bundle-dir output/reports \
  --delivery-dir output/delivery
```

**Step 2** — Provision the Delta Share (Databricks SQL):

Copy the `setup_sql` from `output/delivery/delta_share_preparation_manifest.json` and run it in a Databricks SQL notebook. This creates the share, adds the Gold table, and configures the recipient.

**Step 3** — Create the delivery events table (Databricks SQL):

Copy the `delivery_events_ddl` from the manifest and run it in the workspace.

**Step 4** — Confirm the share is queryable (Databricks SQL):

Run the `c2_validation_queries` from the manifest in order:
1. `confirm_share_exists` — verifies `SHOW ALL IN SHARE caseops_handoff` shows the Gold table
2. `query_export_ready_records` — verifies export-ready records are visible in the shared table
3. `query_delivery_events` — verifies the delivery event row is readable
4. `verify_routing_label_transparency` — verifies routing labels are visible per-record

**Step 5** — Record runtime evidence:

Save the query results as JSON or text files in `output/validation/runtime_evidence/`. These are the proof artifacts for `status = 'validated'`.

**Step 6** — Run C-2 validation with runtime evidence:

```python
result = validate_delivery_layer(
    pipeline_run_id=run_id,
    delivery_event_path=Path(f"output/delivery/delivery_event_{run_id}.json"),
    share_manifest_path=Path("output/delivery/delta_share_preparation_manifest.json"),
    workspace_mode="personal_databricks",
)
# Expected: status = 'validated' (if all checks pass)
```

---

## 8. Integration Observability States

After C-2, this repo can report one of the following states explicitly:

| State | Meaning | How to Reach |
|---|---|---|
| **prepared** | C-1 delivery event written; share configured in repo | Run `classify_gold.py --delivery-dir` |
| **not_provisioned** | Producer-side artifacts correct; share not yet in Unity Catalog | Default C-2 result on local run |
| **partially_validated** | Producer-side correct; share provisioned; runtime evidence pending | Share provisioned but C-2 not run with workspace |
| **validated** | Runtime-confirmed in personal Databricks workspace | All 6 C-2 validation targets passed in workspace |
| **failed** | Validation check explicitly failed | Schema error, ID mismatch, missing required field |

These states must never be collapsed. The C-2 `DeliveryValidationResult` records the state explicitly per run.

---

## 9. What C-2 Does NOT Deliver

- Live Bedrock CaseOps consumer implementation or simulation
- Retrieval index population, vector search, or RAG
- Agent reasoning, escalation, or case-support workflows
- Production Delta Share deployment in an enterprise workspace
- Any credentials, access tokens, or workspace URLs committed to the repo
- Guaranteed successful external delivery (consumer-side confirmation is Bedrock CaseOps' responsibility)

---

## 10. Databricks / Bedrock Boundary After C-2

The boundary defined in C-0 and implemented in C-1 is unchanged in C-2.

| Concern | This Repo (C-2) | Bedrock CaseOps |
|---|---|---|
| Delivery artifact validation | Yes — validates C-1 artifacts are correct | No |
| Share provisioning SQL templates | Yes — generated in C-1 | No |
| Share provisioning execution | Manual step in personal workspace | No |
| Delta Share consumption | No | Yes |
| Delivery event table polling | No | Yes |
| Retrieval, RAG, agent reasoning | No | Yes |
| Delivery receipt acknowledgment | No | Yes |

---

## 11. C-2 Acceptance Criteria Status

| Criterion | Status |
|---|---|
| Repo gains a structured runtime validation/result layer | ✅ `src/schemas/delivery_validation.py` + `src/pipelines/delivery_validation.py` |
| Repo gains structured runtime validation evidence recording | ✅ `write_validation_result()` → JSON + text artifacts |
| Producer-side delivery observability is clearer after C-2 | ✅ 5-state integration health model, 15 named checks |
| V1 file export path still exists and is not broken | ✅ No changes to V1 path in C-2 |
| No Bedrock runtime logic enters this repo | ✅ C-2 is producer-side only |
| Docs clearly distinguish C-1 implemented vs C-2 validated vs pending external proof | ✅ This document + updated core docs |
| Tests cover the new validation layer | ✅ `tests/test_delivery_validation.py` |
| No doc falsely claims end-to-end external validation | ✅ Status 'not_provisioned' is the honest default |
