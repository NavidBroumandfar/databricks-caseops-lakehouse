"""
src/pipelines/delivery_validation.py — C-2: Delivery-Layer Runtime Validation Logic.

Implements the bounded, producer-side delivery-layer validation that constitutes
Phase C-2. This module validates that the C-1 delivery artifacts are internally
correct, cross-artifact consistent, and honest about their provisioning state.

What this module validates (producer-side, locally executable)
--------------------------------------------------------------
1. Delivery event artifact:
   - File exists at the provided path
   - Parses as a valid DeliveryEvent (Pydantic v2)
   - schema_version == 'v0.2.0'
   - status is a known DELIVERY_STATUS_* constant
   - delivery_mechanism == 'delta_sharing'

2. Share preparation manifest:
   - File exists at the provided path
   - Parses as valid JSON with required fields
   - setup_sql is present and non-empty
   - c2_validation_queries list is non-empty

3. Cross-artifact consistency:
   - delivery_event.pipeline_run_id matches the expected pipeline_run_id
   - delivery_event.delivery_event_id matches the expected delivery_event_id (if provided)
   - delivery_event.batch_id == delivery_event.pipeline_run_id (C-1 design invariant)

4. Bundle artifact reference chain:
   - delivery_event.bundle_artifact_path is set (not None)
   - The referenced bundle artifact file exists on disk

5. Routing label presence:
   - If exported_record_count > 0, routing_labels is non-empty

6. Evidence sufficiency:
   - The claimed validation_status is honest given workspace_mode and scope
   - 'validated' MUST NOT be assigned for local_repo_only runs
   - 'validated' MUST NOT be assigned for producer_side_only scope

What this module does NOT validate
-----------------------------------
- Whether the Delta Share is live and queryable in Unity Catalog
- Whether Bedrock CaseOps has received or acknowledged delivery
- Whether SQL validation queries returned the expected results
- Any live Unity Catalog API or Delta Sharing SDK call
- Any live Databricks workspace operation

Honesty invariant
-----------------
If no runtime provisioning has been executed (manifest status == 'designed')
and workspace_mode is 'local_repo_only', the result status is 'not_provisioned'.
This must never be collapsed into 'validated' or left ambiguous.

Phase: C-2
Schema: src/schemas/delivery_validation.py
Architecture context: ARCHITECTURE.md § Phase C-1 Delivery Layer — Implementation Status
Design decision: docs/live-handoff-design.md § 11 (C-2 Validation Direction)
C-2 design: docs/delivery-runtime-validation.md
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.delivery_event import (
    ALL_DELIVERY_STATUSES,
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
    DeliveryEvent,
)
from src.schemas.delivery_validation import (
    CHECK_BUNDLE_PATH_EXISTS,
    CHECK_BUNDLE_PATH_REFERENCED,
    CHECK_CROSS_ID_CONSISTENCY,
    CHECK_DELIVERY_EVENT_EXISTS,
    CHECK_DELIVERY_EVENT_PARSEABLE,
    CHECK_DELIVERY_EVENT_SCHEMA_VERSION,
    CHECK_DELIVERY_EVENT_STATUS_KNOWN,
    CHECK_DELIVERY_MECHANISM_KNOWN,
    CHECK_EVIDENCE_SUFFICIENCY,
    CHECK_ROUTING_LABELS_PRESENT,
    CHECK_SHARE_MANIFEST_EXISTS,
    CHECK_SHARE_MANIFEST_HAS_C2_QUERIES,
    CHECK_SHARE_MANIFEST_HAS_SETUP_SQL,
    CHECK_SHARE_MANIFEST_PARSEABLE,
    CHECK_SHARE_PROVISIONING_ACKNOWLEDGED,
    VALIDATION_SCHEMA_VERSION,
    VALIDATION_SCOPE_END_TO_END,
    VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
    VALIDATION_STATUS_FAILED,
    VALIDATION_STATUS_NOT_PROVISIONED,
    VALIDATION_STATUS_PARTIALLY_VALIDATED,
    VALIDATION_STATUS_VALIDATED,
    WORKSPACE_MODE_LOCAL_REPO_ONLY,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
    CheckResult,
    DeliveryValidationResult,
)


# ---------------------------------------------------------------------------
# Internal check builder helpers
# ---------------------------------------------------------------------------


def _pass(name: str, detail: Optional[str] = None) -> CheckResult:
    return CheckResult(check_name=name, passed=True, detail=detail)


def _fail(name: str, detail: str) -> CheckResult:
    return CheckResult(check_name=name, passed=False, detail=detail)


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------


def check_delivery_event_exists(delivery_event_path: Optional[Path]) -> CheckResult:
    """Check that the delivery event JSON artifact exists on disk."""
    if delivery_event_path is None:
        return _fail(CHECK_DELIVERY_EVENT_EXISTS, "No delivery event path provided.")
    if not delivery_event_path.exists():
        return _fail(
            CHECK_DELIVERY_EVENT_EXISTS,
            f"Delivery event artifact not found at: {delivery_event_path}",
        )
    return _pass(CHECK_DELIVERY_EVENT_EXISTS, f"Found: {delivery_event_path}")


def check_delivery_event_parseable(
    delivery_event_path: Optional[Path],
) -> Tuple[CheckResult, Optional[DeliveryEvent]]:
    """
    Check that the delivery event file parses as a valid DeliveryEvent.

    Returns (CheckResult, parsed DeliveryEvent or None).
    """
    if delivery_event_path is None or not delivery_event_path.exists():
        return (
            _fail(
                CHECK_DELIVERY_EVENT_PARSEABLE,
                "Delivery event path is missing or does not exist.",
            ),
            None,
        )
    try:
        raw = json.loads(delivery_event_path.read_text(encoding="utf-8"))
        event = DeliveryEvent.model_validate(raw)
        return (
            _pass(
                CHECK_DELIVERY_EVENT_PARSEABLE,
                f"Parsed as DeliveryEvent (delivery_event_id={event.delivery_event_id}).",
            ),
            event,
        )
    except Exception as exc:
        return (
            _fail(CHECK_DELIVERY_EVENT_PARSEABLE, f"Failed to parse delivery event: {exc}"),
            None,
        )


def check_delivery_event_schema_version(event: Optional[DeliveryEvent]) -> CheckResult:
    """Check that schema_version == 'v0.2.0' (C-1 delivery contract version)."""
    if event is None:
        return _fail(CHECK_DELIVERY_EVENT_SCHEMA_VERSION, "No delivery event to check.")
    if event.schema_version != DELIVERY_SCHEMA_VERSION:
        return _fail(
            CHECK_DELIVERY_EVENT_SCHEMA_VERSION,
            f"Expected schema_version='{DELIVERY_SCHEMA_VERSION}', "
            f"got '{event.schema_version}'.",
        )
    return _pass(
        CHECK_DELIVERY_EVENT_SCHEMA_VERSION,
        f"schema_version='{event.schema_version}' is correct.",
    )


def check_delivery_event_status_known(event: Optional[DeliveryEvent]) -> CheckResult:
    """Check that the delivery event status is a known DELIVERY_STATUS_* constant."""
    if event is None:
        return _fail(CHECK_DELIVERY_EVENT_STATUS_KNOWN, "No delivery event to check.")
    if event.status not in ALL_DELIVERY_STATUSES:
        return _fail(
            CHECK_DELIVERY_EVENT_STATUS_KNOWN,
            f"Unknown delivery status: '{event.status}'. "
            f"Known: {sorted(ALL_DELIVERY_STATUSES)}",
        )
    return _pass(
        CHECK_DELIVERY_EVENT_STATUS_KNOWN,
        f"status='{event.status}' is a known delivery status.",
    )


def check_delivery_mechanism_known(event: Optional[DeliveryEvent]) -> CheckResult:
    """Check that the delivery mechanism is 'delta_sharing' (C-0 decision)."""
    if event is None:
        return _fail(CHECK_DELIVERY_MECHANISM_KNOWN, "No delivery event to check.")
    if event.delivery_mechanism != DELIVERY_MECHANISM_DELTA_SHARING:
        return _fail(
            CHECK_DELIVERY_MECHANISM_KNOWN,
            f"Expected delivery_mechanism='{DELIVERY_MECHANISM_DELTA_SHARING}', "
            f"got '{event.delivery_mechanism}'.",
        )
    return _pass(
        CHECK_DELIVERY_MECHANISM_KNOWN,
        f"delivery_mechanism='{event.delivery_mechanism}' matches C-0 decision.",
    )


def check_share_manifest_exists(share_manifest_path: Optional[Path]) -> CheckResult:
    """Check that the share preparation manifest exists on disk."""
    if share_manifest_path is None:
        return _fail(CHECK_SHARE_MANIFEST_EXISTS, "No share manifest path provided.")
    if not share_manifest_path.exists():
        return _fail(
            CHECK_SHARE_MANIFEST_EXISTS,
            f"Share preparation manifest not found at: {share_manifest_path}",
        )
    return _pass(CHECK_SHARE_MANIFEST_EXISTS, f"Found: {share_manifest_path}")


def check_share_manifest_parseable(
    share_manifest_path: Optional[Path],
) -> Tuple[CheckResult, Optional[dict]]:
    """
    Check that the share manifest file parses as valid JSON with expected structure.

    Returns (CheckResult, parsed manifest dict or None).
    """
    if share_manifest_path is None or not share_manifest_path.exists():
        return (
            _fail(
                CHECK_SHARE_MANIFEST_PARSEABLE,
                "Share manifest path is missing or does not exist.",
            ),
            None,
        )
    try:
        raw = json.loads(share_manifest_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return (
                _fail(
                    CHECK_SHARE_MANIFEST_PARSEABLE,
                    "Share manifest is not a JSON object.",
                ),
                None,
            )
        return (
            _pass(
                CHECK_SHARE_MANIFEST_PARSEABLE,
                f"Parsed share manifest (status='{raw.get('status', '?')}').",
            ),
            raw,
        )
    except Exception as exc:
        return (
            _fail(CHECK_SHARE_MANIFEST_PARSEABLE, f"Failed to parse share manifest: {exc}"),
            None,
        )


def check_share_manifest_has_setup_sql(manifest_dict: Optional[dict]) -> CheckResult:
    """Check that the manifest contains non-empty setup_sql DDL."""
    if manifest_dict is None:
        return _fail(CHECK_SHARE_MANIFEST_HAS_SETUP_SQL, "No manifest to check.")
    setup_sql = manifest_dict.get("setup_sql", "")
    if not setup_sql or not str(setup_sql).strip():
        return _fail(
            CHECK_SHARE_MANIFEST_HAS_SETUP_SQL,
            "Share manifest has empty or missing setup_sql field.",
        )
    return _pass(
        CHECK_SHARE_MANIFEST_HAS_SETUP_SQL,
        f"setup_sql present ({len(str(setup_sql))} chars).",
    )


def check_share_manifest_has_c2_queries(manifest_dict: Optional[dict]) -> CheckResult:
    """Check that the manifest contains at least one C-2 validation query descriptor."""
    if manifest_dict is None:
        return _fail(CHECK_SHARE_MANIFEST_HAS_C2_QUERIES, "No manifest to check.")
    queries = manifest_dict.get("c2_validation_queries", [])
    if not isinstance(queries, list) or len(queries) == 0:
        return _fail(
            CHECK_SHARE_MANIFEST_HAS_C2_QUERIES,
            "Share manifest has no c2_validation_queries.",
        )
    return _pass(
        CHECK_SHARE_MANIFEST_HAS_C2_QUERIES,
        f"{len(queries)} C-2 validation query descriptor(s) present.",
    )


def check_cross_id_consistency(
    event: Optional[DeliveryEvent],
    expected_pipeline_run_id: Optional[str] = None,
    expected_delivery_event_id: Optional[str] = None,
) -> CheckResult:
    """
    Check cross-artifact identifier consistency.

    Checks:
    - delivery_event.pipeline_run_id matches expected_pipeline_run_id (if provided)
    - delivery_event.delivery_event_id matches expected_delivery_event_id (if provided)
    - delivery_event.batch_id == delivery_event.pipeline_run_id (C-1 design invariant)
    """
    if event is None:
        return _fail(CHECK_CROSS_ID_CONSISTENCY, "No delivery event to check.")

    errors: List[str] = []

    if expected_pipeline_run_id and event.pipeline_run_id != expected_pipeline_run_id:
        errors.append(
            f"pipeline_run_id mismatch: event has '{event.pipeline_run_id}', "
            f"expected '{expected_pipeline_run_id}'."
        )

    if expected_delivery_event_id and event.delivery_event_id != expected_delivery_event_id:
        errors.append(
            f"delivery_event_id mismatch: event has '{event.delivery_event_id}', "
            f"expected '{expected_delivery_event_id}'."
        )

    if event.batch_id != event.pipeline_run_id:
        errors.append(
            f"batch_id ('{event.batch_id}') does not equal pipeline_run_id "
            f"('{event.pipeline_run_id}'). C-1 design requires batch_id == pipeline_run_id."
        )

    if errors:
        return _fail(CHECK_CROSS_ID_CONSISTENCY, " | ".join(errors))

    return _pass(
        CHECK_CROSS_ID_CONSISTENCY,
        "delivery_event_id, pipeline_run_id, and batch_id are internally consistent.",
    )


def check_bundle_path_referenced(event: Optional[DeliveryEvent]) -> CheckResult:
    """Check that the delivery event references a B-5 bundle artifact path."""
    if event is None:
        return _fail(CHECK_BUNDLE_PATH_REFERENCED, "No delivery event to check.")
    if not event.bundle_artifact_path:
        return _fail(
            CHECK_BUNDLE_PATH_REFERENCED,
            "delivery_event.bundle_artifact_path is not set.",
        )
    return _pass(
        CHECK_BUNDLE_PATH_REFERENCED,
        f"bundle_artifact_path='{event.bundle_artifact_path}'.",
    )


def check_bundle_path_exists(event: Optional[DeliveryEvent]) -> CheckResult:
    """Check that the referenced B-5 bundle artifact file exists on disk."""
    if event is None:
        return _fail(CHECK_BUNDLE_PATH_EXISTS, "No delivery event to check.")
    if not event.bundle_artifact_path:
        return _fail(
            CHECK_BUNDLE_PATH_EXISTS,
            "No bundle_artifact_path set in delivery event.",
        )
    bundle_path = Path(event.bundle_artifact_path)
    if not bundle_path.exists():
        return _fail(
            CHECK_BUNDLE_PATH_EXISTS,
            f"Bundle artifact not found at: {bundle_path}",
        )
    return _pass(
        CHECK_BUNDLE_PATH_EXISTS,
        f"Bundle artifact exists at: {bundle_path}",
    )


def check_routing_labels_present(event: Optional[DeliveryEvent]) -> CheckResult:
    """
    Check that routing_labels is non-empty when exported_record_count > 0.

    Empty batches (zero exports) are allowed and pass this check.
    """
    if event is None:
        return _fail(CHECK_ROUTING_LABELS_PRESENT, "No delivery event to check.")
    if event.exported_record_count == 0:
        return _pass(
            CHECK_ROUTING_LABELS_PRESENT,
            "No exported records in batch; routing labels check not applicable.",
        )
    if not event.routing_labels:
        return _fail(
            CHECK_ROUTING_LABELS_PRESENT,
            f"exported_record_count={event.exported_record_count} "
            f"but routing_labels is empty.",
        )
    return _pass(
        CHECK_ROUTING_LABELS_PRESENT,
        f"routing_labels present: {event.routing_labels}.",
    )


def check_share_provisioning_acknowledged(manifest_dict: Optional[dict]) -> CheckResult:
    """
    Check that the share manifest explicitly acknowledges its provisioning state.

    The manifest must have a 'status' field with value 'designed' or 'provisioned'.
    """
    if manifest_dict is None:
        return _fail(CHECK_SHARE_PROVISIONING_ACKNOWLEDGED, "No manifest to check.")
    status = manifest_dict.get("status", "")
    if not status:
        return _fail(
            CHECK_SHARE_PROVISIONING_ACKNOWLEDGED,
            "Share manifest has no 'status' field.",
        )
    known_statuses = {"designed", "provisioned"}
    if status not in known_statuses:
        return _fail(
            CHECK_SHARE_PROVISIONING_ACKNOWLEDGED,
            f"Unknown manifest status: '{status}'. Known: {sorted(known_statuses)}",
        )
    return _pass(
        CHECK_SHARE_PROVISIONING_ACKNOWLEDGED,
        f"Share provisioning status is acknowledged as '{status}'.",
    )


def check_evidence_sufficiency(
    validation_status: str,
    workspace_mode: str,
    validation_scope: str,
    checks_failed: List[str],
) -> CheckResult:
    """
    Validate that the claimed validation_status is honest given available evidence.

    Rules enforced:
    - 'validated' requires workspace_mode == 'personal_databricks'
    - 'validated' requires no failed checks
    - Any other status combination is acceptable

    Note: validation_scope is always 'producer_side_only' for validate_delivery_layer()
    because this function only checks repo artifacts. The workspace_mode claim of
    'personal_databricks' is the caller's assertion that runtime evidence was collected.

    This check is the honesty gate of the validation layer.
    """
    if validation_status == VALIDATION_STATUS_VALIDATED:
        if workspace_mode == WORKSPACE_MODE_LOCAL_REPO_ONLY:
            return _fail(
                CHECK_EVIDENCE_SUFFICIENCY,
                "Status 'validated' claimed but workspace_mode is 'local_repo_only'. "
                "Full validation requires personal_databricks workspace access. "
                "Use 'partially_validated' or 'not_provisioned' for local-only runs.",
            )
        # Note: validation_scope is always 'producer_side_only' for this function
        # (it only checks repo artifacts). The workspace_mode claim of
        # 'personal_databricks' is the caller's assertion that runtime evidence
        # was collected in a Databricks workspace. That is sufficient for 'validated'.
        if checks_failed:
            return _fail(
                CHECK_EVIDENCE_SUFFICIENCY,
                f"Status 'validated' claimed but {len(checks_failed)} check(s) failed: "
                f"{checks_failed}.",
            )

    return _pass(
        CHECK_EVIDENCE_SUFFICIENCY,
        f"Evidence is appropriate for claimed status '{validation_status}' "
        f"(scope={validation_scope}, workspace={workspace_mode}).",
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _derive_validation_status(
    checks_failed: List[str],
    parsed_manifest: Optional[dict],
    workspace_mode: str,
) -> tuple[str, str]:
    """
    Derive the validation_status and validation_reason from check results and context.

    Returns (validation_status, validation_reason).

    Design rules:
    - Critical check failures (parseable, schema version, cross-id, mechanism) → 'failed'
    - Non-critical failures (bundle path, routing labels) → informational; do not force 'failed'
    - Share not provisioned + local workspace → 'not_provisioned'
    - No critical failures + local workspace + share provisioned → 'partially_validated'
    - No critical failures + personal_databricks → 'validated'
    """
    # Critical failures that make the status 'failed' regardless of other factors.
    # Non-critical failures (bundle_path_exists, bundle_path_referenced,
    # routing_labels_present, share_manifest_exists, share_manifest_parseable)
    # are surfaced in checks_failed and observations but do not force 'failed'.
    _CRITICAL_CHECKS = {
        CHECK_DELIVERY_EVENT_PARSEABLE,
        CHECK_DELIVERY_EVENT_SCHEMA_VERSION,
        CHECK_DELIVERY_MECHANISM_KNOWN,
        CHECK_CROSS_ID_CONSISTENCY,
        CHECK_SHARE_MANIFEST_HAS_SETUP_SQL,
        CHECK_SHARE_MANIFEST_HAS_C2_QUERIES,
    }

    critical_failed = [c for c in checks_failed if c in _CRITICAL_CHECKS]

    if critical_failed:
        return (
            VALIDATION_STATUS_FAILED,
            f"Validation failed: {len(critical_failed)} critical check(s) failed — "
            f"{critical_failed}. "
            "Producer-side delivery artifacts have schema or consistency errors. "
            "Review check_details for full detail.",
        )

    # No critical failures — derive status from provisioning state and workspace mode.
    # Non-critical failures (bundle path, routing labels) are surfaced in
    # checks_failed and observations but do not change the top-level status.
    share_provisioned = (
        parsed_manifest is not None
        and parsed_manifest.get("status") == "provisioned"
    )

    if workspace_mode == WORKSPACE_MODE_LOCAL_REPO_ONLY:
        if not share_provisioned:
            return (
                VALIDATION_STATUS_NOT_PROVISIONED,
                "Producer-side artifact validation completed. "
                "Delta Share provisioning status is 'designed': the share has not been "
                "created in Unity Catalog. "
                "Runtime validation cannot proceed until setup_sql is executed in a "
                "Databricks workspace with CREATE SHARE privilege. "
                "This is not an error — it is the honest C-1 baseline state. "
                + (
                    f"Non-critical check failures noted: {checks_failed}. "
                    if checks_failed
                    else ""
                ),
            )
        return (
            VALIDATION_STATUS_PARTIALLY_VALIDATED,
            "Producer-side artifact validation completed. "
            "Workspace mode is 'local_repo_only': no live Databricks workspace queries "
            "were executed. "
            "Runtime validation (Delta Share queryability, delivery event table row, "
            "payload conformance at v0.2.0) requires Databricks workspace access. "
            "Status: partially_validated — producer-side layer is verified; "
            "runtime end-to-end validation is pending."
            + (
                f" Non-critical check failures noted: {checks_failed}."
                if checks_failed
                else ""
            ),
        )

    # workspace_mode == personal_databricks
    return (
        VALIDATION_STATUS_VALIDATED,
        "All critical producer-side checks passed. "
        "Workspace mode is 'personal_databricks' with runtime evidence provided. "
        "Delivery layer is confirmed as runtime-checkable in the personal workspace."
        + (
            f" Non-critical check failures noted: {checks_failed}."
            if checks_failed
            else ""
        ),
    )


def _build_observations(
    parsed_event: Optional[DeliveryEvent],
    parsed_manifest: Optional[dict],
    validation_status: str,
    workspace_mode: str,
    checks_passed: List[str],
    checks_failed: List[str],
    share_provisioned: bool,
) -> List[str]:
    """Build the ordered observations list for the validation result."""
    observations: List[str] = []

    if parsed_event:
        observations.append(
            f"Delivery event '{parsed_event.delivery_event_id}' validated: "
            f"schema_version={parsed_event.schema_version}, "
            f"status='{parsed_event.status}', "
            f"exported_record_count={parsed_event.exported_record_count}, "
            f"routing_labels={parsed_event.routing_labels}."
        )
    else:
        observations.append(
            "Delivery event artifact was not found or could not be parsed."
        )

    if parsed_manifest:
        manifest_status = parsed_manifest.get("status", "unknown")
        n_queries = len(parsed_manifest.get("c2_validation_queries", []))
        observations.append(
            f"Share preparation manifest parsed: "
            f"status='{manifest_status}', "
            f"c2_validation_queries={n_queries}."
        )
        if not share_provisioned:
            observations.append(
                "Share provisioning status is 'designed': the Delta Share has not been "
                "executed in Unity Catalog. To proceed, run setup_sql from the manifest "
                "in a Databricks SQL notebook with CREATE SHARE privilege."
            )
    else:
        observations.append(
            "Share preparation manifest was not found or could not be parsed."
        )

    if workspace_mode == WORKSPACE_MODE_LOCAL_REPO_ONLY:
        observations.append(
            "Workspace mode is 'local_repo_only': no live Databricks workspace queries "
            "were executed. Runtime validation evidence (share queryability, "
            "delivery event table row, payload conformance) was not collected."
        )

    if checks_failed:
        observations.append(
            f"{len(checks_failed)} check(s) failed: {checks_failed}."
        )

    observations.append(
        f"Overall validation status: '{validation_status}'. "
        f"Checks passed: {len(checks_passed)}. "
        f"Checks failed: {len(checks_failed)}."
    )

    return observations


# ---------------------------------------------------------------------------
# Main validation entry point
# ---------------------------------------------------------------------------


def validate_delivery_layer(
    pipeline_run_id: str,
    delivery_event_path: Optional[Path] = None,
    share_manifest_path: Optional[Path] = None,
    expected_delivery_event_id: Optional[str] = None,
    workspace_mode: str = WORKSPACE_MODE_LOCAL_REPO_ONLY,
) -> DeliveryValidationResult:
    """
    Run the C-2 producer-side delivery-layer validation for a given pipeline run.

    This function validates the C-1 delivery artifacts (delivery event JSON and
    share preparation manifest) for the specified pipeline run. It is locally
    executable, credential-free, and does not require a live Databricks workspace.

    The result is always honest about what was and was not validated:
    - If no delivery event artifact exists, status is 'failed'.
    - If the Delta Share has not been provisioned and workspace is local,
      status is 'not_provisioned'.
    - If repo-side artifacts are correct but no runtime evidence exists,
      status is 'partially_validated'.
    - Status 'validated' requires personal_databricks workspace mode.

    Parameters
    ----------
    pipeline_run_id
        The pipeline run ID to validate. Must match the delivery event's
        pipeline_run_id for cross-ID consistency to pass.
    delivery_event_path
        Path to the delivery event JSON artifact produced by C-1.
        If None or the file does not exist, CHECK_DELIVERY_EVENT_EXISTS fails.
    share_manifest_path
        Path to the Delta Share preparation manifest JSON artifact.
        If None or the file does not exist, CHECK_SHARE_MANIFEST_EXISTS fails.
    expected_delivery_event_id
        Optional delivery event ID for cross-reference consistency check.
        If provided, must match delivery_event.delivery_event_id.
    workspace_mode
        Context for this validation run. Defaults to 'local_repo_only'.
        Use 'personal_databricks' only when runtime evidence was actually
        collected in a Databricks workspace.

    Returns
    -------
    DeliveryValidationResult
        Structured validation result with per-check detail, observations,
        and an honest overall validation_status.
    """
    validation_run_id = str(uuid.uuid4())
    validated_at = datetime.now(tz=timezone.utc).isoformat()
    validation_scope = VALIDATION_SCOPE_PRODUCER_SIDE_ONLY
    artifacts_checked: List[str] = []
    all_check_results: List[CheckResult] = []

    # --- Delivery event checks ---
    r = check_delivery_event_exists(delivery_event_path)
    all_check_results.append(r)
    if delivery_event_path:
        artifacts_checked.append(str(delivery_event_path))

    r_parse, parsed_event = check_delivery_event_parseable(delivery_event_path)
    all_check_results.append(r_parse)

    all_check_results.append(check_delivery_event_schema_version(parsed_event))
    all_check_results.append(check_delivery_event_status_known(parsed_event))
    all_check_results.append(check_delivery_mechanism_known(parsed_event))
    all_check_results.append(
        check_cross_id_consistency(
            parsed_event,
            expected_pipeline_run_id=pipeline_run_id,
            expected_delivery_event_id=expected_delivery_event_id,
        )
    )
    all_check_results.append(check_bundle_path_referenced(parsed_event))
    all_check_results.append(check_bundle_path_exists(parsed_event))
    all_check_results.append(check_routing_labels_present(parsed_event))

    # --- Share manifest checks ---
    all_check_results.append(check_share_manifest_exists(share_manifest_path))
    if share_manifest_path:
        artifacts_checked.append(str(share_manifest_path))

    r_sm_parse, parsed_manifest = check_share_manifest_parseable(share_manifest_path)
    all_check_results.append(r_sm_parse)

    all_check_results.append(check_share_manifest_has_setup_sql(parsed_manifest))
    all_check_results.append(check_share_manifest_has_c2_queries(parsed_manifest))
    all_check_results.append(check_share_provisioning_acknowledged(parsed_manifest))

    # --- Derive preliminary pass/fail sets ---
    checks_passed = [r.check_name for r in all_check_results if r.passed]
    checks_failed = [r.check_name for r in all_check_results if not r.passed]

    # --- Derive validation status ---
    validation_status, validation_reason = _derive_validation_status(
        checks_failed=checks_failed,
        parsed_manifest=parsed_manifest,
        workspace_mode=workspace_mode,
    )

    # --- Evidence sufficiency check (applied post-status derivation) ---
    r_evidence = check_evidence_sufficiency(
        validation_status=validation_status,
        workspace_mode=workspace_mode,
        validation_scope=validation_scope,
        checks_failed=checks_failed,
    )
    all_check_results.append(r_evidence)

    if r_evidence.passed:
        checks_passed.append(CHECK_EVIDENCE_SUFFICIENCY)
    else:
        checks_failed.append(CHECK_EVIDENCE_SUFFICIENCY)
        if validation_status == VALIDATION_STATUS_VALIDATED:
            validation_status = VALIDATION_STATUS_FAILED
            validation_reason = (
                f"Status downgraded to 'failed': evidence sufficiency check failed. "
                f"{r_evidence.detail}"
            )

    # --- Share provisioning state (for observations) ---
    share_provisioned = (
        parsed_manifest is not None
        and parsed_manifest.get("status") == "provisioned"
    )

    # --- Build observations ---
    observations = _build_observations(
        parsed_event=parsed_event,
        parsed_manifest=parsed_manifest,
        validation_status=validation_status,
        workspace_mode=workspace_mode,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        share_provisioned=share_provisioned,
    )

    # --- Resolve delivery details for the result record ---
    actual_delivery_event_id = (
        parsed_event.delivery_event_id if parsed_event else expected_delivery_event_id
    )
    share_name = (
        parsed_event.share_name
        if parsed_event and parsed_event.share_name
        else DEFAULT_SHARE_NAME
    )
    shared_object_name = (
        parsed_event.shared_object_name
        if parsed_event and parsed_event.shared_object_name
        else DEFAULT_SHARED_OBJECT_NAME
    )

    return DeliveryValidationResult(
        validation_run_id=validation_run_id,
        pipeline_run_id=pipeline_run_id,
        delivery_event_id=actual_delivery_event_id,
        validated_at=validated_at,
        delivery_mechanism=DELIVERY_MECHANISM_DELTA_SHARING,
        share_name=share_name,
        shared_object_name=shared_object_name,
        validation_scope=validation_scope,
        validation_status=validation_status,
        validation_reason=validation_reason,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        check_details=all_check_results,
        observations=observations,
        artifacts_checked=artifacts_checked,
        queries_executed=[],
        workspace_mode=workspace_mode,
        schema_version=VALIDATION_SCHEMA_VERSION,
    )


# ---------------------------------------------------------------------------
# Artifact path computation
# ---------------------------------------------------------------------------


def compute_validation_result_path(output_dir: Path, validation_run_id: str) -> Path:
    """Deterministic path for the validation result JSON artifact."""
    safe_id = validation_run_id.replace("/", "_").replace("\\", "_")
    return output_dir / f"delivery_validation_{safe_id}.json"


def compute_validation_result_text_path(output_dir: Path, validation_run_id: str) -> Path:
    """Deterministic text companion path for the validation result artifact."""
    safe_id = validation_run_id.replace("/", "_").replace("\\", "_")
    return output_dir / f"delivery_validation_{safe_id}.txt"


# ---------------------------------------------------------------------------
# Text formatter
# ---------------------------------------------------------------------------


def format_validation_result_text(result: DeliveryValidationResult) -> str:
    """Format a DeliveryValidationResult as a human-readable text summary."""
    lines = [
        "=" * 70,
        "DELIVERY LAYER VALIDATION RESULT — DATABRICKS CASEOPS LAKEHOUSE (C-2)",
        "=" * 70,
        f"Validation Run ID  : {result.validation_run_id}",
        f"Pipeline Run ID    : {result.pipeline_run_id}",
        f"Delivery Event ID  : {result.delivery_event_id or '(not resolved)'}",
        f"Validated At       : {result.validated_at}",
        f"Workspace Mode     : {result.workspace_mode}",
        f"Validation Scope   : {result.validation_scope}",
        f"Schema Version     : {result.schema_version}",
        "",
        "DELIVERY CONFIGURATION",
        "-" * 40,
        f"Delivery Mechanism : {result.delivery_mechanism}",
        f"Share Name         : {result.share_name or '(not set)'}",
        f"Shared Object      : {result.shared_object_name or '(not set)'}",
        "",
        "VALIDATION STATUS",
        "-" * 40,
        f"Status             : {result.validation_status.upper()}",
        f"Reason             : {result.validation_reason}",
        "",
        "CHECK SUMMARY",
        "-" * 40,
        f"Checks Passed ({len(result.checks_passed)})  : {result.checks_passed}",
        f"Checks Failed ({len(result.checks_failed)})  : {result.checks_failed}",
        "",
        "OBSERVATIONS",
        "-" * 40,
    ]
    for obs in result.observations:
        lines.append(f"  • {obs}")

    lines += [
        "",
        "ARTIFACTS CHECKED",
        "-" * 40,
    ]
    if result.artifacts_checked:
        for artifact in result.artifacts_checked:
            lines.append(f"  - {artifact}")
    else:
        lines.append("  (none)")

    if result.queries_executed:
        lines += [
            "",
            "QUERIES EXECUTED",
            "-" * 40,
        ]
        for q in result.queries_executed:
            lines.append(f"  - {q}")

    lines += [
        "",
        "=" * 70,
        "Validation scope: producer_side_only (local_repo_only baseline).",
        "Runtime end-to-end validation requires Databricks workspace access.",
        "See docs/delivery-runtime-validation.md for the C-2 runbook.",
        "=" * 70,
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------


def write_validation_result(
    result: DeliveryValidationResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """
    Write validation result artifacts (JSON + text) to output_dir.

    Creates the directory if it does not exist.

    Returns
    -------
    (json_path, text_path)
        Paths to the written JSON and text artifacts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = compute_validation_result_path(output_dir, result.validation_run_id)
    text_path = compute_validation_result_text_path(output_dir, result.validation_run_id)

    json_path.write_text(result.to_json_str(), encoding="utf-8")
    text_path.write_text(format_validation_result_text(result), encoding="utf-8")

    return json_path, text_path


# ---------------------------------------------------------------------------
# Load helper
# ---------------------------------------------------------------------------


def load_validation_result(json_path: Path) -> DeliveryValidationResult:
    """Load and validate a DeliveryValidationResult from a JSON artifact file."""
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    return DeliveryValidationResult.model_validate(raw)
