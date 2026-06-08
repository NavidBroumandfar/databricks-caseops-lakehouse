"""
Phase 4 runtime smoke evidence package validator.

This module validates the sanitized artifact package produced after a Databricks
runtime smoke run. It is local-safe: no Databricks SDK, no workspace URLs, no
credential access, and no live API calls.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.delivery_event import DeliveryEvent
from src.schemas.delivery_validation import (
    VALIDATION_SCOPE_END_TO_END,
    VALIDATION_STATUS_VALIDATED,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
    DeliveryValidationResult,
)
from src.schemas.runtime_evidence import DeliveryRuntimeEvidence
from src.schemas.runtime_smoke import (
    SMOKE_STATUS_FAILED,
    SMOKE_STATUS_INCOMPLETE,
    SMOKE_STATUS_READY,
    SMOKE_WORKSPACE_PERSONAL,
    RuntimeSmokeCapturePlan,
    RuntimeSmokeCheck,
    RuntimeSmokeValidationResult,
)
from src.pipelines.runtime_smoke_plan import (
    ARTIFACT_DELIVERY_EVENT,
    ARTIFACT_DELIVERY_VALIDATION,
    ARTIFACT_RUNTIME_EVIDENCE,
    ARTIFACT_SHARE_MANIFEST,
)


CHECK_ENVIRONMENT_SUPPORTED = "environment_supported"
CHECK_DELIVERY_EVENT_EXISTS = "delivery_event_exists"
CHECK_DELIVERY_EVENT_PARSEABLE = "delivery_event_parseable"
CHECK_DELIVERY_EVENT_RUN_MATCHES = "delivery_event_pipeline_run_matches"
CHECK_SHARE_MANIFEST_EXISTS = "share_manifest_exists"
CHECK_SHARE_MANIFEST_PARSEABLE = "share_manifest_parseable"
CHECK_SHARE_MANIFEST_PROVISIONED = "share_manifest_provisioned"
CHECK_RUNTIME_EVIDENCE_EXISTS = "runtime_evidence_exists"
CHECK_RUNTIME_EVIDENCE_PARSEABLE = "runtime_evidence_parseable"
CHECK_RUNTIME_EVIDENCE_RUN_MATCHES = "runtime_evidence_pipeline_run_matches"
CHECK_RUNTIME_EVIDENCE_COMPLETE = "runtime_evidence_complete"
CHECK_DELIVERY_VALIDATION_EXISTS = "delivery_validation_result_exists"
CHECK_DELIVERY_VALIDATION_PARSEABLE = "delivery_validation_result_parseable"
CHECK_DELIVERY_VALIDATION_RUN_MATCHES = "delivery_validation_pipeline_run_matches"
CHECK_DELIVERY_VALIDATION_VALIDATED = "delivery_validation_result_validated"
CHECK_CAPTURE_PLAN_PROVIDED = "capture_plan_provided"
CHECK_CAPTURE_PLAN_EXISTS = "capture_plan_exists"
CHECK_CAPTURE_PLAN_PARSEABLE = "capture_plan_parseable"
CHECK_CAPTURE_PLAN_RUN_MATCHES = "capture_plan_pipeline_run_matches"
CHECK_CAPTURE_PLAN_ENVIRONMENT_MATCHES = "capture_plan_environment_matches"
CHECK_CAPTURE_PLAN_WORKSPACE_MODE_MATCHES = "capture_plan_workspace_mode_matches"
CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH = "capture_plan_artifact_paths_match"
CHECK_ARTIFACTS_SANITIZED = "artifacts_sanitized"

_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"dbc-[a-z0-9-]+\.cloud\.databricks\.com", re.IGNORECASE),
    re.compile(r"dapi[a-f0-9]{32}", re.IGNORECASE),
    re.compile(r"activation\s+link", re.IGNORECASE),
    re.compile(r"workspace\s+url", re.IGNORECASE),
    re.compile(r"(token|secret|password)\s*[:=]", re.IGNORECASE),
)


def _pass(name: str, detail: Optional[str] = None) -> RuntimeSmokeCheck:
    return RuntimeSmokeCheck(check_name=name, passed=True, detail=detail)


def _fail(name: str, detail: str) -> RuntimeSmokeCheck:
    return RuntimeSmokeCheck(check_name=name, passed=False, detail=detail)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_text_if_exists(path: Optional[Path]) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def _parse_delivery_event(path: Optional[Path]) -> Tuple[list[RuntimeSmokeCheck], Optional[DeliveryEvent]]:
    checks: list[RuntimeSmokeCheck] = []
    if path is None:
        return [ _fail(CHECK_DELIVERY_EVENT_EXISTS, "No delivery event path provided.") ], None
    if not path.exists():
        return [ _fail(CHECK_DELIVERY_EVENT_EXISTS, f"Not found: {path}") ], None

    checks.append(_pass(CHECK_DELIVERY_EVENT_EXISTS, f"Found: {path}"))
    try:
        event = DeliveryEvent.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_DELIVERY_EVENT_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(
        _pass(
            CHECK_DELIVERY_EVENT_PARSEABLE,
            f"Parsed delivery_event_id={event.delivery_event_id}.",
        )
    )
    return checks, event


def _parse_share_manifest(path: Optional[Path]) -> Tuple[list[RuntimeSmokeCheck], Optional[dict]]:
    checks: list[RuntimeSmokeCheck] = []
    if path is None:
        return [ _fail(CHECK_SHARE_MANIFEST_EXISTS, "No share manifest path provided.") ], None
    if not path.exists():
        return [ _fail(CHECK_SHARE_MANIFEST_EXISTS, f"Not found: {path}") ], None

    checks.append(_pass(CHECK_SHARE_MANIFEST_EXISTS, f"Found: {path}"))
    try:
        manifest = _load_json(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_SHARE_MANIFEST_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    if not isinstance(manifest, dict):
        checks.append(_fail(CHECK_SHARE_MANIFEST_PARSEABLE, "Manifest is not a JSON object."))
        return checks, None
    checks.append(
        _pass(
            CHECK_SHARE_MANIFEST_PARSEABLE,
            f"Parsed share manifest status={manifest.get('status', '?')!r}.",
        )
    )
    return checks, manifest


def _parse_runtime_evidence(
    path: Optional[Path],
) -> Tuple[list[RuntimeSmokeCheck], Optional[DeliveryRuntimeEvidence]]:
    checks: list[RuntimeSmokeCheck] = []
    if path is None:
        return [ _fail(CHECK_RUNTIME_EVIDENCE_EXISTS, "No runtime evidence path provided.") ], None
    if not path.exists():
        return [ _fail(CHECK_RUNTIME_EVIDENCE_EXISTS, f"Not found: {path}") ], None

    checks.append(_pass(CHECK_RUNTIME_EVIDENCE_EXISTS, f"Found: {path}"))
    try:
        evidence = DeliveryRuntimeEvidence.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_RUNTIME_EVIDENCE_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(
        _pass(
            CHECK_RUNTIME_EVIDENCE_PARSEABLE,
            f"Parsed runtime evidence for share={evidence.share_name!r}.",
        )
    )
    return checks, evidence


def _parse_delivery_validation_result(
    path: Optional[Path],
) -> Tuple[list[RuntimeSmokeCheck], Optional[DeliveryValidationResult]]:
    checks: list[RuntimeSmokeCheck] = []
    if path is None:
        return [ _fail(CHECK_DELIVERY_VALIDATION_EXISTS, "No validation result path provided.") ], None
    if not path.exists():
        return [ _fail(CHECK_DELIVERY_VALIDATION_EXISTS, f"Not found: {path}") ], None

    checks.append(_pass(CHECK_DELIVERY_VALIDATION_EXISTS, f"Found: {path}"))
    try:
        result = DeliveryValidationResult.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_DELIVERY_VALIDATION_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(
        _pass(
            CHECK_DELIVERY_VALIDATION_PARSEABLE,
            f"Parsed validation_run_id={result.validation_run_id}.",
        )
    )
    return checks, result


def _parse_capture_plan(
    path: Optional[Path],
) -> Tuple[list[RuntimeSmokeCheck], Optional[RuntimeSmokeCapturePlan]]:
    checks: list[RuntimeSmokeCheck] = []
    if path is None:
        return [
            _fail(
                CHECK_CAPTURE_PLAN_PROVIDED,
                "No capture plan path provided. Phase 4 closeout requires a run-scoped capture plan.",
            )
        ], None
    checks.append(_pass(CHECK_CAPTURE_PLAN_PROVIDED, f"Provided: {path}"))
    if not path.exists():
        return [_fail(CHECK_CAPTURE_PLAN_EXISTS, f"Not found: {path}")], None

    checks.append(_pass(CHECK_CAPTURE_PLAN_EXISTS, f"Found: {path}"))
    try:
        plan = RuntimeSmokeCapturePlan.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_CAPTURE_PLAN_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(
        _pass(
            CHECK_CAPTURE_PLAN_PARSEABLE,
            f"Parsed capture_plan_id={plan.capture_plan_id}.",
        )
    )
    return checks, plan


def _planned_artifact_path_matches(
    plan: RuntimeSmokeCapturePlan,
    artifact_name: str,
    actual_path: Optional[Path],
) -> Optional[str]:
    expected = plan.artifact_path(artifact_name)
    if expected is None:
        return f"Capture plan is missing expected artifact {artifact_name!r}."
    if actual_path is None:
        return f"Capture plan expected {expected!r}, but no path was provided."
    observed = actual_path.as_posix()
    if observed != expected:
        return (
            f"Capture plan expected {artifact_name} at {expected!r}, "
            f"but validator received {observed!r}."
        )
    return None


def _validate_capture_plan_consistency(
    *,
    plan: Optional[RuntimeSmokeCapturePlan],
    pipeline_run_id: str,
    environment: str,
    workspace_mode: str,
    delivery_event_path: Optional[Path],
    share_manifest_path: Optional[Path],
    runtime_evidence_path: Optional[Path],
    delivery_validation_result_path: Optional[Path],
) -> list[RuntimeSmokeCheck]:
    if plan is None:
        return []

    checks: list[RuntimeSmokeCheck] = []
    if plan.pipeline_run_id == pipeline_run_id:
        checks.append(_pass(CHECK_CAPTURE_PLAN_RUN_MATCHES, "Capture plan run ID matches."))
    else:
        checks.append(
            _fail(
                CHECK_CAPTURE_PLAN_RUN_MATCHES,
                f"Expected {pipeline_run_id!r}, observed {plan.pipeline_run_id!r}.",
            )
        )

    if plan.environment == environment:
        checks.append(_pass(CHECK_CAPTURE_PLAN_ENVIRONMENT_MATCHES, "Capture plan environment matches."))
    else:
        checks.append(
            _fail(
                CHECK_CAPTURE_PLAN_ENVIRONMENT_MATCHES,
                f"Expected {environment!r}, observed {plan.environment!r}.",
            )
        )

    if plan.workspace_mode == workspace_mode:
        checks.append(
            _pass(CHECK_CAPTURE_PLAN_WORKSPACE_MODE_MATCHES, "Capture plan workspace mode matches.")
        )
    else:
        checks.append(
            _fail(
                CHECK_CAPTURE_PLAN_WORKSPACE_MODE_MATCHES,
                f"Expected {workspace_mode!r}, observed {plan.workspace_mode!r}.",
            )
        )

    mismatches = [
        mismatch
        for mismatch in (
            _planned_artifact_path_matches(plan, ARTIFACT_DELIVERY_EVENT, delivery_event_path),
            _planned_artifact_path_matches(plan, ARTIFACT_SHARE_MANIFEST, share_manifest_path),
            _planned_artifact_path_matches(plan, ARTIFACT_RUNTIME_EVIDENCE, runtime_evidence_path),
            _planned_artifact_path_matches(
                plan,
                ARTIFACT_DELIVERY_VALIDATION,
                delivery_validation_result_path,
            ),
        )
        if mismatch is not None
    ]
    if mismatches:
        checks.append(
            _fail(
                CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH,
                " ".join(mismatches),
            )
        )
    else:
        checks.append(
            _pass(
                CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH,
                "Validator artifact paths match the capture plan.",
            )
        )
    return checks


def _scan_artifacts_for_sensitive_text(paths: list[Path]) -> RuntimeSmokeCheck:
    for path in paths:
        text = _read_text_if_exists(path)
        for pattern in _SENSITIVE_TEXT_PATTERNS:
            if pattern.search(text):
                return _fail(
                    CHECK_ARTIFACTS_SANITIZED,
                    f"Potential sensitive text found in {path}: {pattern.pattern}",
                )
    return _pass(
        CHECK_ARTIFACTS_SANITIZED,
        "Checked smoke package artifacts for URL/token/activation-link patterns.",
    )


def _status_from_checks(checks: list[RuntimeSmokeCheck]) -> tuple[str, str]:
    failed = [check.check_name for check in checks if not check.passed]
    if not failed:
        return (
            SMOKE_STATUS_READY,
            "Phase 4 smoke evidence package is complete and ready for closeout review.",
        )
    critical_parse_failures = {
        CHECK_DELIVERY_EVENT_PARSEABLE,
        CHECK_SHARE_MANIFEST_PARSEABLE,
        CHECK_RUNTIME_EVIDENCE_PARSEABLE,
        CHECK_DELIVERY_VALIDATION_PARSEABLE,
        CHECK_ARTIFACTS_SANITIZED,
    }
    if any(name in critical_parse_failures for name in failed):
        return (
            SMOKE_STATUS_FAILED,
            f"Smoke package validation failed: {failed}.",
        )
    return (
        SMOKE_STATUS_INCOMPLETE,
        f"Smoke package is incomplete: {failed}.",
    )


def validate_runtime_smoke_package(
    *,
    pipeline_run_id: str,
    environment: str,
    delivery_event_path: Optional[Path],
    share_manifest_path: Optional[Path],
    runtime_evidence_path: Optional[Path],
    delivery_validation_result_path: Optional[Path],
    capture_plan_path: Optional[Path] = None,
    workspace_mode: str = SMOKE_WORKSPACE_PERSONAL,
) -> RuntimeSmokeValidationResult:
    """Validate a Phase 4 runtime smoke evidence package."""
    checks: list[RuntimeSmokeCheck] = []
    artifacts_checked = [
        str(path)
        for path in (
            delivery_event_path,
            share_manifest_path,
            runtime_evidence_path,
            delivery_validation_result_path,
            capture_plan_path,
        )
        if path is not None
    ]

    if environment in {"dev", "staging", "prod"}:
        checks.append(_pass(CHECK_ENVIRONMENT_SUPPORTED, f"environment={environment!r}."))
    else:
        checks.append(_fail(CHECK_ENVIRONMENT_SUPPORTED, f"Unsupported environment: {environment!r}."))

    event_checks, event = _parse_delivery_event(delivery_event_path)
    checks.extend(event_checks)
    manifest_checks, manifest = _parse_share_manifest(share_manifest_path)
    checks.extend(manifest_checks)
    evidence_checks, evidence = _parse_runtime_evidence(runtime_evidence_path)
    checks.extend(evidence_checks)
    validation_checks, validation = _parse_delivery_validation_result(
        delivery_validation_result_path
    )
    checks.extend(validation_checks)
    capture_plan_checks, capture_plan = _parse_capture_plan(capture_plan_path)
    checks.extend(capture_plan_checks)

    if event is not None and event.pipeline_run_id == pipeline_run_id:
        checks.append(_pass(CHECK_DELIVERY_EVENT_RUN_MATCHES, "Delivery event run ID matches."))
    else:
        observed = event.pipeline_run_id if event is not None else None
        checks.append(
            _fail(
                CHECK_DELIVERY_EVENT_RUN_MATCHES,
                f"Expected {pipeline_run_id!r}, observed {observed!r}.",
            )
        )

    if manifest is not None and manifest.get("status") == "provisioned":
        checks.append(_pass(CHECK_SHARE_MANIFEST_PROVISIONED, "Share manifest status is provisioned."))
    else:
        observed = manifest.get("status") if isinstance(manifest, dict) else None
        checks.append(
            _fail(
                CHECK_SHARE_MANIFEST_PROVISIONED,
                f"Expected share manifest status 'provisioned', observed {observed!r}.",
            )
        )

    if evidence is not None and evidence.pipeline_run_id == pipeline_run_id:
        checks.append(_pass(CHECK_RUNTIME_EVIDENCE_RUN_MATCHES, "Runtime evidence run ID matches."))
    else:
        observed = evidence.pipeline_run_id if evidence is not None else None
        checks.append(
            _fail(
                CHECK_RUNTIME_EVIDENCE_RUN_MATCHES,
                f"Expected {pipeline_run_id!r}, observed {observed!r}.",
            )
        )

    if evidence is not None and (
        not evidence.missing_required_queries()
        and not evidence.failed_required_queries()
        and not evidence.missing_or_false_assertions()
        and evidence.sanitization.all_confirmed()
    ):
        checks.append(
            _pass(
                CHECK_RUNTIME_EVIDENCE_COMPLETE,
                "Required queries, assertions, and sanitization flags are complete.",
            )
        )
    else:
        detail = "Runtime evidence is missing, incomplete, failed, or not fully sanitized."
        if evidence is not None:
            detail = (
                f"missing_queries={evidence.missing_required_queries()}, "
                f"failed_queries={evidence.failed_required_queries()}, "
                f"missing_assertions={evidence.missing_or_false_assertions()}, "
                f"sanitized={evidence.sanitization.all_confirmed()}."
            )
        checks.append(_fail(CHECK_RUNTIME_EVIDENCE_COMPLETE, detail))

    if validation is not None and validation.pipeline_run_id == pipeline_run_id:
        checks.append(_pass(CHECK_DELIVERY_VALIDATION_RUN_MATCHES, "Delivery validation run ID matches."))
    else:
        observed = validation.pipeline_run_id if validation is not None else None
        checks.append(
            _fail(
                CHECK_DELIVERY_VALIDATION_RUN_MATCHES,
                f"Expected {pipeline_run_id!r}, observed {observed!r}.",
            )
        )

    if (
        validation is not None
        and validation.validation_status == VALIDATION_STATUS_VALIDATED
        and validation.validation_scope == VALIDATION_SCOPE_END_TO_END
        and validation.workspace_mode == WORKSPACE_MODE_PERSONAL_DATABRICKS
    ):
        checks.append(
            _pass(
                CHECK_DELIVERY_VALIDATION_VALIDATED,
                "Delivery validation result is validated end_to_end in personal_databricks mode.",
            )
        )
    else:
        observed = None
        if validation is not None:
            observed = (
                validation.validation_status,
                validation.validation_scope,
                validation.workspace_mode,
            )
        checks.append(
            _fail(
                CHECK_DELIVERY_VALIDATION_VALIDATED,
                "Expected ('validated', 'end_to_end', 'personal_databricks'), "
                f"observed {observed!r}.",
            )
        )

    checks.extend(
        _validate_capture_plan_consistency(
            plan=capture_plan,
            pipeline_run_id=pipeline_run_id,
            environment=environment,
            workspace_mode=workspace_mode,
            delivery_event_path=delivery_event_path,
            share_manifest_path=share_manifest_path,
            runtime_evidence_path=runtime_evidence_path,
            delivery_validation_result_path=delivery_validation_result_path,
        )
    )

    checks.append(
        _scan_artifacts_for_sensitive_text(
            [
                path
                for path in (
                    delivery_event_path,
                    share_manifest_path,
                    runtime_evidence_path,
                    delivery_validation_result_path,
                )
                if path is not None
            ]
        )
    )

    status, reason = _status_from_checks(checks)
    checks_passed = [check.check_name for check in checks if check.passed]
    checks_failed = [check.check_name for check in checks if not check.passed]
    observations = [
        f"Artifacts checked: {len(artifacts_checked)}.",
        f"Checks passed: {len(checks_passed)}. Checks failed: {len(checks_failed)}.",
        f"Smoke status: {status}.",
    ]
    if status != SMOKE_STATUS_READY:
        observations.append(
            "This package must not be used as Phase 4 closeout evidence until all checks pass."
        )

    return RuntimeSmokeValidationResult(
        validation_run_id=str(uuid.uuid4()),
        pipeline_run_id=pipeline_run_id,
        environment=environment,
        workspace_mode=workspace_mode,
        validated_at=datetime.now(tz=timezone.utc).isoformat(),
        smoke_status=status,
        smoke_reason=reason,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        check_details=checks,
        artifacts_checked=artifacts_checked,
        observations=observations,
    )


def format_runtime_smoke_result_text(result: RuntimeSmokeValidationResult) -> str:
    """Format a runtime smoke validation result for review."""
    lines = [
        "Databricks Runtime Smoke Package Validation",
        "=" * 52,
        f"validation_run_id : {result.validation_run_id}",
        f"pipeline_run_id   : {result.pipeline_run_id}",
        f"environment       : {result.environment}",
        f"workspace_mode    : {result.workspace_mode}",
        f"smoke_status      : {result.smoke_status}",
        f"smoke_reason      : {result.smoke_reason}",
        "",
        "Checks",
        "-" * 52,
    ]
    for check in result.check_details:
        marker = "PASS" if check.passed else "FAIL"
        detail = f" - {check.detail}" if check.detail else ""
        lines.append(f"[{marker}] {check.check_name}{detail}")
    if result.observations:
        lines.extend(["", "Observations", "-" * 52])
        lines.extend(f"- {observation}" for observation in result.observations)
    return "\n".join(lines) + "\n"


def _result_json_path(output_dir: Path, pipeline_run_id: str) -> Path:
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", pipeline_run_id)
    return output_dir / f"runtime_smoke_validation_{safe_id}.json"


def _result_text_path(output_dir: Path, pipeline_run_id: str) -> Path:
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", pipeline_run_id)
    return output_dir / f"runtime_smoke_validation_{safe_id}.txt"


def write_runtime_smoke_result(
    result: RuntimeSmokeValidationResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write JSON and text runtime smoke validation artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = _result_json_path(output_dir, result.pipeline_run_id)
    text_path = _result_text_path(output_dir, result.pipeline_run_id)
    json_path.write_text(result.to_json_str(indent=2) + "\n", encoding="utf-8")
    text_path.write_text(format_runtime_smoke_result_text(result), encoding="utf-8")
    return json_path, text_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a sanitized Phase 4 Databricks runtime smoke package.",
    )
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--delivery-event-path", required=True)
    parser.add_argument("--share-manifest-path", required=True)
    parser.add_argument("--runtime-evidence-path", required=True)
    parser.add_argument("--delivery-validation-result-path", required=True)
    parser.add_argument(
        "--capture-plan-path",
        default=None,
        help=(
            "Runtime smoke capture plan JSON. Required for ready_for_phase4_closeout; "
            "omitting it leaves the package incomplete."
        ),
    )
    parser.add_argument("--output-dir", default="output/validation")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = validate_runtime_smoke_package(
        pipeline_run_id=args.pipeline_run_id,
        environment=args.environment,
        delivery_event_path=Path(args.delivery_event_path),
        share_manifest_path=Path(args.share_manifest_path),
        runtime_evidence_path=Path(args.runtime_evidence_path),
        delivery_validation_result_path=Path(args.delivery_validation_result_path),
        capture_plan_path=Path(args.capture_plan_path) if args.capture_plan_path else None,
    )
    json_path, text_path = write_runtime_smoke_result(result, Path(args.output_dir))
    print(f"Runtime smoke status: {result.smoke_status}")
    print(f"Runtime smoke reason: {result.smoke_reason}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {text_path}")


if __name__ == "__main__":
    main()
