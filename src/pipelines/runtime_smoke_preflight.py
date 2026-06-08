"""
Phase 4 runtime smoke preflight validator.

This module checks local, non-secret coordination artifacts before an operator
runs the Databricks workspace smoke workflow. It does not contact Databricks and
does not prove that runtime smoke evidence exists.
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

from src.pipelines.runtime_smoke_plan import (
    ARTIFACT_DELIVERY_EVENT,
    ARTIFACT_DELIVERY_VALIDATION,
    ARTIFACT_RUNTIME_EVIDENCE,
    ARTIFACT_SHARE_MANIFEST,
)
from src.schemas.runtime_smoke import (
    SMOKE_PREFLIGHT_STATUS_BLOCKED,
    SMOKE_PREFLIGHT_STATUS_READY,
    SMOKE_WORKSPACE_PERSONAL,
    RuntimeSmokeCapturePlan,
    RuntimeSmokeCheck,
    RuntimeSmokePreflightResult,
    RuntimeSmokeRunContext,
)


CHECK_PLAN_EXISTS = "capture_plan_exists"
CHECK_PLAN_PARSEABLE = "capture_plan_parseable"
CHECK_CONTEXT_EXISTS = "run_context_exists"
CHECK_CONTEXT_PARSEABLE = "run_context_parseable"
CHECK_RUN_ID_MATCHES = "run_id_matches"
CHECK_ENVIRONMENT_MATCHES = "environment_matches"
CHECK_WORKSPACE_MODE_MATCHES = "workspace_mode_matches"
CHECK_CONTEXT_REFERENCES_PLAN = "run_context_references_capture_plan"
CHECK_CONTEXT_REQUIRED_ENV_VARS = "run_context_required_env_vars"
CHECK_CONTEXT_ARTIFACT_PATHS_MATCH_PLAN = "run_context_artifact_paths_match_plan"
CHECK_PLAN_COMMANDS_COVER_WORKFLOW = "capture_plan_commands_cover_workflow"
CHECK_PREFLIGHT_ARTIFACTS_SANITIZED = "preflight_artifacts_sanitized"

_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"dbc-[a-z0-9-]+\.cloud\.databricks\.com", re.IGNORECASE),
    re.compile(r"dapi[a-f0-9]{32}", re.IGNORECASE),
    re.compile(r"activation\s+link\s*[:=]", re.IGNORECASE),
    re.compile(r"workspace\s+url\s*[:=]", re.IGNORECASE),
    re.compile(r"(token|secret|password)\s*[:=]", re.IGNORECASE),
)

_REQUIRED_CONTEXT_ENV_VARS = {
    "CASEOPS_ENV",
    "CASEOPS_DELIVERY_PIPELINE_RUN_ID",
    "CASEOPS_DELIVERY_EVENT_PATH",
    "CASEOPS_SHARE_MANIFEST_PATH",
    "CASEOPS_RUNTIME_EVIDENCE_PATH",
    "CASEOPS_SMOKE_CAPTURE_PLAN_PATH",
}

_ENV_VAR_TO_ARTIFACT = {
    "CASEOPS_DELIVERY_EVENT_PATH": ARTIFACT_DELIVERY_EVENT,
    "CASEOPS_SHARE_MANIFEST_PATH": ARTIFACT_SHARE_MANIFEST,
    "CASEOPS_RUNTIME_EVIDENCE_PATH": ARTIFACT_RUNTIME_EVIDENCE,
}


def _pass(name: str, detail: Optional[str] = None) -> RuntimeSmokeCheck:
    return RuntimeSmokeCheck(check_name=name, passed=True, detail=detail)


def _fail(name: str, detail: str) -> RuntimeSmokeCheck:
    return RuntimeSmokeCheck(check_name=name, passed=False, detail=detail)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_capture_plan(
    path: Optional[Path],
) -> Tuple[list[RuntimeSmokeCheck], Optional[RuntimeSmokeCapturePlan]]:
    if path is None:
        return [_fail(CHECK_PLAN_EXISTS, "No capture plan path provided.")], None
    if not path.exists():
        return [_fail(CHECK_PLAN_EXISTS, f"Not found: {path}")], None

    checks = [_pass(CHECK_PLAN_EXISTS, f"Found: {path}")]
    try:
        plan = RuntimeSmokeCapturePlan.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_PLAN_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(_pass(CHECK_PLAN_PARSEABLE, f"Parsed capture_plan_id={plan.capture_plan_id}."))
    return checks, plan


def _parse_run_context(
    path: Optional[Path],
) -> Tuple[list[RuntimeSmokeCheck], Optional[RuntimeSmokeRunContext]]:
    if path is None:
        return [_fail(CHECK_CONTEXT_EXISTS, "No run context path provided.")], None
    if not path.exists():
        return [_fail(CHECK_CONTEXT_EXISTS, f"Not found: {path}")], None

    checks = [_pass(CHECK_CONTEXT_EXISTS, f"Found: {path}")]
    try:
        context = RuntimeSmokeRunContext.model_validate(_load_json(path))
    except Exception as exc:  # noqa: BLE001
        checks.append(_fail(CHECK_CONTEXT_PARSEABLE, f"Failed to parse: {exc}"))
        return checks, None
    checks.append(_pass(CHECK_CONTEXT_PARSEABLE, f"Parsed context_id={context.context_id}."))
    return checks, context


def _validate_plan_context_consistency(
    *,
    plan: Optional[RuntimeSmokeCapturePlan],
    context: Optional[RuntimeSmokeRunContext],
    capture_plan_path: Optional[Path],
    pipeline_run_id: str,
    environment: str,
    workspace_mode: str,
) -> list[RuntimeSmokeCheck]:
    checks: list[RuntimeSmokeCheck] = []
    if plan is None or context is None:
        return checks

    if plan.pipeline_run_id == context.pipeline_run_id == pipeline_run_id:
        checks.append(_pass(CHECK_RUN_ID_MATCHES, "Plan, context, and requested run ID match."))
    else:
        checks.append(
            _fail(
                CHECK_RUN_ID_MATCHES,
                (
                    f"Expected {pipeline_run_id!r}; plan={plan.pipeline_run_id!r}, "
                    f"context={context.pipeline_run_id!r}."
                ),
            )
        )

    if plan.environment == context.environment == environment:
        checks.append(_pass(CHECK_ENVIRONMENT_MATCHES, "Plan, context, and requested environment match."))
    else:
        checks.append(
            _fail(
                CHECK_ENVIRONMENT_MATCHES,
                f"Expected {environment!r}; plan={plan.environment!r}, context={context.environment!r}.",
            )
        )

    if plan.workspace_mode == context.workspace_mode == workspace_mode:
        checks.append(_pass(CHECK_WORKSPACE_MODE_MATCHES, "Plan, context, and workspace mode match."))
    else:
        checks.append(
            _fail(
                CHECK_WORKSPACE_MODE_MATCHES,
                (
                    f"Expected {workspace_mode!r}; plan={plan.workspace_mode!r}, "
                    f"context={context.workspace_mode!r}."
                ),
            )
        )

    observed_plan_path = capture_plan_path.as_posix() if capture_plan_path is not None else None
    if context.capture_plan_path == observed_plan_path:
        checks.append(_pass(CHECK_CONTEXT_REFERENCES_PLAN, "Run context points at the capture plan path."))
    else:
        checks.append(
            _fail(
                CHECK_CONTEXT_REFERENCES_PLAN,
                f"Expected {observed_plan_path!r}, observed {context.capture_plan_path!r}.",
            )
        )

    missing_vars = sorted(
        name
        for name in _REQUIRED_CONTEXT_ENV_VARS
        if context.environment_variable(name) in (None, "")
    )
    if missing_vars:
        checks.append(
            _fail(
                CHECK_CONTEXT_REQUIRED_ENV_VARS,
                f"Missing required run context variables: {missing_vars}.",
            )
        )
    else:
        checks.append(
            _pass(
                CHECK_CONTEXT_REQUIRED_ENV_VARS,
                "Run context includes all required non-secret variables.",
            )
        )

    path_mismatches = []
    for variable_name, artifact_name in _ENV_VAR_TO_ARTIFACT.items():
        expected = plan.artifact_path(artifact_name)
        observed = context.environment_variable(variable_name)
        if expected != observed:
            path_mismatches.append(f"{variable_name}: expected {expected!r}, observed {observed!r}")
    expected_plan_path = capture_plan_path.as_posix() if capture_plan_path is not None else None
    observed_plan_var = context.environment_variable("CASEOPS_SMOKE_CAPTURE_PLAN_PATH")
    if expected_plan_path != observed_plan_var:
        path_mismatches.append(
            (
                "CASEOPS_SMOKE_CAPTURE_PLAN_PATH: "
                f"expected {expected_plan_path!r}, observed {observed_plan_var!r}"
            )
        )
    if path_mismatches:
        checks.append(
            _fail(
                CHECK_CONTEXT_ARTIFACT_PATHS_MATCH_PLAN,
                " ".join(path_mismatches),
            )
        )
    else:
        checks.append(
            _pass(
                CHECK_CONTEXT_ARTIFACT_PATHS_MATCH_PLAN,
                "Run context artifact variables match capture plan paths.",
            )
        )

    command_text = "\n".join(plan.workspace_commands + plan.local_validation_commands)
    missing_command_parts = [
        part
        for part in (
            "--stage all",
            "--stage delivery_validation",
            "--capture-plan-path",
            f"--pipeline-run-id {pipeline_run_id}",
            f"--environment {environment}",
        )
        if part not in command_text
    ]
    if missing_command_parts:
        checks.append(
            _fail(
                CHECK_PLAN_COMMANDS_COVER_WORKFLOW,
                f"Capture plan commands are missing: {missing_command_parts}.",
            )
        )
    else:
        checks.append(
            _pass(
                CHECK_PLAN_COMMANDS_COVER_WORKFLOW,
                "Capture plan includes runtime, delivery validation, and local validation commands.",
            )
        )
    return checks


def _scan_artifacts_for_sensitive_text(paths: list[Path]) -> RuntimeSmokeCheck:
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in _SENSITIVE_TEXT_PATTERNS:
            if pattern.search(text):
                return _fail(
                    CHECK_PREFLIGHT_ARTIFACTS_SANITIZED,
                    f"Potential sensitive text found in {path}: {pattern.pattern}",
                )
    return _pass(
        CHECK_PREFLIGHT_ARTIFACTS_SANITIZED,
        "Checked preflight artifacts for URL/token/activation-link patterns.",
    )


def validate_runtime_smoke_preflight(
    *,
    pipeline_run_id: str,
    environment: str,
    capture_plan_path: Optional[Path],
    run_context_path: Optional[Path],
    workspace_mode: str = SMOKE_WORKSPACE_PERSONAL,
) -> RuntimeSmokePreflightResult:
    """Validate run-scoped coordination artifacts before workspace execution."""
    artifacts_checked = [
        str(path)
        for path in (capture_plan_path, run_context_path)
        if path is not None
    ]
    checks: list[RuntimeSmokeCheck] = []

    plan_checks, plan = _parse_capture_plan(capture_plan_path)
    checks.extend(plan_checks)
    context_checks, context = _parse_run_context(run_context_path)
    checks.extend(context_checks)
    checks.extend(
        _validate_plan_context_consistency(
            plan=plan,
            context=context,
            capture_plan_path=capture_plan_path,
            pipeline_run_id=pipeline_run_id,
            environment=environment,
            workspace_mode=workspace_mode,
        )
    )
    checks.append(
        _scan_artifacts_for_sensitive_text(
            [
                path
                for path in (capture_plan_path, run_context_path)
                if path is not None
            ]
        )
    )

    checks_passed = [check.check_name for check in checks if check.passed]
    checks_failed = [check.check_name for check in checks if not check.passed]
    if checks_failed:
        status = SMOKE_PREFLIGHT_STATUS_BLOCKED
        reason = f"Runtime smoke preflight is blocked: {checks_failed}."
    else:
        status = SMOKE_PREFLIGHT_STATUS_READY
        reason = "Runtime smoke coordination artifacts are ready for workspace execution."

    observations = [
        "This preflight result is not Databricks runtime smoke evidence.",
        "Phase 4 closeout still requires sanitized workspace evidence and smoke package validation.",
        f"Artifacts checked: {len(artifacts_checked)}.",
        f"Checks passed: {len(checks_passed)}. Checks failed: {len(checks_failed)}.",
    ]

    return RuntimeSmokePreflightResult(
        preflight_run_id=str(uuid.uuid4()),
        pipeline_run_id=pipeline_run_id,
        environment=environment,
        workspace_mode=workspace_mode,
        checked_at=datetime.now(tz=timezone.utc).isoformat(),
        preflight_status=status,
        preflight_reason=reason,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        check_details=checks,
        artifacts_checked=artifacts_checked,
        observations=observations,
    )


def format_runtime_smoke_preflight_text(result: RuntimeSmokePreflightResult) -> str:
    """Format a runtime smoke preflight result for operator review."""
    lines = [
        "Databricks Runtime Smoke Preflight",
        "=" * 52,
        f"preflight_run_id : {result.preflight_run_id}",
        f"pipeline_run_id  : {result.pipeline_run_id}",
        f"environment      : {result.environment}",
        f"workspace_mode   : {result.workspace_mode}",
        f"preflight_status : {result.preflight_status}",
        f"preflight_reason : {result.preflight_reason}",
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


def _safe_pipeline_run_id(pipeline_run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", pipeline_run_id)


def _result_json_path(output_dir: Path, pipeline_run_id: str) -> Path:
    return output_dir / f"runtime_smoke_preflight_{_safe_pipeline_run_id(pipeline_run_id)}.json"


def _result_text_path(output_dir: Path, pipeline_run_id: str) -> Path:
    return output_dir / f"runtime_smoke_preflight_{_safe_pipeline_run_id(pipeline_run_id)}.txt"


def write_runtime_smoke_preflight_result(
    result: RuntimeSmokePreflightResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write JSON and text runtime smoke preflight artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = _result_json_path(output_dir, result.pipeline_run_id)
    text_path = _result_text_path(output_dir, result.pipeline_run_id)
    json_path.write_text(result.to_json_str(indent=2) + "\n", encoding="utf-8")
    text_path.write_text(format_runtime_smoke_preflight_text(result), encoding="utf-8")
    return json_path, text_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate local Phase 4 Databricks runtime smoke coordination artifacts.",
    )
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--capture-plan-path", required=True)
    parser.add_argument("--run-context-path", required=True)
    parser.add_argument("--output-dir", default="output/validation")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = validate_runtime_smoke_preflight(
        pipeline_run_id=args.pipeline_run_id,
        environment=args.environment,
        capture_plan_path=Path(args.capture_plan_path),
        run_context_path=Path(args.run_context_path),
    )
    json_path, text_path = write_runtime_smoke_preflight_result(result, Path(args.output_dir))
    print(f"Runtime smoke preflight status: {result.preflight_status}")
    print(f"Runtime smoke preflight reason: {result.preflight_reason}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {text_path}")
    print("Preflight complete. This is not runtime smoke evidence.")


if __name__ == "__main__":
    main()
