from __future__ import annotations

import json
from pathlib import Path

from src.pipelines.runtime_smoke_plan import (
    build_runtime_smoke_capture_plan,
    build_runtime_smoke_run_context,
    write_runtime_smoke_capture_plan,
    write_runtime_smoke_run_context,
)
from src.pipelines.runtime_smoke_preflight import (
    CHECK_CONTEXT_ARTIFACT_PATHS_MATCH_PLAN,
    CHECK_CONTEXT_REQUIRED_ENV_VARS,
    CHECK_PREFLIGHT_ARTIFACTS_SANITIZED,
    CHECK_RUN_ID_MATCHES,
    validate_runtime_smoke_preflight,
    write_runtime_smoke_preflight_result,
)
from src.schemas.runtime_smoke import (
    SMOKE_PREFLIGHT_STATUS_BLOCKED,
    SMOKE_PREFLIGHT_STATUS_READY,
)


PIPELINE_RUN_ID = "runtime-smoke-run-011"


def _write_ready_preflight_inputs(tmp_path: Path) -> dict[str, Path]:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        output_root=Path("output"),
        generated_at="2026-06-08T00:00:00+00:00",
    )
    capture_plan_path, capture_plan_text_path = write_runtime_smoke_capture_plan(
        plan,
        tmp_path / "output" / "validation",
    )
    context = build_runtime_smoke_run_context(
        plan=plan,
        capture_plan_json_path=capture_plan_path,
        capture_plan_text_path=capture_plan_text_path,
        generated_at="2026-06-08T00:00:00+00:00",
    )
    run_context_path, _ = write_runtime_smoke_run_context(
        context,
        tmp_path / "output" / "validation",
    )
    return {
        "capture_plan": capture_plan_path,
        "run_context": run_context_path,
    }


def test_runtime_smoke_preflight_ready_for_matching_plan_and_context(
    tmp_path: Path,
) -> None:
    paths = _write_ready_preflight_inputs(tmp_path)

    result = validate_runtime_smoke_preflight(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        capture_plan_path=paths["capture_plan"],
        run_context_path=paths["run_context"],
    )

    assert result.preflight_status == SMOKE_PREFLIGHT_STATUS_READY
    assert result.checks_failed == []
    assert CHECK_RUN_ID_MATCHES in result.checks_passed
    assert "not Databricks runtime smoke evidence" in " ".join(result.observations)


def test_runtime_smoke_preflight_blocked_when_context_artifact_path_drifts(
    tmp_path: Path,
) -> None:
    paths = _write_ready_preflight_inputs(tmp_path)
    raw_context = json.loads(paths["run_context"].read_text(encoding="utf-8"))
    for variable in raw_context["environment_variables"]:
        if variable["name"] == "CASEOPS_RUNTIME_EVIDENCE_PATH":
            variable["value"] = "output/validation/runtime_evidence/drifted.json"
    paths["run_context"].write_text(json.dumps(raw_context, indent=2), encoding="utf-8")

    result = validate_runtime_smoke_preflight(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        capture_plan_path=paths["capture_plan"],
        run_context_path=paths["run_context"],
    )

    assert result.preflight_status == SMOKE_PREFLIGHT_STATUS_BLOCKED
    assert CHECK_CONTEXT_ARTIFACT_PATHS_MATCH_PLAN in result.checks_failed


def test_runtime_smoke_preflight_blocked_when_required_context_var_missing(
    tmp_path: Path,
) -> None:
    paths = _write_ready_preflight_inputs(tmp_path)
    raw_context = json.loads(paths["run_context"].read_text(encoding="utf-8"))
    raw_context["environment_variables"] = [
        variable
        for variable in raw_context["environment_variables"]
        if variable["name"] != "CASEOPS_DELIVERY_EVENT_PATH"
    ]
    paths["run_context"].write_text(json.dumps(raw_context, indent=2), encoding="utf-8")

    result = validate_runtime_smoke_preflight(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        capture_plan_path=paths["capture_plan"],
        run_context_path=paths["run_context"],
    )

    assert result.preflight_status == SMOKE_PREFLIGHT_STATUS_BLOCKED
    assert CHECK_CONTEXT_REQUIRED_ENV_VARS in result.checks_failed


def test_runtime_smoke_preflight_blocked_when_preflight_artifact_has_sensitive_text(
    tmp_path: Path,
) -> None:
    paths = _write_ready_preflight_inputs(tmp_path)
    raw_plan = json.loads(paths["capture_plan"].read_text(encoding="utf-8"))
    raw_plan["observations"].append("workspace url: dbc-example.cloud.databricks.com")
    paths["capture_plan"].write_text(json.dumps(raw_plan, indent=2), encoding="utf-8")

    result = validate_runtime_smoke_preflight(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        capture_plan_path=paths["capture_plan"],
        run_context_path=paths["run_context"],
    )

    assert result.preflight_status == SMOKE_PREFLIGHT_STATUS_BLOCKED
    assert CHECK_PREFLIGHT_ARTIFACTS_SANITIZED in result.checks_failed


def test_write_runtime_smoke_preflight_result_writes_json_and_text(
    tmp_path: Path,
) -> None:
    paths = _write_ready_preflight_inputs(tmp_path)
    result = validate_runtime_smoke_preflight(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        capture_plan_path=paths["capture_plan"],
        run_context_path=paths["run_context"],
    )

    json_path, text_path = write_runtime_smoke_preflight_result(result, tmp_path / "out")

    assert json_path.exists()
    assert text_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["preflight_status"] == (
        SMOKE_PREFLIGHT_STATUS_READY
    )
    assert "Databricks Runtime Smoke Preflight" in text_path.read_text(encoding="utf-8")
