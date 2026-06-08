from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.pipelines.runtime_smoke_plan import (
    ARTIFACT_DELIVERY_EVENT,
    ARTIFACT_DELIVERY_VALIDATION,
    ARTIFACT_RUNTIME_EVIDENCE,
    ARTIFACT_RUNTIME_SMOKE_VALIDATION,
    ARTIFACT_SHARE_MANIFEST,
    build_runtime_smoke_capture_plan,
    format_runtime_smoke_capture_plan_text,
    write_runtime_smoke_capture_plan,
)
from src.schemas.runtime_smoke import SMOKE_WORKSPACE_PERSONAL, RuntimeSmokeCapturePlan


PIPELINE_RUN_ID = "runtime-smoke/run 001"


def test_build_runtime_smoke_capture_plan_uses_run_scoped_paths() -> None:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        output_root=Path("output"),
        generated_at="2026-06-08T00:00:00+00:00",
    )

    assert plan.environment == "dev"
    assert plan.workspace_mode == SMOKE_WORKSPACE_PERSONAL
    assert plan.artifact_path(ARTIFACT_DELIVERY_EVENT) == (
        "output/delivery/delivery_event_runtime-smoke_run_001.json"
    )
    assert plan.artifact_path(ARTIFACT_RUNTIME_EVIDENCE) == (
        "output/validation/runtime_evidence/runtime_evidence_runtime-smoke_run_001.json"
    )
    assert plan.artifact_path(ARTIFACT_DELIVERY_VALIDATION) == (
        "output/validation/delivery_validation_runtime-smoke_run_001.json"
    )
    assert plan.artifact_path(ARTIFACT_RUNTIME_SMOKE_VALIDATION) == (
        "output/validation/runtime_smoke_validation_runtime-smoke_run_001.json"
    )
    assert plan.artifact_path(ARTIFACT_SHARE_MANIFEST) == (
        "output/delivery/delta_share_preparation_manifest.json"
    )


def test_build_runtime_smoke_capture_plan_includes_validation_commands() -> None:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id="runtime-smoke-run-002",
        environment="staging",
        generated_at="2026-06-08T00:00:00+00:00",
    )

    assert any("--pipeline-run-id runtime-smoke-run-002" in c for c in plan.workspace_commands)
    assert any("--delivery-pipeline-run-id runtime-smoke-run-002" in c for c in plan.workspace_commands)
    assert any("--environment staging" in c for c in plan.local_validation_commands)
    assert any("--capture-plan-path" in c for c in plan.local_validation_commands)
    assert any("--workspace-mode personal_databricks" in c for c in plan.workspace_commands)


def test_runtime_smoke_capture_plan_rejects_unsupported_environment() -> None:
    with pytest.raises(ValueError, match="environment must be one of"):
        build_runtime_smoke_capture_plan(
            pipeline_run_id="runtime-smoke-run-003",
            environment="qa",
        )


def test_format_runtime_smoke_capture_plan_text_has_guardrails() -> None:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id="runtime-smoke-run-004",
        environment="prod",
        generated_at="2026-06-08T00:00:00+00:00",
    )

    text = format_runtime_smoke_capture_plan_text(plan)

    assert "Databricks Runtime Smoke Capture Plan" in text
    assert "This capture plan is not runtime evidence" in text
    assert "Remove Databricks workspace URLs" in text
    assert "ready_for_phase4_closeout" in text


def test_write_runtime_smoke_capture_plan_writes_json_and_text(tmp_path: Path) -> None:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id="runtime-smoke-run-005",
        environment="dev",
        generated_at="2026-06-08T00:00:00+00:00",
    )

    json_path, text_path = write_runtime_smoke_capture_plan(plan, tmp_path)

    assert json_path.exists()
    assert text_path.exists()
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    parsed = RuntimeSmokeCapturePlan.model_validate(raw)
    assert parsed.pipeline_run_id == "runtime-smoke-run-005"
    assert "Workspace Commands" in text_path.read_text(encoding="utf-8")


def test_capture_plan_contains_no_sensitive_placeholder_values() -> None:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id="runtime-smoke-run-006",
        environment="dev",
        generated_at="2026-06-08T00:00:00+00:00",
    )
    rendered = plan.to_json_str()

    assert "https://" not in rendered
    assert "dbc-" not in rendered
    assert "activation link:" not in rendered.lower()
    assert "token:" not in rendered.lower()
