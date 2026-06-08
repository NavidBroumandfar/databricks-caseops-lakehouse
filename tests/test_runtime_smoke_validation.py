from __future__ import annotations

import json
import uuid
from pathlib import Path

from src.pipelines.runtime_smoke_validation import (
    CHECK_ARTIFACTS_SANITIZED,
    CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH,
    CHECK_CAPTURE_PLAN_PROVIDED,
    CHECK_CAPTURE_PLAN_RUN_MATCHES,
    CHECK_DELIVERY_VALIDATION_VALIDATED,
    CHECK_SHARE_MANIFEST_PROVISIONED,
    validate_runtime_smoke_package,
    write_runtime_smoke_result,
)
from src.pipelines.runtime_smoke_plan import (
    ARTIFACT_DELIVERY_EVENT,
    ARTIFACT_DELIVERY_VALIDATION,
    ARTIFACT_RUNTIME_EVIDENCE,
    ARTIFACT_SHARE_MANIFEST,
    build_runtime_smoke_capture_plan,
    write_runtime_smoke_capture_plan,
)
from src.schemas.delivery_event import DeliveryEvent
from src.schemas.delivery_validation import (
    VALIDATION_SCHEMA_VERSION,
    VALIDATION_SCOPE_END_TO_END,
    VALIDATION_STATUS_NOT_PROVISIONED,
    VALIDATION_STATUS_VALIDATED,
    WORKSPACE_MODE_LOCAL_REPO_ONLY,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
    DeliveryValidationResult,
)
from src.schemas.runtime_smoke import (
    SMOKE_STATUS_FAILED,
    SMOKE_STATUS_INCOMPLETE,
    SMOKE_STATUS_READY,
)


PIPELINE_RUN_ID = "runtime-smoke-run-001"
DELIVERY_EVENT_ID = "11111111-1111-4111-8111-111111111111"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _delivery_event_dict(pipeline_run_id: str = PIPELINE_RUN_ID) -> dict:
    return DeliveryEvent(
        delivery_event_id=DELIVERY_EVENT_ID,
        pipeline_run_id=pipeline_run_id,
        batch_id=pipeline_run_id,
        generated_at="2026-06-08T00:00:00+00:00",
        eligible_record_count=3,
        exported_record_count=2,
        quarantined_record_count=1,
        contract_blocked_count=0,
        routing_labels=["regulatory_review"],
        bundle_artifact_path="output/reports/handoff_bundle_runtime-smoke-run-001.json",
    ).to_json_dict()


def _share_manifest(status: str = "provisioned") -> dict:
    return {
        "manifest_version": "v0.2.0",
        "share_name": "caseops_handoff",
        "shared_object_name": "gold.gold_ai_ready_assets",
        "status": status,
        "setup_sql": "CREATE SHARE IF NOT EXISTS caseops_handoff;",
        "c2_validation_queries": [
            {"name": "confirm_share_exists", "sql": "SHOW ALL IN SHARE caseops_handoff"},
        ],
    }


def _runtime_evidence(pipeline_run_id: str = PIPELINE_RUN_ID, note: str = "Sanitized.") -> dict:
    return {
        "evidence_version": "v0.1.0",
        "captured_at": "2026-06-08T00:00:00+00:00",
        "workspace_mode": "personal_databricks",
        "validation_scope": "end_to_end",
        "pipeline_run_id": pipeline_run_id,
        "delivery_event_id": DELIVERY_EVENT_ID,
        "share_name": "caseops_handoff",
        "shared_object_name": "gold.gold_ai_ready_assets",
        "manifest_status": "provisioned",
        "queries": [
            {
                "name": "confirm_share_exists",
                "executed": True,
                "passed": True,
                "row_count": 2,
                "observed_fields": ["database", "table"],
                "notes": "Shared objects visible.",
            },
            {
                "name": "query_export_ready_records",
                "executed": True,
                "passed": True,
                "row_count": 2,
                "observed_fields": ["document_id", "routing_label", "schema_version"],
                "notes": "Export-ready records visible.",
            },
            {
                "name": "query_delivery_events",
                "executed": True,
                "passed": True,
                "row_count": 1,
                "observed_fields": ["delivery_event_id", "batch_id"],
                "notes": "Delivery event row visible.",
            },
            {
                "name": "verify_routing_label_transparency",
                "executed": True,
                "passed": True,
                "row_count": 1,
                "observed_fields": ["routing_label"],
                "notes": "Routing labels visible.",
            },
        ],
        "runtime_assertions": {
            "share_exists": True,
            "gold_table_visible": True,
            "export_ready_records_visible": True,
            "delivery_event_row_visible": True,
            "routing_labels_visible": True,
            "schema_versions_visible": True,
        },
        "sanitization": {
            "workspace_url_removed": True,
            "activation_links_removed": True,
            "tokens_removed": True,
            "personal_identifiers_removed": True,
        },
        "notes": [note],
    }


def _delivery_validation_result(
    *,
    pipeline_run_id: str = PIPELINE_RUN_ID,
    status: str = VALIDATION_STATUS_VALIDATED,
    workspace_mode: str = WORKSPACE_MODE_PERSONAL_DATABRICKS,
    scope: str = VALIDATION_SCOPE_END_TO_END,
) -> dict:
    return DeliveryValidationResult(
        validation_run_id=str(uuid.uuid4()),
        pipeline_run_id=pipeline_run_id,
        delivery_event_id=DELIVERY_EVENT_ID,
        validated_at="2026-06-08T00:00:00+00:00",
        delivery_mechanism="delta_sharing",
        share_name="caseops_handoff",
        shared_object_name="gold.gold_ai_ready_assets",
        validation_scope=scope,
        validation_status=status,
        validation_reason="Validated with sanitized runtime evidence.",
        checks_passed=["runtime_evidence_queries_passed"],
        checks_failed=[],
        check_details=[],
        observations=[],
        artifacts_checked=[],
        queries_executed=["confirm_share_exists"],
        workspace_mode=workspace_mode,
        schema_version=VALIDATION_SCHEMA_VERSION,
    ).to_json_dict()


def _write_ready_package(tmp_path: Path) -> dict[str, Path]:
    return {
        "delivery_event": _write_json(tmp_path / "delivery_event.json", _delivery_event_dict()),
        "share_manifest": _write_json(tmp_path / "share_manifest.json", _share_manifest()),
        "runtime_evidence": _write_json(tmp_path / "runtime_evidence.json", _runtime_evidence()),
        "delivery_validation": _write_json(
            tmp_path / "delivery_validation.json",
            _delivery_validation_result(),
        ),
    }


def _write_ready_planned_package(tmp_path: Path) -> dict[str, Path]:
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        output_root=tmp_path / "output",
        generated_at="2026-06-08T00:00:00+00:00",
    )
    capture_plan_path, _ = write_runtime_smoke_capture_plan(plan, tmp_path / "output" / "validation")
    return {
        "capture_plan": capture_plan_path,
        "delivery_event": _write_json(
            Path(plan.artifact_path(ARTIFACT_DELIVERY_EVENT) or ""),
            _delivery_event_dict(),
        ),
        "share_manifest": _write_json(
            Path(plan.artifact_path(ARTIFACT_SHARE_MANIFEST) or ""),
            _share_manifest(),
        ),
        "runtime_evidence": _write_json(
            Path(plan.artifact_path(ARTIFACT_RUNTIME_EVIDENCE) or ""),
            _runtime_evidence(),
        ),
        "delivery_validation": _write_json(
            Path(plan.artifact_path(ARTIFACT_DELIVERY_VALIDATION) or ""),
            _delivery_validation_result(),
        ),
    }


def test_runtime_smoke_package_incomplete_without_capture_plan(tmp_path: Path) -> None:
    paths = _write_ready_package(tmp_path)

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
    )

    assert result.smoke_status == SMOKE_STATUS_INCOMPLETE
    assert CHECK_CAPTURE_PLAN_PROVIDED in result.checks_failed
    assert CHECK_DELIVERY_VALIDATION_VALIDATED in result.checks_passed


def test_runtime_smoke_package_ready_when_capture_plan_matches_package(tmp_path: Path) -> None:
    paths = _write_ready_planned_package(tmp_path)

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
        capture_plan_path=paths["capture_plan"],
    )

    assert result.smoke_status == SMOKE_STATUS_READY
    assert CHECK_CAPTURE_PLAN_RUN_MATCHES in result.checks_passed
    assert CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH in result.checks_passed


def test_runtime_smoke_package_incomplete_when_capture_plan_paths_drift(tmp_path: Path) -> None:
    paths = _write_ready_planned_package(tmp_path)
    drifted_delivery_event = _write_json(tmp_path / "drifted" / "delivery_event.json", _delivery_event_dict())

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=drifted_delivery_event,
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
        capture_plan_path=paths["capture_plan"],
    )

    assert result.smoke_status == SMOKE_STATUS_INCOMPLETE
    assert CHECK_CAPTURE_PLAN_ARTIFACT_PATHS_MATCH in result.checks_failed


def test_runtime_smoke_package_incomplete_when_manifest_is_not_provisioned(tmp_path: Path) -> None:
    paths = _write_ready_package(tmp_path)
    _write_json(paths["share_manifest"], _share_manifest(status="designed"))

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
    )

    assert result.smoke_status == SMOKE_STATUS_INCOMPLETE
    assert CHECK_SHARE_MANIFEST_PROVISIONED in result.checks_failed


def test_runtime_smoke_package_incomplete_when_delivery_result_is_local_only(tmp_path: Path) -> None:
    paths = _write_ready_package(tmp_path)
    _write_json(
        paths["delivery_validation"],
        _delivery_validation_result(
            status=VALIDATION_STATUS_NOT_PROVISIONED,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
            scope="producer_side_only",
        ),
    )

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
    )

    assert result.smoke_status == SMOKE_STATUS_INCOMPLETE
    assert CHECK_DELIVERY_VALIDATION_VALIDATED in result.checks_failed


def test_runtime_smoke_package_fails_when_artifact_contains_sensitive_text(tmp_path: Path) -> None:
    paths = _write_ready_package(tmp_path)
    raw = json.loads(paths["share_manifest"].read_text(encoding="utf-8"))
    raw["operator_note"] = "workspace url: https://dbc-example.cloud.databricks.com"
    _write_json(paths["share_manifest"], raw)

    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
    )

    assert result.smoke_status == SMOKE_STATUS_FAILED
    assert CHECK_ARTIFACTS_SANITIZED in result.checks_failed


def test_write_runtime_smoke_result_writes_json_and_text(tmp_path: Path) -> None:
    paths = _write_ready_planned_package(tmp_path)
    result = validate_runtime_smoke_package(
        pipeline_run_id=PIPELINE_RUN_ID,
        environment="dev",
        delivery_event_path=paths["delivery_event"],
        share_manifest_path=paths["share_manifest"],
        runtime_evidence_path=paths["runtime_evidence"],
        delivery_validation_result_path=paths["delivery_validation"],
        capture_plan_path=paths["capture_plan"],
    )

    json_path, text_path = write_runtime_smoke_result(result, tmp_path / "out")

    assert json_path.exists()
    assert text_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["smoke_status"] == SMOKE_STATUS_READY
    assert "Databricks Runtime Smoke Package Validation" in text_path.read_text(encoding="utf-8")
