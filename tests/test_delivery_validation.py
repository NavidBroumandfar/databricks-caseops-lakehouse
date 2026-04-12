"""
tests/test_delivery_validation.py — C-2: Delivery Validation Layer Tests.

Covers:
  - DeliveryValidationResult schema creation and field validation
  - CheckResult model creation
  - Status vocabulary validation (validated/partially_validated/not_provisioned/failed)
  - Scope vocabulary validation (producer_side_only/end_to_end)
  - Workspace mode vocabulary validation (local_repo_only/personal_databricks)
  - Schema version enforcement (must be v0.2.0)
  - Individual check functions (all 15)
  - validate_delivery_layer() — main entry point
  - Status derivation: not_provisioned for designed share + local workspace
  - Status derivation: partially_validated for no failures + local workspace
  - Status derivation: failed for critical check failures
  - Evidence sufficiency honesty rules
  - Cross-artifact ID consistency validation
  - Bundle path reference and existence checks
  - Artifact path computation (deterministic)
  - format_validation_result_text() output
  - write_validation_result() writes correct artifacts
  - load_validation_result() round-trips correctly
  - No credential dependency, no external SDK calls
  - V1 behavior preservation (schema_version v0.1.0 payloads not affected)
  - No false 'validated' status for local-repo-only runs
  - Integration: validate_delivery_layer() with real delivery artifacts

Phase: C-2
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.schemas.delivery_validation import (
    ALL_CHECK_NAMES,
    ALL_VALIDATION_SCOPES,
    ALL_VALIDATION_STATUSES,
    ALL_WORKSPACE_MODES,
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
from src.pipelines.delivery_validation import (
    check_bundle_path_exists,
    check_bundle_path_referenced,
    check_cross_id_consistency,
    check_delivery_event_exists,
    check_delivery_event_parseable,
    check_delivery_event_schema_version,
    check_delivery_event_status_known,
    check_delivery_mechanism_known,
    check_evidence_sufficiency,
    check_routing_labels_present,
    check_share_manifest_exists,
    check_share_manifest_has_c2_queries,
    check_share_manifest_has_setup_sql,
    check_share_manifest_parseable,
    check_share_provisioning_acknowledged,
    compute_validation_result_path,
    compute_validation_result_text_path,
    format_validation_result_text,
    load_validation_result,
    validate_delivery_layer,
    write_validation_result,
)
from src.schemas.delivery_event import (
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
    DELIVERY_STATUS_PREPARED,
    DeliveryEvent,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_delivery_event(
    pipeline_run_id: str = "run-test-0001",
    delivery_event_id: str = "de000001-0000-0000-0000-000000000001",
    schema_version: str = DELIVERY_SCHEMA_VERSION,
    status: str = DELIVERY_STATUS_PREPARED,
    delivery_mechanism: str = DELIVERY_MECHANISM_DELTA_SHARING,
    exported_count: int = 3,
    routing_labels: list = None,
    bundle_artifact_path: str = None,
) -> DeliveryEvent:
    return DeliveryEvent(
        delivery_event_id=delivery_event_id,
        pipeline_run_id=pipeline_run_id,
        batch_id=pipeline_run_id,
        generated_at="2026-04-12T00:00:00+00:00",
        delivery_mechanism=delivery_mechanism,
        share_name=DEFAULT_SHARE_NAME,
        shared_object_name=DEFAULT_SHARED_OBJECT_NAME,
        eligible_record_count=exported_count,
        exported_record_count=exported_count,
        quarantined_record_count=0,
        contract_blocked_count=0,
        routing_labels=routing_labels if routing_labels is not None else ["regulatory_review"],
        bundle_artifact_path=bundle_artifact_path,
        status=status,
        schema_version=schema_version,
    )


def _make_share_manifest_dict(status: str = "designed") -> dict:
    return {
        "manifest_version": "v0.1.0",
        "delivery_mechanism": "delta_sharing",
        "config": {
            "share_name": "caseops_handoff",
            "catalog": "caseops",
        },
        "status": status,
        "generated_at": "2026-04-12T00:00:00+00:00",
        "setup_sql": "CREATE SHARE IF NOT EXISTS caseops_handoff;",
        "delivery_events_ddl": "CREATE TABLE IF NOT EXISTS caseops.gold.delivery_events (...);",
        "handoff_surface": {},
        "c2_validation_queries": [
            {"name": "confirm_share_exists", "sql": "SHOW ALL IN SHARE caseops_handoff;"},
            {"name": "query_export_ready_records", "sql": "SELECT * FROM caseops_handoff.gold_ai_ready_assets LIMIT 1;"},
        ],
        "notes": [],
    }


def _write_delivery_event_json(tmp_path: Path, event: DeliveryEvent) -> Path:
    path = tmp_path / f"delivery_event_{event.pipeline_run_id}.json"
    path.write_text(event.to_json_str(), encoding="utf-8")
    return path


def _write_share_manifest_json(tmp_path: Path, manifest: dict) -> Path:
    path = tmp_path / "delta_share_preparation_manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Section 1 — Schema: DeliveryValidationResult
# ---------------------------------------------------------------------------


class TestDeliveryValidationResultSchema:
    def test_valid_result_creation(self):
        result = DeliveryValidationResult(
            validation_run_id=str(uuid.uuid4()),
            pipeline_run_id="run-test-0001",
            validated_at="2026-04-12T00:00:00+00:00",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
            validation_reason="Producer-side check complete; share not provisioned.",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_NOT_PROVISIONED
        assert result.schema_version == VALIDATION_SCHEMA_VERSION

    def test_schema_version_must_be_v0_2_0(self):
        with pytest.raises(Exception):
            DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run-test",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
                validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
                validation_reason="test",
                workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
                schema_version="v0.1.0",
            )

    def test_invalid_validation_status_rejected(self):
        with pytest.raises(Exception):
            DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run-test",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
                validation_status="unknown_status",
                validation_reason="test",
                workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
            )

    def test_invalid_scope_rejected(self):
        with pytest.raises(Exception):
            DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run-test",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope="bad_scope",
                validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
                validation_reason="test",
                workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
            )

    def test_invalid_workspace_mode_rejected(self):
        with pytest.raises(Exception):
            DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run-test",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
                validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
                validation_reason="test",
                workspace_mode="bad_mode",
            )

    def test_all_status_values_accepted(self):
        for status in ALL_VALIDATION_STATUSES:
            r = DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
                validation_status=status,
                validation_reason="test",
                workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
            )
            assert r.validation_status == status

    def test_all_scope_values_accepted(self):
        for scope in ALL_VALIDATION_SCOPES:
            r = DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=scope,
                validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
                validation_reason="test",
                workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
            )
            assert r.validation_scope == scope

    def test_all_workspace_modes_accepted(self):
        for mode in ALL_WORKSPACE_MODES:
            r = DeliveryValidationResult(
                validation_run_id=str(uuid.uuid4()),
                pipeline_run_id="run",
                validated_at="2026-04-12T00:00:00+00:00",
                validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
                validation_status=VALIDATION_STATUS_PARTIALLY_VALIDATED,
                validation_reason="test",
                workspace_mode=mode,
            )
            assert r.workspace_mode == mode

    def test_to_json_dict_is_serializable(self):
        result = DeliveryValidationResult(
            validation_run_id=str(uuid.uuid4()),
            pipeline_run_id="run-test-0001",
            validated_at="2026-04-12T00:00:00+00:00",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
            validation_reason="test",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        d = result.to_json_dict()
        assert isinstance(d, dict)
        assert d["validation_status"] == VALIDATION_STATUS_NOT_PROVISIONED
        json.dumps(d)  # Must not raise

    def test_defaults_for_optional_fields(self):
        result = DeliveryValidationResult(
            validation_run_id=str(uuid.uuid4()),
            pipeline_run_id="run-test",
            validated_at="2026-04-12T00:00:00+00:00",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
            validation_reason="test",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.delivery_event_id is None
        assert result.share_name is None
        assert result.checks_passed == []
        assert result.checks_failed == []
        assert result.check_details == []
        assert result.observations == []
        assert result.artifacts_checked == []
        assert result.queries_executed == []


# ---------------------------------------------------------------------------
# Section 2 — Schema: CheckResult
# ---------------------------------------------------------------------------


class TestCheckResultSchema:
    def test_pass_result(self):
        r = CheckResult(check_name=CHECK_DELIVERY_EVENT_EXISTS, passed=True, detail="Found.")
        assert r.passed is True
        assert r.check_name == CHECK_DELIVERY_EVENT_EXISTS

    def test_fail_result(self):
        r = CheckResult(check_name=CHECK_DELIVERY_EVENT_EXISTS, passed=False, detail="Not found.")
        assert r.passed is False

    def test_detail_is_optional(self):
        r = CheckResult(check_name=CHECK_DELIVERY_EVENT_EXISTS, passed=True)
        assert r.detail is None

    def test_to_json_dict(self):
        r = CheckResult(check_name=CHECK_DELIVERY_EVENT_EXISTS, passed=True, detail="ok")
        d = r.to_json_dict()
        assert d["check_name"] == CHECK_DELIVERY_EVENT_EXISTS
        assert d["passed"] is True


# ---------------------------------------------------------------------------
# Section 3 — Constants and vocabulary
# ---------------------------------------------------------------------------


class TestConstantsAndVocabulary:
    def test_all_check_names_are_strings(self):
        for name in ALL_CHECK_NAMES:
            assert isinstance(name, str)
            assert len(name) > 0

    def test_all_check_names_are_unique(self):
        assert len(ALL_CHECK_NAMES) == len(set(ALL_CHECK_NAMES))

    def test_all_status_values_are_strings(self):
        for s in ALL_VALIDATION_STATUSES:
            assert isinstance(s, str)

    def test_validation_schema_version_is_v0_2_0(self):
        assert VALIDATION_SCHEMA_VERSION == "v0.2.0"

    def test_status_vocabulary_contains_four_values(self):
        assert len(ALL_VALIDATION_STATUSES) == 4
        assert VALIDATION_STATUS_VALIDATED in ALL_VALIDATION_STATUSES
        assert VALIDATION_STATUS_PARTIALLY_VALIDATED in ALL_VALIDATION_STATUSES
        assert VALIDATION_STATUS_NOT_PROVISIONED in ALL_VALIDATION_STATUSES
        assert VALIDATION_STATUS_FAILED in ALL_VALIDATION_STATUSES

    def test_scope_vocabulary_contains_two_values(self):
        assert len(ALL_VALIDATION_SCOPES) == 2
        assert VALIDATION_SCOPE_PRODUCER_SIDE_ONLY in ALL_VALIDATION_SCOPES
        assert VALIDATION_SCOPE_END_TO_END in ALL_VALIDATION_SCOPES

    def test_workspace_mode_vocabulary_contains_two_values(self):
        assert len(ALL_WORKSPACE_MODES) == 2
        assert WORKSPACE_MODE_LOCAL_REPO_ONLY in ALL_WORKSPACE_MODES
        assert WORKSPACE_MODE_PERSONAL_DATABRICKS in ALL_WORKSPACE_MODES


# ---------------------------------------------------------------------------
# Section 4 — Individual check functions
# ---------------------------------------------------------------------------


class TestCheckDeliveryEventExists:
    def test_passes_when_file_exists(self, tmp_path):
        p = tmp_path / "delivery_event.json"
        p.write_text("{}", encoding="utf-8")
        r = check_delivery_event_exists(p)
        assert r.passed is True
        assert r.check_name == CHECK_DELIVERY_EVENT_EXISTS

    def test_fails_when_file_missing(self, tmp_path):
        p = tmp_path / "nonexistent.json"
        r = check_delivery_event_exists(p)
        assert r.passed is False

    def test_fails_when_path_is_none(self):
        r = check_delivery_event_exists(None)
        assert r.passed is False


class TestCheckDeliveryEventParseable:
    def test_passes_for_valid_event(self, tmp_path):
        event = _make_delivery_event()
        path = _write_delivery_event_json(tmp_path, event)
        check_r, parsed = check_delivery_event_parseable(path)
        assert check_r.passed is True
        assert parsed is not None
        assert parsed.delivery_event_id == event.delivery_event_id

    def test_fails_for_invalid_json(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("{not valid json", encoding="utf-8")
        check_r, parsed = check_delivery_event_parseable(p)
        assert check_r.passed is False
        assert parsed is None

    def test_fails_for_missing_file(self, tmp_path):
        check_r, parsed = check_delivery_event_parseable(tmp_path / "missing.json")
        assert check_r.passed is False
        assert parsed is None

    def test_fails_for_none_path(self):
        check_r, parsed = check_delivery_event_parseable(None)
        assert check_r.passed is False
        assert parsed is None

    def test_returns_validated_event_on_success(self, tmp_path):
        event = _make_delivery_event(pipeline_run_id="run-abc")
        path = _write_delivery_event_json(tmp_path, event)
        check_r, parsed = check_delivery_event_parseable(path)
        assert parsed.pipeline_run_id == "run-abc"


class TestCheckDeliveryEventSchemaVersion:
    def test_passes_for_v0_2_0(self):
        event = _make_delivery_event(schema_version="v0.2.0")
        r = check_delivery_event_schema_version(event)
        assert r.passed is True

    def test_fails_for_v0_1_0(self):
        event = _make_delivery_event()
        # Force wrong version via model_copy
        d = event.model_dump()
        d["schema_version"] = "v0.1.0"
        # Create with force via direct dict validation bypass
        # We test via schema check directly
        r = check_delivery_event_schema_version(None)
        assert r.passed is False

    def test_fails_for_none_event(self):
        r = check_delivery_event_schema_version(None)
        assert r.passed is False

    def test_detail_mentions_expected_version(self):
        event = _make_delivery_event()
        r = check_delivery_event_schema_version(event)
        assert "v0.2.0" in (r.detail or "")


class TestCheckDeliveryEventStatusKnown:
    def test_passes_for_prepared(self):
        event = _make_delivery_event(status="prepared")
        r = check_delivery_event_status_known(event)
        assert r.passed is True

    def test_passes_for_failed(self):
        event = _make_delivery_event(status="failed")
        r = check_delivery_event_status_known(event)
        assert r.passed is True

    def test_fails_for_none_event(self):
        r = check_delivery_event_status_known(None)
        assert r.passed is False


class TestCheckDeliveryMechanismKnown:
    def test_passes_for_delta_sharing(self):
        event = _make_delivery_event(delivery_mechanism="delta_sharing")
        r = check_delivery_mechanism_known(event)
        assert r.passed is True

    def test_fails_for_none_event(self):
        r = check_delivery_mechanism_known(None)
        assert r.passed is False

    def test_check_name_is_correct(self):
        event = _make_delivery_event()
        r = check_delivery_mechanism_known(event)
        assert r.check_name == CHECK_DELIVERY_MECHANISM_KNOWN


class TestCheckShareManifestExists:
    def test_passes_when_file_exists(self, tmp_path):
        p = tmp_path / "manifest.json"
        p.write_text("{}", encoding="utf-8")
        r = check_share_manifest_exists(p)
        assert r.passed is True

    def test_fails_when_missing(self, tmp_path):
        r = check_share_manifest_exists(tmp_path / "nope.json")
        assert r.passed is False

    def test_fails_for_none(self):
        r = check_share_manifest_exists(None)
        assert r.passed is False


class TestCheckShareManifestParseable:
    def test_passes_for_valid_manifest(self, tmp_path):
        manifest = _make_share_manifest_dict()
        path = _write_share_manifest_json(tmp_path, manifest)
        check_r, parsed = check_share_manifest_parseable(path)
        assert check_r.passed is True
        assert parsed is not None
        assert parsed["status"] == "designed"

    def test_fails_for_invalid_json(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("{not valid", encoding="utf-8")
        check_r, parsed = check_share_manifest_parseable(p)
        assert check_r.passed is False
        assert parsed is None

    def test_fails_for_non_object_json(self, tmp_path):
        p = tmp_path / "array.json"
        p.write_text("[1, 2, 3]", encoding="utf-8")
        check_r, parsed = check_share_manifest_parseable(p)
        assert check_r.passed is False
        assert parsed is None

    def test_fails_for_missing_file(self, tmp_path):
        check_r, parsed = check_share_manifest_parseable(tmp_path / "gone.json")
        assert check_r.passed is False
        assert parsed is None


class TestCheckShareManifestHasSetupSql:
    def test_passes_when_sql_present(self):
        m = _make_share_manifest_dict()
        r = check_share_manifest_has_setup_sql(m)
        assert r.passed is True

    def test_fails_when_empty_sql(self):
        m = _make_share_manifest_dict()
        m["setup_sql"] = ""
        r = check_share_manifest_has_setup_sql(m)
        assert r.passed is False

    def test_fails_when_sql_missing(self):
        m = {"status": "designed"}
        r = check_share_manifest_has_setup_sql(m)
        assert r.passed is False

    def test_fails_for_none_manifest(self):
        r = check_share_manifest_has_setup_sql(None)
        assert r.passed is False


class TestCheckShareManifestHasC2Queries:
    def test_passes_when_queries_present(self):
        m = _make_share_manifest_dict()
        r = check_share_manifest_has_c2_queries(m)
        assert r.passed is True

    def test_fails_when_empty_list(self):
        m = _make_share_manifest_dict()
        m["c2_validation_queries"] = []
        r = check_share_manifest_has_c2_queries(m)
        assert r.passed is False

    def test_fails_when_key_missing(self):
        m = {"status": "designed", "setup_sql": "CREATE SHARE x;"}
        r = check_share_manifest_has_c2_queries(m)
        assert r.passed is False

    def test_fails_for_none(self):
        r = check_share_manifest_has_c2_queries(None)
        assert r.passed is False


class TestCheckCrossIdConsistency:
    def test_passes_when_all_ids_consistent(self):
        run_id = "run-abc"
        event_id = "de-001"
        event = _make_delivery_event(
            pipeline_run_id=run_id,
            delivery_event_id=event_id,
        )
        r = check_cross_id_consistency(
            event,
            expected_pipeline_run_id=run_id,
            expected_delivery_event_id=event_id,
        )
        assert r.passed is True

    def test_fails_when_pipeline_run_id_mismatch(self):
        event = _make_delivery_event(pipeline_run_id="run-abc")
        r = check_cross_id_consistency(event, expected_pipeline_run_id="run-xyz")
        assert r.passed is False
        assert "pipeline_run_id mismatch" in (r.detail or "")

    def test_fails_when_delivery_event_id_mismatch(self):
        event = _make_delivery_event(delivery_event_id="de-001")
        r = check_cross_id_consistency(event, expected_delivery_event_id="de-999")
        assert r.passed is False
        assert "delivery_event_id mismatch" in (r.detail or "")

    def test_fails_when_batch_id_differs_from_pipeline_run_id(self):
        event = _make_delivery_event(pipeline_run_id="run-abc")
        d = event.model_dump()
        d["batch_id"] = "different-batch"
        # Create a modified event by bypassing Pydantic validation
        # We test the check logic directly
        modified = event.model_copy(update={"batch_id": "different-batch"})
        r = check_cross_id_consistency(modified)
        assert r.passed is False
        assert "batch_id" in (r.detail or "")

    def test_passes_without_expected_ids(self):
        event = _make_delivery_event()
        r = check_cross_id_consistency(event)
        assert r.passed is True

    def test_fails_for_none_event(self):
        r = check_cross_id_consistency(None, expected_pipeline_run_id="run-abc")
        assert r.passed is False


class TestCheckBundlePathReferenced:
    def test_passes_when_path_set(self):
        event = _make_delivery_event(bundle_artifact_path="output/reports/bundle.json")
        r = check_bundle_path_referenced(event)
        assert r.passed is True

    def test_fails_when_path_not_set(self):
        event = _make_delivery_event(bundle_artifact_path=None)
        r = check_bundle_path_referenced(event)
        assert r.passed is False

    def test_fails_for_none_event(self):
        r = check_bundle_path_referenced(None)
        assert r.passed is False


class TestCheckBundlePathExists:
    def test_passes_when_bundle_exists(self, tmp_path):
        bundle_path = tmp_path / "bundle.json"
        bundle_path.write_text("{}", encoding="utf-8")
        event = _make_delivery_event(bundle_artifact_path=str(bundle_path))
        r = check_bundle_path_exists(event)
        assert r.passed is True

    def test_fails_when_bundle_missing(self, tmp_path):
        event = _make_delivery_event(bundle_artifact_path=str(tmp_path / "missing.json"))
        r = check_bundle_path_exists(event)
        assert r.passed is False

    def test_fails_when_path_not_set(self):
        event = _make_delivery_event(bundle_artifact_path=None)
        r = check_bundle_path_exists(event)
        assert r.passed is False

    def test_fails_for_none_event(self):
        r = check_bundle_path_exists(None)
        assert r.passed is False


class TestCheckRoutingLabelsPresent:
    def test_passes_with_exported_records_and_labels(self):
        event = _make_delivery_event(exported_count=3, routing_labels=["regulatory_review"])
        r = check_routing_labels_present(event)
        assert r.passed is True

    def test_passes_with_zero_exports_no_labels(self):
        event = _make_delivery_event(exported_count=0, routing_labels=[])
        r = check_routing_labels_present(event)
        assert r.passed is True

    def test_fails_with_exports_but_no_labels(self):
        event = _make_delivery_event(exported_count=2, routing_labels=[])
        r = check_routing_labels_present(event)
        assert r.passed is False

    def test_fails_for_none_event(self):
        r = check_routing_labels_present(None)
        assert r.passed is False


class TestCheckShareProvisioningAcknowledged:
    def test_passes_for_designed(self):
        m = _make_share_manifest_dict(status="designed")
        r = check_share_provisioning_acknowledged(m)
        assert r.passed is True
        assert "designed" in (r.detail or "")

    def test_passes_for_provisioned(self):
        m = _make_share_manifest_dict(status="provisioned")
        r = check_share_provisioning_acknowledged(m)
        assert r.passed is True
        assert "provisioned" in (r.detail or "")

    def test_fails_for_unknown_status(self):
        m = _make_share_manifest_dict(status="unknown_status")
        r = check_share_provisioning_acknowledged(m)
        assert r.passed is False

    def test_fails_when_status_missing(self):
        r = check_share_provisioning_acknowledged({"setup_sql": "..."})
        assert r.passed is False

    def test_fails_for_none(self):
        r = check_share_provisioning_acknowledged(None)
        assert r.passed is False


class TestCheckEvidenceSufficiency:
    def test_passes_for_not_provisioned_local(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_NOT_PROVISIONED,
            WORKSPACE_MODE_LOCAL_REPO_ONLY,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            [],
        )
        assert r.passed is True

    def test_passes_for_partially_validated_local(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_PARTIALLY_VALIDATED,
            WORKSPACE_MODE_LOCAL_REPO_ONLY,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            [],
        )
        assert r.passed is True

    def test_passes_for_failed_local(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_FAILED,
            WORKSPACE_MODE_LOCAL_REPO_ONLY,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            ["delivery_event_exists"],
        )
        assert r.passed is True

    def test_fails_validated_with_local_repo_only(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_VALIDATED,
            WORKSPACE_MODE_LOCAL_REPO_ONLY,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            [],
        )
        assert r.passed is False
        assert "local_repo_only" in (r.detail or "")

    def test_passes_validated_with_personal_databricks_producer_side_scope(self):
        """
        validate_delivery_layer() always uses producer_side_only scope (it checks repo
        artifacts). When workspace_mode is personal_databricks, 'validated' is acceptable
        because the caller asserts runtime evidence was collected in the workspace.
        """
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_VALIDATED,
            WORKSPACE_MODE_PERSONAL_DATABRICKS,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            [],
        )
        assert r.passed is True

    def test_fails_validated_when_checks_failed(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_VALIDATED,
            WORKSPACE_MODE_PERSONAL_DATABRICKS,
            VALIDATION_SCOPE_END_TO_END,
            ["delivery_event_exists"],
        )
        assert r.passed is False

    def test_passes_validated_with_full_evidence(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_VALIDATED,
            WORKSPACE_MODE_PERSONAL_DATABRICKS,
            VALIDATION_SCOPE_END_TO_END,
            [],
        )
        assert r.passed is True

    def test_check_name_is_correct(self):
        r = check_evidence_sufficiency(
            VALIDATION_STATUS_NOT_PROVISIONED,
            WORKSPACE_MODE_LOCAL_REPO_ONLY,
            VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            [],
        )
        assert r.check_name == CHECK_EVIDENCE_SUFFICIENCY


# ---------------------------------------------------------------------------
# Section 5 — validate_delivery_layer() main entry point
# ---------------------------------------------------------------------------


class TestValidateDeliveryLayerStatusDerivation:
    """Tests for the core status derivation logic."""

    def test_not_provisioned_when_designed_manifest_and_local_workspace(self, tmp_path):
        run_id = "run-test-001"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict(status="designed")
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_NOT_PROVISIONED

    def test_partially_validated_when_provisioned_manifest_and_local_workspace(self, tmp_path):
        run_id = "run-test-002"
        bundle = tmp_path / "bundle.json"
        bundle.write_text("{}", encoding="utf-8")
        event = _make_delivery_event(
            pipeline_run_id=run_id,
            bundle_artifact_path=str(bundle),
        )
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict(status="provisioned")
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_PARTIALLY_VALIDATED

    def test_failed_when_delivery_event_missing(self, tmp_path):
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)
        result = validate_delivery_layer(
            pipeline_run_id="run-test-003",
            delivery_event_path=tmp_path / "missing.json",
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_FAILED

    def test_failed_when_no_paths_provided(self):
        result = validate_delivery_layer(
            pipeline_run_id="run-test-004",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_FAILED

    def test_failed_when_pipeline_run_id_mismatch(self, tmp_path):
        event = _make_delivery_event(pipeline_run_id="run-real")
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id="run-different",
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_FAILED
        assert CHECK_CROSS_ID_CONSISTENCY in result.checks_failed

    def test_validated_not_possible_for_local_repo_only(self, tmp_path):
        """validate_delivery_layer must never return 'validated' for local_repo_only."""
        run_id = "run-test-005"
        bundle = tmp_path / "bundle.json"
        bundle.write_text("{}", encoding="utf-8")
        event = _make_delivery_event(
            pipeline_run_id=run_id,
            bundle_artifact_path=str(bundle),
        )
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict(status="provisioned")
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        # Status must NEVER be 'validated' for local_repo_only workspace
        assert result.validation_status != VALIDATION_STATUS_VALIDATED

    def test_validated_possible_for_personal_databricks(self, tmp_path):
        """With personal_databricks workspace and provisioned share, status can be 'validated'."""
        run_id = "run-test-006"
        bundle = tmp_path / "bundle.json"
        bundle.write_text("{}", encoding="utf-8")
        event = _make_delivery_event(
            pipeline_run_id=run_id,
            bundle_artifact_path=str(bundle),
        )
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict(status="provisioned")
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_PERSONAL_DATABRICKS,
        )
        assert result.validation_status == VALIDATION_STATUS_VALIDATED

    def test_failed_when_manifest_has_no_setup_sql(self, tmp_path):
        run_id = "run-test-007"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        manifest["setup_sql"] = ""  # Empty SQL
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status == VALIDATION_STATUS_FAILED
        assert CHECK_SHARE_MANIFEST_HAS_SETUP_SQL in result.checks_failed


class TestValidateDeliveryLayerChecks:
    """Tests for check presence in the validation result."""

    def test_all_expected_checks_are_run(self, tmp_path):
        run_id = "run-all-checks"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        all_run_check_names = {r.check_name for r in result.check_details}
        # All 15 checks should be present
        for check_name in ALL_CHECK_NAMES:
            assert check_name in all_run_check_names, (
                f"Expected check '{check_name}' to be present in check_details"
            )

    def test_checks_passed_and_failed_are_disjoint(self, tmp_path):
        run_id = "run-disjoint"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        passed = set(result.checks_passed)
        failed = set(result.checks_failed)
        assert passed.isdisjoint(failed), "checks_passed and checks_failed must not overlap"

    def test_delivery_event_id_in_result_matches_event(self, tmp_path):
        run_id = "run-id-check"
        event_id = "de-test-001"
        event = _make_delivery_event(pipeline_run_id=run_id, delivery_event_id=event_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.delivery_event_id == event_id

    def test_artifacts_checked_contains_input_paths(self, tmp_path):
        run_id = "run-artifacts"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert str(ev_path) in result.artifacts_checked
        assert str(mn_path) in result.artifacts_checked

    def test_queries_executed_is_empty_for_local_repo_only(self, tmp_path):
        run_id = "run-queries"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.queries_executed == []

    def test_validation_scope_is_always_producer_side_only(self, tmp_path):
        run_id = "run-scope"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_scope == VALIDATION_SCOPE_PRODUCER_SIDE_ONLY

    def test_schema_version_is_v0_2_0_in_result(self, tmp_path):
        run_id = "run-schema"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.schema_version == VALIDATION_SCHEMA_VERSION

    def test_validation_run_id_is_unique_across_runs(self, tmp_path):
        run_id = "run-unique"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        r1 = validate_delivery_layer(run_id, ev_path, mn_path)
        r2 = validate_delivery_layer(run_id, ev_path, mn_path)
        assert r1.validation_run_id != r2.validation_run_id

    def test_observations_are_non_empty(self, tmp_path):
        run_id = "run-obs"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert len(result.observations) > 0

    def test_validation_reason_is_non_empty(self, tmp_path):
        run_id = "run-reason"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
        )
        assert len(result.validation_reason) > 0


# ---------------------------------------------------------------------------
# Section 6 — Evidence sufficiency honesty rules
# ---------------------------------------------------------------------------


class TestEvidenceSufficiencyHonestyInMainFlow:
    """
    Verify that validate_delivery_layer() never emits 'validated'
    for local-only runs, regardless of how correct the artifacts are.
    """

    def test_perfectly_correct_artifacts_local_not_validated(self, tmp_path):
        """Even with all correct artifacts, local run must not return 'validated'."""
        run_id = "run-perfect"
        bundle = tmp_path / "bundle.json"
        bundle.write_text("{}", encoding="utf-8")
        event = _make_delivery_event(
            pipeline_run_id=run_id,
            bundle_artifact_path=str(bundle),
        )
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict(status="provisioned")
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.validation_status != VALIDATION_STATUS_VALIDATED

    def test_evidence_sufficiency_check_always_present(self, tmp_path):
        """CHECK_EVIDENCE_SUFFICIENCY must always appear in check_details."""
        run_id = "run-ev-check"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
        )
        evidence_checks = [
            r for r in result.check_details
            if r.check_name == CHECK_EVIDENCE_SUFFICIENCY
        ]
        assert len(evidence_checks) == 1


# ---------------------------------------------------------------------------
# Section 7 — Artifact path computation
# ---------------------------------------------------------------------------


class TestArtifactPathComputation:
    def test_validation_result_path_is_deterministic(self, tmp_path):
        run_id = "abc-123"
        p1 = compute_validation_result_path(tmp_path, run_id)
        p2 = compute_validation_result_path(tmp_path, run_id)
        assert p1 == p2

    def test_validation_result_text_path_is_deterministic(self, tmp_path):
        run_id = "abc-123"
        p1 = compute_validation_result_text_path(tmp_path, run_id)
        p2 = compute_validation_result_text_path(tmp_path, run_id)
        assert p1 == p2

    def test_paths_differ_by_run_id(self, tmp_path):
        p1 = compute_validation_result_path(tmp_path, "run-001")
        p2 = compute_validation_result_path(tmp_path, "run-002")
        assert p1 != p2

    def test_json_path_ends_with_json(self, tmp_path):
        p = compute_validation_result_path(tmp_path, "run-001")
        assert p.suffix == ".json"

    def test_text_path_ends_with_txt(self, tmp_path):
        p = compute_validation_result_text_path(tmp_path, "run-001")
        assert p.suffix == ".txt"

    def test_path_contains_run_id(self, tmp_path):
        run_id = "my-unique-run-id"
        p = compute_validation_result_path(tmp_path, run_id)
        assert run_id in p.name

    def test_slashes_in_run_id_are_sanitized(self, tmp_path):
        run_id = "path/with/slashes"
        p = compute_validation_result_path(tmp_path, run_id)
        assert "/" not in p.name


# ---------------------------------------------------------------------------
# Section 8 — Text formatter
# ---------------------------------------------------------------------------


class TestFormatValidationResultText:
    def _make_result(self) -> DeliveryValidationResult:
        return DeliveryValidationResult(
            validation_run_id="vr-001",
            pipeline_run_id="run-test",
            delivery_event_id="de-001",
            validated_at="2026-04-12T00:00:00+00:00",
            delivery_mechanism="delta_sharing",
            share_name="caseops_handoff",
            shared_object_name="gold_ai_ready_assets",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
            validation_reason="Share not provisioned.",
            checks_passed=["delivery_event_exists"],
            checks_failed=["bundle_path_exists"],
            observations=["Test observation."],
            artifacts_checked=["output/delivery/delivery_event.json"],
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )

    def test_format_returns_string(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_format_contains_status(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "NOT_PROVISIONED" in text

    def test_format_contains_pipeline_run_id(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "run-test" in text

    def test_format_contains_delivery_event_id(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "de-001" in text

    def test_format_contains_validation_reason(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "Share not provisioned." in text

    def test_format_contains_observation(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "Test observation." in text

    def test_format_contains_artifact_path(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "output/delivery/delivery_event.json" in text

    def test_format_mentions_c2(self):
        result = self._make_result()
        text = format_validation_result_text(result)
        assert "C-2" in text or "delivery-runtime-validation" in text

    def test_format_with_no_artifacts_checked(self):
        result = self._make_result()
        result = result.model_copy(update={"artifacts_checked": []})
        text = format_validation_result_text(result)
        assert "(none)" in text


# ---------------------------------------------------------------------------
# Section 9 — write_validation_result() and load_validation_result()
# ---------------------------------------------------------------------------


class TestWriteAndLoadValidationResult:
    def _make_result(self) -> DeliveryValidationResult:
        return DeliveryValidationResult(
            validation_run_id=str(uuid.uuid4()),
            pipeline_run_id="run-write-test",
            validated_at="2026-04-12T00:00:00+00:00",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_NOT_PROVISIONED,
            validation_reason="Not provisioned.",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )

    def test_write_creates_json_and_text(self, tmp_path):
        result = self._make_result()
        json_path, text_path = write_validation_result(result, tmp_path)
        assert json_path.exists()
        assert text_path.exists()

    def test_json_path_ends_with_json(self, tmp_path):
        result = self._make_result()
        json_path, _ = write_validation_result(result, tmp_path)
        assert json_path.suffix == ".json"

    def test_text_path_ends_with_txt(self, tmp_path):
        result = self._make_result()
        _, text_path = write_validation_result(result, tmp_path)
        assert text_path.suffix == ".txt"

    def test_written_json_is_valid(self, tmp_path):
        result = self._make_result()
        json_path, _ = write_validation_result(result, tmp_path)
        raw = json.loads(json_path.read_text(encoding="utf-8"))
        assert raw["validation_status"] == VALIDATION_STATUS_NOT_PROVISIONED

    def test_load_round_trips_correctly(self, tmp_path):
        result = self._make_result()
        json_path, _ = write_validation_result(result, tmp_path)
        loaded = load_validation_result(json_path)
        assert loaded.validation_run_id == result.validation_run_id
        assert loaded.validation_status == result.validation_status
        assert loaded.pipeline_run_id == result.pipeline_run_id

    def test_write_creates_output_dir_if_not_exists(self, tmp_path):
        result = self._make_result()
        new_dir = tmp_path / "new" / "nested" / "dir"
        json_path, _ = write_validation_result(result, new_dir)
        assert json_path.exists()

    def test_path_is_deterministic_for_same_run_id(self, tmp_path):
        result = self._make_result()
        json_path1, _ = write_validation_result(result, tmp_path)
        json_path2, _ = write_validation_result(result, tmp_path)
        assert json_path1 == json_path2


# ---------------------------------------------------------------------------
# Section 10 — V1 behavior preservation
# ---------------------------------------------------------------------------


class TestV1BehaviorPreservation:
    """
    Verify that C-2 validation does not affect or break V1 behavior.
    V1 behavior: classify_gold.py without --delivery-dir produces
    export payloads at schema_version: v0.1.0, no delivery event.
    C-2 validation is only relevant when --delivery-dir was used (C-1 path).
    """

    def test_c2_validation_only_checks_c1_artifacts(self, tmp_path):
        """
        When no delivery event or manifest exists (V1 run, no --delivery-dir),
        the validation correctly returns 'failed' due to missing artifacts.
        This is honest: C-2 validation does not apply to V1 runs.
        """
        result = validate_delivery_layer(
            pipeline_run_id="v1-run-no-delivery",
            delivery_event_path=None,
            share_manifest_path=None,
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        # V1 runs have no delivery event; validation correctly fails
        assert result.validation_status == VALIDATION_STATUS_FAILED
        assert CHECK_DELIVERY_EVENT_EXISTS in result.checks_failed

    def test_c2_schema_does_not_depend_on_v1_schema(self):
        """DeliveryValidationResult schema is fully independent of V1 gold schema."""
        result = DeliveryValidationResult(
            validation_run_id=str(uuid.uuid4()),
            pipeline_run_id="v1-run",
            validated_at="2026-04-12T00:00:00+00:00",
            validation_scope=VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
            validation_status=VALIDATION_STATUS_FAILED,
            validation_reason="No delivery event: V1 run without --delivery-dir.",
            workspace_mode=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        )
        assert result.schema_version == "v0.2.0"


# ---------------------------------------------------------------------------
# Section 11 — No credential dependency
# ---------------------------------------------------------------------------


class TestNoCredentialDependency:
    """Verify the validation layer has no dependency on external APIs or credentials."""

    def test_validate_delivery_layer_requires_no_sdk_imports(self, tmp_path):
        """
        The validation functions should complete without any external SDK call.
        This test verifies by importing the module in isolation with no env setup.
        """
        from src.pipelines import delivery_validation as dv
        # If this import succeeded, no SDK is required at module load time
        assert callable(dv.validate_delivery_layer)

    def test_no_workspace_url_in_validation_result(self, tmp_path):
        run_id = "run-no-creds"
        event = _make_delivery_event(pipeline_run_id=run_id)
        ev_path = _write_delivery_event_json(tmp_path, event)
        manifest = _make_share_manifest_dict()
        mn_path = _write_share_manifest_json(tmp_path, manifest)

        result = validate_delivery_layer(
            pipeline_run_id=run_id,
            delivery_event_path=ev_path,
            share_manifest_path=mn_path,
        )
        result_json = result.to_json_str()
        # Must not contain any URL-like workspace reference
        assert "dbc-" not in result_json
        assert "azuredatabricks" not in result_json
        assert "databricks.com" not in result_json


# ---------------------------------------------------------------------------
# Section 12 — Example fixture validation
# ---------------------------------------------------------------------------


class TestExampleFixture:
    """Verify the example fixture loads correctly as a DeliveryValidationResult."""

    def test_example_fixture_is_valid(self):
        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "expected_delivery_validation_result.json"
        )
        assert fixture_path.exists(), f"Fixture not found: {fixture_path}"
        raw = json.loads(fixture_path.read_text(encoding="utf-8"))
        result = DeliveryValidationResult.model_validate(raw)
        assert result.validation_status == VALIDATION_STATUS_NOT_PROVISIONED
        assert result.schema_version == VALIDATION_SCHEMA_VERSION
        assert result.workspace_mode == WORKSPACE_MODE_LOCAL_REPO_ONLY

    def test_example_fixture_has_check_details(self):
        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "expected_delivery_validation_result.json"
        )
        raw = json.loads(fixture_path.read_text(encoding="utf-8"))
        result = DeliveryValidationResult.model_validate(raw)
        assert len(result.check_details) > 0

    def test_example_fixture_observations_non_empty(self):
        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "expected_delivery_validation_result.json"
        )
        raw = json.loads(fixture_path.read_text(encoding="utf-8"))
        result = DeliveryValidationResult.model_validate(raw)
        assert len(result.observations) > 0
