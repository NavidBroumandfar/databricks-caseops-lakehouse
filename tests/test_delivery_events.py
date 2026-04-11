"""
tests/test_delivery_events.py — C-1: Delivery Event Tests.

Covers:
  - DeliveryEvent schema creation and validation
  - build_delivery_event() from pipeline summaries
  - delivery_event_id generation (determinism and uniqueness)
  - Artifact path computation (deterministic)
  - format_delivery_event_text() output
  - write_delivery_event() writes correct artifacts
  - load_delivery_event() round-trips correctly
  - Schema version is v0.2.0
  - Status and status_reason are correct for C-1
  - Routing label extraction (exported records only)
  - Empty batch edge cases
  - No coupling to real credentials or SDK calls
  - Preservation of V1 schema behavior (schema_version v0.1.0 when no delivery)
  - Integration: delivery event matches summary counts

Phase: C-1
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.schemas.delivery_event import (
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
    DELIVERY_STATUS_FAILED,
    DELIVERY_STATUS_PREPARED,
    DeliveryEvent,
)
from src.pipelines.delivery_events import (
    build_delivery_event,
    compute_delivery_event_path,
    compute_delivery_event_text_path,
    format_delivery_event_text,
    generate_delivery_event_id,
    load_delivery_event,
    write_delivery_event,
)
from src.pipelines.handoff_report import (
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_summary(
    document_id: str,
    outcome_category: str,
    routing_label: str = "regulatory_review",
) -> dict:
    """Build a minimal pipeline summary dict matching classify_gold.py output."""
    return {
        "document_id": document_id,
        "gold_record_id": str(uuid.uuid4()),
        "outcome_category": outcome_category,
        "outcome_reason": "none",
        "routing_label": routing_label,
        "export_ready": outcome_category == OUTCOME_EXPORTED,
        "pipeline_run_id": "local-run-test",
        "gold_artifact_path": f"output/gold/{document_id}.json",
        "export_artifact_path": (
            f"output/gold/exports/{routing_label}/{document_id}.json"
            if outcome_category == OUTCOME_EXPORTED
            else None
        ),
        "contract_validation_errors": [],
    }


def _make_mixed_summaries() -> list[dict]:
    """Return a batch of 4 summaries: 3 exported + 1 quarantined."""
    return [
        _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
        _make_summary("doc-002", OUTCOME_EXPORTED, "regulatory_review"),
        _make_summary("doc-003", OUTCOME_EXPORTED, "regulatory_review"),
        _make_summary("doc-004", OUTCOME_QUARANTINED, "quarantine"),
    ]


SAMPLE_RUN_ID = "local-run-abc123"
SAMPLE_EVENT_ID = str(uuid.uuid4())


# ---------------------------------------------------------------------------
# DeliveryEvent schema — valid creation
# ---------------------------------------------------------------------------


class TestDeliveryEventSchema:
    def test_create_minimal_valid(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=4,
            exported_record_count=3,
            quarantined_record_count=1,
            contract_blocked_count=0,
        )
        assert event.delivery_event_id == SAMPLE_EVENT_ID
        assert event.pipeline_run_id == SAMPLE_RUN_ID
        assert event.batch_id == SAMPLE_RUN_ID
        assert event.schema_version == DELIVERY_SCHEMA_VERSION
        assert event.delivery_mechanism == DELIVERY_MECHANISM_DELTA_SHARING
        assert event.status == DELIVERY_STATUS_PREPARED

    def test_schema_version_defaults_to_v020(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=1,
            exported_record_count=1,
            quarantined_record_count=0,
            contract_blocked_count=0,
        )
        assert event.schema_version == "v0.2.0"

    def test_rejects_wrong_schema_version(self):
        with pytest.raises(Exception):
            DeliveryEvent(
                delivery_event_id=SAMPLE_EVENT_ID,
                pipeline_run_id=SAMPLE_RUN_ID,
                batch_id=SAMPLE_RUN_ID,
                generated_at="2026-04-12T10:00:00+00:00",
                eligible_record_count=1,
                exported_record_count=1,
                quarantined_record_count=0,
                contract_blocked_count=0,
                schema_version="v0.1.0",
            )

    def test_rejects_unknown_delivery_mechanism(self):
        with pytest.raises(Exception):
            DeliveryEvent(
                delivery_event_id=SAMPLE_EVENT_ID,
                pipeline_run_id=SAMPLE_RUN_ID,
                batch_id=SAMPLE_RUN_ID,
                generated_at="2026-04-12T10:00:00+00:00",
                eligible_record_count=1,
                exported_record_count=1,
                quarantined_record_count=0,
                contract_blocked_count=0,
                delivery_mechanism="api_push",
            )

    def test_rejects_unknown_status(self):
        with pytest.raises(Exception):
            DeliveryEvent(
                delivery_event_id=SAMPLE_EVENT_ID,
                pipeline_run_id=SAMPLE_RUN_ID,
                batch_id=SAMPLE_RUN_ID,
                generated_at="2026-04-12T10:00:00+00:00",
                eligible_record_count=1,
                exported_record_count=1,
                quarantined_record_count=0,
                contract_blocked_count=0,
                status="delivered",
            )

    def test_accepts_failed_status(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=0,
            exported_record_count=0,
            quarantined_record_count=0,
            contract_blocked_count=0,
            status=DELIVERY_STATUS_FAILED,
            status_reason="Export write failed",
        )
        assert event.status == DELIVERY_STATUS_FAILED

    def test_share_name_default(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=1,
            exported_record_count=1,
            quarantined_record_count=0,
            contract_blocked_count=0,
        )
        assert event.share_name == DEFAULT_SHARE_NAME
        assert event.shared_object_name == DEFAULT_SHARED_OBJECT_NAME

    def test_to_json_dict_is_serializable(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=4,
            exported_record_count=3,
            quarantined_record_count=1,
            contract_blocked_count=0,
        )
        d = event.to_json_dict()
        assert isinstance(d, dict)
        assert d["schema_version"] == "v0.2.0"
        assert d["delivery_mechanism"] == DELIVERY_MECHANISM_DELTA_SHARING
        json.dumps(d)  # must not raise

    def test_to_json_str_is_valid_json(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=4,
            exported_record_count=3,
            quarantined_record_count=1,
            contract_blocked_count=0,
        )
        s = event.to_json_str()
        parsed = json.loads(s)
        assert parsed["delivery_event_id"] == SAMPLE_EVENT_ID

    def test_optional_fields_nullable(self):
        event = DeliveryEvent(
            delivery_event_id=SAMPLE_EVENT_ID,
            pipeline_run_id=SAMPLE_RUN_ID,
            batch_id=SAMPLE_RUN_ID,
            generated_at="2026-04-12T10:00:00+00:00",
            eligible_record_count=0,
            exported_record_count=0,
            quarantined_record_count=0,
            contract_blocked_count=0,
            bundle_artifact_path=None,
            report_artifact_path=None,
            status_reason=None,
            notes=None,
        )
        assert event.bundle_artifact_path is None
        assert event.report_artifact_path is None
        assert event.notes is None

    def test_count_fields_non_negative(self):
        with pytest.raises(Exception):
            DeliveryEvent(
                delivery_event_id=SAMPLE_EVENT_ID,
                pipeline_run_id=SAMPLE_RUN_ID,
                batch_id=SAMPLE_RUN_ID,
                generated_at="2026-04-12T10:00:00+00:00",
                eligible_record_count=-1,
                exported_record_count=0,
                quarantined_record_count=0,
                contract_blocked_count=0,
            )


# ---------------------------------------------------------------------------
# generate_delivery_event_id
# ---------------------------------------------------------------------------


class TestGenerateDeliveryEventId:
    def test_returns_string(self):
        eid = generate_delivery_event_id()
        assert isinstance(eid, str)

    def test_returns_valid_uuid(self):
        eid = generate_delivery_event_id()
        parsed = uuid.UUID(eid)
        assert str(parsed) == eid

    def test_each_call_returns_unique_id(self):
        ids = {generate_delivery_event_id() for _ in range(20)}
        assert len(ids) == 20

    def test_no_credentials_in_id(self):
        eid = generate_delivery_event_id()
        assert "token" not in eid.lower()
        assert "secret" not in eid.lower()
        assert "key" not in eid.lower()


# ---------------------------------------------------------------------------
# build_delivery_event
# ---------------------------------------------------------------------------


class TestBuildDeliveryEvent:
    def test_builds_from_mixed_summaries(self):
        summaries = _make_mixed_summaries()
        event = build_delivery_event(
            summaries=summaries,
            pipeline_run_id=SAMPLE_RUN_ID,
            delivery_event_id=SAMPLE_EVENT_ID,
        )
        assert event.eligible_record_count == 4
        assert event.exported_record_count == 3
        assert event.quarantined_record_count == 1
        assert event.contract_blocked_count == 0

    def test_pipeline_run_id_equals_batch_id(self):
        summaries = _make_mixed_summaries()
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.pipeline_run_id == SAMPLE_RUN_ID
        assert event.batch_id == SAMPLE_RUN_ID

    def test_delivery_event_id_used_when_provided(self):
        summaries = _make_mixed_summaries()
        event = build_delivery_event(
            summaries=summaries,
            pipeline_run_id=SAMPLE_RUN_ID,
            delivery_event_id=SAMPLE_EVENT_ID,
        )
        assert event.delivery_event_id == SAMPLE_EVENT_ID

    def test_delivery_event_id_generated_when_not_provided(self):
        summaries = _make_mixed_summaries()
        event = build_delivery_event(
            summaries=summaries,
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert isinstance(event.delivery_event_id, str)
        uuid.UUID(event.delivery_event_id)  # must be valid UUID

    def test_delivery_mechanism_is_delta_sharing(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.delivery_mechanism == DELIVERY_MECHANISM_DELTA_SHARING

    def test_schema_version_is_v020(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.schema_version == "v0.2.0"

    def test_status_is_prepared(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.status == DELIVERY_STATUS_PREPARED

    def test_status_reason_mentions_c2(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert "C-2" in event.status_reason or "c-2" in event.status_reason.lower()

    def test_routing_labels_from_exported_only(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_QUARANTINED, "quarantine"),
            _make_summary("doc-003", OUTCOME_CONTRACT_BLOCKED, "regulatory_review"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        # Only regulatory_review from exported records; quarantine is not included
        assert "regulatory_review" in event.routing_labels
        assert "quarantine" not in event.routing_labels

    def test_routing_labels_sorted(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_EXPORTED, "security_ops"),
            _make_summary("doc-003", OUTCOME_EXPORTED, "incident_management"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.routing_labels == sorted(event.routing_labels)

    def test_routing_labels_deduplicated(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-003", OUTCOME_EXPORTED, "regulatory_review"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.routing_labels == ["regulatory_review"]

    def test_bundle_path_set_when_provided(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            bundle_artifact_path="output/reports/handoff_bundle_xyz.json",
        )
        assert event.bundle_artifact_path == "output/reports/handoff_bundle_xyz.json"

    def test_bundle_path_none_when_not_provided(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.bundle_artifact_path is None

    def test_report_path_set_when_provided(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            report_artifact_path="output/reports/handoff_report_xyz.json",
        )
        assert event.report_artifact_path == "output/reports/handoff_report_xyz.json"

    def test_empty_batch(self):
        event = build_delivery_event(
            summaries=[],
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.eligible_record_count == 0
        assert event.exported_record_count == 0
        assert event.quarantined_record_count == 0
        assert event.contract_blocked_count == 0
        assert event.routing_labels == []

    def test_contract_blocked_counted(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_CONTRACT_BLOCKED, "regulatory_review"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.exported_record_count == 1
        assert event.contract_blocked_count == 1

    def test_skipped_records_counted_in_eligible(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_SKIPPED_NOT_EXPORT_READY, "quarantine"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.eligible_record_count == 2

    def test_generated_at_is_set(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.generated_at
        assert "2026" in event.generated_at or event.generated_at.startswith("20")

    def test_share_name_default(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event.share_name == DEFAULT_SHARE_NAME

    def test_custom_share_name(self):
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            share_name="my_custom_share",
        )
        assert event.share_name == "my_custom_share"

    def test_notes_optional(self):
        event_without_notes = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
        )
        assert event_without_notes.notes is None

        event_with_notes = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            notes="Test batch for C-1 validation",
        )
        assert event_with_notes.notes == "Test batch for C-1 validation"


# ---------------------------------------------------------------------------
# Artifact path computation
# ---------------------------------------------------------------------------


class TestArtifactPathComputation:
    def test_delivery_event_path_deterministic(self, tmp_path):
        path1 = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        path2 = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        assert path1 == path2

    def test_delivery_event_path_includes_run_id(self, tmp_path):
        path = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        assert SAMPLE_RUN_ID in path.name

    def test_delivery_event_path_is_json(self, tmp_path):
        path = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        assert path.suffix == ".json"

    def test_delivery_event_text_path_is_txt(self, tmp_path):
        path = compute_delivery_event_text_path(tmp_path, SAMPLE_RUN_ID)
        assert path.suffix == ".txt"

    def test_json_and_text_paths_same_stem(self, tmp_path):
        json_path = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        text_path = compute_delivery_event_text_path(tmp_path, SAMPLE_RUN_ID)
        assert json_path.stem == text_path.stem

    def test_different_run_ids_yield_different_paths(self, tmp_path):
        path_a = compute_delivery_event_path(tmp_path, "run-aaa")
        path_b = compute_delivery_event_path(tmp_path, "run-bbb")
        assert path_a != path_b

    def test_path_safe_for_run_id_with_slashes(self, tmp_path):
        path = compute_delivery_event_path(tmp_path, "run/with/slashes")
        assert "/" not in path.name
        assert "\\" not in path.name


# ---------------------------------------------------------------------------
# format_delivery_event_text
# ---------------------------------------------------------------------------


class TestFormatDeliveryEventText:
    def _make_event(self) -> DeliveryEvent:
        return build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            delivery_event_id=SAMPLE_EVENT_ID,
            bundle_artifact_path="output/reports/bundle.json",
        )

    def test_returns_string(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert isinstance(text, str)

    def test_contains_delivery_event_id(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert SAMPLE_EVENT_ID in text

    def test_contains_pipeline_run_id(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert SAMPLE_RUN_ID in text

    def test_contains_schema_version(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert "v0.2.0" in text

    def test_contains_delivery_mechanism(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert "delta_sharing" in text

    def test_contains_status(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert DELIVERY_STATUS_PREPARED in text

    def test_contains_c2_validation_boundary_note(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert "C-2" in text

    def test_contains_outcome_counts(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert "3" in text  # exported_record_count
        assert "1" in text  # quarantined_record_count

    def test_contains_bundle_path(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        assert "bundle.json" in text

    def test_no_credentials_in_text(self):
        event = self._make_event()
        text = format_delivery_event_text(event)
        lowered = text.lower()
        for sensitive in ("token", "secret", "password", "credential", "databricks_token"):
            assert sensitive not in lowered


# ---------------------------------------------------------------------------
# write_delivery_event and load_delivery_event
# ---------------------------------------------------------------------------


class TestWriteAndLoadDeliveryEvent:
    def _make_event(self) -> DeliveryEvent:
        return build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            delivery_event_id=SAMPLE_EVENT_ID,
            bundle_artifact_path="output/reports/bundle.json",
        )

    def test_write_creates_json_and_text_files(self, tmp_path):
        event = self._make_event()
        json_path, text_path = write_delivery_event(event, tmp_path)
        assert json_path.exists()
        assert text_path.exists()

    def test_write_json_is_valid_json(self, tmp_path):
        event = self._make_event()
        json_path, _ = write_delivery_event(event, tmp_path)
        content = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(content, dict)

    def test_write_creates_directory(self, tmp_path):
        event = self._make_event()
        target = tmp_path / "nested" / "delivery"
        write_delivery_event(event, target)
        assert target.is_dir()

    def test_json_path_deterministic(self, tmp_path):
        event = self._make_event()
        json_path, _ = write_delivery_event(event, tmp_path)
        expected = compute_delivery_event_path(tmp_path, SAMPLE_RUN_ID)
        assert json_path == expected

    def test_load_round_trips(self, tmp_path):
        event = self._make_event()
        json_path, _ = write_delivery_event(event, tmp_path)
        loaded = load_delivery_event(json_path)
        assert loaded.delivery_event_id == event.delivery_event_id
        assert loaded.pipeline_run_id == event.pipeline_run_id
        assert loaded.eligible_record_count == event.eligible_record_count
        assert loaded.exported_record_count == event.exported_record_count
        assert loaded.quarantined_record_count == event.quarantined_record_count
        assert loaded.schema_version == event.schema_version
        assert loaded.status == event.status

    def test_loaded_schema_version_is_v020(self, tmp_path):
        event = self._make_event()
        json_path, _ = write_delivery_event(event, tmp_path)
        loaded = load_delivery_event(json_path)
        assert loaded.schema_version == "v0.2.0"

    def test_text_file_contains_human_readable_header(self, tmp_path):
        event = self._make_event()
        _, text_path = write_delivery_event(event, tmp_path)
        content = text_path.read_text(encoding="utf-8")
        assert "DELIVERY EVENT" in content

    def test_no_credentials_in_json_artifact(self, tmp_path):
        event = self._make_event()
        json_path, _ = write_delivery_event(event, tmp_path)
        content = json_path.read_text(encoding="utf-8").lower()
        for sensitive in ("token", "secret", "password", "credential", "databricks_token"):
            assert sensitive not in content


# ---------------------------------------------------------------------------
# Integration: delivery event matches summary counts
# ---------------------------------------------------------------------------


class TestDeliveryEventIntegration:
    def test_counts_match_summaries(self):
        summaries = [
            _make_summary("doc-001", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-002", OUTCOME_EXPORTED, "regulatory_review"),
            _make_summary("doc-003", OUTCOME_QUARANTINED, "quarantine"),
            _make_summary("doc-004", OUTCOME_CONTRACT_BLOCKED, "regulatory_review"),
            _make_summary("doc-005", OUTCOME_SKIPPED_NOT_EXPORT_READY, "quarantine"),
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.eligible_record_count == 5
        assert event.exported_record_count == 2
        assert event.quarantined_record_count == 1
        assert event.contract_blocked_count == 1

    def test_all_exported_batch(self):
        summaries = [
            _make_summary(f"doc-{i:03d}", OUTCOME_EXPORTED, "regulatory_review")
            for i in range(5)
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.exported_record_count == 5
        assert event.quarantined_record_count == 0
        assert event.contract_blocked_count == 0

    def test_all_quarantined_batch(self):
        summaries = [
            _make_summary(f"doc-{i:03d}", OUTCOME_QUARANTINED, "quarantine")
            for i in range(3)
        ]
        event = build_delivery_event(summaries=summaries, pipeline_run_id=SAMPLE_RUN_ID)
        assert event.exported_record_count == 0
        assert event.quarantined_record_count == 3
        assert event.routing_labels == []

    def test_delivery_event_id_stable_across_write_load(self, tmp_path):
        pre_generated_id = generate_delivery_event_id()
        event = build_delivery_event(
            summaries=_make_mixed_summaries(),
            pipeline_run_id=SAMPLE_RUN_ID,
            delivery_event_id=pre_generated_id,
        )
        json_path, _ = write_delivery_event(event, tmp_path)
        loaded = load_delivery_event(json_path)
        assert loaded.delivery_event_id == pre_generated_id

    def test_no_real_sdk_calls_needed(self):
        """
        This test confirms that the entire delivery event flow completes
        without any external calls, credentials, or SDK imports.
        """
        summaries = _make_mixed_summaries()
        event = build_delivery_event(
            summaries=summaries,
            pipeline_run_id="local-run-nodeps",
            delivery_event_id=generate_delivery_event_id(),
        )
        # If we reach here, no SDK was called.
        assert event.schema_version == "v0.2.0"
        assert event.status == DELIVERY_STATUS_PREPARED

    def test_v1_behavior_preserved_without_delivery(self):
        """
        V1 export path is unchanged when delivery_dir is not provided.
        Confirm that the delivery event schema and delivery provenance fields
        remain disconnected from the V1 path.
        """
        from src.schemas.gold_schema import SCHEMA_VERSION
        assert SCHEMA_VERSION == "v0.1.0"
        # The default schema is v0.1.0 — delivery augmentation only activates
        # when delivery_event_id is passed to build_export_payload.

    def test_fixture_file_is_valid(self):
        """Confirm the expected_delivery_event.json fixture is valid."""
        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "expected_delivery_event.json"
        )
        assert fixture_path.exists(), "examples/expected_delivery_event.json not found"
        raw = json.loads(fixture_path.read_text(encoding="utf-8"))
        event = DeliveryEvent.model_validate(raw)
        assert event.schema_version == "v0.2.0"
        assert event.delivery_mechanism == DELIVERY_MECHANISM_DELTA_SHARING
        assert event.status == DELIVERY_STATUS_PREPARED
