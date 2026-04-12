"""
tests/test_e0_review_queue.py — Phase E-0 Human Review Queue and Reprocessing Tests.

Tests for the E-0 upstream human review queue layer:
  - Review queue schema and factory helpers
  - Review queue derivation from pipeline summaries
  - Review decision schema and validation
  - Reprocessing request schema and validation
  - Queue artifact write/read round-trip
  - Integration boundary (no Bedrock/runtime/UI drift)
  - Preservation of existing pipeline behavior (no regression)

Phase: E-0
Modules under test:
  src/schemas/review_queue.py
  src/schemas/review_decision.py
  src/pipelines/review_queue.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import pytest

# Allow running from repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.schemas.review_queue import (
    ALL_REVIEW_REASON_CATEGORIES,
    REVIEW_QUEUE_SCHEMA_VERSION,
    REVIEW_REASON_CONTRACT_BLOCKED,
    REVIEW_REASON_EXTRACTION_FAILED,
    REVIEW_REASON_QUARANTINED,
    ReviewQueueArtifact,
    ReviewQueueEntry,
    make_queue_entry_id,
    make_review_queue_id,
    make_review_reason,
)
from src.schemas.review_decision import (
    ALL_REVIEW_DECISIONS,
    ACTIONABLE_DECISIONS,
    DECISION_APPROVE_FOR_EXPORT,
    DECISION_CONFIRM_QUARANTINE,
    DECISION_REQUEST_REPROCESSING,
    DECISION_REJECT_UNRESOLVED,
    REVIEW_DECISION_SCHEMA_VERSION,
    REPROCESSING_REQUEST_SCHEMA_VERSION,
    TERMINAL_DECISIONS,
    ReviewDecision,
    ReprocessingRequest,
    build_reprocessing_request,
    make_decision_id,
    make_reprocessing_request_id,
    validate_review_decision,
    validate_reprocessing_request,
)
from src.pipelines.review_queue import (
    _should_include_in_review_queue,
    build_review_queue_from_summaries,
    compute_review_queue_path,
    format_review_queue_text,
    write_review_queue,
    load_review_queue,
)


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

PIPELINE_RUN_ID = "local-run-test-11111111-1111-1111-1111-111111111111"


def _quarantine_summary(**overrides) -> dict:
    """Minimal quarantined record summary."""
    base = {
        "document_id": "doc-quar-0001",
        "gold_record_id": "gold-quar-0001",
        "pipeline_run_id": PIPELINE_RUN_ID,
        "document_type_label": "unknown",
        "routing_label": "quarantine",
        "outcome_category": "quarantined",
        "outcome_reason": "routing_quarantine",
        "export_ready": False,
        "gold_artifact_path": "output/gold/gold-quar-0001.json",
        "export_artifact_path": None,
        "contract_validation_errors": [],
    }
    base.update(overrides)
    return base


def _contract_blocked_summary(**overrides) -> dict:
    """Minimal contract-blocked record summary."""
    base = {
        "document_id": "doc-blocked-0001",
        "gold_record_id": "gold-blocked-0001",
        "pipeline_run_id": PIPELINE_RUN_ID,
        "document_type_label": "fda_warning_letter",
        "routing_label": "regulatory_review",
        "outcome_category": "contract_blocked",
        "outcome_reason": "contract_validation_failed",
        "export_ready": False,
        "gold_artifact_path": "output/gold/gold-blocked-0001.json",
        "export_artifact_path": None,
        "contract_validation_errors": [
            "violation_type is required",
            "corrective_action_requested is required",
        ],
    }
    base.update(overrides)
    return base


def _exported_summary(**overrides) -> dict:
    """Minimal successfully exported record summary."""
    base = {
        "document_id": "doc-exp-0001",
        "gold_record_id": "gold-exp-0001",
        "pipeline_run_id": PIPELINE_RUN_ID,
        "document_type_label": "fda_warning_letter",
        "routing_label": "regulatory_review",
        "outcome_category": "exported",
        "outcome_reason": "none",
        "export_ready": True,
        "gold_artifact_path": "output/gold/gold-exp-0001.json",
        "export_artifact_path": "output/gold/exports/regulatory_review/doc-exp-0001.json",
        "contract_validation_errors": [],
    }
    base.update(overrides)
    return base


def _skipped_unknown_summary(**overrides) -> dict:
    """Minimal skipped-not-export-ready record with unknown document type (extraction_failed)."""
    base = {
        "document_id": "doc-skip-0001",
        "gold_record_id": "gold-skip-0001",
        "pipeline_run_id": PIPELINE_RUN_ID,
        "document_type_label": "unknown",
        "routing_label": "quarantine",
        "outcome_category": "skipped_not_export_ready",
        "outcome_reason": "export_not_attempted",
        "export_ready": False,
        "gold_artifact_path": "output/gold/gold-skip-0001.json",
        "export_artifact_path": None,
        "contract_validation_errors": [],
    }
    base.update(overrides)
    return base


def _make_valid_decision(
    decision: str = DECISION_CONFIRM_QUARANTINE,
    reprocessing_request_id: str | None = None,
) -> ReviewDecision:
    return ReviewDecision(
        decision_id=make_decision_id(),
        queue_entry_id=make_queue_entry_id(),
        document_id="doc-test-0001",
        gold_record_id="gold-test-0001",
        pipeline_run_id=PIPELINE_RUN_ID,
        decided_at=datetime.now(tz=timezone.utc).isoformat(),
        schema_version=REVIEW_DECISION_SCHEMA_VERSION,
        decision=decision,
        decision_rationale="Test rationale for unit test.",
        reprocessing_request_id=reprocessing_request_id,
    )


def _make_valid_reprocessing_request(decision: ReviewDecision) -> ReprocessingRequest:
    return ReprocessingRequest(
        reprocessing_request_id=make_reprocessing_request_id(),
        document_id=decision.document_id,
        gold_record_id=decision.gold_record_id,
        original_pipeline_run_id=decision.pipeline_run_id,
        requested_at=datetime.now(tz=timezone.utc).isoformat(),
        schema_version=REPROCESSING_REQUEST_SCHEMA_VERSION,
        reprocessing_reason="Unit test reprocessing reason.",
        suggested_document_class_hint="fda_warning_letter",
        suggested_extraction_notes=None,
        linked_queue_entry_id=decision.queue_entry_id,
        linked_decision_id=decision.decision_id,
        original_gold_artifact_path="output/gold/gold-test-0001.json",
        original_bundle_path=None,
    )


# ===========================================================================
# I. Review Queue Schema Tests
# ===========================================================================


class TestReviewQueueConstants:
    """ALL_REVIEW_REASON_CATEGORIES contains the expected constants."""

    def test_all_reason_categories_present(self):
        assert REVIEW_REASON_QUARANTINED in ALL_REVIEW_REASON_CATEGORIES
        assert REVIEW_REASON_CONTRACT_BLOCKED in ALL_REVIEW_REASON_CATEGORIES
        assert REVIEW_REASON_EXTRACTION_FAILED in ALL_REVIEW_REASON_CATEGORIES

    def test_exactly_three_reason_categories(self):
        assert len(ALL_REVIEW_REASON_CATEGORIES) == 3

    def test_schema_version_is_string(self):
        assert isinstance(REVIEW_QUEUE_SCHEMA_VERSION, str)
        assert REVIEW_QUEUE_SCHEMA_VERSION.startswith("v")

    def test_reason_category_values_are_strings(self):
        for cat in ALL_REVIEW_REASON_CATEGORIES:
            assert isinstance(cat, str)
            assert len(cat) > 0


class TestMakeQueueEntryId:
    def test_returns_string(self):
        assert isinstance(make_queue_entry_id(), str)

    def test_generates_unique_ids(self):
        ids = {make_queue_entry_id() for _ in range(50)}
        assert len(ids) == 50

    def test_non_empty(self):
        assert len(make_queue_entry_id()) > 0


class TestMakeReviewQueueId:
    def test_returns_string(self):
        assert isinstance(make_review_queue_id(), str)

    def test_generates_unique_ids(self):
        ids = {make_review_queue_id() for _ in range(50)}
        assert len(ids) == 50


class TestMakeReviewReason:
    def test_quarantined_reason_contains_expected_text(self):
        reason = make_review_reason(
            reason_category=REVIEW_REASON_QUARANTINED,
            document_type_label="unknown",
            routing_label="quarantine",
        )
        assert "quarantined" in reason.lower()
        assert "unknown" in reason

    def test_contract_blocked_reason_contains_expected_text(self):
        reason = make_review_reason(
            reason_category=REVIEW_REASON_CONTRACT_BLOCKED,
            document_type_label="fda_warning_letter",
            routing_label="regulatory_review",
            contract_validation_errors=["field_x is required"],
        )
        assert "contract" in reason.lower()
        assert "field_x is required" in reason

    def test_contract_blocked_reason_without_errors(self):
        reason = make_review_reason(
            reason_category=REVIEW_REASON_CONTRACT_BLOCKED,
            document_type_label="fda_warning_letter",
            routing_label="regulatory_review",
        )
        assert "contract" in reason.lower()

    def test_extraction_failed_reason_contains_expected_text(self):
        reason = make_review_reason(
            reason_category=REVIEW_REASON_EXTRACTION_FAILED,
            document_type_label="unknown",
            routing_label="quarantine",
        )
        assert "unknown" in reason.lower() or "classification" in reason.lower()

    def test_contract_blocked_reason_truncates_long_error_list(self):
        errors = [f"error_{i}" for i in range(10)]
        reason = make_review_reason(
            reason_category=REVIEW_REASON_CONTRACT_BLOCKED,
            document_type_label="fda_warning_letter",
            routing_label="regulatory_review",
            contract_validation_errors=errors,
        )
        assert "and 7 more" in reason

    def test_unknown_reason_category_returns_fallback(self):
        reason = make_review_reason(
            reason_category="some_unknown_category",
            document_type_label="fda_warning_letter",
            routing_label="regulatory_review",
        )
        assert "some_unknown_category" in reason


class TestReviewQueueEntry:
    def test_to_dict_contains_all_fields(self):
        entry = ReviewQueueEntry(
            queue_entry_id="q1",
            document_id="doc1",
            gold_record_id="gold1",
            pipeline_run_id=PIPELINE_RUN_ID,
            document_type_label="unknown",
            routing_label="quarantine",
            review_reason="Test reason.",
            review_reason_category=REVIEW_REASON_QUARANTINED,
            gold_artifact_path="output/gold/gold1.json",
            export_artifact_path=None,
            bundle_path=None,
            report_path=None,
            contract_validation_errors=[],
        )
        d = entry.to_dict()
        assert d["queue_entry_id"] == "q1"
        assert d["document_id"] == "doc1"
        assert d["gold_record_id"] == "gold1"
        assert d["review_reason_category"] == REVIEW_REASON_QUARANTINED
        assert d["export_artifact_path"] is None
        assert d["contract_validation_errors"] == []

    def test_to_dict_is_json_serializable(self):
        entry = ReviewQueueEntry(
            queue_entry_id="q1",
            document_id="doc1",
            gold_record_id=None,
            pipeline_run_id=PIPELINE_RUN_ID,
            document_type_label="unknown",
            routing_label="quarantine",
            review_reason="reason",
            review_reason_category=REVIEW_REASON_QUARANTINED,
            gold_artifact_path=None,
            export_artifact_path=None,
            bundle_path=None,
            report_path=None,
        )
        json_str = json.dumps(entry.to_dict())
        assert len(json_str) > 0


class TestReviewQueueArtifact:
    def test_to_dict_contains_all_fields(self):
        artifact = ReviewQueueArtifact(
            review_queue_id="rq1",
            pipeline_run_id=PIPELINE_RUN_ID,
            generated_at="2026-04-12T00:00:00+00:00",
            schema_version=REVIEW_QUEUE_SCHEMA_VERSION,
            total_records_reviewed=3,
            total_entries=1,
            entries_by_reason={REVIEW_REASON_QUARANTINED: 1},
            queue_entries=[],
            review_notes=["Note 1."],
        )
        d = artifact.to_dict()
        assert d["review_queue_id"] == "rq1"
        assert d["total_entries"] == 1
        assert d["schema_version"] == REVIEW_QUEUE_SCHEMA_VERSION

    def test_to_json_str_is_valid_json(self):
        artifact = ReviewQueueArtifact(
            review_queue_id="rq1",
            pipeline_run_id=PIPELINE_RUN_ID,
            generated_at="2026-04-12T00:00:00+00:00",
            schema_version=REVIEW_QUEUE_SCHEMA_VERSION,
            total_records_reviewed=1,
            total_entries=0,
            entries_by_reason={},
            queue_entries=[],
        )
        parsed = json.loads(artifact.to_json_str())
        assert parsed["review_queue_id"] == "rq1"


# ===========================================================================
# II. Review Queue Derivation Tests
# ===========================================================================


class TestShouldIncludeInReviewQueue:
    """Unit tests for the per-record inclusion predicate."""

    def test_quarantined_record_is_included(self):
        summary = _quarantine_summary()
        include, reason = _should_include_in_review_queue(summary)
        assert include is True
        assert reason == REVIEW_REASON_QUARANTINED

    def test_contract_blocked_record_is_included(self):
        summary = _contract_blocked_summary()
        include, reason = _should_include_in_review_queue(summary)
        assert include is True
        assert reason == REVIEW_REASON_CONTRACT_BLOCKED

    def test_exported_record_is_not_included(self):
        summary = _exported_summary()
        include, reason = _should_include_in_review_queue(summary)
        assert include is False
        assert reason == ""

    def test_skipped_unknown_type_is_included_as_extraction_failed(self):
        summary = _skipped_unknown_summary()
        include, reason = _should_include_in_review_queue(summary)
        assert include is True
        assert reason == REVIEW_REASON_EXTRACTION_FAILED

    def test_skipped_known_type_is_not_included(self):
        # Skipped records with a known document type don't enter the queue
        summary = _skipped_unknown_summary(
            document_type_label="fda_warning_letter",
            outcome_category="skipped_not_export_ready",
        )
        include, reason = _should_include_in_review_queue(summary)
        assert include is False

    def test_missing_outcome_category_is_not_included(self):
        summary = {"document_id": "doc-x", "document_type_label": "fda_warning_letter"}
        include, reason = _should_include_in_review_queue(summary)
        assert include is False


class TestBuildReviewQueueFromSummaries:
    """Integration-level tests for the full queue builder."""

    def test_empty_summaries_produces_empty_queue(self):
        queue = build_review_queue_from_summaries(
            summaries=[], pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 0
        assert queue.total_records_reviewed == 0
        assert queue.queue_entries == []

    def test_quarantined_record_enters_queue(self):
        summaries = [_quarantine_summary()]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 1
        assert queue.total_records_reviewed == 1
        entry = queue.queue_entries[0]
        assert entry.review_reason_category == REVIEW_REASON_QUARANTINED
        assert entry.document_id == "doc-quar-0001"

    def test_contract_blocked_record_enters_queue(self):
        summaries = [_contract_blocked_summary()]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 1
        entry = queue.queue_entries[0]
        assert entry.review_reason_category == REVIEW_REASON_CONTRACT_BLOCKED
        assert entry.contract_validation_errors == [
            "violation_type is required",
            "corrective_action_requested is required",
        ]

    def test_exported_record_does_not_enter_queue(self):
        summaries = [_exported_summary()]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 0

    def test_mixed_batch_correct_queue_count(self):
        summaries = [
            _exported_summary(document_id="doc-exp-1"),
            _quarantine_summary(document_id="doc-quar-1"),
            _contract_blocked_summary(document_id="doc-blocked-1"),
            _exported_summary(document_id="doc-exp-2"),
        ]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_records_reviewed == 4
        assert queue.total_entries == 2
        categories = [e.review_reason_category for e in queue.queue_entries]
        assert REVIEW_REASON_QUARANTINED in categories
        assert REVIEW_REASON_CONTRACT_BLOCKED in categories

    def test_queue_has_correct_pipeline_run_id(self):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()],
            pipeline_run_id=PIPELINE_RUN_ID,
        )
        assert queue.pipeline_run_id == PIPELINE_RUN_ID
        assert queue.queue_entries[0].pipeline_run_id == PIPELINE_RUN_ID

    def test_queue_schema_version_is_correct(self):
        queue = build_review_queue_from_summaries(
            summaries=[], pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.schema_version == REVIEW_QUEUE_SCHEMA_VERSION

    def test_queue_id_is_unique_per_call(self):
        q1 = build_review_queue_from_summaries(summaries=[], pipeline_run_id=PIPELINE_RUN_ID)
        q2 = build_review_queue_from_summaries(summaries=[], pipeline_run_id=PIPELINE_RUN_ID)
        assert q1.review_queue_id != q2.review_queue_id

    def test_entry_ids_are_unique_within_queue(self):
        summaries = [
            _quarantine_summary(document_id=f"doc-q-{i}") for i in range(5)
        ]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        entry_ids = [e.queue_entry_id for e in queue.queue_entries]
        assert len(entry_ids) == len(set(entry_ids))

    def test_bundle_path_propagated_to_entries(self):
        summaries = [_quarantine_summary()]
        bundle_path = "output/reports/handoff_bundle_test.json"
        queue = build_review_queue_from_summaries(
            summaries=summaries,
            pipeline_run_id=PIPELINE_RUN_ID,
            bundle_path=bundle_path,
        )
        assert queue.queue_entries[0].bundle_path == bundle_path

    def test_report_path_propagated_to_entries(self):
        summaries = [_quarantine_summary()]
        report_path = "output/reports/handoff_report_test.json"
        queue = build_review_queue_from_summaries(
            summaries=summaries,
            pipeline_run_id=PIPELINE_RUN_ID,
            report_path=report_path,
        )
        assert queue.queue_entries[0].report_path == report_path

    def test_entries_by_reason_counts_correct(self):
        summaries = [
            _quarantine_summary(document_id="q1"),
            _quarantine_summary(document_id="q2"),
            _contract_blocked_summary(document_id="b1"),
            _exported_summary(document_id="e1"),
        ]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.entries_by_reason[REVIEW_REASON_QUARANTINED] == 2
        assert queue.entries_by_reason[REVIEW_REASON_CONTRACT_BLOCKED] == 1
        assert queue.entries_by_reason[REVIEW_REASON_EXTRACTION_FAILED] == 0

    def test_extraction_failed_entry_in_queue(self):
        summaries = [_skipped_unknown_summary()]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 1
        assert queue.queue_entries[0].review_reason_category == REVIEW_REASON_EXTRACTION_FAILED

    def test_generated_at_defaults_to_utc(self):
        queue = build_review_queue_from_summaries(
            summaries=[], pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.generated_at is not None
        assert "+" in queue.generated_at or "Z" in queue.generated_at or "T" in queue.generated_at

    def test_generated_at_can_be_overridden(self):
        ts = "2026-01-01T00:00:00+00:00"
        queue = build_review_queue_from_summaries(
            summaries=[], pipeline_run_id=PIPELINE_RUN_ID, generated_at=ts
        )
        assert queue.generated_at == ts

    def test_review_notes_are_non_empty(self):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()], pipeline_run_id=PIPELINE_RUN_ID
        )
        assert len(queue.review_notes) > 0
        for note in queue.review_notes:
            assert isinstance(note, str)

    def test_all_domains_can_appear_in_queue(self):
        """CISA and incident records can also appear in the review queue."""
        summaries = [
            _quarantine_summary(
                document_id="cisa-q1",
                document_type_label="cisa_advisory",
            ),
            _quarantine_summary(
                document_id="incident-q1",
                document_type_label="incident_report",
            ),
        ]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        assert queue.total_entries == 2
        labels = [e.document_type_label for e in queue.queue_entries]
        assert "cisa_advisory" in labels
        assert "incident_report" in labels


# ===========================================================================
# III. Artifact Path and Write Tests
# ===========================================================================


class TestComputeReviewQueuePath:
    def test_path_contains_run_id(self):
        path = compute_review_queue_path(Path("output/review"), "test-run-001")
        assert "test-run-001" in path.name

    def test_path_is_json(self):
        path = compute_review_queue_path(Path("output/review"), "test-run-001")
        assert path.suffix == ".json"

    def test_path_is_deterministic(self):
        path1 = compute_review_queue_path(Path("output/review"), "run-abc")
        path2 = compute_review_queue_path(Path("output/review"), "run-abc")
        assert path1 == path2

    def test_path_sanitizes_special_chars(self):
        path = compute_review_queue_path(Path("output/review"), "run/with:spaces here")
        assert "/" not in path.name
        assert ":" not in path.name
        assert " " not in path.name


class TestFormatReviewQueueText:
    def test_format_includes_header(self):
        queue = build_review_queue_from_summaries([], pipeline_run_id=PIPELINE_RUN_ID)
        text = format_review_queue_text(queue)
        assert "Review Queue" in text

    def test_format_includes_pipeline_run_id(self):
        queue = build_review_queue_from_summaries([], pipeline_run_id=PIPELINE_RUN_ID)
        text = format_review_queue_text(queue)
        assert PIPELINE_RUN_ID in text

    def test_format_includes_entry_details(self):
        summaries = [_quarantine_summary()]
        queue = build_review_queue_from_summaries(
            summaries=summaries, pipeline_run_id=PIPELINE_RUN_ID
        )
        text = format_review_queue_text(queue)
        assert "doc-quar-0001" in text

    def test_format_empty_queue_notes_no_entries(self):
        queue = build_review_queue_from_summaries([], pipeline_run_id=PIPELINE_RUN_ID)
        text = format_review_queue_text(queue)
        assert "no entries" in text.lower() or "0" in text


class TestWriteReviewQueue:
    def test_write_creates_json_and_text(self, tmp_path):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()],
            pipeline_run_id="test-write-run",
        )
        json_path, text_path = write_review_queue(queue, tmp_path)
        assert json_path.exists()
        assert text_path.exists()
        assert json_path.suffix == ".json"
        assert text_path.suffix == ".txt"

    def test_json_artifact_is_valid_json(self, tmp_path):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()],
            pipeline_run_id="test-write-run-2",
        )
        json_path, _ = write_review_queue(queue, tmp_path)
        parsed = json.loads(json_path.read_text())
        assert "review_queue_id" in parsed
        assert "queue_entries" in parsed

    def test_json_artifact_preserves_pipeline_run_id(self, tmp_path):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()],
            pipeline_run_id="preserve-run-id-test",
        )
        json_path, _ = write_review_queue(queue, tmp_path)
        parsed = json.loads(json_path.read_text())
        assert parsed["pipeline_run_id"] == "preserve-run-id-test"

    def test_load_review_queue_round_trip(self, tmp_path):
        queue = build_review_queue_from_summaries(
            summaries=[_quarantine_summary()],
            pipeline_run_id="round-trip-test",
        )
        json_path, _ = write_review_queue(queue, tmp_path)
        loaded = load_review_queue(json_path)
        assert loaded["pipeline_run_id"] == "round-trip-test"
        assert len(loaded["queue_entries"]) == 1
        assert loaded["schema_version"] == REVIEW_QUEUE_SCHEMA_VERSION

    def test_write_creates_output_dir_if_absent(self, tmp_path):
        new_dir = tmp_path / "deep" / "nested" / "dir"
        queue = build_review_queue_from_summaries(
            summaries=[], pipeline_run_id="dir-creation-test"
        )
        json_path, _ = write_review_queue(queue, new_dir)
        assert json_path.exists()


# ===========================================================================
# IV. Review Decision Schema Tests
# ===========================================================================


class TestReviewDecisionConstants:
    def test_all_decisions_present(self):
        assert DECISION_APPROVE_FOR_EXPORT in ALL_REVIEW_DECISIONS
        assert DECISION_CONFIRM_QUARANTINE in ALL_REVIEW_DECISIONS
        assert DECISION_REQUEST_REPROCESSING in ALL_REVIEW_DECISIONS
        assert DECISION_REJECT_UNRESOLVED in ALL_REVIEW_DECISIONS

    def test_exactly_four_decisions(self):
        assert len(ALL_REVIEW_DECISIONS) == 4

    def test_actionable_decisions(self):
        assert DECISION_APPROVE_FOR_EXPORT in ACTIONABLE_DECISIONS
        assert DECISION_REQUEST_REPROCESSING in ACTIONABLE_DECISIONS
        assert DECISION_CONFIRM_QUARANTINE not in ACTIONABLE_DECISIONS
        assert DECISION_REJECT_UNRESOLVED not in ACTIONABLE_DECISIONS

    def test_terminal_decisions(self):
        assert DECISION_CONFIRM_QUARANTINE in TERMINAL_DECISIONS
        assert DECISION_REJECT_UNRESOLVED in TERMINAL_DECISIONS
        assert DECISION_APPROVE_FOR_EXPORT not in TERMINAL_DECISIONS
        assert DECISION_REQUEST_REPROCESSING not in TERMINAL_DECISIONS

    def test_schema_version_is_string(self):
        assert isinstance(REVIEW_DECISION_SCHEMA_VERSION, str)

    def test_reprocessing_schema_version_is_string(self):
        assert isinstance(REPROCESSING_REQUEST_SCHEMA_VERSION, str)


class TestReviewDecisionModel:
    def test_to_dict_contains_all_fields(self):
        decision = _make_valid_decision()
        d = decision.to_dict()
        assert "decision_id" in d
        assert "queue_entry_id" in d
        assert "document_id" in d
        assert "decision" in d
        assert "decision_rationale" in d
        assert "schema_version" in d

    def test_to_json_str_is_valid_json(self):
        decision = _make_valid_decision()
        parsed = json.loads(decision.to_json_str())
        assert parsed["decision"] == DECISION_CONFIRM_QUARANTINE

    def test_is_actionable_approve_for_export(self):
        decision = _make_valid_decision(DECISION_APPROVE_FOR_EXPORT)
        assert decision.is_actionable is True
        assert decision.is_terminal is False

    def test_is_actionable_request_reprocessing(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        assert decision.is_actionable is True

    def test_is_terminal_confirm_quarantine(self):
        decision = _make_valid_decision(DECISION_CONFIRM_QUARANTINE)
        assert decision.is_terminal is True
        assert decision.is_actionable is False

    def test_is_terminal_reject_unresolved(self):
        decision = _make_valid_decision(DECISION_REJECT_UNRESOLVED)
        assert decision.is_terminal is True

    def test_reprocessing_request_id_optional_by_default(self):
        decision = _make_valid_decision(DECISION_CONFIRM_QUARANTINE)
        assert decision.reprocessing_request_id is None


class TestValidateReviewDecision:
    def test_valid_decision_has_no_errors(self):
        decision = _make_valid_decision(DECISION_CONFIRM_QUARANTINE)
        errors = validate_review_decision(decision)
        assert errors == []

    def test_invalid_decision_vocabulary_raises_error(self):
        decision = _make_valid_decision()
        decision = ReviewDecision(**{**decision.to_dict(), "decision": "invalid_decision"})
        errors = validate_review_decision(decision)
        assert any("not a valid review decision" in e for e in errors)

    def test_empty_rationale_raises_error(self):
        d = _make_valid_decision().to_dict()
        d["decision_rationale"] = ""
        decision = ReviewDecision(**d)
        errors = validate_review_decision(decision)
        assert any("decision_rationale" in e for e in errors)

    def test_missing_decision_id_raises_error(self):
        d = _make_valid_decision().to_dict()
        d["decision_id"] = ""
        decision = ReviewDecision(**d)
        errors = validate_review_decision(decision)
        assert any("decision_id" in e for e in errors)

    def test_missing_document_id_raises_error(self):
        d = _make_valid_decision().to_dict()
        d["document_id"] = ""
        decision = ReviewDecision(**d)
        errors = validate_review_decision(decision)
        assert any("document_id" in e for e in errors)

    def test_missing_queue_entry_id_raises_error(self):
        d = _make_valid_decision().to_dict()
        d["queue_entry_id"] = ""
        decision = ReviewDecision(**d)
        errors = validate_review_decision(decision)
        assert any("queue_entry_id" in e for e in errors)

    def test_request_reprocessing_requires_reprocessing_request_id(self):
        decision = _make_valid_decision(DECISION_REQUEST_REPROCESSING)
        errors = validate_review_decision(decision)
        assert any("reprocessing_request_id" in e for e in errors)

    def test_request_reprocessing_with_id_is_valid(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        errors = validate_review_decision(decision)
        assert errors == []

    def test_all_valid_decisions_are_accepted(self):
        for dec in ALL_REVIEW_DECISIONS:
            repr_id = "repr-0001" if dec == DECISION_REQUEST_REPROCESSING else None
            decision = _make_valid_decision(dec, reprocessing_request_id=repr_id)
            errors = validate_review_decision(decision)
            assert errors == [], f"Decision '{dec}' unexpectedly invalid: {errors}"


# ===========================================================================
# V. Reprocessing Request Tests
# ===========================================================================


class TestReprocessingRequestModel:
    def test_to_dict_contains_all_fields(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        d = req.to_dict()
        assert "reprocessing_request_id" in d
        assert "document_id" in d
        assert "original_pipeline_run_id" in d
        assert "reprocessing_reason" in d
        assert "linked_queue_entry_id" in d
        assert "linked_decision_id" in d

    def test_to_json_str_is_valid_json(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        parsed = json.loads(req.to_json_str())
        assert parsed["document_id"] == "doc-test-0001"

    def test_linked_ids_match_decision(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        assert req.linked_decision_id == decision.decision_id
        assert req.linked_queue_entry_id == decision.queue_entry_id


class TestValidateReprocessingRequest:
    def test_valid_request_has_no_errors(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        errors = validate_reprocessing_request(req)
        assert errors == []

    def test_empty_reprocessing_reason_raises_error(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        req = ReprocessingRequest(**{**req.to_dict(), "reprocessing_reason": ""})
        errors = validate_reprocessing_request(req)
        assert any("reprocessing_reason" in e for e in errors)

    def test_missing_document_id_raises_error(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        req = ReprocessingRequest(**{**req.to_dict(), "document_id": ""})
        errors = validate_reprocessing_request(req)
        assert any("document_id" in e for e in errors)

    def test_missing_pipeline_run_id_raises_error(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        req = ReprocessingRequest(**{**req.to_dict(), "original_pipeline_run_id": ""})
        errors = validate_reprocessing_request(req)
        assert any("original_pipeline_run_id" in e for e in errors)

    def test_missing_linked_queue_entry_id_raises_error(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        req = ReprocessingRequest(**{**req.to_dict(), "linked_queue_entry_id": ""})
        errors = validate_reprocessing_request(req)
        assert any("linked_queue_entry_id" in e for e in errors)

    def test_missing_linked_decision_id_raises_error(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = _make_valid_reprocessing_request(decision)
        req = ReprocessingRequest(**{**req.to_dict(), "linked_decision_id": ""})
        errors = validate_reprocessing_request(req)
        assert any("linked_decision_id" in e for e in errors)


class TestBuildReprocessingRequest:
    def test_build_from_decision_populates_all_fields(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Test reprocessing reason.",
            suggested_document_class_hint="fda_warning_letter",
        )
        assert req.document_id == decision.document_id
        assert req.linked_decision_id == decision.decision_id
        assert req.linked_queue_entry_id == decision.queue_entry_id
        assert req.original_pipeline_run_id == decision.pipeline_run_id
        assert req.suggested_document_class_hint == "fda_warning_letter"

    def test_build_is_valid_after_construction(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Reprocessing needed.",
        )
        errors = validate_reprocessing_request(req)
        assert errors == []

    def test_build_reprocessing_request_id_is_unique(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req1 = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Reason 1.",
        )
        req2 = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Reason 2.",
        )
        assert req1.reprocessing_request_id != req2.reprocessing_request_id

    def test_build_respects_requested_at_override(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        ts = "2026-01-01T00:00:00+00:00"
        req = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Reason.",
            requested_at=ts,
        )
        assert req.requested_at == ts

    def test_optional_fields_default_to_none(self):
        decision = _make_valid_decision(
            DECISION_REQUEST_REPROCESSING,
            reprocessing_request_id="repr-0001",
        )
        req = build_reprocessing_request(
            decision=decision,
            reprocessing_reason="Reason.",
        )
        assert req.suggested_extraction_notes is None
        assert req.original_bundle_path is None


# ===========================================================================
# VI. Integration and Boundary Tests
# ===========================================================================


class TestE0ModuleBoundary:
    """Confirm E-0 modules do not import Bedrock, AWS, or UI dependencies."""

    def test_review_queue_schema_has_no_bedrock_sdk_imports(self):
        import src.schemas.review_queue as rq_mod
        source = Path(rq_mod.__file__).read_text()
        # Word "bedrock" appears in boundary documentation — what must NOT appear
        # is any actual Bedrock SDK import or runtime call.
        assert "boto3" not in source
        assert "botocore" not in source
        assert "import bedrock" not in source.lower()
        assert "bedrock_runtime" not in source.lower()

    def test_review_decision_schema_has_no_bedrock_sdk_imports(self):
        import src.schemas.review_decision as rd_mod
        source = Path(rd_mod.__file__).read_text()
        assert "boto3" not in source
        assert "botocore" not in source
        assert "import bedrock" not in source.lower()
        assert "bedrock_runtime" not in source.lower()

    def test_review_queue_pipeline_has_no_bedrock_sdk_imports(self):
        import src.pipelines.review_queue as rqp_mod
        source = Path(rqp_mod.__file__).read_text()
        assert "boto3" not in source
        assert "botocore" not in source
        assert "import bedrock" not in source.lower()
        assert "bedrock_runtime" not in source.lower()

    def test_review_queue_pipeline_has_no_ui_imports(self):
        import src.pipelines.review_queue as rqp_mod
        source = Path(rqp_mod.__file__).read_text()
        for ui_lib in ("flask", "django", "fastapi", "streamlit", "dash"):
            assert ui_lib not in source.lower()

    def test_review_queue_schema_is_importable_without_databricks(self):
        from src.schemas.review_queue import ReviewQueueArtifact, ReviewQueueEntry
        assert ReviewQueueArtifact is not None
        assert ReviewQueueEntry is not None

    def test_review_decision_schema_is_importable_without_databricks(self):
        from src.schemas.review_decision import ReviewDecision, ReprocessingRequest
        assert ReviewDecision is not None
        assert ReprocessingRequest is not None


class TestExistingPipelineBehaviorPreserved:
    """Confirm E-0 additions do not break existing pipeline behavior."""

    def test_classify_gold_still_importable(self):
        from src.pipelines.classify_gold import run_classify_gold
        assert callable(run_classify_gold)

    def test_handoff_report_still_importable(self):
        from src.pipelines.handoff_report import (
            build_handoff_batch_report,
            HandoffBatchReport,
        )
        assert callable(build_handoff_batch_report)

    def test_handoff_bundle_still_importable(self):
        from src.pipelines.handoff_bundle import (
            build_handoff_batch_manifest,
            HandoffBatchManifest,
        )
        assert callable(build_handoff_batch_manifest)

    def test_handoff_bundle_validation_still_importable(self):
        from src.pipelines.handoff_bundle_validation import (
            validate_handoff_bundle_from_manifest,
        )
        assert callable(validate_handoff_bundle_from_manifest)

    def test_bedrock_contract_still_importable(self):
        from src.schemas.bedrock_contract import (
            validate_export_payload,
            validate_quarantine_record,
        )
        assert callable(validate_export_payload)

    def test_export_handoff_still_importable(self):
        from src.pipelines.export_handoff import execute_export
        assert callable(execute_export)

    def test_domain_registry_still_importable(self):
        from src.utils.domain_registry import (
            is_domain_active,
            require_active_domain,
        )
        assert callable(is_domain_active)

    def test_all_three_domains_still_active(self):
        from src.utils.domain_registry import is_domain_active
        assert is_domain_active("fda_warning_letter") is True
        assert is_domain_active("cisa_advisory") is True
        assert is_domain_active("incident_report") is True


class TestE0ExampleFixtures:
    """Confirm example JSON fixtures are well-formed."""

    def test_review_queue_fixture_is_valid_json(self):
        fixture = Path("examples/expected_review_queue.json")
        parsed = json.loads(fixture.read_text())
        assert "review_queue_id" in parsed
        assert "queue_entries" in parsed
        assert parsed["schema_version"] == "v0.1.0"

    def test_review_decision_fixture_is_valid_json(self):
        fixture = Path("examples/expected_review_decision.json")
        parsed = json.loads(fixture.read_text())
        assert "decision_id" in parsed
        assert "decision" in parsed
        assert parsed["decision"] in ALL_REVIEW_DECISIONS

    def test_reprocessing_request_fixture_is_valid_json(self):
        fixture = Path("examples/expected_reprocessing_request.json")
        parsed = json.loads(fixture.read_text())
        assert "reprocessing_request_id" in parsed
        assert "document_id" in parsed
        assert "reprocessing_reason" in parsed

    def test_review_queue_fixture_has_correct_entry_count(self):
        fixture = Path("examples/expected_review_queue.json")
        parsed = json.loads(fixture.read_text())
        assert parsed["total_entries"] == len(parsed["queue_entries"])

    def test_review_decision_fixture_decision_is_known(self):
        fixture = Path("examples/expected_review_decision.json")
        parsed = json.loads(fixture.read_text())
        assert parsed["decision"] in ALL_REVIEW_DECISIONS

    def test_reprocessing_request_fixture_has_required_link_ids(self):
        fixture = Path("examples/expected_reprocessing_request.json")
        parsed = json.loads(fixture.read_text())
        assert len(parsed["linked_queue_entry_id"]) > 0
        assert len(parsed["linked_decision_id"]) > 0
