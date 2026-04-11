"""
tests/test_b4_handoff_report.py — B-4 export outcome observability and handoff reporting tests.

B-4 purpose: verify that the Gold → Bedrock export path produces structured,
batch-level handoff outcome summaries with explicit outcome categories, reason
codes, and counts suitable for downstream readiness review.

What these tests cover:
  - Outcome category constants exist and are strings
  - Reason code constants exist and are strings
  - derive_outcome() maps all four export path states to correct (category, reason)
  - build_handoff_batch_report() correctly counts exported / quarantined /
    contract_blocked / skipped_not_export_ready
  - build_handoff_batch_report() correctly populates outcome_distribution and
    reason_code_distribution
  - build_handoff_batch_report() correctly tracks contract_blocked_document_ids
    and quarantined_document_ids
  - HandoffBatchReport.to_dict() is JSON-serializable and contains all expected keys
  - write_handoff_report() produces JSON and text artifacts on disk
  - format_handoff_report_text() contains all expected sections
  - Integration: run_classify_gold() with report_dir writes a correct batch report
  - Integration: per-record summaries from run_classify_gold() include outcome_category
    and outcome_reason for all record types
  - handoff_report.py contains no AWS/Bedrock SDK imports

What these tests do NOT imply:
  - No live Bedrock/AWS integration exists or is required
  - No S3, boto3, or Bedrock SDK usage
  - These tests validate local module structure and reporting behavior only

Authoritative contract: docs/bedrock-handoff-contract.md
B-4 reporting module: src/pipelines/handoff_report.py
Pipeline integration: src/pipelines/classify_gold.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.pipelines.handoff_report import (
    ALL_OUTCOME_CATEGORIES,
    ALL_REASON_CODES,
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
    REASON_CONTRACT_VALIDATION_FAILED,
    REASON_EXPORT_NOT_ATTEMPTED,
    REASON_NONE,
    REASON_ROUTING_QUARANTINE,
    HandoffBatchReport,
    build_handoff_batch_report,
    derive_outcome,
    format_handoff_report_text,
    write_handoff_report,
)
from src.pipelines.classify_gold import run_classify_gold


# ---------------------------------------------------------------------------
# Silver fixture helpers (for integration tests via run_classify_gold)
# ---------------------------------------------------------------------------


def write_silver_fixture(tmp_path: Path, record: dict) -> Path:
    """Write a Silver fixture JSON to a temp directory and return the path."""
    p = tmp_path / f"{record['document_id']}.json"
    p.write_text(json.dumps(record), encoding="utf-8")
    return p


def make_valid_silver_record(doc_id: str = "b4aa0001-0000-0000-0000-000000000001") -> dict:
    """Silver record the FDA classifier classifies as export-ready."""
    return {
        "document_id": doc_id,
        "bronze_record_id": f"b4bb{doc_id[4:]}",
        "extraction_id": f"b4cc{doc_id[4:]}",
        "pipeline_run_id": "local-run-b4-test",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.85,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "TestCorp Pharma Inc.",
            "issue_date": "2024-03-15",
            "violation_type": ["Current Good Manufacturing Practice"],
            "corrective_action_requested": True,
        },
    }


def make_contract_blocking_silver_record(
    doc_id: str = "b4aa0002-0000-0000-0000-000000000001",
) -> dict:
    """Silver record that produces an export-ready Gold but fails B-1 contract validation."""
    return {
        "document_id": doc_id,
        "bronze_record_id": f"b4bb{doc_id[4:]}",
        "extraction_id": f"b4cc{doc_id[4:]}",
        "pipeline_run_id": "local-run-b4-block",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.80,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Compliance",
            "recipient_company": "BlockedCorp Inc.",
            "issue_date": "2024-03-15",
            "violation_type": [],        # empty — fails contract §4.4
            "corrective_action_requested": None,  # null — fails contract §4.4
        },
    }


def make_quarantine_silver_record(
    doc_id: str = "b4aa0003-0000-0000-0000-000000000001",
) -> dict:
    """Silver record with no signals → classified as 'unknown' → quarantine."""
    return {
        "document_id": doc_id,
        "bronze_record_id": f"b4bb{doc_id[4:]}",
        "extraction_id": f"b4cc{doc_id[4:]}",
        "pipeline_run_id": "local-run-b4-quarantine",
        "document_class_hint": None,
        "validation_status": "valid",
        "field_coverage_pct": 0.10,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {},
    }


def make_ineligible_silver_record(
    doc_id: str = "b4aa0004-0000-0000-0000-000000000001",
) -> dict:
    """Silver record with validation_status='invalid' — skipped before classification."""
    return {
        "document_id": doc_id,
        "bronze_record_id": f"b4bb{doc_id[4:]}",
        "extraction_id": f"b4cc{doc_id[4:]}",
        "pipeline_run_id": "local-run-b4-ineligible",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "invalid",
        "field_coverage_pct": 0.0,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {},
    }


# ---------------------------------------------------------------------------
# Outcome category constant tests
# ---------------------------------------------------------------------------


class TestOutcomeCategoryConstants:
    def test_exported_is_string(self):
        assert isinstance(OUTCOME_EXPORTED, str)
        assert OUTCOME_EXPORTED == "exported"

    def test_quarantined_is_string(self):
        assert isinstance(OUTCOME_QUARANTINED, str)
        assert OUTCOME_QUARANTINED == "quarantined"

    def test_contract_blocked_is_string(self):
        assert isinstance(OUTCOME_CONTRACT_BLOCKED, str)
        assert OUTCOME_CONTRACT_BLOCKED == "contract_blocked"

    def test_skipped_not_export_ready_is_string(self):
        assert isinstance(OUTCOME_SKIPPED_NOT_EXPORT_READY, str)
        assert OUTCOME_SKIPPED_NOT_EXPORT_READY == "skipped_not_export_ready"

    def test_all_outcome_categories_contains_four_entries(self):
        assert len(ALL_OUTCOME_CATEGORIES) == 4

    def test_all_outcome_categories_contains_expected_values(self):
        assert OUTCOME_EXPORTED in ALL_OUTCOME_CATEGORIES
        assert OUTCOME_QUARANTINED in ALL_OUTCOME_CATEGORIES
        assert OUTCOME_CONTRACT_BLOCKED in ALL_OUTCOME_CATEGORIES
        assert OUTCOME_SKIPPED_NOT_EXPORT_READY in ALL_OUTCOME_CATEGORIES


# ---------------------------------------------------------------------------
# Reason code constant tests
# ---------------------------------------------------------------------------


class TestReasonCodeConstants:
    def test_none_is_string(self):
        assert isinstance(REASON_NONE, str)
        assert REASON_NONE == "none"

    def test_routing_quarantine_is_string(self):
        assert isinstance(REASON_ROUTING_QUARANTINE, str)
        assert REASON_ROUTING_QUARANTINE == "routing_quarantine"

    def test_contract_validation_failed_is_string(self):
        assert isinstance(REASON_CONTRACT_VALIDATION_FAILED, str)
        assert REASON_CONTRACT_VALIDATION_FAILED == "contract_validation_failed"

    def test_export_not_attempted_is_string(self):
        assert isinstance(REASON_EXPORT_NOT_ATTEMPTED, str)
        assert REASON_EXPORT_NOT_ATTEMPTED == "export_not_attempted"

    def test_all_reason_codes_contains_four_entries(self):
        assert len(ALL_REASON_CODES) == 4

    def test_all_reason_codes_contains_expected_values(self):
        assert REASON_NONE in ALL_REASON_CODES
        assert REASON_ROUTING_QUARANTINE in ALL_REASON_CODES
        assert REASON_CONTRACT_VALIDATION_FAILED in ALL_REASON_CODES
        assert REASON_EXPORT_NOT_ATTEMPTED in ALL_REASON_CODES


# ---------------------------------------------------------------------------
# derive_outcome tests
# ---------------------------------------------------------------------------


class TestDeriveOutcome:
    def test_exported_case(self):
        """export_ready=True, no errors → exported / none."""
        cat, reason = derive_outcome(
            export_ready=True,
            routing_label="regulatory_review",
            contract_validation_errors=[],
        )
        assert cat == OUTCOME_EXPORTED
        assert reason == REASON_NONE

    def test_contract_blocked_case(self):
        """export_ready=False, contract errors present → contract_blocked / contract_validation_failed."""
        cat, reason = derive_outcome(
            export_ready=False,
            routing_label="regulatory_review",
            contract_validation_errors=["Missing required field: 'violation_type'"],
        )
        assert cat == OUTCOME_CONTRACT_BLOCKED
        assert reason == REASON_CONTRACT_VALIDATION_FAILED

    def test_quarantined_case(self):
        """export_ready=False, no errors, routing_label='quarantine' → quarantined / routing_quarantine."""
        cat, reason = derive_outcome(
            export_ready=False,
            routing_label="quarantine",
            contract_validation_errors=[],
        )
        assert cat == OUTCOME_QUARANTINED
        assert reason == REASON_ROUTING_QUARANTINE

    def test_skipped_not_export_ready_case(self):
        """export_ready=False, no errors, routing_label != 'quarantine' → skipped_not_export_ready."""
        cat, reason = derive_outcome(
            export_ready=False,
            routing_label="regulatory_review",
            contract_validation_errors=[],
        )
        assert cat == OUTCOME_SKIPPED_NOT_EXPORT_READY
        assert reason == REASON_EXPORT_NOT_ATTEMPTED

    def test_contract_blocked_takes_priority_over_quarantine(self):
        """
        If both contract_validation_errors are present AND routing_label='quarantine',
        contract_blocked wins. (Edge case: quarantine-routed record with contract errors.)
        """
        cat, reason = derive_outcome(
            export_ready=False,
            routing_label="quarantine",
            contract_validation_errors=["Some error"],
        )
        assert cat == OUTCOME_CONTRACT_BLOCKED
        assert reason == REASON_CONTRACT_VALIDATION_FAILED

    def test_exported_ignores_routing_label(self):
        """export_ready=True is the only exported signal — routing_label doesn't matter."""
        cat, reason = derive_outcome(
            export_ready=True,
            routing_label="some_label",
            contract_validation_errors=[],
        )
        assert cat == OUTCOME_EXPORTED

    def test_return_types_are_strings(self):
        """derive_outcome always returns two strings."""
        for export_ready in [True, False]:
            for routing_label in ["regulatory_review", "quarantine"]:
                for errors in [[], ["error"]]:
                    cat, reason = derive_outcome(export_ready, routing_label, errors)
                    assert isinstance(cat, str)
                    assert isinstance(reason, str)


# ---------------------------------------------------------------------------
# build_handoff_batch_report tests
# ---------------------------------------------------------------------------


def _make_exported_summary(doc_id: str = "doc-001") -> dict:
    return {
        "document_id": doc_id,
        "routing_label": "regulatory_review",
        "export_ready": True,
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_EXPORTED,
        "outcome_reason": REASON_NONE,
    }


def _make_quarantined_summary(doc_id: str = "doc-002") -> dict:
    return {
        "document_id": doc_id,
        "routing_label": "quarantine",
        "export_ready": False,
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_QUARANTINED,
        "outcome_reason": REASON_ROUTING_QUARANTINE,
    }


def _make_blocked_summary(doc_id: str = "doc-003") -> dict:
    return {
        "document_id": doc_id,
        "routing_label": "regulatory_review",
        "export_ready": False,
        "contract_validation_errors": ["'violation_type' must be a non-empty array"],
        "outcome_category": OUTCOME_CONTRACT_BLOCKED,
        "outcome_reason": REASON_CONTRACT_VALIDATION_FAILED,
    }


def _make_skipped_summary(doc_id: str = "doc-004") -> dict:
    return {
        "document_id": doc_id,
        "routing_label": "regulatory_review",
        "export_ready": False,
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_SKIPPED_NOT_EXPORT_READY,
        "outcome_reason": REASON_EXPORT_NOT_ATTEMPTED,
    }


class TestBuildHandoffBatchReport:
    def test_single_exported_record(self):
        """One exported record → total_exported=1, others=0."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
        )
        assert report.total_exported == 1
        assert report.total_quarantined == 0
        assert report.total_contract_blocked == 0
        assert report.total_skipped_not_export_ready == 0

    def test_single_quarantined_record(self):
        """One quarantined record → total_quarantined=1, others=0."""
        report = build_handoff_batch_report(
            summaries=[_make_quarantined_summary()],
            pipeline_run_id="run-001",
        )
        assert report.total_quarantined == 1
        assert report.total_exported == 0
        assert report.total_contract_blocked == 0
        assert report.total_skipped_not_export_ready == 0

    def test_single_contract_blocked_record(self):
        """One blocked record → total_contract_blocked=1, others=0."""
        report = build_handoff_batch_report(
            summaries=[_make_blocked_summary()],
            pipeline_run_id="run-001",
        )
        assert report.total_contract_blocked == 1
        assert report.total_exported == 0
        assert report.total_quarantined == 0
        assert report.total_skipped_not_export_ready == 0

    def test_single_skipped_record(self):
        """One skipped record → total_skipped_not_export_ready=1, others=0."""
        report = build_handoff_batch_report(
            summaries=[_make_skipped_summary()],
            pipeline_run_id="run-001",
        )
        assert report.total_skipped_not_export_ready == 1
        assert report.total_exported == 0
        assert report.total_quarantined == 0
        assert report.total_contract_blocked == 0

    def test_mixed_batch_counts(self):
        """Mixed batch: correct counts for all four outcome types."""
        summaries = [
            _make_exported_summary("doc-e1"),
            _make_exported_summary("doc-e2"),
            _make_quarantined_summary("doc-q1"),
            _make_blocked_summary("doc-b1"),
            _make_blocked_summary("doc-b2"),
            _make_skipped_summary("doc-s1"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-mix")

        assert report.total_exported == 2
        assert report.total_quarantined == 1
        assert report.total_contract_blocked == 2
        assert report.total_skipped_not_export_ready == 1
        assert report.total_eligible == 6

    def test_total_eligible_equals_len_summaries(self):
        """total_eligible is always len(summaries)."""
        summaries = [
            _make_exported_summary(),
            _make_quarantined_summary(),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")
        assert report.total_eligible == 2

    def test_total_export_attempts(self):
        """total_export_attempts = exported + contract_blocked only."""
        summaries = [
            _make_exported_summary("e1"),
            _make_exported_summary("e2"),
            _make_blocked_summary("b1"),
            _make_quarantined_summary("q1"),
            _make_skipped_summary("s1"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")
        # Exported (2) + blocked (1) = 3; quarantine and skipped are not attempts
        assert report.total_export_attempts == 3

    def test_total_records_processed_defaults(self):
        """total_records_processed defaults to len(summaries) + total_ineligible_skipped."""
        summaries = [_make_exported_summary()]
        report = build_handoff_batch_report(
            summaries=summaries,
            pipeline_run_id="run-001",
            total_ineligible_skipped=2,
        )
        assert report.total_records_processed == 3  # 1 eligible + 2 ineligible

    def test_total_records_processed_explicit(self):
        """total_records_processed can be explicitly set."""
        summaries = [_make_exported_summary()]
        report = build_handoff_batch_report(
            summaries=summaries,
            pipeline_run_id="run-001",
            total_records_processed=10,
        )
        assert report.total_records_processed == 10

    def test_ineligible_skipped_count(self):
        """total_ineligible_skipped is stored correctly."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
            total_ineligible_skipped=3,
        )
        assert report.total_ineligible_skipped == 3

    def test_outcome_distribution_keys_present(self):
        """outcome_distribution contains all four outcome category keys."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
        )
        for cat in ALL_OUTCOME_CATEGORIES:
            assert cat in report.outcome_distribution

    def test_outcome_distribution_values_correct(self):
        """outcome_distribution values match the individual counts."""
        summaries = [
            _make_exported_summary("e1"),
            _make_quarantined_summary("q1"),
            _make_blocked_summary("b1"),
            _make_skipped_summary("s1"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")

        assert report.outcome_distribution[OUTCOME_EXPORTED] == 1
        assert report.outcome_distribution[OUTCOME_QUARANTINED] == 1
        assert report.outcome_distribution[OUTCOME_CONTRACT_BLOCKED] == 1
        assert report.outcome_distribution[OUTCOME_SKIPPED_NOT_EXPORT_READY] == 1

    def test_reason_code_distribution_keys_present(self):
        """reason_code_distribution contains all four reason code keys."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
        )
        for code in ALL_REASON_CODES:
            assert code in report.reason_code_distribution

    def test_reason_code_distribution_values_correct(self):
        """reason_code_distribution values are consistent with outcomes."""
        summaries = [
            _make_exported_summary("e1"),
            _make_exported_summary("e2"),
            _make_quarantined_summary("q1"),
            _make_blocked_summary("b1"),
            _make_skipped_summary("s1"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")

        assert report.reason_code_distribution[REASON_NONE] == 2
        assert report.reason_code_distribution[REASON_ROUTING_QUARANTINE] == 1
        assert report.reason_code_distribution[REASON_CONTRACT_VALIDATION_FAILED] == 1
        assert report.reason_code_distribution[REASON_EXPORT_NOT_ATTEMPTED] == 1

    def test_contract_blocked_document_ids(self):
        """contract_blocked_document_ids lists IDs of blocked records in order."""
        summaries = [
            _make_exported_summary("e-good"),
            _make_blocked_summary("b-bad-1"),
            _make_blocked_summary("b-bad-2"),
            _make_quarantined_summary("q-quaran"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")

        assert "b-bad-1" in report.contract_blocked_document_ids
        assert "b-bad-2" in report.contract_blocked_document_ids
        assert "e-good" not in report.contract_blocked_document_ids
        assert "q-quaran" not in report.contract_blocked_document_ids

    def test_quarantined_document_ids(self):
        """quarantined_document_ids lists IDs of quarantined records."""
        summaries = [
            _make_quarantined_summary("q-001"),
            _make_quarantined_summary("q-002"),
            _make_exported_summary("e-good"),
        ]
        report = build_handoff_batch_report(summaries=summaries, pipeline_run_id="run-001")

        assert "q-001" in report.quarantined_document_ids
        assert "q-002" in report.quarantined_document_ids
        assert "e-good" not in report.quarantined_document_ids

    def test_empty_batch(self):
        """Empty summaries list produces a valid report with all zeros."""
        report = build_handoff_batch_report(summaries=[], pipeline_run_id="run-empty")

        assert report.total_eligible == 0
        assert report.total_exported == 0
        assert report.total_quarantined == 0
        assert report.total_contract_blocked == 0
        assert report.total_skipped_not_export_ready == 0
        assert report.total_export_attempts == 0
        assert report.contract_blocked_document_ids == []
        assert report.quarantined_document_ids == []

    def test_pipeline_run_id_stored(self):
        """pipeline_run_id is stored on the report."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="my-run-id-xyz",
        )
        assert report.pipeline_run_id == "my-run-id-xyz"

    def test_batch_processed_at_is_set(self):
        """batch_processed_at is populated (defaults to UTC now)."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
        )
        assert report.batch_processed_at is not None
        assert len(report.batch_processed_at) > 0

    def test_batch_processed_at_explicit(self):
        """Explicit batch_processed_at is used when provided."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
            batch_processed_at="2024-06-01T12:00:00+00:00",
        )
        assert report.batch_processed_at == "2024-06-01T12:00:00+00:00"


# ---------------------------------------------------------------------------
# HandoffBatchReport.to_dict tests
# ---------------------------------------------------------------------------


class TestHandoffBatchReportToDict:
    def test_to_dict_is_json_serializable(self):
        """to_dict() must produce a JSON-serializable dict (no datetime objects, etc.)."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary(), _make_quarantined_summary()],
            pipeline_run_id="run-001",
        )
        d = report.to_dict()
        # Should not raise
        json_str = json.dumps(d)
        assert isinstance(json_str, str)

    def test_to_dict_contains_all_expected_keys(self):
        """to_dict() must contain all expected top-level keys."""
        expected_keys = {
            "pipeline_run_id",
            "batch_processed_at",
            "total_records_processed",
            "total_ineligible_skipped",
            "total_eligible",
            "total_export_attempts",
            "total_exported",
            "total_quarantined",
            "total_contract_blocked",
            "total_skipped_not_export_ready",
            "outcome_distribution",
            "reason_code_distribution",
            "contract_blocked_document_ids",
            "quarantined_document_ids",
        }
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-001",
        )
        d = report.to_dict()
        for key in expected_keys:
            assert key in d, f"Expected key '{key}' missing from to_dict() output"

    def test_to_dict_values_match_report_fields(self):
        """to_dict() values must match the HandoffBatchReport field values."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary(), _make_quarantined_summary("q-doc")],
            pipeline_run_id="run-check",
            total_ineligible_skipped=1,
        )
        d = report.to_dict()

        assert d["pipeline_run_id"] == report.pipeline_run_id
        assert d["total_exported"] == report.total_exported
        assert d["total_quarantined"] == report.total_quarantined
        assert d["total_eligible"] == report.total_eligible
        assert d["quarantined_document_ids"] == report.quarantined_document_ids


# ---------------------------------------------------------------------------
# write_handoff_report tests
# ---------------------------------------------------------------------------


class TestWriteHandoffReport:
    def test_writes_json_and_text_artifacts(self, tmp_path: Path):
        """write_handoff_report must produce both a .json and a .txt artifact."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-write-test",
        )
        json_path, text_path = write_handoff_report(report, tmp_path / "reports")

        assert json_path.exists(), f"JSON artifact not found at {json_path}"
        assert text_path.exists(), f"Text artifact not found at {text_path}"

    def test_json_artifact_is_valid_json(self, tmp_path: Path):
        """The written JSON artifact must be valid JSON."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-json-valid",
        )
        json_path, _ = write_handoff_report(report, tmp_path / "reports")

        content = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(content, dict)

    def test_json_artifact_contains_expected_keys(self, tmp_path: Path):
        """The JSON artifact must contain all HandoffBatchReport field keys."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary(), _make_quarantined_summary()],
            pipeline_run_id="run-keys-test",
        )
        json_path, _ = write_handoff_report(report, tmp_path / "reports")

        content = json.loads(json_path.read_text(encoding="utf-8"))
        assert "total_exported" in content
        assert "total_quarantined" in content
        assert "total_contract_blocked" in content
        assert "total_skipped_not_export_ready" in content
        assert "outcome_distribution" in content
        assert "reason_code_distribution" in content

    def test_json_artifact_values_match_report(self, tmp_path: Path):
        """JSON artifact values must reflect the actual report counts."""
        summaries = [
            _make_exported_summary("e1"),
            _make_quarantined_summary("q1"),
            _make_blocked_summary("b1"),
        ]
        report = build_handoff_batch_report(
            summaries=summaries,
            pipeline_run_id="run-values-test",
        )
        json_path, _ = write_handoff_report(report, tmp_path / "reports")

        content = json.loads(json_path.read_text(encoding="utf-8"))
        assert content["total_exported"] == 1
        assert content["total_quarantined"] == 1
        assert content["total_contract_blocked"] == 1
        assert "b1" in content["contract_blocked_document_ids"]

    def test_creates_output_directory(self, tmp_path: Path):
        """write_handoff_report must create the output directory if it does not exist."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-mkdir-test",
        )
        output_dir = tmp_path / "new" / "nested" / "reports"
        assert not output_dir.exists()

        write_handoff_report(report, output_dir)

        assert output_dir.exists()

    def test_artifact_names_contain_run_id(self, tmp_path: Path):
        """Artifact file names must include the pipeline_run_id."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="my-specific-run-id",
        )
        json_path, text_path = write_handoff_report(report, tmp_path / "reports")

        assert "my-specific-run-id" in json_path.name
        assert "my-specific-run-id" in text_path.name


# ---------------------------------------------------------------------------
# format_handoff_report_text tests
# ---------------------------------------------------------------------------


class TestFormatHandoffReportText:
    def test_contains_report_header(self):
        """Text report must start with the B-4 report header."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-text-test",
        )
        text = format_handoff_report_text(report)
        assert "Handoff Batch Report" in text
        assert "B-4" in text

    def test_contains_pipeline_run_id(self):
        """Text report must include the pipeline run ID."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-text-id-check",
        )
        text = format_handoff_report_text(report)
        assert "run-text-id-check" in text

    def test_contains_all_count_sections(self):
        """Text report must include record count, outcome count, and distribution sections."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary(), _make_quarantined_summary()],
            pipeline_run_id="run-sections-test",
        )
        text = format_handoff_report_text(report)
        assert "Record Counts" in text
        assert "Outcome Counts" in text
        assert "Outcome Distribution" in text
        assert "Reason Code Distribution" in text

    def test_contract_blocked_ids_appear_when_present(self):
        """Text report must list contract-blocked document IDs when present."""
        report = build_handoff_batch_report(
            summaries=[_make_blocked_summary("b-doc-12345")],
            pipeline_run_id="run-blocked-ids-test",
        )
        text = format_handoff_report_text(report)
        assert "b-doc-12345" in text

    def test_quarantined_ids_appear_when_present(self):
        """Text report must list quarantined document IDs when present."""
        report = build_handoff_batch_report(
            summaries=[_make_quarantined_summary("q-doc-67890")],
            pipeline_run_id="run-quarantined-ids-test",
        )
        text = format_handoff_report_text(report)
        assert "q-doc-67890" in text

    def test_no_blocked_section_when_none_blocked(self):
        """Text report must not include the blocked IDs section when no records are blocked."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-no-blocked",
        )
        text = format_handoff_report_text(report)
        assert "Contract Blocked Document IDs" not in text

    def test_returns_string(self):
        """format_handoff_report_text must return a str."""
        report = build_handoff_batch_report(
            summaries=[_make_exported_summary()],
            pipeline_run_id="run-type-test",
        )
        text = format_handoff_report_text(report)
        assert isinstance(text, str)


# ---------------------------------------------------------------------------
# Integration: run_classify_gold emits outcome fields in per-record summaries
# ---------------------------------------------------------------------------


class TestRunClassifyGoldOutcomeFields:
    """
    Verify that run_classify_gold() per-record summaries include outcome_category
    and outcome_reason for all four outcome types.
    """

    def test_exported_record_has_correct_outcome_fields(self, tmp_path: Path):
        """Valid Silver → exported outcome_category and none reason."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
        )

        assert len(summaries) == 1
        s = summaries[0]
        assert "outcome_category" in s, "outcome_category must be in per-record summary"
        assert "outcome_reason" in s, "outcome_reason must be in per-record summary"
        assert s["outcome_category"] == OUTCOME_EXPORTED
        assert s["outcome_reason"] == REASON_NONE

    def test_quarantine_record_has_correct_outcome_fields(self, tmp_path: Path):
        """Quarantined Silver → quarantined outcome_category and routing_quarantine reason."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
        )

        assert len(summaries) == 1
        s = summaries[0]
        assert s["outcome_category"] == OUTCOME_QUARANTINED
        assert s["outcome_reason"] == REASON_ROUTING_QUARANTINE

    def test_contract_blocked_record_has_correct_outcome_fields(self, tmp_path: Path):
        """Contract-blocking Silver → contract_blocked outcome_category."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
        )

        assert len(summaries) == 1
        s = summaries[0]
        assert s["outcome_category"] == OUTCOME_CONTRACT_BLOCKED
        assert s["outcome_reason"] == REASON_CONTRACT_VALIDATION_FAILED

    def test_outcome_fields_present_for_all_records_in_mixed_batch(self, tmp_path: Path):
        """Every per-record summary must have outcome_category and outcome_reason."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        write_silver_fixture(silver_dir, make_valid_silver_record("b4aa0001-0000-0000-0000-000000000001"))
        write_silver_fixture(silver_dir, make_quarantine_silver_record("b4aa0003-0000-0000-0000-000000000001"))
        write_silver_fixture(silver_dir, make_contract_blocking_silver_record("b4aa0002-0000-0000-0000-000000000001"))

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
        )

        assert len(summaries) == 3
        for s in summaries:
            assert "outcome_category" in s
            assert "outcome_reason" in s
            assert s["outcome_category"] in ALL_OUTCOME_CATEGORIES
            assert s["outcome_reason"] in ALL_REASON_CODES


# ---------------------------------------------------------------------------
# Integration: run_classify_gold with report_dir writes batch report
# ---------------------------------------------------------------------------


class TestRunClassifyGoldWithReportDir:
    """
    Verify that run_classify_gold() writes a correct HandoffBatchReport
    when report_dir is provided.
    """

    def test_report_dir_writes_json_artifact(self, tmp_path: Path):
        """Providing report_dir must produce a JSON report artifact."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        report_dir = tmp_path / "reports"
        write_silver_fixture(silver_dir, make_valid_silver_record())

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            report_dir=str(report_dir),
        )

        report_files = list(report_dir.glob("handoff_report_*.json"))
        assert len(report_files) == 1, (
            f"Expected one JSON report file; found {report_files}"
        )

    def test_report_dir_writes_text_artifact(self, tmp_path: Path):
        """Providing report_dir must produce a text report artifact."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        report_dir = tmp_path / "reports"
        write_silver_fixture(silver_dir, make_valid_silver_record())

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            report_dir=str(report_dir),
        )

        text_files = list(report_dir.glob("handoff_report_*.txt"))
        assert len(text_files) == 1

    def test_report_json_reflects_real_outcomes(self, tmp_path: Path):
        """The JSON report must reflect the actual outcomes from the pipeline run."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        report_dir = tmp_path / "reports"

        write_silver_fixture(silver_dir, make_valid_silver_record("b4aa0001-0000-0000-0000-000000000001"))
        write_silver_fixture(silver_dir, make_quarantine_silver_record("b4aa0003-0000-0000-0000-000000000001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            report_dir=str(report_dir),
        )

        report_file = next(report_dir.glob("handoff_report_*.json"))
        report_data = json.loads(report_file.read_text(encoding="utf-8"))

        assert report_data["total_exported"] == 1
        assert report_data["total_quarantined"] == 1
        assert report_data["total_contract_blocked"] == 0
        assert report_data["total_eligible"] == 2

    def test_report_not_written_when_report_dir_omitted(self, tmp_path: Path):
        """When report_dir is not provided, no report is written."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        write_silver_fixture(silver_dir, make_valid_silver_record())

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            # no report_dir
        )

        # No report directory should have been created
        report_dir = tmp_path / "reports"
        assert not report_dir.exists()

    def test_report_counts_ineligible_skipped(self, tmp_path: Path):
        """
        When ineligible Silver records are present, the batch report reflects
        total_ineligible_skipped correctly.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        report_dir = tmp_path / "reports"

        write_silver_fixture(silver_dir, make_valid_silver_record("b4aa0001-0000-0000-0000-000000000001"))
        write_silver_fixture(silver_dir, make_ineligible_silver_record("b4aa0004-0000-0000-0000-000000000001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            report_dir=str(report_dir),
        )

        report_file = next(report_dir.glob("handoff_report_*.json"))
        report_data = json.loads(report_file.read_text(encoding="utf-8"))

        assert report_data["total_records_processed"] == 2
        assert report_data["total_ineligible_skipped"] == 1
        assert report_data["total_eligible"] == 1
        assert report_data["total_exported"] == 1

    def test_report_contract_blocked_ids_match_actual_blocks(self, tmp_path: Path):
        """contract_blocked_document_ids must list the actual blocked records."""
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        report_dir = tmp_path / "reports"

        blocked_record = make_contract_blocking_silver_record("b4aa0002-0000-0000-0000-000000000001")
        write_silver_fixture(silver_dir, blocked_record)

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "gold" / "exports"),
            report_dir=str(report_dir),
        )

        report_file = next(report_dir.glob("handoff_report_*.json"))
        report_data = json.loads(report_file.read_text(encoding="utf-8"))

        assert "b4aa0002-0000-0000-0000-000000000001" in report_data["contract_blocked_document_ids"]


# ---------------------------------------------------------------------------
# Module boundary: handoff_report.py has no AWS/Bedrock imports
# ---------------------------------------------------------------------------


class TestHandoffReportModuleBoundary:
    def test_handoff_report_has_no_aws_or_bedrock_imports(self):
        """
        handoff_report.py must not import any AWS/Bedrock SDK packages.
        B-4 reporting is strictly local aggregation and artifact writing — no
        live integration is permitted.
        """
        report_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "handoff_report.py"
        )
        source = report_path.read_text(encoding="utf-8")
        import_lines = [
            line.strip()
            for line in source.splitlines()
            if re.match(r"^\s*(import|from)\s+", line)
        ]

        forbidden_packages = ["boto3", "botocore", "bedrock_runtime", "sagemaker", "awscrt"]
        for pkg in forbidden_packages:
            for imp_line in import_lines:
                assert pkg not in imp_line.lower(), (
                    f"handoff_report.py must not import '{pkg}' — "
                    f"no live Bedrock/AWS integration in B-4. Found: {imp_line!r}"
                )

    def test_handoff_report_does_not_import_bedrock_contract(self):
        """
        handoff_report.py owns reporting logic only.
        It must not import from bedrock_contract — contract validation
        is owned by export_handoff.py.
        """
        report_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "handoff_report.py"
        )
        source = report_path.read_text(encoding="utf-8")
        import_lines = [
            line.strip()
            for line in source.splitlines()
            if re.match(r"^\s*(import|from)\s+", line)
        ]
        for imp_line in import_lines:
            assert "bedrock_contract" not in imp_line, (
                "handoff_report.py must not import from bedrock_contract — "
                f"contract validation is owned by export_handoff.py. Found: {imp_line!r}"
            )

    def test_classify_gold_imports_handoff_report(self):
        """classify_gold.py must import from handoff_report for B-4 integration."""
        pipeline_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "classify_gold.py"
        )
        source = pipeline_path.read_text(encoding="utf-8")
        assert "from src.pipelines.handoff_report import" in source, (
            "classify_gold.py must import from handoff_report (B-4 integration)"
        )
        assert "derive_outcome" in source, (
            "classify_gold.py must call derive_outcome (B-4 outcome derivation)"
        )
        assert "build_handoff_batch_report" in source, (
            "classify_gold.py must call build_handoff_batch_report (B-4 batch reporting)"
        )
