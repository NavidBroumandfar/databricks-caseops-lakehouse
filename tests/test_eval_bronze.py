"""
tests/test_eval_bronze.py — Unit tests for Bronze parse quality evaluation.

Tests cover:
  - compute_metrics with all-success records
  - compute_metrics with failed records triggering threshold violations
  - compute_metrics with near-empty parses (char_count < 100)
  - zero_char_count_rate detection
  - check_thresholds violation detection
  - flagged record identification logic
  - edge cases: single record, empty input
"""

import sys
from pathlib import Path

# Ensure evaluation module is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

import pytest
from eval_bronze import compute_metrics, check_thresholds, _identify_flagged_records


# ---------------------------------------------------------------------------
# Fixtures — minimal Bronze record dicts
# ---------------------------------------------------------------------------

def make_bronze(
    bronze_record_id: str = "br-1",
    document_id: str = "doc-1",
    parse_status: str = "success",
    char_count: int = 5000,
    parse_failure_reason: str | None = None,
) -> dict:
    r = {
        "bronze_record_id": bronze_record_id,
        "document_id": document_id,
        "parse_status": parse_status,
        "char_count": char_count,
        "schema_version": "v0.1.0",
        "pipeline_run_id": "local-run-001",
    }
    if parse_failure_reason:
        r["parse_failure_reason"] = parse_failure_reason
    return r


# ---------------------------------------------------------------------------
# compute_metrics — basic correctness
# ---------------------------------------------------------------------------

class TestComputeMetricsBronze:
    def test_all_success(self):
        records = [
            make_bronze("br-1", "doc-1", "success", 8000),
            make_bronze("br-2", "doc-2", "success", 6500),
            make_bronze("br-3", "doc-3", "success", 7200),
        ]
        m = compute_metrics(records)
        assert m["total_records"] == 3
        assert m["parse_success_rate"] == 1.0
        assert m["parse_failure_rate"] == 0.0
        assert m["parse_partial_rate"] == 0.0
        assert m["zero_char_count_rate"] == 0.0
        assert m["flagged_record_count"] == 0

    def test_mixed_statuses(self):
        records = [
            make_bronze("br-1", "doc-1", "success", 8000),
            make_bronze("br-2", "doc-2", "partial", 300),
            make_bronze("br-3", "doc-3", "failed", None, "PDF parse error"),
        ]
        m = compute_metrics(records)
        assert m["total_records"] == 3
        assert m["parse_success_rate"] == pytest.approx(1 / 3, abs=0.001)
        assert m["parse_partial_rate"] == pytest.approx(1 / 3, abs=0.001)
        assert m["parse_failure_rate"] == pytest.approx(1 / 3, abs=0.001)

    def test_failed_record_has_no_char_count(self):
        # Failed records should not contribute to char_count statistics
        records = [
            make_bronze("br-1", "doc-1", "failed", None, "Could not read PDF"),
            make_bronze("br-2", "doc-2", "success", 2000),
        ]
        m = compute_metrics(records)
        assert m["median_char_count"] == 2000
        assert m["p10_char_count"] is not None

    def test_empty_records_returns_error(self):
        m = compute_metrics([])
        assert "error" in m

    def test_single_record(self):
        records = [make_bronze("br-1", "doc-1", "success", 1500)]
        m = compute_metrics(records)
        assert m["total_records"] == 1
        assert m["parse_success_rate"] == 1.0
        assert m["median_char_count"] == 1500

    def test_zero_char_count_rate(self):
        records = [
            make_bronze("br-1", "doc-1", "success", 0),   # suspicious: zero despite success
            make_bronze("br-2", "doc-2", "success", 5000),
        ]
        m = compute_metrics(records)
        assert m["zero_char_count_rate"] == 0.5

    def test_near_empty_parses_in_p10(self):
        # All records have small char counts — p10 should be below threshold
        records = [make_bronze(f"br-{i}", f"doc-{i}", "success", 50 + i) for i in range(10)]
        m = compute_metrics(records)
        assert m["p10_char_count"] < 100


# ---------------------------------------------------------------------------
# check_thresholds
# ---------------------------------------------------------------------------

class TestCheckThresholdsBronze:
    def test_all_targets_met(self):
        records = [make_bronze(f"br-{i}", f"doc-{i}", "success", 2000 + i * 100) for i in range(5)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert warnings == []

    def test_low_parse_success_rate_triggers_warning(self):
        records = [
            make_bronze("br-1", "doc-1", "success", 5000),
            make_bronze("br-2", "doc-2", "failed", None, "err"),
            make_bronze("br-3", "doc-3", "failed", None, "err"),
            make_bronze("br-4", "doc-4", "failed", None, "err"),
            make_bronze("br-5", "doc-5", "failed", None, "err"),
        ]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("parse_success_rate" in w for w in warnings)
        assert any("parse_failure_rate" in w for w in warnings)

    def test_low_median_char_count_triggers_warning(self):
        records = [make_bronze(f"br-{i}", f"doc-{i}", "success", 50) for i in range(5)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("median_char_count" in w for w in warnings)

    def test_zero_char_count_triggers_warning(self):
        records = [
            make_bronze("br-1", "doc-1", "success", 0),
            make_bronze("br-2", "doc-2", "success", 5000),
        ]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("zero_char_count" in w for w in warnings)


# ---------------------------------------------------------------------------
# _identify_flagged_records
# ---------------------------------------------------------------------------

class TestFlaggedRecordsBronze:
    def test_failed_parse_is_flagged(self):
        records = [make_bronze("br-1", "doc-1", "failed", None, "Image-only PDF")]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("parse_status=failed" in r for r in flagged[0]["review_reasons"])

    def test_suspiciously_short_parse_is_flagged(self):
        records = [make_bronze("br-1", "doc-1", "success", 50)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("suspiciously_short_parse" in r for r in flagged[0]["review_reasons"])

    def test_zero_char_success_is_flagged(self):
        records = [make_bronze("br-1", "doc-1", "success", 0)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        reasons = flagged[0]["review_reasons"]
        assert any("zero_char_count" in r for r in reasons)

    def test_clean_record_not_flagged(self):
        records = [make_bronze("br-1", "doc-1", "success", 5000)]
        flagged = _identify_flagged_records(records)
        assert flagged == []

    def test_partial_with_ok_char_count_not_flagged(self):
        records = [make_bronze("br-1", "doc-1", "partial", 1500)]
        flagged = _identify_flagged_records(records)
        assert flagged == []
