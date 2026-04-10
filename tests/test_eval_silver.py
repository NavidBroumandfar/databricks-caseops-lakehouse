"""
tests/test_eval_silver.py — Unit tests for Silver extraction quality evaluation.

Tests cover:
  - schema_validity_rate, partial_validity_rate, invalid_rate computation
  - mean_field_coverage_pct and p25_field_coverage_pct
  - required_field_null_rate logic
  - validation_error_frequency ranking
  - check_thresholds violations
  - flagged record identification
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest
from eval_silver import (
    compute_metrics,
    check_thresholds,
    _identify_flagged_records,
    _compute_required_null_rate,
    _compute_error_frequency,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_silver(
    extraction_id: str = "ex-1",
    document_id: str = "doc-1",
    bronze_record_id: str = "br-1",
    validation_status: str = "valid",
    field_coverage_pct: float = 1.0,
    validation_errors: list[str] | None = None,
    extracted_fields: dict | None = None,
) -> dict:
    return {
        "extraction_id": extraction_id,
        "document_id": document_id,
        "bronze_record_id": bronze_record_id,
        "pipeline_run_id": "local-run-001",
        "extracted_at": "2025-01-01T00:00:00+00:00",
        "validation_status": validation_status,
        "field_coverage_pct": field_coverage_pct,
        "validation_errors": validation_errors or [],
        "extracted_fields": extracted_fields if extracted_fields is not None else {
            "issuing_office": "FDA Chicago",
            "recipient_company": "Acme Corp",
            "issue_date": "2025-01-01",
            "violation_type": ["21 CFR 211.160"],
            "corrective_action_requested": True,
        },
        "schema_version": "v0.1.0",
        "extraction_model": "local_rule_extractor/v1",
    }


# ---------------------------------------------------------------------------
# compute_metrics — basic correctness
# ---------------------------------------------------------------------------

class TestComputeMetricsSilver:
    def test_all_valid_records(self):
        records = [make_silver(f"ex-{i}", f"doc-{i}") for i in range(4)]
        m = compute_metrics(records)
        assert m["total_records"] == 4
        assert m["schema_validity_rate"] == 1.0
        assert m["invalid_rate"] == 0.0
        assert m["mean_field_coverage_pct"] == pytest.approx(1.0)
        assert m["flagged_record_count"] == 0

    def test_mixed_validity(self):
        records = [
            make_silver("ex-1", "doc-1", validation_status="valid", field_coverage_pct=1.0),
            make_silver("ex-2", "doc-2", validation_status="partial", field_coverage_pct=0.7),
            make_silver("ex-3", "doc-3", validation_status="invalid", field_coverage_pct=0.2,
                        validation_errors=["issuing_office: null"]),
            make_silver("ex-4", "doc-4", validation_status="valid", field_coverage_pct=0.9),
        ]
        m = compute_metrics(records)
        assert m["total_records"] == 4
        assert m["schema_validity_rate"] == pytest.approx(0.5)
        assert m["partial_validity_rate"] == pytest.approx(0.25)
        assert m["invalid_rate"] == pytest.approx(0.25)

    def test_mean_and_p25_coverage(self):
        # coverages: 0.5, 0.6, 0.8, 1.0  → mean = 0.725, p25 = 0.5
        records = [
            make_silver("ex-1", "doc-1", field_coverage_pct=0.5),
            make_silver("ex-2", "doc-2", field_coverage_pct=0.6),
            make_silver("ex-3", "doc-3", field_coverage_pct=0.8),
            make_silver("ex-4", "doc-4", field_coverage_pct=1.0),
        ]
        m = compute_metrics(records)
        assert m["mean_field_coverage_pct"] == pytest.approx(0.725, abs=0.01)
        assert m["p25_field_coverage_pct"] is not None
        assert m["p25_field_coverage_pct"] <= 0.6

    def test_empty_records(self):
        m = compute_metrics([])
        assert "error" in m

    def test_validation_error_frequency(self):
        records = [
            make_silver("ex-1", "doc-1", validation_status="invalid",
                        validation_errors=["issuing_office: null", "issue_date: null"]),
            make_silver("ex-2", "doc-2", validation_status="invalid",
                        validation_errors=["issuing_office: null"]),
            make_silver("ex-3", "doc-3", validation_status="valid"),
        ]
        m = compute_metrics(records)
        freq = m["validation_error_frequency"]
        assert freq[0]["error"] == "issuing_office: null"
        assert freq[0]["count"] == 2


# ---------------------------------------------------------------------------
# _compute_required_null_rate
# ---------------------------------------------------------------------------

class TestRequiredNullRate:
    def test_all_required_fields_present(self):
        records = [make_silver("ex-1", "doc-1", extracted_fields={
            "issuing_office": "FDA",
            "recipient_company": "Corp",
            "issue_date": "2025-01-01",
            "violation_type": ["21 CFR"],
            "corrective_action_requested": True,
        })]
        rate = _compute_required_null_rate(records)
        assert rate == 0.0

    def test_some_required_fields_null(self):
        records = [make_silver("ex-1", "doc-1", extracted_fields={
            "issuing_office": None,     # required, null
            "recipient_company": "Corp",
            "issue_date": None,          # required, null
            "violation_type": ["21 CFR"],
            "corrective_action_requested": True,
        })]
        rate = _compute_required_null_rate(records)
        # 2 nulls out of 5 required fields * 1 record = 2/5 = 0.4
        assert rate == pytest.approx(0.4, abs=0.01)

    def test_empty_extracted_fields(self):
        records = [make_silver("ex-1", "doc-1", extracted_fields={})]
        rate = _compute_required_null_rate(records)
        # All required fields absent → all null
        assert rate == 1.0

    def test_empty_records(self):
        assert _compute_required_null_rate([]) == 0.0


# ---------------------------------------------------------------------------
# check_thresholds
# ---------------------------------------------------------------------------

class TestCheckThresholdsSilver:
    def test_all_targets_met(self):
        records = [make_silver(f"ex-{i}", f"doc-{i}") for i in range(5)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert warnings == []

    def test_high_invalid_rate_triggers_warning(self):
        records = [
            make_silver("ex-1", "doc-1", validation_status="invalid", field_coverage_pct=0.1,
                        validation_errors=["issuing_office: null"]),
            make_silver("ex-2", "doc-2", validation_status="invalid", field_coverage_pct=0.1,
                        validation_errors=["recipient_company: null"]),
        ]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("invalid_rate" in w for w in warnings)

    def test_low_schema_validity_rate_triggers_warning(self):
        # Less than 80% valid
        records = [
            make_silver("ex-1", "doc-1", validation_status="invalid", field_coverage_pct=0.3,
                        validation_errors=["issue_date: null"]),
            make_silver("ex-2", "doc-2", validation_status="invalid", field_coverage_pct=0.3,
                        validation_errors=["issue_date: null"]),
            make_silver("ex-3", "doc-3", validation_status="valid", field_coverage_pct=1.0),
        ]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("schema_validity_rate" in w for w in warnings)


# ---------------------------------------------------------------------------
# _identify_flagged_records
# ---------------------------------------------------------------------------

class TestFlaggedRecordsSilver:
    def test_invalid_record_is_flagged(self):
        records = [make_silver("ex-1", "doc-1", validation_status="invalid",
                               field_coverage_pct=0.3, validation_errors=["issue_date: null"])]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("validation_status=invalid" in r for r in flagged[0]["review_reasons"])

    def test_low_coverage_record_is_flagged(self):
        records = [make_silver("ex-1", "doc-1", validation_status="partial",
                               field_coverage_pct=0.30)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("low_field_coverage" in r for r in flagged[0]["review_reasons"])

    def test_clean_valid_record_not_flagged(self):
        records = [make_silver("ex-1", "doc-1", validation_status="valid", field_coverage_pct=1.0)]
        flagged = _identify_flagged_records(records)
        assert flagged == []

    def test_partial_with_ok_coverage_not_flagged(self):
        records = [make_silver("ex-1", "doc-1", validation_status="partial", field_coverage_pct=0.7)]
        flagged = _identify_flagged_records(records)
        assert flagged == []
