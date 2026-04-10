"""
tests/test_eval_gold.py — Unit tests for Gold classification quality evaluation.

Tests cover:
  - compute_metrics with real (non-null) confidence values
  - compute_metrics when ALL classification_confidence values are null (A-3B bootstrap path)
  - compute_metrics when SOME confidence values are null (mixed path)
  - confidence_null_rate metric computation
  - observations list populated when null confidence present
  - check_thresholds: confidence thresholds skipped gracefully when all confidence null
  - check_thresholds: confidence_null_rate warning emitted when any null
  - flagged record identification (export_ready=False, unknown label)
  - label distribution computation

A-3B bootstrap path note:
  In the validated A-3B Databricks bootstrap, classification_confidence is NULL
  because the ai_classify SQL AI Function response variant does not expose a scalar
  confidence score. The evaluator must handle this gracefully without raising
  errors or producing misleading zero-confidence metrics.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

import pytest
from eval_gold import (
    compute_metrics,
    check_thresholds,
    _identify_flagged_records,
    _compute_label_distribution,
    _build_observations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_gold(
    gold_record_id: str = "gr-1",
    document_id: str = "doc-1",
    extraction_id: str = "ex-1",
    document_type_label: str = "fda_warning_letter",
    routing_label: str = "regulatory_review",
    classification_confidence: float | None = 0.90,
    export_ready: bool = True,
    pipeline_run_id: str = "local-run-001",
) -> dict:
    return {
        "gold_record_id": gold_record_id,
        "document_id": document_id,
        "extraction_id": extraction_id,
        "pipeline_run_id": pipeline_run_id,
        "document_type_label": document_type_label,
        "routing_label": routing_label,
        "classification_confidence": classification_confidence,
        "export_ready": export_ready,
        "classified_at": "2025-01-01T00:00:00+00:00",
        "schema_version": "v0.1.0",
    }


# ---------------------------------------------------------------------------
# compute_metrics — real confidence values
# ---------------------------------------------------------------------------

class TestComputeMetricsGoldWithConfidence:
    def test_all_export_ready_records(self):
        records = [
            make_gold("gr-1", "doc-1", "ex-1", classification_confidence=0.92),
            make_gold("gr-2", "doc-2", "ex-2", classification_confidence=0.87),
            make_gold("gr-3", "doc-3", "ex-3", classification_confidence=0.95),
        ]
        m = compute_metrics(records)
        assert m["total_records"] == 3
        assert m["classification_success_rate"] == 1.0
        assert m["export_ready_rate"] == 1.0
        assert m["quarantine_rate"] == 0.0
        assert m["unknown_label_rate"] == 0.0
        assert m["confidence_null_rate"] == 0.0
        assert m["mean_classification_confidence"] == pytest.approx(0.9133, abs=0.001)
        assert m["low_confidence_rate"] == 0.0

    def test_quarantine_increases_quarantine_rate(self):
        records = [
            make_gold("gr-1", "doc-1", "ex-1", routing_label="quarantine",
                      export_ready=False, classification_confidence=0.45),
            make_gold("gr-2", "doc-2", "ex-2", classification_confidence=0.90),
        ]
        m = compute_metrics(records)
        assert m["quarantine_rate"] == 0.5
        assert m["export_ready_rate"] == 0.5

    def test_low_confidence_rate_computed_correctly(self):
        # 2 records below 0.70 threshold out of 4 with non-null confidence
        records = [
            make_gold("gr-1", classification_confidence=0.50),
            make_gold("gr-2", classification_confidence=0.60),
            make_gold("gr-3", classification_confidence=0.85),
            make_gold("gr-4", classification_confidence=0.90),
        ]
        m = compute_metrics(records)
        assert m["low_confidence_rate"] == pytest.approx(0.5, abs=0.001)


# ---------------------------------------------------------------------------
# compute_metrics — NULL confidence (A-3B bootstrap path)
# ---------------------------------------------------------------------------

class TestComputeMetricsGoldNullConfidence:
    def test_all_null_confidence_does_not_raise(self):
        """
        Core A-4 requirement: null confidence must not break the evaluator.
        This mirrors the validated A-3B bootstrap result where all records
        have classification_confidence = NULL.
        """
        records = [
            make_gold("gr-1", "doc-1", "ex-1", classification_confidence=None),
            make_gold("gr-2", "doc-2", "ex-2", classification_confidence=None),
            make_gold("gr-3", "doc-3", "ex-3", classification_confidence=None),
            make_gold("gr-4", "doc-4", "ex-4", classification_confidence=None,
                      routing_label="quarantine", export_ready=False),
        ]
        m = compute_metrics(records)
        assert "error" not in m
        assert m["total_records"] == 4

    def test_all_null_confidence_gives_none_for_mean(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(4)]
        m = compute_metrics(records)
        assert m["mean_classification_confidence"] is None

    def test_all_null_confidence_gives_none_for_low_confidence_rate(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(4)]
        m = compute_metrics(records)
        assert m["low_confidence_rate"] is None

    def test_all_null_confidence_null_rate_is_1(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(3)]
        m = compute_metrics(records)
        assert m["confidence_null_rate"] == 1.0

    def test_all_null_non_confidence_metrics_still_computed(self):
        """Classification success, export ready, quarantine rates work without confidence."""
        records = [
            make_gold("gr-1", "doc-1", "ex-1", classification_confidence=None,
                      document_type_label="fda_warning_letter", routing_label="regulatory_review",
                      export_ready=True),
            make_gold("gr-2", "doc-2", "ex-2", classification_confidence=None,
                      document_type_label="fda_warning_letter", routing_label="regulatory_review",
                      export_ready=True),
            make_gold("gr-3", "doc-3", "ex-3", classification_confidence=None,
                      document_type_label="unknown", routing_label="quarantine",
                      export_ready=False),
        ]
        m = compute_metrics(records)
        assert m["classification_success_rate"] == pytest.approx(2 / 3, abs=0.001)
        assert m["quarantine_rate"] == pytest.approx(1 / 3, abs=0.001)
        assert m["export_ready_rate"] == pytest.approx(2 / 3, abs=0.001)
        assert m["unknown_label_rate"] == pytest.approx(1 / 3, abs=0.001)

    def test_null_confidence_observations_populated(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(2)]
        m = compute_metrics(records)
        assert m["observations"]
        assert any("null" in obs.lower() for obs in m["observations"])

    def test_placeholder_run_id_observation_populated(self):
        records = [
            make_gold("gr-1", "doc-1", "ex-1", pipeline_run_id="bootstrap_sql_v1",
                      classification_confidence=None),
        ]
        m = compute_metrics(records)
        assert any("bootstrap_sql_v1" in obs for obs in m["observations"])


# ---------------------------------------------------------------------------
# compute_metrics — MIXED null and non-null confidence
# ---------------------------------------------------------------------------

class TestComputeMetricsGoldMixedConfidence:
    def test_mixed_confidence_computes_mean_over_non_null_only(self):
        """
        When some records have null confidence and others don't, mean and
        low_confidence_rate are computed only over non-null values.
        """
        records = [
            make_gold("gr-1", classification_confidence=None),
            make_gold("gr-2", classification_confidence=None),
            make_gold("gr-3", classification_confidence=0.80),
            make_gold("gr-4", classification_confidence=0.60),
        ]
        m = compute_metrics(records)
        assert m["confidence_null_rate"] == 0.5
        # mean over [0.80, 0.60] = 0.70
        assert m["mean_classification_confidence"] == pytest.approx(0.70, abs=0.01)
        # low confidence: [0.60] out of 2 non-null = 0.5
        assert m["low_confidence_rate"] == pytest.approx(0.5, abs=0.01)


# ---------------------------------------------------------------------------
# check_thresholds — null confidence path
# ---------------------------------------------------------------------------

class TestCheckThresholdsGold:
    def test_all_null_confidence_emits_warning(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(4)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert any("confidence_null_rate" in w for w in warnings)

    def test_all_null_confidence_does_not_emit_mean_confidence_warning(self):
        """
        When confidence is all null, mean_classification_confidence is None.
        The threshold check should not fire the mean confidence warning.
        """
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(4)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert not any("mean_classification_confidence" in w for w in warnings)

    def test_all_null_confidence_does_not_emit_low_confidence_rate_warning(self):
        records = [make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=None)
                   for i in range(4)]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        assert not any("low_confidence_rate" in w for w in warnings)

    def test_thresholds_met_with_real_confidence(self):
        records = [
            make_gold(f"gr-{i}", f"doc-{i}", f"ex-{i}", classification_confidence=0.85)
            for i in range(10)
        ]
        m = compute_metrics(records)
        warnings = check_thresholds(m)
        # confidence_null_rate = 0 → no confidence_null warning
        # all other thresholds met
        assert warnings == []


# ---------------------------------------------------------------------------
# _identify_flagged_records
# ---------------------------------------------------------------------------

class TestFlaggedRecordsGold:
    def test_unknown_label_flagged(self):
        records = [make_gold("gr-1", document_type_label="unknown",
                             routing_label="quarantine", export_ready=False,
                             classification_confidence=0.30)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("document_type_label=unknown" in r for r in flagged[0]["review_reasons"])

    def test_very_low_confidence_flagged(self):
        records = [make_gold("gr-1", classification_confidence=0.40)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("very_low_confidence" in r for r in flagged[0]["review_reasons"])

    def test_export_not_ready_flagged(self):
        records = [make_gold("gr-1", export_ready=False, classification_confidence=0.65)]
        flagged = _identify_flagged_records(records)
        assert len(flagged) == 1
        assert any("export_ready=false" in r for r in flagged[0]["review_reasons"])

    def test_null_confidence_does_not_trigger_very_low_confidence_flag(self):
        """
        Null confidence records (bootstrap path) must NOT be flagged as
        'very_low_confidence' — that flag requires an actual numeric value < 0.50.
        """
        records = [make_gold("gr-1", classification_confidence=None, export_ready=True)]
        flagged = _identify_flagged_records(records)
        assert flagged == []

    def test_clean_export_ready_record_not_flagged(self):
        records = [make_gold("gr-1", classification_confidence=0.90, export_ready=True)]
        flagged = _identify_flagged_records(records)
        assert flagged == []


# ---------------------------------------------------------------------------
# _compute_label_distribution
# ---------------------------------------------------------------------------

class TestLabelDistribution:
    def test_label_distribution_counts_and_rates(self):
        records = [
            make_gold("gr-1", document_type_label="fda_warning_letter"),
            make_gold("gr-2", document_type_label="fda_warning_letter"),
            make_gold("gr-3", document_type_label="cisa_advisory"),
        ]
        dist = _compute_label_distribution(records)
        assert dist[0]["label"] == "fda_warning_letter"
        assert dist[0]["count"] == 2
        assert dist[0]["rate"] == pytest.approx(2 / 3, abs=0.001)

    def test_single_label_distribution(self):
        records = [make_gold(f"gr-{i}", document_type_label="fda_warning_letter") for i in range(3)]
        dist = _compute_label_distribution(records)
        assert len(dist) == 1
        assert dist[0]["rate"] == 1.0
