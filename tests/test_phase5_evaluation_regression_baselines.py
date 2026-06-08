"""
tests/test_phase5_evaluation_regression_baselines.py — Phase 5 regression guardrails.

This test file is the acceptance layer for richer evaluation fixtures.
It validates:
  - per-domain silver quality baselines
  - full-link traceability baseline
  - explicit failure paths when traceability regresses
  - explicit failure path when extraction validity regresses below baseline
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

from eval_silver import compute_metrics as compute_silver_metrics, check_thresholds as check_silver_thresholds
from eval_traceability import (
    compute_metrics as compute_traceability_metrics,
    check_thresholds as check_traceability_thresholds,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "examples" / "evaluation" / "phase5_regression_fixtures.json"


def _load_fixture() -> dict:
    assert FIXTURE_PATH.exists(), f"Fixture not found: {FIXTURE_PATH}"
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class TestPhase5RegressionSilverBaselines:
    def test_per_domain_silver_metrics_stay_within_baseline_ranges(self):
        fixture = _load_fixture()
        ranges = fixture["baseline_ranges"]

        for domain, records in fixture["silver_records"].items():
            metrics = compute_silver_metrics(records)
            domain_ranges = ranges[domain]

            assert metrics["schema_validity_rate"] >= domain_ranges["min_schema_validity_rate"]
            assert metrics["partial_validity_rate"] >= domain_ranges["min_partial_validity_rate"]
            assert metrics["invalid_rate"] <= domain_ranges["max_invalid_rate"]
            assert metrics["mean_field_coverage_pct"] >= domain_ranges["min_mean_field_coverage_pct"]
            assert metrics["p25_field_coverage_pct"] >= domain_ranges["min_p25_field_coverage_pct"]
            assert metrics["required_field_null_rate"] <= domain_ranges["max_required_field_null_rate"]

            # Phase-5 baseline keeps this warning channel stable for current fixtures.
            warnings = check_silver_thresholds(metrics)
            assert warnings == []

    def test_silver_regression_fails_quality_gates_when_forced_degraded(self):
        fixture = _load_fixture()
        records = copy.deepcopy(fixture["silver_records"]["fda_warning_letter"])
        for record in records:
            record["validation_status"] = "invalid"
            record["validation_errors"] = ["forced regression"]
        metrics = compute_silver_metrics(records)
        assert metrics["schema_validity_rate"] < fixture["baseline_ranges"]["fda_warning_letter"]["min_schema_validity_rate"]

        warnings = check_silver_thresholds(metrics)
        assert any("schema_validity_rate" in warning for warning in warnings)


class TestPhase5RegressionTraceabilityBaselines:
    def test_traceability_baseline_has_full_linkage(self):
        fixture = _load_fixture()
        trace = fixture["traceability_records"]
        metrics = compute_traceability_metrics(
            trace["bronze"],
            trace["silver"],
            trace["gold"],
        )

        assert metrics["gold_to_silver_link_rate"] == 1.0
        assert metrics["silver_to_bronze_link_rate"] == 1.0
        assert metrics["pipeline_run_id_coverage"] == 1.0
        assert metrics["schema_version_coverage"] == 1.0
        assert metrics["orphaned_silver_count"] == 0
        assert metrics["orphaned_gold_count"] == 0

        warnings = check_traceability_thresholds(metrics)
        assert warnings == []

    def test_traceability_regression_reports_linkage_warning(self):
        fixture = _load_fixture()
        trace = copy.deepcopy(fixture["traceability_records"])

        # Remove one Bronze row while keeping Silver referencing it to force an orphan.
        trace["bronze"] = trace["bronze"][1:]
        metrics = compute_traceability_metrics(
            trace["bronze"],
            trace["silver"],
            trace["gold"],
        )

        assert metrics["silver_to_bronze_link_rate"] < 1.0
        assert metrics["orphaned_silver_count"] >= 1

        warnings = check_traceability_thresholds(metrics)
        assert any("silver_to_bronze_link_rate" in warning for warning in warnings)
