"""
tests/test_run_evaluation.py — Unit tests for evaluation report assembly.

Tests cover:
  - assemble_report collects warnings from all layers
  - assemble_report collects observations from all layers
  - assemble_report computes review_queue_size correctly
  - bootstrap_path_detected is True when gold has null confidence
  - bootstrap_path_detected is True when traceability has placeholder run IDs
  - bootstrap_path_detected is False for clean local pipeline
  - EvaluationReport can be serialised to JSON (report_models round-trip)
  - report_writer produces non-empty JSON and text files
"""

import json
import sys
import tempfile
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

import pytest
from report_models import EvaluationReport, LayerEvalResult, TraceabilityResult
from report_writer import write_report
from run_evaluation import assemble_report, _is_bootstrap_path_detected


# ---------------------------------------------------------------------------
# Fixtures — minimal LayerEvalResult and TraceabilityResult
# ---------------------------------------------------------------------------

def make_layer_result(
    layer: str = "bronze",
    experiment: str = "caseops/bronze/parse_quality",
    total_records: int = 4,
    threshold_warnings: list[str] | None = None,
    observations: list[str] | None = None,
    flagged_record_count: int = 0,
    metrics: dict | None = None,
) -> LayerEvalResult:
    return LayerEvalResult(
        layer=layer,
        experiment=experiment,
        eval_run_id=str(uuid.uuid4()),
        evaluated_at="2025-01-01T00:00:00+00:00",
        total_records=total_records,
        metrics=metrics or {"parse_success_rate": 1.0},
        threshold_warnings=threshold_warnings or [],
        observations=observations or [],
        flagged_record_count=flagged_record_count,
        flagged_records=[],
    )


def make_traceability_result(
    placeholder_run_id_count: int = 0,
    threshold_warnings: list[str] | None = None,
    observations: list[str] | None = None,
) -> TraceabilityResult:
    return TraceabilityResult(
        experiment="caseops/pipeline/traceability",
        eval_run_id=str(uuid.uuid4()),
        evaluated_at="2025-01-01T00:00:00+00:00",
        total_bronze=4,
        total_silver=4,
        total_gold=3,
        gold_to_silver_link_rate=1.0,
        silver_to_bronze_link_rate=1.0,
        pipeline_run_id_coverage=1.0,
        schema_version_coverage=1.0,
        orphaned_silver_count=0,
        orphaned_gold_count=0,
        placeholder_run_id_count=placeholder_run_id_count,
        placeholder_run_id_note="bootstrap" if placeholder_run_id_count > 0 else "",
        threshold_warnings=threshold_warnings or [],
        observations=observations or [],
    )


# ---------------------------------------------------------------------------
# assemble_report
# ---------------------------------------------------------------------------

class TestAssembleReport:
    def test_all_layers_warnings_collected(self):
        bronze = make_layer_result("bronze", threshold_warnings=["bronze warning 1"])
        silver = make_layer_result("silver", threshold_warnings=["silver warning 1"])
        gold = make_layer_result("gold", threshold_warnings=["gold warning 1"],
                                 metrics={"confidence_null_rate": 0.0})
        trace = make_traceability_result(threshold_warnings=["traceability warning 1"])

        report = assemble_report(bronze, silver, gold, trace)
        assert len(report.all_warnings) == 4

    def test_all_layers_observations_collected(self):
        bronze = make_layer_result("bronze", observations=["bronze note"])
        gold = make_layer_result("gold", observations=["gold note"],
                                 metrics={"confidence_null_rate": 0.0})
        trace = make_traceability_result(observations=["traceability note"])

        report = assemble_report(bronze, None, gold, trace)
        assert len(report.all_observations) == 3

    def test_review_queue_size_is_sum_of_flagged(self):
        bronze = make_layer_result("bronze", flagged_record_count=2)
        silver = make_layer_result("silver", flagged_record_count=1)
        gold = make_layer_result("gold", flagged_record_count=0,
                                 metrics={"confidence_null_rate": 0.0})

        report = assemble_report(bronze, silver, gold, None)
        assert report.review_queue_size == 3

    def test_pipeline_run_id_filter_stored(self):
        report = assemble_report(None, None, None, None, pipeline_run_id_filter="test-run-123")
        assert report.pipeline_run_id_filter == "test-run-123"

    def test_none_layers_tolerated(self):
        report = assemble_report(None, None, None, None)
        assert report.bronze is None
        assert report.silver is None
        assert report.gold is None
        assert report.traceability is None
        assert report.review_queue_size == 0


# ---------------------------------------------------------------------------
# _is_bootstrap_path_detected
# ---------------------------------------------------------------------------

class TestBootstrapPathDetection:
    def test_detected_via_null_confidence(self):
        gold = make_layer_result("gold", metrics={"confidence_null_rate": 1.0})
        result = _is_bootstrap_path_detected(gold, None)
        assert result is True

    def test_detected_via_placeholder_run_id(self):
        trace = make_traceability_result(placeholder_run_id_count=3)
        result = _is_bootstrap_path_detected(None, trace)
        assert result is True

    def test_not_detected_for_clean_pipeline(self):
        gold = make_layer_result("gold", metrics={"confidence_null_rate": 0.0})
        trace = make_traceability_result(placeholder_run_id_count=0)
        result = _is_bootstrap_path_detected(gold, trace)
        assert result is False

    def test_not_detected_when_gold_is_none(self):
        trace = make_traceability_result(placeholder_run_id_count=0)
        result = _is_bootstrap_path_detected(None, trace)
        assert result is False


# ---------------------------------------------------------------------------
# EvaluationReport — dataclass round-trip serialisation
# ---------------------------------------------------------------------------

class TestEvaluationReportSerialisation:
    def test_report_fields_accessible(self):
        report = EvaluationReport(
            report_id="test-report-id",
            generated_at="2025-01-01T00:00:00+00:00",
            pipeline_run_id_filter=None,
        )
        assert report.report_id == "test-report-id"
        assert report.all_warnings == []
        assert report.bootstrap_path_detected is False

    def test_report_with_layers_json_serialisable(self):
        bronze = make_layer_result("bronze")
        silver = make_layer_result("silver")
        gold = make_layer_result("gold", metrics={"confidence_null_rate": 0.0})
        trace = make_traceability_result()
        report = assemble_report(bronze, silver, gold, trace)

        # Should not raise
        import dataclasses

        def _to_json(obj):
            if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
                return {k: _to_json(v) for k, v in dataclasses.asdict(obj).items()}
            if isinstance(obj, list):
                return [_to_json(i) for i in obj]
            return obj

        serialised = json.dumps(_to_json(report))
        parsed = json.loads(serialised)
        assert parsed["report_id"] == report.report_id


# ---------------------------------------------------------------------------
# report_writer — file output
# ---------------------------------------------------------------------------

class TestReportWriter:
    def test_write_report_creates_json_and_text(self):
        report = EvaluationReport(
            report_id="test-id",
            generated_at="2025-01-01T00:00:00+00:00",
            pipeline_run_id_filter=None,
            all_warnings=["test warning"],
            all_observations=["test observation"],
            review_queue_size=2,
            bootstrap_path_detected=True,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, text_path = write_report(report, Path(tmpdir))

            assert json_path.exists()
            assert text_path.exists()

            # JSON content is valid and has the report_id
            data = json.loads(json_path.read_text())
            assert data["report_id"] == "test-id"

            # Text content is non-empty and mentions warnings
            text = text_path.read_text()
            assert "test warning" in text
            assert "Bootstrap" in text or "bootstrap" in text

    def test_write_report_with_layer_results(self):
        bronze = make_layer_result("bronze")
        gold = make_layer_result(
            "gold",
            metrics={"confidence_null_rate": 1.0, "export_ready_rate": 0.75},
            observations=["null confidence: bootstrap path"],
        )
        report = assemble_report(bronze, None, gold, None)

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, text_path = write_report(report, Path(tmpdir))
            data = json.loads(json_path.read_text())
            assert data["bootstrap_path_detected"] is True
            assert data["gold"]["layer"] == "gold"
