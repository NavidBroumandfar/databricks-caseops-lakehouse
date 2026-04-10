"""
tests/test_eval_traceability.py — Unit tests for cross-layer traceability evaluation.

Tests cover:
  - All records linked (no orphans, clean pipeline)
  - Orphaned Silver records (no matching Bronze)
  - Orphaned Gold records (no matching Silver)
  - pipeline_run_id coverage computation
  - schema_version coverage computation
  - Placeholder run ID detection (A-3B bootstrap path)
  - check_thresholds: warning for any link rate < 1.0
  - check_thresholds: warning for any orphan count > 0
  - Observations populated for placeholder run IDs
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

import pytest
from eval_traceability import (
    compute_metrics,
    check_thresholds,
    _build_flagged_records,
    _build_observations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_bronze(
    bronze_record_id: str = "br-1",
    document_id: str = "doc-1",
    pipeline_run_id: str = "local-run-001",
    schema_version: str = "v0.1.0",
) -> dict:
    return {
        "bronze_record_id": bronze_record_id,
        "document_id": document_id,
        "pipeline_run_id": pipeline_run_id,
        "schema_version": schema_version,
        "parse_status": "success",
    }


def make_silver(
    extraction_id: str = "ex-1",
    document_id: str = "doc-1",
    bronze_record_id: str = "br-1",
    pipeline_run_id: str = "local-run-001",
    schema_version: str = "v0.1.0",
) -> dict:
    return {
        "extraction_id": extraction_id,
        "document_id": document_id,
        "bronze_record_id": bronze_record_id,
        "pipeline_run_id": pipeline_run_id,
        "schema_version": schema_version,
        "validation_status": "valid",
    }


def make_gold(
    gold_record_id: str = "gr-1",
    document_id: str = "doc-1",
    extraction_id: str = "ex-1",
    pipeline_run_id: str = "local-run-001",
    schema_version: str = "v0.1.0",
) -> dict:
    return {
        "gold_record_id": gold_record_id,
        "document_id": document_id,
        "extraction_id": extraction_id,
        "pipeline_run_id": pipeline_run_id,
        "schema_version": schema_version,
        "export_ready": True,
    }


# ---------------------------------------------------------------------------
# compute_metrics — clean pipeline (no orphans)
# ---------------------------------------------------------------------------

class TestComputeMetricsTraceabilityClean:
    def test_all_linked_no_orphans(self):
        bronze = [make_bronze("br-1", "doc-1"), make_bronze("br-2", "doc-2")]
        silver = [make_silver("ex-1", "doc-1", "br-1"), make_silver("ex-2", "doc-2", "br-2")]
        gold = [make_gold("gr-1", "doc-1", "ex-1"), make_gold("gr-2", "doc-2", "ex-2")]
        m = compute_metrics(bronze, silver, gold)

        assert m["total_bronze"] == 2
        assert m["total_silver"] == 2
        assert m["total_gold"] == 2
        assert m["gold_to_silver_link_rate"] == 1.0
        assert m["silver_to_bronze_link_rate"] == 1.0
        assert m["orphaned_silver_count"] == 0
        assert m["orphaned_gold_count"] == 0
        assert m["flagged_record_count"] == 0

    def test_pipeline_run_id_coverage_all_present(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["pipeline_run_id_coverage"] == 1.0

    def test_schema_version_coverage_all_present(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["schema_version_coverage"] == 1.0

    def test_empty_all_layers_returns_error(self):
        m = compute_metrics([], [], [])
        assert "error" in m


# ---------------------------------------------------------------------------
# compute_metrics — orphaned records
# ---------------------------------------------------------------------------

class TestComputeMetricsOrphans:
    def test_orphaned_gold_detected(self):
        """Gold record references extraction_id that does not exist in Silver."""
        bronze = [make_bronze("br-1", "doc-1")]
        silver = [make_silver("ex-1", "doc-1", "br-1")]
        gold = [
            make_gold("gr-1", "doc-1", "ex-1"),                      # linked
            make_gold("gr-2", "doc-2", "ex-MISSING", "doc-2"),        # orphaned
        ]
        m = compute_metrics(bronze, silver, gold)
        assert m["orphaned_gold_count"] == 1
        assert m["gold_to_silver_link_rate"] == pytest.approx(0.5, abs=0.001)
        assert m["flagged_record_count"] >= 1

    def test_orphaned_silver_detected(self):
        """Silver record references bronze_record_id that does not exist in Bronze."""
        bronze = [make_bronze("br-1", "doc-1")]
        silver = [
            make_silver("ex-1", "doc-1", "br-1"),                    # linked
            make_silver("ex-2", "doc-2", "br-MISSING"),               # orphaned
        ]
        gold = [make_gold("gr-1", "doc-1", "ex-1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["orphaned_silver_count"] == 1
        assert m["silver_to_bronze_link_rate"] == pytest.approx(0.5, abs=0.001)
        assert m["flagged_record_count"] >= 1

    def test_orphaned_records_in_flagged_list(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1", "doc-1", "br-MISSING")]
        gold = []
        m = compute_metrics(bronze, silver, gold)
        flagged = m["flagged_records"]
        assert len(flagged) == 1
        assert flagged[0]["layer"] == "silver"
        assert "orphaned_silver" in flagged[0]["issue"]


# ---------------------------------------------------------------------------
# compute_metrics — pipeline_run_id and schema_version coverage
# ---------------------------------------------------------------------------

class TestCoverageMetrics:
    def test_null_pipeline_run_id_reduces_coverage(self):
        bronze = [
            make_bronze("br-1", pipeline_run_id="local-run-001"),
            make_bronze("br-2", pipeline_run_id=None),
        ]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        # 1 out of 4 total records has null pipeline_run_id
        assert m["pipeline_run_id_coverage"] < 1.0

    def test_null_schema_version_reduces_coverage(self):
        bronze = [make_bronze("br-1", schema_version=None)]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["schema_version_coverage"] < 1.0


# ---------------------------------------------------------------------------
# compute_metrics — placeholder run ID (A-3B bootstrap path)
# ---------------------------------------------------------------------------

class TestPlaceholderRunIds:
    def test_bootstrap_placeholder_detected(self):
        """
        Records with pipeline_run_id = 'bootstrap_sql_v1' are from the A-3B
        bootstrap SQL execution. These are valid records with intact lineage —
        they just use a placeholder run ID instead of a real MLflow run ID.
        """
        bronze = [make_bronze("br-1", pipeline_run_id="bootstrap_sql_v1")]
        silver = [make_silver("ex-1", pipeline_run_id="bootstrap_sql_v1")]
        gold = [make_gold("gr-1", pipeline_run_id="bootstrap_sql_v1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["placeholder_run_id_count"] == 3

    def test_placeholder_not_counted_as_null(self):
        """Placeholder run IDs are not null, so pipeline_run_id_coverage stays 1.0."""
        bronze = [make_bronze("br-1", pipeline_run_id="bootstrap_sql_v1")]
        silver = [make_silver("ex-1", pipeline_run_id="bootstrap_sql_v1")]
        gold = [make_gold("gr-1", pipeline_run_id="bootstrap_sql_v1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["pipeline_run_id_coverage"] == 1.0

    def test_placeholder_observation_populated(self):
        bronze = [make_bronze("br-1", pipeline_run_id="bootstrap_sql_v1")]
        silver = [make_silver("ex-1", pipeline_run_id="bootstrap_sql_v1")]
        gold = [make_gold("gr-1", pipeline_run_id="bootstrap_sql_v1")]
        m = compute_metrics(bronze, silver, gold)
        assert m["observations"]
        assert any("bootstrap" in obs.lower() for obs in m["observations"])

    def test_real_and_placeholder_mixed(self):
        bronze = [
            make_bronze("br-1", pipeline_run_id="mlflow-run-abc123"),
            make_bronze("br-2", pipeline_run_id="bootstrap_sql_v1"),
        ]
        silver = [
            make_silver("ex-1", bronze_record_id="br-1", pipeline_run_id="mlflow-run-abc123"),
            make_silver("ex-2", document_id="doc-2", bronze_record_id="br-2",
                        pipeline_run_id="bootstrap_sql_v1"),
        ]
        gold = [
            make_gold("gr-1", pipeline_run_id="mlflow-run-abc123"),
            make_gold("gr-2", document_id="doc-2", extraction_id="ex-2",
                      pipeline_run_id="bootstrap_sql_v1"),
        ]
        m = compute_metrics(bronze, silver, gold)
        assert m["placeholder_run_id_count"] == 3  # br-2, ex-2, gr-2


# ---------------------------------------------------------------------------
# check_thresholds
# ---------------------------------------------------------------------------

class TestCheckThresholdsTraceability:
    def test_clean_pipeline_no_warnings(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        warnings = check_thresholds(m)
        assert warnings == []

    def test_orphaned_gold_triggers_warning(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1", extraction_id="ex-MISSING")]
        m = compute_metrics(bronze, silver, gold)
        warnings = check_thresholds(m)
        assert any("gold_to_silver_link_rate" in w or "orphaned_gold" in w for w in warnings)

    def test_orphaned_silver_triggers_warning(self):
        bronze = [make_bronze("br-1")]
        silver = [make_silver("ex-1", bronze_record_id="br-MISSING")]
        gold = []
        m = compute_metrics(bronze, silver, gold)
        warnings = check_thresholds(m)
        assert any("silver_to_bronze_link_rate" in w or "orphaned_silver" in w for w in warnings)

    def test_null_pipeline_run_id_triggers_warning(self):
        bronze = [make_bronze("br-1", pipeline_run_id=None)]
        silver = [make_silver("ex-1")]
        gold = [make_gold("gr-1")]
        m = compute_metrics(bronze, silver, gold)
        warnings = check_thresholds(m)
        assert any("pipeline_run_id_coverage" in w for w in warnings)
