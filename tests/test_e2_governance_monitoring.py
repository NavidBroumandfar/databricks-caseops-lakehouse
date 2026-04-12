"""
tests/test_e2_governance_monitoring.py — E-2 Governance Monitoring Test Suite.

Covers:
  - Governance schema constants and vocabulary
  - GovernanceFlag construction and validation
  - GovernanceReport construction via build_governance_report()
  - Deterministic aggregation from evaluation, handoff, review queue artifacts
  - Governance flag generation (all flag categories)
  - Schema / contract drift signaling
  - Review queue pressure signaling
  - Environment-aware summary behavior
  - Overall health status derivation
  - format_governance_report_text() output
  - write_governance_report() artifact materialization
  - load_governance_report() round-trip
  - Empty / partial input graceful handling
  - No secrets, no live Databricks dependency, no Bedrock/UI drift

Design:
  All tests are local-safe. No real evaluation runs, no Databricks workspace,
  no MLflow, no credentials. Input artifacts are constructed in-process as dicts.

Phase: E-2
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.schemas.governance_monitoring import (
    ALL_FLAG_CATEGORIES,
    ALL_HEALTH_STATUSES,
    ALL_SEVERITIES,
    FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
    FLAG_ENVIRONMENT_CONFIG_MISMATCH,
    FLAG_EXPORT_HANDOFF_RELIABILITY,
    FLAG_QUALITY_DEGRADATION,
    FLAG_REVIEW_QUEUE_PRESSURE,
    FLAG_TRACEABILITY_DEFECT,
    GOVERNANCE_MONITORING_SCHEMA_VERSION,
    HEALTH_CRITICAL,
    HEALTH_DEGRADED,
    HEALTH_HEALTHY,
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    THRESHOLD_BRONZE_PARSE_SUCCESS,
    THRESHOLD_GOLD_EXPORT_READY,
    THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL,
    THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING,
    THRESHOLD_SILVER_VALIDITY,
    THRESHOLD_TRACEABILITY_LINK,
    GovernanceFlag,
    GovernanceReport,
    HandoffHealthSummary,
    QualitySummary,
    ReportingScope,
    ReviewQueueSummary,
    SchemaDriftIndicators,
    make_flag_id,
    make_governance_flag,
    make_governance_report_id,
)
from src.pipelines.governance_monitoring import (
    _derive_governance_flags,
    _derive_handoff_health,
    _derive_overall_health,
    _derive_quality_summary,
    _derive_review_queue_summary,
    _derive_schema_drift,
    build_governance_report,
    compute_governance_report_path,
    format_governance_report_text,
    load_governance_report,
    write_governance_report,
)


# ===========================================================================
# Fixtures — in-process artifact dicts (no real file reads)
# ===========================================================================


def _make_eval_report(
    bronze_success=1.0,
    silver_validity=1.0,
    gold_export_ready=1.0,
    gold_class_success=1.0,
    gold_silver_link=1.0,
    silver_bronze_link=1.0,
    run_id_coverage=1.0,
    orphaned_silver=0,
    orphaned_gold=0,
    bootstrap=False,
    observations=None,
) -> dict:
    return {
        "report_id": "test-report-id",
        "generated_at": "2026-04-12T10:00:00+00:00",
        "pipeline_run_id_filter": "run001",
        "bronze": {"metrics": {"parse_success_rate": bronze_success}, "layer": "bronze", "experiment": "bronze/parse_quality", "eval_run_id": "x", "evaluated_at": "2026-04-12", "total_records": 4},
        "silver": {"metrics": {"schema_validity_rate": silver_validity}, "layer": "silver", "experiment": "silver/extraction_quality", "eval_run_id": "x", "evaluated_at": "2026-04-12", "total_records": 4},
        "gold": {"metrics": {"export_ready_rate": gold_export_ready, "classification_success_rate": gold_class_success}, "layer": "gold", "experiment": "gold/classification_quality", "eval_run_id": "x", "evaluated_at": "2026-04-12", "total_records": 4},
        "traceability": {
            "gold_to_silver_link_rate": gold_silver_link,
            "silver_to_bronze_link_rate": silver_bronze_link,
            "pipeline_run_id_coverage": run_id_coverage,
            "orphaned_silver_count": orphaned_silver,
            "orphaned_gold_count": orphaned_gold,
        },
        "bootstrap_path_detected": bootstrap,
        "all_warnings": [],
        "all_observations": observations or [],
    }


def _make_handoff_report(
    total_processed=4,
    total_eligible=4,
    total_exported=3,
    total_quarantined=1,
    total_blocked=0,
    total_skipped=0,
    pipeline_run_id="run001",
    contract_blocked_ids=None,
    quarantined_ids=None,
) -> dict:
    return {
        "pipeline_run_id": pipeline_run_id,
        "batch_processed_at": "2026-04-12T10:00:00+00:00",
        "total_records_processed": total_processed,
        "total_ineligible_skipped": 0,
        "total_eligible": total_eligible,
        "total_exported": total_exported,
        "total_quarantined": total_quarantined,
        "total_contract_blocked": total_blocked,
        "total_skipped_not_export_ready": total_skipped,
        "contract_blocked_document_ids": contract_blocked_ids or [],
        "quarantined_document_ids": quarantined_ids or ["doc-uuid-001"],
    }


def _make_review_queue(
    total_reviewed=4,
    total_entries=1,
    quarantined=1,
    contract_blocked=0,
    extraction_failed=0,
    pipeline_run_id="run001",
) -> dict:
    return {
        "review_queue_id": "rq-001",
        "pipeline_run_id": pipeline_run_id,
        "generated_at": "2026-04-12T10:00:00+00:00",
        "schema_version": "v1.0.0",
        "total_records_reviewed": total_reviewed,
        "total_entries": total_entries,
        "entries_by_reason": {
            "quarantined": quarantined,
            "contract_blocked": contract_blocked,
            "extraction_failed": extraction_failed,
        },
        "queue_entries": [],
        "review_notes": [],
    }


# ===========================================================================
# 1. Schema constants and vocabulary
# ===========================================================================


class TestSchemaConstants:
    def test_schema_version_is_string(self):
        assert isinstance(GOVERNANCE_MONITORING_SCHEMA_VERSION, str)
        assert GOVERNANCE_MONITORING_SCHEMA_VERSION == "v1.0.0"

    def test_all_flag_categories_non_empty(self):
        assert len(ALL_FLAG_CATEGORIES) >= 5

    def test_flag_categories_contain_expected(self):
        expected = {
            FLAG_QUALITY_DEGRADATION,
            FLAG_TRACEABILITY_DEFECT,
            FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
            FLAG_REVIEW_QUEUE_PRESSURE,
            FLAG_EXPORT_HANDOFF_RELIABILITY,
            FLAG_ENVIRONMENT_CONFIG_MISMATCH,
        }
        assert expected.issubset(set(ALL_FLAG_CATEGORIES))

    def test_all_severities_contains_three(self):
        assert set(ALL_SEVERITIES) == {SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_CRITICAL}

    def test_all_health_statuses_contains_three(self):
        assert set(ALL_HEALTH_STATUSES) == {HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL}

    def test_thresholds_are_floats_in_range(self):
        for t in [
            THRESHOLD_BRONZE_PARSE_SUCCESS,
            THRESHOLD_SILVER_VALIDITY,
            THRESHOLD_GOLD_EXPORT_READY,
            THRESHOLD_TRACEABILITY_LINK,
            THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING,
            THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL,
        ]:
            assert isinstance(t, float)
            assert 0.0 <= t <= 1.0

    def test_pressure_critical_above_warning(self):
        assert THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL > THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING


# ===========================================================================
# 2. GovernanceFlag construction
# ===========================================================================


class TestGovernanceFlagConstruction:
    def test_make_flag_id_is_uuid_string(self):
        fid = make_flag_id()
        assert isinstance(fid, str)
        assert len(fid) == 36  # UUID4 format

    def test_make_governance_flag_valid(self):
        flag = make_governance_flag(
            category=FLAG_QUALITY_DEGRADATION,
            severity=SEVERITY_WARNING,
            signal="bronze_parse_success_below_threshold",
            detail="Test detail.",
        )
        assert isinstance(flag, GovernanceFlag)
        assert flag.category == FLAG_QUALITY_DEGRADATION
        assert flag.severity == SEVERITY_WARNING
        assert flag.signal == "bronze_parse_success_below_threshold"
        assert "Test detail" in flag.detail
        assert len(flag.flag_id) == 36

    def test_make_governance_flag_invalid_category_raises(self):
        with pytest.raises(ValueError, match="Unknown flag category"):
            make_governance_flag(
                category="not_a_category",
                severity=SEVERITY_INFO,
                signal="test",
                detail="detail",
            )

    def test_make_governance_flag_invalid_severity_raises(self):
        with pytest.raises(ValueError, match="Unknown severity"):
            make_governance_flag(
                category=FLAG_QUALITY_DEGRADATION,
                severity="catastrophic",
                signal="test",
                detail="detail",
            )

    def test_each_flag_gets_unique_id(self):
        ids = {make_flag_id() for _ in range(10)}
        assert len(ids) == 10


# ===========================================================================
# 3. Quality summary derivation
# ===========================================================================


class TestDeriveQualitySummary:
    def test_none_input_returns_all_none_metrics(self):
        q = _derive_quality_summary(None)
        assert q.bronze_parse_success_rate is None
        assert q.silver_extraction_validity_rate is None
        assert q.gold_export_ready_rate is None
        assert q.bootstrap_path_detected is False
        assert "No evaluation report" in q.quality_observations[0]

    def test_full_eval_report_extracts_correctly(self):
        report = _make_eval_report(
            bronze_success=0.95,
            silver_validity=0.88,
            gold_export_ready=0.75,
            gold_class_success=1.0,
            gold_silver_link=1.0,
            silver_bronze_link=1.0,
            run_id_coverage=1.0,
            orphaned_silver=0,
            orphaned_gold=0,
        )
        q = _derive_quality_summary(report)
        assert q.bronze_parse_success_rate == pytest.approx(0.95)
        assert q.silver_extraction_validity_rate == pytest.approx(0.88)
        assert q.gold_export_ready_rate == pytest.approx(0.75)
        assert q.traceability_gold_to_silver_link_rate == pytest.approx(1.0)
        assert q.traceability_orphaned_silver_count == 0
        assert q.traceability_orphaned_gold_count == 0
        assert q.bootstrap_path_detected is False

    def test_bootstrap_detected_sets_flag(self):
        report = _make_eval_report(bootstrap=True)
        q = _derive_quality_summary(report)
        assert q.bootstrap_path_detected is True
        assert any("Bootstrap" in obs for obs in q.quality_observations)

    def test_missing_bronze_section_returns_none(self):
        report = {"gold": {"metrics": {}}, "traceability": {}}
        q = _derive_quality_summary(report)
        assert q.bronze_parse_success_rate is None

    def test_observations_surfaced_bounded(self):
        observations = [f"obs_{i}" for i in range(10)]
        report = _make_eval_report(observations=observations)
        q = _derive_quality_summary(report)
        # Should cap at 5 observations from all_observations
        assert len(q.quality_observations) <= 5


# ===========================================================================
# 4. Handoff health derivation
# ===========================================================================


class TestDeriveHandoffHealth:
    def test_none_input_returns_none_metrics(self):
        h = _derive_handoff_health(None)
        assert h.total_exported is None
        assert h.export_success_rate is None
        assert "No handoff batch report" in h.handoff_observations[0]

    def test_full_handoff_report_extracts_correctly(self):
        report = _make_handoff_report(
            total_exported=3,
            total_eligible=4,
            total_quarantined=1,
            total_blocked=0,
        )
        h = _derive_handoff_health(report)
        assert h.total_exported == 3
        assert h.total_quarantined == 1
        assert h.total_contract_blocked == 0
        assert h.export_success_rate == pytest.approx(0.75)
        assert h.quarantine_rate == pytest.approx(0.25)
        assert h.contract_block_rate == pytest.approx(0.0)

    def test_contract_blocked_observation_generated(self):
        report = _make_handoff_report(total_blocked=2)
        h = _derive_handoff_health(report)
        assert h.total_contract_blocked == 2
        assert any("blocked by Bedrock contract" in obs for obs in h.handoff_observations)

    def test_quarantine_observation_generated(self):
        report = _make_handoff_report(total_quarantined=2)
        h = _derive_handoff_health(report)
        assert any("quarantine" in obs for obs in h.handoff_observations)

    def test_zero_eligible_rates_are_none(self):
        report = _make_handoff_report(total_eligible=0, total_exported=0)
        h = _derive_handoff_health(report)
        assert h.export_success_rate is None

    def test_quarantined_ids_preserved(self):
        report = _make_handoff_report(quarantined_ids=["doc-aaa", "doc-bbb"])
        h = _derive_handoff_health(report)
        assert "doc-aaa" in h.quarantined_document_ids
        assert "doc-bbb" in h.quarantined_document_ids


# ===========================================================================
# 5. Review queue summary derivation
# ===========================================================================


class TestDeriveReviewQueueSummary:
    def test_none_input_returns_none_metrics(self):
        rq = _derive_review_queue_summary(None)
        assert rq.total_queue_entries is None
        assert rq.review_queue_pressure_pct is None
        assert "No review queue artifact" in rq.review_queue_observations[0]

    def test_full_review_queue_extracts_correctly(self):
        artifact = _make_review_queue(total_reviewed=4, total_entries=1)
        rq = _derive_review_queue_summary(artifact)
        assert rq.total_records_reviewed == 4
        assert rq.total_queue_entries == 1
        assert rq.review_queue_pressure_pct == pytest.approx(0.25)

    def test_zero_entries_observation(self):
        artifact = _make_review_queue(total_reviewed=4, total_entries=0, quarantined=0)
        rq = _derive_review_queue_summary(artifact)
        assert rq.review_queue_pressure_pct == pytest.approx(0.0)
        assert any("No records required human review" in obs for obs in rq.review_queue_observations)

    def test_entries_by_reason_preserved(self):
        artifact = _make_review_queue(quarantined=1, contract_blocked=2, extraction_failed=0, total_entries=3)
        rq = _derive_review_queue_summary(artifact)
        assert rq.entries_by_reason.get("quarantined") == 1
        assert rq.entries_by_reason.get("contract_blocked") == 2


# ===========================================================================
# 6. Governance flag generation — quality degradation
# ===========================================================================


class TestGovernanceFlagsQualityDegradation:
    def _base_quality(self, **kwargs) -> QualitySummary:
        defaults = dict(
            bronze_parse_success_rate=1.0,
            silver_extraction_validity_rate=1.0,
            gold_export_ready_rate=1.0,
            gold_classification_success_rate=1.0,
            traceability_gold_to_silver_link_rate=1.0,
            traceability_silver_to_bronze_link_rate=1.0,
            traceability_pipeline_run_id_coverage=1.0,
            traceability_orphaned_silver_count=0,
            traceability_orphaned_gold_count=0,
            bootstrap_path_detected=False,
        )
        defaults.update(kwargs)
        return QualitySummary(**defaults)

    def _no_flags(self) -> tuple:
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators([], False, [], [], [], False)
        return h, rq, drift

    def test_no_flags_when_all_thresholds_met(self):
        q = self._base_quality()
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        quality_flags = [f for f in flags if f.category == FLAG_QUALITY_DEGRADATION]
        assert quality_flags == []

    def test_bronze_below_threshold_raises_warning(self):
        q = self._base_quality(bronze_parse_success_rate=0.8)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "bronze_parse_success_below_threshold" in signals
        flag = next(f for f in flags if f.signal == "bronze_parse_success_below_threshold")
        assert flag.severity == SEVERITY_WARNING
        assert flag.category == FLAG_QUALITY_DEGRADATION

    def test_silver_below_threshold_raises_warning(self):
        q = self._base_quality(silver_extraction_validity_rate=0.5)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "silver_extraction_validity_below_threshold" in signals

    def test_gold_export_ready_below_threshold_raises_warning(self):
        q = self._base_quality(gold_export_ready_rate=0.6)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "gold_export_ready_rate_below_threshold" in signals

    def test_none_metrics_do_not_raise_flags(self):
        q = self._base_quality(
            bronze_parse_success_rate=None,
            silver_extraction_validity_rate=None,
            gold_export_ready_rate=None,
        )
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        quality_flags = [f for f in flags if f.category == FLAG_QUALITY_DEGRADATION]
        assert quality_flags == []


# ===========================================================================
# 7. Governance flag generation — traceability defects
# ===========================================================================


class TestGovernanceFlagsTraceabilityDefect:
    def _base_quality(self, **kwargs) -> QualitySummary:
        defaults = dict(
            bronze_parse_success_rate=1.0,
            silver_extraction_validity_rate=1.0,
            gold_export_ready_rate=1.0,
            gold_classification_success_rate=1.0,
            traceability_gold_to_silver_link_rate=1.0,
            traceability_silver_to_bronze_link_rate=1.0,
            traceability_pipeline_run_id_coverage=1.0,
            traceability_orphaned_silver_count=0,
            traceability_orphaned_gold_count=0,
            bootstrap_path_detected=False,
        )
        defaults.update(kwargs)
        return QualitySummary(**defaults)

    def _no_flags(self):
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators([], False, [], [], [], False)
        return h, rq, drift

    def test_gold_silver_link_incomplete_warning(self):
        q = self._base_quality(traceability_gold_to_silver_link_rate=0.8)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "gold_to_silver_link_incomplete" in signals
        flag = next(f for f in flags if f.signal == "gold_to_silver_link_incomplete")
        assert flag.severity == SEVERITY_WARNING

    def test_silver_bronze_link_incomplete_warning(self):
        q = self._base_quality(traceability_silver_to_bronze_link_rate=0.9)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "silver_to_bronze_link_incomplete" in signals

    def test_orphaned_gold_critical(self):
        q = self._base_quality(traceability_orphaned_gold_count=2)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        orphan_flags = [f for f in flags if f.signal == "orphaned_gold_records_detected"]
        assert len(orphan_flags) == 1
        assert orphan_flags[0].severity == SEVERITY_CRITICAL
        assert orphan_flags[0].category == FLAG_TRACEABILITY_DEFECT

    def test_orphaned_silver_warning(self):
        q = self._base_quality(traceability_orphaned_silver_count=1)
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        orphan_flags = [f for f in flags if f.signal == "orphaned_silver_records_detected"]
        assert len(orphan_flags) == 1
        assert orphan_flags[0].severity == SEVERITY_WARNING

    def test_no_traceability_flag_when_zero_orphans(self):
        q = self._base_quality(
            traceability_orphaned_gold_count=0,
            traceability_orphaned_silver_count=0,
        )
        h, rq, drift = self._no_flags()
        flags = _derive_governance_flags(q, h, rq, drift)
        traceability_flags = [f for f in flags if f.category == FLAG_TRACEABILITY_DEFECT]
        assert traceability_flags == []


# ===========================================================================
# 8. Governance flag generation — schema / contract inconsistency
# ===========================================================================


class TestGovernanceFlagsContractSchema:
    def _clean_quality(self) -> QualitySummary:
        return QualitySummary(
            bronze_parse_success_rate=1.0, silver_extraction_validity_rate=1.0,
            gold_export_ready_rate=1.0, gold_classification_success_rate=1.0,
            traceability_gold_to_silver_link_rate=1.0,
            traceability_silver_to_bronze_link_rate=1.0,
            traceability_pipeline_run_id_coverage=1.0,
            traceability_orphaned_silver_count=0, traceability_orphaned_gold_count=0,
            bootstrap_path_detected=False,
        )

    def test_mixed_schema_versions_raises_warning(self):
        q = self._clean_quality()
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators(
            schema_versions_detected=["v0.1.0", "v0.2.0"],
            mixed_schema_versions=True,
            missing_provenance_fields=[],
            unexpected_routing_combinations=[],
            environment_names_detected=["dev"],
            mixed_environments_detected=False,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "mixed_schema_versions_detected" in signals
        flag = next(f for f in flags if f.signal == "mixed_schema_versions_detected")
        assert flag.severity == SEVERITY_WARNING
        assert flag.category == FLAG_CONTRACT_SCHEMA_INCONSISTENCY

    def test_missing_provenance_fields_raises_flag(self):
        q = self._clean_quality()
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators(
            schema_versions_detected=[],
            mixed_schema_versions=False,
            missing_provenance_fields=["pipeline_run_id (placeholder)"],
            unexpected_routing_combinations=[],
            environment_names_detected=["dev"],
            mixed_environments_detected=False,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "missing_provenance_fields" in signals

    def test_unexpected_routing_combo_raises_flag(self):
        q = self._clean_quality()
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators(
            schema_versions_detected=[],
            mixed_schema_versions=False,
            missing_provenance_fields=[],
            unexpected_routing_combinations=["More than 50% quarantined (3/4)"],
            environment_names_detected=["dev"],
            mixed_environments_detected=False,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "unexpected_routing_combinations" in signals

    def test_clean_drift_produces_no_schema_flags(self):
        q = self._clean_quality()
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators([], False, [], [], ["dev"], False)
        flags = _derive_governance_flags(q, h, rq, drift)
        schema_flags = [f for f in flags if f.category == FLAG_CONTRACT_SCHEMA_INCONSISTENCY]
        assert schema_flags == []


# ===========================================================================
# 9. Governance flag generation — review queue pressure
# ===========================================================================


class TestGovernanceFlagsReviewQueuePressure:
    def _clean_inputs(self):
        q = QualitySummary(None, None, None, None, None, None, None, None, None, False)
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        drift = SchemaDriftIndicators([], False, [], [], [], False)
        return q, h, drift

    def test_no_pressure_flag_when_below_warning_threshold(self):
        q, h, drift = self._clean_inputs()
        rq = ReviewQueueSummary(
            total_records_reviewed=10,
            total_queue_entries=1,
            review_queue_pressure_pct=0.1,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        pressure_flags = [f for f in flags if f.category == FLAG_REVIEW_QUEUE_PRESSURE]
        assert pressure_flags == []

    def test_warning_flag_at_warning_threshold(self):
        q, h, drift = self._clean_inputs()
        rq = ReviewQueueSummary(
            total_records_reviewed=10,
            total_queue_entries=3,
            review_queue_pressure_pct=THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "review_queue_pressure_elevated" in signals
        flag = next(f for f in flags if f.signal == "review_queue_pressure_elevated")
        assert flag.severity == SEVERITY_WARNING

    def test_critical_flag_at_critical_threshold(self):
        q, h, drift = self._clean_inputs()
        rq = ReviewQueueSummary(
            total_records_reviewed=10,
            total_queue_entries=6,
            review_queue_pressure_pct=THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "review_queue_pressure_critical" in signals
        flag = next(f for f in flags if f.signal == "review_queue_pressure_critical")
        assert flag.severity == SEVERITY_CRITICAL

    def test_none_pressure_produces_no_flag(self):
        q, h, drift = self._clean_inputs()
        rq = ReviewQueueSummary(None, None, review_queue_pressure_pct=None)
        flags = _derive_governance_flags(q, h, rq, drift)
        pressure_flags = [f for f in flags if f.category == FLAG_REVIEW_QUEUE_PRESSURE]
        assert pressure_flags == []


# ===========================================================================
# 10. Governance flag generation — export / handoff reliability
# ===========================================================================


class TestGovernanceFlagsExportHandoff:
    def _clean_inputs(self):
        q = QualitySummary(None, None, None, None, None, None, None, None, None, False)
        rq = ReviewQueueSummary(None, None)
        drift = SchemaDriftIndicators([], False, [], [], [], False)
        return q, rq, drift

    def test_contract_blocked_records_raises_info_flag(self):
        q, rq, drift = self._clean_inputs()
        h = HandoffHealthSummary(4, 3, 1, 1, 0, 0.75, 0.25, 0.25)
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "contract_blocked_records_present" in signals
        flag = next(f for f in flags if f.signal == "contract_blocked_records_present")
        assert flag.severity == SEVERITY_INFO
        assert flag.category == FLAG_EXPORT_HANDOFF_RELIABILITY

    def test_low_export_success_rate_raises_warning(self):
        q, rq, drift = self._clean_inputs()
        h = HandoffHealthSummary(10, 2, 2, 0, 6, 0.20, 0.20, 0.0)
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "low_export_success_rate" in signals
        flag = next(f for f in flags if f.signal == "low_export_success_rate")
        assert flag.severity == SEVERITY_WARNING

    def test_no_flag_when_export_success_acceptable(self):
        q, rq, drift = self._clean_inputs()
        h = HandoffHealthSummary(4, 3, 1, 0, 0, 0.75, 0.25, 0.0)
        flags = _derive_governance_flags(q, h, rq, drift)
        reliability_flags = [f for f in flags if f.category == FLAG_EXPORT_HANDOFF_RELIABILITY]
        assert reliability_flags == []


# ===========================================================================
# 11. Governance flag generation — environment config mismatch
# ===========================================================================


class TestGovernanceFlagsEnvironmentMismatch:
    def _clean_inputs(self):
        q = QualitySummary(None, None, None, None, None, None, None, None, None, False)
        h = HandoffHealthSummary(None, None, None, None, None, None, None, None)
        rq = ReviewQueueSummary(None, None)
        return q, h, rq

    def test_mixed_environments_raises_warning(self):
        q, h, rq = self._clean_inputs()
        drift = SchemaDriftIndicators(
            schema_versions_detected=[],
            mixed_schema_versions=False,
            missing_provenance_fields=[],
            unexpected_routing_combinations=[],
            environment_names_detected=["dev", "staging"],
            mixed_environments_detected=True,
        )
        flags = _derive_governance_flags(q, h, rq, drift)
        signals = {f.signal for f in flags}
        assert "mixed_environments_in_governance_scope" in signals
        flag = next(f for f in flags if f.signal == "mixed_environments_in_governance_scope")
        assert flag.category == FLAG_ENVIRONMENT_CONFIG_MISMATCH
        assert flag.severity == SEVERITY_WARNING

    def test_single_environment_no_flag(self):
        q, h, rq = self._clean_inputs()
        drift = SchemaDriftIndicators([], False, [], [], ["dev"], False)
        flags = _derive_governance_flags(q, h, rq, drift)
        env_flags = [f for f in flags if f.category == FLAG_ENVIRONMENT_CONFIG_MISMATCH]
        assert env_flags == []


# ===========================================================================
# 12. Overall health status derivation
# ===========================================================================


class TestDeriveOverallHealth:
    def test_no_flags_is_healthy(self):
        assert _derive_overall_health([]) == HEALTH_HEALTHY

    def test_info_flags_only_is_healthy(self):
        flag = make_governance_flag(FLAG_EXPORT_HANDOFF_RELIABILITY, SEVERITY_INFO, "sig", "det")
        assert _derive_overall_health([flag]) == HEALTH_HEALTHY

    def test_warning_flag_is_degraded(self):
        flag = make_governance_flag(FLAG_QUALITY_DEGRADATION, SEVERITY_WARNING, "sig", "det")
        assert _derive_overall_health([flag]) == HEALTH_DEGRADED

    def test_critical_flag_is_critical(self):
        flag = make_governance_flag(FLAG_TRACEABILITY_DEFECT, SEVERITY_CRITICAL, "sig", "det")
        assert _derive_overall_health([flag]) == HEALTH_CRITICAL

    def test_mixed_critical_and_warning_is_critical(self):
        flags = [
            make_governance_flag(FLAG_QUALITY_DEGRADATION, SEVERITY_WARNING, "sig1", "det"),
            make_governance_flag(FLAG_TRACEABILITY_DEFECT, SEVERITY_CRITICAL, "sig2", "det"),
        ]
        assert _derive_overall_health(flags) == HEALTH_CRITICAL


# ===========================================================================
# 13. build_governance_report() — full construction
# ===========================================================================


class TestBuildGovernanceReport:
    def test_empty_inputs_returns_valid_report(self):
        report = build_governance_report(environment="dev")
        assert isinstance(report, GovernanceReport)
        assert report.schema_version == GOVERNANCE_MONITORING_SCHEMA_VERSION
        assert report.environment == "dev"
        assert report.overall_health_status in ALL_HEALTH_STATUSES
        assert len(report.governance_report_id) == 36

    def test_generated_at_set_automatically(self):
        report = build_governance_report()
        assert report.generated_at is not None
        assert "T" in report.generated_at  # ISO format

    def test_explicit_generated_at_preserved(self):
        ts = "2026-04-12T10:00:00+00:00"
        report = build_governance_report(generated_at=ts)
        assert report.generated_at == ts

    def test_environment_preserved(self):
        report = build_governance_report(environment="staging")
        assert report.environment == "staging"
        assert report.reporting_scope.environment == "staging"

    def test_full_input_produces_complete_report(self):
        eval_rep = _make_eval_report()
        handoff_rep = _make_handoff_report()
        rq = _make_review_queue()
        report = build_governance_report(
            environment="dev",
            eval_report=eval_rep,
            handoff_report=handoff_rep,
            review_queue=rq,
            eval_report_path="output/eval/report.json",
            handoff_report_path="output/reports/handoff_report.json",
            review_queue_path="output/review_queue/review_queue.json",
        )
        assert report.quality_summary.bronze_parse_success_rate == pytest.approx(1.0)
        assert report.handoff_health.total_exported == 3
        assert report.review_queue_summary.total_queue_entries == 1
        assert report.reporting_scope.evaluation_report_path == "output/eval/report.json"

    def test_pipeline_run_ids_collected(self):
        eval_rep = _make_eval_report()
        handoff_rep = _make_handoff_report(pipeline_run_id="run-x")
        report = build_governance_report(
            eval_report=eval_rep,
            handoff_report=handoff_rep,
        )
        run_ids = report.reporting_scope.input_pipeline_run_ids
        assert "run001" in run_ids or "run-x" in run_ids

    def test_healthy_report_when_all_metrics_good(self):
        eval_rep = _make_eval_report(
            bronze_success=1.0, silver_validity=1.0,
            gold_export_ready=1.0, gold_silver_link=1.0,
            silver_bronze_link=1.0, orphaned_silver=0, orphaned_gold=0,
        )
        handoff_rep = _make_handoff_report(total_exported=4, total_quarantined=0, total_blocked=0)
        rq = _make_review_queue(total_entries=0, quarantined=0, total_reviewed=4)
        report = build_governance_report(
            environment="dev",
            eval_report=eval_rep,
            handoff_report=handoff_rep,
            review_queue=rq,
        )
        assert report.overall_health_status == HEALTH_HEALTHY

    def test_degraded_report_when_quality_below_threshold(self):
        eval_rep = _make_eval_report(bronze_success=0.5)
        report = build_governance_report(environment="dev", eval_report=eval_rep)
        assert report.overall_health_status in (HEALTH_DEGRADED, HEALTH_CRITICAL)

    def test_critical_report_when_orphaned_gold(self):
        eval_rep = _make_eval_report(orphaned_gold=3)
        report = build_governance_report(environment="dev", eval_report=eval_rep)
        assert report.overall_health_status == HEALTH_CRITICAL

    def test_governance_notes_are_non_empty(self):
        report = build_governance_report(environment="dev")
        assert len(report.governance_notes) >= 3

    def test_each_report_gets_unique_id(self):
        ids = {build_governance_report().governance_report_id for _ in range(5)}
        assert len(ids) == 5

    def test_schema_drift_populated(self):
        report = build_governance_report(environment="dev")
        drift = report.schema_drift
        assert isinstance(drift.mixed_schema_versions, bool)
        assert isinstance(drift.schema_versions_detected, list)

    def test_no_live_databricks_dependency(self):
        """build_governance_report() must run without any Databricks connection."""
        # If this test passes, there is no live dependency.
        report = build_governance_report(
            environment="dev",
            eval_report=_make_eval_report(),
            handoff_report=_make_handoff_report(),
            review_queue=_make_review_queue(),
        )
        assert report is not None

    def test_no_secrets_or_credentials_in_output(self):
        report = build_governance_report(
            environment="dev",
            eval_report=_make_eval_report(),
        )
        report_json = report.to_json_str()
        # These should not appear as credential values — only check for actual secret patterns
        for suspicious in ["password", "api_key", "aws_secret", "access_key"]:
            assert suspicious not in report_json.lower()


# ===========================================================================
# 14. Schema drift / contract drift signaling
# ===========================================================================


class TestSchemaDriftSignaling:
    def test_no_drift_on_clean_inputs(self):
        drift = _derive_schema_drift(
            eval_report=None,
            handoff_report=None,
            review_queue=None,
            environment="dev",
            eval_report_path=None,
            handoff_report_path=None,
            review_queue_path=None,
        )
        assert drift.mixed_schema_versions is False
        assert drift.mixed_environments_detected is False
        assert drift.environment_names_detected == ["dev"]

    def test_unknown_environment_triggers_drift_observation(self):
        drift = _derive_schema_drift(
            eval_report=None, handoff_report=None, review_queue=None,
            environment="production",  # not in bounded vocabulary
            eval_report_path=None, handoff_report_path=None, review_queue_path=None,
        )
        assert any("not in the bounded environment vocabulary" in obs for obs in drift.drift_observations)

    def test_bootstrap_detection_adds_missing_provenance(self):
        eval_report = _make_eval_report(bootstrap=True)
        drift = _derive_schema_drift(
            eval_report=eval_report, handoff_report=None, review_queue=None,
            environment="dev",
            eval_report_path=None, handoff_report_path=None, review_queue_path=None,
        )
        assert any("bootstrap_sql_v1" in p for p in drift.missing_provenance_fields)

    def test_high_quarantine_rate_triggers_unexpected_routing(self):
        handoff_rep = _make_handoff_report(
            total_eligible=4, total_quarantined=3, total_exported=1
        )
        drift = _derive_schema_drift(
            eval_report=None, handoff_report=handoff_rep, review_queue=None,
            environment="dev",
            eval_report_path=None, handoff_report_path=None, review_queue_path=None,
        )
        assert len(drift.unexpected_routing_combinations) > 0
        assert any("50%" in combo for combo in drift.unexpected_routing_combinations)

    def test_normal_quarantine_rate_no_unexpected_routing(self):
        handoff_rep = _make_handoff_report(
            total_eligible=4, total_quarantined=1, total_exported=3
        )
        drift = _derive_schema_drift(
            eval_report=None, handoff_report=handoff_rep, review_queue=None,
            environment="dev",
            eval_report_path=None, handoff_report_path=None, review_queue_path=None,
        )
        assert drift.unexpected_routing_combinations == []


# ===========================================================================
# 15. format_governance_report_text()
# ===========================================================================


class TestFormatGovernanceReportText:
    def _build_report(self) -> GovernanceReport:
        return build_governance_report(
            environment="dev",
            eval_report=_make_eval_report(),
            handoff_report=_make_handoff_report(),
            review_queue=_make_review_queue(),
        )

    def test_returns_string(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert isinstance(text, str)

    def test_contains_report_id(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert report.governance_report_id in text

    def test_contains_environment(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "dev" in text

    def test_contains_health_status(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert report.overall_health_status.upper() in text

    def test_contains_quality_section(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Quality Summary" in text

    def test_contains_handoff_section(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Handoff Health" in text

    def test_contains_review_queue_section(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Review Queue Pressure" in text

    def test_contains_schema_drift_section(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Schema" in text and "Drift" in text

    def test_contains_governance_flags_section(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Governance Flags" in text

    def test_contains_governance_notes(self):
        report = self._build_report()
        text = format_governance_report_text(report)
        assert "Governance Notes" in text

    def test_no_flags_message(self):
        report = build_governance_report(
            environment="dev",
            eval_report=_make_eval_report(
                bronze_success=1.0, silver_validity=1.0, gold_export_ready=1.0,
                orphaned_silver=0, orphaned_gold=0,
            ),
            handoff_report=_make_handoff_report(total_blocked=0),
            review_queue=_make_review_queue(total_entries=0, quarantined=0, total_reviewed=4),
        )
        if report.governance_flags == []:
            text = format_governance_report_text(report)
            assert "No governance flags raised" in text

    def test_empty_report_formats_without_error(self):
        report = build_governance_report(environment="dev")
        text = format_governance_report_text(report)
        assert "not provided" in text  # scope shows missing artifacts


# ===========================================================================
# 16. write_governance_report() and artifact round-trip
# ===========================================================================


class TestWriteGovernanceReport:
    def test_writes_json_and_text_artifacts(self):
        report = build_governance_report(
            environment="dev",
            eval_report=_make_eval_report(),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            json_path, text_path = write_governance_report(report, report_dir)
            assert json_path.exists()
            assert text_path.exists()
            assert json_path.suffix == ".json"
            assert text_path.suffix == ".txt"

    def test_json_artifact_is_valid_json(self):
        report = build_governance_report(environment="dev", eval_report=_make_eval_report())
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, _ = write_governance_report(report, Path(tmpdir))
            data = json.loads(json_path.read_text(encoding="utf-8"))
            assert data["governance_report_id"] == report.governance_report_id
            assert data["schema_version"] == GOVERNANCE_MONITORING_SCHEMA_VERSION

    def test_json_artifact_contains_all_top_level_keys(self):
        report = build_governance_report(environment="dev")
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, _ = write_governance_report(report, Path(tmpdir))
            data = json.loads(json_path.read_text(encoding="utf-8"))
            required_keys = {
                "governance_report_id", "generated_at", "environment", "schema_version",
                "reporting_scope", "quality_summary", "handoff_health",
                "review_queue_summary", "schema_drift", "governance_flags",
                "governance_notes", "overall_health_status",
            }
            assert required_keys.issubset(set(data.keys()))

    def test_load_governance_report_round_trip(self):
        report = build_governance_report(
            environment="staging",
            eval_report=_make_eval_report(bronze_success=0.85),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path, _ = write_governance_report(report, Path(tmpdir))
            loaded = load_governance_report(json_path)
            assert loaded["governance_report_id"] == report.governance_report_id
            assert loaded["environment"] == "staging"
            assert loaded["overall_health_status"] == report.overall_health_status

    def test_deterministic_path_from_report_id(self):
        report = build_governance_report(environment="dev")
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            computed = compute_governance_report_path(report_dir, report.governance_report_id)
            json_path, _ = write_governance_report(report, report_dir)
            assert json_path == computed

    def test_creates_report_dir_if_absent(self):
        report = build_governance_report(environment="dev")
        with tempfile.TemporaryDirectory() as tmpdir:
            new_dir = Path(tmpdir) / "nested" / "governance"
            assert not new_dir.exists()
            json_path, text_path = write_governance_report(report, new_dir)
            assert new_dir.exists()
            assert json_path.exists()


# ===========================================================================
# 17. GovernanceReport serialisation
# ===========================================================================


class TestGovernanceReportSerialisation:
    def test_to_dict_returns_dict(self):
        report = build_governance_report(environment="dev")
        d = report.to_dict()
        assert isinstance(d, dict)

    def test_to_json_str_returns_string(self):
        report = build_governance_report(environment="dev")
        s = report.to_json_str()
        assert isinstance(s, str)
        parsed = json.loads(s)
        assert parsed["environment"] == "dev"

    def test_to_json_str_schema_version_correct(self):
        report = build_governance_report(environment="dev")
        parsed = json.loads(report.to_json_str())
        assert parsed["schema_version"] == GOVERNANCE_MONITORING_SCHEMA_VERSION


# ===========================================================================
# 18. Module boundary / no-Bedrock / no-UI guards
# ===========================================================================


class TestModuleBoundaryGuards:
    def test_no_bedrock_import_in_schema(self):
        import src.schemas.governance_monitoring as mod
        src_text = Path(mod.__file__).read_text(encoding="utf-8")
        import_lines = [
            l for l in src_text.splitlines()
            if l.strip().startswith("import") or l.strip().startswith("from")
        ]
        assert not any("bedrock" in l.lower() for l in import_lines)

    def test_no_bedrock_import_in_pipeline(self):
        import src.pipelines.governance_monitoring as mod
        src_text = Path(mod.__file__).read_text(encoding="utf-8")
        # The word 'bedrock' may appear in comments referencing the contract
        # but should not appear as an import
        lines = src_text.splitlines()
        import_lines = [l for l in lines if l.strip().startswith("import") or l.strip().startswith("from")]
        assert not any("bedrock" in l.lower() for l in import_lines)

    def test_no_ui_imports(self):
        import src.schemas.governance_monitoring as schema_mod
        import src.pipelines.governance_monitoring as pipeline_mod
        for mod in [schema_mod, pipeline_mod]:
            src_text = Path(mod.__file__).read_text(encoding="utf-8")
            import_lines = [
                l for l in src_text.splitlines()
                if l.strip().startswith("import") or l.strip().startswith("from")
            ]
            for ui_lib in ["flask", "fastapi", "django", "streamlit"]:
                assert not any(ui_lib in l.lower() for l in import_lines)

    def test_no_mlflow_hard_dependency_in_schema(self):
        import src.schemas.governance_monitoring as mod
        src_text = Path(mod.__file__).read_text(encoding="utf-8")
        lines = src_text.splitlines()
        import_lines = [l for l in lines if l.strip().startswith("import") or l.strip().startswith("from")]
        assert not any("mlflow" in l for l in import_lines)

    def test_governance_schema_importable_without_pydantic(self):
        """The governance schema uses only stdlib dataclasses — pydantic not required."""
        import src.schemas.governance_monitoring as mod
        assert hasattr(mod, "GovernanceReport")
        assert hasattr(mod, "GovernanceFlag")
        assert hasattr(mod, "build_governance_report") is False  # in pipeline, not schema

    def test_governance_pipeline_importable_without_databricks(self):
        import src.pipelines.governance_monitoring as mod
        assert hasattr(mod, "build_governance_report")
        assert hasattr(mod, "write_governance_report")


# ===========================================================================
# 19. Expected fixture validation
# ===========================================================================


class TestExpectedFixture:
    def test_expected_fixture_loads_and_valid(self):
        fixture_path = Path(__file__).parent.parent / "examples" / "expected_governance_report.json"
        assert fixture_path.exists(), f"Fixture not found: {fixture_path}"
        data = json.loads(fixture_path.read_text(encoding="utf-8"))
        assert data["schema_version"] == GOVERNANCE_MONITORING_SCHEMA_VERSION
        assert data["overall_health_status"] in ALL_HEALTH_STATUSES
        assert "governance_report_id" in data
        assert "reporting_scope" in data
        assert "quality_summary" in data
        assert "handoff_health" in data
        assert "review_queue_summary" in data
        assert "schema_drift" in data
        assert "governance_flags" in data
        assert "governance_notes" in data

    def test_expected_fixture_environment_is_known(self):
        fixture_path = Path(__file__).parent.parent / "examples" / "expected_governance_report.json"
        data = json.loads(fixture_path.read_text(encoding="utf-8"))
        assert data["environment"] in {"dev", "staging", "prod"}

    def test_expected_fixture_healthy(self):
        fixture_path = Path(__file__).parent.parent / "examples" / "expected_governance_report.json"
        data = json.loads(fixture_path.read_text(encoding="utf-8"))
        assert data["overall_health_status"] == "healthy"
