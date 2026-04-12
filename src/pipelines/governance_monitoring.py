"""
src/pipelines/governance_monitoring.py — E-2 Governance Monitoring Aggregation.

Derives a GovernanceReport from existing structured pipeline artifacts.
All derivation logic is deterministic. No live Databricks workspace required.
No secrets or credentials consumed.

This module owns:
  - build_governance_report() — primary entry point; assembles GovernanceReport
    from optional input artifact dicts
  - _derive_quality_summary() — extract quality indicators from evaluation report
  - _derive_handoff_health() — extract handoff outcome health from B-4 report
  - _derive_review_queue_summary() — extract queue pressure from E-0 artifact
  - _derive_schema_drift() — detect schema/contract/environment drift across artifacts
  - _derive_governance_flags() — bounded, deterministic flag generation
  - _derive_overall_health() — determine overall health status from flags
  - format_governance_report_text() — human-readable summary string
  - write_governance_report() — write JSON + text artifacts
  - compute_governance_report_path() — deterministic artifact path

This module does NOT own:
  - Pipeline execution (classify_gold.py)
  - Evaluation runners (eval_bronze.py etc.)
  - Schema validation (bedrock_contract.py)
  - Review decisions (review_decision.py)
  - Dashboard or UI views
  - Downstream Bedrock runtime monitoring
  - Enterprise alerting infrastructure

Preferred inputs (all optional — report degrades gracefully with partial input):
  - A-4 EvaluationReport artifact dict
  - B-4 HandoffBatchReport artifact dict
  - B-5 HandoffBatchManifest artifact dict (for bundle path reference only)
  - E-0 ReviewQueueArtifact dict
  - Environment name string

Phase: E-2
Authoritative scope: PROJECT_SPEC.md § Phase E-2
Architecture context: ARCHITECTURE.md § Future Evolution (Governance monitoring)
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.governance_monitoring import (
    ALL_FLAG_CATEGORIES,
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

# ---------------------------------------------------------------------------
# Known schema versions (for drift detection)
# ---------------------------------------------------------------------------

_KNOWN_SCHEMA_VERSIONS = {"v0.1.0", "v0.2.0"}
_KNOWN_ENVIRONMENTS = {"dev", "staging", "prod"}

# ---------------------------------------------------------------------------
# Internal derivation helpers
# ---------------------------------------------------------------------------


def _derive_quality_summary(eval_report: Optional[dict]) -> QualitySummary:
    """
    Extract quality indicators from an A-4 EvaluationReport dict.

    All fields default to None when the evaluator was not run or data is absent.
    """
    if not eval_report:
        return QualitySummary(
            bronze_parse_success_rate=None,
            silver_extraction_validity_rate=None,
            gold_export_ready_rate=None,
            gold_classification_success_rate=None,
            traceability_gold_to_silver_link_rate=None,
            traceability_silver_to_bronze_link_rate=None,
            traceability_pipeline_run_id_coverage=None,
            traceability_orphaned_silver_count=None,
            traceability_orphaned_gold_count=None,
            bootstrap_path_detected=False,
            quality_observations=["No evaluation report available for this governance pass."],
        )

    observations: list[str] = []

    # Bronze
    bronze = eval_report.get("bronze") or {}
    bronze_metrics = bronze.get("metrics") or {}
    bronze_success = bronze_metrics.get("parse_success_rate")

    # Silver
    silver = eval_report.get("silver") or {}
    silver_metrics = silver.get("metrics") or {}
    silver_validity = silver_metrics.get("schema_validity_rate")

    # Gold
    gold = eval_report.get("gold") or {}
    gold_metrics = gold.get("metrics") or {}
    gold_export_ready = gold_metrics.get("export_ready_rate")
    gold_classification_success = gold_metrics.get("classification_success_rate")

    # Traceability
    traceability = eval_report.get("traceability") or {}
    trace_gold_silver = traceability.get("gold_to_silver_link_rate")
    trace_silver_bronze = traceability.get("silver_to_bronze_link_rate")
    trace_run_id_coverage = traceability.get("pipeline_run_id_coverage")
    orphaned_silver = traceability.get("orphaned_silver_count")
    orphaned_gold = traceability.get("orphaned_gold_count")

    bootstrap_detected = eval_report.get("bootstrap_path_detected", False)
    if bootstrap_detected:
        observations.append(
            "Bootstrap path detected: placeholder pipeline_run_id values present. "
            "classification_confidence may be null for bootstrap-origin records."
        )

    all_obs = list(eval_report.get("all_observations") or [])
    observations.extend(all_obs[:5])  # surface top observations, bounded

    return QualitySummary(
        bronze_parse_success_rate=bronze_success,
        silver_extraction_validity_rate=silver_validity,
        gold_export_ready_rate=gold_export_ready,
        gold_classification_success_rate=gold_classification_success,
        traceability_gold_to_silver_link_rate=trace_gold_silver,
        traceability_silver_to_bronze_link_rate=trace_silver_bronze,
        traceability_pipeline_run_id_coverage=trace_run_id_coverage,
        traceability_orphaned_silver_count=orphaned_silver,
        traceability_orphaned_gold_count=orphaned_gold,
        bootstrap_path_detected=bootstrap_detected,
        quality_observations=observations,
    )


def _derive_handoff_health(handoff_report: Optional[dict]) -> HandoffHealthSummary:
    """
    Extract handoff outcome health from a B-4 HandoffBatchReport dict.
    """
    if not handoff_report:
        return HandoffHealthSummary(
            total_records_processed=None,
            total_exported=None,
            total_quarantined=None,
            total_contract_blocked=None,
            total_skipped_not_export_ready=None,
            export_success_rate=None,
            quarantine_rate=None,
            contract_block_rate=None,
            handoff_observations=["No handoff batch report available for this governance pass."],
        )

    total_processed = handoff_report.get("total_records_processed")
    total_eligible = handoff_report.get("total_eligible")
    total_exported = handoff_report.get("total_exported")
    total_quarantined = handoff_report.get("total_quarantined")
    total_blocked = handoff_report.get("total_contract_blocked")
    total_skipped = handoff_report.get("total_skipped_not_export_ready")

    # Compute rates against eligible (the denominator that makes sense for quality signals)
    eligible = total_eligible or 0

    export_rate = (total_exported / eligible) if (eligible and total_exported is not None) else None
    quarantine_rate = (total_quarantined / eligible) if (eligible and total_quarantined is not None) else None
    block_rate = (total_blocked / eligible) if (eligible and total_blocked is not None) else None

    contract_blocked_ids = list(handoff_report.get("contract_blocked_document_ids") or [])
    quarantined_ids = list(handoff_report.get("quarantined_document_ids") or [])

    observations: list[str] = []
    if total_blocked and total_blocked > 0:
        observations.append(
            f"{total_blocked} record(s) blocked by Bedrock contract validation. "
            "Review contract_blocked_document_ids for field-level detail."
        )
    if total_quarantined and total_quarantined > 0:
        observations.append(
            f"{total_quarantined} record(s) routed to quarantine. "
            "Review the Gold artifacts for classification context."
        )

    return HandoffHealthSummary(
        total_records_processed=total_processed,
        total_exported=total_exported,
        total_quarantined=total_quarantined,
        total_contract_blocked=total_blocked,
        total_skipped_not_export_ready=total_skipped,
        export_success_rate=export_rate,
        quarantine_rate=quarantine_rate,
        contract_block_rate=block_rate,
        contract_blocked_document_ids=contract_blocked_ids,
        quarantined_document_ids=quarantined_ids,
        handoff_observations=observations,
    )


def _derive_review_queue_summary(review_queue: Optional[dict]) -> ReviewQueueSummary:
    """
    Extract review queue pressure indicators from an E-0 ReviewQueueArtifact dict.
    """
    if not review_queue:
        return ReviewQueueSummary(
            total_records_reviewed=None,
            total_queue_entries=None,
            review_queue_observations=["No review queue artifact available for this governance pass."],
        )

    total_reviewed = review_queue.get("total_records_reviewed")
    total_entries = review_queue.get("total_entries")
    entries_by_reason = dict(review_queue.get("entries_by_reason") or {})

    pressure_pct: Optional[float] = None
    if total_reviewed and total_reviewed > 0 and total_entries is not None:
        pressure_pct = total_entries / total_reviewed

    observations: list[str] = []
    if total_entries == 0:
        observations.append("No records required human review in this batch.")
    elif total_entries is not None and total_entries > 0:
        observations.append(
            f"{total_entries} record(s) entered the review queue. "
            "Review decisions should be recorded using the ReviewDecision schema."
        )

    return ReviewQueueSummary(
        total_records_reviewed=total_reviewed,
        total_queue_entries=total_entries,
        entries_by_reason=entries_by_reason,
        review_queue_pressure_pct=pressure_pct,
        review_queue_observations=observations,
    )


def _collect_schema_versions(
    eval_report: Optional[dict],
    handoff_report: Optional[dict],
    review_queue: Optional[dict],
) -> list[str]:
    """Collect all schema_version strings observed across input artifacts."""
    versions: set[str] = set()

    # Evaluation report does not carry schema_version at top level in A-4
    # Gold artifacts carry schema_version in their provenance fields
    # We extract from gold metrics if available
    if eval_report:
        gold = eval_report.get("gold") or {}
        gold_metrics = gold.get("metrics") or {}
        sv = gold_metrics.get("schema_version_distribution")
        if isinstance(sv, dict):
            versions.update(str(k) for k in sv.keys())

    # Review queue carries schema_version
    if review_queue:
        sv = review_queue.get("schema_version")
        if sv:
            versions.add(str(sv))

    return sorted(versions)


def _collect_environment_names(
    eval_report: Optional[dict],
    handoff_report: Optional[dict],
    review_queue: Optional[dict],
    explicit_env: Optional[str],
) -> list[str]:
    """Collect all environment names referenced across input artifacts."""
    envs: set[str] = set()
    if explicit_env:
        envs.add(explicit_env)
    # Artifacts do not embed env name in A-4/B-4/E-0 schemas; we rely on the
    # explicit env argument. This is the honest baseline.
    return sorted(envs)


def _derive_schema_drift(
    eval_report: Optional[dict],
    handoff_report: Optional[dict],
    review_queue: Optional[dict],
    environment: str,
    eval_report_path: Optional[str],
    handoff_report_path: Optional[str],
    review_queue_path: Optional[str],
) -> SchemaDriftIndicators:
    """
    Derive schema / contract / environment drift signals from artifact metadata.

    This does not require re-running the pipeline. It examines the metadata
    embedded in the artifact dicts and flags anomalies deterministically.
    """
    drift_observations: list[str] = []
    missing_provenance: list[str] = []
    unexpected_routing: list[str] = []

    schema_versions = _collect_schema_versions(eval_report, handoff_report, review_queue)
    mixed_schema = len(schema_versions) > 1

    if mixed_schema:
        drift_observations.append(
            f"Mixed schema versions detected across artifacts: {schema_versions}. "
            "Artifacts produced at different schema versions should be reviewed "
            "for provenance field completeness."
        )

    env_names = _collect_environment_names(eval_report, handoff_report, review_queue, environment)
    mixed_envs = len(env_names) > 1

    if mixed_envs:
        drift_observations.append(
            f"Multiple environment names referenced in this governance pass: {env_names}. "
            "Governance reports should aggregate artifacts from a single environment."
        )

    if environment not in _KNOWN_ENVIRONMENTS:
        drift_observations.append(
            f"Environment '{environment}' is not in the bounded environment vocabulary "
            f"{sorted(_KNOWN_ENVIRONMENTS)}. "
            "Use 'dev', 'staging', or 'prod'."
        )

    # Check for missing provenance fields in gold export metrics
    if eval_report:
        gold = eval_report.get("gold") or {}
        gold_metrics = gold.get("metrics") or {}
        # If pipeline_run_id_coverage < 1.0, that's a provenance gap
        if eval_report.get("traceability"):
            trace = eval_report["traceability"]
            run_id_coverage = trace.get("pipeline_run_id_coverage")
            if run_id_coverage is not None and run_id_coverage < 1.0:
                missing_provenance.append("pipeline_run_id (coverage < 100%)")

        # Detect placeholder run ID as a provenance concern
        if eval_report.get("bootstrap_path_detected"):
            missing_provenance.append(
                "pipeline_run_id (placeholder 'bootstrap_sql_v1' values present — "
                "not real MLflow run IDs)"
            )

    if missing_provenance:
        drift_observations.append(
            "Missing or placeholder provenance fields detected: "
            + "; ".join(missing_provenance)
        )

    # Unexpected routing: detect if quarantine rate is anomalously high
    # (signals classification/routing contract pressure)
    if handoff_report:
        total_eligible = handoff_report.get("total_eligible") or 0
        total_quarantined = handoff_report.get("total_quarantined") or 0
        if total_eligible > 0 and total_quarantined / total_eligible > 0.5:
            unexpected_routing.append(
                f"More than 50% of eligible records quarantined "
                f"({total_quarantined}/{total_eligible}) — "
                "review classification taxonomy and routing logic."
            )

    return SchemaDriftIndicators(
        schema_versions_detected=schema_versions,
        mixed_schema_versions=mixed_schema,
        missing_provenance_fields=missing_provenance,
        unexpected_routing_combinations=unexpected_routing,
        environment_names_detected=env_names,
        mixed_environments_detected=mixed_envs,
        drift_observations=drift_observations,
    )


def _derive_governance_flags(
    quality: QualitySummary,
    handoff: HandoffHealthSummary,
    review_queue: ReviewQueueSummary,
    drift: SchemaDriftIndicators,
) -> list[GovernanceFlag]:
    """
    Deterministically derive governance flags from the four sub-summaries.

    All flags are derived from the summaries, not from raw artifact reads.
    Flag generation is bounded, deterministic, and testable.
    """
    flags: list[GovernanceFlag] = []

    # --- Quality degradation flags ---

    if (
        quality.bronze_parse_success_rate is not None
        and quality.bronze_parse_success_rate < THRESHOLD_BRONZE_PARSE_SUCCESS
    ):
        flags.append(make_governance_flag(
            category=FLAG_QUALITY_DEGRADATION,
            severity=SEVERITY_WARNING,
            signal="bronze_parse_success_below_threshold",
            detail=(
                f"Bronze parse success rate {quality.bronze_parse_success_rate:.1%} is below "
                f"the {THRESHOLD_BRONZE_PARSE_SUCCESS:.0%} threshold. "
                "Review Bronze artifacts for parse failures."
            ),
        ))

    if (
        quality.silver_extraction_validity_rate is not None
        and quality.silver_extraction_validity_rate < THRESHOLD_SILVER_VALIDITY
    ):
        flags.append(make_governance_flag(
            category=FLAG_QUALITY_DEGRADATION,
            severity=SEVERITY_WARNING,
            signal="silver_extraction_validity_below_threshold",
            detail=(
                f"Silver extraction validity rate {quality.silver_extraction_validity_rate:.1%} "
                f"is below the {THRESHOLD_SILVER_VALIDITY:.0%} threshold. "
                "Review extraction prompts and Silver schema alignment."
            ),
        ))

    if (
        quality.gold_export_ready_rate is not None
        and quality.gold_export_ready_rate < THRESHOLD_GOLD_EXPORT_READY
    ):
        flags.append(make_governance_flag(
            category=FLAG_QUALITY_DEGRADATION,
            severity=SEVERITY_WARNING,
            signal="gold_export_ready_rate_below_threshold",
            detail=(
                f"Gold export-ready rate {quality.gold_export_ready_rate:.1%} is below "
                f"the {THRESHOLD_GOLD_EXPORT_READY:.0%} threshold. "
                "Review classification taxonomy and routing rules."
            ),
        ))

    # --- Traceability defect flags ---

    if (
        quality.traceability_gold_to_silver_link_rate is not None
        and quality.traceability_gold_to_silver_link_rate < THRESHOLD_TRACEABILITY_LINK
    ):
        flags.append(make_governance_flag(
            category=FLAG_TRACEABILITY_DEFECT,
            severity=SEVERITY_WARNING,
            signal="gold_to_silver_link_incomplete",
            detail=(
                f"Gold→Silver traceability link rate "
                f"{quality.traceability_gold_to_silver_link_rate:.1%} is below 100%. "
                "Some Gold records cannot be traced to their Silver origin."
            ),
        ))

    if (
        quality.traceability_silver_to_bronze_link_rate is not None
        and quality.traceability_silver_to_bronze_link_rate < THRESHOLD_TRACEABILITY_LINK
    ):
        flags.append(make_governance_flag(
            category=FLAG_TRACEABILITY_DEFECT,
            severity=SEVERITY_WARNING,
            signal="silver_to_bronze_link_incomplete",
            detail=(
                f"Silver→Bronze traceability link rate "
                f"{quality.traceability_silver_to_bronze_link_rate:.1%} is below 100%. "
                "Some Silver records cannot be traced to their Bronze origin."
            ),
        ))

    if quality.traceability_orphaned_gold_count and quality.traceability_orphaned_gold_count > 0:
        flags.append(make_governance_flag(
            category=FLAG_TRACEABILITY_DEFECT,
            severity=SEVERITY_CRITICAL,
            signal="orphaned_gold_records_detected",
            detail=(
                f"{quality.traceability_orphaned_gold_count} orphaned Gold record(s) detected "
                "— no matching Silver record found. "
                "Full lineage is broken for these records."
            ),
        ))

    if quality.traceability_orphaned_silver_count and quality.traceability_orphaned_silver_count > 0:
        flags.append(make_governance_flag(
            category=FLAG_TRACEABILITY_DEFECT,
            severity=SEVERITY_WARNING,
            signal="orphaned_silver_records_detected",
            detail=(
                f"{quality.traceability_orphaned_silver_count} orphaned Silver record(s) detected "
                "— no matching Bronze record found."
            ),
        ))

    # --- Contract / schema inconsistency flags ---

    if drift.mixed_schema_versions:
        flags.append(make_governance_flag(
            category=FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
            severity=SEVERITY_WARNING,
            signal="mixed_schema_versions_detected",
            detail=(
                f"Multiple schema versions detected across artifacts: "
                f"{drift.schema_versions_detected}. "
                "Ensure artifacts in the same governance scope share a consistent schema version."
            ),
        ))

    if drift.missing_provenance_fields:
        flags.append(make_governance_flag(
            category=FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
            severity=SEVERITY_WARNING,
            signal="missing_provenance_fields",
            detail=(
                "Missing or incomplete provenance fields detected: "
                + "; ".join(drift.missing_provenance_fields)
                + ". Provenance is required for full traceability and Bedrock contract compliance."
            ),
        ))

    if drift.unexpected_routing_combinations:
        flags.append(make_governance_flag(
            category=FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
            severity=SEVERITY_WARNING,
            signal="unexpected_routing_combinations",
            detail=(
                "Unexpected document/routing patterns detected: "
                + "; ".join(drift.unexpected_routing_combinations)
            ),
        ))

    # --- Review queue pressure flags ---

    rq_pressure = review_queue.review_queue_pressure_pct
    if rq_pressure is not None:
        if rq_pressure >= THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL:
            flags.append(make_governance_flag(
                category=FLAG_REVIEW_QUEUE_PRESSURE,
                severity=SEVERITY_CRITICAL,
                signal="review_queue_pressure_critical",
                detail=(
                    f"Review queue pressure {rq_pressure:.1%} exceeds critical threshold "
                    f"({THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL:.0%}). "
                    "Immediate review of queued records is recommended."
                ),
            ))
        elif rq_pressure >= THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING:
            flags.append(make_governance_flag(
                category=FLAG_REVIEW_QUEUE_PRESSURE,
                severity=SEVERITY_WARNING,
                signal="review_queue_pressure_elevated",
                detail=(
                    f"Review queue pressure {rq_pressure:.1%} exceeds warning threshold "
                    f"({THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING:.0%}). "
                    "Consider reviewing queued records before the next pipeline run."
                ),
            ))

    # --- Export / handoff reliability concern flags ---

    if handoff.total_contract_blocked and handoff.total_contract_blocked > 0:
        flags.append(make_governance_flag(
            category=FLAG_EXPORT_HANDOFF_RELIABILITY,
            severity=SEVERITY_INFO,
            signal="contract_blocked_records_present",
            detail=(
                f"{handoff.total_contract_blocked} record(s) blocked by Bedrock contract validation. "
                "Export artifacts were NOT written for these records. "
                "Review contract_blocked_document_ids for field-level detail."
            ),
        ))

    if handoff.export_success_rate is not None and handoff.export_success_rate < 0.5:
        flags.append(make_governance_flag(
            category=FLAG_EXPORT_HANDOFF_RELIABILITY,
            severity=SEVERITY_WARNING,
            signal="low_export_success_rate",
            detail=(
                f"Export success rate {handoff.export_success_rate:.1%} is below 50% of eligible records. "
                "Review classification, contract, and routing rules."
            ),
        ))

    # --- Environment config mismatch flags ---

    if drift.mixed_environments_detected:
        flags.append(make_governance_flag(
            category=FLAG_ENVIRONMENT_CONFIG_MISMATCH,
            severity=SEVERITY_WARNING,
            signal="mixed_environments_in_governance_scope",
            detail=(
                f"Multiple environment names detected in governance scope: "
                f"{drift.environment_names_detected}. "
                "Governance reports should aggregate artifacts from a single named environment."
            ),
        ))

    return flags


def _derive_overall_health(flags: list[GovernanceFlag]) -> str:
    """Compute overall health status from the full flag list."""
    if any(f.severity == SEVERITY_CRITICAL for f in flags):
        return HEALTH_CRITICAL
    if any(f.severity == SEVERITY_WARNING for f in flags):
        return HEALTH_DEGRADED
    return HEALTH_HEALTHY


def _build_governance_notes(
    environment: str,
    reporting_scope: "ReportingScope",
    quality: QualitySummary,
    handoff: HandoffHealthSummary,
    review_queue: ReviewQueueSummary,
    drift: SchemaDriftIndicators,
    flags: list[GovernanceFlag],
    overall_health: str,
) -> list[str]:
    """Build human-readable governance notes for the report."""
    notes: list[str] = []

    notes.append(
        f"Governance report generated for environment: '{environment}'. "
        f"Overall health status: {overall_health.upper()}."
    )

    available = []
    if reporting_scope.evaluation_report_path:
        available.append("evaluation report (A-4)")
    if reporting_scope.handoff_report_path:
        available.append("handoff batch report (B-4)")
    if reporting_scope.handoff_bundle_path:
        available.append("handoff bundle manifest (B-5)")
    if reporting_scope.review_queue_path:
        available.append("review queue artifact (E-0)")

    if available:
        notes.append("Input artifacts consumed: " + ", ".join(available) + ".")
    else:
        notes.append(
            "No input artifacts provided. Governance report is empty — "
            "run evaluation, handoff, and review queue pipelines first."
        )

    n_flags = len(flags)
    n_critical = sum(1 for f in flags if f.severity == SEVERITY_CRITICAL)
    n_warning = sum(1 for f in flags if f.severity == SEVERITY_WARNING)
    n_info = sum(1 for f in flags if f.severity == SEVERITY_INFO)
    notes.append(
        f"{n_flags} governance flag(s) derived: "
        f"{n_critical} critical, {n_warning} warning, {n_info} info."
    )

    notes.append(
        "This governance report is an upstream-only artifact. It summarizes pipeline "
        "quality and operational state derived from existing structured outputs. "
        "It does not represent a dashboard, cross-case analytics product, or "
        "downstream Bedrock runtime monitoring layer."
    )
    notes.append(
        "Governance signals are derived deterministically from evaluation, handoff, "
        "review queue, and environment artifacts. No live Databricks workspace or "
        "external credentials are required to produce this report."
    )

    return notes


# ---------------------------------------------------------------------------
# Primary entry point
# ---------------------------------------------------------------------------


def build_governance_report(
    environment: str = "dev",
    eval_report: Optional[dict] = None,
    handoff_report: Optional[dict] = None,
    handoff_bundle: Optional[dict] = None,
    review_queue: Optional[dict] = None,
    eval_report_path: Optional[str] = None,
    handoff_report_path: Optional[str] = None,
    handoff_bundle_path: Optional[str] = None,
    review_queue_path: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> GovernanceReport:
    """
    Build a GovernanceReport from existing structured pipeline artifacts.

    All artifact inputs are optional. The report degrades gracefully when
    artifacts are absent — metrics are None and observations note the gap.

    Args:
        environment:
            One of 'dev', 'staging', 'prod'. Defaults to 'dev'.
        eval_report:
            A-4 EvaluationReport dict (from report_<id>.json).
        handoff_report:
            B-4 HandoffBatchReport dict.
        handoff_bundle:
            B-5 HandoffBatchManifest dict (used for bundle_path reference only).
        review_queue:
            E-0 ReviewQueueArtifact dict.
        eval_report_path:
            Optional filesystem path to the evaluation report artifact.
        handoff_report_path:
            Optional filesystem path to the handoff report artifact.
        handoff_bundle_path:
            Optional filesystem path to the handoff bundle artifact.
        review_queue_path:
            Optional filesystem path to the review queue artifact.
        generated_at:
            ISO 8601 UTC timestamp. Defaults to UTC now.

    Returns:
        GovernanceReport — fully populated, schema-valid governance artifact.
    """
    if generated_at is None:
        generated_at = datetime.now(tz=timezone.utc).isoformat()

    # Collect pipeline_run_ids from available artifacts
    run_ids: list[str] = []
    if eval_report and eval_report.get("pipeline_run_id_filter"):
        run_ids.append(eval_report["pipeline_run_id_filter"])
    if handoff_report and handoff_report.get("pipeline_run_id"):
        pid = handoff_report["pipeline_run_id"]
        if pid not in run_ids:
            run_ids.append(pid)
    if review_queue and review_queue.get("pipeline_run_id"):
        pid = review_queue["pipeline_run_id"]
        if pid not in run_ids:
            run_ids.append(pid)

    # Bundle path from B-5 manifest or explicit arg
    effective_bundle_path = handoff_bundle_path
    if not effective_bundle_path and handoff_bundle:
        effective_bundle_path = str(handoff_bundle.get("batch_id", ""))

    reporting_scope = ReportingScope(
        evaluation_report_path=eval_report_path,
        handoff_report_path=handoff_report_path,
        handoff_bundle_path=effective_bundle_path,
        review_queue_path=review_queue_path,
        environment=environment,
        input_pipeline_run_ids=run_ids,
    )

    quality = _derive_quality_summary(eval_report)
    handoff_health = _derive_handoff_health(handoff_report)
    rq_summary = _derive_review_queue_summary(review_queue)
    drift = _derive_schema_drift(
        eval_report=eval_report,
        handoff_report=handoff_report,
        review_queue=review_queue,
        environment=environment,
        eval_report_path=eval_report_path,
        handoff_report_path=handoff_report_path,
        review_queue_path=review_queue_path,
    )
    flags = _derive_governance_flags(quality, handoff_health, rq_summary, drift)
    overall_health = _derive_overall_health(flags)

    notes = _build_governance_notes(
        environment=environment,
        reporting_scope=reporting_scope,
        quality=quality,
        handoff=handoff_health,
        review_queue=rq_summary,
        drift=drift,
        flags=flags,
        overall_health=overall_health,
    )

    return GovernanceReport(
        governance_report_id=make_governance_report_id(),
        generated_at=generated_at,
        environment=environment,
        schema_version=GOVERNANCE_MONITORING_SCHEMA_VERSION,
        reporting_scope=reporting_scope,
        quality_summary=quality,
        handoff_health=handoff_health,
        review_queue_summary=rq_summary,
        schema_drift=drift,
        governance_flags=flags,
        governance_notes=notes,
        overall_health_status=overall_health,
    )


# ---------------------------------------------------------------------------
# Artifact path computation
# ---------------------------------------------------------------------------


def compute_governance_report_path(report_dir: Path, report_id: str) -> Path:
    """
    Compute the deterministic governance report artifact JSON path.

    Path: <report_dir>/governance_report_<report_id>.json
    """
    safe_id = report_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    return report_dir / f"governance_report_{safe_id}.json"


# ---------------------------------------------------------------------------
# Format helper
# ---------------------------------------------------------------------------

_SEP = "=" * 72
_SUBSEP = "-" * 60


def format_governance_report_text(report: GovernanceReport) -> str:
    """
    Format a GovernanceReport as a human-readable text summary.

    Suitable for terminal output and the .txt governance report artifact.
    """
    lines = [
        _SEP,
        "  CaseOps Pipeline — Governance Monitoring Report (E-2)",
        _SEP,
        f"  Report ID        : {report.governance_report_id}",
        f"  Generated at     : {report.generated_at}",
        f"  Environment      : {report.environment}",
        f"  Schema version   : {report.schema_version}",
        f"  Health status    : {report.overall_health_status.upper()}",
        _SEP,
        "",
    ]

    # Reporting scope
    lines += [
        _SUBSEP,
        "  Reporting Scope",
        _SUBSEP,
    ]
    scope = report.reporting_scope
    lines.append(f"  Environment            : {scope.environment}")
    lines.append(f"  Evaluation report      : {scope.evaluation_report_path or 'not provided'}")
    lines.append(f"  Handoff report (B-4)   : {scope.handoff_report_path or 'not provided'}")
    lines.append(f"  Handoff bundle (B-5)   : {scope.handoff_bundle_path or 'not provided'}")
    lines.append(f"  Review queue (E-0)     : {scope.review_queue_path or 'not provided'}")
    if scope.input_pipeline_run_ids:
        lines.append(f"  Pipeline run IDs       : {', '.join(scope.input_pipeline_run_ids)}")
    lines.append("")

    # Quality summary
    lines += [_SUBSEP, "  Quality Summary", _SUBSEP]
    q = report.quality_summary
    _fmtr = lambda v: f"{v:.1%}" if v is not None else "N/A"
    lines += [
        f"  Bronze parse success rate      : {_fmtr(q.bronze_parse_success_rate)}",
        f"  Silver extraction validity     : {_fmtr(q.silver_extraction_validity_rate)}",
        f"  Gold export-ready rate         : {_fmtr(q.gold_export_ready_rate)}",
        f"  Gold classification success    : {_fmtr(q.gold_classification_success_rate)}",
        f"  Traceability Gold→Silver       : {_fmtr(q.traceability_gold_to_silver_link_rate)}",
        f"  Traceability Silver→Bronze     : {_fmtr(q.traceability_silver_to_bronze_link_rate)}",
        f"  Pipeline run_id coverage       : {_fmtr(q.traceability_pipeline_run_id_coverage)}",
        f"  Orphaned Silver records        : {q.traceability_orphaned_silver_count if q.traceability_orphaned_silver_count is not None else 'N/A'}",
        f"  Orphaned Gold records          : {q.traceability_orphaned_gold_count if q.traceability_orphaned_gold_count is not None else 'N/A'}",
        f"  Bootstrap path detected        : {'YES' if q.bootstrap_path_detected else 'no'}",
    ]
    if q.quality_observations:
        lines.append("")
        lines.append("  Observations:")
        for obs in q.quality_observations:
            lines.append(f"    [note] {obs}")
    lines.append("")

    # Handoff health
    lines += [_SUBSEP, "  Handoff Health (B-4)", _SUBSEP]
    h = report.handoff_health
    lines += [
        f"  Total processed           : {h.total_records_processed if h.total_records_processed is not None else 'N/A'}",
        f"  Total exported            : {h.total_exported if h.total_exported is not None else 'N/A'}",
        f"  Total quarantined         : {h.total_quarantined if h.total_quarantined is not None else 'N/A'}",
        f"  Total contract blocked    : {h.total_contract_blocked if h.total_contract_blocked is not None else 'N/A'}",
        f"  Total skipped             : {h.total_skipped_not_export_ready if h.total_skipped_not_export_ready is not None else 'N/A'}",
        f"  Export success rate       : {_fmtr(h.export_success_rate)}",
        f"  Quarantine rate           : {_fmtr(h.quarantine_rate)}",
        f"  Contract block rate       : {_fmtr(h.contract_block_rate)}",
    ]
    if h.handoff_observations:
        lines.append("")
        lines.append("  Observations:")
        for obs in h.handoff_observations:
            lines.append(f"    [note] {obs}")
    lines.append("")

    # Review queue
    lines += [_SUBSEP, "  Review Queue Pressure (E-0)", _SUBSEP]
    rq = report.review_queue_summary
    lines += [
        f"  Records reviewed          : {rq.total_records_reviewed if rq.total_records_reviewed is not None else 'N/A'}",
        f"  Queue entries             : {rq.total_queue_entries if rq.total_queue_entries is not None else 'N/A'}",
        f"  Queue pressure            : {_fmtr(rq.review_queue_pressure_pct)}",
    ]
    if rq.entries_by_reason:
        lines.append("  Entries by reason:")
        for reason, count in rq.entries_by_reason.items():
            lines.append(f"    {reason}: {count}")
    if rq.review_queue_observations:
        lines.append("")
        lines.append("  Observations:")
        for obs in rq.review_queue_observations:
            lines.append(f"    [note] {obs}")
    lines.append("")

    # Schema drift
    lines += [_SUBSEP, "  Schema / Contract Drift Indicators", _SUBSEP]
    d = report.schema_drift
    lines += [
        f"  Schema versions detected  : {d.schema_versions_detected or ['none detected']}",
        f"  Mixed schema versions     : {'YES' if d.mixed_schema_versions else 'no'}",
        f"  Missing provenance fields : {d.missing_provenance_fields or ['none']}",
        f"  Unexpected routing combos : {d.unexpected_routing_combinations or ['none']}",
        f"  Environments detected     : {d.environment_names_detected or ['none']}",
        f"  Mixed environments        : {'YES' if d.mixed_environments_detected else 'no'}",
    ]
    if d.drift_observations:
        lines.append("")
        lines.append("  Drift observations:")
        for obs in d.drift_observations:
            lines.append(f"    [note] {obs}")
    lines.append("")

    # Governance flags
    lines += [_SUBSEP, f"  Governance Flags ({len(report.governance_flags)} total)", _SUBSEP]
    if report.governance_flags:
        for flag in report.governance_flags:
            lines.append(
                f"  [{flag.severity.upper()}] {flag.category} / {flag.signal}"
            )
            lines.append(f"    {flag.detail}")
    else:
        lines.append("  No governance flags raised. All thresholds met.")
    lines.append("")

    # Governance notes
    if report.governance_notes:
        lines += [_SUBSEP, "  Governance Notes", _SUBSEP]
        for note in report.governance_notes:
            lines.append(f"  - {note}")
        lines.append("")

    lines += [_SEP, ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------


def write_governance_report(
    report: GovernanceReport,
    report_dir: Path,
) -> tuple[Path, Path]:
    """
    Write a GovernanceReport to JSON and text artifacts.

    Artifact names:
        <report_dir>/governance_report_<report_id>.json
        <report_dir>/governance_report_<report_id>.txt

    Args:
        report     : The GovernanceReport to write.
        report_dir : Directory to write artifacts into. Created if absent.

    Returns:
        (json_path, text_path): Paths of the written artifacts.
    """
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = compute_governance_report_path(report_dir, report.governance_report_id)
    safe_id = report.governance_report_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    text_path = report_dir / f"governance_report_{safe_id}.txt"

    json_path.write_text(report.to_json_str(), encoding="utf-8")
    text_path.write_text(format_governance_report_text(report), encoding="utf-8")

    return json_path, text_path


# ---------------------------------------------------------------------------
# Artifact loader helpers
# ---------------------------------------------------------------------------


def load_artifact_json(path: Path) -> dict:
    """Load any JSON artifact file and return the raw dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def load_governance_report(json_path: Path) -> dict:
    """Load a GovernanceReport JSON file and return the raw dict."""
    return json.loads(json_path.read_text(encoding="utf-8"))
