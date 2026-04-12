"""
src/schemas/governance_monitoring.py — E-2 Governance Monitoring Schema.

Defines the structured governance monitoring artifact for the Databricks CaseOps
Lakehouse pipeline. This schema captures a batch-level, governance-level health
summary derived from existing evaluation, handoff, review queue, and environment
artifacts.

This is an upstream-only governance artifact. It does not own:
  - Pipeline execution logic
  - Evaluation runners
  - Dashboards, UI, or frontend views
  - Downstream Bedrock runtime monitoring
  - Enterprise alerting infrastructure

It owns:
  - The canonical schema for a governance monitoring report
  - Bounded governance signal vocabulary (flags, categories, severities)
  - Schema/contract drift indicator definitions
  - Serialization helpers

Phase: E-2
Authoritative scope: PROJECT_SPEC.md § Phase E-2
Architecture context: ARCHITECTURE.md § Phase E-1 (governance monitoring is E-2)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Schema version
# ---------------------------------------------------------------------------

GOVERNANCE_MONITORING_SCHEMA_VERSION = "v1.0.0"

# ---------------------------------------------------------------------------
# Governance flag vocabulary — bounded, explicit, testable
# ---------------------------------------------------------------------------

# Flag categories (bounded set — do not add without updating tests and docs)

FLAG_QUALITY_DEGRADATION = "quality_degradation"
"""Bronze parse success, Silver extraction validity, or Gold export-ready rate below threshold."""

FLAG_TRACEABILITY_DEFECT = "traceability_defect"
"""Orphaned records, broken lineage links, or missing pipeline_run_id coverage."""

FLAG_CONTRACT_SCHEMA_INCONSISTENCY = "contract_schema_inconsistency"
"""Mixed schema versions across artifacts, missing required provenance fields,
or unexpected document type / routing label combinations."""

FLAG_REVIEW_QUEUE_PRESSURE = "review_queue_pressure"
"""High quarantine rate, high contract-blocked rate, or elevated review queue
entry fraction indicating operational strain on the manual review path."""

FLAG_EXPORT_HANDOFF_RELIABILITY = "export_handoff_reliability_concern"
"""Export or handoff artifacts show reliability concerns: contract blocks,
missing export paths on export-ready records, or bundle integrity failures."""

FLAG_ENVIRONMENT_CONFIG_MISMATCH = "environment_config_mismatch"
"""Environment name inconsistency: artifacts produced under different environment
names in a single governance report, or environment values that do not match
the bounded vocabulary (dev / staging / prod)."""

ALL_FLAG_CATEGORIES: tuple[str, ...] = (
    FLAG_QUALITY_DEGRADATION,
    FLAG_TRACEABILITY_DEFECT,
    FLAG_CONTRACT_SCHEMA_INCONSISTENCY,
    FLAG_REVIEW_QUEUE_PRESSURE,
    FLAG_EXPORT_HANDOFF_RELIABILITY,
    FLAG_ENVIRONMENT_CONFIG_MISMATCH,
)

# Flag severities

SEVERITY_INFO = "info"
"""Noteworthy signal — no immediate action required."""

SEVERITY_WARNING = "warning"
"""Threshold breach or quality concern — should be reviewed."""

SEVERITY_CRITICAL = "critical"
"""Significant defect or pressure — requires prompt attention."""

ALL_SEVERITIES: tuple[str, ...] = (SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_CRITICAL)

# Overall health status vocabulary

HEALTH_HEALTHY = "healthy"
"""No warnings or critical flags. All thresholds met."""

HEALTH_DEGRADED = "degraded"
"""One or more warning-level flags. Pipeline functional but quality requires attention."""

HEALTH_CRITICAL = "critical"
"""One or more critical-level flags. Immediate review recommended."""

ALL_HEALTH_STATUSES: tuple[str, ...] = (HEALTH_HEALTHY, HEALTH_DEGRADED, HEALTH_CRITICAL)

# ---------------------------------------------------------------------------
# Quality threshold constants
# ---------------------------------------------------------------------------

THRESHOLD_BRONZE_PARSE_SUCCESS = 0.9
"""Bronze parse success rate below this triggers a quality_degradation flag."""

THRESHOLD_SILVER_VALIDITY = 0.8
"""Silver extraction validity rate below this triggers a quality_degradation flag."""

THRESHOLD_GOLD_EXPORT_READY = 0.7
"""Gold export-ready rate below this triggers a quality_degradation flag."""

THRESHOLD_TRACEABILITY_LINK = 1.0
"""Traceability link rate below this triggers a traceability_defect warning."""

THRESHOLD_REVIEW_QUEUE_PRESSURE_WARNING = 0.20
"""Review queue pressure (queued / eligible) at or above this triggers a warning."""

THRESHOLD_REVIEW_QUEUE_PRESSURE_CRITICAL = 0.50
"""Review queue pressure at or above this triggers a critical flag."""

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


@dataclass
class GovernanceFlag:
    """
    A single bounded governance signal derived from the input artifacts.

    Fields:
        flag_id       : UUID for this flag instance.
        category      : One of ALL_FLAG_CATEGORIES.
        severity      : One of ALL_SEVERITIES.
        signal        : Short machine-readable signal key (stable, testable).
        detail        : Human-readable description of the specific condition.
    """

    flag_id: str
    category: str
    severity: str
    signal: str
    detail: str


@dataclass
class QualitySummary:
    """
    Quality indicators derived from the A-4 evaluation report artifacts.

    All rate fields are Optional[float] in [0.0, 1.0].
    None means the metric was not available (e.g. evaluator not run).
    """

    bronze_parse_success_rate: Optional[float]
    silver_extraction_validity_rate: Optional[float]
    gold_export_ready_rate: Optional[float]
    gold_classification_success_rate: Optional[float]
    traceability_gold_to_silver_link_rate: Optional[float]
    traceability_silver_to_bronze_link_rate: Optional[float]
    traceability_pipeline_run_id_coverage: Optional[float]
    traceability_orphaned_silver_count: Optional[int]
    traceability_orphaned_gold_count: Optional[int]
    bootstrap_path_detected: bool
    quality_observations: list[str] = field(default_factory=list)


@dataclass
class HandoffHealthSummary:
    """
    Handoff outcome health derived from the B-4 HandoffBatchReport artifact.

    All rate fields are Optional[float]. None means not available.
    """

    total_records_processed: Optional[int]
    total_exported: Optional[int]
    total_quarantined: Optional[int]
    total_contract_blocked: Optional[int]
    total_skipped_not_export_ready: Optional[int]
    export_success_rate: Optional[float]
    quarantine_rate: Optional[float]
    contract_block_rate: Optional[float]
    contract_blocked_document_ids: list[str] = field(default_factory=list)
    quarantined_document_ids: list[str] = field(default_factory=list)
    handoff_observations: list[str] = field(default_factory=list)


@dataclass
class ReviewQueueSummary:
    """
    Review queue pressure indicators derived from the E-0 ReviewQueueArtifact.
    """

    total_records_reviewed: Optional[int]
    total_queue_entries: Optional[int]
    entries_by_reason: dict[str, int] = field(default_factory=dict)
    review_queue_pressure_pct: Optional[float] = None
    review_queue_observations: list[str] = field(default_factory=list)


@dataclass
class SchemaDriftIndicators:
    """
    Schema version and contract drift signals derived from artifact metadata.

    Detects:
      - Mixed schema versions across artifacts in the same report
      - Missing required provenance fields in export-ready artifacts
      - Environment / catalog name inconsistencies across artifacts
      - Unexpected document type / routing label combinations
    """

    schema_versions_detected: list[str]
    mixed_schema_versions: bool
    missing_provenance_fields: list[str]
    unexpected_routing_combinations: list[str]
    environment_names_detected: list[str]
    mixed_environments_detected: bool
    drift_observations: list[str] = field(default_factory=list)


@dataclass
class ReportingScope:
    """
    Describes the input artifacts used to build this governance report.

    All path fields are Optional[str] — None means that artifact type was not
    available for this report. The report is still valid; coverage is reduced.
    """

    evaluation_report_path: Optional[str]
    handoff_report_path: Optional[str]
    handoff_bundle_path: Optional[str]
    review_queue_path: Optional[str]
    environment: str
    input_pipeline_run_ids: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Top-level governance report
# ---------------------------------------------------------------------------


@dataclass
class GovernanceReport:
    """
    Top-level governance monitoring artifact for one reporting pass.

    Derived deterministically from existing evaluation, handoff, review queue,
    and environment artifacts. Machine-readable (JSON) and human-auditable (text).

    This artifact is upstream-only. It does not contain downstream Bedrock
    runtime data, dashboard views, or enterprise alerting configuration.

    Fields:
        governance_report_id : UUID for this report instance.
        generated_at         : ISO-8601 UTC timestamp.
        environment          : One of 'dev', 'staging', 'prod'.
        schema_version       : Always GOVERNANCE_MONITORING_SCHEMA_VERSION.
        reporting_scope      : What input artifacts were consumed.
        quality_summary      : Quality indicators from evaluation artifacts.
        handoff_health       : Handoff outcome health from B-4 report.
        review_queue_summary : Review queue pressure from E-0 artifact.
        schema_drift         : Schema/contract drift indicators.
        governance_flags     : Bounded list of derived governance signals.
        governance_notes     : Human-readable notes describing the report scope.
        overall_health_status: One of 'healthy', 'degraded', 'critical'.
    """

    governance_report_id: str
    generated_at: str
    environment: str
    schema_version: str

    reporting_scope: ReportingScope
    quality_summary: QualitySummary
    handoff_health: HandoffHealthSummary
    review_queue_summary: ReviewQueueSummary
    schema_drift: SchemaDriftIndicators

    governance_flags: list[GovernanceFlag] = field(default_factory=list)
    governance_notes: list[str] = field(default_factory=list)
    overall_health_status: str = HEALTH_HEALTHY

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict of this report."""
        return asdict(self)

    def to_json_str(self, indent: int = 2) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_governance_report_id() -> str:
    """Generate a fresh UUID for a governance report."""
    return str(uuid.uuid4())


def make_flag_id() -> str:
    """Generate a fresh UUID for a governance flag."""
    return str(uuid.uuid4())


def make_governance_flag(
    category: str,
    severity: str,
    signal: str,
    detail: str,
) -> GovernanceFlag:
    """
    Construct a GovernanceFlag with a new flag_id.

    Args:
        category : One of ALL_FLAG_CATEGORIES.
        severity : One of ALL_SEVERITIES.
        signal   : Short machine-readable identifier for this signal type.
        detail   : Human-readable description of the specific condition.
    """
    if category not in ALL_FLAG_CATEGORIES:
        raise ValueError(
            f"Unknown flag category: {category!r}. "
            f"Valid categories: {ALL_FLAG_CATEGORIES}"
        )
    if severity not in ALL_SEVERITIES:
        raise ValueError(
            f"Unknown severity: {severity!r}. "
            f"Valid severities: {ALL_SEVERITIES}"
        )
    return GovernanceFlag(
        flag_id=make_flag_id(),
        category=category,
        severity=severity,
        signal=signal,
        detail=detail,
    )
