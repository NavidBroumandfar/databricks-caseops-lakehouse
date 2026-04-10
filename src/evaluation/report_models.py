"""
report_models.py — Structured data models for A-4 evaluation reports.

Defines the schema for evaluation summaries produced by the A-4 evaluation layer.
Uses plain dataclasses (no external dependencies) so the models are usable locally
without pydantic or any other library.

All models are JSON-serializable via dataclasses.asdict().

Design:
  - EvaluationReport is the top-level report written by run_evaluation.py.
  - Each layer (Bronze, Silver, Gold) produces a LayerEvalResult.
  - Traceability produces a TraceabilityResult.
  - Observations capture notable non-error conditions (e.g. bootstrap path, null confidence).
  - Warnings capture threshold violations.

These models are intentionally simple — they are not a framework.

Architecture context: ARCHITECTURE.md § Evaluation and Observability Layer
Evaluation plan: docs/evaluation-plan.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class LayerEvalResult:
    """
    Evaluation result for a single pipeline layer (Bronze, Silver, or Gold).

    Fields:
        layer           : One of 'bronze', 'silver', 'gold'.
        experiment      : MLflow experiment path for this layer.
        eval_run_id     : UUID of this evaluation run (not an MLflow run ID).
        evaluated_at    : ISO-8601 UTC timestamp.
        total_records   : Number of records evaluated.
        metrics         : Dict of computed scalar metrics for this layer.
        threshold_warnings: Threshold violations as human-readable strings.
        observations    : Notable non-error conditions (e.g. null confidence rate).
        flagged_record_count: Number of records requiring human review.
        flagged_records : Summaries of flagged records.
    """

    layer: str
    experiment: str
    eval_run_id: str
    evaluated_at: str
    total_records: int
    metrics: dict[str, Any]
    threshold_warnings: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    flagged_record_count: int = 0
    flagged_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TraceabilityResult:
    """
    Cross-layer traceability evaluation result.

    Covers:
      - Gold → Silver link completeness
      - Silver → Bronze link completeness
      - pipeline_run_id coverage
      - schema_version coverage
      - Orphaned record detection
      - Placeholder run ID detection (bootstrap SQL path)
    """

    experiment: str
    eval_run_id: str
    evaluated_at: str
    total_bronze: int
    total_silver: int
    total_gold: int

    # Link rates (target: 1.0)
    gold_to_silver_link_rate: Optional[float]
    silver_to_bronze_link_rate: Optional[float]
    pipeline_run_id_coverage: float
    schema_version_coverage: float

    # Orphan counts (target: 0)
    orphaned_silver_count: int
    orphaned_gold_count: int

    # Placeholder run IDs (bootstrap SQL path; not real MLflow run IDs)
    placeholder_run_id_count: int
    placeholder_run_id_note: str

    threshold_warnings: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    orphaned_silver_records: list[dict[str, Any]] = field(default_factory=list)
    orphaned_gold_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class EvaluationReport:
    """
    Top-level evaluation report assembled by run_evaluation.py.

    Contains results for all evaluated layers plus a cross-layer traceability
    result and a summary of all threshold warnings and observations across
    the entire pipeline run.

    This is the single artifact written at the end of a full evaluation pass.
    It is designed to be machine-readable (JSON) and human-auditable.

    Fields:
        report_id       : UUID for this report.
        generated_at    : ISO-8601 UTC timestamp.
        pipeline_run_id_filter : The pipeline_run_id being evaluated, if filtered.
                                 None means all available artifacts were evaluated.
        bronze          : Bronze layer evaluation result (or None if not run).
        silver          : Silver layer evaluation result (or None if not run).
        gold            : Gold layer evaluation result (or None if not run).
        traceability    : Cross-layer traceability result (or None if not run).
        all_warnings    : Deduplicated list of all threshold warnings across layers.
        all_observations: Deduplicated list of all observations across layers.
        review_queue_size: Total number of flagged records across all layers.
        bootstrap_path_detected: True if any record carries a placeholder run ID,
                                 indicating bootstrap SQL origin (not MLflow pipeline).
    """

    report_id: str
    generated_at: str
    pipeline_run_id_filter: Optional[str]

    bronze: Optional[LayerEvalResult] = None
    silver: Optional[LayerEvalResult] = None
    gold: Optional[LayerEvalResult] = None
    traceability: Optional[TraceabilityResult] = None

    all_warnings: list[str] = field(default_factory=list)
    all_observations: list[str] = field(default_factory=list)
    review_queue_size: int = 0
    bootstrap_path_detected: bool = False
