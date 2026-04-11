"""
run_evaluation.py — Full-pipeline evaluation orchestrator (Phase A-4).

Runs all four evaluation passes (Bronze, Silver, Gold, Traceability) against
local artifact directories, assembles an EvaluationReport, and writes both
a machine-readable JSON report and a human-readable text summary.

## What this script does

1. Runs Bronze evaluation against the Bronze artifact directory.
2. Runs Silver evaluation against the Silver artifact directory.
3. Runs Gold evaluation against the Gold artifact directory (null-confidence safe).
4. Runs cross-layer Traceability evaluation across all three layers.
5. Assembles an EvaluationReport from all results.
6. Writes report_<id>.json and report_<id>.txt to the output directory.
7. Optionally logs all scalar metrics to MLflow when --mlflow is set.

## Local execution (no Databricks required)

This script works entirely against local JSON artifact files — no Databricks
workspace, no credentials, and no live MLflow server are required for a
meaningful evaluation run. If --mlflow is set, MLflow must be installed.

## Usage

    python src/evaluation/run_evaluation.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --gold-dir output/gold

    # With MLflow logging (optional)
    python src/evaluation/run_evaluation.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --gold-dir output/gold \\
        --mlflow

    # Skip a layer (e.g. no Gold artifacts yet)
    python src/evaluation/run_evaluation.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --skip-gold --skip-traceability

Architecture context: ARCHITECTURE.md § Evaluation and Observability Layer
Evaluation plan: docs/evaluation-plan.md
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Add repo root to sys.path so src.* imports work from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Evaluation sub-modules (imported at module level after sys.path is set)
import eval_bronze as _eval_bronze_mod
import eval_silver as _eval_silver_mod
import eval_gold as _eval_gold_mod
import eval_traceability as _eval_traceability_mod

from report_models import EvaluationReport, LayerEvalResult, TraceabilityResult
from report_writer import write_report


# ---------------------------------------------------------------------------
# Optional MLflow support
# ---------------------------------------------------------------------------

try:
    import mlflow  # type: ignore

    _MLFLOW_AVAILABLE = True
except ImportError:
    mlflow = None  # type: ignore
    _MLFLOW_AVAILABLE = False

from mlflow_experiment_paths import end_to_end_experiment, SUFFIX_END_TO_END


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = "output/eval"
MLFLOW_EXPERIMENT_NAME = SUFFIX_END_TO_END


# ---------------------------------------------------------------------------
# Layer result builders
# ---------------------------------------------------------------------------

def _run_bronze(bronze_dir: str) -> Optional[LayerEvalResult]:
    """Run Bronze evaluation and return a LayerEvalResult."""
    bronze_path = Path(bronze_dir)
    try:
        paths = _eval_bronze_mod.collect_artifact_paths(bronze_path, None)
    except ValueError as exc:
        print(f"[run_evaluation] Bronze: {exc}", file=sys.stderr)
        return None

    records = _eval_bronze_mod.load_bronze_artifacts(paths)
    if not records:
        print("[run_evaluation] Bronze: no records loaded.", file=sys.stderr)
        return None

    metrics = _eval_bronze_mod.compute_metrics(records)
    warnings = _eval_bronze_mod.check_thresholds(metrics)
    flagged = metrics.get("flagged_records", [])

    return LayerEvalResult(
        layer="bronze",
        experiment=_eval_bronze_mod.MLFLOW_EXPERIMENT_NAME,
        eval_run_id=str(uuid.uuid4()),
        evaluated_at=metrics.get("evaluated_at", datetime.now(tz=timezone.utc).isoformat()),
        total_records=metrics.get("total_records", 0),
        metrics={k: v for k, v in metrics.items() if k != "flagged_records"},
        threshold_warnings=warnings,
        observations=[],
        flagged_record_count=len(flagged),
        flagged_records=flagged,
    )


def _run_silver(silver_dir: str) -> Optional[LayerEvalResult]:
    """Run Silver evaluation and return a LayerEvalResult."""
    silver_path = Path(silver_dir)
    try:
        paths = _eval_silver_mod.collect_artifact_paths(silver_path, None)
    except ValueError as exc:
        print(f"[run_evaluation] Silver: {exc}", file=sys.stderr)
        return None

    records = _eval_silver_mod.load_silver_artifacts(paths)
    if not records:
        print("[run_evaluation] Silver: no records loaded.", file=sys.stderr)
        return None

    metrics = _eval_silver_mod.compute_metrics(records)
    warnings = _eval_silver_mod.check_thresholds(metrics)
    flagged = metrics.get("flagged_records", [])

    return LayerEvalResult(
        layer="silver",
        experiment=_eval_silver_mod.MLFLOW_EXPERIMENT_NAME,
        eval_run_id=str(uuid.uuid4()),
        evaluated_at=metrics.get("evaluated_at", datetime.now(tz=timezone.utc).isoformat()),
        total_records=metrics.get("total_records", 0),
        metrics={k: v for k, v in metrics.items() if k != "flagged_records"},
        threshold_warnings=warnings,
        observations=[],
        flagged_record_count=len(flagged),
        flagged_records=flagged,
    )


def _run_gold(gold_dir: str) -> Optional[LayerEvalResult]:
    """Run Gold evaluation and return a LayerEvalResult (null-confidence safe)."""
    gold_path = Path(gold_dir)
    try:
        paths = _eval_gold_mod.collect_artifact_paths(gold_path, None)
    except ValueError as exc:
        print(f"[run_evaluation] Gold: {exc}", file=sys.stderr)
        return None

    records = _eval_gold_mod.load_gold_artifacts(paths)
    if not records:
        print("[run_evaluation] Gold: no records loaded.", file=sys.stderr)
        return None

    metrics = _eval_gold_mod.compute_metrics(records)
    warnings = _eval_gold_mod.check_thresholds(metrics)
    flagged = metrics.get("flagged_records", [])
    observations = metrics.get("observations", [])

    return LayerEvalResult(
        layer="gold",
        experiment=_eval_gold_mod.MLFLOW_EXPERIMENT_NAME,
        eval_run_id=str(uuid.uuid4()),
        evaluated_at=metrics.get("evaluated_at", datetime.now(tz=timezone.utc).isoformat()),
        total_records=metrics.get("total_records", 0),
        metrics={k: v for k, v in metrics.items() if k not in ("flagged_records", "observations")},
        threshold_warnings=warnings,
        observations=observations,
        flagged_record_count=len(flagged),
        flagged_records=flagged,
    )


def _run_traceability(
    bronze_dir: str,
    silver_dir: str,
    gold_dir: str,
) -> Optional[TraceabilityResult]:
    """Run traceability evaluation and return a TraceabilityResult."""
    try:
        bronze_records = _eval_traceability_mod.load_artifacts(Path(bronze_dir))
        silver_records = _eval_traceability_mod.load_artifacts(Path(silver_dir))
        gold_records = _eval_traceability_mod.load_artifacts(Path(gold_dir))
    except ValueError as exc:
        print(f"[run_evaluation] Traceability: {exc}", file=sys.stderr)
        return None

    metrics = _eval_traceability_mod.compute_metrics(
        bronze_records, silver_records, gold_records
    )
    warnings = _eval_traceability_mod.check_thresholds(metrics)

    placeholder_note = ""
    if metrics.get("placeholder_run_id_count", 0) > 0:
        placeholder_note = (
            "Records with placeholder pipeline_run_id values originate from the "
            "A-3B bootstrap SQL path, not from a tracked MLflow pipeline run. "
            "Full document_id-based lineage remains intact."
        )

    return TraceabilityResult(
        experiment=_eval_traceability_mod.MLFLOW_EXPERIMENT_NAME,
        eval_run_id=str(uuid.uuid4()),
        evaluated_at=metrics.get("evaluated_at", datetime.now(tz=timezone.utc).isoformat()),
        total_bronze=metrics.get("total_bronze", 0),
        total_silver=metrics.get("total_silver", 0),
        total_gold=metrics.get("total_gold", 0),
        gold_to_silver_link_rate=metrics.get("gold_to_silver_link_rate"),
        silver_to_bronze_link_rate=metrics.get("silver_to_bronze_link_rate"),
        pipeline_run_id_coverage=metrics.get("pipeline_run_id_coverage", 0.0),
        schema_version_coverage=metrics.get("schema_version_coverage", 0.0),
        orphaned_silver_count=metrics.get("orphaned_silver_count", 0),
        orphaned_gold_count=metrics.get("orphaned_gold_count", 0),
        placeholder_run_id_count=metrics.get("placeholder_run_id_count", 0),
        placeholder_run_id_note=placeholder_note,
        threshold_warnings=warnings,
        observations=metrics.get("observations", []),
        orphaned_silver_records=[
            r for r in metrics.get("flagged_records", []) if r.get("layer") == "silver"
        ],
        orphaned_gold_records=[
            r for r in metrics.get("flagged_records", []) if r.get("layer") == "gold"
        ],
    )


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def _is_bootstrap_path_detected(
    gold: Optional[LayerEvalResult],
    traceability: Optional[TraceabilityResult],
) -> bool:
    """Return True if any indicator of the A-3B bootstrap SQL path is visible."""
    if gold and gold.metrics.get("confidence_null_rate", 0.0) > 0.0:
        return True
    if traceability and traceability.placeholder_run_id_count > 0:
        return True
    return False


def assemble_report(
    bronze: Optional[LayerEvalResult],
    silver: Optional[LayerEvalResult],
    gold: Optional[LayerEvalResult],
    traceability: Optional[TraceabilityResult],
    pipeline_run_id_filter: Optional[str] = None,
) -> EvaluationReport:
    """Assemble an EvaluationReport from individual layer results."""
    all_warnings: list[str] = []
    all_observations: list[str] = []
    review_queue_size = 0

    for result in (bronze, silver, gold):
        if result:
            all_warnings.extend(result.threshold_warnings)
            all_observations.extend(result.observations)
            review_queue_size += result.flagged_record_count

    if traceability:
        all_warnings.extend(traceability.threshold_warnings)
        all_observations.extend(traceability.observations)

    bootstrap_detected = _is_bootstrap_path_detected(gold, traceability)

    return EvaluationReport(
        report_id=str(uuid.uuid4()),
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        pipeline_run_id_filter=pipeline_run_id_filter,
        bronze=bronze,
        silver=silver,
        gold=gold,
        traceability=traceability,
        all_warnings=all_warnings,
        all_observations=all_observations,
        review_queue_size=review_queue_size,
        bootstrap_path_detected=bootstrap_detected,
    )


# ---------------------------------------------------------------------------
# Optional MLflow logging
# ---------------------------------------------------------------------------

def log_report_to_mlflow(report: EvaluationReport, json_path: Path) -> None:
    """Log cross-layer summary metrics from the assembled report to MLflow."""
    if not _MLFLOW_AVAILABLE:
        print(
            "[run_evaluation] MLflow is not installed. Skipping MLflow logging.",
            file=sys.stderr,
        )
        return

    experiment_name = end_to_end_experiment()
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name="end_to_end_evaluation"):
        mlflow.log_param("report_id", report.report_id)
        mlflow.log_param("pipeline_run_id_filter", report.pipeline_run_id_filter or "all")
        mlflow.log_param("bootstrap_path_detected", str(report.bootstrap_path_detected))
        mlflow.log_param("review_queue_size", report.review_queue_size)
        mlflow.log_param("total_warnings", len(report.all_warnings))

        for layer_result in (report.bronze, report.silver, report.gold):
            if not layer_result:
                continue
            prefix = layer_result.layer
            for key, val in layer_result.metrics.items():
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    try:
                        mlflow.log_metric(f"{prefix}_{key}", float(val))
                    except Exception:  # noqa: BLE001
                        pass

        if report.traceability:
            t = report.traceability
            for key, val in [
                ("gold_to_silver_link_rate", t.gold_to_silver_link_rate),
                ("silver_to_bronze_link_rate", t.silver_to_bronze_link_rate),
                ("pipeline_run_id_coverage", t.pipeline_run_id_coverage),
                ("schema_version_coverage", t.schema_version_coverage),
            ]:
                if val is not None:
                    mlflow.log_metric(f"traceability_{key}", float(val))

        mlflow.log_artifact(str(json_path))
        print(f"[run_evaluation] End-to-end report logged to MLflow: {experiment_name}")


# ---------------------------------------------------------------------------
# Main orchestration function
# ---------------------------------------------------------------------------

def run_full_evaluation(
    bronze_dir: str,
    silver_dir: str,
    gold_dir: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    log_mlflow: bool = False,
    skip_bronze: bool = False,
    skip_silver: bool = False,
    skip_gold: bool = False,
    skip_traceability: bool = False,
    pipeline_run_id_filter: Optional[str] = None,
) -> EvaluationReport:
    """
    Run all evaluation passes and assemble a full EvaluationReport.

    Returns the assembled EvaluationReport.
    """
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    print("[run_evaluation] Starting full pipeline evaluation...")

    bronze_result: Optional[LayerEvalResult] = None
    silver_result: Optional[LayerEvalResult] = None
    gold_result: Optional[LayerEvalResult] = None
    trace_result: Optional[TraceabilityResult] = None

    if not skip_bronze:
        print("\n[run_evaluation] --- Bronze ---")
        bronze_result = _run_bronze(bronze_dir)

    if not skip_silver:
        print("\n[run_evaluation] --- Silver ---")
        silver_result = _run_silver(silver_dir)

    if not skip_gold:
        print("\n[run_evaluation] --- Gold ---")
        gold_result = _run_gold(gold_dir)

    if not skip_traceability:
        print("\n[run_evaluation] --- Traceability ---")
        trace_result = _run_traceability(bronze_dir, silver_dir, gold_dir)

    print("\n[run_evaluation] --- Assembling report ---")
    report = assemble_report(
        bronze=bronze_result,
        silver=silver_result,
        gold=gold_result,
        traceability=trace_result,
        pipeline_run_id_filter=pipeline_run_id_filter,
    )

    json_path, text_path = write_report(report, output_dir_path)
    print(f"[run_evaluation] JSON report  → {json_path}")
    print(f"[run_evaluation] Text summary → {text_path}")

    if report.bootstrap_path_detected:
        print(
            "\n[run_evaluation] NOTE: Bootstrap path detected. One or more records "
            "originate from the A-3B Databricks bootstrap SQL execution "
            "(null classification_confidence or placeholder pipeline_run_id). "
            "See report observations for details."
        )

    if report.all_warnings:
        print(f"\n[run_evaluation] {len(report.all_warnings)} threshold warning(s) — review report.")
    else:
        print("\n[run_evaluation] All thresholds met.")

    if log_mlflow:
        log_report_to_mlflow(report, json_path)

    return report


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_evaluation",
        description=(
            "Run the full A-4 evaluation pipeline across Bronze, Silver, Gold, and "
            "Traceability layers. Writes a JSON report and text summary."
        ),
    )
    p.add_argument("--bronze-dir", default="output/bronze", metavar="DIR")
    p.add_argument("--silver-dir", default="output/silver", metavar="DIR")
    p.add_argument("--gold-dir", default="output/gold", metavar="DIR")
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory for evaluation reports. Default: {DEFAULT_OUTPUT_DIR}",
    )
    p.add_argument(
        "--mlflow",
        action="store_true",
        default=False,
        help="Log end-to-end metrics to MLflow.",
    )
    p.add_argument("--skip-bronze", action="store_true", default=False)
    p.add_argument("--skip-silver", action="store_true", default=False)
    p.add_argument("--skip-gold", action="store_true", default=False)
    p.add_argument("--skip-traceability", action="store_true", default=False)
    p.add_argument(
        "--pipeline-run-id",
        default=None,
        metavar="RUN_ID",
        help="Optional: label the evaluation as targeting a specific pipeline_run_id.",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_full_evaluation(
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        output_dir=args.output_dir,
        log_mlflow=args.mlflow,
        skip_bronze=args.skip_bronze,
        skip_silver=args.skip_silver,
        skip_gold=args.skip_gold,
        skip_traceability=args.skip_traceability,
        pipeline_run_id_filter=args.pipeline_run_id,
    )


if __name__ == "__main__":
    main()
