"""
eval_bronze.py — Bronze parse quality evaluation (Phase A-1)

Reads one or more Bronze JSON artifacts produced by ingest_bronze.py,
computes parse quality metrics, prints a readable summary, and writes a
JSON evaluation artifact.

This is an independent evaluation script — it is not run inline during
pipeline execution. This separation keeps the pipeline logic clean and
allows re-evaluation against any Bronze snapshot without re-running ingest.

Evaluation metrics align with docs/evaluation-plan.md § 1. Parse Quality (Bronze).
MLflow logging hooks are included but optional; they are no-ops when MLflow
is not installed or not configured.

Usage:
    # Evaluate all Bronze artifacts in a directory
    python src/evaluation/eval_bronze.py --input-dir output/bronze

    # Evaluate a specific artifact file
    python src/evaluation/eval_bronze.py --input output/bronze/<record>.json

    # Write evaluation output to a specific directory
    python src/evaluation/eval_bronze.py --input-dir output/bronze --output-dir output/eval

    # Log metrics to MLflow (optional)
    python src/evaluation/eval_bronze.py --input-dir output/bronze --mlflow

Authoritative evaluation plan: docs/evaluation-plan.md § Parse Quality (Bronze)
Architecture context: ARCHITECTURE.md § Evaluation and Observability Layer
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Optional MLflow support
# ---------------------------------------------------------------------------

try:
    import mlflow  # type: ignore

    _MLFLOW_AVAILABLE = True
except ImportError:
    mlflow = None  # type: ignore
    _MLFLOW_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = "output/eval"
MLFLOW_EXPERIMENT_NAME = "caseops/bronze/parse_quality"

# Thresholds from evaluation-plan.md
TARGET_PARSE_SUCCESS_RATE = 0.95
TARGET_PARSE_FAILURE_RATE = 0.05
TARGET_MEDIAN_CHAR_COUNT = 500
TARGET_P10_CHAR_COUNT = 100


# ---------------------------------------------------------------------------
# Artifact loading
# ---------------------------------------------------------------------------

def load_bronze_artifacts(paths: list[Path]) -> list[dict]:
    """Load and return a list of Bronze record dicts from JSON files."""
    records = []
    for p in paths:
        try:
            records.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as exc:  # noqa: BLE001
            print(f"[eval_bronze] Warning: could not load {p}: {exc}", file=sys.stderr)
    return records


def collect_artifact_paths(input_dir: Optional[Path], input_file: Optional[Path]) -> list[Path]:
    """Resolve the list of Bronze artifact paths to evaluate."""
    if input_file is not None:
        if not input_file.exists():
            raise ValueError(f"Input file does not exist: {input_file}")
        return [input_file]

    if input_dir is not None:
        if not input_dir.is_dir():
            raise ValueError(f"Input directory does not exist: {input_dir}")
        paths = sorted(input_dir.glob("*.json"))
        if not paths:
            raise ValueError(f"No JSON artifacts found in: {input_dir}")
        return paths

    raise ValueError("Provide --input or --input-dir.")


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_metrics(records: list[dict]) -> dict:
    """
    Compute Bronze parse quality metrics over a list of Bronze record dicts.

    Returns a metrics dict with:
        - total_records
        - parse_success_rate
        - parse_partial_rate
        - parse_failure_rate
        - median_char_count
        - p10_char_count
        - zero_char_count_rate
        - flagged_records (list of record summaries that warrant human review)
    """
    total = len(records)
    if total == 0:
        return {"error": "No records to evaluate."}

    success_count = sum(1 for r in records if r.get("parse_status") == "success")
    partial_count = sum(1 for r in records if r.get("parse_status") == "partial")
    failed_count = sum(1 for r in records if r.get("parse_status") == "failed")

    # char_count for success/partial records only
    char_counts = [
        r["char_count"]
        for r in records
        if r.get("parse_status") in ("success", "partial")
        and r.get("char_count") is not None
    ]

    zero_char_success = [
        r for r in records
        if r.get("parse_status") == "success" and r.get("char_count", -1) == 0
    ]

    median_char = statistics.median(char_counts) if char_counts else None
    p10_char = _percentile(char_counts, 10) if char_counts else None

    flagged = _identify_flagged_records(records)

    return {
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_records": total,
        "parse_success_rate": round(success_count / total, 4),
        "parse_partial_rate": round(partial_count / total, 4),
        "parse_failure_rate": round(failed_count / total, 4),
        "median_char_count": median_char,
        "p10_char_count": p10_char,
        "zero_char_count_rate": round(len(zero_char_success) / total, 4),
        "flagged_record_count": len(flagged),
        "flagged_records": flagged,
    }


def _percentile(values: list[float], pct: int) -> float:
    """Return the Nth percentile of a sorted list (nearest-rank method)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = max(0, int(len(sorted_vals) * pct / 100) - 1)
    return sorted_vals[idx]


def _identify_flagged_records(records: list[dict]) -> list[dict]:
    """
    Return summary dicts for records that warrant human review.

    Flags from evaluation-plan.md § Human Review Triggers (Bronze):
        - parse_status = 'failed'
        - char_count < 100 and parse_status = 'success'
        - char_count = 0 (despite success/partial status)
    """
    flagged = []
    for r in records:
        reasons = []
        status = r.get("parse_status")
        char_count = r.get("char_count")

        if status == "failed":
            reasons.append("parse_status=failed")
        if status == "success" and char_count is not None and char_count < 100:
            reasons.append(f"suspiciously_short_parse (char_count={char_count})")
        if status in ("success", "partial") and char_count == 0:
            reasons.append("zero_char_count_despite_non_failed_status")

        if reasons:
            flagged.append({
                "bronze_record_id": r.get("bronze_record_id"),
                "document_id": r.get("document_id"),
                "parse_status": status,
                "char_count": char_count,
                "parse_failure_reason": r.get("parse_failure_reason"),
                "review_reasons": reasons,
            })
    return flagged


# ---------------------------------------------------------------------------
# Threshold checks
# ---------------------------------------------------------------------------

def check_thresholds(metrics: dict) -> list[str]:
    """
    Compare computed metrics against targets from evaluation-plan.md.
    Return a list of human-readable warning strings for any violations.
    """
    warnings = []
    psr = metrics.get("parse_success_rate")
    pfr = metrics.get("parse_failure_rate")
    median = metrics.get("median_char_count")
    p10 = metrics.get("p10_char_count")
    zcr = metrics.get("zero_char_count_rate")

    if psr is not None and psr < TARGET_PARSE_SUCCESS_RATE:
        warnings.append(
            f"parse_success_rate {psr:.2%} is below target {TARGET_PARSE_SUCCESS_RATE:.2%}"
        )
    if pfr is not None and pfr > TARGET_PARSE_FAILURE_RATE:
        warnings.append(
            f"parse_failure_rate {pfr:.2%} exceeds target {TARGET_PARSE_FAILURE_RATE:.2%} — "
            "review batch for format issues or path errors"
        )
    if median is not None and median < TARGET_MEDIAN_CHAR_COUNT:
        warnings.append(
            f"median_char_count {median} is below target {TARGET_MEDIAN_CHAR_COUNT} — "
            "possible PDF parsing issue (scanned or image-only documents)"
        )
    if p10 is not None and p10 < TARGET_P10_CHAR_COUNT:
        warnings.append(
            f"p10_char_count {p10} is below target {TARGET_P10_CHAR_COUNT} — "
            "near-empty parses detected at the 10th percentile"
        )
    if zcr is not None and zcr > 0.0:
        warnings.append(
            f"zero_char_count_rate is {zcr:.2%} — investigate ai_parse_document call immediately"
        )
    return warnings


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------

def print_evaluation_summary(metrics: dict, warnings: list[str]) -> None:
    """Print a readable evaluation summary to stdout."""
    sep = "-" * 60
    print(sep)
    print("  Bronze Parse Quality — Evaluation Summary")
    print(sep)
    print(f"  Evaluated at     : {metrics.get('evaluated_at', 'N/A')}")
    print(f"  Total records    : {metrics.get('total_records', 0)}")
    print()
    print(f"  Parse success    : {metrics.get('parse_success_rate', 0):.2%}")
    print(f"  Parse partial    : {metrics.get('parse_partial_rate', 0):.2%}")
    print(f"  Parse failure    : {metrics.get('parse_failure_rate', 0):.2%}")
    print()
    print(f"  Median char count: {metrics.get('median_char_count', 'N/A')}")
    print(f"  P10 char count   : {metrics.get('p10_char_count', 'N/A')}")
    print(f"  Zero-char rate   : {metrics.get('zero_char_count_rate', 0):.2%}")
    print()
    print(f"  Flagged records  : {metrics.get('flagged_record_count', 0)}")
    print(sep)

    if warnings:
        print("  THRESHOLD WARNINGS:")
        for w in warnings:
            print(f"    ⚠  {w}")
        print(sep)
    else:
        print("  All thresholds met.")
        print(sep)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------

def write_eval_artifact(metrics: dict, warnings: list[str], output_dir: Path) -> Path:
    """Write the evaluation summary as a JSON artifact."""
    output_dir.mkdir(parents=True, exist_ok=True)
    eval_run_id = str(uuid.uuid4())
    artifact = {
        "eval_run_id": eval_run_id,
        "experiment": MLFLOW_EXPERIMENT_NAME,
        "metrics": metrics,
        "threshold_warnings": warnings,
    }
    path = output_dir / f"eval_bronze_{eval_run_id}.json"
    path.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Optional MLflow logging
# ---------------------------------------------------------------------------

def log_to_mlflow(metrics: dict, warnings: list[str], flagged_path: Optional[Path]) -> None:
    """
    Log metrics and artifacts to an MLflow run.
    Only called when --mlflow flag is set and MLflow is installed.
    """
    if not _MLFLOW_AVAILABLE:
        print(
            "[eval_bronze] MLflow is not installed. Skipping MLflow logging.",
            file=sys.stderr,
        )
        return

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    with mlflow.start_run(run_name="bronze_parse_quality"):
        # Log scalar metrics
        scalar_keys = [
            "parse_success_rate",
            "parse_partial_rate",
            "parse_failure_rate",
            "median_char_count",
            "p10_char_count",
            "zero_char_count_rate",
        ]
        for key in scalar_keys:
            val = metrics.get(key)
            if val is not None:
                mlflow.log_metric(key, float(val))

        mlflow.log_param("total_records", metrics.get("total_records", 0))
        mlflow.log_param("flagged_record_count", metrics.get("flagged_record_count", 0))
        mlflow.log_param("threshold_warnings_count", len(warnings))

        # Log flagged records as artifact if there are any
        if flagged_path and flagged_path.exists():
            mlflow.log_artifact(str(flagged_path))

        print(f"[eval_bronze] Metrics logged to MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")


# ---------------------------------------------------------------------------
# Top-level evaluation function
# ---------------------------------------------------------------------------

def run_eval_bronze(
    input_dir: Optional[str] = None,
    input_file: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    log_mlflow: bool = False,
) -> dict:
    """
    Run Bronze parse quality evaluation.

    Returns the metrics dict.
    """
    input_dir_path = Path(input_dir) if input_dir else None
    input_file_path = Path(input_file) if input_file else None
    output_dir_path = Path(output_dir)

    artifact_paths = collect_artifact_paths(input_dir_path, input_file_path)
    print(f"[eval_bronze] Evaluating {len(artifact_paths)} Bronze artifact(s)...")

    records = load_bronze_artifacts(artifact_paths)
    if not records:
        raise ValueError("No valid Bronze records could be loaded.")

    metrics = compute_metrics(records)
    warnings = check_thresholds(metrics)

    print_evaluation_summary(metrics, warnings)

    # Write evaluation artifact
    eval_artifact_path = write_eval_artifact(metrics, warnings, output_dir_path)
    print(f"[eval_bronze] Evaluation artifact written → {eval_artifact_path}")

    # Write flagged records as a separate artifact (mirrors evaluation-plan.md)
    flagged_path = None
    if metrics.get("flagged_records"):
        flagged_path = output_dir_path / "flagged_records.json"
        flagged_path.write_text(
            json.dumps(metrics["flagged_records"], indent=2, default=str), encoding="utf-8"
        )
        print(f"[eval_bronze] Flagged records written → {flagged_path}")

    if log_mlflow:
        log_to_mlflow(metrics, warnings, flagged_path)

    return metrics


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="eval_bronze",
        description=(
            "Evaluate Bronze JSON artifacts produced by ingest_bronze.py. "
            "Computes parse quality metrics and writes a JSON evaluation summary."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing Bronze JSON artifacts to evaluate.",
    )
    group.add_argument(
        "--input",
        metavar="FILE",
        help="Single Bronze JSON artifact file to evaluate.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory to write the evaluation JSON artifact. Default: {DEFAULT_OUTPUT_DIR}",
    )
    p.add_argument(
        "--mlflow",
        action="store_true",
        default=False,
        help="Log metrics to MLflow (requires mlflow to be installed and configured).",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_eval_bronze(
        input_dir=args.input_dir,
        input_file=args.input,
        output_dir=args.output_dir,
        log_mlflow=args.mlflow,
    )


if __name__ == "__main__":
    main()
