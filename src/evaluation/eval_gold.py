"""
eval_gold.py — Gold classification quality evaluation (Phase A-3)

Reads one or more Gold JSON artifacts produced by classify_gold.py,
computes classification quality metrics, prints a readable summary, and writes
a JSON evaluation artifact.

This is an independent evaluation script — it is not run inline during
pipeline execution. This separation keeps pipeline logic clean and allows
re-evaluation against any Gold snapshot without re-running classification.

Evaluation metrics align with docs/evaluation-plan.md § 3. Classification Quality (Gold).
MLflow logging hooks are included but optional; they are no-ops when MLflow
is not installed or not configured.

Usage:
    # Evaluate all Gold artifacts in a directory
    python src/evaluation/eval_gold.py --input-dir output/gold

    # Evaluate a single Gold artifact
    python src/evaluation/eval_gold.py --input output/gold/<record>.json

    # Write evaluation output to a specific directory
    python src/evaluation/eval_gold.py --input-dir output/gold --output-dir output/eval

    # Log metrics to MLflow (optional)
    python src/evaluation/eval_gold.py --input-dir output/gold --mlflow

Authoritative evaluation plan: docs/evaluation-plan.md § Classification Quality (Gold)
Architecture context: ARCHITECTURE.md § Evaluation and Observability Layer
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


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
MLFLOW_EXPERIMENT_NAME = "caseops/gold/classification_quality"

# Thresholds from docs/evaluation-plan.md § 3. Classification Quality (Gold)
TARGET_CLASSIFICATION_SUCCESS_RATE = 0.85
TARGET_UNKNOWN_LABEL_RATE = 0.15
TARGET_QUARANTINE_RATE = 0.15
TARGET_EXPORT_READY_RATE = 0.70
TARGET_MEAN_CONFIDENCE = 0.75
TARGET_LOW_CONFIDENCE_RATE = 0.20

LOW_CONFIDENCE_THRESHOLD = 0.70
VERY_LOW_CONFIDENCE_THRESHOLD = 0.50

QUARANTINE_LABEL = "quarantine"
UNKNOWN_LABEL = "unknown"


# ---------------------------------------------------------------------------
# Artifact loading
# ---------------------------------------------------------------------------


def load_gold_artifacts(paths: list[Path]) -> list[dict]:
    """Load and return a list of Gold record dicts from JSON files."""
    records = []
    for p in paths:
        try:
            records.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as exc:  # noqa: BLE001
            print(f"[eval_gold] Warning: could not load {p}: {exc}", file=sys.stderr)
    return records


def collect_artifact_paths(input_dir: Optional[Path], input_file: Optional[Path]) -> list[Path]:
    """Resolve the list of Gold artifact paths to evaluate."""
    if input_file is not None:
        if not input_file.exists():
            raise ValueError(f"Input file does not exist: {input_file}")
        return [input_file]
    if input_dir is not None:
        if not input_dir.is_dir():
            raise ValueError(f"Input directory does not exist: {input_dir}")
        # Gold artifacts are directly in the output_dir (not in exports subdirs)
        paths = sorted(p for p in input_dir.glob("*.json"))
        if not paths:
            raise ValueError(f"No JSON artifacts found in: {input_dir}")
        return paths
    raise ValueError("Provide --input or --input-dir.")


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------


def compute_metrics(records: list[dict]) -> dict:
    """
    Compute Gold classification quality metrics over a list of Gold record dicts.

    Metrics align with docs/evaluation-plan.md § 3. Classification Quality (Gold):
        - classification_success_rate
        - unknown_label_rate
        - quarantine_rate
        - export_ready_rate
        - mean_classification_confidence
        - low_confidence_rate
        - label_distribution
        - flagged_records
    """
    total = len(records)
    if total == 0:
        return {"error": "No records to evaluate."}

    # Label counts
    unknown_count = sum(
        1 for r in records if r.get("document_type_label") == UNKNOWN_LABEL
    )
    classified_count = total - unknown_count

    quarantine_count = sum(
        1 for r in records if r.get("routing_label") == QUARANTINE_LABEL
    )
    export_ready_count = sum(1 for r in records if r.get("export_ready") is True)

    # Confidence values
    confidence_values = [
        r["classification_confidence"]
        for r in records
        if r.get("classification_confidence") is not None
    ]
    mean_confidence = (
        round(statistics.mean(confidence_values), 4) if confidence_values else None
    )
    low_confidence_count = sum(
        1 for v in confidence_values if v < LOW_CONFIDENCE_THRESHOLD
    )

    # Label distribution
    label_dist = _compute_label_distribution(records)

    # Flagged records
    flagged = _identify_flagged_records(records)

    return {
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_records": total,
        "classification_success_rate": round(classified_count / total, 4),
        "unknown_label_rate": round(unknown_count / total, 4),
        "quarantine_rate": round(quarantine_count / total, 4),
        "export_ready_rate": round(export_ready_count / total, 4),
        "mean_classification_confidence": mean_confidence,
        "low_confidence_rate": round(low_confidence_count / total, 4) if total else None,
        "label_distribution": label_dist,
        "flagged_record_count": len(flagged),
        "flagged_records": flagged,
    }


def _compute_label_distribution(records: list[dict]) -> list[dict]:
    """
    Count occurrences of each document_type_label.

    Returns a list of {label, count, rate} dicts ordered by count descending.
    """
    total = len(records)
    counter = Counter(r.get("document_type_label", "unknown") for r in records)
    return [
        {
            "label": label,
            "count": count,
            "rate": round(count / total, 4),
        }
        for label, count in counter.most_common()
    ]


def _identify_flagged_records(records: list[dict]) -> list[dict]:
    """
    Return summary dicts for records that warrant human review.

    Flags from docs/evaluation-plan.md § Human Review Readiness:
        - document_type_label == 'unknown'
        - classification_confidence < 0.50
        - export_ready == False
    """
    flagged = []
    for r in records:
        reasons = []
        label = r.get("document_type_label")
        confidence = r.get("classification_confidence")
        export_ready = r.get("export_ready")

        if label == UNKNOWN_LABEL:
            reasons.append("document_type_label=unknown")
        if confidence is not None and confidence < VERY_LOW_CONFIDENCE_THRESHOLD:
            reasons.append(f"very_low_confidence (classification_confidence={confidence:.2f})")
        if export_ready is False:
            reasons.append("export_ready=false")

        if reasons:
            flagged.append({
                "gold_record_id": r.get("gold_record_id"),
                "document_id": r.get("document_id"),
                "extraction_id": r.get("extraction_id"),
                "document_type_label": label,
                "routing_label": r.get("routing_label"),
                "classification_confidence": confidence,
                "export_ready": export_ready,
                "review_reasons": reasons,
            })
    return flagged


# ---------------------------------------------------------------------------
# Threshold checks
# ---------------------------------------------------------------------------


def check_thresholds(metrics: dict) -> list[str]:
    """
    Compare computed metrics against targets from docs/evaluation-plan.md.
    Return a list of human-readable warning strings for any violations.
    """
    warnings = []

    csr = metrics.get("classification_success_rate")
    ulr = metrics.get("unknown_label_rate")
    qr = metrics.get("quarantine_rate")
    err = metrics.get("export_ready_rate")
    mc = metrics.get("mean_classification_confidence")
    lcr = metrics.get("low_confidence_rate")

    if csr is not None and csr < TARGET_CLASSIFICATION_SUCCESS_RATE:
        warnings.append(
            f"classification_success_rate {csr:.2%} is below target "
            f"{TARGET_CLASSIFICATION_SUCCESS_RATE:.2%} — "
            "taxonomy may need expansion or classifier logic may need review"
        )
    if ulr is not None and ulr > TARGET_UNKNOWN_LABEL_RATE:
        warnings.append(
            f"unknown_label_rate {ulr:.2%} exceeds target {TARGET_UNKNOWN_LABEL_RATE:.2%} — "
            "review document class hints or classifier signal thresholds"
        )
    if qr is not None and qr > TARGET_QUARANTINE_RATE:
        warnings.append(
            f"quarantine_rate {qr:.2%} exceeds target {TARGET_QUARANTINE_RATE:.2%} — "
            "systematic quality issue upstream (extraction or classification)"
        )
    if err is not None and err < TARGET_EXPORT_READY_RATE:
        warnings.append(
            f"export_ready_rate {err:.2%} is below target {TARGET_EXPORT_READY_RATE:.2%} — "
            "review Silver coverage or classification confidence thresholds"
        )
    if mc is not None and mc < TARGET_MEAN_CONFIDENCE:
        warnings.append(
            f"mean_classification_confidence {mc:.2%} is below target "
            f"{TARGET_MEAN_CONFIDENCE:.2%} — "
            "classifier is uncertain; review rule logic or label taxonomy"
        )
    if lcr is not None and lcr > TARGET_LOW_CONFIDENCE_RATE:
        warnings.append(
            f"low_confidence_rate {lcr:.2%} exceeds target {TARGET_LOW_CONFIDENCE_RATE:.2%}"
        )
    return warnings


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------


def print_evaluation_summary(metrics: dict, warnings: list[str]) -> None:
    """Print a readable evaluation summary to stdout."""
    sep = "-" * 60
    print(sep)
    print("  Gold Classification Quality — Evaluation Summary")
    print(sep)
    print(f"  Evaluated at              : {metrics.get('evaluated_at', 'N/A')}")
    print(f"  Total records             : {metrics.get('total_records', 0)}")
    print()
    print(f"  Classification success    : {metrics.get('classification_success_rate', 0):.2%}")
    print(f"  Unknown label rate        : {metrics.get('unknown_label_rate', 0):.2%}")
    print(f"  Quarantine rate           : {metrics.get('quarantine_rate', 0):.2%}")
    print(f"  Export ready rate         : {metrics.get('export_ready_rate', 0):.2%}")
    print()
    mc = metrics.get("mean_classification_confidence")
    lcr = metrics.get("low_confidence_rate")
    print(
        f"  Mean confidence           : {mc:.2%}" if mc is not None
        else "  Mean confidence           : N/A"
    )
    print(
        f"  Low confidence rate       : {lcr:.2%}" if lcr is not None
        else "  Low confidence rate       : N/A"
    )
    print()
    print(f"  Flagged records           : {metrics.get('flagged_record_count', 0)}")

    label_dist = metrics.get("label_distribution", [])
    if label_dist:
        print()
        print("  Label distribution:")
        for entry in label_dist:
            print(f"    {entry['label']:<35} {entry['count']:>4}  ({entry['rate']:.1%})")

    print(sep)

    if warnings:
        print("  THRESHOLD WARNINGS:")
        for w in warnings:
            print(f"    !  {w}")
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
    path = output_dir / f"eval_gold_{eval_run_id}.json"
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
            "[eval_gold] MLflow is not installed. Skipping MLflow logging.",
            file=sys.stderr,
        )
        return

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    with mlflow.start_run(run_name="gold_classification_quality"):
        scalar_keys = [
            "classification_success_rate",
            "unknown_label_rate",
            "quarantine_rate",
            "export_ready_rate",
            "mean_classification_confidence",
            "low_confidence_rate",
        ]
        for key in scalar_keys:
            val = metrics.get(key)
            if val is not None:
                mlflow.log_metric(key, float(val))

        mlflow.log_param("total_records", metrics.get("total_records", 0))
        mlflow.log_param("flagged_record_count", metrics.get("flagged_record_count", 0))
        mlflow.log_param("threshold_warnings_count", len(warnings))

        if flagged_path and flagged_path.exists():
            mlflow.log_artifact(str(flagged_path))

        print(f"[eval_gold] Metrics logged to MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")


# ---------------------------------------------------------------------------
# Top-level evaluation function
# ---------------------------------------------------------------------------


def run_eval_gold(
    input_dir: Optional[str] = None,
    input_file: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    log_mlflow: bool = False,
) -> dict:
    """
    Run Gold classification quality evaluation.

    Returns the metrics dict.
    """
    input_dir_path = Path(input_dir) if input_dir else None
    input_file_path = Path(input_file) if input_file else None
    output_dir_path = Path(output_dir)

    artifact_paths = collect_artifact_paths(input_dir_path, input_file_path)
    print(f"[eval_gold] Evaluating {len(artifact_paths)} Gold artifact(s)...")

    records = load_gold_artifacts(artifact_paths)
    if not records:
        raise ValueError("No valid Gold records could be loaded.")

    metrics = compute_metrics(records)
    warnings = check_thresholds(metrics)

    print_evaluation_summary(metrics, warnings)

    eval_artifact_path = write_eval_artifact(metrics, warnings, output_dir_path)
    print(f"[eval_gold] Evaluation artifact written → {eval_artifact_path}")

    # Write flagged records as a separate artifact
    flagged_path = None
    if metrics.get("flagged_records"):
        flagged_path = output_dir_path / "flagged_gold_records.json"
        flagged_path.write_text(
            json.dumps(metrics["flagged_records"], indent=2, default=str), encoding="utf-8"
        )
        print(f"[eval_gold] Flagged records written → {flagged_path}")

    if log_mlflow:
        log_to_mlflow(metrics, warnings, flagged_path)

    return metrics


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="eval_gold",
        description=(
            "Evaluate Gold JSON artifacts produced by classify_gold.py. "
            "Computes classification quality metrics and writes a JSON evaluation summary."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing Gold JSON artifacts to evaluate.",
    )
    group.add_argument(
        "--input",
        metavar="FILE",
        help="Single Gold JSON artifact file to evaluate.",
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
    run_eval_gold(
        input_dir=args.input_dir,
        input_file=args.input,
        output_dir=args.output_dir,
        log_mlflow=args.mlflow,
    )


if __name__ == "__main__":
    main()
