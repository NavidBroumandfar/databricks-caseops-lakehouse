"""
eval_silver.py — Silver extraction quality evaluation (Phase A-2)

Reads one or more Silver JSON artifacts produced by extract_silver.py,
computes extraction quality metrics, prints a readable summary, and writes
a JSON evaluation artifact.

This is an independent evaluation script — it is not run inline during
pipeline execution. This separation keeps pipeline logic clean and allows
re-evaluation against any Silver snapshot without re-running extraction.

Evaluation metrics align with docs/evaluation-plan.md § 2. Extraction Quality (Silver).
MLflow logging hooks are included but optional; they are no-ops when MLflow
is not installed or not configured.

Usage:
    # Evaluate all Silver artifacts in a directory
    python src/evaluation/eval_silver.py --input-dir output/silver

    # Evaluate a single Silver artifact
    python src/evaluation/eval_silver.py --input output/silver/<record>.json

    # Write evaluation output to a specific directory
    python src/evaluation/eval_silver.py --input-dir output/silver --output-dir output/eval

    # Log metrics to MLflow (optional)
    python src/evaluation/eval_silver.py --input-dir output/silver --mlflow

Authoritative evaluation plan: docs/evaluation-plan.md § Extraction Quality (Silver)
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

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.silver_schema import FDA_REQUIRED_FIELDS, FDA_ALL_FIELDS


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
MLFLOW_EXPERIMENT_NAME = "caseops/silver/extraction_quality"

# Thresholds from docs/evaluation-plan.md § Extraction Quality (Silver)
TARGET_SCHEMA_VALIDITY_RATE = 0.80
TARGET_INVALID_RATE = 0.10
TARGET_MEAN_COVERAGE = 0.75
TARGET_P25_COVERAGE = 0.50
TARGET_REQUIRED_NULL_RATE = 0.05


# ---------------------------------------------------------------------------
# Artifact loading
# ---------------------------------------------------------------------------

def load_silver_artifacts(paths: list[Path]) -> list[dict]:
    """Load and return a list of Silver record dicts from JSON files."""
    records = []
    for p in paths:
        try:
            records.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as exc:  # noqa: BLE001
            print(f"[eval_silver] Warning: could not load {p}: {exc}", file=sys.stderr)
    return records


def collect_artifact_paths(input_dir: Optional[Path], input_file: Optional[Path]) -> list[Path]:
    """Resolve the list of Silver artifact paths to evaluate."""
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
    Compute Silver extraction quality metrics over a list of Silver record dicts.

    Metrics align with docs/evaluation-plan.md § 2. Extraction Quality (Silver):
        - schema_validity_rate
        - partial_validity_rate
        - invalid_rate
        - mean_field_coverage_pct
        - p25_field_coverage_pct
        - required_field_null_rate
        - validation_error_frequency
        - flagged_records
    """
    total = len(records)
    if total == 0:
        return {"error": "No records to evaluate."}

    valid_count = sum(1 for r in records if r.get("validation_status") == "valid")
    partial_count = sum(1 for r in records if r.get("validation_status") == "partial")
    invalid_count = sum(1 for r in records if r.get("validation_status") == "invalid")

    # Field coverage percentages
    coverage_values = [
        r["field_coverage_pct"]
        for r in records
        if r.get("field_coverage_pct") is not None
    ]
    mean_coverage = round(statistics.mean(coverage_values), 4) if coverage_values else None
    p25_coverage = round(_percentile(coverage_values, 25), 4) if coverage_values else None

    # Required field null rate across all records
    required_field_null_rate = _compute_required_null_rate(records)

    # Validation error frequency
    error_freq = _compute_error_frequency(records)

    # Flagged records
    flagged = _identify_flagged_records(records)

    return {
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_records": total,
        "schema_validity_rate": round(valid_count / total, 4),
        "partial_validity_rate": round(partial_count / total, 4),
        "invalid_rate": round(invalid_count / total, 4),
        "mean_field_coverage_pct": mean_coverage,
        "p25_field_coverage_pct": p25_coverage,
        "required_field_null_rate": required_field_null_rate,
        "validation_error_frequency": error_freq,
        "flagged_record_count": len(flagged),
        "flagged_records": flagged,
    }


def _percentile(values: list[float], pct: int) -> float:
    """Return the Nth percentile of a list (nearest-rank method)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = max(0, int(len(sorted_vals) * pct / 100) - 1)
    return sorted_vals[idx]


def _compute_required_null_rate(records: list[dict]) -> float:
    """
    Compute the fraction of required fields that are null across all records.

    Denominator = total_records * len(FDA_REQUIRED_FIELDS).
    Null is defined as: field absent, null, empty string, or empty list.
    """
    if not records:
        return 0.0

    total_required_slots = len(records) * len(FDA_REQUIRED_FIELDS)
    if total_required_slots == 0:
        return 0.0

    null_count = 0
    for record in records:
        fields_dict = (record.get("extracted_fields") or {})
        for field_name in FDA_REQUIRED_FIELDS:
            value = fields_dict.get(field_name)
            if value is None or value == [] or value == "":
                null_count += 1

    return round(null_count / total_required_slots, 4)


def _compute_error_frequency(records: list[dict]) -> list[dict]:
    """
    Count and rank the most frequently occurring validation error messages.

    Returns a list of {error, count, rate} dicts ordered by frequency descending.
    """
    all_errors = []
    for record in records:
        errors = record.get("validation_errors") or []
        all_errors.extend(errors)

    if not all_errors:
        return []

    counter = Counter(all_errors)
    total_records = len(records)
    return [
        {
            "error": error,
            "count": count,
            "rate": round(count / total_records, 4),
        }
        for error, count in counter.most_common()
    ]


def _identify_flagged_records(records: list[dict]) -> list[dict]:
    """
    Return summary dicts for records that warrant human review.

    Flags from docs/evaluation-plan.md § Human Review Readiness:
        - validation_status = 'invalid'
        - field_coverage_pct < 0.40
    """
    flagged = []
    for r in records:
        reasons = []
        status = r.get("validation_status")
        coverage = r.get("field_coverage_pct")

        if status == "invalid":
            reasons.append("validation_status=invalid")
        if coverage is not None and coverage < 0.40:
            reasons.append(f"low_field_coverage (field_coverage_pct={coverage:.2f})")

        if reasons:
            flagged.append({
                "extraction_id": r.get("extraction_id"),
                "bronze_record_id": r.get("bronze_record_id"),
                "document_id": r.get("document_id"),
                "validation_status": status,
                "field_coverage_pct": coverage,
                "validation_errors": r.get("validation_errors", []),
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

    svr = metrics.get("schema_validity_rate")
    ir = metrics.get("invalid_rate")
    mc = metrics.get("mean_field_coverage_pct")
    p25 = metrics.get("p25_field_coverage_pct")
    rnr = metrics.get("required_field_null_rate")

    if svr is not None and svr < TARGET_SCHEMA_VALIDITY_RATE:
        warnings.append(
            f"schema_validity_rate {svr:.2%} is below target {TARGET_SCHEMA_VALIDITY_RATE:.2%} "
            "— review extraction prompt or rule logic for the failing document class"
        )
    if ir is not None and ir > TARGET_INVALID_RATE:
        warnings.append(
            f"invalid_rate {ir:.2%} exceeds target {TARGET_INVALID_RATE:.2%} "
            "— review prompt template and required field extraction coverage"
        )
    if mc is not None and mc < TARGET_MEAN_COVERAGE:
        warnings.append(
            f"mean_field_coverage_pct {mc:.2%} is below target {TARGET_MEAN_COVERAGE:.2%}"
        )
    if p25 is not None and p25 < TARGET_P25_COVERAGE:
        warnings.append(
            f"p25_field_coverage_pct {p25:.2%} is below target {TARGET_P25_COVERAGE:.2%} "
            "— bottom quartile of records have low coverage"
        )
    if rnr is not None and rnr > TARGET_REQUIRED_NULL_RATE:
        warnings.append(
            f"required_field_null_rate {rnr:.2%} exceeds target {TARGET_REQUIRED_NULL_RATE:.2%} "
            "— specific required fields may not exist in this document domain; "
            "check schema or extraction logic"
        )
    return warnings


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------

def print_evaluation_summary(metrics: dict, warnings: list[str]) -> None:
    """Print a readable evaluation summary to stdout."""
    sep = "-" * 60
    print(sep)
    print("  Silver Extraction Quality — Evaluation Summary")
    print(sep)
    print(f"  Evaluated at          : {metrics.get('evaluated_at', 'N/A')}")
    print(f"  Total records         : {metrics.get('total_records', 0)}")
    print()
    print(f"  Valid rate            : {metrics.get('schema_validity_rate', 0):.2%}")
    print(f"  Partial rate          : {metrics.get('partial_validity_rate', 0):.2%}")
    print(f"  Invalid rate          : {metrics.get('invalid_rate', 0):.2%}")
    print()
    mc = metrics.get("mean_field_coverage_pct")
    p25 = metrics.get("p25_field_coverage_pct")
    print(f"  Mean field coverage   : {mc:.2%}" if mc is not None else "  Mean field coverage   : N/A")
    print(f"  P25 field coverage    : {p25:.2%}" if p25 is not None else "  P25 field coverage    : N/A")
    print(f"  Req. field null rate  : {metrics.get('required_field_null_rate', 0):.2%}")
    print()
    print(f"  Flagged records       : {metrics.get('flagged_record_count', 0)}")

    error_freq = metrics.get("validation_error_frequency", [])
    if error_freq:
        print()
        print("  Top validation errors:")
        for entry in error_freq[:5]:
            print(f"    [{entry['count']}x] {entry['error']}")

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
    path = output_dir / f"eval_silver_{eval_run_id}.json"
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
            "[eval_silver] MLflow is not installed. Skipping MLflow logging.",
            file=sys.stderr,
        )
        return

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    with mlflow.start_run(run_name="silver_extraction_quality"):
        scalar_keys = [
            "schema_validity_rate",
            "partial_validity_rate",
            "invalid_rate",
            "mean_field_coverage_pct",
            "p25_field_coverage_pct",
            "required_field_null_rate",
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

        print(f"[eval_silver] Metrics logged to MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")


# ---------------------------------------------------------------------------
# Top-level evaluation function
# ---------------------------------------------------------------------------

def run_eval_silver(
    input_dir: Optional[str] = None,
    input_file: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    log_mlflow: bool = False,
) -> dict:
    """
    Run Silver extraction quality evaluation.

    Returns the metrics dict.
    """
    input_dir_path = Path(input_dir) if input_dir else None
    input_file_path = Path(input_file) if input_file else None
    output_dir_path = Path(output_dir)

    artifact_paths = collect_artifact_paths(input_dir_path, input_file_path)
    print(f"[eval_silver] Evaluating {len(artifact_paths)} Silver artifact(s)...")

    records = load_silver_artifacts(artifact_paths)
    if not records:
        raise ValueError("No valid Silver records could be loaded.")

    metrics = compute_metrics(records)
    warnings = check_thresholds(metrics)

    print_evaluation_summary(metrics, warnings)

    eval_artifact_path = write_eval_artifact(metrics, warnings, output_dir_path)
    print(f"[eval_silver] Evaluation artifact written → {eval_artifact_path}")

    # Write flagged records as a separate artifact
    flagged_path = None
    if metrics.get("flagged_records"):
        flagged_path = output_dir_path / "flagged_silver_records.json"
        flagged_path.write_text(
            json.dumps(metrics["flagged_records"], indent=2, default=str), encoding="utf-8"
        )
        print(f"[eval_silver] Flagged records written → {flagged_path}")

    if log_mlflow:
        log_to_mlflow(metrics, warnings, flagged_path)

    return metrics


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="eval_silver",
        description=(
            "Evaluate Silver JSON artifacts produced by extract_silver.py. "
            "Computes extraction quality metrics and writes a JSON evaluation summary."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing Silver JSON artifacts to evaluate.",
    )
    group.add_argument(
        "--input",
        metavar="FILE",
        help="Single Silver JSON artifact file to evaluate.",
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
    run_eval_silver(
        input_dir=args.input_dir,
        input_file=args.input,
        output_dir=args.output_dir,
        log_mlflow=args.mlflow,
    )


if __name__ == "__main__":
    main()
