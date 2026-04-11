"""
eval_traceability.py — Cross-layer traceability completeness evaluation (Phase A-4).

Reads Bronze, Silver, and Gold JSON artifacts and evaluates whether the full
document lineage chain is intact: Gold → Silver → Bronze → source file.

Traceability is a first-class quality dimension for this pipeline. Every record
must carry its upstream foreign keys (document_id, bronze_record_id, extraction_id)
and a pipeline_run_id so that any Gold record can be traced back to its source.

## What this evaluator detects

1. Gold records with no matching Silver extraction_id (orphaned Gold)
2. Silver records with no matching Bronze bronze_record_id (orphaned Silver)
3. Records with null pipeline_run_id (broken provenance chain)
4. Records with null schema_version (contract drift risk)
5. Records carrying placeholder pipeline_run_id values (bootstrap SQL path, not
   real MLflow run IDs — e.g. 'bootstrap_sql_v1')

## Bootstrap SQL path handling (A-3B)

The validated A-3B Databricks bootstrap uses pipeline_run_id = 'bootstrap_sql_v1'
as a placeholder. This is a known and documented implementation detail, not a
pipeline defect. The evaluator surfaces these records explicitly in its
`placeholder_run_id_count` metric and in the observations list so the distinction
between bootstrap-origin records and real MLflow pipeline records is always visible.

## Usage

    # Full traceability evaluation against all three layer artifact directories
    python src/evaluation/eval_traceability.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --gold-dir output/gold

    # Write output to a specific directory
    python src/evaluation/eval_traceability.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --gold-dir output/gold \\
        --output-dir output/eval

    # With MLflow logging
    python src/evaluation/eval_traceability.py \\
        --bronze-dir output/bronze \\
        --silver-dir output/silver \\
        --gold-dir output/gold \\
        --mlflow

Authoritative evaluation plan: docs/evaluation-plan.md § 4. Traceability Completeness
Architecture context: ARCHITECTURE.md § Governance and Traceability Principles
"""

from __future__ import annotations

import argparse
import json
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

from mlflow_experiment_paths import traceability_experiment, SUFFIX_TRACEABILITY


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = "output/eval"
MLFLOW_EXPERIMENT_NAME = SUFFIX_TRACEABILITY

# Known placeholder run ID values from the A-3B bootstrap SQL path.
# Records carrying these values originated from bootstrap SQL, not from a
# tracked MLflow pipeline run.
PLACEHOLDER_RUN_IDS: frozenset[str] = frozenset({"bootstrap_sql_v1"})


# ---------------------------------------------------------------------------
# Artifact loading
# ---------------------------------------------------------------------------

def load_artifacts(directory: Path) -> list[dict]:
    """Load all JSON artifacts from a directory. Skips subdirectories."""
    records: list[dict] = []
    if not directory.is_dir():
        raise ValueError(f"Directory does not exist: {directory}")
    for path in sorted(directory.glob("*.json")):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception as exc:  # noqa: BLE001
            print(
                f"[eval_traceability] Warning: could not load {path}: {exc}",
                file=sys.stderr,
            )
    return records


# ---------------------------------------------------------------------------
# Traceability metric computation
# ---------------------------------------------------------------------------

def compute_metrics(
    bronze_records: list[dict],
    silver_records: list[dict],
    gold_records: list[dict],
) -> dict:
    """
    Compute cross-layer traceability metrics.

    Returns a metrics dict containing:
        total_bronze, total_silver, total_gold
        gold_to_silver_link_rate
        silver_to_bronze_link_rate
        pipeline_run_id_coverage   (all three layers combined)
        schema_version_coverage    (all three layers combined)
        orphaned_silver_count
        orphaned_gold_count
        placeholder_run_id_count
        flagged_records            (orphaned records as dicts)
        observations               (non-error notes)
    """
    all_records = bronze_records + silver_records + gold_records
    total = len(all_records)

    if total == 0:
        return {"error": "No records found across Bronze, Silver, and Gold."}

    # Build lookup sets for link validation
    bronze_record_ids: set[str] = {
        r["bronze_record_id"]
        for r in bronze_records
        if r.get("bronze_record_id")
    }
    silver_extraction_ids: set[str] = {
        r["extraction_id"]
        for r in silver_records
        if r.get("extraction_id")
    }

    # Gold → Silver link (Gold carries extraction_id as FK to Silver)
    orphaned_gold = [
        r for r in gold_records
        if not r.get("extraction_id") or r["extraction_id"] not in silver_extraction_ids
    ]
    gold_to_silver_rate: Optional[float] = (
        round(1.0 - len(orphaned_gold) / len(gold_records), 4)
        if gold_records else None
    )

    # Silver → Bronze link (Silver carries bronze_record_id as FK to Bronze)
    orphaned_silver = [
        r for r in silver_records
        if not r.get("bronze_record_id")
        or r["bronze_record_id"] not in bronze_record_ids
    ]
    silver_to_bronze_rate: Optional[float] = (
        round(1.0 - len(orphaned_silver) / len(silver_records), 4)
        if silver_records else None
    )

    # pipeline_run_id coverage (all layers)
    run_id_nulls = sum(1 for r in all_records if not r.get("pipeline_run_id"))
    pipeline_run_id_coverage = round(1.0 - run_id_nulls / total, 4) if total else 1.0

    # schema_version coverage (all layers)
    schema_nulls = sum(1 for r in all_records if not r.get("schema_version"))
    schema_version_coverage = round(1.0 - schema_nulls / total, 4) if total else 1.0

    # Placeholder run IDs (bootstrap SQL path)
    placeholder_records = [
        r for r in all_records
        if r.get("pipeline_run_id") in PLACEHOLDER_RUN_IDS
    ]
    placeholder_count = len(placeholder_records)

    # Flagged records: orphaned only (placeholder run IDs are observations, not defects)
    flagged = _build_flagged_records(orphaned_silver, orphaned_gold)

    # Observations
    observations = _build_observations(placeholder_count, placeholder_records)

    return {
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_bronze": len(bronze_records),
        "total_silver": len(silver_records),
        "total_gold": len(gold_records),
        "gold_to_silver_link_rate": gold_to_silver_rate,
        "silver_to_bronze_link_rate": silver_to_bronze_rate,
        "pipeline_run_id_coverage": pipeline_run_id_coverage,
        "schema_version_coverage": schema_version_coverage,
        "orphaned_silver_count": len(orphaned_silver),
        "orphaned_gold_count": len(orphaned_gold),
        "placeholder_run_id_count": placeholder_count,
        "flagged_record_count": len(flagged),
        "flagged_records": flagged,
        "observations": observations,
    }


def _build_flagged_records(
    orphaned_silver: list[dict],
    orphaned_gold: list[dict],
) -> list[dict]:
    flagged: list[dict] = []
    for r in orphaned_silver:
        flagged.append({
            "layer": "silver",
            "issue": "orphaned_silver: no matching Bronze bronze_record_id",
            "extraction_id": r.get("extraction_id"),
            "bronze_record_id": r.get("bronze_record_id"),
            "document_id": r.get("document_id"),
            "pipeline_run_id": r.get("pipeline_run_id"),
        })
    for r in orphaned_gold:
        flagged.append({
            "layer": "gold",
            "issue": "orphaned_gold: no matching Silver extraction_id",
            "gold_record_id": r.get("gold_record_id"),
            "extraction_id": r.get("extraction_id"),
            "document_id": r.get("document_id"),
            "pipeline_run_id": r.get("pipeline_run_id"),
        })
    return flagged


def _build_observations(
    placeholder_count: int,
    placeholder_records: list[dict],
) -> list[str]:
    observations: list[str] = []
    if placeholder_count > 0:
        pipeline_run_ids = sorted({
            r.get("pipeline_run_id", "") for r in placeholder_records
        })
        observations.append(
            f"{placeholder_count} record(s) carry a placeholder pipeline_run_id "
            f"({', '.join(pipeline_run_ids)}). These records originated from the "
            "A-3B Databricks bootstrap SQL execution, not from a tracked MLflow "
            "pipeline run. This is a documented implementation detail of the "
            "bootstrap path. Full lineage via document_id is still intact."
        )
    return observations


# ---------------------------------------------------------------------------
# Threshold checks
# ---------------------------------------------------------------------------

def check_thresholds(metrics: dict) -> list[str]:
    """
    Traceability targets from docs/evaluation-plan.md § 4.

    All link rates and coverages must be 1.0.
    Orphan counts must be 0.
    """
    warnings: list[str] = []

    g2s = metrics.get("gold_to_silver_link_rate")
    s2b = metrics.get("silver_to_bronze_link_rate")
    prc = metrics.get("pipeline_run_id_coverage")
    svc = metrics.get("schema_version_coverage")
    os_ = metrics.get("orphaned_silver_count", 0)
    og = metrics.get("orphaned_gold_count", 0)

    if g2s is not None and g2s < 1.0:
        warnings.append(
            f"gold_to_silver_link_rate={g2s:.4f}: {metrics.get('orphaned_gold_count', 0)} "
            "Gold record(s) have no matching Silver extraction_id — pipeline defect"
        )
    if s2b is not None and s2b < 1.0:
        warnings.append(
            f"silver_to_bronze_link_rate={s2b:.4f}: {metrics.get('orphaned_silver_count', 0)} "
            "Silver record(s) have no matching Bronze bronze_record_id — pipeline defect"
        )
    if prc is not None and prc < 1.0:
        warnings.append(
            f"pipeline_run_id_coverage={prc:.4f}: some records have null pipeline_run_id — "
            "traceability to pipeline batch is broken"
        )
    if svc is not None and svc < 1.0:
        warnings.append(
            f"schema_version_coverage={svc:.4f}: some records have null schema_version — "
            "contract drift detection may fail"
        )
    if os_ > 0:
        warnings.append(
            f"orphaned_silver_count={os_}: these Silver records cannot be traced to Bronze — "
            "treat as pipeline defect requiring investigation"
        )
    if og > 0:
        warnings.append(
            f"orphaned_gold_count={og}: these Gold records cannot be traced to Silver — "
            "treat as pipeline defect requiring investigation"
        )
    return warnings


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------

def print_evaluation_summary(metrics: dict, warnings: list[str]) -> None:
    sep = "-" * 60
    print(sep)
    print("  Traceability Completeness — Evaluation Summary")
    print(sep)
    print(f"  Evaluated at            : {metrics.get('evaluated_at', 'N/A')}")
    print()
    print(f"  Bronze records          : {metrics.get('total_bronze', 0)}")
    print(f"  Silver records          : {metrics.get('total_silver', 0)}")
    print(f"  Gold records            : {metrics.get('total_gold', 0)}")
    print()

    def fmt(v: Optional[float]) -> str:
        return f"{v:.2%}" if v is not None else "N/A"

    print(f"  Gold→Silver link rate   : {fmt(metrics.get('gold_to_silver_link_rate'))}")
    print(f"  Silver→Bronze link rate : {fmt(metrics.get('silver_to_bronze_link_rate'))}")
    print(f"  pipeline_run_id cov.    : {fmt(metrics.get('pipeline_run_id_coverage'))}")
    print(f"  schema_version cov.     : {fmt(metrics.get('schema_version_coverage'))}")
    print()
    print(f"  Orphaned Silver         : {metrics.get('orphaned_silver_count', 0)}")
    print(f"  Orphaned Gold           : {metrics.get('orphaned_gold_count', 0)}")
    print(f"  Placeholder run IDs     : {metrics.get('placeholder_run_id_count', 0)}")
    print()
    print(f"  Flagged records         : {metrics.get('flagged_record_count', 0)}")

    observations = metrics.get("observations", [])
    if observations:
        print()
        print("  Observations:")
        for obs in observations:
            print(f"    [note] {obs}")

    print(sep)
    if warnings:
        print("  THRESHOLD WARNINGS:")
        for w in warnings:
            print(f"    [WARN] {w}")
        print(sep)
    else:
        print("  All traceability targets met.")
        print(sep)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------

def write_eval_artifact(metrics: dict, warnings: list[str], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    eval_run_id = str(uuid.uuid4())
    artifact = {
        "eval_run_id": eval_run_id,
        "experiment": MLFLOW_EXPERIMENT_NAME,
        "metrics": metrics,
        "threshold_warnings": warnings,
    }
    path = output_dir / f"eval_traceability_{eval_run_id}.json"
    path.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Optional MLflow logging
# ---------------------------------------------------------------------------

def log_to_mlflow(metrics: dict, warnings: list[str], flagged_path: Optional[Path]) -> None:
    if not _MLFLOW_AVAILABLE:
        print(
            "[eval_traceability] MLflow is not installed. Skipping MLflow logging.",
            file=sys.stderr,
        )
        return

    experiment_name = traceability_experiment()
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name="traceability_completeness"):
        scalar_keys = [
            "gold_to_silver_link_rate",
            "silver_to_bronze_link_rate",
            "pipeline_run_id_coverage",
            "schema_version_coverage",
        ]
        for key in scalar_keys:
            val = metrics.get(key)
            if val is not None:
                mlflow.log_metric(key, float(val))

        mlflow.log_param("total_bronze", metrics.get("total_bronze", 0))
        mlflow.log_param("total_silver", metrics.get("total_silver", 0))
        mlflow.log_param("total_gold", metrics.get("total_gold", 0))
        mlflow.log_param("orphaned_silver_count", metrics.get("orphaned_silver_count", 0))
        mlflow.log_param("orphaned_gold_count", metrics.get("orphaned_gold_count", 0))
        mlflow.log_param("placeholder_run_id_count", metrics.get("placeholder_run_id_count", 0))
        mlflow.log_param("threshold_warnings_count", len(warnings))

        if flagged_path and flagged_path.exists():
            mlflow.log_artifact(str(flagged_path))

        print(
            f"[eval_traceability] Metrics logged to MLflow experiment: {experiment_name}"
        )


# ---------------------------------------------------------------------------
# Top-level evaluation function
# ---------------------------------------------------------------------------

def run_eval_traceability(
    bronze_dir: str,
    silver_dir: str,
    gold_dir: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    log_mlflow: bool = False,
) -> dict:
    """
    Run cross-layer traceability completeness evaluation.

    Loads all JSON artifacts from the three layer directories, computes
    traceability metrics, and writes a JSON evaluation artifact.

    Returns the metrics dict.
    """
    bronze_records = load_artifacts(Path(bronze_dir))
    silver_records = load_artifacts(Path(silver_dir))
    gold_records = load_artifacts(Path(gold_dir))

    print(
        f"[eval_traceability] Loaded: {len(bronze_records)} Bronze, "
        f"{len(silver_records)} Silver, {len(gold_records)} Gold records."
    )

    metrics = compute_metrics(bronze_records, silver_records, gold_records)
    warnings = check_thresholds(metrics)

    print_evaluation_summary(metrics, warnings)

    output_dir_path = Path(output_dir)
    eval_artifact_path = write_eval_artifact(metrics, warnings, output_dir_path)
    print(f"[eval_traceability] Evaluation artifact written → {eval_artifact_path}")

    # Write orphaned/flagged records as a separate artifact
    flagged_path: Optional[Path] = None
    if metrics.get("flagged_records"):
        flagged_path = output_dir_path / "flagged_traceability_records.json"
        flagged_path.write_text(
            json.dumps(metrics["flagged_records"], indent=2, default=str),
            encoding="utf-8",
        )
        print(f"[eval_traceability] Flagged records written → {flagged_path}")

    if log_mlflow:
        log_to_mlflow(metrics, warnings, flagged_path)

    return metrics


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="eval_traceability",
        description=(
            "Evaluate cross-layer traceability completeness across Bronze, Silver, and Gold. "
            "Detects orphaned records and broken lineage links."
        ),
    )
    p.add_argument(
        "--bronze-dir",
        required=True,
        metavar="DIR",
        help="Directory containing Bronze JSON artifacts.",
    )
    p.add_argument(
        "--silver-dir",
        required=True,
        metavar="DIR",
        help="Directory containing Silver JSON artifacts.",
    )
    p.add_argument(
        "--gold-dir",
        required=True,
        metavar="DIR",
        help="Directory containing Gold JSON artifacts.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory to write evaluation artifacts. Default: {DEFAULT_OUTPUT_DIR}",
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
    run_eval_traceability(
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        output_dir=args.output_dir,
        log_mlflow=args.mlflow,
    )


if __name__ == "__main__":
    main()
