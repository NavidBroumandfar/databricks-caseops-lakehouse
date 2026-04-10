"""
report_writer.py — Write structured A-4 evaluation reports to disk.

Takes an EvaluationReport (assembled by run_evaluation.py) and writes:
  1. A machine-readable JSON report: output/eval/report_<report_id>.json
  2. A human-readable text summary:  output/eval/report_<report_id>.txt

Design principles:
  - No external dependencies (stdlib only).
  - Reports are self-contained — every field needed to audit the evaluation is present.
  - The text summary is readable without any tooling.
  - Observations and warnings are surfaced prominently so nothing is hidden.

Architecture context: ARCHITECTURE.md § Evaluation and Observability Layer
"""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path
from typing import Optional

# Allow running from any working directory by ensuring the evaluation dir is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from report_models import EvaluationReport, LayerEvalResult, TraceabilityResult


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------

def _to_json(obj: object) -> object:
    """Recursively convert dataclasses and unknowns to JSON-serialisable form."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_json(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _to_json(v) for k, v in obj.items()}
    return obj


# ---------------------------------------------------------------------------
# JSON report writer
# ---------------------------------------------------------------------------

def write_json_report(report: EvaluationReport, output_dir: Path) -> Path:
    """Write the full EvaluationReport as a JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"report_{report.report_id}.json"
    path.write_text(
        json.dumps(_to_json(report), indent=2, default=str),
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# Text summary writer
# ---------------------------------------------------------------------------

_SEP = "=" * 70
_SUBSEP = "-" * 60


def _fmt_rate(val: Optional[float], decimals: int = 1) -> str:
    if val is None:
        return "N/A"
    return f"{val:.{decimals}%}"


def _layer_block(result: LayerEvalResult) -> list[str]:
    lines = [
        _SUBSEP,
        f"  Layer: {result.layer.upper()}  |  Experiment: {result.experiment}",
        _SUBSEP,
        f"  Records evaluated : {result.total_records}",
        f"  Flagged records   : {result.flagged_record_count}",
        "",
        "  Metrics:",
    ]
    for k, v in result.metrics.items():
        if k in ("flagged_records",):
            continue
        if isinstance(v, float):
            lines.append(f"    {k:<40} {v:.4f}")
        elif isinstance(v, list):
            lines.append(f"    {k:<40} [{len(v)} entries]")
        else:
            lines.append(f"    {k:<40} {v}")

    if result.threshold_warnings:
        lines.append("")
        lines.append("  Threshold warnings:")
        for w in result.threshold_warnings:
            lines.append(f"    [WARN] {w}")

    if result.observations:
        lines.append("")
        lines.append("  Observations:")
        for obs in result.observations:
            lines.append(f"    [note] {obs}")

    lines.append("")
    return lines


def _traceability_block(result: TraceabilityResult) -> list[str]:
    lines = [
        _SUBSEP,
        f"  Traceability  |  Experiment: {result.experiment}",
        _SUBSEP,
        f"  Bronze records    : {result.total_bronze}",
        f"  Silver records    : {result.total_silver}",
        f"  Gold records      : {result.total_gold}",
        "",
        f"  Gold→Silver link  : {_fmt_rate(result.gold_to_silver_link_rate)}",
        f"  Silver→Bronze link: {_fmt_rate(result.silver_to_bronze_link_rate)}",
        f"  pipeline_run_id   : {_fmt_rate(result.pipeline_run_id_coverage)}",
        f"  schema_version    : {_fmt_rate(result.schema_version_coverage)}",
        "",
        f"  Orphaned Silver   : {result.orphaned_silver_count}",
        f"  Orphaned Gold     : {result.orphaned_gold_count}",
        f"  Placeholder IDs   : {result.placeholder_run_id_count}",
    ]

    if result.placeholder_run_id_note:
        lines.append(f"    [note] {result.placeholder_run_id_note}")

    if result.threshold_warnings:
        lines.append("")
        lines.append("  Threshold warnings:")
        for w in result.threshold_warnings:
            lines.append(f"    [WARN] {w}")

    if result.observations:
        lines.append("")
        lines.append("  Observations:")
        for obs in result.observations:
            lines.append(f"    [note] {obs}")

    lines.append("")
    return lines


def write_text_summary(report: EvaluationReport, output_dir: Path) -> Path:
    """Write a human-readable text summary of the EvaluationReport."""
    output_dir.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        _SEP,
        "  CaseOps Pipeline — Evaluation Report",
        _SEP,
        f"  Report ID        : {report.report_id}",
        f"  Generated at     : {report.generated_at}",
        f"  Pipeline filter  : {report.pipeline_run_id_filter or 'all'}",
        f"  Review queue     : {report.review_queue_size} record(s) flagged",
        f"  Bootstrap path   : {'DETECTED' if report.bootstrap_path_detected else 'not detected'}",
        _SEP,
        "",
    ]

    if report.bronze:
        lines.extend(_layer_block(report.bronze))
    if report.silver:
        lines.extend(_layer_block(report.silver))
    if report.gold:
        lines.extend(_layer_block(report.gold))
    if report.traceability:
        lines.extend(_traceability_block(report.traceability))

    if report.all_warnings:
        lines.extend([
            _SEP,
            "  ALL THRESHOLD WARNINGS",
            _SEP,
        ])
        for w in report.all_warnings:
            lines.append(f"  [WARN] {w}")
        lines.append("")
    else:
        lines.extend([_SEP, "  All thresholds met.", _SEP, ""])

    if report.all_observations:
        lines.extend([_SUBSEP, "  All Observations", _SUBSEP])
        for obs in report.all_observations:
            lines.append(f"  [note] {obs}")
        lines.append("")

    path = output_dir / f"report_{report.report_id}.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Convenience: write both formats
# ---------------------------------------------------------------------------

def write_report(report: EvaluationReport, output_dir: Path) -> tuple[Path, Path]:
    """
    Write both JSON and text versions of an EvaluationReport.

    Returns (json_path, text_path).
    """
    json_path = write_json_report(report, output_dir)
    text_path = write_text_summary(report, output_dir)
    return json_path, text_path
