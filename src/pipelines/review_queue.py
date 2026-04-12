"""
src/pipelines/review_queue.py — E-0 Human Review Queue Derivation and Materialization.

Derives the human review queue from existing pipeline artifacts (run summaries
produced by run_classify_gold) and writes the queue artifact to disk.

This module is upstream-only. It works entirely from the pipeline summaries
and artifact paths already produced by the Gold pipeline. It does not re-run
classification, call any AI service, or interact with Bedrock.

Design:
    The review queue is derived deterministically from the per-record pipeline
    summaries collected by run_classify_gold(). Records are included in the
    review queue if they meet any of the following criteria:
        1. outcome_category == 'quarantined'     → review_reason_category: quarantined
        2. outcome_category == 'contract_blocked' → review_reason_category: contract_blocked
        3. document_type_label == 'unknown'       → review_reason_category: extraction_failed
           (when not already quarantined by routing)

    Records that exported successfully are NOT included in the review queue.
    Records that are skipped_not_export_ready are NOT included unless they
    carry an explicit extraction failure signal (unknown document type).

    This logic is additive — the existing pipeline results are unchanged.
    The review queue is an additional artifact written after the pipeline run.

Integration:
    Standalone entry point:
        build_review_queue_from_summaries(summaries, pipeline_run_id, ...)

    Pipeline integration (optional, via classify_gold.py --review-queue-dir):
        When --review-queue-dir is provided, run_classify_gold() calls
        build_review_queue_from_summaries() after the batch and writes
        the queue artifact. The automated pipeline path is unchanged.

Module boundary (E-0):
    classify_gold.py   → runs pipeline, collects summaries → calls build_review_queue_from_summaries
    review_queue.py    → derives queue from summaries → writes review queue artifact

This module does NOT own:
    - Classification logic (classify_gold.py)
    - Export or contract validation (export_handoff.py)
    - Review decisions or reprocessing requests (review_decision.py)
    - Any downstream review workflow, UI, or case management tooling

Phase: E-0
Authoritative scope: PROJECT_SPEC.md § Phase E-0
Architecture context: ARCHITECTURE.md § Future Evolution (Human Review Loop)
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.review_queue import (
    ALL_REVIEW_REASON_CATEGORIES,
    REVIEW_QUEUE_SCHEMA_VERSION,
    REVIEW_REASON_CONTRACT_BLOCKED,
    REVIEW_REASON_EXTRACTION_FAILED,
    REVIEW_REASON_QUARANTINED,
    ReviewQueueArtifact,
    ReviewQueueEntry,
    make_queue_entry_id,
    make_review_queue_id,
    make_review_reason,
)


# ---------------------------------------------------------------------------
# Outcome constants (mirrored from handoff_report to avoid circular import)
# ---------------------------------------------------------------------------

_OUTCOME_QUARANTINED = "quarantined"
_OUTCOME_CONTRACT_BLOCKED = "contract_blocked"
_OUTCOME_SKIPPED = "skipped_not_export_ready"
_DOCUMENT_TYPE_UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Queue derivation
# ---------------------------------------------------------------------------


def _should_include_in_review_queue(summary: dict) -> tuple[bool, str]:
    """
    Determine whether a per-record pipeline summary should enter the review queue.

    Returns (include: bool, reason_category: str).

    Inclusion criteria (checked in priority order):
        1. outcome_category == 'quarantined'      → quarantined
        2. outcome_category == 'contract_blocked' → contract_blocked
        3. document_type_label == 'unknown' AND
           outcome_category == 'skipped_not_export_ready' → extraction_failed

    Records that exported successfully are NOT included.
    """
    outcome = summary.get("outcome_category", "")
    document_type = summary.get("document_type_label", "")

    if outcome == _OUTCOME_QUARANTINED:
        return True, REVIEW_REASON_QUARANTINED
    if outcome == _OUTCOME_CONTRACT_BLOCKED:
        return True, REVIEW_REASON_CONTRACT_BLOCKED
    if outcome == _OUTCOME_SKIPPED and document_type == _DOCUMENT_TYPE_UNKNOWN:
        return True, REVIEW_REASON_EXTRACTION_FAILED

    return False, ""


def build_review_queue_from_summaries(
    summaries: list[dict],
    pipeline_run_id: str,
    bundle_path: Optional[str] = None,
    report_path: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> ReviewQueueArtifact:
    """
    Derive a ReviewQueueArtifact from per-record pipeline summaries.

    Examines each summary produced by run_classify_gold() and adds records
    to the review queue if they meet review-worthy criteria (quarantined,
    contract-blocked, or extraction-failed).

    Args:
        summaries:
            Per-record summary dicts from run_classify_gold(). Each must
            include outcome_category, document_id, gold_record_id,
            document_type_label, routing_label, gold_artifact_path,
            export_artifact_path, and contract_validation_errors.
        pipeline_run_id:
            The run ID shared across all records in this batch.
        bundle_path:
            Path to the B-5 batch manifest for this run (optional).
        report_path:
            Path to the B-4 handoff batch report for this run (optional).
        generated_at:
            ISO 8601 UTC timestamp; defaults to UTC now.

    Returns:
        ReviewQueueArtifact with all queue entries populated.
    """
    if generated_at is None:
        generated_at = datetime.now(tz=timezone.utc).isoformat()

    entries: list[ReviewQueueEntry] = []
    reason_counts: dict[str, int] = {cat: 0 for cat in ALL_REVIEW_REASON_CATEGORIES}

    for summary in summaries:
        include, reason_category = _should_include_in_review_queue(summary)
        if not include:
            continue

        document_type_label = summary.get("document_type_label", "unknown")
        routing_label = summary.get("routing_label", "")
        contract_errors = summary.get("contract_validation_errors") or []

        review_reason = make_review_reason(
            reason_category=reason_category,
            document_type_label=document_type_label,
            routing_label=routing_label,
            contract_validation_errors=contract_errors if contract_errors else None,
        )

        entry = ReviewQueueEntry(
            queue_entry_id=make_queue_entry_id(),
            document_id=summary.get("document_id", ""),
            gold_record_id=summary.get("gold_record_id"),
            pipeline_run_id=pipeline_run_id,
            document_type_label=document_type_label,
            routing_label=routing_label,
            review_reason=review_reason,
            review_reason_category=reason_category,
            gold_artifact_path=summary.get("gold_artifact_path"),
            export_artifact_path=summary.get("export_artifact_path"),
            bundle_path=bundle_path,
            report_path=report_path,
            contract_validation_errors=list(contract_errors),
        )
        entries.append(entry)

        if reason_category in reason_counts:
            reason_counts[reason_category] += 1
        else:
            reason_counts[reason_category] = 1

    review_notes = _build_review_notes(
        total_eligible=len(summaries),
        n_queued=len(entries),
        reason_counts=reason_counts,
        bundle_path=bundle_path,
        report_path=report_path,
    )

    return ReviewQueueArtifact(
        review_queue_id=make_review_queue_id(),
        pipeline_run_id=pipeline_run_id,
        generated_at=generated_at,
        schema_version=REVIEW_QUEUE_SCHEMA_VERSION,
        total_records_reviewed=len(summaries),
        total_entries=len(entries),
        entries_by_reason=dict(reason_counts),
        queue_entries=entries,
        review_notes=review_notes,
    )


# ---------------------------------------------------------------------------
# Review notes builder
# ---------------------------------------------------------------------------


def _build_review_notes(
    total_eligible: int,
    n_queued: int,
    reason_counts: dict,
    bundle_path: Optional[str],
    report_path: Optional[str],
) -> list[str]:
    """Build human-readable review notes for the queue artifact."""
    notes: list[str] = []

    notes.append(
        f"Review queue generated for {total_eligible} eligible pipeline records. "
        f"{n_queued} record(s) added to the review queue."
    )

    n_quarantined = reason_counts.get(REVIEW_REASON_QUARANTINED, 0)
    n_blocked = reason_counts.get(REVIEW_REASON_CONTRACT_BLOCKED, 0)
    n_failed = reason_counts.get(REVIEW_REASON_EXTRACTION_FAILED, 0)

    if n_quarantined > 0:
        notes.append(
            f"{n_quarantined} record(s) quarantined during pipeline classification. "
            "Review the Gold artifact for classification context and routing reason."
        )
    if n_blocked > 0:
        notes.append(
            f"{n_blocked} record(s) blocked by B-1 Bedrock contract validation. "
            "Review the Gold artifact and contract_validation_errors for field-level detail."
        )
    if n_failed > 0:
        notes.append(
            f"{n_failed} record(s) produced an 'unknown' document type during classification. "
            "Review the Silver artifact and consider re-extraction with a corrected class hint."
        )
    if n_queued == 0:
        notes.append(
            "No records required human review in this batch. "
            "All eligible records were exported successfully."
        )

    if bundle_path:
        notes.append(
            f"B-5 batch manifest referenced at: {bundle_path}. "
            "The manifest contains full per-record artifact paths for this batch."
        )
    if report_path:
        notes.append(
            f"B-4 handoff batch report referenced at: {report_path}. "
            "The report contains outcome counts and reason code distributions."
        )

    notes.append(
        "Review decisions should be recorded using the ReviewDecision schema "
        "(src/schemas/review_decision.py). For records requiring reprocessing, "
        "a ReprocessingRequest artifact should be produced alongside the decision."
    )
    notes.append(
        "This is an upstream review queue artifact. Human review workflows, "
        "case management tooling, and downstream decisioning are outside this repo's boundary."
    )

    return notes


# ---------------------------------------------------------------------------
# Artifact path computation
# ---------------------------------------------------------------------------


def compute_review_queue_path(queue_dir: Path, pipeline_run_id: str) -> Path:
    """
    Compute the deterministic review queue artifact JSON path.

    Path: <queue_dir>/review_queue_<safe_run_id>.json

    The path is deterministic: same (queue_dir, pipeline_run_id) always maps
    to the same path. No randomness or extra timestamps in the filename.
    """
    safe_run_id = (
        pipeline_run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    )
    return queue_dir / f"review_queue_{safe_run_id}.json"


# ---------------------------------------------------------------------------
# Format helper
# ---------------------------------------------------------------------------


def format_review_queue_text(queue: ReviewQueueArtifact) -> str:
    """
    Format a ReviewQueueArtifact as a human-readable text summary.

    Suitable for terminal output and the .txt review queue artifact.
    """
    lines = [
        "=== Human Review Queue (E-0) ===",
        f"Review Queue ID    : {queue.review_queue_id}",
        f"Pipeline Run ID    : {queue.pipeline_run_id}",
        f"Generated At       : {queue.generated_at}",
        f"Schema Version     : {queue.schema_version}",
        "",
        "--- Queue Counts ---",
        f"Total Records Reviewed : {queue.total_records_reviewed}",
        f"Total Queue Entries    : {queue.total_entries}",
        "",
        "--- Entries by Reason ---",
    ]
    for cat in ALL_REVIEW_REASON_CATEGORIES:
        count = queue.entries_by_reason.get(cat, 0)
        lines.append(f"  {cat}: {count}")

    if queue.queue_entries:
        lines += ["", "--- Queue Entries ---"]
        for i, entry in enumerate(queue.queue_entries, 1):
            e = entry if isinstance(entry, ReviewQueueEntry) else None
            if e is None:
                lines.append(f"  [{i}] {entry}")
                continue
            lines += [
                f"  [{i}] document_id       : {e.document_id}",
                f"       gold_record_id    : {e.gold_record_id or 'n/a'}",
                f"       reason_category   : {e.review_reason_category}",
                f"       document_type     : {e.document_type_label}",
                f"       routing_label     : {e.routing_label}",
            ]
            if e.gold_artifact_path:
                lines.append(f"       gold_artifact    : {e.gold_artifact_path}")
            if e.contract_validation_errors:
                lines.append(
                    f"       contract_errors  : "
                    + "; ".join(e.contract_validation_errors[:2])
                )
    else:
        lines += ["", "  (no entries — all records exported successfully)"]

    if queue.review_notes:
        lines += ["", "--- Review Notes ---"]
        for note in queue.review_notes:
            lines.append(f"  - {note}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------


def write_review_queue(
    queue: ReviewQueueArtifact,
    queue_dir: Path,
) -> tuple[Path, Path]:
    """
    Write a ReviewQueueArtifact to JSON and text artifacts.

    Artifact names use the pipeline_run_id (sanitized for filesystem):
        <queue_dir>/review_queue_<pipeline_run_id>.json
        <queue_dir>/review_queue_<pipeline_run_id>.txt

    Args:
        queue: The ReviewQueueArtifact to write.
        queue_dir: Directory to write artifacts into. Created if absent.

    Returns:
        (json_path, text_path): Paths of the written artifacts.
    """
    queue_dir.mkdir(parents=True, exist_ok=True)
    json_path = compute_review_queue_path(queue_dir, queue.pipeline_run_id)
    safe_run_id = (
        queue.pipeline_run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    )
    text_path = queue_dir / f"review_queue_{safe_run_id}.txt"

    json_path.write_text(queue.to_json_str(), encoding="utf-8")
    text_path.write_text(format_review_queue_text(queue), encoding="utf-8")

    return json_path, text_path


# ---------------------------------------------------------------------------
# Artifact loader
# ---------------------------------------------------------------------------


def load_review_queue(json_path: Path) -> dict:
    """Load a ReviewQueueArtifact JSON file and return the raw dict."""
    return json.loads(json_path.read_text(encoding="utf-8"))
