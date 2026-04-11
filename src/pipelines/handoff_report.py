"""
src/pipelines/handoff_report.py — B-4 Export Outcome Observability and Handoff Reporting.

Makes Gold → Bedrock handoff outcomes operationally visible, structured, and
reviewable at batch level. Provides the explicit outcome vocabulary, reason codes,
and batch-level summary infrastructure for the export path.

This module owns:
  - Outcome category constants: the four explicit handoff path outcomes
  - Reason code constants: structured reason for each non-export outcome
  - HandoffBatchReport: batch-level structured summary of export outcomes
  - derive_outcome(): canonical mapping from ExportResult state → B-4 vocabulary
  - build_handoff_batch_report(): aggregate per-record summaries into a batch report
  - format_handoff_report_text(): human-readable summary string
  - write_handoff_report(): write JSON + text report artifacts

This module does NOT own:
  - Classification logic (classify_gold.py)
  - Export artifact writing or contract validation (export_handoff.py)
  - The B-1 contract validator (bedrock_contract.py)

Strategic boundary:
  classify_gold.py   → runs pipeline, collects per-record summaries
  export_handoff.py  → executes export, returns ExportResult with outcome fields
  handoff_report.py  → derives outcome categories, aggregates batch report, writes artifacts

Design constraint (B-4 vs A-4):
  A-4 covers generic pipeline evaluation: parse quality, extraction quality,
  classification quality, traceability completeness.
  B-4 is narrower: what happened in the handoff/export path specifically —
  what was written, what was blocked, why, and what a downstream readiness
  review would need to inspect.

No live Bedrock/AWS integration is implied or implemented here.

Phase: B-4
Architecture context: ARCHITECTURE.md § Bedrock Handoff Design
Authoritative contract: docs/bedrock-handoff-contract.md
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Outcome categories
# ---------------------------------------------------------------------------

# Explicit, stable outcome vocabulary for the Gold → Bedrock handoff path.
# These correspond 1:1 to the four distinct execution paths in execute_export().
#
# Use these constants — not raw strings — when writing or comparing outcomes.

OUTCOME_EXPORTED = "exported"
"""Record successfully written as a Bedrock handoff artifact."""

OUTCOME_QUARANTINED = "quarantined"
"""Record routed to quarantine (governance path). No export file produced."""

OUTCOME_CONTRACT_BLOCKED = "contract_blocked"
"""Record was export-ready but B-1 contract validation rejected the payload.
Export artifact NOT written; Gold record demoted to export_ready=False."""

OUTCOME_SKIPPED_NOT_EXPORT_READY = "skipped_not_export_ready"
"""Record was not export-ready and not quarantined. No export attempted."""

ALL_OUTCOME_CATEGORIES: tuple[str, ...] = (
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
)


# ---------------------------------------------------------------------------
# Reason codes
# ---------------------------------------------------------------------------

# Structured reason codes for each outcome path.
# Always present in per-record summaries and the batch report.

REASON_NONE = "none"
"""Successful export — no blocking reason."""

REASON_ROUTING_QUARANTINE = "routing_quarantine"
"""Document was routed to quarantine by classification/routing logic.
Typical causes: document_type='unknown', confidence below threshold,
Silver validation_status='invalid', Silver field coverage too low."""

REASON_CONTRACT_VALIDATION_FAILED = "contract_validation_failed"
"""B-1 contract validator (validate_export_payload) rejected the payload.
The contract_validation_errors field in the summary carries field-level detail."""

REASON_EXPORT_NOT_ATTEMPTED = "export_not_attempted"
"""Record was not export-ready and not quarantined.
No export was attempted. Typically an internal pipeline state (edge case)."""

ALL_REASON_CODES: tuple[str, ...] = (
    REASON_NONE,
    REASON_ROUTING_QUARANTINE,
    REASON_CONTRACT_VALIDATION_FAILED,
    REASON_EXPORT_NOT_ATTEMPTED,
)


# ---------------------------------------------------------------------------
# Outcome derivation
# ---------------------------------------------------------------------------


def derive_outcome(
    export_ready: bool,
    routing_label: str,
    contract_validation_errors: list[str],
) -> tuple[str, str]:
    """
    Derive (outcome_category, outcome_reason) from the export path state.

    This is the canonical mapping from ExportResult + routing_label →
    B-4 outcome vocabulary. Priority order is important:

      1. export_ready=True, no errors  → exported         / none
      2. contract_validation_errors    → contract_blocked  / contract_validation_failed
      3. routing_label='quarantine'    → quarantined       / routing_quarantine
      4. else                          → skipped_not_export_ready / export_not_attempted

    Args:
        export_ready: ExportResult.export_ready (post-export value).
        routing_label: GoldRecord.routing_label (unchanged by export).
        contract_validation_errors: ExportResult.contract_validation_errors.

    Returns:
        (outcome_category, outcome_reason) — both from the constants above.
    """
    if export_ready:
        return OUTCOME_EXPORTED, REASON_NONE
    if contract_validation_errors:
        return OUTCOME_CONTRACT_BLOCKED, REASON_CONTRACT_VALIDATION_FAILED
    if routing_label == "quarantine":
        return OUTCOME_QUARANTINED, REASON_ROUTING_QUARANTINE
    return OUTCOME_SKIPPED_NOT_EXPORT_READY, REASON_EXPORT_NOT_ATTEMPTED


# ---------------------------------------------------------------------------
# Batch report model
# ---------------------------------------------------------------------------


@dataclass
class HandoffBatchReport:
    """
    Batch-level structured summary of Gold → Bedrock export outcomes.

    Produced after every run_classify_gold() call via build_handoff_batch_report().
    Represents what happened across the full batch: how many records were exported,
    quarantined, contract-blocked, or skipped, and why.

    Fields
    ------
    pipeline_run_id
        Run ID shared across all records in this batch.
    batch_processed_at
        ISO 8601 UTC timestamp when this report was assembled.
    total_records_processed
        All Silver records that entered the pipeline loop (eligible + ineligible).
    total_ineligible_skipped
        Records skipped due to validation_status='invalid' (not classified).
    total_eligible
        Records that proceeded to classification and export evaluation.
        Equals total_records_processed - total_ineligible_skipped.
    total_export_attempts
        Records where export was attempted: exported + contract_blocked.
        Records that were quarantined or skipped are not counted as attempts.
    total_exported
        Records successfully written as Bedrock handoff artifacts.
    total_quarantined
        Records routed to quarantine (governance path, no export file).
    total_contract_blocked
        Records that were export-ready but failed B-1 contract validation.
    total_skipped_not_export_ready
        Non-export-ready, non-quarantine records. No export attempted.
    outcome_distribution
        Counts by outcome category: {category: count}.
    reason_code_distribution
        Counts by reason code: {reason_code: count}.
    contract_blocked_document_ids
        Document IDs of contract-blocked records (for readiness review).
    quarantined_document_ids
        Document IDs of quarantined records (for governance review).
    """

    pipeline_run_id: str
    batch_processed_at: str
    total_records_processed: int
    total_ineligible_skipped: int
    total_eligible: int
    total_export_attempts: int
    total_exported: int
    total_quarantined: int
    total_contract_blocked: int
    total_skipped_not_export_ready: int
    outcome_distribution: dict = field(default_factory=dict)
    reason_code_distribution: dict = field(default_factory=dict)
    contract_blocked_document_ids: list = field(default_factory=list)
    quarantined_document_ids: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "batch_processed_at": self.batch_processed_at,
            "total_records_processed": self.total_records_processed,
            "total_ineligible_skipped": self.total_ineligible_skipped,
            "total_eligible": self.total_eligible,
            "total_export_attempts": self.total_export_attempts,
            "total_exported": self.total_exported,
            "total_quarantined": self.total_quarantined,
            "total_contract_blocked": self.total_contract_blocked,
            "total_skipped_not_export_ready": self.total_skipped_not_export_ready,
            "outcome_distribution": self.outcome_distribution,
            "reason_code_distribution": self.reason_code_distribution,
            "contract_blocked_document_ids": self.contract_blocked_document_ids,
            "quarantined_document_ids": self.quarantined_document_ids,
        }


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


def build_handoff_batch_report(
    summaries: list[dict],
    pipeline_run_id: str,
    total_records_processed: Optional[int] = None,
    total_ineligible_skipped: int = 0,
    batch_processed_at: Optional[str] = None,
) -> HandoffBatchReport:
    """
    Aggregate per-record summaries into a batch-level HandoffBatchReport.

    Each summary dict must contain:
        document_id, routing_label, export_ready, contract_validation_errors,
        outcome_category, outcome_reason.

    These fields are added to summaries by run_classify_gold() via derive_outcome().

    Args:
        summaries: Per-record summary dicts from run_classify_gold().
        pipeline_run_id: Run ID for this batch.
        total_records_processed: All Silver records loaded (eligible + ineligible).
            Defaults to len(summaries) + total_ineligible_skipped.
        total_ineligible_skipped: Records that failed eligibility check (skipped
            before classification). Tracked by the pipeline loop counter.
        batch_processed_at: ISO timestamp for report assembly; defaults to UTC now.

    Returns:
        HandoffBatchReport with all counts, distributions, and affected IDs populated.
    """
    if batch_processed_at is None:
        batch_processed_at = datetime.now(tz=timezone.utc).isoformat()

    total_eligible = len(summaries)
    if total_records_processed is None:
        total_records_processed = total_eligible + total_ineligible_skipped

    outcome_counts: dict[str, int] = {cat: 0 for cat in ALL_OUTCOME_CATEGORIES}
    reason_counts: dict[str, int] = {code: 0 for code in ALL_REASON_CODES}
    contract_blocked_ids: list[str] = []
    quarantined_ids: list[str] = []
    total_export_attempts = 0

    for s in summaries:
        outcome_cat = s.get("outcome_category", OUTCOME_SKIPPED_NOT_EXPORT_READY)
        reason_code = s.get("outcome_reason", REASON_EXPORT_NOT_ATTEMPTED)
        doc_id = s.get("document_id", "")

        if outcome_cat in outcome_counts:
            outcome_counts[outcome_cat] += 1
        else:
            outcome_counts[outcome_cat] = outcome_counts.get(outcome_cat, 0) + 1

        if reason_code in reason_counts:
            reason_counts[reason_code] += 1
        else:
            reason_counts[reason_code] = reason_counts.get(reason_code, 0) + 1

        # Export attempts: records where export was actually gated by the contract
        # (exported successfully OR blocked by contract validation)
        if outcome_cat in (OUTCOME_EXPORTED, OUTCOME_CONTRACT_BLOCKED):
            total_export_attempts += 1

        if outcome_cat == OUTCOME_CONTRACT_BLOCKED:
            contract_blocked_ids.append(doc_id)
        elif outcome_cat == OUTCOME_QUARANTINED:
            quarantined_ids.append(doc_id)

    return HandoffBatchReport(
        pipeline_run_id=pipeline_run_id,
        batch_processed_at=batch_processed_at,
        total_records_processed=total_records_processed,
        total_ineligible_skipped=total_ineligible_skipped,
        total_eligible=total_eligible,
        total_export_attempts=total_export_attempts,
        total_exported=outcome_counts[OUTCOME_EXPORTED],
        total_quarantined=outcome_counts[OUTCOME_QUARANTINED],
        total_contract_blocked=outcome_counts[OUTCOME_CONTRACT_BLOCKED],
        total_skipped_not_export_ready=outcome_counts[OUTCOME_SKIPPED_NOT_EXPORT_READY],
        outcome_distribution=dict(outcome_counts),
        reason_code_distribution=dict(reason_counts),
        contract_blocked_document_ids=contract_blocked_ids,
        quarantined_document_ids=quarantined_ids,
    )


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------


def format_handoff_report_text(report: HandoffBatchReport) -> str:
    """
    Format a HandoffBatchReport as a human-readable text summary.

    Suitable for terminal output and the .txt report artifact.
    """
    lines = [
        "=== Handoff Batch Report (B-4) ===",
        f"Pipeline Run ID    : {report.pipeline_run_id}",
        f"Batch Processed At : {report.batch_processed_at}",
        "",
        "--- Record Counts ---",
        f"Total Processed         : {report.total_records_processed}",
        f"Ineligible Skipped      : {report.total_ineligible_skipped}",
        f"Eligible for Export     : {report.total_eligible}",
        f"Export Attempts         : {report.total_export_attempts}",
        "",
        "--- Outcome Counts ---",
        f"Exported                : {report.total_exported}",
        f"Quarantined             : {report.total_quarantined}",
        f"Contract Blocked        : {report.total_contract_blocked}",
        f"Skipped (not ready)     : {report.total_skipped_not_export_ready}",
        "",
        "--- Outcome Distribution ---",
    ]
    for cat in ALL_OUTCOME_CATEGORIES:
        count = report.outcome_distribution.get(cat, 0)
        lines.append(f"  {cat}: {count}")
    lines += [
        "",
        "--- Reason Code Distribution ---",
    ]
    for code in ALL_REASON_CODES:
        count = report.reason_code_distribution.get(code, 0)
        lines.append(f"  {code}: {count}")

    if report.contract_blocked_document_ids:
        lines += [
            "",
            "--- Contract Blocked Document IDs ---",
        ]
        for doc_id in report.contract_blocked_document_ids:
            lines.append(f"  {doc_id}")

    if report.quarantined_document_ids:
        lines += [
            "",
            "--- Quarantined Document IDs ---",
        ]
        for doc_id in report.quarantined_document_ids:
            lines.append(f"  {doc_id}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------


def write_handoff_report(report: HandoffBatchReport, output_dir: Path) -> tuple[Path, Path]:
    """
    Write a HandoffBatchReport to JSON and text artifacts.

    Artifact names use the pipeline_run_id (sanitized for filesystem):
        <output_dir>/handoff_report_<pipeline_run_id>.json
        <output_dir>/handoff_report_<pipeline_run_id>.txt

    Args:
        report: The HandoffBatchReport to write.
        output_dir: Directory to write artifacts into. Created if absent.

    Returns:
        (json_path, text_path): Paths of the written artifacts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    # Sanitize run ID for filesystem use (replace path-unsafe characters)
    safe_run_id = report.pipeline_run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    json_path = output_dir / f"handoff_report_{safe_run_id}.json"
    text_path = output_dir / f"handoff_report_{safe_run_id}.txt"

    json_path.write_text(
        json.dumps(report.to_dict(), indent=2), encoding="utf-8"
    )
    text_path.write_text(format_handoff_report_text(report), encoding="utf-8")

    return json_path, text_path
