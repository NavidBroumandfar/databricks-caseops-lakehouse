"""
src/schemas/review_queue.py — E-0 Human Review Queue Schema.

Defines the structured artifact for the upstream human review queue.
The review queue collects records that require human attention before
they can proceed to export or be definitively classified as quarantined.

This schema is upstream-only. It does not implement a review UI, a case
management tool, or any downstream analyst workflow. It defines what the
upstream pipeline knows about a record that needs review, so that review
decisions can be made and recorded against a governed artifact.

Review reason categories (bounded vocabulary):
    quarantined          — record was routed to quarantine by pipeline
                           routing logic (unknown type, low confidence,
                           low coverage, or invalid Silver validation)
    contract_blocked     — record was export-ready but rejected by the B-1
                           contract validator; structural fix required before export
    extraction_failed    — record could not be classified at all due to a
                           pipeline error during classification

Per-entry fields capture:
    - identifiers (document_id, gold_record_id)
    - classification context (document_type_label, routing_label)
    - review reason vocabulary
    - artifact paths for human navigation

The queue artifact is additive and optional — the existing automated pipeline
path (Bronze → Silver → Gold → export) is unchanged.

Authoritative scope: PROJECT_SPEC.md § Phase E-0
Architecture context: ARCHITECTURE.md § Future Evolution (Human Review Loop)
Phase: E-0
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# Schema version for the review queue artifact structure.
# Increment when the queue structure changes in a breaking way.
REVIEW_QUEUE_SCHEMA_VERSION = "v0.1.0"


# ---------------------------------------------------------------------------
# Review reason categories — bounded vocabulary
# ---------------------------------------------------------------------------

REVIEW_REASON_QUARANTINED = "quarantined"
"""Record was routed to quarantine by pipeline routing logic.
Causes: document_type='unknown', confidence below threshold,
Silver validation_status='invalid', or Silver field coverage too low."""

REVIEW_REASON_CONTRACT_BLOCKED = "contract_blocked"
"""Record was export-ready but the B-1 contract validator rejected the payload.
The record could not be written as a valid Bedrock handoff artifact.
A structural fix or re-extraction is required before export."""

REVIEW_REASON_EXTRACTION_FAILED = "extraction_failed"
"""Record could not be classified due to a pipeline error during classification.
Typically an internal pipeline fault — not a document content failure."""

ALL_REVIEW_REASON_CATEGORIES: tuple[str, ...] = (
    REVIEW_REASON_QUARANTINED,
    REVIEW_REASON_CONTRACT_BLOCKED,
    REVIEW_REASON_EXTRACTION_FAILED,
)


# ---------------------------------------------------------------------------
# Per-entry model
# ---------------------------------------------------------------------------


@dataclass
class ReviewQueueEntry:
    """
    A single record requiring human review.

    Captures the identifiers, classification context, review reason, and
    artifact references needed for a reviewer to locate and assess the record.

    Fields
    ------
    queue_entry_id
        Stable UUID identifying this queue entry.
    document_id
        Cross-layer document identifier from Bronze.
    gold_record_id
        UUID of the Gold classification record. Optional — may be absent
        for records that failed classification entirely.
    pipeline_run_id
        Pipeline run that produced this record.
    document_type_label
        Document type label assigned by classification.
    routing_label
        Routing label assigned by the pipeline (typically 'quarantine'
        for quarantined entries).
    review_reason
        Human-readable description of why this record needs review.
    review_reason_category
        Bounded machine-readable category from ALL_REVIEW_REASON_CATEGORIES.
    gold_artifact_path
        Filesystem path to the Gold JSON artifact. The full Gold record
        (classification result, export payload, provenance) is here.
    export_artifact_path
        Path to the export payload artifact, if one was written.
        None for quarantined and contract-blocked records.
    bundle_path
        Path to the B-5 batch manifest that includes this record.
        Optional — present when the batch bundle was generated.
    report_path
        Path to the B-4 handoff batch report for this run.
        Optional — present when the batch report was generated.
    contract_validation_errors
        Field-level contract validation error messages.
        Non-empty only for contract_blocked entries.
    """

    queue_entry_id: str
    document_id: str
    gold_record_id: Optional[str]
    pipeline_run_id: str
    document_type_label: str
    routing_label: str
    review_reason: str
    review_reason_category: str
    gold_artifact_path: Optional[str]
    export_artifact_path: Optional[str]
    bundle_path: Optional[str]
    report_path: Optional[str]
    contract_validation_errors: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "queue_entry_id": self.queue_entry_id,
            "document_id": self.document_id,
            "gold_record_id": self.gold_record_id,
            "pipeline_run_id": self.pipeline_run_id,
            "document_type_label": self.document_type_label,
            "routing_label": self.routing_label,
            "review_reason": self.review_reason,
            "review_reason_category": self.review_reason_category,
            "gold_artifact_path": self.gold_artifact_path,
            "export_artifact_path": self.export_artifact_path,
            "bundle_path": self.bundle_path,
            "report_path": self.report_path,
            "contract_validation_errors": self.contract_validation_errors,
        }


# ---------------------------------------------------------------------------
# Review queue artifact model
# ---------------------------------------------------------------------------


@dataclass
class ReviewQueueArtifact:
    """
    Structured human review queue for a single pipeline batch.

    Collects all records from a pipeline run that require human attention.
    This is the upstream-side artifact — it captures which records need
    review and why, without prescribing what a reviewer does with them.

    The downstream review tool (Bedrock CaseOps or an operator workflow)
    is responsible for acting on the queue entries. This repo produces
    the artifact; it does not own the review workflow.

    Fields
    ------
    review_queue_id
        Stable UUID identifying this review queue artifact.
    pipeline_run_id
        Pipeline run that produced the records in this queue.
    generated_at
        ISO 8601 UTC timestamp when this queue artifact was assembled.
    schema_version
        Artifact schema version. Currently REVIEW_QUEUE_SCHEMA_VERSION.
    total_records_reviewed
        Total eligible records that were evaluated for queue inclusion.
    total_entries
        Number of records added to the review queue.
    entries_by_reason
        Count of entries grouped by review_reason_category.
    queue_entries
        The list of ReviewQueueEntry items in this queue.
    review_notes
        Human-readable notes about queue composition and context.
    """

    review_queue_id: str
    pipeline_run_id: str
    generated_at: str
    schema_version: str
    total_records_reviewed: int
    total_entries: int
    entries_by_reason: dict
    queue_entries: list = field(default_factory=list)
    review_notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "review_queue_id": self.review_queue_id,
            "pipeline_run_id": self.pipeline_run_id,
            "generated_at": self.generated_at,
            "schema_version": self.schema_version,
            "total_records_reviewed": self.total_records_reviewed,
            "total_entries": self.total_entries,
            "entries_by_reason": self.entries_by_reason,
            "queue_entries": [
                e.to_dict() if isinstance(e, ReviewQueueEntry) else e
                for e in self.queue_entries
            ],
            "review_notes": self.review_notes,
        }

    def to_json_str(self, indent: int = 2) -> str:
        """Return a pretty-printed JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_queue_entry_id() -> str:
    """Generate a new queue entry UUID."""
    return str(uuid.uuid4())


def make_review_queue_id() -> str:
    """Generate a new review queue artifact UUID."""
    return str(uuid.uuid4())


def make_review_reason(
    reason_category: str,
    document_type_label: str,
    routing_label: str,
    contract_validation_errors: Optional[list] = None,
) -> str:
    """
    Generate a human-readable review reason string for a given reason category.

    Args:
        reason_category: One of the ALL_REVIEW_REASON_CATEGORIES constants.
        document_type_label: Classification label from Gold.
        routing_label: Routing label from Gold.
        contract_validation_errors: Field-level contract errors (for contract_blocked).

    Returns:
        A concise human-readable reason string for the queue entry.
    """
    if reason_category == REVIEW_REASON_QUARANTINED:
        return (
            f"Record quarantined during pipeline classification. "
            f"document_type='{document_type_label}', routing='{routing_label}'. "
            "Causes include: unknown document type, low classification confidence, "
            "low Silver field coverage, or invalid Silver extraction."
        )
    if reason_category == REVIEW_REASON_CONTRACT_BLOCKED:
        errors_summary = ""
        if contract_validation_errors:
            top = contract_validation_errors[:3]
            errors_summary = " Validation errors: " + "; ".join(top)
            if len(contract_validation_errors) > 3:
                errors_summary += f" (and {len(contract_validation_errors) - 3} more)"
        return (
            f"Record failed Bedrock contract validation (B-1). "
            f"Export payload was not written.{errors_summary} "
            "Review the Gold artifact and correct the extraction or contract fields."
        )
    if reason_category == REVIEW_REASON_EXTRACTION_FAILED:
        return (
            "Record could not be classified due to a pipeline classification error. "
            "The document was processed but produced an 'unknown' type result. "
            "Review the Silver artifact and consider re-extraction."
        )
    return f"Review required. reason_category='{reason_category}'."
