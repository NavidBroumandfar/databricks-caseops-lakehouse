"""
src/schemas/review_decision.py — E-0 Review Decision and Reprocessing Request Schemas.

Defines the structured artifacts for recording a human review decision
and, when applicable, a structured reprocessing request.

This module is upstream-only. It does not implement the review workflow,
the UI, or any downstream case management tool. It defines what a completed
review decision looks like as a governed artifact, and what a reprocessing
request looks like as a handoff artifact.

Review decisions (bounded vocabulary):
    approve_for_export      — reviewer approves the record for export to Bedrock;
                              the record should be re-submitted through the export path
    confirm_quarantine      — reviewer confirms the record should remain quarantined;
                              no reprocessing or export is warranted
    request_reprocessing    — reviewer requests a re-extraction or re-classification pass;
                              a ReprocessingRequest artifact is produced alongside the decision
    reject_unresolved       — record cannot be resolved at this time; neither approved
                              nor confirmed for quarantine; treated as unresolved

Reprocessing requests define:
    - which record is being reprocessed
    - why (linked back to queue entry and decision)
    - what should change or be retried (class hint, re-extraction notes)
    - how the request links to the original pipeline artifacts

The repo does not implement an automated re-run workflow in E-0. The
reprocessing request is the upstream-side artifact that communicates intent
to a downstream or operator-side reprocessing trigger. A well-defined,
testable artifact is the E-0 deliverable — not an orchestration engine.

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


# ---------------------------------------------------------------------------
# Review decision vocabulary — bounded set
# ---------------------------------------------------------------------------

DECISION_APPROVE_FOR_EXPORT = "approve_for_export"
"""Reviewer approves the record for export. The record should be re-submitted
through the Gold export path. This decision does not automatically trigger
re-export — it records the reviewer's intent for the reprocessing handoff."""

DECISION_CONFIRM_QUARANTINE = "confirm_quarantine"
"""Reviewer confirms that quarantine is correct. The record should not be
re-exported or re-extracted. No further action by this pipeline is needed."""

DECISION_REQUEST_REPROCESSING = "request_reprocessing"
"""Reviewer requests a re-extraction or re-classification pass.
A ReprocessingRequest artifact is produced alongside this decision."""

DECISION_REJECT_UNRESOLVED = "reject_unresolved"
"""Record cannot be resolved at this time. Not approved for export, not confirmed
as quarantine. Treated as unresolved — may re-enter the review queue later."""

ALL_REVIEW_DECISIONS: tuple[str, ...] = (
    DECISION_APPROVE_FOR_EXPORT,
    DECISION_CONFIRM_QUARANTINE,
    DECISION_REQUEST_REPROCESSING,
    DECISION_REJECT_UNRESOLVED,
)

# Decisions that imply downstream action (export or reprocessing)
ACTIONABLE_DECISIONS: tuple[str, ...] = (
    DECISION_APPROVE_FOR_EXPORT,
    DECISION_REQUEST_REPROCESSING,
)

# Decisions that resolve the record without further pipeline action
TERMINAL_DECISIONS: tuple[str, ...] = (
    DECISION_CONFIRM_QUARANTINE,
    DECISION_REJECT_UNRESOLVED,
)

# Schema version for review decision and reprocessing request artifacts.
REVIEW_DECISION_SCHEMA_VERSION = "v0.1.0"
REPROCESSING_REQUEST_SCHEMA_VERSION = "v0.1.0"


# ---------------------------------------------------------------------------
# Reprocessing request model
# ---------------------------------------------------------------------------


@dataclass
class ReprocessingRequest:
    """
    Structured upstream reprocessing request artifact.

    Defines what record is being reprocessed, why, what should change,
    and how the request links back to the original pipeline artifacts.

    This is an upstream-only artifact. It communicates reprocessing intent
    to a downstream or operator-side trigger. E-0 does not implement an
    automated re-run workflow — the request artifact is the deliverable.

    Fields
    ------
    reprocessing_request_id
        Stable UUID identifying this reprocessing request.
    document_id
        Cross-layer document identifier from Bronze.
    gold_record_id
        UUID of the Gold record being reprocessed. Optional if Gold record
        was never written for this document.
    original_pipeline_run_id
        Pipeline run ID that produced the record being reprocessed.
    requested_at
        ISO 8601 UTC timestamp when this request was generated.
    schema_version
        Artifact schema version (REPROCESSING_REQUEST_SCHEMA_VERSION).
    reprocessing_reason
        Human-readable explanation of why reprocessing is needed.
    suggested_document_class_hint
        If the reviewer believes the document was mis-classified, they can
        suggest a different document_class_hint for the re-run. Optional.
    suggested_extraction_notes
        Free-text notes for the extraction pass (e.g., "focus on violation
        section", "re-parse with OCR fallback"). Optional.
    linked_queue_entry_id
        UUID of the ReviewQueueEntry that prompted this request.
    linked_decision_id
        UUID of the ReviewDecision that produced this request.
    original_gold_artifact_path
        Path to the original Gold JSON artifact for traceability.
    original_bundle_path
        Path to the B-5 batch manifest that included the original record.
    """

    reprocessing_request_id: str
    document_id: str
    gold_record_id: Optional[str]
    original_pipeline_run_id: str
    requested_at: str
    schema_version: str
    reprocessing_reason: str
    suggested_document_class_hint: Optional[str]
    suggested_extraction_notes: Optional[str]
    linked_queue_entry_id: str
    linked_decision_id: str
    original_gold_artifact_path: Optional[str]
    original_bundle_path: Optional[str]

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "reprocessing_request_id": self.reprocessing_request_id,
            "document_id": self.document_id,
            "gold_record_id": self.gold_record_id,
            "original_pipeline_run_id": self.original_pipeline_run_id,
            "requested_at": self.requested_at,
            "schema_version": self.schema_version,
            "reprocessing_reason": self.reprocessing_reason,
            "suggested_document_class_hint": self.suggested_document_class_hint,
            "suggested_extraction_notes": self.suggested_extraction_notes,
            "linked_queue_entry_id": self.linked_queue_entry_id,
            "linked_decision_id": self.linked_decision_id,
            "original_gold_artifact_path": self.original_gold_artifact_path,
            "original_bundle_path": self.original_bundle_path,
        }

    def to_json_str(self, indent: int = 2) -> str:
        """Return a pretty-printed JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Review decision model
# ---------------------------------------------------------------------------


@dataclass
class ReviewDecision:
    """
    Structured record of a human review decision for a queued document.

    Captures the outcome of a human reviewing a ReviewQueueEntry. The
    decision vocabulary is bounded — only the four defined outcomes are valid.

    When decision == DECISION_REQUEST_REPROCESSING, a ReprocessingRequest
    artifact should be produced alongside this decision. The decision carries
    the reprocessing_request_id as a forward reference.

    This artifact is upstream-only. It records what a reviewer decided.
    Downstream workflow tooling (Bedrock CaseOps) or an operator trigger
    is responsible for acting on the decision.

    Fields
    ------
    decision_id
        Stable UUID identifying this review decision record.
    queue_entry_id
        UUID of the ReviewQueueEntry this decision resolves.
    document_id
        Cross-layer document identifier from Bronze.
    gold_record_id
        UUID of the Gold record this decision concerns.
    pipeline_run_id
        Pipeline run that produced the record being reviewed.
    decided_at
        ISO 8601 UTC timestamp when this decision was recorded.
    schema_version
        Artifact schema version (REVIEW_DECISION_SCHEMA_VERSION).
    decision
        One of the ALL_REVIEW_DECISIONS constants.
    decision_rationale
        Human-readable explanation of why this decision was made.
    reprocessing_request_id
        UUID of the associated ReprocessingRequest artifact.
        Only populated when decision == DECISION_REQUEST_REPROCESSING.
    """

    decision_id: str
    queue_entry_id: str
    document_id: str
    gold_record_id: Optional[str]
    pipeline_run_id: str
    decided_at: str
    schema_version: str
    decision: str
    decision_rationale: str
    reprocessing_request_id: Optional[str] = None

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "decision_id": self.decision_id,
            "queue_entry_id": self.queue_entry_id,
            "document_id": self.document_id,
            "gold_record_id": self.gold_record_id,
            "pipeline_run_id": self.pipeline_run_id,
            "decided_at": self.decided_at,
            "schema_version": self.schema_version,
            "decision": self.decision,
            "decision_rationale": self.decision_rationale,
            "reprocessing_request_id": self.reprocessing_request_id,
        }

    def to_json_str(self, indent: int = 2) -> str:
        """Return a pretty-printed JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @property
    def is_actionable(self) -> bool:
        """True if this decision implies downstream action (export or reprocessing)."""
        return self.decision in ACTIONABLE_DECISIONS

    @property
    def is_terminal(self) -> bool:
        """True if this decision resolves the record without further pipeline action."""
        return self.decision in TERMINAL_DECISIONS


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_review_decision(decision: ReviewDecision) -> list[str]:
    """
    Validate a ReviewDecision artifact for structural correctness.

    Returns a list of validation error strings. Empty list = valid.

    Checks:
    - decision field is a known decision vocabulary value
    - decision_rationale is non-empty
    - decision_id is non-empty
    - queue_entry_id is non-empty
    - document_id is non-empty
    - pipeline_run_id is non-empty
    - decided_at is non-empty
    - reprocessing_request_id is present when decision == request_reprocessing
    """
    errors: list[str] = []

    if not decision.decision_id:
        errors.append("decision_id is required.")
    if not decision.queue_entry_id:
        errors.append("queue_entry_id is required.")
    if not decision.document_id:
        errors.append("document_id is required.")
    if not decision.pipeline_run_id:
        errors.append("pipeline_run_id is required.")
    if not decision.decided_at:
        errors.append("decided_at is required.")
    if not decision.decision_rationale:
        errors.append("decision_rationale is required and must not be empty.")
    if decision.decision not in ALL_REVIEW_DECISIONS:
        errors.append(
            f"decision '{decision.decision}' is not a valid review decision. "
            f"Must be one of: {ALL_REVIEW_DECISIONS}."
        )
    if (
        decision.decision == DECISION_REQUEST_REPROCESSING
        and not decision.reprocessing_request_id
    ):
        errors.append(
            "reprocessing_request_id is required when decision == 'request_reprocessing'."
        )

    return errors


def validate_reprocessing_request(request: ReprocessingRequest) -> list[str]:
    """
    Validate a ReprocessingRequest artifact for structural correctness.

    Returns a list of validation error strings. Empty list = valid.

    Checks:
    - reprocessing_request_id is non-empty
    - document_id is non-empty
    - original_pipeline_run_id is non-empty
    - reprocessing_reason is non-empty
    - linked_queue_entry_id is non-empty
    - linked_decision_id is non-empty
    - requested_at is non-empty
    """
    errors: list[str] = []

    if not request.reprocessing_request_id:
        errors.append("reprocessing_request_id is required.")
    if not request.document_id:
        errors.append("document_id is required.")
    if not request.original_pipeline_run_id:
        errors.append("original_pipeline_run_id is required.")
    if not request.reprocessing_reason:
        errors.append("reprocessing_reason is required and must not be empty.")
    if not request.linked_queue_entry_id:
        errors.append("linked_queue_entry_id is required.")
    if not request.linked_decision_id:
        errors.append("linked_decision_id is required.")
    if not request.requested_at:
        errors.append("requested_at is required.")

    return errors


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_decision_id() -> str:
    """Generate a new review decision UUID."""
    return str(uuid.uuid4())


def make_reprocessing_request_id() -> str:
    """Generate a new reprocessing request UUID."""
    return str(uuid.uuid4())


def build_reprocessing_request(
    decision: ReviewDecision,
    reprocessing_reason: str,
    suggested_document_class_hint: Optional[str] = None,
    suggested_extraction_notes: Optional[str] = None,
    original_gold_artifact_path: Optional[str] = None,
    original_bundle_path: Optional[str] = None,
    requested_at: Optional[str] = None,
) -> ReprocessingRequest:
    """
    Build a ReprocessingRequest from a ReviewDecision.

    The request inherits identifiers from the decision and queue entry.

    Args:
        decision: The ReviewDecision that triggered this request.
        reprocessing_reason: Human-readable reason for reprocessing.
        suggested_document_class_hint: Optional class hint for the re-run.
        suggested_extraction_notes: Optional extraction guidance notes.
        original_gold_artifact_path: Path to the original Gold artifact.
        original_bundle_path: Path to the B-5 bundle that included this record.
        requested_at: ISO timestamp; defaults to UTC now.

    Returns:
        ReprocessingRequest with all fields populated.
    """
    if requested_at is None:
        requested_at = datetime.now(tz=timezone.utc).isoformat()

    return ReprocessingRequest(
        reprocessing_request_id=make_reprocessing_request_id(),
        document_id=decision.document_id,
        gold_record_id=decision.gold_record_id,
        original_pipeline_run_id=decision.pipeline_run_id,
        requested_at=requested_at,
        schema_version=REPROCESSING_REQUEST_SCHEMA_VERSION,
        reprocessing_reason=reprocessing_reason,
        suggested_document_class_hint=suggested_document_class_hint,
        suggested_extraction_notes=suggested_extraction_notes,
        linked_queue_entry_id=decision.queue_entry_id,
        linked_decision_id=decision.decision_id,
        original_gold_artifact_path=original_gold_artifact_path,
        original_bundle_path=original_bundle_path,
    )
