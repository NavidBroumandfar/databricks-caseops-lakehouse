"""
src/schemas/delivery_event.py — C-1: Delivery Event Schema.

Pydantic model for per-batch delivery event records written by the
producer-side delivery layer (src/pipelines/delivery_events.py).

A delivery event is the authoritative per-batch record of a Gold →
Bedrock CaseOps handoff attempt. One delivery event is written per
pipeline run that includes a delivery handoff step.

The delivery event captures:
  - which batch / pipeline run triggered the delivery
  - which delivery mechanism was used (delta_sharing in V2-C)
  - which Delta Share and shared object are involved
  - batch-level outcome counts (exported, quarantined, contract_blocked)
  - the path to the B-5 HandoffBatchManifest for this batch
  - the status of the producer-side preparation (not the consumer receipt)
  - schema_version = 'v0.2.0' for all C-1+ delivery events

C-1 implementation scope:
  - This record is written by this repo (producer side only)
  - It does NOT record Bedrock CaseOps receipt — that is consumer-side (C-2 validation)
  - status = 'prepared' means the producer-side layer is complete;
    runtime end-to-end validation is a C-2 concern
  - No live Delta Sharing SDK calls are made when this record is written

V2 contract: docs/live-handoff-design.md §10
Architecture context: ARCHITECTURE.md § Bedrock Handoff Design
Data contracts: docs/data-contracts.md § V2 Contract Version Plan — v0.2.0

Phase: C-1
"""

from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DELIVERY_SCHEMA_VERSION = "v0.2.0"

DELIVERY_MECHANISM_DELTA_SHARING = "delta_sharing"

DEFAULT_SHARE_NAME = "caseops_handoff"
DEFAULT_SHARED_OBJECT_NAME = "gold_ai_ready_assets"

# Producer-side status vocabulary.
# These reflect what THIS REPO has done — not whether Bedrock has consumed.
DELIVERY_STATUS_PREPARED = "prepared"
"""
Producer-side delivery event written; share preparation manifest
generated; runtime end-to-end validation is pending (C-2).
"""

DELIVERY_STATUS_FAILED = "failed"
"""
Delivery event write or manifest preparation encountered an error.
"""

ALL_DELIVERY_STATUSES = (DELIVERY_STATUS_PREPARED, DELIVERY_STATUS_FAILED)


# ---------------------------------------------------------------------------
# Delivery event model
# ---------------------------------------------------------------------------


class DeliveryEvent(BaseModel):
    """
    Per-batch producer-side delivery event record.

    Written after a successful batch pipeline run that includes the C-1
    delivery augmentation layer. One record per pipeline run / batch.

    This record is the producer-side delivery notification artifact. It is
    the Databricks CaseOps Lakehouse's authoritative statement that:
      - a batch was processed
      - the Gold table is shared via the named Delta Share
      - the B-5 batch manifest is available at bundle_artifact_path
      - the producer-side preparation is complete

    It does NOT claim that a Bedrock CaseOps consumer has received or
    validated the delivery. That is recorded in C-2 validation.

    Fields
    ------
    delivery_event_id
        UUID v4 uniquely identifying this delivery event.
        Stable: same batch always receives the same ID within a run.
    pipeline_run_id
        Pipeline run ID of the batch that triggered this event.
        Matches the pipeline_run_id in the B-5 HandoffBatchManifest.
    batch_id
        Stable batch identifier. Equal to pipeline_run_id by design
        (consistent with HandoffBatchManifest.batch_id).
    generated_at
        UTC ISO 8601 timestamp when this delivery event was written.
    delivery_mechanism
        Primary delivery mechanism. 'delta_sharing' for all V2-C events.
    share_name
        Delta Share name. Default: 'caseops_handoff'.
    shared_object_name
        Name of the shared table within the share. Default: 'gold_ai_ready_assets'.
    eligible_record_count
        Total eligible records that entered the classification loop.
    exported_record_count
        Records successfully exported as Bedrock handoff artifacts.
    quarantined_record_count
        Records routed to quarantine (governance path; no export file).
    contract_blocked_count
        Records that were export-ready but failed B-1 contract validation.
    routing_labels
        List of distinct routing labels present in this batch's exported records.
    bundle_artifact_path
        Path to the B-5 HandoffBatchManifest JSON artifact for this batch.
        Bedrock CaseOps uses this to locate per-record export payload files.
    report_artifact_path
        Path to the B-4 HandoffBatchReport JSON artifact, if written.
    status
        Producer-side delivery status. See DELIVERY_STATUS_* constants.
    status_reason
        Optional human-readable note about the status, especially for failures.
    schema_version
        Data contract version. 'v0.2.0' for all C-1+ delivery events.
    notes
        Optional free-text batch-level notes.
    """

    delivery_event_id: str = Field(
        description="UUID v4 uniquely identifying this delivery event."
    )
    pipeline_run_id: str = Field(
        description="Pipeline run ID of the batch that triggered this delivery event."
    )
    batch_id: str = Field(
        description=(
            "Stable batch identifier. Equal to pipeline_run_id. "
            "Matches HandoffBatchManifest.batch_id."
        )
    )
    generated_at: str = Field(
        description="UTC ISO 8601 timestamp when this delivery event was written."
    )
    delivery_mechanism: str = Field(
        default=DELIVERY_MECHANISM_DELTA_SHARING,
        description="Primary delivery mechanism. 'delta_sharing' for V2-C.",
    )
    share_name: Optional[str] = Field(
        default=DEFAULT_SHARE_NAME,
        description="Delta Share name provisioned in Unity Catalog.",
    )
    shared_object_name: Optional[str] = Field(
        default=DEFAULT_SHARED_OBJECT_NAME,
        description="Shared table name within the Delta Share.",
    )
    eligible_record_count: int = Field(
        description="Total records that entered the classification loop in this batch.",
        ge=0,
    )
    exported_record_count: int = Field(
        description="Records successfully exported as Bedrock handoff artifacts.",
        ge=0,
    )
    quarantined_record_count: int = Field(
        description="Records routed to quarantine (no export file produced).",
        ge=0,
    )
    contract_blocked_count: int = Field(
        description="Records that failed B-1 contract validation and were not exported.",
        ge=0,
    )
    routing_labels: List[str] = Field(
        default_factory=list,
        description="Distinct routing labels present in exported records for this batch.",
    )
    bundle_artifact_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to the B-5 HandoffBatchManifest JSON artifact for this batch. "
            "Bedrock CaseOps uses this manifest to locate per-record export files."
        ),
    )
    report_artifact_path: Optional[str] = Field(
        default=None,
        description="Path to the B-4 HandoffBatchReport JSON artifact, if written.",
    )
    status: str = Field(
        default=DELIVERY_STATUS_PREPARED,
        description=(
            "Producer-side delivery status. 'prepared' = this repo's delivery layer "
            "is complete; consumer-side runtime validation is pending (C-2). "
            "'failed' = an error occurred in the producer-side layer."
        ),
    )
    status_reason: Optional[str] = Field(
        default=None,
        description="Human-readable explanation of the status, especially for failures.",
    )
    schema_version: str = Field(
        default=DELIVERY_SCHEMA_VERSION,
        description="Data contract version. 'v0.2.0' for all C-1+ delivery events.",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional free-text batch-level notes.",
    )

    @field_validator("delivery_mechanism")
    @classmethod
    def validate_delivery_mechanism(cls, v: str) -> str:
        """Delivery mechanism must be a known value."""
        known = {DELIVERY_MECHANISM_DELTA_SHARING}
        if v not in known:
            raise ValueError(
                f"delivery_mechanism '{v}' is not a known mechanism. "
                f"Known: {sorted(known)}"
            )
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Status must be a known delivery status constant."""
        if v not in ALL_DELIVERY_STATUSES:
            raise ValueError(
                f"status '{v}' is not a known delivery status. "
                f"Known: {sorted(ALL_DELIVERY_STATUSES)}"
            )
        return v

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        """Schema version must be v0.2.0 or later for delivery events."""
        if v != DELIVERY_SCHEMA_VERSION:
            raise ValueError(
                f"Delivery events require schema_version='{DELIVERY_SCHEMA_VERSION}'. "
                f"Got: '{v}'"
            )
        return v

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        """Return a formatted JSON string."""
        return json.dumps(self.to_json_dict(), indent=indent)
