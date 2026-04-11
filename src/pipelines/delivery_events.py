"""
src/pipelines/delivery_events.py — C-1: Delivery Event Materialization.

Implements the producer-side logic to generate and write per-batch delivery
event artifacts from existing batch outputs (Gold pipeline summaries, B-4
report paths, B-5 bundle paths).

A delivery event is the authoritative producer-side record stating:
  - which batch was processed and what its outcome counts were
  - which delivery mechanism and Delta Share are configured
  - where the B-5 HandoffBatchManifest artifact lives (for consumer navigation)
  - that the producer-side preparation is complete (status = 'prepared')

This module is additive to the existing B-4/B-5/B-6 reporting/bundle flow:
  classify_gold.py → B-4 report → B-5 bundle → [C-1] delivery event

This module does NOT:
  - Execute a real Delta Share (no credentials, no Unity Catalog SDK calls)
  - Claim Bedrock CaseOps has consumed the delivery
  - Replace or duplicate B-4 / B-5 artifacts
  - Implement retrieval, RAG, or agent logic

Consumer-side validation (confirming the Delta Share is queryable, the
delivery event row is readable, and payloads conform to v0.2.0) is a
Phase C-2 concern.

Phase: C-1
Schema: src/schemas/delivery_event.py
Architecture context: ARCHITECTURE.md § Delivery Mechanism (V2 — C-0)
Design decision: docs/live-handoff-design.md § 6, § 10
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# Allow running from repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.pipelines.handoff_report import (
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
)
from src.schemas.delivery_event import (
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
    DELIVERY_STATUS_PREPARED,
    DeliveryEvent,
)


# ---------------------------------------------------------------------------
# Delivery event ID generation
# ---------------------------------------------------------------------------


def generate_delivery_event_id() -> str:
    """
    Generate a stable UUID v4 for a delivery event.

    Called once per pipeline run before the classification loop so the
    ID can be embedded in export payload provenance AND referenced in
    the delivery event record — linking each payload to its batch delivery.
    """
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Delivery event builder
# ---------------------------------------------------------------------------


def build_delivery_event(
    summaries: list[dict],
    pipeline_run_id: str,
    delivery_event_id: Optional[str] = None,
    bundle_artifact_path: Optional[str] = None,
    report_artifact_path: Optional[str] = None,
    share_name: str = DEFAULT_SHARE_NAME,
    shared_object_name: str = DEFAULT_SHARED_OBJECT_NAME,
    notes: Optional[str] = None,
) -> DeliveryEvent:
    """
    Build a DeliveryEvent from the per-record summaries of a pipeline run.

    This is called after all Gold records have been classified and the
    B-5 bundle has been written (when bundle_artifact_path is provided).

    Parameters
    ----------
    summaries
        Per-record summary dicts from run_classify_gold(). Each summary
        carries outcome_category, routing_label, and record IDs.
    pipeline_run_id
        The pipeline run ID shared across all records in this batch.
        Matches the batch_id in the B-5 HandoffBatchManifest.
    delivery_event_id
        UUID for this delivery event. If not provided, a new UUID is
        generated. Should be generated before the pipeline loop so the
        same ID can be embedded in export payload provenance.
    bundle_artifact_path
        Path to the B-5 HandoffBatchManifest JSON artifact. Bedrock
        CaseOps navigates from the delivery event to this manifest to
        locate per-record export payload files.
    report_artifact_path
        Path to the B-4 HandoffBatchReport JSON artifact, if written.
    share_name
        Name of the Delta Share. Default: 'caseops_handoff'.
    shared_object_name
        Shared table name within the share. Default: 'gold_ai_ready_assets'.
    notes
        Optional batch-level notes.

    Returns
    -------
    DeliveryEvent
        Assembled delivery event record. Not yet written to disk.
    """
    event_id = delivery_event_id or generate_delivery_event_id()
    generated_at = datetime.now(tz=timezone.utc).isoformat()

    exported_count = sum(
        1 for s in summaries if s.get("outcome_category") == OUTCOME_EXPORTED
    )
    quarantined_count = sum(
        1 for s in summaries if s.get("outcome_category") == OUTCOME_QUARANTINED
    )
    contract_blocked_count = sum(
        1 for s in summaries if s.get("outcome_category") == OUTCOME_CONTRACT_BLOCKED
    )
    eligible_count = len(summaries)

    routing_labels = _extract_exported_routing_labels(summaries)

    return DeliveryEvent(
        delivery_event_id=event_id,
        pipeline_run_id=pipeline_run_id,
        batch_id=pipeline_run_id,
        generated_at=generated_at,
        delivery_mechanism=DELIVERY_MECHANISM_DELTA_SHARING,
        share_name=share_name,
        shared_object_name=shared_object_name,
        eligible_record_count=eligible_count,
        exported_record_count=exported_count,
        quarantined_record_count=quarantined_count,
        contract_blocked_count=contract_blocked_count,
        routing_labels=routing_labels,
        bundle_artifact_path=bundle_artifact_path,
        report_artifact_path=report_artifact_path,
        status=DELIVERY_STATUS_PREPARED,
        status_reason=(
            "Producer-side delivery layer complete. "
            "Runtime validation (share query + consumer receipt) is pending (C-2)."
        ),
        schema_version=DELIVERY_SCHEMA_VERSION,
        notes=notes,
    )


def _extract_exported_routing_labels(summaries: list[dict]) -> List[str]:
    """
    Extract the distinct routing labels present in exported records.

    Only includes routing labels for records that were actually exported
    (outcome_category == 'exported'), not quarantined or contract-blocked.
    """
    labels = {
        s["routing_label"]
        for s in summaries
        if s.get("outcome_category") == OUTCOME_EXPORTED
        and s.get("routing_label")
    }
    return sorted(labels)


# ---------------------------------------------------------------------------
# Artifact path computation
# ---------------------------------------------------------------------------


def compute_delivery_event_path(delivery_dir: Path, pipeline_run_id: str) -> Path:
    """
    Compute the deterministic delivery event artifact path.

    Pattern: <delivery_dir>/delivery_event_<pipeline_run_id>.json

    Deterministic: same (delivery_dir, pipeline_run_id) always maps to
    the same path. No randomness or secondary timestamps in the path.
    """
    safe_run_id = pipeline_run_id.replace("/", "_").replace("\\", "_")
    return delivery_dir / f"delivery_event_{safe_run_id}.json"


def compute_delivery_event_text_path(delivery_dir: Path, pipeline_run_id: str) -> Path:
    """Text (human-readable) companion to the delivery event JSON artifact."""
    safe_run_id = pipeline_run_id.replace("/", "_").replace("\\", "_")
    return delivery_dir / f"delivery_event_{safe_run_id}.txt"


# ---------------------------------------------------------------------------
# Text formatter
# ---------------------------------------------------------------------------


def format_delivery_event_text(event: DeliveryEvent) -> str:
    """
    Format a delivery event as a human-readable text summary.

    Intended for quick review alongside the JSON artifact. Does not
    duplicate every field — focuses on operational clarity.
    """
    lines = [
        "=" * 70,
        "DELIVERY EVENT — DATABRICKS CASEOPS LAKEHOUSE (C-1)",
        "=" * 70,
        f"Delivery Event ID  : {event.delivery_event_id}",
        f"Pipeline Run ID    : {event.pipeline_run_id}",
        f"Batch ID           : {event.batch_id}",
        f"Generated At       : {event.generated_at}",
        f"Schema Version     : {event.schema_version}",
        "",
        "DELIVERY CONFIGURATION",
        "-" * 40,
        f"Mechanism          : {event.delivery_mechanism}",
        f"Share Name         : {event.share_name or '(not set)'}",
        f"Shared Object      : {event.shared_object_name or '(not set)'}",
        "",
        "BATCH OUTCOME COUNTS",
        "-" * 40,
        f"Eligible Records   : {event.eligible_record_count}",
        f"Exported           : {event.exported_record_count}",
        f"Quarantined        : {event.quarantined_record_count}",
        f"Contract Blocked   : {event.contract_blocked_count}",
        f"Routing Labels     : {', '.join(event.routing_labels) if event.routing_labels else '(none)'}",
        "",
        "ARTIFACT REFERENCES",
        "-" * 40,
        f"Bundle Manifest    : {event.bundle_artifact_path or '(not set)'}",
        f"Batch Report       : {event.report_artifact_path or '(not set)'}",
        "",
        "STATUS",
        "-" * 40,
        f"Status             : {event.status}",
        f"Status Reason      : {event.status_reason or '(none)'}",
    ]
    if event.notes:
        lines += ["", "NOTES", "-" * 40, event.notes]
    lines += [
        "",
        "VALIDATION BOUNDARY",
        "-" * 40,
        "C-1 status 'prepared' means: producer-side delivery layer is complete.",
        "Runtime end-to-end validation (Delta Share query, consumer receipt,",
        "payload conformance) is a Phase C-2 concern.",
        "=" * 70,
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------


def write_delivery_event(
    event: DeliveryEvent,
    delivery_dir: Path,
) -> tuple[Path, Path]:
    """
    Write delivery event artifacts (JSON + text) to the delivery directory.

    Creates the directory if it does not exist.

    Returns
    -------
    (json_path, text_path)
        Paths to the written JSON and text artifacts.
    """
    delivery_dir.mkdir(parents=True, exist_ok=True)

    json_path = compute_delivery_event_path(delivery_dir, event.pipeline_run_id)
    text_path = compute_delivery_event_text_path(delivery_dir, event.pipeline_run_id)

    json_path.write_text(event.to_json_str(), encoding="utf-8")
    text_path.write_text(format_delivery_event_text(event), encoding="utf-8")

    return json_path, text_path


# ---------------------------------------------------------------------------
# Load helper
# ---------------------------------------------------------------------------


def load_delivery_event(json_path: Path) -> DeliveryEvent:
    """Load and validate a delivery event from a JSON artifact file."""
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    return DeliveryEvent.model_validate(raw)
