"""
src/pipelines/export_handoff.py — B-3 Export Packaging and Handoff Service Boundary.

Centralizes the Gold → Bedrock export/handoff materialization behavior that was
previously mixed into the main classification loop in classify_gold.py.

This module owns:
  - Export artifact path computation (deterministic, per B-0 §7)
  - Contract-gated export artifact write behavior (B-1 / B-2 enforcement)
  - Quarantine governance shape validation
  - ExportResult: the outcome object returned to the classify_gold.py pipeline loop

This module does NOT own:
  - Classification logic (LocalFDAWarningLetterClassifier, compute_routing_label)
  - Gold record assembly (assemble_gold_record stays in classify_gold.py)
  - Silver artifact loading or eligibility checks
  - Gold artifact writing (write_gold_artifact stays in classify_gold.py)

Strategic boundary:
  classify_gold.py  → assembles GoldRecord, calls execute_export, writes Gold artifact
  export_handoff.py → validates, decides, writes export artifact, returns ExportResult

No live Bedrock/AWS integration is implied or implemented here. This is strictly
upstream export packaging behavior — local structural validation and file write only.

Phase: B-3
Authoritative contract: docs/bedrock-handoff-contract.md
Contract validator: src/schemas/bedrock_contract.py (B-1)
Architecture context: ARCHITECTURE.md § Bedrock Handoff Design
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Allow running from repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.bedrock_contract import (
    validate_export_payload,
    validate_quarantine_record,
)
from src.schemas.gold_schema import GoldRecord
from src.utils.classification_taxonomy import ROUTING_LABEL_QUARANTINE


# ---------------------------------------------------------------------------
# Export result
# ---------------------------------------------------------------------------


@dataclass
class ExportResult:
    """
    Outcome of a single export/handoff materialization attempt.

    Returned by execute_export() to the classify_gold.py pipeline loop.
    The caller uses this to:
      - finalize the Gold record's export_ready and export_path fields before writing
      - populate the pipeline summary's contract_validation_errors field
      - log the export artifact path if successfully written
    """

    export_artifact_path: Optional[Path]
    export_path: Optional[str]
    export_ready: bool
    contract_validation_errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Export path computation
# ---------------------------------------------------------------------------


def compute_export_path(
    export_base_dir: Path,
    routing_label: str,
    document_id: str,
) -> Path:
    """
    Compute the deterministic export artifact path for an export-ready record.

    Path pattern: <export_base_dir>/<routing_label>/<document_id>.json

    This layout allows Bedrock consumers to subscribe to a specific routing-label
    subdirectory without reading the full Gold table. Defined in B-0 §7 and
    ARCHITECTURE.md § Delivery Mechanism (V1).

    The path is deterministic: same (export_base_dir, routing_label, document_id)
    always maps to the same path. No randomness or timestamps in the path.
    """
    return export_base_dir / routing_label / f"{document_id}.json"


# ---------------------------------------------------------------------------
# Export artifact writer
# ---------------------------------------------------------------------------


def write_export_artifact(record: GoldRecord, export_base_dir: Path) -> Path:
    """
    Write the export payload as a standalone JSON artifact at the deterministic path.

    Path: <export_base_dir>/<routing_label>/<document_id>.json

    Only called for records that have already passed contract validation.
    Creates the routing-label subdirectory if it does not exist.
    Returns the path where the artifact was written.
    """
    artifact_path = compute_export_path(
        export_base_dir, record.routing_label, record.document_id
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(record.export_payload.to_json_str(), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# Contract-gated export execution
# ---------------------------------------------------------------------------


def execute_export(
    gold_record: GoldRecord,
    export_base_dir: Path,
) -> ExportResult:
    """
    Execute the contract-gated export/handoff materialization for a single Gold record.

    This function centralizes three export-path decisions:

    1. export_ready=True (candidate handoff unit):
       - Validate export_payload against the B-1 contract (validate_export_payload).
       - If valid:   write the export artifact at the deterministic path; return success.
       - If invalid: do NOT write; return errors; set export_ready=False in the result.

    2. routing_label='quarantine' (implies export_ready=False):
       - Validate governance shape (validate_quarantine_record) as a correctness assertion.
       - No export file is written regardless of shape validation outcome.
       - Shape validation failures are logged to stderr.

    3. All other non-export-ready records:
       - No export file written; no contract validation attempted.

    Returns ExportResult with the final export_artifact_path, export_path (str for
    Gold record storage), export_ready state, and any contract_validation_errors.

    The caller (run_classify_gold) applies the result to the Gold record before
    writing the Gold artifact — ensuring the Gold record is written once with its
    final resolved state.

    No live Bedrock/AWS integration is called or implied.
    """
    if gold_record.export_ready:
        payload_dict = gold_record.export_payload.to_json_dict()
        contract_result = validate_export_payload(payload_dict)

        if contract_result.valid:
            artifact_path = write_export_artifact(gold_record, export_base_dir)
            return ExportResult(
                export_artifact_path=artifact_path,
                export_path=str(artifact_path),
                export_ready=True,
                contract_validation_errors=[],
            )
        else:
            # Contract block: invalid payload must not become a Bedrock handoff artifact.
            # Caller demotes the Gold record to export_ready=False and logs the block.
            return ExportResult(
                export_artifact_path=None,
                export_path=None,
                export_ready=False,
                contract_validation_errors=contract_result.errors,
            )

    elif gold_record.routing_label == ROUTING_LABEL_QUARANTINE:
        # Quarantine records produce no export file.
        # Shape validation is a governance correctness check, not a write gate.
        quarantine_result = validate_quarantine_record(gold_record.to_json_dict())
        if not quarantine_result.valid:
            print(
                f"[export_handoff] QUARANTINE SHAPE INVALID for {gold_record.document_id}: "
                f"{quarantine_result.errors}",
                file=sys.stderr,
            )
        return ExportResult(
            export_artifact_path=None,
            export_path=None,
            export_ready=False,
            contract_validation_errors=[],
        )

    else:
        # Non-export-ready, non-quarantine record — no export materialization attempted.
        return ExportResult(
            export_artifact_path=None,
            export_path=None,
            export_ready=False,
            contract_validation_errors=[],
        )
