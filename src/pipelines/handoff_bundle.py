"""
src/pipelines/handoff_bundle.py — B-5 Handoff Batch Manifest and Review Bundle.

Packages the Gold → Bedrock export batch outputs into a single, coherent,
reviewable batch handoff bundle. Each pipeline run that produces a B-5 bundle
has a single manifest artifact that links together:

  - batch metadata (batch_id, pipeline_run_id, generated_at, manifest_version)
  - aggregate outcome counts
  - per-record artifact references organized by outcome category:
    exported, quarantined, contract_blocked, skipped_not_export_ready
  - paths to the B-4 HandoffBatchReport artifacts (json + text), when available
  - review notes describing the batch state and limitations

This module owns:
  - MANIFEST_VERSION: stable schema version for the bundle structure
  - RecordArtifactRef: per-record artifact reference (IDs + paths)
  - HandoffBatchManifest: the full batch review bundle dataclass
  - build_handoff_batch_manifest(): assemble manifest from per-record summaries
  - compute_bundle_path(): deterministic bundle artifact path
  - format_bundle_text(): human-readable review summary string
  - write_handoff_bundle(): write JSON + text bundle artifacts

This module does NOT own:
  - Classification logic (classify_gold.py)
  - Export artifact writing or contract validation (export_handoff.py)
  - Outcome derivation or batch summary reporting (handoff_report.py)

Strategic boundary (B-0 through B-5):
  classify_gold.py   → runs pipeline, collects per-record summaries
  export_handoff.py  → validates contract, writes export artifact, returns ExportResult
  handoff_report.py  → derives outcome categories, aggregates batch report, writes report
  handoff_bundle.py  → packages batch into a single reviewable manifest/bundle

Design constraint (B-5 vs B-4):
  B-4 tells us what happened in the handoff path: outcome categories, reason codes,
  counts, and affected document ID lists. B-5 packages that batch into a clean
  manifest with full per-record artifact references — the single artifact a reviewer
  opens to understand and navigate the full state of a batch handoff run. B-5 may
  reuse B-4 report outputs but is a distinct bundle-level review artifact.

No live Bedrock/AWS integration is implied or implemented here. This is strictly
upstream handoff packaging behavior — local structural assembly and file write only.

Phase: B-5
Architecture context: ARCHITECTURE.md § Bedrock Handoff Design
Authoritative contract: docs/bedrock-handoff-contract.md
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.pipelines.handoff_report import (
    ALL_OUTCOME_CATEGORIES,
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
)


# Bundle manifest schema version.
# Increment when the manifest structure changes in a breaking way.
MANIFEST_VERSION = "v0.1.0"


# ---------------------------------------------------------------------------
# Per-record artifact reference
# ---------------------------------------------------------------------------


@dataclass
class RecordArtifactRef:
    """
    Artifact reference for a single Gold record in a batch.

    Groups the record's identifiers and artifact paths so a reviewer can
    locate every artifact associated with a given document in one place.

    Fields
    ------
    document_id
        Stable cross-layer document UUID from Bronze.
    gold_record_id
        UUID of the Gold classification record for this document.
    outcome_category
        One of the B-4 OUTCOME_* constants (exported, quarantined, etc.).
    outcome_reason
        One of the B-4 REASON_* constants (none, routing_quarantine, etc.).
    routing_label
        Routing label assigned during classification (regulatory_review, quarantine, etc.).
    gold_artifact_path
        Filesystem path to the Gold JSON artifact. Always present for eligible records.
    export_artifact_path
        Filesystem path to the export payload JSON artifact.
        Only present for exported records; None for all other outcomes.
    """

    document_id: str
    gold_record_id: str
    outcome_category: str
    outcome_reason: str
    routing_label: str
    gold_artifact_path: Optional[str]
    export_artifact_path: Optional[str]

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return {
            "document_id": self.document_id,
            "gold_record_id": self.gold_record_id,
            "outcome_category": self.outcome_category,
            "outcome_reason": self.outcome_reason,
            "routing_label": self.routing_label,
            "gold_artifact_path": self.gold_artifact_path,
            "export_artifact_path": self.export_artifact_path,
        }


# ---------------------------------------------------------------------------
# Batch manifest model
# ---------------------------------------------------------------------------


@dataclass
class HandoffBatchManifest:
    """
    Single reviewable batch handoff bundle for one Gold/export pipeline run.

    Packages batch metadata, aggregate outcome counts, per-record artifact
    references organized by outcome category, and links to B-4 report artifacts.

    This is the single artifact a reviewer opens to understand the full state
    of a batch handoff run — what was exported, what was blocked, where each
    artifact lives, and what the B-4 report says about the batch.

    Fields
    ------
    manifest_version
        Schema version for this manifest structure. Currently "v0.1.0".
    batch_id
        Stable identifier for this batch. Equal to pipeline_run_id.
    pipeline_run_id
        Pipeline run ID shared across all Gold records in this batch.
    generated_at
        ISO 8601 UTC timestamp when this manifest was assembled.
    total_records_processed
        All Silver records that entered the pipeline loop (eligible + ineligible).
    total_ineligible_skipped
        Records skipped due to validation_status='invalid' (before classification).
    total_eligible
        Records that proceeded to classification and export evaluation.
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
    exported_records
        RecordArtifactRef list for successfully exported records.
        Each entry includes the Gold artifact path and the export artifact path.
    quarantined_records
        RecordArtifactRef list for quarantined records.
        Each entry includes the Gold artifact path; export_artifact_path is None.
    contract_blocked_records
        RecordArtifactRef list for contract-blocked records.
        Each entry includes the Gold artifact path; export_artifact_path is None.
    skipped_records
        RecordArtifactRef list for skipped (not-export-ready) records.
    report_artifacts
        Paths to the B-4 HandoffBatchReport artifacts, or None if not written.
        Keys: "json_path", "text_path".
    review_notes
        Human-readable notes about the batch state and limitations.
        Auto-generated by build_handoff_batch_manifest().
    """

    manifest_version: str
    batch_id: str
    pipeline_run_id: str
    generated_at: str
    total_records_processed: int
    total_ineligible_skipped: int
    total_eligible: int
    total_exported: int
    total_quarantined: int
    total_contract_blocked: int
    total_skipped_not_export_ready: int
    outcome_distribution: dict = field(default_factory=dict)
    exported_records: list = field(default_factory=list)
    quarantined_records: list = field(default_factory=list)
    contract_blocked_records: list = field(default_factory=list)
    skipped_records: list = field(default_factory=list)
    report_artifacts: Optional[dict] = None
    review_notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "manifest_version": self.manifest_version,
            "batch_id": self.batch_id,
            "pipeline_run_id": self.pipeline_run_id,
            "generated_at": self.generated_at,
            "total_records_processed": self.total_records_processed,
            "total_ineligible_skipped": self.total_ineligible_skipped,
            "total_eligible": self.total_eligible,
            "total_exported": self.total_exported,
            "total_quarantined": self.total_quarantined,
            "total_contract_blocked": self.total_contract_blocked,
            "total_skipped_not_export_ready": self.total_skipped_not_export_ready,
            "outcome_distribution": self.outcome_distribution,
            "exported_records": [
                r.to_dict() if isinstance(r, RecordArtifactRef) else r
                for r in self.exported_records
            ],
            "quarantined_records": [
                r.to_dict() if isinstance(r, RecordArtifactRef) else r
                for r in self.quarantined_records
            ],
            "contract_blocked_records": [
                r.to_dict() if isinstance(r, RecordArtifactRef) else r
                for r in self.contract_blocked_records
            ],
            "skipped_records": [
                r.to_dict() if isinstance(r, RecordArtifactRef) else r
                for r in self.skipped_records
            ],
            "report_artifacts": self.report_artifacts,
            "review_notes": self.review_notes,
        }


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------


def build_handoff_batch_manifest(
    summaries: list[dict],
    pipeline_run_id: str,
    total_records_processed: Optional[int] = None,
    total_ineligible_skipped: int = 0,
    report_artifact_paths: Optional[dict] = None,
    generated_at: Optional[str] = None,
) -> HandoffBatchManifest:
    """
    Assemble a HandoffBatchManifest from per-record summaries and run context.

    Each summary dict must contain the B-4 outcome fields added by run_classify_gold():
        document_id, gold_record_id, routing_label, export_ready,
        gold_artifact_path, export_artifact_path,
        outcome_category, outcome_reason.

    Args:
        summaries:
            Per-record summary dicts from run_classify_gold(). Each must include
            outcome_category and outcome_reason (B-4 fields).
        pipeline_run_id:
            Run ID for this batch. Used as the batch_id in the manifest.
        total_records_processed:
            All Silver records loaded (eligible + ineligible).
            Defaults to len(summaries) + total_ineligible_skipped.
        total_ineligible_skipped:
            Records that failed the eligibility check before classification.
        report_artifact_paths:
            Paths to B-4 HandoffBatchReport artifacts, if they were written.
            Expected keys: "json_path", "text_path". Pass None if no report was
            written (the manifest will note this in review_notes).
        generated_at:
            ISO 8601 UTC timestamp for report assembly. Defaults to UTC now.

    Returns:
        HandoffBatchManifest with all counts, record references, and review notes.
    """
    if generated_at is None:
        generated_at = datetime.now(tz=timezone.utc).isoformat()

    total_eligible = len(summaries)
    if total_records_processed is None:
        total_records_processed = total_eligible + total_ineligible_skipped

    outcome_dist: dict[str, int] = {cat: 0 for cat in ALL_OUTCOME_CATEGORIES}

    exported: list[RecordArtifactRef] = []
    quarantined: list[RecordArtifactRef] = []
    contract_blocked: list[RecordArtifactRef] = []
    skipped: list[RecordArtifactRef] = []

    for s in summaries:
        outcome_cat = s.get("outcome_category", OUTCOME_SKIPPED_NOT_EXPORT_READY)
        outcome_reason = s.get("outcome_reason", "export_not_attempted")

        if outcome_cat in outcome_dist:
            outcome_dist[outcome_cat] += 1
        else:
            # Defensive: unknown category — count but don't drop
            outcome_dist[outcome_cat] = outcome_dist.get(outcome_cat, 0) + 1

        ref = RecordArtifactRef(
            document_id=s.get("document_id", ""),
            gold_record_id=s.get("gold_record_id", ""),
            outcome_category=outcome_cat,
            outcome_reason=outcome_reason,
            routing_label=s.get("routing_label", ""),
            gold_artifact_path=s.get("gold_artifact_path"),
            export_artifact_path=s.get("export_artifact_path"),
        )

        if outcome_cat == OUTCOME_EXPORTED:
            exported.append(ref)
        elif outcome_cat == OUTCOME_QUARANTINED:
            quarantined.append(ref)
        elif outcome_cat == OUTCOME_CONTRACT_BLOCKED:
            contract_blocked.append(ref)
        else:
            skipped.append(ref)

    review_notes = _build_review_notes(
        total_records_processed=total_records_processed,
        total_eligible=total_eligible,
        total_ineligible_skipped=total_ineligible_skipped,
        n_exported=len(exported),
        n_quarantined=len(quarantined),
        n_contract_blocked=len(contract_blocked),
        n_skipped=len(skipped),
        report_artifact_paths=report_artifact_paths,
    )

    return HandoffBatchManifest(
        manifest_version=MANIFEST_VERSION,
        batch_id=pipeline_run_id,
        pipeline_run_id=pipeline_run_id,
        generated_at=generated_at,
        total_records_processed=total_records_processed,
        total_ineligible_skipped=total_ineligible_skipped,
        total_eligible=total_eligible,
        total_exported=len(exported),
        total_quarantined=len(quarantined),
        total_contract_blocked=len(contract_blocked),
        total_skipped_not_export_ready=len(skipped),
        outcome_distribution=outcome_dist,
        exported_records=exported,
        quarantined_records=quarantined,
        contract_blocked_records=contract_blocked,
        skipped_records=skipped,
        report_artifacts=report_artifact_paths,
        review_notes=review_notes,
    )


# ---------------------------------------------------------------------------
# Bundle path computation
# ---------------------------------------------------------------------------


def compute_bundle_path(bundle_dir: Path, pipeline_run_id: str) -> Path:
    """
    Compute the deterministic bundle manifest JSON artifact path.

    Path: <bundle_dir>/handoff_bundle_<safe_run_id>.json

    The path is deterministic: same (bundle_dir, pipeline_run_id) always maps
    to the same path. No randomness or extra timestamps in the file name.
    """
    safe_run_id = (
        pipeline_run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    )
    return bundle_dir / f"handoff_bundle_{safe_run_id}.json"


# ---------------------------------------------------------------------------
# Bundle text formatter
# ---------------------------------------------------------------------------


def format_bundle_text(manifest: HandoffBatchManifest) -> str:
    """
    Format a HandoffBatchManifest as a human-readable text review summary.

    Suitable for terminal output and the .txt bundle artifact.
    """

    def _record_section(title: str, records: list, show_export_path: bool = False) -> list[str]:
        out = [f"--- {title} ({len(records)}) ---"]
        if not records:
            out.append("  (none)")
            return out
        for i, r in enumerate(records, 1):
            if isinstance(r, RecordArtifactRef):
                out.append(f"  [{i}] document_id : {r.document_id}")
                out.append(f"       routing    : {r.routing_label}")
                out.append(f"       outcome    : {r.outcome_category} / {r.outcome_reason}")
                if r.gold_artifact_path:
                    out.append(f"       gold       : {r.gold_artifact_path}")
                if show_export_path and r.export_artifact_path:
                    out.append(f"       export     : {r.export_artifact_path}")
            else:
                out.append(f"  [{i}] {r}")
        return out

    lines = [
        "=== Handoff Batch Review Bundle (B-5) ===",
        f"Manifest Version   : {manifest.manifest_version}",
        f"Batch ID           : {manifest.batch_id}",
        f"Pipeline Run ID    : {manifest.pipeline_run_id}",
        f"Generated At       : {manifest.generated_at}",
        "",
        "--- Record Counts ---",
        f"Total Processed    : {manifest.total_records_processed}",
        f"Ineligible Skipped : {manifest.total_ineligible_skipped}",
        f"Eligible           : {manifest.total_eligible}",
        "",
        "--- Outcome Counts ---",
        f"Exported           : {manifest.total_exported}",
        f"Quarantined        : {manifest.total_quarantined}",
        f"Contract Blocked   : {manifest.total_contract_blocked}",
        f"Skipped (not ready): {manifest.total_skipped_not_export_ready}",
        "",
    ]

    lines += _record_section("Exported Records", manifest.exported_records, show_export_path=True)
    lines.append("")
    lines += _record_section("Quarantined Records", manifest.quarantined_records)
    lines.append("")
    lines += _record_section("Contract Blocked Records", manifest.contract_blocked_records)
    lines.append("")
    lines += _record_section("Skipped Records", manifest.skipped_records)
    lines.append("")

    if manifest.report_artifacts:
        lines += [
            "--- Report Artifacts (B-4) ---",
            f"  JSON : {manifest.report_artifacts.get('json_path', 'n/a')}",
            f"  Text : {manifest.report_artifacts.get('text_path', 'n/a')}",
            "",
        ]

    if manifest.review_notes:
        lines.append("--- Review Notes ---")
        for note in manifest.review_notes:
            lines.append(f"  - {note}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bundle writer
# ---------------------------------------------------------------------------


def write_handoff_bundle(
    manifest: HandoffBatchManifest,
    bundle_dir: Path,
) -> tuple[Path, Path]:
    """
    Write a HandoffBatchManifest to JSON and text bundle artifacts.

    Artifact names use the pipeline_run_id (sanitized for filesystem):
        <bundle_dir>/handoff_bundle_<pipeline_run_id>.json
        <bundle_dir>/handoff_bundle_<pipeline_run_id>.txt

    Args:
        manifest: The HandoffBatchManifest to write.
        bundle_dir: Directory to write artifacts into. Created if absent.

    Returns:
        (json_path, text_path): Paths of the written artifacts.
    """
    bundle_dir.mkdir(parents=True, exist_ok=True)
    json_path = compute_bundle_path(bundle_dir, manifest.pipeline_run_id)
    safe_run_id = (
        manifest.pipeline_run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    )
    text_path = bundle_dir / f"handoff_bundle_{safe_run_id}.txt"

    json_path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")
    text_path.write_text(format_bundle_text(manifest), encoding="utf-8")

    return json_path, text_path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_review_notes(
    total_records_processed: int,
    total_eligible: int,
    total_ineligible_skipped: int,
    n_exported: int,
    n_quarantined: int,
    n_contract_blocked: int,
    n_skipped: int,
    report_artifact_paths: Optional[dict],
) -> list[str]:
    """
    Generate human-readable review notes for this batch's state.

    Notes describe batch composition, outcome highlights, downstream
    readiness, and any limitations — enough for a reviewer to understand
    the batch without re-running the pipeline.
    """
    notes: list[str] = []

    notes.append(
        f"Batch processed {total_records_processed} record(s): "
        f"{total_eligible} eligible, {total_ineligible_skipped} ineligible/skipped "
        "before classification."
    )
    notes.append(
        f"Export outcomes: {n_exported} exported, {n_quarantined} quarantined, "
        f"{n_contract_blocked} contract-blocked, {n_skipped} skipped."
    )

    if n_exported > 0:
        notes.append(
            f"{n_exported} record(s) successfully written as Bedrock handoff artifact(s). "
            "Export paths are in exported_records[].export_artifact_path."
        )
    if n_quarantined > 0:
        notes.append(
            f"{n_quarantined} record(s) routed to quarantine (governance path). "
            "No export file was written for quarantined records. "
            "Gold artifact paths are in quarantined_records[].gold_artifact_path."
        )
    if n_contract_blocked > 0:
        notes.append(
            f"{n_contract_blocked} record(s) were export-ready but failed B-1 contract "
            "validation. No export file was written for blocked records. "
            "Review contract_blocked_records and the corresponding Gold artifacts "
            "for field-level contract violation detail."
        )
    if n_skipped > 0:
        notes.append(
            f"{n_skipped} record(s) were not export-ready and not quarantined. "
            "No export was attempted. Gold artifact paths are in skipped_records[]."
        )

    if report_artifact_paths:
        notes.append(
            "B-4 HandoffBatchReport is attached via report_artifacts. "
            "The report provides outcome counts, reason code distributions, "
            "and a human-readable batch summary."
        )
    else:
        notes.append(
            "B-4 HandoffBatchReport was not generated for this bundle. "
            "Rerun with --report-dir to produce a structured outcome report "
            "alongside this bundle."
        )

    notes.append(
        "Export artifact path pattern: "
        "<export_dir>/<routing_label>/<document_id>.json "
        "(per B-0 §7 and ARCHITECTURE.md § Delivery Mechanism V1)."
    )
    notes.append(
        f"Manifest schema version: {MANIFEST_VERSION}. "
        "Authoritative contract: docs/bedrock-handoff-contract.md."
    )

    return notes
