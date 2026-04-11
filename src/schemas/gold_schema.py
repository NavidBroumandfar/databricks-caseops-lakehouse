"""
Gold layer schema for the Databricks CaseOps Lakehouse pipeline.

A Gold record represents one classification pass on a Silver record.
It carries the assigned document type label, routing label, classification
confidence, and the fully assembled export payload for downstream Bedrock
consumption.

Records preserve full lineage: document_id → bronze_record_id → extraction_id → gold_record_id.
No Silver record is silently dropped; all classification outcomes are written.

V1 scope: FDA warning letters only. The taxonomy module (classification_taxonomy.py)
defines the closed label sets enforced here.

Authoritative contract: docs/data-contracts.md § Gold: AI-Ready Asset Contract
Bedrock handoff contract: docs/bedrock-handoff-contract.md § 4
Architecture context: ARCHITECTURE.md § Gold Layer

B-1 note: classification_confidence is Optional[float] in both ExportProvenance
and GoldRecord. Per B-0 §4.3 and §9: confidence is null in A-3B bootstrap-origin
records because ai_classify does not expose a scalar confidence score in that
implementation. The target-state pipeline populates this field; bootstrap-origin
records carry None. Contract validation enforces this distinction explicitly via
src/schemas/bedrock_contract.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


SCHEMA_VERSION = "v0.1.0"


# ---------------------------------------------------------------------------
# Export payload provenance sub-struct
# ---------------------------------------------------------------------------


class ExportProvenance(BaseModel):
    """
    Provenance block embedded in every export payload.

    Records the full transformation lineage so a Bedrock consumer can
    reconstruct how the document was processed.
    """

    ingested_at: str = Field(
        description="UTC timestamp when the source file was ingested (from Bronze)."
    )
    pipeline_run_id: str = Field(
        description="Pipeline run ID shared across all layers for this batch."
    )
    extraction_model: str = Field(
        description="Model identifier used by the Silver extraction step."
    )
    classification_model: str = Field(
        description="Model identifier used by the Gold classification step."
    )
    classification_confidence: Optional[float] = Field(
        default=None,
        description=(
            "Confidence score produced by the classifier. Range: 0.0–1.0 when present. "
            "Null in bootstrap-origin records (A-3B path) — see docs/bedrock-handoff-contract.md § 9."
        ),
    )
    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Data contract version this payload was written against.",
    )

    @field_validator("classification_confidence")
    @classmethod
    def confidence_range(cls, v: Optional[float]) -> Optional[float]:
        """When present, confidence must be in [0.0, 1.0]."""
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError(
                f"classification_confidence {v} out of range [0.0, 1.0]"
            )
        return v


# ---------------------------------------------------------------------------
# Export payload
# ---------------------------------------------------------------------------


class ExportPayload(BaseModel):
    """
    AI-ready export payload — the Bedrock handoff unit.

    Materalized as a standalone JSON file alongside the Gold record.
    Must contain all fields required by a downstream Bedrock retrieval index
    without any additional pipeline queries.

    Authoritative structure: docs/data-contracts.md § Export Payload Structure
    """

    document_id: str = Field(
        description="Stable UUID identifying the source document across all layers."
    )
    source_file: str = Field(
        description="Original filename of the ingested source document."
    )
    document_type: str = Field(
        description="Classification label assigned to this document (from taxonomy)."
    )
    routing_label: str = Field(
        description="Routing label determining the downstream Bedrock target."
    )
    extracted_fields: Dict[str, Any] = Field(
        description="Domain-specific structured fields extracted in the Silver stage."
    )
    parsed_text_excerpt: str = Field(
        description="First N characters of the Bronze parsed text (default: 2000 chars)."
    )
    provenance: ExportProvenance = Field(
        description="Full transformation lineage for this document."
    )

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Gold record
# ---------------------------------------------------------------------------


class GoldRecord(BaseModel):
    """
    Full Gold classification record.

    Combines traceability lineage, classification metadata, the assembled
    export payload, and export readiness determination for one Silver record.
    """

    # --- Cross-layer lineage ---
    document_id: str = Field(
        description="UUID v4 from Bronze. Stable across all pipeline layers."
    )
    bronze_record_id: str = Field(
        description="UUID v4 of the Bronze parse record used as input."
    )
    extraction_id: str = Field(
        description="UUID v4 of the Silver extraction record used as input."
    )
    gold_record_id: str = Field(
        description="UUID v4 unique to this Gold classification record."
    )
    pipeline_run_id: str = Field(
        description="MLflow run ID (or local run UUID) of the batch that produced this record."
    )

    # --- Timing ---
    classified_at: datetime = Field(
        description="UTC timestamp when classification completed."
    )

    # --- Classification output ---
    document_type_label: str = Field(
        description="Primary document type label assigned by the classifier (from taxonomy)."
    )
    routing_label: str = Field(
        description="Routing label determining which downstream system receives this record."
    )
    classification_confidence: Optional[float] = Field(
        default=None,
        description=(
            "Classifier confidence score. Range: 0.0–1.0 when present. "
            "Null in bootstrap-origin records — see docs/bedrock-handoff-contract.md § 9."
        ),
    )
    classification_model: str = Field(
        description="Model or function identifier used for classification."
    )

    # --- Export ---
    export_payload: ExportPayload = Field(
        description="Assembled AI-ready payload for Bedrock consumption."
    )
    export_ready: bool = Field(
        description="True if this record meets all export quality thresholds."
    )
    export_path: Optional[str] = Field(
        default=None,
        description="Volume path where the export JSON artifact was written. Null if not export-ready.",
    )

    # --- Contract version ---
    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Data contract version this record was written against.",
    )

    # --- Export helpers ---

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict with datetime fields as ISO 8601 strings."""
        raw = json.loads(self.model_dump_json())
        return raw

    def to_json_str(self, indent: int = 2) -> str:
        """Return a formatted JSON string suitable for writing to a local artifact file."""
        return json.dumps(self.to_json_dict(), indent=indent)

    @classmethod
    def utcnow(cls) -> datetime:
        """Convenience: current UTC datetime with tzinfo set."""
        return datetime.now(tz=timezone.utc)
