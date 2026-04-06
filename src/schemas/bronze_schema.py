"""
Bronze layer schema for the Databricks CaseOps Lakehouse pipeline.

A Bronze record represents the output of one parse pass on a single source document.
Records are append-only: reprocessing a document creates a new bronze_record_id.

Authoritative contract: docs/data-contracts.md § Bronze: Parse Output Contract
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".md"}
SCHEMA_VERSION = "v0.1.0"


class ParseStatus(str, Enum):
    success = "success"
    partial = "partial"
    failed = "failed"


class BronzeRecord(BaseModel):
    """
    Full Bronze parse record.

    Combines source document metadata (captured at ingest) with parse output
    (populated after ai_parse_document or a local parser completes).
    """

    # --- Stable document identity ---
    document_id: str = Field(
        description="UUID v4 assigned at ingest. Stable across all downstream layers."
    )
    bronze_record_id: str = Field(
        description="UUID v4 unique to this parse pass. New record on every reprocess."
    )

    # --- Source file provenance ---
    source_path: str = Field(
        description="Full path to the original file (Volume path or local path for dev)."
    )
    file_name: str = Field(description="Original filename as it appeared at ingest.")
    file_extension: str = Field(
        description="Lowercased file extension. Allowed: .pdf, .docx, .txt, .md"
    )
    file_size_bytes: int = Field(gt=0, description="File size in bytes at ingestion time.")
    file_hash: str = Field(
        description="SHA-256 hex digest of the raw file bytes. Used for deduplication."
    )
    mime_type: str = Field(description="Detected or declared MIME type (RFC 2045 format).")

    # --- Timing ---
    ingested_at: datetime = Field(description="UTC timestamp when the file was registered.")
    parsed_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when parsing completed. Null if parse not yet attempted.",
    )

    # --- Parse outcome ---
    parse_status: ParseStatus = Field(
        description="Outcome of the parse attempt: success, partial, or failed."
    )
    parse_failure_reason: Optional[str] = Field(
        default=None,
        description="Human-readable reason for parse failure. Required when parse_status=failed.",
    )
    parsed_text: Optional[str] = Field(
        default=None,
        description="Full extracted text content. Null for failed records.",
    )
    page_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of pages parsed. Null for non-paginated formats (e.g., .txt).",
    )
    char_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Character count of parsed_text. Populated for success/partial records.",
    )

    # --- Parser identity ---
    parse_model: str = Field(
        description="Identifier for the parser used (e.g., 'local_text_parser/v1', 'ai_parse_document/v1')."
    )

    # --- Traceability ---
    pipeline_run_id: str = Field(
        description="MLflow run ID (or local run UUID) of the pipeline batch that produced this record."
    )
    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Data contract version this record was written against.",
    )

    # --- Optional context fields ---
    document_class_hint: Optional[str] = Field(
        default=None,
        description="Operator-supplied document class hint (e.g., 'fda_warning_letter'). Not validated at ingest.",
    )
    source_system: Optional[str] = Field(
        default=None,
        description="Label for the originating system (e.g., 'fda_portal', 'local_dev').",
    )
    ingested_by: Optional[str] = Field(
        default=None,
        description="Principal or job identity that triggered ingest.",
    )

    # --- Validators ---

    @field_validator("file_extension")
    @classmethod
    def validate_extension(cls, v: str) -> str:
        normalized = v.lower()
        if normalized not in ALLOWED_EXTENSIONS:
            raise ValueError(
                f"file_extension '{v}' is not in the allowed set: {sorted(ALLOWED_EXTENSIONS)}"
            )
        return normalized

    @field_validator("file_hash")
    @classmethod
    def validate_sha256_format(cls, v: str) -> str:
        if len(v) != 64 or not all(c in "0123456789abcdef" for c in v.lower()):
            raise ValueError(
                f"file_hash must be a 64-character lowercase hex SHA-256 digest, got: '{v[:16]}...'"
            )
        return v.lower()

    @model_validator(mode="after")
    def validate_parse_status_constraints(self) -> "BronzeRecord":
        if self.parse_status == ParseStatus.failed:
            if not self.parse_failure_reason:
                raise ValueError(
                    "parse_failure_reason is required when parse_status is 'failed'."
                )
            if self.parsed_text is not None:
                raise ValueError(
                    "parsed_text must be null when parse_status is 'failed'."
                )
        if self.parse_status in (ParseStatus.success, ParseStatus.partial):
            if self.char_count is None:
                raise ValueError(
                    f"char_count is required when parse_status is '{self.parse_status.value}'."
                )
        return self

    # --- Export helpers ---

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict with datetime fields as ISO 8601 strings."""
        raw = self.model_dump()
        for key in ("ingested_at", "parsed_at"):
            if raw[key] is not None and isinstance(raw[key], datetime):
                raw[key] = raw[key].isoformat()
        if raw.get("parse_status") and hasattr(raw["parse_status"], "value"):
            raw["parse_status"] = raw["parse_status"].value
        return raw

    def to_json_str(self, indent: int = 2) -> str:
        """Return a formatted JSON string. Safe for writing to a local artifact file."""
        return json.dumps(self.to_json_dict(), indent=indent, default=str)

    @classmethod
    def utcnow(cls) -> datetime:
        """Convenience: current UTC datetime with tzinfo set."""
        return datetime.now(tz=timezone.utc)
