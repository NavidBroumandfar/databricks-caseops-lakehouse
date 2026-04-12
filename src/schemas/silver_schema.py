"""
Silver layer schema for the Databricks CaseOps Lakehouse pipeline.

A Silver record represents one structured extraction pass on a Bronze record.
Records carry full lineage back to the Bronze parse record and the source document.

V1 implements the FDA warning letter field set only.
D-1 adds the CISA advisory field set as the second executable domain.
Additional domain field sets are planned for V2+ and should be added as
separate Pydantic models following the same pattern.

Authoritative contract: docs/data-contracts.md § Silver: Extraction Schema Contract
Architecture context: ARCHITECTURE.md § Silver Layer / Multi-Domain Framework (D-0)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator


SCHEMA_VERSION = "v0.1.0"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ValidationStatus(str, Enum):
    valid = "valid"
    partial = "partial"
    invalid = "invalid"


# ---------------------------------------------------------------------------
# FDA Warning Letter — V1 extracted fields
# ---------------------------------------------------------------------------


class FDAWarningLetterFields(BaseModel):
    """
    Structured fields extracted from an FDA warning letter.

    Required fields must be present for validation_status = 'valid'.
    Optional fields may be absent without causing an 'invalid' status.

    Authoritative field contract: docs/data-contracts.md § FDA Warning Letter Fields
    """

    # Required
    issuing_office: Optional[str] = Field(
        default=None,
        description="FDA office or district that issued the letter.",
    )
    recipient_company: Optional[str] = Field(
        default=None,
        description="Company or facility named in the letter.",
    )
    issue_date: Optional[str] = Field(
        default=None,
        description="Date the warning letter was issued. ISO 8601 date string (YYYY-MM-DD) or human-readable.",
    )
    violation_type: Optional[List[str]] = Field(
        default=None,
        description="List of violation category labels cited in the letter.",
    )
    corrective_action_requested: Optional[bool] = Field(
        default=None,
        description="True if the letter explicitly requests corrective action.",
    )

    # Optional
    recipient_name: Optional[str] = Field(
        default=None,
        description="Name of the individual recipient (person) at the company.",
    )
    cited_regulations: Optional[List[str]] = Field(
        default=None,
        description="List of regulation citations (e.g., '21 CFR § 211.160(b)').",
    )
    response_deadline_days: Optional[int] = Field(
        default=None,
        description="Number of working days given to respond, if stated.",
    )
    product_involved: Optional[str] = Field(
        default=None,
        description="Primary product or product category named in the violations.",
    )
    summary: Optional[str] = Field(
        default=None,
        description="Brief narrative summary of the letter's key findings.",
    )


# Required fields for FDA warning letter — used for coverage calculation and validation
FDA_REQUIRED_FIELDS: List[str] = [
    "issuing_office",
    "recipient_company",
    "issue_date",
    "violation_type",
    "corrective_action_requested",
]

FDA_OPTIONAL_FIELDS: List[str] = [
    "recipient_name",
    "cited_regulations",
    "response_deadline_days",
    "product_involved",
    "summary",
]

FDA_ALL_FIELDS: List[str] = FDA_REQUIRED_FIELDS + FDA_OPTIONAL_FIELDS


# ---------------------------------------------------------------------------
# CISA Advisory — D-1 extracted fields
# ---------------------------------------------------------------------------


class CISAAdvisoryFields(BaseModel):
    """
    Structured fields extracted from a CISA cybersecurity advisory.

    Required fields must be present for validation_status = 'valid'.
    Optional fields may be absent without causing an 'invalid' status.

    Authoritative field contract: docs/data-contracts.md § CISA Advisory Fields
    Phase D-1 domain implementation.
    """

    # Required
    advisory_id: Optional[str] = Field(
        default=None,
        description="CISA advisory identifier (e.g., 'ICSA-24-046-01' or 'AA24-046A').",
    )
    title: Optional[str] = Field(
        default=None,
        description="Full title of the advisory.",
    )
    published_date: Optional[str] = Field(
        default=None,
        description="Date the advisory was published. ISO 8601 date string (YYYY-MM-DD) or human-readable.",
    )
    severity_level: Optional[str] = Field(
        default=None,
        description="Severity classification: Critical, High, Medium, or Low.",
    )
    remediation_available: Optional[bool] = Field(
        default=None,
        description="True if patches, mitigations, or workarounds are available.",
    )

    # Optional
    affected_products: Optional[List[str]] = Field(
        default=None,
        description="List of affected product names and versions.",
    )
    cve_ids: Optional[List[str]] = Field(
        default=None,
        description="List of CVE identifiers referenced in the advisory (e.g., 'CVE-2024-12345').",
    )
    remediation_summary: Optional[str] = Field(
        default=None,
        description="Brief description of recommended mitigations or patch actions.",
    )
    summary: Optional[str] = Field(
        default=None,
        description="Brief narrative summary of the advisory's key findings.",
    )


CISA_REQUIRED_FIELDS: List[str] = [
    "advisory_id",
    "title",
    "published_date",
    "severity_level",
    "remediation_available",
]

CISA_OPTIONAL_FIELDS: List[str] = [
    "affected_products",
    "cve_ids",
    "remediation_summary",
    "summary",
]

CISA_ALL_FIELDS: List[str] = CISA_REQUIRED_FIELDS + CISA_OPTIONAL_FIELDS


# ---------------------------------------------------------------------------
# Coverage calculation helpers
# ---------------------------------------------------------------------------


def compute_field_coverage(fields: FDAWarningLetterFields) -> float:
    """
    Return the fraction of FDA warning letter fields that are non-null.

    Coverage is computed over all expected fields (required + optional).
    Returns a float in [0.0, 1.0].
    """
    fields_dict = fields.model_dump()
    populated = sum(
        1 for f in FDA_ALL_FIELDS
        if fields_dict.get(f) is not None
        and fields_dict.get(f) != []
        and fields_dict.get(f) != ""
    )
    return round(populated / len(FDA_ALL_FIELDS), 4) if FDA_ALL_FIELDS else 0.0


def compute_cisa_field_coverage(fields: CISAAdvisoryFields) -> float:
    """
    Return the fraction of CISA advisory fields that are non-null.

    Coverage is computed over all expected fields (required + optional).
    Returns a float in [0.0, 1.0].
    """
    fields_dict = fields.model_dump()
    populated = sum(
        1 for f in CISA_ALL_FIELDS
        if fields_dict.get(f) is not None
        and fields_dict.get(f) != []
        and fields_dict.get(f) != ""
    )
    return round(populated / len(CISA_ALL_FIELDS), 4) if CISA_ALL_FIELDS else 0.0


# ---------------------------------------------------------------------------
# Silver record
# ---------------------------------------------------------------------------


class SilverRecord(BaseModel):
    """
    Full Silver extraction record.

    Combines core traceability fields, extraction metadata, and the
    domain-specific extracted field struct for one Bronze record.
    """

    # --- Cross-layer lineage ---
    document_id: str = Field(
        description="UUID v4 from Bronze. Stable across all pipeline layers."
    )
    bronze_record_id: str = Field(
        description="UUID v4 of the Bronze parse record used as input for this extraction."
    )
    extraction_id: str = Field(
        description="UUID v4 unique to this extraction pass."
    )
    pipeline_run_id: str = Field(
        description="MLflow run ID (or local run UUID) of the pipeline batch that produced this record."
    )

    # --- Timing ---
    extracted_at: datetime = Field(
        description="UTC timestamp when extraction completed."
    )

    # --- Extraction metadata ---
    document_class_hint: Optional[str] = Field(
        default=None,
        description="Document class hint used to select the extraction prompt.",
    )
    extraction_prompt_id: str = Field(
        description="Stable identifier for the prompt template used for extraction."
    )
    extraction_model: str = Field(
        description="Model or function identifier used for extraction (e.g., 'local_rule_extractor/v1')."
    )

    # --- Extracted content ---
    # Typed as Any to support multiple domain field models (FDAWarningLetterFields,
    # CISAAdvisoryFields, and future D-2+ models). At construction time the caller
    # always passes a real Pydantic model; Pydantic serializes it to a dict on
    # model_dump(). Future domains add their model here without breaking V1.
    extracted_fields: Optional[Any] = Field(
        default=None,
        description="Domain-specific extracted fields. Null only for invalid records with no output.",
    )

    # --- Quality signals ---
    field_coverage_pct: float = Field(
        ge=0.0,
        le=1.0,
        description="Fraction of expected fields with non-null values. Range: 0.0–1.0.",
    )
    validation_status: ValidationStatus = Field(
        description="Extraction validity: valid, partial, or invalid."
    )
    validation_errors: List[str] = Field(
        default_factory=list,
        description="Field-level validation error messages. Empty for valid records.",
    )

    # --- Contract version ---
    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Data contract version this record was written against.",
    )

    # --- Validators ---

    @model_validator(mode="after")
    def validate_status_consistency(self) -> "SilverRecord":
        """
        Enforce logical consistency between validation_status and extracted_fields.

        - invalid records without any extracted output may have extracted_fields=None
        - valid records must have extracted_fields populated
        - partial records should have extracted_fields populated (possibly with gaps)
        """
        if self.validation_status == ValidationStatus.valid:
            if self.extracted_fields is None:
                raise ValueError(
                    "extracted_fields must be populated when validation_status is 'valid'."
                )
        return self

    # --- Export helpers ---

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict with datetime fields as ISO 8601 strings."""
        raw = self.model_dump()
        if raw.get("extracted_at") and isinstance(raw["extracted_at"], datetime):
            raw["extracted_at"] = raw["extracted_at"].isoformat()
        if raw.get("validation_status") and hasattr(raw["validation_status"], "value"):
            raw["validation_status"] = raw["validation_status"].value
        return raw

    def to_json_str(self, indent: int = 2) -> str:
        """Return a formatted JSON string suitable for writing to a local artifact file."""
        return json.dumps(self.to_json_dict(), indent=indent, default=str)

    @classmethod
    def utcnow(cls) -> datetime:
        """Convenience: current UTC datetime with tzinfo set."""
        return datetime.now(tz=timezone.utc)
