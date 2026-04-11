"""
src/schemas/bedrock_contract.py — B-1 Bedrock handoff contract validation.

Validates Gold export_payload dicts against the B-0 contract requirements
documented in docs/bedrock-handoff-contract.md.

B-1 purpose: convert the B-0 contract from documentation into repo-enforced,
testable behavior. This module is the authoritative validator for Gold export
payload structural compliance.

Scope:
- Validates required vs optional field presence per B-0 §4.1, §4.2, §4.3
- Validates provenance completeness per B-0 §4.3
- Validates required FDA warning letter extracted fields per B-0 §4.4
- Validates quarantine record shape per B-0 §6
- Does NOT call any Bedrock SDK, AWS service, or live integration endpoint

No live Bedrock integration is implied or implemented here. This is purely
structural validation of the export payload shape against the documented contract.

Authoritative contract: docs/bedrock-handoff-contract.md
Authoritative scope: PROJECT_SPEC.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Contract version this validator was written against.
# Must match SCHEMA_VERSION in gold_schema.py and the contract doc.
CONTRACT_VERSION = "v0.1.0"

# ---------------------------------------------------------------------------
# Required and optional field definitions per B-0 §4
# ---------------------------------------------------------------------------

# Required top-level payload fields per B-0 §4.1.
# Absence of any field is grounds to reject the handoff unit.
REQUIRED_PAYLOAD_FIELDS: tuple[str, ...] = (
    "document_id",
    "source_file",
    "document_type",
    "routing_label",
    "extracted_fields",
    "parsed_text_excerpt",
    "provenance",
)

# Optional top-level payload fields per B-0 §4.2.
# Their absence must not cause validation failure.
OPTIONAL_PAYLOAD_FIELDS: tuple[str, ...] = (
    "page_count",
    "char_count",
    "extraction_prompt_id",
)

# Required provenance sub-object fields per B-0 §4.3.
# classification_confidence is in this list but is nullable — its key must be
# present; a None/null value is acceptable (bootstrap path).
REQUIRED_PROVENANCE_FIELDS: tuple[str, ...] = (
    "ingested_at",
    "pipeline_run_id",
    "extraction_model",
    "classification_model",
    "classification_confidence",
    "schema_version",
)

# Required extracted_fields for document_type = 'fda_warning_letter' per B-0 §4.4.
# These are the V1 required fields for the single executable domain.
REQUIRED_FDA_EXTRACTED_FIELDS: tuple[str, ...] = (
    "issuing_office",
    "recipient_company",
    "issue_date",
    "violation_type",
    "corrective_action_requested",
)

# Document types that are valid for a handoff unit (not 'unknown').
VALID_HANDOFF_DOCUMENT_TYPES: frozenset[str] = frozenset({
    "fda_warning_letter",
    "cisa_advisory",
    "incident_report",
    "standard_operating_procedure",
    "quality_audit_record",
    "technical_case_record",
})

# Routing labels valid for a handoff unit (not 'quarantine').
VALID_HANDOFF_ROUTING_LABELS: frozenset[str] = frozenset({
    "regulatory_review",
    "security_ops",
    "incident_management",
    "quality_management",
    "knowledge_base",
})


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ContractValidationResult:
    """
    Outcome of a single contract validation pass.

    valid=True only if all required fields are present and structurally correct.
    errors: list of specific contract violations.
    warnings: non-blocking observations (e.g. null confidence on non-bootstrap record).
    """

    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def __repr__(self) -> str:
        status = "VALID" if self.valid else f"INVALID ({len(self.errors)} error(s))"
        return f"ContractValidationResult({status})"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_export_payload(payload: Any) -> ContractValidationResult:
    """
    Validate an export payload dict against the B-0 Bedrock handoff contract.

    A valid handoff unit per B-0 §3 must:
    - Be a well-formed dict with all required top-level fields (§4.1)
    - Have document_type != 'unknown'
    - Have routing_label != 'quarantine'
    - Carry a complete provenance sub-object (§4.3)
    - Carry required extracted_fields for the declared document_type (§4.4)

    Optional fields (§4.2) are never required — their absence must not produce
    a validation error.

    classification_confidence in provenance is allowed to be null (bootstrap
    path per B-0 §9). All other required provenance fields must be non-null.

    Returns ContractValidationResult. valid=True iff all required checks pass.
    No live Bedrock integration is called or implied.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(payload, dict):
        return ContractValidationResult(
            valid=False, errors=["payload must be a dict object"]
        )

    # --- Required top-level field presence ---
    for req_field in REQUIRED_PAYLOAD_FIELDS:
        if req_field not in payload:
            errors.append(f"Missing required field: '{req_field}'")

    # Early exit if structural fields missing — subsequent checks would cascade.
    if errors:
        return ContractValidationResult(valid=False, errors=errors, warnings=warnings)

    # --- document_id: must be a non-empty string ---
    document_id = payload["document_id"]
    if not isinstance(document_id, str) or not document_id.strip():
        errors.append("'document_id' must be a non-empty string")

    # --- source_file: must be a non-empty string ---
    source_file = payload["source_file"]
    if not isinstance(source_file, str) or not source_file.strip():
        errors.append("'source_file' must be a non-empty string")

    # --- document_type: must not be 'unknown' ---
    document_type = payload["document_type"]
    if not isinstance(document_type, str) or not document_type.strip():
        errors.append("'document_type' must be a non-empty string")
    elif document_type == "unknown":
        errors.append(
            "'document_type' is 'unknown' — this record should be quarantined, "
            "not treated as a valid handoff unit (B-0 §3)"
        )

    # --- routing_label: must not be 'quarantine' ---
    routing_label = payload["routing_label"]
    if not isinstance(routing_label, str) or not routing_label.strip():
        errors.append("'routing_label' must be a non-empty string")
    elif routing_label == "quarantine":
        errors.append(
            "'routing_label' is 'quarantine' — quarantine records must not be "
            "treated as handoff units; they have export_ready=False and produce "
            "no export file (B-0 §3, §6)"
        )

    # --- extracted_fields: must be a dict ---
    extracted_fields = payload["extracted_fields"]
    if not isinstance(extracted_fields, dict):
        errors.append("'extracted_fields' must be an object")
    else:
        # V1 FDA warning letter specific field validation (B-0 §4.4)
        if isinstance(document_type, str) and document_type == "fda_warning_letter":
            errors.extend(_validate_fda_extracted_fields(extracted_fields))

    # --- parsed_text_excerpt: must be a string ---
    excerpt = payload["parsed_text_excerpt"]
    if not isinstance(excerpt, str):
        errors.append("'parsed_text_excerpt' must be a string")

    # --- provenance: must be a dict with required sub-fields ---
    provenance = payload["provenance"]
    if not isinstance(provenance, dict):
        errors.append("'provenance' must be an object")
    else:
        prov_errors = _validate_provenance(provenance)
        errors.extend(prov_errors)

    return ContractValidationResult(
        valid=len(errors) == 0, errors=errors, warnings=warnings
    )


def validate_quarantine_record(gold_record: Any) -> ContractValidationResult:
    """
    Validate that a Gold record is correctly shaped as a quarantine record per B-0 §6.

    A valid quarantine record must have:
    - export_ready = False
    - routing_label = 'quarantine'
    - export_path = None (no file materialized)

    This validates governance correctness, not handoff validity. Quarantine records
    are never handoff units — they are governance signals.
    """
    errors: list[str] = []

    if not isinstance(gold_record, dict):
        return ContractValidationResult(
            valid=False, errors=["gold_record must be a dict object"]
        )

    export_ready = gold_record.get("export_ready")
    if export_ready is not False:
        errors.append(
            f"Quarantine records must have export_ready=False; got {export_ready!r}"
        )

    routing_label = gold_record.get("routing_label")
    if routing_label != "quarantine":
        errors.append(
            f"Quarantine records must have routing_label='quarantine'; got {routing_label!r}"
        )

    export_path = gold_record.get("export_path")
    if export_path is not None:
        errors.append(
            f"Quarantine records must not have an export_path; got {export_path!r}. "
            "Only export_ready=True records produce export files (B-0 §7)."
        )

    return ContractValidationResult(valid=len(errors) == 0, errors=errors)


# ---------------------------------------------------------------------------
# Internal validators
# ---------------------------------------------------------------------------


def _validate_provenance(provenance: dict) -> list[str]:
    """
    Validate the provenance sub-object per B-0 §4.3.

    All required provenance fields must be present. All must be non-null
    EXCEPT classification_confidence, which is explicitly nullable for
    bootstrap-origin records (B-0 §9, §4.3).
    """
    errors: list[str] = []

    for prov_field in REQUIRED_PROVENANCE_FIELDS:
        if prov_field not in provenance:
            errors.append(f"Missing required provenance field: '{prov_field}'")
        elif provenance[prov_field] is None and prov_field != "classification_confidence":
            errors.append(
                f"Required provenance field '{prov_field}' is null. "
                "All provenance fields except classification_confidence must be non-null."
            )

    # When classification_confidence is present and non-null, validate range.
    confidence = provenance.get("classification_confidence")
    if confidence is not None:
        if not isinstance(confidence, (int, float)):
            errors.append(
                "provenance.classification_confidence must be a number (0.0–1.0) or null"
            )
        elif not (0.0 <= float(confidence) <= 1.0):
            errors.append(
                f"provenance.classification_confidence {confidence} out of range [0.0, 1.0]"
            )

    return errors


def _validate_fda_extracted_fields(extracted_fields: dict) -> list[str]:
    """
    Validate required extracted_fields for document_type = 'fda_warning_letter' per B-0 §4.4.

    Required FDA fields: issuing_office, recipient_company, issue_date,
    violation_type (non-empty array), corrective_action_requested (boolean).
    """
    errors: list[str] = []

    for req_field in REQUIRED_FDA_EXTRACTED_FIELDS:
        if req_field not in extracted_fields:
            errors.append(
                f"Missing required FDA warning letter extracted_field: '{req_field}'"
            )
            continue

        value = extracted_fields[req_field]

        if value is None:
            errors.append(
                f"Required FDA warning letter extracted_field '{req_field}' is null"
            )
        elif req_field == "violation_type":
            if not isinstance(value, list) or len(value) == 0:
                errors.append(
                    "'violation_type' must be a non-empty array of violation category strings"
                )
        elif req_field == "corrective_action_requested":
            if not isinstance(value, bool):
                errors.append(
                    "'corrective_action_requested' must be a boolean"
                )

    return errors
