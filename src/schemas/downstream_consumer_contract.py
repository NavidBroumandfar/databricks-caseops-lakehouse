"""
src/schemas/downstream_consumer_contract.py — Phase 6 downstream compatibility scaffolding.

Provides deterministic, testable expectations for a non-runtime downstream CaseOps
consumer that ingests the existing Gold export payload shape.

This module has no Bedrock SDK/AWS integration and performs only structural and
routing compatibility checks against the upstream-export contract from:
- docs/bedrock-handoff-contract.md
- src/schemas/bedrock_contract.py

Use this module to build consumer tests, contract compatibility suites, and
failure-handling simulations without introducing any downstream runtime
dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.schemas.bedrock_contract import (
    REQUIRED_CISA_EXTRACTED_FIELDS,
    REQUIRED_FDA_EXTRACTED_FIELDS,
    REQUIRED_INCIDENT_EXTRACTED_FIELDS,
    REQUIRED_PAYLOAD_FIELDS,
    REQUIRED_PROVENANCE_FIELDS,
    validate_export_payload,
)
from src.utils.classification_taxonomy import (
    DOCUMENT_TYPE_CISA_ADVISORY,
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_INCIDENT_REPORT,
    DOCUMENT_TYPE_QUALITY_AUDIT,
    DOCUMENT_TYPE_TECHNICAL_CASE,
    ROUTING_LABEL_INCIDENT_MANAGEMENT,
    ROUTING_LABEL_KNOWLEDGE_BASE,
    ROUTING_LABEL_QUALITY_MANAGEMENT,
    ROUTING_LABEL_QUARANTINE,
    ROUTING_LABEL_REGULATORY_REVIEW,
    ROUTING_LABEL_SECURITY_OPS,
)


# ---------------------------------------------------------------------------
# Contract status constants
# ---------------------------------------------------------------------------

CONSUMER_STATUS_ACTIVE = "active"
"""Routing contract is ready for productionized downstream ingestion."""

CONSUMER_STATUS_PLANNED = "planned"
"""Routing contract is documented but not actively consumed in this repo scope."""


# ---------------------------------------------------------------------------
# Downstream simulation result statuses
# ---------------------------------------------------------------------------

SIMULATION_STATUS_ACCEPTED = "accepted"
"""Payload is valid and can be routed to an active downstream consumer."""

SIMULATION_STATUS_REJECTED_CONTRACT = "contract_validation_failed"
"""Payload failed the upstream contract validator and cannot be routed."""

SIMULATION_STATUS_ROUTING_MISMATCH = "routing_document_type_mismatch"
"""`document_type` does not match the documented route contract for routing label."""

SIMULATION_STATUS_ROUTING_UNKNOWN = "routing_unknown_or_unsupported"
"""Routing label is not recognized by the downstream contract table."""

SIMULATION_STATUS_ROUTING_PLANNED = "routing_planned_not_active"
"""Routing label is documented but currently mapped to a planned downstream consumer."""

SIMULATION_STATUS_QUARANTINE = "governance_signal"
"""Routing label indicates quarantine; payload must stay in governance queue."""


@dataclass(frozen=True)
class DownstreamRoutingContract:
    """
    Explicit routing row for downstream consumer simulation.

    The `required_extracted_fields` list is used only for explicit test fixtures
    and diagnostics. Upstream contract validation already enforces required
    extracted fields for the three current executable document types.
    """

    routing_label: str
    downstream_consumer: str
    expected_document_type: str
    required_extracted_fields: tuple[str, ...]
    status: str = CONSUMER_STATUS_ACTIVE


# ---------------------------------------------------------------------------
# Active + planned routing contracts
# ---------------------------------------------------------------------------

ACTIVE_DOWNSTREAM_ROUTING_CONTRACTS: dict[str, DownstreamRoutingContract] = {
    ROUTING_LABEL_REGULATORY_REVIEW: DownstreamRoutingContract(
        routing_label=ROUTING_LABEL_REGULATORY_REVIEW,
        downstream_consumer="Bedrock regulatory intelligence index",
        expected_document_type=DOCUMENT_TYPE_FDA_WARNING_LETTER,
        required_extracted_fields=REQUIRED_FDA_EXTRACTED_FIELDS,
        status=CONSUMER_STATUS_ACTIVE,
    ),
    ROUTING_LABEL_SECURITY_OPS: DownstreamRoutingContract(
        routing_label=ROUTING_LABEL_SECURITY_OPS,
        downstream_consumer="Bedrock security operations index",
        expected_document_type=DOCUMENT_TYPE_CISA_ADVISORY,
        required_extracted_fields=REQUIRED_CISA_EXTRACTED_FIELDS,
        status=CONSUMER_STATUS_ACTIVE,
    ),
    ROUTING_LABEL_INCIDENT_MANAGEMENT: DownstreamRoutingContract(
        routing_label=ROUTING_LABEL_INCIDENT_MANAGEMENT,
        downstream_consumer="Bedrock incident management workflow",
        expected_document_type=DOCUMENT_TYPE_INCIDENT_REPORT,
        required_extracted_fields=REQUIRED_INCIDENT_EXTRACTED_FIELDS,
        status=CONSUMER_STATUS_ACTIVE,
    ),
}

PLANNED_DOWNSTREAM_ROUTING_CONTRACTS: dict[str, DownstreamRoutingContract] = {
    ROUTING_LABEL_QUALITY_MANAGEMENT: DownstreamRoutingContract(
        routing_label=ROUTING_LABEL_QUALITY_MANAGEMENT,
        downstream_consumer="Bedrock quality assurance workflow",
        expected_document_type=DOCUMENT_TYPE_QUALITY_AUDIT,
        required_extracted_fields=(),
        status=CONSUMER_STATUS_PLANNED,
    ),
    ROUTING_LABEL_KNOWLEDGE_BASE: DownstreamRoutingContract(
        routing_label=ROUTING_LABEL_KNOWLEDGE_BASE,
        downstream_consumer="Bedrock general knowledge base index",
        expected_document_type=DOCUMENT_TYPE_TECHNICAL_CASE,
        required_extracted_fields=(),
        status=CONSUMER_STATUS_PLANNED,
    ),
}


# Known optional top-level fields that are safe to ignore for forward compatibility.
KNOWN_TOP_LEVEL_FIELDS: set[str] = set(REQUIRED_PAYLOAD_FIELDS) | {
    "page_count",
    "char_count",
    "extraction_prompt_id",
}

KNOWN_PROVENANCE_FIELDS: set[str] = set(REQUIRED_PROVENANCE_FIELDS) | {
    "delivery_mechanism",
    "delta_share_name",
    "delivery_event_id",
}


@dataclass
class DownstreamSimulationResult:
    """
    Result of local no-runtime downstream ingestion simulation.

    Consumers should treat a payload as ingested only when `accepted=True`.
    """

    accepted: bool
    status: str
    document_id: str | None
    routing_label: str
    downstream_consumer: str | None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def simulate_downstream_consumption(payload: Any) -> DownstreamSimulationResult:
    """
    Simulate downstream CaseOps consumption of one export payload.

    Simulation steps:
      1. Reject quarantine routing as governance signal.
      2. Validate against upstream contract.
      3. Match routing label to downstream consumer contract.
      4. Reject unknown or planned routing targets.
      5. Verify document_type is the expected one for that routing target.
      6. Surface compatibility warnings for unknown optional fields only.

    Returns
    -------
    DownstreamSimulationResult
        status: one of SIMULATION_STATUS_* constants.
    """
    if isinstance(payload, dict) and payload.get("routing_label") == ROUTING_LABEL_QUARANTINE:
        return DownstreamSimulationResult(
            accepted=False,
            status=SIMULATION_STATUS_QUARANTINE,
            document_id=payload.get("document_id"),
            downstream_consumer="human_review_queue",
            routing_label=ROUTING_LABEL_QUARANTINE,
            errors=[
                "routing_label='quarantine' is a governance signal and is not forwarded"
            ],
        )

    contract_result = validate_export_payload(payload)

    if not contract_result.valid:
        return DownstreamSimulationResult(
            accepted=False,
            status=SIMULATION_STATUS_REJECTED_CONTRACT,
            document_id=payload.get("document_id") if isinstance(payload, dict) else None,
            routing_label=payload.get("routing_label") if isinstance(payload, dict) else "unknown",
            downstream_consumer=None,
            errors=list(contract_result.errors),
            warnings=list(contract_result.warnings),
        )

    if not isinstance(payload, dict):
        return DownstreamSimulationResult(
            accepted=False,
            status=SIMULATION_STATUS_REJECTED_CONTRACT,
            document_id=None,
            routing_label="unknown",
            downstream_consumer=None,
            errors=["Payload must be a dict object for downstream consumption"],
        )

    routing_label = payload["routing_label"]
    contract = ACTIVE_DOWNSTREAM_ROUTING_CONTRACTS.get(routing_label)
    if contract is None:
        contract = PLANNED_DOWNSTREAM_ROUTING_CONTRACTS.get(routing_label)
        if contract is None:
            return DownstreamSimulationResult(
                accepted=False,
                status=SIMULATION_STATUS_ROUTING_UNKNOWN,
                document_id=payload.get("document_id"),
                routing_label=routing_label,
                downstream_consumer=None,
                errors=[
                    f"routing_label '{routing_label}' is not in the downstream routing contract"
                ],
            )

        if contract.status == CONSUMER_STATUS_PLANNED:
            return DownstreamSimulationResult(
                accepted=False,
                status=SIMULATION_STATUS_ROUTING_PLANNED,
                document_id=payload.get("document_id"),
                routing_label=routing_label,
                downstream_consumer=contract.downstream_consumer,
                errors=[
                    f"routing_label '{routing_label}' is documented but not active in this phase"
                ],
            )

    expected_doc_type = contract.expected_document_type
    if payload["document_type"] != expected_doc_type:
        return DownstreamSimulationResult(
            accepted=False,
            status=SIMULATION_STATUS_ROUTING_MISMATCH,
            document_id=payload.get("document_id"),
            routing_label=routing_label,
            downstream_consumer=contract.downstream_consumer,
            errors=[
                f"routing_label '{routing_label}' expects document_type "
                f"'{expected_doc_type}', got '{payload['document_type']}'"
            ],
        )

    warnings = _collect_compatibility_warnings(payload)
    return DownstreamSimulationResult(
        accepted=True,
        status=SIMULATION_STATUS_ACCEPTED,
        document_id=payload.get("document_id"),
        routing_label=routing_label,
        downstream_consumer=contract.downstream_consumer,
        warnings=warnings,
    )


def _collect_compatibility_warnings(payload: dict[str, Any]) -> list[str]:
    """Return forward-compatibility warnings for unexpected optional fields."""
    warnings: list[str] = []
    unknown_top_level = sorted(set(payload.keys()) - KNOWN_TOP_LEVEL_FIELDS)
    if unknown_top_level:
        warnings.append(
            "Payload contains unknown top-level fields that downstream should ignore if safe: "
            f"{unknown_top_level}"
        )

    provenance = payload.get("provenance")
    if isinstance(provenance, dict):
        unknown_provenance = sorted(set(provenance.keys()) - KNOWN_PROVENANCE_FIELDS)
        if unknown_provenance:
            warnings.append(
                "Provenance contains unknown fields that downstream should ignore if safe: "
                f"{unknown_provenance}"
            )

    return warnings
