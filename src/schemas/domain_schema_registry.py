"""
domain_schema_registry.py — Per-domain Silver schema family registry (Phase D-0)

Provides the architectural home for per-domain Silver schema selection,
validation routing, and field set access. D-0 establishes this framework;
D-1 and D-2 will fill in the CISA advisory and incident report schema families.

Design:
    One DomainSchemaInfo per domain, keyed by domain_key.
    Each entry carries:
      - required_fields / optional_fields / all_fields: field name lists
        (sourced from docs/data-contracts.md § Domain-Specific Extracted Fields)
      - build_fields_model: factory (raw dict → Pydantic model instance)
        For ACTIVE domains this constructs the real model.
        For PLANNED domains this raises DomainNotImplementedError.
      - status: mirrors the domain's DomainStatus in domain_registry.py

    This module does NOT own the Pydantic models themselves — those live in
    src/schemas/silver_schema.py (and future D-1/D-2 schema files).
    It provides the routing layer that connects domain_key → schema family.

Usage:
    from src.schemas.domain_schema_registry import (
        get_schema_info,
        build_fields_for_domain,
        list_schema_domain_keys,
    )

    # Inspect metadata for a domain (works for PLANNED too):
    info = get_schema_info("fda_warning_letter")
    required_fields = info.required_fields   # ['issuing_office', ...]
    optional_fields = info.optional_fields   # ['recipient_name', ...]

    # Inspect planned domain metadata (field contract without model):
    cisa_info = get_schema_info("cisa_advisory")  # returns info; status=PLANNED
    print(cisa_info.required_fields)  # ['advisory_id', 'title', ...]

    # Build a fields model (ACTIVE domains only):
    model = build_fields_for_domain("fda_warning_letter", raw_dict)

    # This raises DomainNotImplementedError:
    build_fields_for_domain("cisa_advisory", raw_dict)

Architecture context: ARCHITECTURE.md § Multi-Domain Framework (D-0)
D-1 will implement: cisa_advisory Pydantic schema model and build_fields_model
D-2 will implement: incident_report Pydantic schema model and build_fields_model
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.utils.domain_registry import (
    DomainNotFoundError,
    DomainNotImplementedError,
    DomainStatus,
)


# ---------------------------------------------------------------------------
# Domain schema info descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DomainSchemaInfo:
    """
    Metadata and factory reference for a domain-specific Silver schema family.

    Fields
    ------
    domain_key
        Matches the key in DOMAIN_REGISTRY and _DOMAIN_SCHEMA_REGISTRY.
    required_fields
        Field names that must be non-null for validation_status='valid'.
        Sourced from docs/data-contracts.md § Domain-Specific Extracted Fields.
    optional_fields
        Field names that may be null without causing validation_status='invalid'.
    all_fields
        Union of required + optional (used for coverage calculation).
    build_fields_model
        Factory: (raw_dict: dict) -> Pydantic model instance.
        For ACTIVE domains: constructs and returns the real Pydantic model.
        For PLANNED domains: raises DomainNotImplementedError on call.
    status
        Mirrors DomainStatus from the domain registry.
    notes
        Brief implementation notes (e.g., 'D-1 pending', 'active').
    """

    domain_key: str
    required_fields: list[str]
    optional_fields: list[str]
    all_fields: list[str]
    build_fields_model: Callable[[dict[str, Any]], Any]
    status: DomainStatus
    notes: str = ""


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _build_fda_fields(raw: dict[str, Any]) -> Any:
    """
    Construct FDAWarningLetterFields from a raw extraction dict.
    Filters raw to known FDA fields to prevent unexpected keyword errors.
    """
    from src.schemas.silver_schema import FDA_ALL_FIELDS, FDAWarningLetterFields

    return FDAWarningLetterFields(**{k: v for k, v in raw.items() if k in FDA_ALL_FIELDS})


def _planned_domain_factory(domain_key: str) -> Callable[[dict[str, Any]], Any]:
    """
    Return a factory function that raises DomainNotImplementedError.
    Used for PLANNED domains where the Pydantic model does not yet exist.
    """

    def _factory(raw: dict[str, Any]) -> Any:
        raise DomainNotImplementedError(domain_key, "schema construction")

    return _factory


# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------

_DOMAIN_SCHEMA_REGISTRY: dict[str, DomainSchemaInfo] = {
    # ------------------------------------------------------------------
    # FDA Warning Letter — ACTIVE (V1 + D-0)
    # Full Pydantic model: src/schemas/silver_schema.py FDAWarningLetterFields
    # Fields sourced from docs/data-contracts.md § FDA Warning Letter Fields
    # ------------------------------------------------------------------
    "fda_warning_letter": DomainSchemaInfo(
        domain_key="fda_warning_letter",
        required_fields=[
            "issuing_office",
            "recipient_company",
            "issue_date",
            "violation_type",
            "corrective_action_requested",
        ],
        optional_fields=[
            "recipient_name",
            "cited_regulations",
            "response_deadline_days",
            "product_involved",
            "summary",
        ],
        all_fields=[
            "issuing_office",
            "recipient_company",
            "issue_date",
            "violation_type",
            "corrective_action_requested",
            "recipient_name",
            "cited_regulations",
            "response_deadline_days",
            "product_involved",
            "summary",
        ],
        build_fields_model=_build_fda_fields,
        status=DomainStatus.ACTIVE,
        notes="FDA warning letter schema — V1 active, D-0 framework-registered.",
    ),
    # ------------------------------------------------------------------
    # CISA Advisory — PLANNED (D-1)
    # Field contract defined in docs/data-contracts.md § CISA Advisory Fields.
    # Pydantic model and extractor will be implemented in Phase D-1.
    # build_fields_model raises DomainNotImplementedError until D-1.
    # ------------------------------------------------------------------
    "cisa_advisory": DomainSchemaInfo(
        domain_key="cisa_advisory",
        required_fields=[
            "advisory_id",
            "title",
            "published_date",
            "severity_level",
            "remediation_available",
        ],
        optional_fields=[
            "affected_products",
            "cve_ids",
            "remediation_summary",
            "summary",
        ],
        all_fields=[
            "advisory_id",
            "title",
            "published_date",
            "severity_level",
            "remediation_available",
            "affected_products",
            "cve_ids",
            "remediation_summary",
            "summary",
        ],
        build_fields_model=_planned_domain_factory("cisa_advisory"),
        status=DomainStatus.PLANNED,
        notes=(
            "CISA advisory schema — field contract defined per data-contracts.md; "
            "Pydantic model pending D-1."
        ),
    ),
    # ------------------------------------------------------------------
    # Incident Report — PLANNED (D-2)
    # Field contract defined in docs/data-contracts.md § Incident Report Fields.
    # Pydantic model and extractor will be implemented in Phase D-2.
    # build_fields_model raises DomainNotImplementedError until D-2.
    # ------------------------------------------------------------------
    "incident_report": DomainSchemaInfo(
        domain_key="incident_report",
        required_fields=[
            "incident_date",
            "incident_type",
            "severity",
            "status",
        ],
        optional_fields=[
            "incident_id",
            "affected_systems",
            "root_cause",
            "resolution_summary",
            "reported_by",
        ],
        all_fields=[
            "incident_date",
            "incident_type",
            "severity",
            "status",
            "incident_id",
            "affected_systems",
            "root_cause",
            "resolution_summary",
            "reported_by",
        ],
        build_fields_model=_planned_domain_factory("incident_report"),
        status=DomainStatus.PLANNED,
        notes=(
            "Incident report schema — field contract defined per data-contracts.md; "
            "Pydantic model pending D-2."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Registry accessors
# ---------------------------------------------------------------------------


def get_schema_info(domain_key: str) -> DomainSchemaInfo:
    """
    Return the DomainSchemaInfo for the given domain_key.

    Works for both ACTIVE and PLANNED domains — returns the metadata
    without attempting to construct a model. Useful for introspection,
    coverage calculation scaffolding, and documentation.

    Raises DomainNotFoundError if domain_key is not in the schema registry.
    """
    if domain_key not in _DOMAIN_SCHEMA_REGISTRY:
        raise DomainNotFoundError(domain_key)
    return _DOMAIN_SCHEMA_REGISTRY[domain_key]


def build_fields_for_domain(domain_key: str, raw: dict[str, Any]) -> Any:
    """
    Build the domain-specific extracted fields Pydantic model.

    For ACTIVE domains: constructs and returns the Pydantic fields model.
    For PLANNED domains: raises DomainNotImplementedError.
    For unknown domains: raises DomainNotFoundError.

    This is the single entry point for domain-branched schema construction
    in the Silver extraction pipeline.
    """
    info = get_schema_info(domain_key)
    if info.status != DomainStatus.ACTIVE:
        raise DomainNotImplementedError(domain_key, "schema construction")
    return info.build_fields_model(raw)


def list_schema_domain_keys() -> list[str]:
    """Return all domain keys registered in the schema registry, sorted."""
    return sorted(_DOMAIN_SCHEMA_REGISTRY.keys())
