"""
domain_registry.py — Multi-domain framework registry (Phase D-0)

Formal registry for all document domains in the Databricks CaseOps Lakehouse
pipeline. This is the single authoritative place for domain identity, prompt
references, schema families, routing labels, and implementation status.

D-0 establishes this registry as the architectural foundation for multi-domain
expansion. FDA warning letters are the only fully executable domain after D-0.
CISA advisory and incident report domains are registered as 'planned' — they
have structural presence but are not executable until D-1 / D-2 respectively.

Design principles:
    - One explicit registry dict: DOMAIN_REGISTRY
    - One config type: DomainConfig (NamedTuple — immutable, hashable)
    - One status enum: DomainStatus (ACTIVE | PLANNED)
    - Two error types: DomainNotFoundError (not registered), DomainNotImplementedError (planned)
    - No abstract factories, no plugin machinery, no runtime config loading

Usage:
    from src.utils.domain_registry import (
        DOMAIN_REGISTRY,
        DomainStatus,
        get_domain,
        get_active_domains,
        is_domain_active,
        require_active_domain,
    )

    domain = get_domain("fda_warning_letter")
    assert domain.status == DomainStatus.ACTIVE
    assert domain.extraction_prompt_id == "fda_warning_letter_extract_v1"

    active = get_active_domains()  # [DomainConfig(domain_key='fda_warning_letter', ...)]

    require_active_domain("cisa_advisory")  # raises DomainNotImplementedError (planned)
    require_active_domain("unknown_key")    # raises DomainNotFoundError

Authoritative: this module is the D-0 framework source of truth for domain status.
Architecture context: ARCHITECTURE.md § Multi-Domain Framework (D-0)
"""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple, Optional


# ---------------------------------------------------------------------------
# Domain status
# ---------------------------------------------------------------------------


class DomainStatus(str, Enum):
    """
    Lifecycle status for a registered document domain.

    ACTIVE:  Fully executable — extraction, classification, routing, and
             export are all implemented and validated.
    PLANNED: Registered in the framework but not yet implemented.
             Operations on a PLANNED domain raise DomainNotImplementedError.
    """

    ACTIVE = "active"
    PLANNED = "planned"


# ---------------------------------------------------------------------------
# Domain config
# ---------------------------------------------------------------------------


class DomainConfig(NamedTuple):
    """
    Immutable descriptor for a single document domain in the pipeline.

    Fields
    ------
    domain_key
        Stable string identifier. Matches classification taxonomy document
        type labels (e.g., 'fda_warning_letter').
    document_type_label
        Gold taxonomy label assigned by the classifier for this domain.
    routing_label
        Target downstream Bedrock consumer label for export-ready records
        in this domain.
    source_family
        Broad source category (e.g., 'regulatory', 'security', 'operations').
    extraction_prompt_id
        ID of the ExtractionPrompt registered in extraction_prompts.py.
        None if the domain has no registered prompt yet (planned).
    schema_family
        Identifier used by domain_schema_registry.py to route validation.
        Matches the domain_key for all registered domains in D-0.
    status
        DomainStatus.ACTIVE or DomainStatus.PLANNED.
    description
        Brief, human-readable domain description.
    """

    domain_key: str
    document_type_label: str
    routing_label: str
    source_family: str
    extraction_prompt_id: Optional[str]
    schema_family: str
    status: DomainStatus
    description: str


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------


class DomainNotFoundError(KeyError):
    """
    Raised when a domain key is not registered in DOMAIN_REGISTRY.

    Distinct from DomainNotImplementedError: this means the key is entirely
    unknown, not merely unimplemented.
    """

    def __init__(self, domain_key: str) -> None:
        super().__init__(
            f"Domain '{domain_key}' is not registered in DOMAIN_REGISTRY. "
            f"Registered domains: {sorted(DOMAIN_REGISTRY.keys())}"
        )
        self.domain_key = domain_key


class DomainNotImplementedError(NotImplementedError):
    """
    Raised when an operation requires a domain that is registered as PLANNED.

    Provides an explicit, informative message that distinguishes 'registered
    but not yet implemented' from 'not registered at all'.

    D-1 will resolve this for the 'cisa_advisory' domain.
    D-2 will resolve this for the 'incident_report' domain.
    """

    def __init__(self, domain_key: str, operation: str = "execution") -> None:
        super().__init__(
            f"Domain '{domain_key}' is registered as "
            f"'{DomainStatus.PLANNED.value}' and cannot be used for {operation}. "
            f"This domain will be implemented in a future phase (D-1 or D-2). "
            f"Only domains with status='{DomainStatus.ACTIVE.value}' are currently executable."
        )
        self.domain_key = domain_key
        self.operation = operation


# ---------------------------------------------------------------------------
# Domain registry
# ---------------------------------------------------------------------------

DOMAIN_REGISTRY: dict[str, DomainConfig] = {
    # ------------------------------------------------------------------
    # FDA Warning Letter — ACTIVE (V1 + D-0)
    # The only fully executable domain. Extraction, classification, routing,
    # and export are all implemented and validated through V1 and V2-C.
    # ------------------------------------------------------------------
    "fda_warning_letter": DomainConfig(
        domain_key="fda_warning_letter",
        document_type_label="fda_warning_letter",
        routing_label="regulatory_review",
        source_family="regulatory",
        extraction_prompt_id="fda_warning_letter_extract_v1",
        schema_family="fda_warning_letter",
        status=DomainStatus.ACTIVE,
        description=(
            "FDA-issued warning letter to a regulated company. "
            "V1 fully executable domain. Routing target: regulatory_review."
        ),
    ),
    # ------------------------------------------------------------------
    # CISA Advisory — PLANNED (D-1)
    # Draft schema in docs/data-contracts.md § CISA Advisory Fields.
    # Classification label and routing label are defined in the taxonomy.
    # Full implementation (Pydantic model, prompt, extractor, classifier)
    # is the scope of Phase D-1.
    # ------------------------------------------------------------------
    "cisa_advisory": DomainConfig(
        domain_key="cisa_advisory",
        document_type_label="cisa_advisory",
        routing_label="security_ops",
        source_family="security",
        extraction_prompt_id=None,  # D-1: prompt not yet registered
        schema_family="cisa_advisory",
        status=DomainStatus.PLANNED,
        description=(
            "CISA-issued cybersecurity advisory or bulletin. "
            "Planned for D-1 implementation. Routing target: security_ops."
        ),
    ),
    # ------------------------------------------------------------------
    # Incident Report — PLANNED (D-2)
    # Draft schema in docs/data-contracts.md § Incident Report Fields.
    # Classification label and routing label are defined in the taxonomy.
    # Full implementation is the scope of Phase D-2.
    # ------------------------------------------------------------------
    "incident_report": DomainConfig(
        domain_key="incident_report",
        document_type_label="incident_report",
        routing_label="incident_management",
        source_family="operations",
        extraction_prompt_id=None,  # D-2: prompt not yet registered
        schema_family="incident_report",
        status=DomainStatus.PLANNED,
        description=(
            "Internal or regulatory incident report or post-mortem. "
            "Planned for D-2 implementation. Routing target: incident_management."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Registry accessors
# ---------------------------------------------------------------------------


def get_domain(domain_key: str) -> DomainConfig:
    """
    Return the DomainConfig for the given domain_key.

    Raises DomainNotFoundError if the key is not in the registry.
    Does NOT check whether the domain is active — the caller is responsible
    for checking domain.status before invoking domain-specific operations.
    """
    if domain_key not in DOMAIN_REGISTRY:
        raise DomainNotFoundError(domain_key)
    return DOMAIN_REGISTRY[domain_key]


def get_active_domains() -> list[DomainConfig]:
    """Return all ACTIVE domains, sorted by domain_key."""
    return sorted(
        [d for d in DOMAIN_REGISTRY.values() if d.status == DomainStatus.ACTIVE],
        key=lambda d: d.domain_key,
    )


def get_planned_domains() -> list[DomainConfig]:
    """Return all PLANNED domains, sorted by domain_key."""
    return sorted(
        [d for d in DOMAIN_REGISTRY.values() if d.status == DomainStatus.PLANNED],
        key=lambda d: d.domain_key,
    )


def list_domain_keys() -> list[str]:
    """Return all registered domain keys, sorted."""
    return sorted(DOMAIN_REGISTRY.keys())


def is_domain_active(domain_key: str) -> bool:
    """
    Return True if the domain_key is registered AND status=ACTIVE.
    Returns False if the domain is not registered or is PLANNED.
    """
    if domain_key not in DOMAIN_REGISTRY:
        return False
    return DOMAIN_REGISTRY[domain_key].status == DomainStatus.ACTIVE


def require_active_domain(domain_key: str, operation: str = "execution") -> DomainConfig:
    """
    Return the DomainConfig only if the domain is ACTIVE.

    Raises
    ------
    DomainNotFoundError
        If domain_key is not in the registry.
    DomainNotImplementedError
        If the domain is registered but status=PLANNED.
    """
    domain = get_domain(domain_key)
    if domain.status != DomainStatus.ACTIVE:
        raise DomainNotImplementedError(domain_key, operation)
    return domain
