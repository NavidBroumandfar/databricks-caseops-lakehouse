"""
classification_taxonomy.py — Gold classification label taxonomy (Phase A-3 / D-0 / D-1 / D-2)

Defines the closed-set document type labels and routing labels used by the
Gold classification stage. All label values in Gold records must come from
the constants defined here.

D-0 Multi-domain framework additions:
    DOMAIN_ROUTING_MAP is the declarative per-domain routing map covering all
    registered domains. Active domains are mapped to their routing labels.
    Planned domains are guarded by their domain status.

    resolve_routing_label_for_domain(domain_key, document_type_label) provides
    domain-aware routing. is_domain_executable(domain_key) checks registry status.

D-1 CISA advisory activation:
    'cisa_advisory' → 'security_ops' is now active in V1_ROUTING_MAP.
    ROUTING_LABEL_SECURITY_OPS is now an active routing path.
    V1_EXECUTABLE_DOCUMENT_TYPES and V1_EXECUTABLE_ROUTING_LABELS updated.
    FDA behavior is unchanged.

D-2 Incident report activation:
    'incident_report' → 'incident_management' is now active in V1_ROUTING_MAP.
    ROUTING_LABEL_INCIDENT_MANAGEMENT is now an active routing path.
    V1_EXECUTABLE_DOCUMENT_TYPES and V1_EXECUTABLE_ROUTING_LABELS updated.
    FDA and CISA behavior is unchanged.

Authoritative contract: docs/data-contracts.md § Classification Labels
Domain registry: src/utils/domain_registry.py
Architecture context: ARCHITECTURE.md § Gold Layer / Multi-Domain Framework (D-0)
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Document type labels — full taxonomy (V1 + planned V2+)
# ---------------------------------------------------------------------------

# Active V1 labels
DOCUMENT_TYPE_FDA_WARNING_LETTER = "fda_warning_letter"

# Planned V2+ labels (not active in V1 execution)
DOCUMENT_TYPE_CISA_ADVISORY = "cisa_advisory"
DOCUMENT_TYPE_INCIDENT_REPORT = "incident_report"
DOCUMENT_TYPE_SOP = "standard_operating_procedure"
DOCUMENT_TYPE_QUALITY_AUDIT = "quality_audit_record"
DOCUMENT_TYPE_TECHNICAL_CASE = "technical_case_record"

# Fallback for unrecognized documents
DOCUMENT_TYPE_UNKNOWN = "unknown"

# All defined document type labels (for validation and reporting)
ALL_DOCUMENT_TYPE_LABELS: list[str] = [
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_CISA_ADVISORY,
    DOCUMENT_TYPE_INCIDENT_REPORT,
    DOCUMENT_TYPE_SOP,
    DOCUMENT_TYPE_QUALITY_AUDIT,
    DOCUMENT_TYPE_TECHNICAL_CASE,
    DOCUMENT_TYPE_UNKNOWN,
]

# Executable document type labels (V1 FDA + D-1 CISA + D-2 Incident)
# Classification logic in classify_gold.py assigns these labels.
V1_EXECUTABLE_DOCUMENT_TYPES: list[str] = [
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_CISA_ADVISORY,    # D-1 active
    DOCUMENT_TYPE_INCIDENT_REPORT,  # D-2 active
    DOCUMENT_TYPE_UNKNOWN,
]


# ---------------------------------------------------------------------------
# Routing labels — full taxonomy (V1 + planned V2+)
# ---------------------------------------------------------------------------

# Active V1 labels
ROUTING_LABEL_REGULATORY_REVIEW = "regulatory_review"
ROUTING_LABEL_QUARANTINE = "quarantine"

# Planned V2+ labels (not active in V1 execution)
ROUTING_LABEL_SECURITY_OPS = "security_ops"
ROUTING_LABEL_INCIDENT_MANAGEMENT = "incident_management"
ROUTING_LABEL_QUALITY_MANAGEMENT = "quality_management"
ROUTING_LABEL_KNOWLEDGE_BASE = "knowledge_base"

# All defined routing labels (for validation and reporting)
ALL_ROUTING_LABELS: list[str] = [
    ROUTING_LABEL_REGULATORY_REVIEW,
    ROUTING_LABEL_QUARANTINE,
    ROUTING_LABEL_SECURITY_OPS,
    ROUTING_LABEL_INCIDENT_MANAGEMENT,
    ROUTING_LABEL_QUALITY_MANAGEMENT,
    ROUTING_LABEL_KNOWLEDGE_BASE,
]

# Active routing labels (V1 regulatory_review + D-1 security_ops + D-2 incident_management + quarantine)
# Routing logic in classify_gold.py assigns these labels.
V1_EXECUTABLE_ROUTING_LABELS: list[str] = [
    ROUTING_LABEL_REGULATORY_REVIEW,
    ROUTING_LABEL_SECURITY_OPS,        # D-1 active
    ROUTING_LABEL_INCIDENT_MANAGEMENT, # D-2 active
    ROUTING_LABEL_QUARANTINE,
]


# ---------------------------------------------------------------------------
# V1 routing map — document type → routing label
#
# This explicit mapping is the source of truth for routing decisions in V1.
# For unknown or quarantine-bound documents, ROUTING_LABEL_QUARANTINE applies
# regardless of this map.
# ---------------------------------------------------------------------------

V1_ROUTING_MAP: dict[str, str] = {
    # V1 active
    DOCUMENT_TYPE_FDA_WARNING_LETTER: ROUTING_LABEL_REGULATORY_REVIEW,
    # D-1 active
    DOCUMENT_TYPE_CISA_ADVISORY: ROUTING_LABEL_SECURITY_OPS,
    # D-2 active
    DOCUMENT_TYPE_INCIDENT_REPORT: ROUTING_LABEL_INCIDENT_MANAGEMENT,
    # Future planned entries (not yet active):
    # DOCUMENT_TYPE_SOP:                ROUTING_LABEL_KNOWLEDGE_BASE,
    # DOCUMENT_TYPE_QUALITY_AUDIT:      ROUTING_LABEL_QUALITY_MANAGEMENT,
    # DOCUMENT_TYPE_TECHNICAL_CASE:     ROUTING_LABEL_KNOWLEDGE_BASE,
}


# ---------------------------------------------------------------------------
# D-0: Domain routing map — full registry-aligned mapping
#
# DOMAIN_ROUTING_MAP declares the intended document_type_label → routing_label
# mapping for ALL registered domains. Active domains produce real routing;
# planned domains are present structurally but not yet executable.
#
# This replaces the commented-out V1_ROUTING_MAP entries with explicit
# declarations. The map is consulted by resolve_routing_label_for_domain().
# V1 behavior (V1_ROUTING_MAP) is unchanged and preserved above.
# ---------------------------------------------------------------------------

DOMAIN_ROUTING_MAP: dict[str, str] = {
    # ACTIVE — fully executable
    DOCUMENT_TYPE_FDA_WARNING_LETTER: ROUTING_LABEL_REGULATORY_REVIEW,
    DOCUMENT_TYPE_CISA_ADVISORY: ROUTING_LABEL_SECURITY_OPS,      # D-1 active
    DOCUMENT_TYPE_INCIDENT_REPORT: ROUTING_LABEL_INCIDENT_MANAGEMENT,  # D-2 active
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_routing_label(document_type_label: str) -> str:
    """
    Return the routing label for a given document type label.

    Unknown labels and any type not in the V1 routing map fall through to
    quarantine. This makes the routing decision explicit and auditable.

    Preserved from V1 for backward compatibility. Callers in the Gold pipeline
    continue to use this function — FDA behavior is unchanged.
    """
    if document_type_label == DOCUMENT_TYPE_UNKNOWN:
        return ROUTING_LABEL_QUARANTINE
    return V1_ROUTING_MAP.get(document_type_label, ROUTING_LABEL_QUARANTINE)


def resolve_routing_label_for_domain(domain_key: str, document_type_label: str) -> str:
    """
    D-0: Domain-aware routing label resolution.

    For ACTIVE domains: returns the routing label from DOMAIN_ROUTING_MAP.
    For PLANNED domains: raises DomainNotImplementedError (not yet executable).
    For unknown domains: raises DomainNotFoundError.
    For the 'unknown' document_type_label: always returns ROUTING_LABEL_QUARANTINE.

    This is the D-0 replacement for select_routing_label() in new callers.
    Existing callers using resolve_routing_label() are unaffected.
    """
    from src.utils.domain_registry import (
        DomainNotImplementedError,
        DomainStatus,
        get_domain,
    )

    if document_type_label == DOCUMENT_TYPE_UNKNOWN:
        return ROUTING_LABEL_QUARANTINE

    domain = get_domain(domain_key)
    if domain.status != DomainStatus.ACTIVE:
        raise DomainNotImplementedError(domain_key, "routing")

    return DOMAIN_ROUTING_MAP.get(document_type_label, ROUTING_LABEL_QUARANTINE)


def is_domain_executable(domain_key: str) -> bool:
    """
    D-0: Return True if the domain_key is registered and ACTIVE.

    Used by pipeline code to guard domain-specific execution paths.
    Returns False for PLANNED domains and unregistered keys.
    Delegates to domain_registry.is_domain_active() to avoid duplication.
    """
    from src.utils.domain_registry import is_domain_active

    return is_domain_active(domain_key)


def is_valid_document_type(label: str) -> bool:
    """Return True if the label is in the full defined taxonomy."""
    return label in ALL_DOCUMENT_TYPE_LABELS


def is_valid_routing_label(label: str) -> bool:
    """Return True if the label is in the full defined taxonomy."""
    return label in ALL_ROUTING_LABELS
