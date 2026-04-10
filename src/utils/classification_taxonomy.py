"""
classification_taxonomy.py — Gold classification label taxonomy (Phase A-3)

Defines the closed-set document type labels and routing labels used by the
Gold classification stage. All label values in Gold records must come from
the constants defined here.

V1 executable scope: FDA warning letters only.
The full taxonomy lists additional labels for future V2+ domain expansion.
Only labels in V1_EXECUTABLE_DOCUMENT_TYPES and V1_EXECUTABLE_ROUTING_LABELS
are used by the active V1 classification logic.

Authoritative contract: docs/data-contracts.md § Classification Labels
Architecture context: ARCHITECTURE.md § Gold Layer
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

# V1-only executable document type labels
# Classification logic in classify_gold.py only assigns these labels.
V1_EXECUTABLE_DOCUMENT_TYPES: list[str] = [
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
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

# V1-only executable routing labels
# Routing logic in classify_gold.py only assigns these labels.
V1_EXECUTABLE_ROUTING_LABELS: list[str] = [
    ROUTING_LABEL_REGULATORY_REVIEW,
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
    DOCUMENT_TYPE_FDA_WARNING_LETTER: ROUTING_LABEL_REGULATORY_REVIEW,
    # Future V2+ entries — not active:
    # DOCUMENT_TYPE_CISA_ADVISORY:      ROUTING_LABEL_SECURITY_OPS,
    # DOCUMENT_TYPE_INCIDENT_REPORT:    ROUTING_LABEL_INCIDENT_MANAGEMENT,
    # DOCUMENT_TYPE_SOP:                ROUTING_LABEL_KNOWLEDGE_BASE,
    # DOCUMENT_TYPE_QUALITY_AUDIT:      ROUTING_LABEL_QUALITY_MANAGEMENT,
    # DOCUMENT_TYPE_TECHNICAL_CASE:     ROUTING_LABEL_KNOWLEDGE_BASE,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_routing_label(document_type_label: str) -> str:
    """
    Return the routing label for a given document type label.

    Unknown labels and any type not in the V1 routing map fall through to
    quarantine. This makes the routing decision explicit and auditable.
    """
    if document_type_label == DOCUMENT_TYPE_UNKNOWN:
        return ROUTING_LABEL_QUARANTINE
    return V1_ROUTING_MAP.get(document_type_label, ROUTING_LABEL_QUARANTINE)


def is_valid_document_type(label: str) -> bool:
    """Return True if the label is in the full defined taxonomy."""
    return label in ALL_DOCUMENT_TYPE_LABELS


def is_valid_routing_label(label: str) -> bool:
    """Return True if the label is in the full defined taxonomy."""
    return label in ALL_ROUTING_LABELS
