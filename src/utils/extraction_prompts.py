"""
extraction_prompts.py — Extraction prompt templates for Silver pipeline (Phase A-2 / D-0 / D-1 / D-2)

This module contains prompt templates for structured field extraction.
These prompts are designed for future integration with Databricks ai_extract.

IMPORTANT: These prompts do NOT trigger any live LLM call in local execution.
The local extraction baseline in extract_silver.py uses deterministic rule-based
logic, not these prompt templates. The prompts are defined here as a stable,
versioned artifact so that:

1. The Silver record can reference a stable extraction_prompt_id for traceability.
2. The Databricks ai_extract adapter has a ready-to-use prompt when that
   integration is enabled.
3. The prompt design is reviewable and version-controlled alongside the schema.

Prompt ID naming convention: <domain>_extract_v<N>
The ID written into Silver records must match the constant defined here.

D-0 Multi-domain framework additions:
    get_prompt_for_domain(domain_key) routes prompt selection through the domain
    registry. For ACTIVE domains, it returns the registered ExtractionPrompt.
    For PLANNED domains, it raises DomainNotImplementedError.
    FDA behavior is unchanged.

D-1 CISA advisory prompt:
    CISA_ADVISORY_PROMPT_ID and CISA_ADVISORY_PROMPT are now registered.
    get_prompt_for_domain("cisa_advisory") returns the CISA prompt.

D-2 Incident report prompt:
    INCIDENT_REPORT_PROMPT_ID and INCIDENT_REPORT_PROMPT are now registered.
    get_prompt_for_domain("incident_report") returns the incident prompt.
    All three domains are now ACTIVE and prompt-registered.
"""

from __future__ import annotations

from typing import NamedTuple


# ---------------------------------------------------------------------------
# Prompt descriptor
# ---------------------------------------------------------------------------


class ExtractionPrompt(NamedTuple):
    """A versioned extraction prompt with a stable ID."""

    prompt_id: str
    document_domain: str
    description: str
    template: str


# ---------------------------------------------------------------------------
# FDA Warning Letter — V1 extraction prompt
# ---------------------------------------------------------------------------

FDA_WARNING_LETTER_PROMPT_ID = "fda_warning_letter_extract_v1"

FDA_WARNING_LETTER_PROMPT = ExtractionPrompt(
    prompt_id=FDA_WARNING_LETTER_PROMPT_ID,
    document_domain="fda_warning_letter",
    description=(
        "Extract structured fields from an FDA warning letter. "
        "Returns a JSON object with required and optional fields as defined in "
        "the FDA warning letter Silver schema (docs/data-contracts.md)."
    ),
    template="""You are a regulatory document extraction assistant.

Extract the following structured fields from the FDA warning letter text provided below.
Return a valid JSON object. Use null for any field you cannot find in the text.
Do not infer or hallucinate values — extract only what is explicitly stated.

Fields to extract:

REQUIRED:
- issuing_office (string): The FDA office, district, or division that issued the letter.
- recipient_company (string): The company or facility name the letter is addressed to.
- issue_date (string): The date the letter was issued in YYYY-MM-DD format. If only a
  partial date is available, use the closest ISO 8601 representation you can extract.
- violation_type (array of strings): A list of violation category labels cited in the
  letter (e.g., "Failure to Establish Adequate Laboratory Controls",
  "Inadequate Investigation of Out-of-Specification Results").
  Extract one entry per distinct violation heading.
- corrective_action_requested (boolean): true if the letter explicitly requests corrective
  action from the recipient, false if not.

OPTIONAL:
- recipient_name (string): The name of the individual person the letter is addressed to.
- cited_regulations (array of strings): All specific regulation citations mentioned
  (e.g., "21 CFR § 211.160(b)", "21 CFR § 211.192").
- response_deadline_days (integer): The number of working days given to respond, if stated.
- product_involved (string): The primary product or product category named in the violations.
- summary (string): A concise 1–3 sentence summary of the letter's key findings.

Document text:
{parsed_text}

Respond with only the JSON object. Do not include any explanation or preamble.
""",
)


# ---------------------------------------------------------------------------
# CISA Advisory — D-1 extraction prompt
# ---------------------------------------------------------------------------

CISA_ADVISORY_PROMPT_ID = "cisa_advisory_extract_v1"

CISA_ADVISORY_PROMPT = ExtractionPrompt(
    prompt_id=CISA_ADVISORY_PROMPT_ID,
    document_domain="cisa_advisory",
    description=(
        "Extract structured fields from a CISA cybersecurity advisory. "
        "Returns a JSON object with required and optional fields as defined in "
        "the CISA advisory Silver schema (docs/data-contracts.md)."
    ),
    template="""You are a cybersecurity document extraction assistant.

Extract the following structured fields from the CISA advisory text provided below.
Return a valid JSON object. Use null for any field you cannot find in the text.
Do not infer or hallucinate values — extract only what is explicitly stated.

Fields to extract:

REQUIRED:
- advisory_id (string): The CISA advisory identifier (e.g., 'ICSA-24-046-01',
  'AA24-046A', 'CISA-2024-0001'). Look for an ID near the title or in the header.
- title (string): The full title of the advisory as stated in the document.
- published_date (string): The date the advisory was published, in YYYY-MM-DD format.
  Look for 'Release Date:', 'Published:', or a date near the title.
- severity_level (string): Must be exactly one of: 'Critical', 'High', 'Medium', 'Low'.
  Look for an explicit severity rating, CVSS score range (9.0–10.0 = Critical,
  7.0–8.9 = High, 4.0–6.9 = Medium, 0.1–3.9 = Low), or severity classification section.
- remediation_available (boolean): true if the advisory describes available patches,
  mitigations, updates, or workarounds; false if no remediation is available.

OPTIONAL:
- affected_products (array of strings): List of affected vendor/product combinations
  and version ranges explicitly named in the advisory.
- cve_ids (array of strings): All CVE identifiers referenced (e.g., 'CVE-2024-12345').
  Extract the full CVE-YYYY-NNNNN format for each one found.
- remediation_summary (string): A concise description of the recommended mitigation
  steps, patches, or workarounds described in the advisory.
- summary (string): A concise 1–3 sentence summary of the advisory's key findings
  and security impact.

Document text:
{parsed_text}

Respond with only the JSON object. Do not include any explanation or preamble.
""",
)


# ---------------------------------------------------------------------------
# Incident Report — D-2 extraction prompt
# ---------------------------------------------------------------------------

INCIDENT_REPORT_PROMPT_ID = "incident_report_extract_v1"

INCIDENT_REPORT_PROMPT = ExtractionPrompt(
    prompt_id=INCIDENT_REPORT_PROMPT_ID,
    document_domain="incident_report",
    description=(
        "Extract structured fields from an incident report or post-mortem document. "
        "Returns a JSON object with required and optional fields as defined in "
        "the incident report Silver schema (docs/data-contracts.md)."
    ),
    template="""You are an incident management document extraction assistant.

Extract the following structured fields from the incident report text provided below.
Return a valid JSON object. Use null for any field you cannot find in the text.
Do not infer or hallucinate values — extract only what is explicitly stated.

Fields to extract:

REQUIRED:
- incident_date (string): The date the incident occurred in YYYY-MM-DD format.
  Look for 'Date:', 'Incident Date:', 'Occurred:', or a date near the top.
  If only a partial date is available, use the closest ISO 8601 representation.
- incident_type (string): The category or type of incident
  (e.g., 'outage', 'security breach', 'data loss', 'performance degradation',
  'network failure', 'unauthorized access'). Extract the explicit type label
  or the most descriptive phrase used in the document.
- severity (string): The severity level of the incident. Look for an explicit
  severity label such as 'Critical', 'High', 'Medium', or 'Low'. If a P1/P2/P3/P4
  priority system is used, map: P1=Critical, P2=High, P3=Medium, P4=Low.
- status (string): The current status of the incident. Must be exactly one of:
  'open', 'resolved', or 'under_review'. Look for 'Status:', 'Resolution Status:',
  or the overall disposition of the incident in the document.

OPTIONAL:
- incident_id (string): The unique identifier or ticket number for the incident
  (e.g., 'INC-2025-001', 'INC-20250314-001', ticket IDs, or reference numbers).
- affected_systems (array of strings): List of systems, services, or components
  explicitly named as affected by the incident (e.g., ['Payment API', 'Database cluster',
  'CDN edge nodes']). Extract one entry per distinct system or service.
- root_cause (string): The root cause of the incident as stated in the document.
  If root cause analysis is still ongoing, extract any preliminary findings.
- resolution_summary (string): A concise description of how the incident was resolved
  or mitigated. Include the key corrective actions taken.
- reported_by (string): The name, team, or system that first reported or filed the incident.

Document text:
{parsed_text}

Respond with only the JSON object. Do not include any explanation or preamble.
""",
)


# ---------------------------------------------------------------------------
# Prompt registry
# ---------------------------------------------------------------------------

_PROMPT_REGISTRY: dict[str, ExtractionPrompt] = {
    FDA_WARNING_LETTER_PROMPT_ID: FDA_WARNING_LETTER_PROMPT,
    CISA_ADVISORY_PROMPT_ID: CISA_ADVISORY_PROMPT,
    INCIDENT_REPORT_PROMPT_ID: INCIDENT_REPORT_PROMPT,
}


def get_prompt(prompt_id: str) -> ExtractionPrompt:
    """
    Return the ExtractionPrompt for the given prompt_id.
    Raises KeyError if the prompt_id is not registered.
    """
    if prompt_id not in _PROMPT_REGISTRY:
        raise KeyError(
            f"Extraction prompt '{prompt_id}' is not registered. "
            f"Available prompts: {sorted(_PROMPT_REGISTRY.keys())}"
        )
    return _PROMPT_REGISTRY[prompt_id]


def list_prompt_ids() -> list[str]:
    """Return the list of registered prompt IDs."""
    return sorted(_PROMPT_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Domain-aware prompt routing (D-0)
# ---------------------------------------------------------------------------


def get_prompt_for_domain(domain_key: str) -> ExtractionPrompt:
    """
    Return the ExtractionPrompt for the given domain_key via the domain registry.

    For ACTIVE domains with a registered extraction_prompt_id: returns the prompt.
    For PLANNED domains: raises DomainNotImplementedError.
    For unknown domain keys: raises DomainNotFoundError.

    All three implemented domains (fda_warning_letter, cisa_advisory,
    incident_report) are ACTIVE and return their registered prompts.

    FDA warning letter example:
        get_prompt_for_domain("fda_warning_letter")
        # Equivalent to: get_prompt(FDA_WARNING_LETTER_PROMPT_ID)

    This function is the D-0 framework entry point for prompt selection.
    Callers in extract_silver.py should prefer this over hardcoded prompt IDs
    so that adding a new domain in D-1 / D-2 only requires:
      1. Adding the prompt constant below
      2. Registering it in DOMAIN_REGISTRY (extraction_prompt_id field)
      3. No changes needed here

    Raises
    ------
    DomainNotFoundError
        If domain_key is not in DOMAIN_REGISTRY.
    DomainNotImplementedError
        If the domain is PLANNED (no prompt registered yet).
    KeyError
        If the domain is ACTIVE but its extraction_prompt_id is not in
        _PROMPT_REGISTRY (indicates a misconfiguration — both must be kept
        in sync).
    """
    from src.utils.domain_registry import (
        DomainNotImplementedError,
        DomainStatus,
        get_domain,
    )

    domain = get_domain(domain_key)

    if domain.status != DomainStatus.ACTIVE:
        raise DomainNotImplementedError(domain_key, "prompt selection")

    if domain.extraction_prompt_id is None:
        raise DomainNotImplementedError(domain_key, "prompt selection (no prompt_id registered)")

    return get_prompt(domain.extraction_prompt_id)
