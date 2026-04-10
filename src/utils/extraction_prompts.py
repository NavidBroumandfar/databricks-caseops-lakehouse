"""
extraction_prompts.py — Extraction prompt templates for Silver pipeline (Phase A-2)

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
# Prompt registry
# ---------------------------------------------------------------------------

_PROMPT_REGISTRY: dict[str, ExtractionPrompt] = {
    FDA_WARNING_LETTER_PROMPT_ID: FDA_WARNING_LETTER_PROMPT,
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
