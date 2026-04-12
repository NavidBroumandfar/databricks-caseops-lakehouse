"""
extract_silver.py — Silver extraction pipeline (Phase A-2 / D-0 / D-1)

Reads Bronze JSON artifacts produced by ingest_bronze.py, extracts structured
fields into Silver records, validates them against the Silver schema, and writes
one Silver JSON artifact per Bronze input record.

Only Bronze records with parse_status != 'failed' are processed.
Records that produce invalid or partial extractions are still written —
no silent drops. All extraction outcomes are captured as Silver records.

D-0 multi-domain framework:
    select_extractor() uses the domain registry to validate the requested
    domain before dispatching to the extractor. ACTIVE domains are dispatched;
    PLANNED domains (incident_report) raise DomainNotImplementedError.

D-1 CISA advisory domain:
    LocalCISAAdvisoryExtractor implements deterministic rule-based extraction
    for CISA advisory documents. select_extractor() dispatches 'cisa_advisory'
    to this extractor. FDA behavior is unchanged.

Usage:
    # Process all Bronze artifacts in the default directory
    python src/pipelines/extract_silver.py --input-dir output/bronze

    # Process a single Bronze artifact
    python src/pipelines/extract_silver.py --input output/bronze/<record>.json

    # Specify output directory
    python src/pipelines/extract_silver.py --input-dir output/bronze --output-dir output/silver

Outputs:
    output/silver/<extraction_id>.json  — one Silver record per processed Bronze record

Authoritative contract: docs/data-contracts.md § Silver: Extraction Schema Contract
Architecture context: ARCHITECTURE.md § Silver Layer / Multi-Domain Framework (D-0)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from the repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.silver_schema import (
    CISA_ALL_FIELDS,
    CISA_OPTIONAL_FIELDS,
    CISA_REQUIRED_FIELDS,
    CISAAdvisoryFields,
    FDA_ALL_FIELDS,
    FDA_OPTIONAL_FIELDS,
    FDA_REQUIRED_FIELDS,
    FDAWarningLetterFields,
    SCHEMA_VERSION,
    SilverRecord,
    ValidationStatus,
    compute_cisa_field_coverage,
    compute_field_coverage,
)
from src.utils.extraction_prompts import (
    CISA_ADVISORY_PROMPT_ID,
    FDA_WARNING_LETTER_PROMPT_ID,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_INPUT_DIR = "output/bronze"
DEFAULT_OUTPUT_DIR = "output/silver"
LOCAL_PIPELINE_RUN_PREFIX = "local-run"
LOCAL_EXTRACTION_MODEL = "local_rule_extractor/v1"


# ---------------------------------------------------------------------------
# Bronze artifact loader
# ---------------------------------------------------------------------------

def load_bronze_artifact(path: Path) -> dict:
    """Load and return a Bronze record dict from a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def collect_bronze_paths(input_dir: Optional[Path], input_file: Optional[Path]) -> list[Path]:
    """Resolve Bronze artifact paths to process."""
    if input_file is not None:
        if not input_file.exists():
            raise ValueError(f"Input file does not exist: {input_file}")
        return [input_file]
    if input_dir is not None:
        if not input_dir.is_dir():
            raise ValueError(f"Input directory does not exist: {input_dir}")
        paths = sorted(input_dir.glob("*.json"))
        if not paths:
            raise ValueError(f"No JSON artifacts found in: {input_dir}")
        return paths
    raise ValueError("Provide --input or --input-dir.")


def is_eligible_for_extraction(bronze: dict) -> bool:
    """Return True if the Bronze record should proceed to Silver extraction."""
    return bronze.get("parse_status") != "failed"


# ---------------------------------------------------------------------------
# Extractor abstraction
# ---------------------------------------------------------------------------

class FieldExtractor(ABC):
    """
    Strategy interface for structured field extractors.

    Implementations return a dict with raw extracted values keyed by field name.
    The caller is responsible for assembling and validating the Silver record.
    """

    @abstractmethod
    def extract(self, parsed_text: str) -> dict:
        """
        Extract structured fields from parsed document text.

        Returns a dict of {field_name: extracted_value}. Missing fields should
        be omitted or set to None. Never raises on extraction failures —
        incomplete results are valid extraction output.
        """
        ...

    @property
    @abstractmethod
    def model_id(self) -> str:
        """Stable model identifier written into the Silver record."""
        ...


# ---------------------------------------------------------------------------
# Local deterministic FDA warning letter extractor
# ---------------------------------------------------------------------------

class LocalFDAWarningLetterExtractor(FieldExtractor):
    """
    Deterministic rule-based extractor for FDA warning letters.

    Uses regex and simple string parsing against the parsed text.
    No LLM or external service is called. Produces a believable Silver record
    from the sample FDA warning letter fixture.

    This extractor is the active baseline for local-safe execution.
    It is explicitly designed to be readable and auditable — not clever.
    """

    _MODEL_ID = LOCAL_EXTRACTION_MODEL

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def extract(self, parsed_text: str) -> dict:
        return {
            "issuing_office": self._extract_issuing_office(parsed_text),
            "recipient_company": self._extract_recipient_company(parsed_text),
            "recipient_name": self._extract_recipient_name(parsed_text),
            "issue_date": self._extract_issue_date(parsed_text),
            "violation_type": self._extract_violation_types(parsed_text),
            "cited_regulations": self._extract_cited_regulations(parsed_text),
            "corrective_action_requested": self._extract_corrective_action(parsed_text),
            "response_deadline_days": self._extract_response_deadline(parsed_text),
            "product_involved": self._extract_product_involved(parsed_text),
            "summary": self._extract_summary(parsed_text),
        }

    # --- Individual field extractors ---

    def _extract_issuing_office(self, text: str) -> Optional[str]:
        """
        Look for district office or issuing office mention.
        FDA letters typically name the district in the header or signature.
        """
        # Prefer district office from the header section
        patterns = [
            r"District Office[:\s]+([^\n]+)",
            r"([\w\s]+ District(?:\s+Office)?)",
            r"FDA\s+([\w\s]+District(?:\s+Office)?)",
            r"(Office of Regulatory Affairs[^\n]*)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip().rstrip(".,")

        # Fall back: look for "District Director" line and extract office from context
        if "Chicago District" in text:
            return "FDA Chicago District Office"
        if "District Director" in text:
            match = re.search(r"([\w\s]+ District Office)", text, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def _extract_recipient_company(self, text: str) -> Optional[str]:
        """
        Extract the company name from the letter address block.
        FDA letters address an individual at a company; company name follows
        the recipient's name and title lines.
        """
        # Pattern: after a name + title line(s), company name appears
        # Look for "Inc.", "LLC", "Corp.", "Solutions", "Pharmaceuticals", etc.
        company_patterns = [
            r"(?:CEO|Chief Executive Officer|President|Vice President|Director|Manager)\n([^\n]+(?:Inc\.|LLC|Corp\.|Solutions|Pharmaceuticals|Manufacturing|Industries|Company|Co\.|Ltd\.)[^\n]*)",
            r"([A-Z][^\n]+(?:Inc\.|LLC|Corp\.|Solutions|Pharmaceuticals|Manufacturing|Industries|Company|Co\.|Ltd\.))",
        ]
        for pattern in company_patterns:
            match = re.search(pattern, text, re.MULTILINE)
            if match:
                return match.group(1).strip().rstrip(".,")

        return None

    def _extract_recipient_name(self, text: str) -> Optional[str]:
        """
        Extract the recipient individual's name from the address block.
        FDA letters address a named individual, typically before the company.
        The name appears after 'Issued to:' or at the top of the address.
        Handles both plain and Markdown-formatted text (e.g., **Issued to:**).
        """
        # Handle plain or Markdown bold: "Issued to:" or "**Issued to:**"
        issued_to_match = re.search(
            r"\*{0,2}Issued to:\*{0,2}\s*\n+\*{0,2}([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)\*{0,2}",
            text,
            re.MULTILINE,
        )
        if issued_to_match:
            return issued_to_match.group(1).strip()

        return None

    def _extract_issue_date(self, text: str) -> Optional[str]:
        """
        Extract the issue date from the letter.
        FDA letters typically have a 'Date:' line near the top.
        """
        # "Date: March 14, 2025" or "Date: 14 March 2025"
        date_match = re.search(
            r"\*{0,2}Date[:\s*]+\*{0,2}\s*([A-Z][a-z]+ \d{1,2},\s*\d{4}|\d{1,2} [A-Z][a-z]+ \d{4})",
            text,
            re.IGNORECASE,
        )
        if date_match:
            raw_date = date_match.group(1).strip()
            return self._normalize_date(raw_date)

        # Fallback: look for any "Month DD, YYYY" near the top
        top_section = text[:500]
        month_match = re.search(
            r"([A-Z][a-z]+ \d{1,2},\s*\d{4})",
            top_section,
        )
        if month_match:
            return self._normalize_date(month_match.group(1).strip())

        return None

    def _normalize_date(self, raw: str) -> str:
        """
        Best-effort normalization to YYYY-MM-DD. Returns the raw string on failure.
        """
        month_map = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        # "March 14, 2025"
        match = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})", raw)
        if match:
            month_name, day, year = match.groups()
            month_num = month_map.get(month_name.lower())
            if month_num:
                return f"{year}-{month_num}-{int(day):02d}"
        return raw

    def _extract_violation_types(self, text: str) -> Optional[list]:
        """
        Extract violation type labels from section headings.
        FDA warning letters use "Violation N — <Description>" headings.
        """
        # Match "Violation N — <Title>" or "Violation N: <Title>"
        violations = re.findall(
            r"Violation\s+\d+\s*[—\-–:]+\s*([^\n]+)",
            text,
            re.IGNORECASE,
        )
        if violations:
            return [v.strip().rstrip(".,") for v in violations]

        # Fallback: look for CGMP violation language
        if re.search(r"Current Good Manufacturing Practice|CGMP", text, re.IGNORECASE):
            return ["CGMP Violations"]

        return None

    def _extract_cited_regulations(self, text: str) -> Optional[list]:
        """
        Extract all cited CFR regulation references.
        Pattern: "21 CFR § NNN.NNN(x)" or "21 C.F.R. Part NNN"
        """
        found = set()

        # Inline CFR section citations: "21 CFR § 211.160(b)"
        inline = re.findall(r"21\s+CFR\s+§\s+[\d.]+(?:\([a-z]\))?", text)
        for m in inline:
            normalized = re.sub(r"\s+", " ", m.strip())
            found.add(normalized)

        # "Title 21, Code of Federal Regulations (CFR), Parts 210 and 211"
        title_match = re.search(
            r"Title\s+21,\s+Code\s+of\s+Federal\s+Regulations.*?(?:Parts?)\s*([\d, and]+)",
            text,
            re.IGNORECASE,
        )
        if title_match:
            parts_raw = title_match.group(1)
            part_nums = re.findall(r"\d+", parts_raw)
            for pn in part_nums:
                found.add(f"21 CFR Part {pn}")

        return sorted(found) if found else None

    def _extract_corrective_action(self, text: str) -> Optional[bool]:
        """
        Detect whether the letter explicitly requests corrective action.
        """
        corrective_patterns = [
            r"corrective action",
            r"Corrective Action.*?Required",
            r"CAPA",
            r"bring your operations into compliance",
            r"take prompt action to correct",
        ]
        for pattern in corrective_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def _extract_response_deadline(self, text: str) -> Optional[int]:
        """
        Extract the numeric response deadline in working days.
        Pattern: "fifteen (15) working days" or "30 days"
        """
        # "fifteen (15) working days" or "15 working days"
        match = re.search(
            r"(?:[Ww]ithin\s+)?(?:\w+\s+)?\((\d+)\)\s+working\s+days",
            text,
        )
        if match:
            return int(match.group(1))

        # "within N days"
        match = re.search(r"[Ww]ithin\s+(\d+)\s+(?:working\s+)?days", text)
        if match:
            return int(match.group(1))

        return None

    def _extract_product_involved(self, text: str) -> Optional[str]:
        """
        Extract the primary product name from violation descriptions.
        Looks for common pharmaceutical product patterns.
        """
        # Specific lot / product mentions: "Amoxicillin Trihydrate Capsules, 500 mg"
        product_match = re.search(
            r"(?:Lot\s+#[A-Z0-9\-]+\s+\()([^)]+)\)",
            text,
        )
        if product_match:
            return product_match.group(1).strip()

        # Broader: "drug manufacturing" context with product keyword
        drug_match = re.search(
            r"((?:[A-Z][a-z]+\s+){1,4}(?:Capsules|Tablets|Solution|Injection|Cream|Ointment)(?:\s*,\s*\d+\s*mg)?)",
            text,
        )
        if drug_match:
            return drug_match.group(1).strip().rstrip(".,")

        # Generic: "drug products"
        if re.search(r"finished pharmaceuticals|drug product", text, re.IGNORECASE):
            return "Pharmaceutical drug products"

        return None

    def _extract_summary(self, text: str) -> Optional[str]:
        """
        Generate a brief summary from the opening paragraph of the letter body.
        This is a deterministic extraction of the first informational paragraph,
        not a model-generated summary.
        """
        # Find the "Dear ..." section and extract the following paragraph
        dear_match = re.search(r"Dear [^\n]+,\s*\n+(.+?)(?:\n\n|\Z)", text, re.DOTALL)
        if dear_match:
            paragraph = dear_match.group(1).strip()
            # Trim to a reasonable summary length
            sentences = re.split(r"(?<=[.!?])\s+", paragraph)
            summary_sentences = sentences[:3]
            summary = " ".join(summary_sentences).strip()
            if len(summary) > 20:
                return summary

        return None


# ---------------------------------------------------------------------------
# Databricks ai_extract adapter (placeholder)
# ---------------------------------------------------------------------------

class DatabricksAiExtractAdapter(FieldExtractor):
    """
    Adapter placeholder for Databricks ai_extract.

    In a live Databricks execution environment this class would call:
        spark.sql("SELECT ai_extract(parsed_text, :schema)", ...)

    It is intentionally not implemented here because:
    1. There is no Spark session available in a local run.
    2. No credentials or workspace URLs should live in this file.

    To enable Databricks execution, subclass this and inject a SparkSession
    and the extraction schema definition.
    """

    _MODEL_ID = "ai_extract/v1"

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def extract(self, parsed_text: str) -> dict:
        raise NotImplementedError(
            "DatabricksAiExtractAdapter requires a live Databricks runtime. "
            "Use LocalFDAWarningLetterExtractor for local execution, or inject "
            "a SparkSession and override this method for Databricks cluster execution."
        )


# ---------------------------------------------------------------------------
# Local deterministic CISA advisory extractor (D-1)
# ---------------------------------------------------------------------------

class LocalCISAAdvisoryExtractor(FieldExtractor):
    """
    Deterministic rule-based extractor for CISA cybersecurity advisories.

    Uses regex and simple string parsing against the parsed text.
    No LLM or external service is called. Produces a valid Silver record
    from CISA advisory text following the common CISA advisory format.

    CISA advisories typically include:
      - Advisory ID near the title (ICSA-YY-NNN-NN, AA24-NNNA, etc.)
      - "Release Date:" or "Published:" for the date
      - Explicit CVSS score or severity label
      - "Affected Products:" section
      - CVE identifiers in the body
      - "Mitigations:" or "Recommendations:" section

    D-1 implementation. Active in the domain registry.
    """

    _MODEL_ID = "local_rule_extractor/v1"

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def extract(self, parsed_text: str) -> dict:
        return {
            "advisory_id": self._extract_advisory_id(parsed_text),
            "title": self._extract_title(parsed_text),
            "published_date": self._extract_published_date(parsed_text),
            "severity_level": self._extract_severity_level(parsed_text),
            "remediation_available": self._extract_remediation_available(parsed_text),
            "affected_products": self._extract_affected_products(parsed_text),
            "cve_ids": self._extract_cve_ids(parsed_text),
            "remediation_summary": self._extract_remediation_summary(parsed_text),
            "summary": self._extract_summary(parsed_text),
        }

    def _extract_advisory_id(self, text: str) -> Optional[str]:
        """
        Extract the CISA advisory identifier.
        Patterns: ICSA-YY-NNN-NN, AA24-NNNA, CISA-YYYY-NNNN
        """
        patterns = [
            r"\b(ICSA-\d{2}-\d{3}-\d{2}[A-Z]?)\b",
            r"\b(AA\d{2}-\d{3}[A-Z])\b",
            r"\b(CISA-\d{4}-\d{4})\b",
            r"Advisory\s+(?:ID|Number)[:\s]+([A-Z0-9\-]+)",
            r"Alert\s+(?:ID|Code)[:\s]+([A-Z0-9\-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _extract_title(self, text: str) -> Optional[str]:
        """
        Extract the advisory title.
        Often on the first non-blank line or after a 'Title:' label.
        """
        title_match = re.search(r"^(?:#\s+|Title[:\s]+)(.+)$", text, re.MULTILINE)
        if title_match:
            return title_match.group(1).strip().strip("#").strip()

        # First meaningful non-blank line that isn't a date or ID
        for line in text.splitlines():
            stripped = line.strip()
            if stripped and len(stripped) > 10 and not re.match(
                r"^(Release Date|Published|ICSA|AA\d|CVE|https?://)", stripped, re.IGNORECASE
            ):
                # Avoid lines that look like metadata
                if re.search(r"[A-Za-z]{3,}", stripped):
                    return stripped[:200]
        return None

    def _extract_published_date(self, text: str) -> Optional[str]:
        """
        Extract the advisory publication date.
        Patterns: 'Release Date:', 'Published:', or a standalone date near the top.
        """
        date_patterns = [
            r"(?:Release Date|Published|Date)[:\s*]+([A-Z][a-z]+ \d{1,2},?\s*\d{4})",
            r"(?:Release Date|Published|Date)[:\s*]+(\d{1,2}/\d{1,2}/\d{4})",
            r"(?:Release Date|Published|Date)[:\s*]+(\d{4}-\d{2}-\d{2})",
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._normalize_date(match.group(1).strip())

        # Fallback: find a date in the top 500 characters
        top = text[:500]
        month_match = re.search(r"([A-Z][a-z]+ \d{1,2},?\s*\d{4})", top)
        if month_match:
            return self._normalize_date(month_match.group(1).strip())
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", top)
        if iso_match:
            return iso_match.group(1)
        return None

    def _normalize_date(self, raw: str) -> str:
        """Best-effort normalization to YYYY-MM-DD."""
        month_map = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        match = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})", raw)
        if match:
            month_name, day, year = match.groups()
            month_num = month_map.get(month_name.lower())
            if month_num:
                return f"{year}-{month_num}-{int(day):02d}"
        # MM/DD/YYYY
        match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
        if match:
            month, day, year = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"
        return raw

    def _extract_severity_level(self, text: str) -> Optional[str]:
        """
        Extract severity level: Critical / High / Medium / Low.
        Sources: explicit label, CVSS score range, or section heading.
        """
        # Explicit severity label
        explicit_match = re.search(
            r"(?:Severity|Risk Level|Criticality)[:\s]+\*{0,2}(Critical|High|Medium|Low)\b",
            text,
            re.IGNORECASE,
        )
        if explicit_match:
            return explicit_match.group(1).capitalize()

        # CVSS base score
        cvss_match = re.search(r"CVSS(?:\s+v\d)?\s+(?:Base\s+)?Score[:\s]+(\d+\.?\d*)", text, re.IGNORECASE)
        if not cvss_match:
            cvss_match = re.search(r"(?:Base\s+)?Score[:\s]+(\d+\.?\d*)\s*/\s*10", text, re.IGNORECASE)
        if cvss_match:
            try:
                score = float(cvss_match.group(1))
                if score >= 9.0:
                    return "Critical"
                elif score >= 7.0:
                    return "High"
                elif score >= 4.0:
                    return "Medium"
                else:
                    return "Low"
            except ValueError:
                pass

        # Keyword presence fallback
        if re.search(r"\bcritical\b", text[:2000], re.IGNORECASE):
            return "Critical"
        if re.search(r"\bhigh\s+(?:severity|risk|impact)\b", text[:2000], re.IGNORECASE):
            return "High"
        return None

    def _extract_remediation_available(self, text: str) -> Optional[bool]:
        """
        Determine if remediation (patches, mitigations, workarounds) is available.
        """
        # Check for explicit "no fix" phrasing first
        no_fix_patterns = [
            r"no\s+(?:known\s+)?(?:patch|fix|mitigation|workaround)\s+(?:is\s+)?(?:available|exists)",
            r"no\s+known\s+\w+\s+(?:or\s+\w+\s+)?(?:is\s+)?(?:available|exists)",
            r"(?:patch|fix|mitigation)\s+(?:is\s+)?not\s+(?:yet\s+)?available",
        ]
        for pattern in no_fix_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False

        available_patterns = [
            r"patch(?:es)?\s+(?:are\s+)?available",
            r"update(?:s)?\s+(?:are\s+)?available",
            r"mitigation(?:s)?\s+(?:are\s+)?available",
            r"workaround(?:s)?\s+(?:are\s+)?available",
            r"CISA\s+(?:recommends|urges)\s+users?",
            r"vendor\s+has\s+(?:released|issued)\s+(?:a\s+)?(?:patch|update|fix|firmware)",
            r"(?:has\s+)?released\s+(?:firmware|patch|update|fix)",
            r"apply\s+(?:the\s+)?(?:following\s+)?(?:patch|update|mitigation)",
            r"(?:update|upgrade)\s+(?:to\s+)?(?:firmware|version|v)\s*\w",
        ]
        for pattern in available_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True

        # Presence of a Mitigations / Recommendations section implies availability
        if re.search(
            r"(?:^|[\n#\s])(?:Mitigation|Recommendation|Patch|Remediation)s?\s*[:\n]",
            text,
            re.IGNORECASE | re.MULTILINE,
        ):
            return True
        return None

    def _extract_affected_products(self, text: str) -> Optional[list]:
        """
        Extract affected product names and versions.
        Looks for an 'Affected Products:' section listing items.
        """
        section_match = re.search(
            r"Affected\s+Products?[:\s]*\n((?:[-•*]?\s*.+\n?){1,20})",
            text,
            re.IGNORECASE,
        )
        if section_match:
            lines = section_match.group(1).splitlines()
            products = []
            for line in lines:
                cleaned = re.sub(r"^[-•*]\s*", "", line.strip())
                if cleaned and len(cleaned) > 2:
                    products.append(cleaned)
                if len(products) >= 10:
                    break
            if products:
                return products

        # Inline: "affects [Product Name] versions X.X through Y.Y"
        inline_matches = re.findall(
            r"(?:affects?|vulnerable)\s+(?:to\s+)?([A-Z][^\n,;.]{3,50})",
            text,
            re.IGNORECASE,
        )
        if inline_matches:
            return [m.strip() for m in inline_matches[:5]]

        return None

    def _extract_cve_ids(self, text: str) -> Optional[list]:
        """
        Extract all CVE identifiers in CVE-YYYY-NNNNN format.
        """
        found = set(re.findall(r"\bCVE-\d{4}-\d{4,7}\b", text, re.IGNORECASE))
        return sorted(found) if found else None

    def _extract_remediation_summary(self, text: str) -> Optional[str]:
        """
        Extract the key mitigation or remediation recommendation text.
        Looks for Mitigations / Recommendations / Patches sections.
        """
        section_match = re.search(
            r"(?:Mitigation|Recommendation|Patch|Remediation)s?\s*[:\n]\s*\n?((?:.+\n?){1,8})",
            text,
            re.IGNORECASE,
        )
        if section_match:
            body = section_match.group(1).strip()
            # Trim to first 500 characters to keep it concise
            if len(body) > 500:
                body = body[:500].rsplit(" ", 1)[0] + "..."
            return body if len(body) > 10 else None
        return None

    def _extract_summary(self, text: str) -> Optional[str]:
        """
        Generate a brief summary from the executive summary or opening paragraph.
        """
        # Executive summary section
        exec_match = re.search(
            r"(?:Executive\s+)?Summary[:\s]*\n((?:.+\n?){1,5})",
            text,
            re.IGNORECASE,
        )
        if exec_match:
            summary = exec_match.group(1).strip()
            sentences = re.split(r"(?<=[.!?])\s+", summary)
            return " ".join(sentences[:3]).strip() or None

        # First substantive paragraph
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if len(p.strip()) > 50]
        if paragraphs:
            first = paragraphs[0]
            sentences = re.split(r"(?<=[.!?])\s+", first)
            candidate = " ".join(sentences[:3]).strip()
            if len(candidate) > 20:
                return candidate
        return None


# ---------------------------------------------------------------------------
# Extractor selection
# ---------------------------------------------------------------------------

def select_extractor(document_class_hint: Optional[str]) -> FieldExtractor:
    """
    Return the appropriate field extractor for the given document class.

    Domain-registry routing (D-0 framework, D-1 CISA active):
      - None or 'fda_warning_letter' → LocalFDAWarningLetterExtractor (ACTIVE)
      - 'cisa_advisory'              → LocalCISAAdvisoryExtractor (ACTIVE, D-1)
      - 'incident_report'            → DomainNotImplementedError (PLANNED, D-2)
      - Unregistered domain keys     → raises ValueError with registry context

    FDA behavior is preserved exactly. CISA is now the second active domain.
    Incident report remains planned until D-2.
    """
    from src.utils.domain_registry import (
        DOMAIN_REGISTRY,
        DomainNotImplementedError,
        get_active_domains,
        is_domain_active,
    )

    # Treat None as the default FDA domain (backward compatible with V1 callers
    # that do not supply a document_class_hint).
    effective_key = document_class_hint if document_class_hint is not None else "fda_warning_letter"

    # FDA warning letter — ACTIVE, V1 extractor (unchanged)
    if effective_key == "fda_warning_letter":
        return LocalFDAWarningLetterExtractor()

    # CISA advisory — ACTIVE, D-1 extractor
    if effective_key == "cisa_advisory":
        return LocalCISAAdvisoryExtractor()

    # Check registry status for all other keys
    if effective_key not in DOMAIN_REGISTRY:
        raise ValueError(
            f"No extractor registered for document_class_hint '{document_class_hint}'. "
            f"Active domains: {[d.domain_key for d in get_active_domains()]}."
        )

    if not is_domain_active(effective_key):
        # Registered but PLANNED — clear error before D-2 implements it
        raise DomainNotImplementedError(effective_key, "extraction")

    raise ValueError(
        f"Domain '{effective_key}' is registered as ACTIVE in the domain registry "
        "but no extractor has been implemented yet. "
        "Add the extractor class and dispatch case in extract_silver.py."
    )


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def validate_extracted_fields(
    fields: FDAWarningLetterFields,
) -> tuple[ValidationStatus, list[str]]:
    """
    Determine the ValidationStatus for a set of extracted FDA warning letter fields.

    Rules (from docs/data-contracts.md and docs/evaluation-plan.md):
    - valid:   all required fields are non-null and non-empty
    - partial: all required fields present but some optional fields are missing
    - invalid: one or more required fields are null or empty

    Returns (status, list_of_error_messages).
    """
    fields_dict = fields.model_dump()
    errors = []

    for field_name in FDA_REQUIRED_FIELDS:
        value = fields_dict.get(field_name)
        if value is None or value == [] or value == "":
            errors.append(f"Required field '{field_name}' is missing or null.")

    if errors:
        return ValidationStatus.invalid, errors

    optional_missing = [
        f for f in FDA_OPTIONAL_FIELDS
        if fields_dict.get(f) is None or fields_dict.get(f) == [] or fields_dict.get(f) == ""
    ]

    if optional_missing:
        return ValidationStatus.partial, [
            f"Optional field '{f}' is not populated." for f in optional_missing
        ]

    return ValidationStatus.valid, []


def validate_cisa_extracted_fields(
    fields: CISAAdvisoryFields,
) -> tuple[ValidationStatus, list[str]]:
    """
    Determine the ValidationStatus for a set of extracted CISA advisory fields.

    Rules mirror the FDA logic — sourced from docs/data-contracts.md § CISA Advisory Fields:
    - valid:   all required fields are non-null and non-empty
    - partial: required fields present but some optional fields are missing
    - invalid: one or more required fields are null or empty

    Returns (status, list_of_error_messages).
    """
    fields_dict = fields.model_dump()
    errors = []

    for field_name in CISA_REQUIRED_FIELDS:
        value = fields_dict.get(field_name)
        if value is None or value == [] or value == "":
            errors.append(f"Required field '{field_name}' is missing or null.")

    if errors:
        return ValidationStatus.invalid, errors

    optional_missing = [
        f for f in CISA_OPTIONAL_FIELDS
        if fields_dict.get(f) is None or fields_dict.get(f) == [] or fields_dict.get(f) == ""
    ]

    if optional_missing:
        return ValidationStatus.partial, [
            f"Optional field '{f}' is not populated." for f in optional_missing
        ]

    return ValidationStatus.valid, []


# ---------------------------------------------------------------------------
# Silver record assembly
# ---------------------------------------------------------------------------

def assemble_silver_record(
    bronze: dict,
    extracted_raw: dict,
    extraction_model: str,
    pipeline_run_id: str,
    domain_key: str = "fda_warning_letter",
) -> SilverRecord:
    """
    Build a SilverRecord from a Bronze record dict and raw extraction output.

    Handles all three validation outcomes (valid / partial / invalid) and
    always produces a record — no silent drops.

    domain_key controls which field model, coverage calculator, and validator
    are used. Defaults to 'fda_warning_letter' for backward compatibility
    with callers that do not supply a domain_key.

    D-1: 'cisa_advisory' is now a fully supported domain_key.
    """
    extraction_id = str(uuid.uuid4())
    extracted_at = datetime.now(tz=timezone.utc)

    # Resolve domain-specific prompt_id, field builder, coverage, and validator
    if domain_key == "cisa_advisory":
        prompt_id = CISA_ADVISORY_PROMPT_ID
        known_fields = CISA_ALL_FIELDS
        fields_cls = CISAAdvisoryFields
        coverage_fn = compute_cisa_field_coverage
        validate_fn = validate_cisa_extracted_fields
    else:
        # Default: FDA warning letter (also explicit for fda_warning_letter key)
        prompt_id = FDA_WARNING_LETTER_PROMPT_ID
        known_fields = FDA_ALL_FIELDS
        fields_cls = FDAWarningLetterFields
        coverage_fn = compute_field_coverage
        validate_fn = validate_extracted_fields

    # Build the extracted fields model — coerce types where possible
    try:
        fields = fields_cls(**{k: v for k, v in extracted_raw.items() if k in known_fields})
    except Exception as exc:  # noqa: BLE001
        # Field construction itself failed — produce an invalid record
        return SilverRecord(
            document_id=bronze["document_id"],
            bronze_record_id=bronze["bronze_record_id"],
            extraction_id=extraction_id,
            pipeline_run_id=pipeline_run_id,
            extracted_at=extracted_at,
            document_class_hint=bronze.get("document_class_hint"),
            extraction_prompt_id=prompt_id,
            extraction_model=extraction_model,
            extracted_fields=None,
            field_coverage_pct=0.0,
            validation_status=ValidationStatus.invalid,
            validation_errors=[f"Field model construction failed: {exc}"],
            schema_version=SCHEMA_VERSION,
        )

    coverage = coverage_fn(fields)
    status, errors = validate_fn(fields)

    return SilverRecord(
        document_id=bronze["document_id"],
        bronze_record_id=bronze["bronze_record_id"],
        extraction_id=extraction_id,
        pipeline_run_id=pipeline_run_id,
        extracted_at=extracted_at,
        document_class_hint=bronze.get("document_class_hint"),
        extraction_prompt_id=prompt_id,
        extraction_model=extraction_model,
        extracted_fields=fields,
        field_coverage_pct=coverage,
        validation_status=status,
        validation_errors=errors,
        schema_version=SCHEMA_VERSION,
    )


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------

def write_silver_artifact(record: SilverRecord, output_dir: Path) -> Path:
    """
    Write a Silver record as a JSON artifact.
    Each call creates a new file named <extraction_id>.json.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / f"{record.extraction_id}.json"
    artifact_path.write_text(record.to_json_str(), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# Pipeline run ID
# ---------------------------------------------------------------------------

def generate_pipeline_run_id() -> str:
    """Generate a local pipeline run ID. Replaced by MLflow run ID in Databricks."""
    return f"{LOCAL_PIPELINE_RUN_PREFIX}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Top-level pipeline function
# ---------------------------------------------------------------------------

def run_extract_silver(
    input_dir: Optional[str] = None,
    input_file: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    pipeline_run_id: Optional[str] = None,
) -> list[dict]:
    """
    Run the Silver extraction pipeline.

    Reads Bronze JSON artifacts, extracts FDA warning letter fields,
    validates against the Silver schema, and writes Silver artifacts.

    Returns a list of summary dicts (one per processed Bronze record).
    """
    input_dir_path = Path(input_dir) if input_dir else None
    input_file_path = Path(input_file) if input_file else None
    output_dir_path = Path(output_dir)
    run_id = pipeline_run_id or generate_pipeline_run_id()

    bronze_paths = collect_bronze_paths(input_dir_path, input_file_path)
    print(f"[extract_silver] Found {len(bronze_paths)} Bronze artifact(s). Run ID: {run_id}")

    summaries = []

    for bronze_path in bronze_paths:
        bronze = load_bronze_artifact(bronze_path)

        if not is_eligible_for_extraction(bronze):
            print(
                f"[extract_silver] Skipping {bronze_path.name} "
                f"(parse_status={bronze.get('parse_status')})"
            )
            continue

        parsed_text = bronze.get("parsed_text") or ""
        document_class_hint = bronze.get("document_class_hint")

        extractor = select_extractor(document_class_hint)

        # Resolve effective domain key for assembly (None defaults to fda_warning_letter)
        effective_domain_key = document_class_hint or "fda_warning_letter"

        try:
            extracted_raw = extractor.extract(parsed_text)
        except NotImplementedError as exc:
            print(f"[extract_silver] Extractor not available: {exc}", file=sys.stderr)
            extracted_raw = {}
        except Exception as exc:  # noqa: BLE001
            print(f"[extract_silver] Extraction error for {bronze_path.name}: {exc}", file=sys.stderr)
            extracted_raw = {}

        record = assemble_silver_record(
            bronze=bronze,
            extracted_raw=extracted_raw,
            extraction_model=extractor.model_id,
            pipeline_run_id=run_id,
            domain_key=effective_domain_key,
        )

        artifact_path = write_silver_artifact(record, output_dir_path)

        summary = {
            "bronze_record_id": record.bronze_record_id,
            "document_id": record.document_id,
            "extraction_id": record.extraction_id,
            "validation_status": record.validation_status.value,
            "field_coverage_pct": record.field_coverage_pct,
            "pipeline_run_id": run_id,
            "artifact_path": str(artifact_path),
        }
        summaries.append(summary)

        print(
            f"[extract_silver] Silver artifact written → {artifact_path} "
            f"(status={record.validation_status.value}, coverage={record.field_coverage_pct:.0%})"
        )

    print(f"[extract_silver] Done. {len(summaries)} Silver artifact(s) written.")
    return summaries


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="extract_silver",
        description=(
            "Extract structured fields from Bronze JSON artifacts into Silver records. "
            "Processes FDA warning letters using local rule-based extraction. "
            "Only records with parse_status != 'failed' are processed."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing Bronze JSON artifacts to process.",
    )
    group.add_argument(
        "--input",
        metavar="FILE",
        help="Single Bronze JSON artifact file to process.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory to write Silver JSON artifacts. Default: {DEFAULT_OUTPUT_DIR}",
    )
    p.add_argument(
        "--pipeline-run-id",
        default=None,
        metavar="RUN_ID",
        help="Optional pipeline run ID. Auto-generated if not provided.",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_extract_silver(
        input_dir=args.input_dir,
        input_file=args.input,
        output_dir=args.output_dir,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    main()
