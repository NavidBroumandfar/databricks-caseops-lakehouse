"""
tests/test_d1_cisa_domain.py — Phase D-1 CISA advisory domain test suite

Updated for D-2: incident_report is now ACTIVE. All three domains are executable.
Tests that previously asserted incident_report is PLANNED are updated to reflect
the D-2 state (incident_report ACTIVE, fully routable, no planned domains remain).

Coverage:
  1. Domain registry: CISA is ACTIVE, incident_report is now also ACTIVE (D-2)
  2. Prompt routing: CISA prompt selection via get_prompt_for_domain
  3. Schema: CISAAdvisoryFields validation — valid, partial, invalid
  4. Schema registry: CISA build_fields_for_domain works; incident also works (D-2)
  5. Extraction: LocalCISAAdvisoryExtractor field extraction behavior
  6. Silver assembly: CISA Silver record valid, partial, invalid paths
  7. Classification: LocalCISAAdvisoryClassifier signal detection and confidence
  8. Taxonomy: routing label and domain executable flags
  9. Bedrock contract: CISA export payload validation
 10. Gold pipeline: CISA routing resolves to security_ops
 11. FDA regression: FDA domain behavior is unchanged throughout
 12. D-2 confirmation: incident_report is now ACTIVE and executable

Authoritative contracts: docs/data-contracts.md, ARCHITECTURE.md § Multi-Domain Framework
Phase: D-1 (CISA advisory domain activation), updated for D-2
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Allow running from the repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ---------------------------------------------------------------------------
# Domain registry tests
# ---------------------------------------------------------------------------


class TestDomainRegistry:
    """D-1: CISA is ACTIVE in the domain registry. Incident remains PLANNED."""

    def test_cisa_is_active(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY, DomainStatus
        assert DOMAIN_REGISTRY["cisa_advisory"].status == DomainStatus.ACTIVE

    def test_cisa_has_prompt_id(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY
        assert DOMAIN_REGISTRY["cisa_advisory"].extraction_prompt_id == "cisa_advisory_extract_v1"

    def test_cisa_routing_label(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY
        assert DOMAIN_REGISTRY["cisa_advisory"].routing_label == "security_ops"

    def test_cisa_document_type_label(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY
        assert DOMAIN_REGISTRY["cisa_advisory"].document_type_label == "cisa_advisory"

    def test_cisa_source_family(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY
        assert DOMAIN_REGISTRY["cisa_advisory"].source_family == "security"

    def test_incident_now_active_after_d2(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY, DomainStatus
        assert DOMAIN_REGISTRY["incident_report"].status == DomainStatus.ACTIVE

    def test_fda_still_active(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY, DomainStatus
        assert DOMAIN_REGISTRY["fda_warning_letter"].status == DomainStatus.ACTIVE

    def test_is_domain_active_cisa(self):
        from src.utils.domain_registry import is_domain_active
        assert is_domain_active("cisa_advisory") is True

    def test_is_domain_active_incident_true_after_d2(self):
        from src.utils.domain_registry import is_domain_active
        assert is_domain_active("incident_report") is True

    def test_require_active_domain_cisa_returns_config(self):
        from src.utils.domain_registry import require_active_domain
        config = require_active_domain("cisa_advisory")
        assert config.domain_key == "cisa_advisory"

    def test_require_active_domain_incident_succeeds_after_d2(self):
        from src.utils.domain_registry import DomainStatus, require_active_domain
        domain = require_active_domain("incident_report")
        assert domain.status == DomainStatus.ACTIVE

    def test_get_active_domains_includes_cisa(self):
        from src.utils.domain_registry import get_active_domains
        keys = [d.domain_key for d in get_active_domains()]
        assert "cisa_advisory" in keys

    def test_get_active_domains_includes_fda(self):
        from src.utils.domain_registry import get_active_domains
        keys = [d.domain_key for d in get_active_domains()]
        assert "fda_warning_letter" in keys

    def test_get_active_domains_includes_incident_after_d2(self):
        from src.utils.domain_registry import get_active_domains
        keys = [d.domain_key for d in get_active_domains()]
        assert "incident_report" in keys

    def test_get_planned_domains_empty_after_d2(self):
        from src.utils.domain_registry import get_planned_domains
        keys = [d.domain_key for d in get_planned_domains()]
        # D-2: all three domains are ACTIVE — no planned domains remain
        assert len(keys) == 0
        assert "incident_report" not in keys
        assert "cisa_advisory" not in keys
        assert "fda_warning_letter" not in keys


# ---------------------------------------------------------------------------
# Prompt routing tests
# ---------------------------------------------------------------------------


class TestCISAPromptRouting:
    """D-1: CISA prompt is registered and routable."""

    def test_cisa_prompt_id_constant(self):
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT_ID
        assert CISA_ADVISORY_PROMPT_ID == "cisa_advisory_extract_v1"

    def test_cisa_prompt_registered(self):
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT_ID, get_prompt, list_prompt_ids
        assert CISA_ADVISORY_PROMPT_ID in list_prompt_ids()
        prompt = get_prompt(CISA_ADVISORY_PROMPT_ID)
        assert prompt.prompt_id == CISA_ADVISORY_PROMPT_ID

    def test_cisa_prompt_domain(self):
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT
        assert CISA_ADVISORY_PROMPT.document_domain == "cisa_advisory"

    def test_cisa_prompt_has_template(self):
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT
        assert "{parsed_text}" in CISA_ADVISORY_PROMPT.template
        assert len(CISA_ADVISORY_PROMPT.template) > 100

    def test_get_prompt_for_domain_cisa(self):
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT_ID, get_prompt_for_domain
        prompt = get_prompt_for_domain("cisa_advisory")
        assert prompt.prompt_id == CISA_ADVISORY_PROMPT_ID

    def test_get_prompt_for_domain_fda_unchanged(self):
        from src.utils.extraction_prompts import FDA_WARNING_LETTER_PROMPT_ID, get_prompt_for_domain
        prompt = get_prompt_for_domain("fda_warning_letter")
        assert prompt.prompt_id == FDA_WARNING_LETTER_PROMPT_ID

    def test_get_prompt_for_domain_incident_works_after_d2(self):
        from src.utils.extraction_prompts import get_prompt_for_domain
        prompt = get_prompt_for_domain("incident_report")
        assert prompt.document_domain == "incident_report"


# ---------------------------------------------------------------------------
# Schema: CISAAdvisoryFields
# ---------------------------------------------------------------------------


class TestCISAAdvisoryFieldsSchema:
    """D-1: CISAAdvisoryFields Pydantic model validates correctly."""

    def _full_fields(self):
        from src.schemas.silver_schema import CISAAdvisoryFields
        return CISAAdvisoryFields(
            advisory_id="ICSA-24-046-01",
            title="Siemens SCALANCE Vulnerabilities",
            published_date="2024-02-15",
            severity_level="Critical",
            remediation_available=True,
            affected_products=["Siemens SCALANCE W700"],
            cve_ids=["CVE-2024-23814", "CVE-2024-23815"],
            remediation_summary="Apply firmware V6.5.0 or later.",
            summary="Multiple critical vulnerabilities in Siemens SCALANCE W products.",
        )

    def test_full_fields_constructs(self):
        fields = self._full_fields()
        assert fields.advisory_id == "ICSA-24-046-01"
        assert fields.severity_level == "Critical"
        assert fields.remediation_available is True
        assert len(fields.cve_ids) == 2

    def test_required_only_constructs(self):
        from src.schemas.silver_schema import CISAAdvisoryFields
        fields = CISAAdvisoryFields(
            advisory_id="AA24-046A",
            title="Test Advisory",
            published_date="2024-01-01",
            severity_level="High",
            remediation_available=False,
        )
        assert fields.affected_products is None
        assert fields.cve_ids is None

    def test_all_null_constructs(self):
        from src.schemas.silver_schema import CISAAdvisoryFields
        fields = CISAAdvisoryFields()
        assert fields.advisory_id is None

    def test_model_dump_produces_dict(self):
        fields = self._full_fields()
        d = fields.model_dump()
        assert isinstance(d, dict)
        assert d["advisory_id"] == "ICSA-24-046-01"
        assert d["cve_ids"] == ["CVE-2024-23814", "CVE-2024-23815"]

    def test_severity_level_stored_as_string(self):
        fields = self._full_fields()
        assert isinstance(fields.severity_level, str)

    def test_cve_ids_is_list(self):
        fields = self._full_fields()
        assert isinstance(fields.cve_ids, list)

    def test_affected_products_is_list(self):
        fields = self._full_fields()
        assert isinstance(fields.affected_products, list)

    def test_cisa_required_fields_constant(self):
        from src.schemas.silver_schema import CISA_REQUIRED_FIELDS
        assert "advisory_id" in CISA_REQUIRED_FIELDS
        assert "title" in CISA_REQUIRED_FIELDS
        assert "published_date" in CISA_REQUIRED_FIELDS
        assert "severity_level" in CISA_REQUIRED_FIELDS
        assert "remediation_available" in CISA_REQUIRED_FIELDS

    def test_cisa_optional_fields_constant(self):
        from src.schemas.silver_schema import CISA_OPTIONAL_FIELDS
        assert "affected_products" in CISA_OPTIONAL_FIELDS
        assert "cve_ids" in CISA_OPTIONAL_FIELDS
        assert "remediation_summary" in CISA_OPTIONAL_FIELDS
        assert "summary" in CISA_OPTIONAL_FIELDS

    def test_cisa_all_fields_union(self):
        from src.schemas.silver_schema import CISA_ALL_FIELDS, CISA_OPTIONAL_FIELDS, CISA_REQUIRED_FIELDS
        assert set(CISA_ALL_FIELDS) == set(CISA_REQUIRED_FIELDS) | set(CISA_OPTIONAL_FIELDS)

    def test_compute_cisa_field_coverage_full(self):
        from src.schemas.silver_schema import compute_cisa_field_coverage
        fields = self._full_fields()
        coverage = compute_cisa_field_coverage(fields)
        assert coverage == 1.0

    def test_compute_cisa_field_coverage_partial(self):
        from src.schemas.silver_schema import CISAAdvisoryFields, compute_cisa_field_coverage
        fields = CISAAdvisoryFields(
            advisory_id="AA24-001A",
            title="Partial Advisory",
            published_date="2024-01-01",
            severity_level="Medium",
            remediation_available=True,
        )
        coverage = compute_cisa_field_coverage(fields)
        assert 0.0 < coverage < 1.0

    def test_compute_cisa_field_coverage_empty(self):
        from src.schemas.silver_schema import CISAAdvisoryFields, compute_cisa_field_coverage
        fields = CISAAdvisoryFields()
        coverage = compute_cisa_field_coverage(fields)
        assert coverage == 0.0


# ---------------------------------------------------------------------------
# Domain schema registry tests
# ---------------------------------------------------------------------------


class TestDomainSchemaRegistry:
    """D-1: CISA schema registry entry is ACTIVE and build_fields_for_domain works."""

    def test_cisa_schema_info_active(self):
        from src.schemas.domain_schema_registry import get_schema_info
        from src.utils.domain_registry import DomainStatus
        info = get_schema_info("cisa_advisory")
        assert info.status == DomainStatus.ACTIVE

    def test_cisa_schema_info_required_fields(self):
        from src.schemas.domain_schema_registry import get_schema_info
        info = get_schema_info("cisa_advisory")
        assert "advisory_id" in info.required_fields
        assert "remediation_available" in info.required_fields

    def test_cisa_schema_info_optional_fields(self):
        from src.schemas.domain_schema_registry import get_schema_info
        info = get_schema_info("cisa_advisory")
        assert "cve_ids" in info.optional_fields
        assert "affected_products" in info.optional_fields

    def test_build_fields_for_domain_cisa(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import CISAAdvisoryFields
        raw = {
            "advisory_id": "ICSA-24-001-01",
            "title": "Test Advisory",
            "published_date": "2024-01-01",
            "severity_level": "High",
            "remediation_available": True,
            "cve_ids": ["CVE-2024-0001"],
        }
        model = build_fields_for_domain("cisa_advisory", raw)
        assert isinstance(model, CISAAdvisoryFields)
        assert model.advisory_id == "ICSA-24-001-01"
        assert model.cve_ids == ["CVE-2024-0001"]

    def test_build_fields_for_domain_cisa_ignores_unknown_keys(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        raw = {
            "advisory_id": "AA24-001A",
            "title": "Test",
            "published_date": "2024-01-01",
            "severity_level": "Low",
            "remediation_available": False,
            "some_unknown_field": "ignored",
        }
        model = build_fields_for_domain("cisa_advisory", raw)
        assert model.advisory_id == "AA24-001A"

    def test_build_fields_for_domain_incident_works_after_d2(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import IncidentReportFields
        model = build_fields_for_domain("incident_report", {})
        assert isinstance(model, IncidentReportFields)

    def test_build_fields_for_domain_fda_still_works(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import FDAWarningLetterFields
        raw = {"issuing_office": "FDA Chicago", "recipient_company": "Acme Pharma"}
        model = build_fields_for_domain("fda_warning_letter", raw)
        assert isinstance(model, FDAWarningLetterFields)


# ---------------------------------------------------------------------------
# LocalCISAAdvisoryExtractor tests
# ---------------------------------------------------------------------------


SAMPLE_CISA_TEXT = """
# ICSA-24-046-01: Siemens SCALANCE W Products Vulnerabilities

Release Date: February 15, 2024

Severity: Critical

CVSS v3 Base Score: 9.8 / 10.0

## Summary

CISA is aware of multiple critical vulnerabilities in Siemens SCALANCE W-700 and W-1700 series
industrial wireless access points. Successful exploitation could allow an unauthenticated remote
attacker to execute arbitrary code or cause a denial-of-service condition.

## Affected Products

- Siemens SCALANCE W700 Series (all versions prior to V6.5.0)
- Siemens SCALANCE W1700 Series (all versions prior to V2.0.1)

CVE-2024-23814
CVE-2024-23815

## Mitigations

Siemens has released firmware updates to address these vulnerabilities. CISA recommends users
apply patches to all affected SCALANCE W devices as soon as possible.
""".strip()


class TestLocalCISAAdvisoryExtractor:
    """D-1: LocalCISAAdvisoryExtractor produces expected field values."""

    def setup_method(self):
        from src.pipelines.extract_silver import LocalCISAAdvisoryExtractor
        self.extractor = LocalCISAAdvisoryExtractor()

    def test_model_id(self):
        assert self.extractor.model_id == "local_rule_extractor/v1"

    def test_extract_returns_dict(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert isinstance(result, dict)

    def test_extract_advisory_id(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["advisory_id"] == "ICSA-24-046-01"

    def test_extract_published_date(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["published_date"] == "2024-02-15"

    def test_extract_severity_level_from_label(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["severity_level"] == "Critical"

    def test_extract_severity_level_from_cvss(self):
        from src.pipelines.extract_silver import LocalCISAAdvisoryExtractor
        extractor = LocalCISAAdvisoryExtractor()
        text = "CVSS v3 Score: 8.1\nSome vulnerability description."
        result = extractor.extract(text)
        assert result["severity_level"] == "High"

    def test_extract_remediation_available_true(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["remediation_available"] is True

    def test_extract_remediation_available_false(self):
        from src.pipelines.extract_silver import LocalCISAAdvisoryExtractor
        extractor = LocalCISAAdvisoryExtractor()
        text = "No known patch or mitigation is available for this vulnerability."
        result = extractor.extract(text)
        assert result["remediation_available"] is False

    def test_extract_cve_ids(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["cve_ids"] is not None
        assert "CVE-2024-23814" in result["cve_ids"]
        assert "CVE-2024-23815" in result["cve_ids"]

    def test_extract_affected_products(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["affected_products"] is not None
        assert len(result["affected_products"]) >= 1

    def test_extract_remediation_summary(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["remediation_summary"] is not None
        assert len(result["remediation_summary"]) > 10

    def test_extract_summary(self):
        result = self.extractor.extract(SAMPLE_CISA_TEXT)
        assert result["summary"] is not None
        assert len(result["summary"]) > 20

    def test_extract_empty_text_returns_nulls(self):
        result = self.extractor.extract("")
        assert result["advisory_id"] is None
        assert result["published_date"] is None
        assert result["severity_level"] is None

    def test_extract_cve_deduplication(self):
        from src.pipelines.extract_silver import LocalCISAAdvisoryExtractor
        extractor = LocalCISAAdvisoryExtractor()
        text = "See CVE-2024-1234 and CVE-2024-1234 for details. Also CVE-2024-5678."
        result = extractor.extract(text)
        cves = result["cve_ids"]
        assert cves is not None
        assert len(cves) == len(set(cves))


# ---------------------------------------------------------------------------
# CISA validation logic tests
# ---------------------------------------------------------------------------


class TestValidateCISAExtractedFields:
    """D-1: validate_cisa_extracted_fields produces correct status/errors."""

    def _make_full_fields(self):
        from src.schemas.silver_schema import CISAAdvisoryFields
        return CISAAdvisoryFields(
            advisory_id="ICSA-24-046-01",
            title="Test Advisory",
            published_date="2024-02-15",
            severity_level="High",
            remediation_available=True,
            affected_products=["Product A"],
            cve_ids=["CVE-2024-0001"],
            remediation_summary="Apply patch.",
            summary="Summary text.",
        )

    def test_all_fields_valid(self):
        from src.pipelines.extract_silver import validate_cisa_extracted_fields
        from src.schemas.silver_schema import ValidationStatus
        fields = self._make_full_fields()
        status, errors = validate_cisa_extracted_fields(fields)
        assert status == ValidationStatus.valid
        assert errors == []

    def test_required_only_partial(self):
        from src.pipelines.extract_silver import validate_cisa_extracted_fields
        from src.schemas.silver_schema import CISAAdvisoryFields, ValidationStatus
        fields = CISAAdvisoryFields(
            advisory_id="AA24-001A",
            title="Test",
            published_date="2024-01-01",
            severity_level="Medium",
            remediation_available=False,
        )
        status, errors = validate_cisa_extracted_fields(fields)
        assert status == ValidationStatus.partial
        assert len(errors) > 0

    def test_missing_required_field_invalid(self):
        from src.pipelines.extract_silver import validate_cisa_extracted_fields
        from src.schemas.silver_schema import CISAAdvisoryFields, ValidationStatus
        fields = CISAAdvisoryFields(
            title="Test",
            published_date="2024-01-01",
            severity_level="Low",
            remediation_available=True,
        )
        status, errors = validate_cisa_extracted_fields(fields)
        assert status == ValidationStatus.invalid
        assert any("advisory_id" in e for e in errors)

    def test_all_null_invalid(self):
        from src.pipelines.extract_silver import validate_cisa_extracted_fields
        from src.schemas.silver_schema import CISAAdvisoryFields, ValidationStatus
        fields = CISAAdvisoryFields()
        status, errors = validate_cisa_extracted_fields(fields)
        assert status == ValidationStatus.invalid
        assert len(errors) >= len(["advisory_id", "title", "published_date", "severity_level", "remediation_available"])


# ---------------------------------------------------------------------------
# Silver record assembly for CISA
# ---------------------------------------------------------------------------


class TestAssembleCISASilverRecord:
    """D-1: assemble_silver_record correctly handles domain_key='cisa_advisory'."""

    def _bronze(self):
        return {
            "document_id": "aaaabbbb-0000-0000-0000-000000000001",
            "bronze_record_id": "bbbbcccc-0000-0000-0000-000000000002",
            "document_class_hint": "cisa_advisory",
            "parse_status": "success",
        }

    def test_cisa_record_has_correct_prompt_id(self):
        from src.pipelines.extract_silver import assemble_silver_record
        from src.utils.extraction_prompts import CISA_ADVISORY_PROMPT_ID
        raw = {
            "advisory_id": "ICSA-24-001-01",
            "title": "Test",
            "published_date": "2024-01-01",
            "severity_level": "High",
            "remediation_available": True,
        }
        record = assemble_silver_record(
            self._bronze(), raw, "local_rule_extractor/v1", "run-1", domain_key="cisa_advisory"
        )
        assert record.extraction_prompt_id == CISA_ADVISORY_PROMPT_ID

    def test_cisa_record_valid_status(self):
        from src.pipelines.extract_silver import assemble_silver_record
        from src.schemas.silver_schema import ValidationStatus
        raw = {
            "advisory_id": "ICSA-24-001-01",
            "title": "Test Advisory",
            "published_date": "2024-01-01",
            "severity_level": "Critical",
            "remediation_available": True,
            "affected_products": ["Product A"],
            "cve_ids": ["CVE-2024-0001"],
            "remediation_summary": "Patch now.",
            "summary": "Summary.",
        }
        record = assemble_silver_record(
            self._bronze(), raw, "local_rule_extractor/v1", "run-1", domain_key="cisa_advisory"
        )
        assert record.validation_status == ValidationStatus.valid

    def test_cisa_record_invalid_status_missing_required(self):
        from src.pipelines.extract_silver import assemble_silver_record
        from src.schemas.silver_schema import ValidationStatus
        raw = {"summary": "Only a summary, no required fields."}
        record = assemble_silver_record(
            self._bronze(), raw, "local_rule_extractor/v1", "run-1", domain_key="cisa_advisory"
        )
        assert record.validation_status == ValidationStatus.invalid

    def test_cisa_record_field_coverage_full(self):
        from src.pipelines.extract_silver import assemble_silver_record
        raw = {
            "advisory_id": "AA24-001A",
            "title": "Full Coverage Test",
            "published_date": "2024-01-01",
            "severity_level": "High",
            "remediation_available": True,
            "affected_products": ["Product"],
            "cve_ids": ["CVE-2024-0001"],
            "remediation_summary": "Apply patch.",
            "summary": "Summary.",
        }
        record = assemble_silver_record(
            self._bronze(), raw, "local_rule_extractor/v1", "run-1", domain_key="cisa_advisory"
        )
        assert record.field_coverage_pct == 1.0

    def test_cisa_record_lineage_preserved(self):
        from src.pipelines.extract_silver import assemble_silver_record
        raw = {"advisory_id": "AA24-001A", "title": "T", "published_date": "2024-01-01",
               "severity_level": "Low", "remediation_available": False}
        bronze = self._bronze()
        record = assemble_silver_record(
            bronze, raw, "local_rule_extractor/v1", "run-999", domain_key="cisa_advisory"
        )
        assert record.document_id == bronze["document_id"]
        assert record.bronze_record_id == bronze["bronze_record_id"]
        assert record.pipeline_run_id == "run-999"

    def test_fda_assembly_unchanged(self):
        """FDA domain_key default behavior must be unchanged by D-1."""
        from src.pipelines.extract_silver import assemble_silver_record
        from src.utils.extraction_prompts import FDA_WARNING_LETTER_PROMPT_ID
        bronze = {
            "document_id": "fda-doc-001",
            "bronze_record_id": "fda-bronze-001",
            "document_class_hint": "fda_warning_letter",
            "parse_status": "success",
        }
        raw = {
            "issuing_office": "FDA Chicago District",
            "recipient_company": "Acme Pharma",
            "issue_date": "2024-01-15",
            "violation_type": ["CGMP Violation"],
            "corrective_action_requested": True,
        }
        record = assemble_silver_record(
            bronze, raw, "local_rule_extractor/v1", "run-fda", domain_key="fda_warning_letter"
        )
        assert record.extraction_prompt_id == FDA_WARNING_LETTER_PROMPT_ID


# ---------------------------------------------------------------------------
# CISA extractor selection
# ---------------------------------------------------------------------------


class TestSelectExtractor:
    """D-1: select_extractor dispatches CISA correctly; D-2: incident also dispatches."""

    def test_select_extractor_cisa(self):
        from src.pipelines.extract_silver import LocalCISAAdvisoryExtractor, select_extractor
        extractor = select_extractor("cisa_advisory")
        assert isinstance(extractor, LocalCISAAdvisoryExtractor)

    def test_select_extractor_fda_unchanged(self):
        from src.pipelines.extract_silver import LocalFDAWarningLetterExtractor, select_extractor
        assert isinstance(select_extractor("fda_warning_letter"), LocalFDAWarningLetterExtractor)
        assert isinstance(select_extractor(None), LocalFDAWarningLetterExtractor)

    def test_select_extractor_incident_works_after_d2(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, select_extractor
        extractor = select_extractor("incident_report")
        assert isinstance(extractor, LocalIncidentReportExtractor)

    def test_select_extractor_unregistered_raises(self):
        from src.pipelines.extract_silver import select_extractor
        with pytest.raises(ValueError):
            select_extractor("completely_unknown_domain")


# ---------------------------------------------------------------------------
# LocalCISAAdvisoryClassifier tests
# ---------------------------------------------------------------------------


class TestLocalCISAAdvisoryClassifier:
    """D-1: LocalCISAAdvisoryClassifier signal detection and confidence."""

    def setup_method(self):
        from src.pipelines.classify_gold import LocalCISAAdvisoryClassifier
        self.classifier = LocalCISAAdvisoryClassifier()

    def _make_silver(self, class_hint="cisa_advisory", validation_status="valid",
                     coverage=0.8, advisory_id="ICSA-24-001-01",
                     severity_level="High", cve_ids=None):
        return {
            "document_class_hint": class_hint,
            "validation_status": validation_status,
            "field_coverage_pct": coverage,
            "extracted_fields": {
                "advisory_id": advisory_id,
                "severity_level": severity_level,
                "cve_ids": cve_ids or ["CVE-2024-0001"],
                "affected_products": ["Product A"],
                "remediation_available": True,
            },
        }

    def test_model_id(self):
        assert self.classifier.model_id == "local_rule_classifier/v1"

    def test_classify_strong_cisa_signal(self):
        silver = self._make_silver()
        result = self.classifier.classify(silver)
        assert result["document_type_label"] == "cisa_advisory"
        assert result["classification_confidence"] > 0.0

    def test_classify_confidence_with_class_hint(self):
        silver = self._make_silver(class_hint="cisa_advisory", validation_status="valid", coverage=0.8)
        result = self.classifier.classify(silver)
        assert result["classification_confidence"] >= 0.80

    def test_classify_no_signals_returns_unknown(self):
        silver = {
            "document_class_hint": "",
            "validation_status": "invalid",
            "field_coverage_pct": 0.0,
            "extracted_fields": {},
        }
        result = self.classifier.classify(silver)
        assert result["document_type_label"] == "unknown"
        assert result["classification_confidence"] == 0.0

    def test_classify_partial_extraction_classified(self):
        """Advisory with valid hint but partial extraction still classifies."""
        silver = self._make_silver(
            class_hint="cisa_advisory",
            validation_status="partial",
            coverage=0.5,
        )
        result = self.classifier.classify(silver)
        assert result["document_type_label"] == "cisa_advisory"

    def test_classify_no_hint_but_fields_present_classifies(self):
        """CISA fields present without class hint still classifies."""
        silver = {
            "document_class_hint": "",
            "validation_status": "valid",
            "field_coverage_pct": 0.8,
            "extracted_fields": {
                "advisory_id": "ICSA-24-001-01",
                "severity_level": "High",
                "cve_ids": ["CVE-2024-0001"],
                "affected_products": ["Product A"],
            },
        }
        result = self.classifier.classify(silver)
        assert result["document_type_label"] == "cisa_advisory"

    def test_confidence_max_1(self):
        silver = self._make_silver(class_hint="cisa_advisory", validation_status="valid", coverage=0.9)
        result = self.classifier.classify(silver)
        assert result["classification_confidence"] <= 1.0

    def test_confidence_min_for_classified(self):
        silver = self._make_silver(class_hint="cisa_advisory", validation_status="partial", coverage=0.4)
        result = self.classifier.classify(silver)
        if result["document_type_label"] == "cisa_advisory":
            assert result["classification_confidence"] >= 0.60


# ---------------------------------------------------------------------------
# Classifier selection
# ---------------------------------------------------------------------------


class TestSelectClassifier:
    """D-1: select_classifier dispatches CISA correctly; D-2: incident also dispatches."""

    def test_select_classifier_cisa(self):
        from src.pipelines.classify_gold import LocalCISAAdvisoryClassifier, select_classifier
        classifier = select_classifier("cisa_advisory")
        assert isinstance(classifier, LocalCISAAdvisoryClassifier)

    def test_select_classifier_fda_unchanged(self):
        from src.pipelines.classify_gold import LocalFDAWarningLetterClassifier, select_classifier
        assert isinstance(select_classifier("fda_warning_letter"), LocalFDAWarningLetterClassifier)
        assert isinstance(select_classifier(None), LocalFDAWarningLetterClassifier)

    def test_select_classifier_incident_works_after_d2(self):
        from src.pipelines.classify_gold import LocalIncidentReportClassifier, select_classifier
        classifier = select_classifier("incident_report")
        assert isinstance(classifier, LocalIncidentReportClassifier)

    def test_select_classifier_unregistered_raises(self):
        from src.pipelines.classify_gold import select_classifier
        with pytest.raises(ValueError):
            select_classifier("not_registered_domain")


# ---------------------------------------------------------------------------
# Classification taxonomy tests
# ---------------------------------------------------------------------------


class TestClassificationTaxonomy:
    """D-1: CISA routing label is active in V1_ROUTING_MAP and taxonomy."""

    def test_cisa_in_v1_routing_map(self):
        from src.utils.classification_taxonomy import V1_ROUTING_MAP
        assert "cisa_advisory" in V1_ROUTING_MAP
        assert V1_ROUTING_MAP["cisa_advisory"] == "security_ops"

    def test_security_ops_in_executable_routing_labels(self):
        from src.utils.classification_taxonomy import V1_EXECUTABLE_ROUTING_LABELS
        assert "security_ops" in V1_EXECUTABLE_ROUTING_LABELS

    def test_cisa_in_executable_document_types(self):
        from src.utils.classification_taxonomy import V1_EXECUTABLE_DOCUMENT_TYPES
        assert "cisa_advisory" in V1_EXECUTABLE_DOCUMENT_TYPES

    def test_resolve_routing_label_cisa(self):
        from src.utils.classification_taxonomy import resolve_routing_label
        assert resolve_routing_label("cisa_advisory") == "security_ops"

    def test_resolve_routing_label_fda_unchanged(self):
        from src.utils.classification_taxonomy import resolve_routing_label
        assert resolve_routing_label("fda_warning_letter") == "regulatory_review"

    def test_resolve_routing_label_incident_routes_to_incident_management_after_d2(self):
        """D-2: incident_report is in V1_ROUTING_MAP and routes to incident_management."""
        from src.utils.classification_taxonomy import resolve_routing_label
        assert resolve_routing_label("incident_report") == "incident_management"

    def test_is_domain_executable_cisa_true(self):
        from src.utils.classification_taxonomy import is_domain_executable
        assert is_domain_executable("cisa_advisory") is True

    def test_is_domain_executable_incident_true_after_d2(self):
        from src.utils.classification_taxonomy import is_domain_executable
        assert is_domain_executable("incident_report") is True

    def test_resolve_routing_label_for_domain_cisa(self):
        from src.utils.classification_taxonomy import resolve_routing_label_for_domain
        result = resolve_routing_label_for_domain("cisa_advisory", "cisa_advisory")
        assert result == "security_ops"

    def test_resolve_routing_label_for_domain_incident_resolves_after_d2(self):
        from src.utils.classification_taxonomy import resolve_routing_label_for_domain
        result = resolve_routing_label_for_domain("incident_report", "incident_report")
        assert result == "incident_management"

    def test_security_ops_in_all_routing_labels(self):
        from src.utils.classification_taxonomy import ALL_ROUTING_LABELS
        assert "security_ops" in ALL_ROUTING_LABELS

    def test_cisa_advisory_in_all_document_type_labels(self):
        from src.utils.classification_taxonomy import ALL_DOCUMENT_TYPE_LABELS
        assert "cisa_advisory" in ALL_DOCUMENT_TYPE_LABELS


# ---------------------------------------------------------------------------
# Bedrock contract: CISA export payload validation
# ---------------------------------------------------------------------------


class TestCISAContractValidation:
    """D-1: validate_export_payload handles cisa_advisory document_type correctly."""

    def _valid_cisa_payload(self):
        return {
            "document_id": "aaaa-bbbb-cccc-dddd",
            "source_file": "cisa_advisory_sample.md",
            "document_type": "cisa_advisory",
            "routing_label": "security_ops",
            "extracted_fields": {
                "advisory_id": "ICSA-24-046-01",
                "title": "Siemens SCALANCE Vulnerabilities",
                "published_date": "2024-02-15",
                "severity_level": "Critical",
                "remediation_available": True,
            },
            "parsed_text_excerpt": "ICSA-24-046-01 advisory text excerpt...",
            "provenance": {
                "ingested_at": "2024-02-15T12:00:00+00:00",
                "pipeline_run_id": "local-run-d1",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": 0.9,
                "schema_version": "v0.1.0",
            },
        }

    def test_valid_cisa_payload_passes(self):
        from src.schemas.bedrock_contract import validate_export_payload
        result = validate_export_payload(self._valid_cisa_payload())
        assert result.valid is True
        assert result.errors == []

    def test_cisa_missing_advisory_id_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        del payload["extracted_fields"]["advisory_id"]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("advisory_id" in e for e in result.errors)

    def test_cisa_missing_severity_level_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        del payload["extracted_fields"]["severity_level"]
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_cisa_null_remediation_available_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        payload["extracted_fields"]["remediation_available"] = None
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_cisa_remediation_available_not_bool_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        payload["extracted_fields"]["remediation_available"] = "yes"
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_cisa_invalid_severity_level_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        payload["extracted_fields"]["severity_level"] = "EXTREME"
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_cisa_valid_severity_levels(self):
        from src.schemas.bedrock_contract import validate_export_payload
        for level in ("Critical", "High", "Medium", "Low"):
            payload = self._valid_cisa_payload()
            payload["extracted_fields"]["severity_level"] = level
            result = validate_export_payload(payload)
            assert result.valid is True, f"Expected valid for severity_level='{level}'"

    def test_cisa_routing_quarantine_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_cisa_payload()
        payload["routing_label"] = "quarantine"
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_cisa_required_fields_constant(self):
        from src.schemas.bedrock_contract import REQUIRED_CISA_EXTRACTED_FIELDS
        assert "advisory_id" in REQUIRED_CISA_EXTRACTED_FIELDS
        assert "severity_level" in REQUIRED_CISA_EXTRACTED_FIELDS
        assert "remediation_available" in REQUIRED_CISA_EXTRACTED_FIELDS

    def test_fda_contract_unchanged(self):
        """FDA contract validation is not affected by D-1 CISA changes."""
        from src.schemas.bedrock_contract import validate_export_payload
        payload = {
            "document_id": "fda-doc-001",
            "source_file": "fda_sample.md",
            "document_type": "fda_warning_letter",
            "routing_label": "regulatory_review",
            "extracted_fields": {
                "issuing_office": "FDA Chicago",
                "recipient_company": "Acme Pharma",
                "issue_date": "2024-01-15",
                "violation_type": ["CGMP Violation"],
                "corrective_action_requested": True,
            },
            "parsed_text_excerpt": "FDA warning letter text...",
            "provenance": {
                "ingested_at": "2024-01-15T00:00:00+00:00",
                "pipeline_run_id": "run-fda",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": 0.9,
                "schema_version": "v0.1.0",
            },
        }
        result = validate_export_payload(payload)
        assert result.valid is True


# ---------------------------------------------------------------------------
# Gold routing: CISA → security_ops
# ---------------------------------------------------------------------------


class TestCISAGoldRouting:
    """D-1: compute_routing_label produces security_ops for cisa_advisory records."""

    def test_cisa_routes_to_security_ops(self):
        from src.pipelines.classify_gold import compute_routing_label
        result = compute_routing_label(
            document_type_label="cisa_advisory",
            classification_confidence=0.85,
            silver_validation_status="valid",
            silver_coverage=0.8,
        )
        assert result == "security_ops"

    def test_cisa_low_confidence_quarantined(self):
        from src.pipelines.classify_gold import compute_routing_label
        result = compute_routing_label(
            document_type_label="cisa_advisory",
            classification_confidence=0.50,
            silver_validation_status="valid",
            silver_coverage=0.8,
        )
        assert result == "quarantine"

    def test_cisa_unknown_label_quarantined(self):
        from src.pipelines.classify_gold import compute_routing_label
        result = compute_routing_label(
            document_type_label="unknown",
            classification_confidence=0.9,
            silver_validation_status="valid",
            silver_coverage=0.8,
        )
        assert result == "quarantine"

    def test_fda_routing_unchanged(self):
        from src.pipelines.classify_gold import compute_routing_label
        result = compute_routing_label(
            document_type_label="fda_warning_letter",
            classification_confidence=0.85,
            silver_validation_status="valid",
            silver_coverage=0.8,
        )
        assert result == "regulatory_review"


# ---------------------------------------------------------------------------
# FDA regression: key FDA behaviors unchanged
# ---------------------------------------------------------------------------


class TestFDARegressionSafety:
    """D-1 must not change any FDA behavior."""

    def test_fda_domain_still_active(self):
        from src.utils.domain_registry import DomainStatus, get_domain
        assert get_domain("fda_warning_letter").status == DomainStatus.ACTIVE

    def test_fda_prompt_id_unchanged(self):
        from src.utils.extraction_prompts import FDA_WARNING_LETTER_PROMPT_ID
        assert FDA_WARNING_LETTER_PROMPT_ID == "fda_warning_letter_extract_v1"

    def test_fda_routing_label_unchanged(self):
        from src.utils.classification_taxonomy import resolve_routing_label
        assert resolve_routing_label("fda_warning_letter") == "regulatory_review"

    def test_fda_extractor_still_dispatched(self):
        from src.pipelines.extract_silver import LocalFDAWarningLetterExtractor, select_extractor
        assert isinstance(select_extractor("fda_warning_letter"), LocalFDAWarningLetterExtractor)
        assert isinstance(select_extractor(None), LocalFDAWarningLetterExtractor)

    def test_fda_classifier_still_dispatched(self):
        from src.pipelines.classify_gold import LocalFDAWarningLetterClassifier, select_classifier
        assert isinstance(select_classifier("fda_warning_letter"), LocalFDAWarningLetterClassifier)
        assert isinstance(select_classifier(None), LocalFDAWarningLetterClassifier)

    def test_fda_schema_unchanged(self):
        from src.schemas.silver_schema import FDA_REQUIRED_FIELDS
        assert "issuing_office" in FDA_REQUIRED_FIELDS
        assert "recipient_company" in FDA_REQUIRED_FIELDS
        assert "corrective_action_requested" in FDA_REQUIRED_FIELDS

    def test_fda_silver_record_unchanged(self):
        from src.schemas.silver_schema import FDAWarningLetterFields
        fields = FDAWarningLetterFields(
            issuing_office="FDA Chicago",
            recipient_company="Acme Pharma",
            issue_date="2024-01-15",
            violation_type=["CGMP Violation"],
            corrective_action_requested=True,
        )
        assert fields.issuing_office == "FDA Chicago"


# ---------------------------------------------------------------------------
# Incident domain: active after D-2
# ---------------------------------------------------------------------------


class TestIncidentDomainActiveAfterD2:
    """D-2: incident_report is now ACTIVE and fully executable.

    These tests confirm the D-2 graduation: incident_report joined FDA and CISA
    as a fully routable, extractable, classifiable domain. This replaces the
    previous D-1-era "non-executable guard" tests.
    """

    def test_incident_domain_active(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY, DomainStatus
        assert DOMAIN_REGISTRY["incident_report"].status == DomainStatus.ACTIVE

    def test_incident_extractor_works(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, select_extractor
        extractor = select_extractor("incident_report")
        assert isinstance(extractor, LocalIncidentReportExtractor)

    def test_incident_classifier_works(self):
        from src.pipelines.classify_gold import LocalIncidentReportClassifier, select_classifier
        classifier = select_classifier("incident_report")
        assert isinstance(classifier, LocalIncidentReportClassifier)

    def test_incident_prompt_retrievable(self):
        from src.utils.extraction_prompts import get_prompt_for_domain
        prompt = get_prompt_for_domain("incident_report")
        assert prompt.document_domain == "incident_report"

    def test_incident_schema_registry_works(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import IncidentReportFields
        model = build_fields_for_domain("incident_report", {})
        assert isinstance(model, IncidentReportFields)

    def test_incident_routing_resolves_to_incident_management(self):
        """D-2: incident_report is now in V1_ROUTING_MAP."""
        from src.utils.classification_taxonomy import resolve_routing_label
        assert resolve_routing_label("incident_report") == "incident_management"

    def test_incident_require_active_domain_succeeds(self):
        from src.utils.domain_registry import DomainStatus, require_active_domain
        domain = require_active_domain("incident_report")
        assert domain.status == DomainStatus.ACTIVE

    def test_incident_is_domain_executable(self):
        from src.utils.classification_taxonomy import is_domain_executable
        assert is_domain_executable("incident_report") is True
