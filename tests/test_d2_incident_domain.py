"""
tests/test_d2_incident_domain.py — Phase D-2 incident report domain test suite.

Covers:
  - Domain registry: incident_report ACTIVE status and correct config
  - Extraction prompt: incident prompt registered and retrievable
  - Schema: IncidentReportFields Pydantic model + field lists + coverage
  - Domain schema registry: incident factory active, builds real model
  - Extraction: LocalIncidentReportExtractor field extraction
  - Validation: validate_incident_extracted_fields (valid/partial/invalid)
  - Silver assembly: assemble_silver_record with domain_key='incident_report'
  - Extractor selection: select_extractor dispatches incident correctly
  - Classification: LocalIncidentReportClassifier classification logic
  - Classifier selection: select_classifier dispatches incident correctly
  - Routing: incident_management label active in taxonomy
  - Bedrock contract: _validate_incident_extracted_fields, REQUIRED_INCIDENT fields
  - Contract: valid incident export payload passes validation
  - Contract: missing required incident fields fails validation
  - FDA regression safety: FDA behavior unchanged
  - CISA regression safety: CISA behavior unchanged

Phase: D-2
Authoritative contract: docs/data-contracts.md § Incident Report Fields
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Allow running tests from the repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ---------------------------------------------------------------------------
# Fixtures shared across tests
# ---------------------------------------------------------------------------

INCIDENT_SAMPLE_TEXT = """
# Incident Report — INC-2025-042

Incident ID: INC-2025-042
Incident Date: March 14, 2025
Status: Resolved
Severity: High
Reported by: Platform Operations Team

Incident Type: Service Outage

Affected Systems:
- Payment API (primary)
- Order Management Service
- Database cluster

Root Cause:
A misconfigured database connection pool parameter was introduced during deployment.
The max_connections value was set too low, causing the service to exhaust connections.

Resolution:
Rollback of the configuration change restored service within 42 minutes.
All affected downstream services confirmed healthy after rollback.
"""

MINIMAL_INCIDENT_TEXT = """
Incident Report

Incident Date: 2025-03-14
Incident Type: Network failure
Severity: Critical
Status: open
"""


def _make_bronze_record(document_class_hint: str = "incident_report") -> dict:
    """Return a minimal synthetic Bronze record for incident domain tests."""
    return {
        "document_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "bronze_record_id": "b1c2d3e4-f5a6-7890-bcde-fa1234567891",
        "pipeline_run_id": "local-run-d2-test",
        "parse_status": "success",
        "parsed_text": INCIDENT_SAMPLE_TEXT,
        "document_class_hint": document_class_hint,
    }


# ===========================================================================
# 1. Domain registry tests
# ===========================================================================


class TestIncidentDomainRegistry:
    """Verify incident_report is ACTIVE in the domain registry after D-2."""

    def test_incident_domain_is_registered(self):
        from src.utils.domain_registry import DOMAIN_REGISTRY
        assert "incident_report" in DOMAIN_REGISTRY

    def test_incident_domain_is_active(self):
        from src.utils.domain_registry import DomainStatus, get_domain
        domain = get_domain("incident_report")
        assert domain.status == DomainStatus.ACTIVE

    def test_incident_domain_routing_label(self):
        from src.utils.domain_registry import get_domain
        domain = get_domain("incident_report")
        assert domain.routing_label == "incident_management"

    def test_incident_domain_document_type_label(self):
        from src.utils.domain_registry import get_domain
        domain = get_domain("incident_report")
        assert domain.document_type_label == "incident_report"

    def test_incident_domain_source_family(self):
        from src.utils.domain_registry import get_domain
        domain = get_domain("incident_report")
        assert domain.source_family == "operations"

    def test_incident_domain_extraction_prompt_id(self):
        from src.utils.domain_registry import get_domain
        domain = get_domain("incident_report")
        assert domain.extraction_prompt_id == "incident_report_extract_v1"

    def test_incident_domain_schema_family(self):
        from src.utils.domain_registry import get_domain
        domain = get_domain("incident_report")
        assert domain.schema_family == "incident_report"

    def test_incident_in_active_domains_list(self):
        from src.utils.domain_registry import get_active_domains
        active_keys = [d.domain_key for d in get_active_domains()]
        assert "incident_report" in active_keys

    def test_incident_not_in_planned_domains_list(self):
        from src.utils.domain_registry import get_planned_domains
        planned_keys = [d.domain_key for d in get_planned_domains()]
        assert "incident_report" not in planned_keys

    def test_is_domain_active_returns_true(self):
        from src.utils.domain_registry import is_domain_active
        assert is_domain_active("incident_report") is True

    def test_require_active_domain_does_not_raise(self):
        from src.utils.domain_registry import require_active_domain
        domain = require_active_domain("incident_report")
        assert domain.domain_key == "incident_report"

    def test_all_three_domains_active(self):
        """FDA, CISA, and incident must all be ACTIVE after D-2."""
        from src.utils.domain_registry import get_active_domains
        active_keys = {d.domain_key for d in get_active_domains()}
        assert "fda_warning_letter" in active_keys
        assert "cisa_advisory" in active_keys
        assert "incident_report" in active_keys

    def test_three_active_domains_total(self):
        from src.utils.domain_registry import get_active_domains
        assert len(get_active_domains()) == 3

    def test_zero_planned_domains_after_d2(self):
        from src.utils.domain_registry import get_planned_domains
        assert len(get_planned_domains()) == 0


# ===========================================================================
# 2. Extraction prompt tests
# ===========================================================================


class TestIncidentExtractionPrompt:
    """Verify the incident report extraction prompt is registered and correct."""

    def test_prompt_id_constant_exists(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT_ID
        assert INCIDENT_REPORT_PROMPT_ID == "incident_report_extract_v1"

    def test_prompt_object_exists(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT
        assert INCIDENT_REPORT_PROMPT is not None

    def test_prompt_id_matches_constant(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT, INCIDENT_REPORT_PROMPT_ID
        assert INCIDENT_REPORT_PROMPT.prompt_id == INCIDENT_REPORT_PROMPT_ID

    def test_prompt_domain_is_incident_report(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT
        assert INCIDENT_REPORT_PROMPT.document_domain == "incident_report"

    def test_prompt_template_not_empty(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT
        assert len(INCIDENT_REPORT_PROMPT.template) > 100

    def test_prompt_template_contains_required_fields(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT
        template = INCIDENT_REPORT_PROMPT.template
        assert "incident_date" in template
        assert "incident_type" in template
        assert "severity" in template
        assert "status" in template

    def test_prompt_template_contains_optional_fields(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT
        template = INCIDENT_REPORT_PROMPT.template
        assert "incident_id" in template
        assert "affected_systems" in template
        assert "root_cause" in template
        assert "resolution_summary" in template
        assert "reported_by" in template

    def test_get_prompt_by_id(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT_ID, get_prompt
        prompt = get_prompt(INCIDENT_REPORT_PROMPT_ID)
        assert prompt.prompt_id == INCIDENT_REPORT_PROMPT_ID

    def test_get_prompt_for_domain(self):
        from src.utils.extraction_prompts import INCIDENT_REPORT_PROMPT_ID, get_prompt_for_domain
        prompt = get_prompt_for_domain("incident_report")
        assert prompt.prompt_id == INCIDENT_REPORT_PROMPT_ID

    def test_prompt_in_list_prompt_ids(self):
        from src.utils.extraction_prompts import list_prompt_ids
        ids = list_prompt_ids()
        assert "incident_report_extract_v1" in ids

    def test_three_prompts_registered(self):
        from src.utils.extraction_prompts import list_prompt_ids
        ids = list_prompt_ids()
        assert "fda_warning_letter_extract_v1" in ids
        assert "cisa_advisory_extract_v1" in ids
        assert "incident_report_extract_v1" in ids


# ===========================================================================
# 3. Schema tests — IncidentReportFields
# ===========================================================================


class TestIncidentReportFieldsSchema:
    """Verify the IncidentReportFields Pydantic model and field constants."""

    def test_model_imports(self):
        from src.schemas.silver_schema import IncidentReportFields
        assert IncidentReportFields is not None

    def test_field_lists_import(self):
        from src.schemas.silver_schema import (
            INCIDENT_ALL_FIELDS,
            INCIDENT_OPTIONAL_FIELDS,
            INCIDENT_REQUIRED_FIELDS,
        )
        assert len(INCIDENT_REQUIRED_FIELDS) == 4
        assert len(INCIDENT_OPTIONAL_FIELDS) == 5
        assert len(INCIDENT_ALL_FIELDS) == 9

    def test_required_fields_content(self):
        from src.schemas.silver_schema import INCIDENT_REQUIRED_FIELDS
        assert "incident_date" in INCIDENT_REQUIRED_FIELDS
        assert "incident_type" in INCIDENT_REQUIRED_FIELDS
        assert "severity" in INCIDENT_REQUIRED_FIELDS
        assert "status" in INCIDENT_REQUIRED_FIELDS

    def test_optional_fields_content(self):
        from src.schemas.silver_schema import INCIDENT_OPTIONAL_FIELDS
        assert "incident_id" in INCIDENT_OPTIONAL_FIELDS
        assert "affected_systems" in INCIDENT_OPTIONAL_FIELDS
        assert "root_cause" in INCIDENT_OPTIONAL_FIELDS
        assert "resolution_summary" in INCIDENT_OPTIONAL_FIELDS
        assert "reported_by" in INCIDENT_OPTIONAL_FIELDS

    def test_all_fields_is_union(self):
        from src.schemas.silver_schema import (
            INCIDENT_ALL_FIELDS,
            INCIDENT_OPTIONAL_FIELDS,
            INCIDENT_REQUIRED_FIELDS,
        )
        for f in INCIDENT_REQUIRED_FIELDS:
            assert f in INCIDENT_ALL_FIELDS
        for f in INCIDENT_OPTIONAL_FIELDS:
            assert f in INCIDENT_ALL_FIELDS

    def test_model_constructs_with_all_fields(self):
        from src.schemas.silver_schema import IncidentReportFields
        fields = IncidentReportFields(
            incident_id="INC-2025-042",
            incident_date="2025-03-14",
            incident_type="Service Outage",
            severity="High",
            status="resolved",
            affected_systems=["Payment API", "Database"],
            root_cause="Misconfigured connection pool",
            resolution_summary="Rollback resolved the issue",
            reported_by="Platform Ops Team",
        )
        assert fields.incident_id == "INC-2025-042"
        assert fields.incident_date == "2025-03-14"
        assert fields.incident_type == "Service Outage"
        assert fields.severity == "High"
        assert fields.status == "resolved"
        assert len(fields.affected_systems) == 2
        assert fields.root_cause is not None
        assert fields.resolution_summary is not None
        assert fields.reported_by == "Platform Ops Team"

    def test_model_constructs_with_required_only(self):
        from src.schemas.silver_schema import IncidentReportFields
        fields = IncidentReportFields(
            incident_date="2025-03-14",
            incident_type="Network failure",
            severity="Critical",
            status="open",
        )
        assert fields.incident_date == "2025-03-14"
        assert fields.incident_id is None
        assert fields.affected_systems is None

    def test_model_constructs_with_all_nulls(self):
        from src.schemas.silver_schema import IncidentReportFields
        fields = IncidentReportFields()
        assert fields.incident_date is None
        assert fields.severity is None

    def test_model_dump_contains_all_fields(self):
        from src.schemas.silver_schema import INCIDENT_ALL_FIELDS, IncidentReportFields
        fields = IncidentReportFields(
            incident_date="2025-03-14",
            incident_type="Outage",
            severity="High",
            status="resolved",
        )
        d = fields.model_dump()
        for f in INCIDENT_ALL_FIELDS:
            assert f in d

    def test_coverage_function_full(self):
        from src.schemas.silver_schema import IncidentReportFields, compute_incident_field_coverage
        fields = IncidentReportFields(
            incident_id="INC-001",
            incident_date="2025-03-14",
            incident_type="Outage",
            severity="High",
            status="resolved",
            affected_systems=["API"],
            root_cause="Bug",
            resolution_summary="Fixed",
            reported_by="Team",
        )
        coverage = compute_incident_field_coverage(fields)
        assert coverage == 1.0

    def test_coverage_function_partial(self):
        from src.schemas.silver_schema import IncidentReportFields, compute_incident_field_coverage
        fields = IncidentReportFields(
            incident_date="2025-03-14",
            incident_type="Outage",
            severity="High",
            status="resolved",
        )
        coverage = compute_incident_field_coverage(fields)
        # 4 required fields populated out of 9 total
        assert 0.0 < coverage < 1.0
        assert coverage == round(4 / 9, 4)

    def test_coverage_function_zero(self):
        from src.schemas.silver_schema import IncidentReportFields, compute_incident_field_coverage
        fields = IncidentReportFields()
        assert compute_incident_field_coverage(fields) == 0.0


# ===========================================================================
# 4. Domain schema registry tests
# ===========================================================================


class TestIncidentDomainSchemaRegistry:
    """Verify incident_report schema info and factory are active in D-2."""

    def test_get_schema_info_returns_active(self):
        from src.schemas.domain_schema_registry import get_schema_info
        from src.utils.domain_registry import DomainStatus
        info = get_schema_info("incident_report")
        assert info.status == DomainStatus.ACTIVE

    def test_schema_info_required_fields(self):
        from src.schemas.domain_schema_registry import get_schema_info
        info = get_schema_info("incident_report")
        assert "incident_date" in info.required_fields
        assert "incident_type" in info.required_fields
        assert "severity" in info.required_fields
        assert "status" in info.required_fields

    def test_schema_info_optional_fields(self):
        from src.schemas.domain_schema_registry import get_schema_info
        info = get_schema_info("incident_report")
        assert "incident_id" in info.optional_fields
        assert "affected_systems" in info.optional_fields
        assert "root_cause" in info.optional_fields

    def test_build_fields_for_domain_returns_model(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import IncidentReportFields
        raw = {
            "incident_date": "2025-03-14",
            "incident_type": "Outage",
            "severity": "High",
            "status": "resolved",
            "incident_id": "INC-001",
        }
        result = build_fields_for_domain("incident_report", raw)
        assert isinstance(result, IncidentReportFields)
        assert result.incident_date == "2025-03-14"
        assert result.severity == "High"

    def test_build_fields_for_domain_filters_unknown_keys(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        from src.schemas.silver_schema import IncidentReportFields
        raw = {
            "incident_date": "2025-03-14",
            "incident_type": "Outage",
            "severity": "High",
            "status": "resolved",
            "unknown_field_xyz": "should_be_ignored",
        }
        result = build_fields_for_domain("incident_report", raw)
        assert isinstance(result, IncidentReportFields)

    def test_build_fields_for_domain_does_not_raise_for_empty_dict(self):
        from src.schemas.domain_schema_registry import build_fields_for_domain
        result = build_fields_for_domain("incident_report", {})
        assert result is not None


# ===========================================================================
# 5. Extractor tests — LocalIncidentReportExtractor
# ===========================================================================


class TestLocalIncidentReportExtractor:
    """Test the D-2 rule-based incident report extractor."""

    def _get_extractor(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor
        return LocalIncidentReportExtractor()

    def test_extractor_model_id(self):
        extractor = self._get_extractor()
        assert extractor.model_id == "local_rule_extractor/v1"

    def test_extract_returns_dict(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        assert isinstance(result, dict)

    def test_extract_incident_id(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        assert result.get("incident_id") == "INC-2025-042"

    def test_extract_incident_date(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        date = result.get("incident_date")
        assert date is not None
        assert "2025" in date

    def test_extract_incident_type(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        incident_type = result.get("incident_type")
        assert incident_type is not None
        assert len(incident_type) > 0

    def test_extract_severity(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        assert result.get("severity") == "High"

    def test_extract_status_resolved(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        assert result.get("status") == "resolved"

    def test_extract_status_open(self):
        extractor = self._get_extractor()
        text = "Status: open\nIncident Type: Network failure"
        result = extractor.extract(text)
        assert result.get("status") == "open"

    def test_extract_status_under_review(self):
        extractor = self._get_extractor()
        text = "Status: under review\nIncident Type: Security breach"
        result = extractor.extract(text)
        assert result.get("status") == "under_review"

    def test_extract_affected_systems(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        systems = result.get("affected_systems")
        assert systems is not None
        assert isinstance(systems, list)
        assert len(systems) >= 2

    def test_extract_root_cause(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        root_cause = result.get("root_cause")
        assert root_cause is not None
        assert len(root_cause) > 10

    def test_extract_resolution_summary(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        resolution = result.get("resolution_summary")
        assert resolution is not None
        assert len(resolution) > 10

    def test_extract_reported_by(self):
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        reporter = result.get("reported_by")
        assert reporter is not None
        assert "Platform" in reporter or "Operations" in reporter

    def test_extract_returns_all_expected_keys(self):
        from src.schemas.silver_schema import INCIDENT_ALL_FIELDS
        extractor = self._get_extractor()
        result = extractor.extract(INCIDENT_SAMPLE_TEXT)
        for field in INCIDENT_ALL_FIELDS:
            assert field in result, f"Expected field '{field}' missing from extraction result"

    def test_extract_p1_severity_maps_to_critical(self):
        extractor = self._get_extractor()
        text = "Priority: P1\nIncident Type: Outage\nStatus: open"
        result = extractor.extract(text)
        assert result.get("severity") == "Critical"

    def test_extract_p2_severity_maps_to_high(self):
        extractor = self._get_extractor()
        text = "Priority: P2\nIncident Type: Outage\nStatus: open"
        result = extractor.extract(text)
        assert result.get("severity") == "High"

    def test_extract_iso_date_format(self):
        extractor = self._get_extractor()
        text = "Incident Date: 2025-07-04\nIncident Type: Outage\nSeverity: High\nStatus: open"
        result = extractor.extract(text)
        assert result.get("incident_date") == "2025-07-04"

    def test_extract_empty_text_returns_dict_of_nones(self):
        extractor = self._get_extractor()
        result = extractor.extract("")
        assert isinstance(result, dict)
        # All values should be None for empty text
        for v in result.values():
            assert v is None


# ===========================================================================
# 6. Select extractor dispatch tests
# ===========================================================================


class TestSelectExtractorIncident:
    """Verify select_extractor dispatches incident_report correctly."""

    def test_incident_report_dispatches_incident_extractor(self):
        from src.pipelines.extract_silver import (
            LocalIncidentReportExtractor,
            select_extractor,
        )
        extractor = select_extractor("incident_report")
        assert isinstance(extractor, LocalIncidentReportExtractor)

    def test_none_still_dispatches_fda(self):
        from src.pipelines.extract_silver import (
            LocalFDAWarningLetterExtractor,
            select_extractor,
        )
        extractor = select_extractor(None)
        assert isinstance(extractor, LocalFDAWarningLetterExtractor)

    def test_fda_still_dispatches_fda(self):
        from src.pipelines.extract_silver import (
            LocalFDAWarningLetterExtractor,
            select_extractor,
        )
        extractor = select_extractor("fda_warning_letter")
        assert isinstance(extractor, LocalFDAWarningLetterExtractor)

    def test_cisa_still_dispatches_cisa(self):
        from src.pipelines.extract_silver import (
            LocalCISAAdvisoryExtractor,
            select_extractor,
        )
        extractor = select_extractor("cisa_advisory")
        assert isinstance(extractor, LocalCISAAdvisoryExtractor)

    def test_unregistered_raises_value_error(self):
        from src.pipelines.extract_silver import select_extractor
        with pytest.raises(ValueError):
            select_extractor("completely_unknown_domain_xyz")


# ===========================================================================
# 7. Validation logic tests
# ===========================================================================


class TestValidateIncidentExtractedFields:
    """Test validate_incident_extracted_fields (valid/partial/invalid)."""

    def _get_full_fields(self):
        from src.schemas.silver_schema import IncidentReportFields
        return IncidentReportFields(
            incident_id="INC-001",
            incident_date="2025-03-14",
            incident_type="Service Outage",
            severity="High",
            status="resolved",
            affected_systems=["API", "DB"],
            root_cause="Config error",
            resolution_summary="Rollback",
            reported_by="Ops Team",
        )

    def test_valid_when_all_required_present(self):
        from src.pipelines.extract_silver import validate_incident_extracted_fields
        from src.schemas.silver_schema import IncidentReportFields, ValidationStatus
        fields = IncidentReportFields(
            incident_id="INC-001",
            incident_date="2025-03-14",
            incident_type="Outage",
            severity="High",
            status="resolved",
            affected_systems=["API"],
            root_cause="Bug",
            resolution_summary="Fixed",
            reported_by="Ops",
        )
        status, errors = validate_incident_extracted_fields(fields)
        assert status == ValidationStatus.valid
        assert errors == []

    def test_partial_when_optional_missing(self):
        from src.pipelines.extract_silver import validate_incident_extracted_fields
        from src.schemas.silver_schema import IncidentReportFields, ValidationStatus
        fields = IncidentReportFields(
            incident_date="2025-03-14",
            incident_type="Outage",
            severity="High",
            status="resolved",
        )
        status, errors = validate_incident_extracted_fields(fields)
        assert status == ValidationStatus.partial
        assert len(errors) > 0

    def test_invalid_when_required_missing(self):
        from src.pipelines.extract_silver import validate_incident_extracted_fields
        from src.schemas.silver_schema import IncidentReportFields, ValidationStatus
        fields = IncidentReportFields(
            incident_date=None,  # required missing
            incident_type="Outage",
            severity="High",
            status="resolved",
        )
        status, errors = validate_incident_extracted_fields(fields)
        assert status == ValidationStatus.invalid
        assert any("incident_date" in e for e in errors)

    def test_invalid_when_multiple_required_missing(self):
        from src.pipelines.extract_silver import validate_incident_extracted_fields
        from src.schemas.silver_schema import IncidentReportFields, ValidationStatus
        fields = IncidentReportFields()  # all null
        status, errors = validate_incident_extracted_fields(fields)
        assert status == ValidationStatus.invalid
        assert len(errors) >= 4


# ===========================================================================
# 8. Silver record assembly tests
# ===========================================================================


class TestAssembleSilverRecordIncident:
    """Test assemble_silver_record with domain_key='incident_report'."""

    def test_assembles_valid_silver_record(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, assemble_silver_record
        from src.schemas.silver_schema import ValidationStatus
        extractor = LocalIncidentReportExtractor()
        raw = extractor.extract(INCIDENT_SAMPLE_TEXT)
        bronze = _make_bronze_record()
        record = assemble_silver_record(
            bronze=bronze,
            extracted_raw=raw,
            extraction_model=extractor.model_id,
            pipeline_run_id="local-run-test",
            domain_key="incident_report",
        )
        assert record.document_id == bronze["document_id"]
        assert record.extraction_prompt_id == "incident_report_extract_v1"
        assert record.validation_status in (
            ValidationStatus.valid, ValidationStatus.partial
        )
        assert record.extracted_fields is not None

    def test_silver_record_has_correct_prompt_id(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, assemble_silver_record
        extractor = LocalIncidentReportExtractor()
        raw = extractor.extract(INCIDENT_SAMPLE_TEXT)
        bronze = _make_bronze_record()
        record = assemble_silver_record(
            bronze=bronze, extracted_raw=raw,
            extraction_model=extractor.model_id,
            pipeline_run_id="run-id",
            domain_key="incident_report",
        )
        assert record.extraction_prompt_id == "incident_report_extract_v1"

    def test_silver_record_field_coverage_nonzero(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, assemble_silver_record
        extractor = LocalIncidentReportExtractor()
        raw = extractor.extract(INCIDENT_SAMPLE_TEXT)
        bronze = _make_bronze_record()
        record = assemble_silver_record(
            bronze=bronze, extracted_raw=raw,
            extraction_model=extractor.model_id,
            pipeline_run_id="run-id",
            domain_key="incident_report",
        )
        assert record.field_coverage_pct > 0.0

    def test_silver_record_serializes_to_json(self):
        import json
        from src.pipelines.extract_silver import LocalIncidentReportExtractor, assemble_silver_record
        extractor = LocalIncidentReportExtractor()
        raw = extractor.extract(INCIDENT_SAMPLE_TEXT)
        bronze = _make_bronze_record()
        record = assemble_silver_record(
            bronze=bronze, extracted_raw=raw,
            extraction_model=extractor.model_id,
            pipeline_run_id="run-id",
            domain_key="incident_report",
        )
        json_str = record.to_json_str()
        parsed = json.loads(json_str)
        assert parsed["extraction_prompt_id"] == "incident_report_extract_v1"


# ===========================================================================
# 9. Classifier tests — LocalIncidentReportClassifier
# ===========================================================================


class TestLocalIncidentReportClassifier:
    """Test the D-2 rule-based incident report classifier."""

    def _get_classifier(self):
        from src.pipelines.classify_gold import LocalIncidentReportClassifier
        return LocalIncidentReportClassifier()

    def _make_silver_dict(
        self,
        class_hint: str = "incident_report",
        validation_status: str = "valid",
        coverage: float = 0.8,
        extracted_fields: dict | None = None,
    ) -> dict:
        if extracted_fields is None:
            extracted_fields = {
                "incident_date": "2025-03-14",
                "incident_type": "Service Outage",
                "severity": "High",
                "status": "resolved",
                "affected_systems": ["Payment API"],
            }
        return {
            "document_class_hint": class_hint,
            "validation_status": validation_status,
            "field_coverage_pct": coverage,
            "extracted_fields": extracted_fields,
        }

    def test_classifier_model_id(self):
        classifier = self._get_classifier()
        assert classifier.model_id == "local_rule_classifier/v1"

    def test_classifies_incident_with_strong_signals(self):
        from src.utils.classification_taxonomy import DOCUMENT_TYPE_INCIDENT_REPORT
        classifier = self._get_classifier()
        silver = self._make_silver_dict()
        result = classifier.classify(silver)
        assert result["document_type_label"] == DOCUMENT_TYPE_INCIDENT_REPORT

    def test_confidence_above_threshold_for_full_signals(self):
        classifier = self._get_classifier()
        silver = self._make_silver_dict(
            class_hint="incident_report",
            validation_status="valid",
            coverage=0.9,
        )
        result = classifier.classify(silver)
        assert result["classification_confidence"] >= 0.70

    def test_confidence_includes_class_hint_bonus(self):
        classifier = self._get_classifier()
        silver_with_hint = self._make_silver_dict(class_hint="incident_report")
        silver_no_hint = self._make_silver_dict(class_hint="")
        result_hint = classifier.classify(silver_with_hint)
        result_no_hint = classifier.classify(silver_no_hint)
        if result_no_hint["document_type_label"] != "unknown":
            assert result_hint["classification_confidence"] >= result_no_hint["classification_confidence"]

    def test_returns_unknown_when_insufficient_signals(self):
        from src.utils.classification_taxonomy import DOCUMENT_TYPE_UNKNOWN
        classifier = self._get_classifier()
        silver = {
            "document_class_hint": "",
            "validation_status": "invalid",
            "field_coverage_pct": 0.1,
            "extracted_fields": {},
        }
        result = classifier.classify(silver)
        assert result["document_type_label"] == DOCUMENT_TYPE_UNKNOWN
        assert result["classification_confidence"] == 0.0

    def test_classify_returns_dict_with_expected_keys(self):
        classifier = self._get_classifier()
        silver = self._make_silver_dict()
        result = classifier.classify(silver)
        assert "document_type_label" in result
        assert "classification_confidence" in result


# ===========================================================================
# 10. Select classifier dispatch tests
# ===========================================================================


class TestSelectClassifierIncident:
    """Verify select_classifier dispatches incident_report correctly."""

    def test_incident_dispatches_incident_classifier(self):
        from src.pipelines.classify_gold import (
            LocalIncidentReportClassifier,
            select_classifier,
        )
        classifier = select_classifier("incident_report")
        assert isinstance(classifier, LocalIncidentReportClassifier)

    def test_none_still_dispatches_fda_classifier(self):
        from src.pipelines.classify_gold import (
            LocalFDAWarningLetterClassifier,
            select_classifier,
        )
        classifier = select_classifier(None)
        assert isinstance(classifier, LocalFDAWarningLetterClassifier)

    def test_fda_still_dispatches_fda_classifier(self):
        from src.pipelines.classify_gold import (
            LocalFDAWarningLetterClassifier,
            select_classifier,
        )
        classifier = select_classifier("fda_warning_letter")
        assert isinstance(classifier, LocalFDAWarningLetterClassifier)

    def test_cisa_still_dispatches_cisa_classifier(self):
        from src.pipelines.classify_gold import (
            LocalCISAAdvisoryClassifier,
            select_classifier,
        )
        classifier = select_classifier("cisa_advisory")
        assert isinstance(classifier, LocalCISAAdvisoryClassifier)

    def test_unregistered_raises_value_error(self):
        from src.pipelines.classify_gold import select_classifier
        with pytest.raises(ValueError):
            select_classifier("not_a_real_domain")


# ===========================================================================
# 11. Classification taxonomy tests
# ===========================================================================


class TestIncidentTaxonomy:
    """Verify incident_management routing label is active in the taxonomy."""

    def test_incident_management_in_executable_routing_labels(self):
        from src.utils.classification_taxonomy import (
            ROUTING_LABEL_INCIDENT_MANAGEMENT,
            V1_EXECUTABLE_ROUTING_LABELS,
        )
        assert ROUTING_LABEL_INCIDENT_MANAGEMENT in V1_EXECUTABLE_ROUTING_LABELS

    def test_incident_report_in_executable_document_types(self):
        from src.utils.classification_taxonomy import (
            DOCUMENT_TYPE_INCIDENT_REPORT,
            V1_EXECUTABLE_DOCUMENT_TYPES,
        )
        assert DOCUMENT_TYPE_INCIDENT_REPORT in V1_EXECUTABLE_DOCUMENT_TYPES

    def test_incident_report_in_v1_routing_map(self):
        from src.utils.classification_taxonomy import (
            DOCUMENT_TYPE_INCIDENT_REPORT,
            ROUTING_LABEL_INCIDENT_MANAGEMENT,
            V1_ROUTING_MAP,
        )
        assert V1_ROUTING_MAP[DOCUMENT_TYPE_INCIDENT_REPORT] == ROUTING_LABEL_INCIDENT_MANAGEMENT

    def test_incident_report_in_domain_routing_map(self):
        from src.utils.classification_taxonomy import (
            DOCUMENT_TYPE_INCIDENT_REPORT,
            DOMAIN_ROUTING_MAP,
            ROUTING_LABEL_INCIDENT_MANAGEMENT,
        )
        assert DOMAIN_ROUTING_MAP[DOCUMENT_TYPE_INCIDENT_REPORT] == ROUTING_LABEL_INCIDENT_MANAGEMENT

    def test_resolve_routing_label_for_incident(self):
        from src.utils.classification_taxonomy import resolve_routing_label
        result = resolve_routing_label("incident_report")
        assert result == "incident_management"

    def test_is_domain_executable_incident(self):
        from src.utils.classification_taxonomy import is_domain_executable
        assert is_domain_executable("incident_report") is True

    def test_resolve_routing_label_for_domain_incident(self):
        from src.utils.classification_taxonomy import resolve_routing_label_for_domain
        result = resolve_routing_label_for_domain("incident_report", "incident_report")
        assert result == "incident_management"

    def test_three_executable_non_quarantine_routing_labels(self):
        from src.utils.classification_taxonomy import V1_EXECUTABLE_ROUTING_LABELS
        non_quarantine = [r for r in V1_EXECUTABLE_ROUTING_LABELS if r != "quarantine"]
        assert len(non_quarantine) == 3
        assert "regulatory_review" in non_quarantine
        assert "security_ops" in non_quarantine
        assert "incident_management" in non_quarantine


# ===========================================================================
# 12. Bedrock contract tests — incident validation
# ===========================================================================


class TestBedrockContractIncident:
    """Verify the Bedrock contract validator handles incident report payloads."""

    def _valid_incident_payload(self) -> dict:
        return {
            "document_id": "a1b2c3d4-0000-0000-0000-000000000001",
            "source_file": "incident_report_sample.md",
            "document_type": "incident_report",
            "routing_label": "incident_management",
            "extracted_fields": {
                "incident_date": "2025-03-14",
                "incident_type": "Service Outage",
                "severity": "High",
                "status": "resolved",
                "incident_id": "INC-2025-042",
            },
            "parsed_text_excerpt": "Incident Report — INC-2025-042\nDate: March 14, 2025",
            "provenance": {
                "ingested_at": "2025-03-14T10:55:00+00:00",
                "pipeline_run_id": "local-run-d2",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": 0.9,
                "schema_version": "v0.1.0",
            },
        }

    def test_valid_incident_payload_passes(self):
        from src.schemas.bedrock_contract import validate_export_payload
        result = validate_export_payload(self._valid_incident_payload())
        assert result.valid is True
        assert result.errors == []

    def test_required_incident_fields_constant_exists(self):
        from src.schemas.bedrock_contract import REQUIRED_INCIDENT_EXTRACTED_FIELDS
        assert "incident_date" in REQUIRED_INCIDENT_EXTRACTED_FIELDS
        assert "incident_type" in REQUIRED_INCIDENT_EXTRACTED_FIELDS
        assert "severity" in REQUIRED_INCIDENT_EXTRACTED_FIELDS
        assert "status" in REQUIRED_INCIDENT_EXTRACTED_FIELDS

    def test_required_incident_fields_count(self):
        from src.schemas.bedrock_contract import REQUIRED_INCIDENT_EXTRACTED_FIELDS
        assert len(REQUIRED_INCIDENT_EXTRACTED_FIELDS) == 4

    def test_missing_incident_date_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        del payload["extracted_fields"]["incident_date"]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("incident_date" in e for e in result.errors)

    def test_missing_incident_type_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        del payload["extracted_fields"]["incident_type"]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("incident_type" in e for e in result.errors)

    def test_missing_severity_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        del payload["extracted_fields"]["severity"]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("severity" in e for e in result.errors)

    def test_missing_status_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        del payload["extracted_fields"]["status"]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("status" in e for e in result.errors)

    def test_null_incident_date_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        payload["extracted_fields"]["incident_date"] = None
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_invalid_status_value_fails(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        payload["extracted_fields"]["status"] = "unknown_status"
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("status" in e for e in result.errors)

    def test_valid_status_open_passes(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        payload["extracted_fields"]["status"] = "open"
        result = validate_export_payload(payload)
        assert result.valid is True

    def test_valid_status_under_review_passes(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        payload["extracted_fields"]["status"] = "under_review"
        result = validate_export_payload(payload)
        assert result.valid is True

    def test_optional_incident_fields_not_required(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = self._valid_incident_payload()
        # Remove all optional incident fields — should still pass
        for f in ["incident_id", "affected_systems", "root_cause", "resolution_summary", "reported_by"]:
            payload["extracted_fields"].pop(f, None)
        result = validate_export_payload(payload)
        assert result.valid is True


# ===========================================================================
# 13. FDA regression safety tests
# ===========================================================================


class TestFDARegression:
    """Ensure FDA behavior is completely unchanged after D-2."""

    def test_fda_domain_still_active(self):
        from src.utils.domain_registry import DomainStatus, get_domain
        assert get_domain("fda_warning_letter").status == DomainStatus.ACTIVE

    def test_fda_routing_label_unchanged(self):
        from src.utils.domain_registry import get_domain
        assert get_domain("fda_warning_letter").routing_label == "regulatory_review"

    def test_fda_prompt_still_retrievable(self):
        from src.utils.extraction_prompts import (
            FDA_WARNING_LETTER_PROMPT_ID,
            get_prompt_for_domain,
        )
        prompt = get_prompt_for_domain("fda_warning_letter")
        assert prompt.prompt_id == FDA_WARNING_LETTER_PROMPT_ID

    def test_fda_select_extractor_unchanged(self):
        from src.pipelines.extract_silver import (
            LocalFDAWarningLetterExtractor,
            select_extractor,
        )
        assert isinstance(select_extractor("fda_warning_letter"), LocalFDAWarningLetterExtractor)
        assert isinstance(select_extractor(None), LocalFDAWarningLetterExtractor)

    def test_fda_select_classifier_unchanged(self):
        from src.pipelines.classify_gold import (
            LocalFDAWarningLetterClassifier,
            select_classifier,
        )
        assert isinstance(select_classifier("fda_warning_letter"), LocalFDAWarningLetterClassifier)

    def test_fda_routing_map_unchanged(self):
        from src.utils.classification_taxonomy import V1_ROUTING_MAP
        assert V1_ROUTING_MAP["fda_warning_letter"] == "regulatory_review"

    def test_fda_schema_registry_active(self):
        from src.schemas.domain_schema_registry import get_schema_info
        from src.utils.domain_registry import DomainStatus
        info = get_schema_info("fda_warning_letter")
        assert info.status == DomainStatus.ACTIVE

    def test_fda_contract_validation_still_works(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = {
            "document_id": "fda-doc-001",
            "source_file": "fda_letter.pdf",
            "document_type": "fda_warning_letter",
            "routing_label": "regulatory_review",
            "extracted_fields": {
                "issuing_office": "FDA Chicago District",
                "recipient_company": "Acme Pharma Inc.",
                "issue_date": "2025-01-15",
                "violation_type": ["CGMP Violations"],
                "corrective_action_requested": True,
            },
            "parsed_text_excerpt": "FDA Warning Letter...",
            "provenance": {
                "ingested_at": "2025-01-15T10:00:00Z",
                "pipeline_run_id": "local-run-fda",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": 0.95,
                "schema_version": "v0.1.0",
            },
        }
        result = validate_export_payload(payload)
        assert result.valid is True


# ===========================================================================
# 14. CISA regression safety tests
# ===========================================================================


class TestCISARegression:
    """Ensure CISA behavior is completely unchanged after D-2."""

    def test_cisa_domain_still_active(self):
        from src.utils.domain_registry import DomainStatus, get_domain
        assert get_domain("cisa_advisory").status == DomainStatus.ACTIVE

    def test_cisa_routing_label_unchanged(self):
        from src.utils.domain_registry import get_domain
        assert get_domain("cisa_advisory").routing_label == "security_ops"

    def test_cisa_prompt_still_retrievable(self):
        from src.utils.extraction_prompts import (
            CISA_ADVISORY_PROMPT_ID,
            get_prompt_for_domain,
        )
        prompt = get_prompt_for_domain("cisa_advisory")
        assert prompt.prompt_id == CISA_ADVISORY_PROMPT_ID

    def test_cisa_select_extractor_unchanged(self):
        from src.pipelines.extract_silver import (
            LocalCISAAdvisoryExtractor,
            select_extractor,
        )
        assert isinstance(select_extractor("cisa_advisory"), LocalCISAAdvisoryExtractor)

    def test_cisa_select_classifier_unchanged(self):
        from src.pipelines.classify_gold import (
            LocalCISAAdvisoryClassifier,
            select_classifier,
        )
        assert isinstance(select_classifier("cisa_advisory"), LocalCISAAdvisoryClassifier)

    def test_cisa_routing_map_unchanged(self):
        from src.utils.classification_taxonomy import V1_ROUTING_MAP
        assert V1_ROUTING_MAP["cisa_advisory"] == "security_ops"

    def test_cisa_schema_registry_active(self):
        from src.schemas.domain_schema_registry import get_schema_info
        from src.utils.domain_registry import DomainStatus
        info = get_schema_info("cisa_advisory")
        assert info.status == DomainStatus.ACTIVE

    def test_cisa_contract_validation_still_works(self):
        from src.schemas.bedrock_contract import validate_export_payload
        payload = {
            "document_id": "cisa-doc-001",
            "source_file": "cisa_advisory.pdf",
            "document_type": "cisa_advisory",
            "routing_label": "security_ops",
            "extracted_fields": {
                "advisory_id": "ICSA-25-001-01",
                "title": "Test Advisory",
                "published_date": "2025-01-15",
                "severity_level": "High",
                "remediation_available": True,
            },
            "parsed_text_excerpt": "CISA Advisory...",
            "provenance": {
                "ingested_at": "2025-01-15T10:00:00Z",
                "pipeline_run_id": "local-run-cisa",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": 0.9,
                "schema_version": "v0.1.0",
            },
        }
        result = validate_export_payload(payload)
        assert result.valid is True


# ===========================================================================
# 15. Module boundary and no-leak tests
# ===========================================================================


class TestModuleBoundaryIncident:
    """Verify D-2 does not absorb Bedrock logic or add agent/RAG concerns."""

    def test_no_bedrock_sdk_import_in_extract_silver(self):
        import ast
        path = Path(__file__).parents[1] / "src" / "pipelines" / "extract_silver.py"
        source = path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = ""
                if isinstance(node, ast.Import):
                    module = " ".join(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    module = node.module
                assert "boto3" not in module, "bedrock SDK import detected in extract_silver"
                assert "bedrock" not in module.lower(), "bedrock SDK import detected"

    def test_no_bedrock_sdk_import_in_classify_gold(self):
        import ast
        path = Path(__file__).parents[1] / "src" / "pipelines" / "classify_gold.py"
        source = path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = ""
                if isinstance(node, ast.Import):
                    module = " ".join(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    module = node.module
                assert "boto3" not in module, "bedrock SDK import detected in classify_gold"

    def test_incident_extractor_does_not_call_external_service(self):
        from src.pipelines.extract_silver import LocalIncidentReportExtractor
        # Should succeed without any network call or external dependency
        extractor = LocalIncidentReportExtractor()
        result = extractor.extract("Incident Date: 2025-01-01\nStatus: open\nIncident Type: Outage\nSeverity: High")
        assert isinstance(result, dict)

    def test_incident_classifier_does_not_call_external_service(self):
        from src.pipelines.classify_gold import LocalIncidentReportClassifier
        classifier = LocalIncidentReportClassifier()
        silver = {
            "document_class_hint": "incident_report",
            "validation_status": "valid",
            "field_coverage_pct": 0.8,
            "extracted_fields": {"incident_date": "2025-03-14", "incident_type": "Outage",
                                  "severity": "High", "status": "resolved"},
        }
        result = classifier.classify(silver)
        assert isinstance(result, dict)
