"""
tests/test_bedrock_contract_validation.py — B-1 Bedrock handoff contract validation tests.

Tests enforce that the Gold export_payload structure is validated against the B-0
contract (docs/bedrock-handoff-contract.md). These tests make the contract
enforceable in code, not just in documentation.

B-1 scope:
- Valid export-ready FDA Gold payload passes contract validation
- Missing required Bedrock handoff fields fail validation
- Invalid or incomplete provenance fails validation
- Quarantine case is handled correctly per contract
- Optional fields do not incorrectly fail validation
- Null classification_confidence (bootstrap path) passes validation

What these tests do NOT imply:
- No live Bedrock integration exists or is required
- No AWS credentials, S3, or Bedrock SDK calls
- No vector search or retrieval logic
- These tests validate upstream payload shape only

Authoritative contract: docs/bedrock-handoff-contract.md
Validator: src/schemas/bedrock_contract.py
"""

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.schemas.bedrock_contract import (
    ContractValidationResult,
    CONTRACT_VERSION,
    REQUIRED_PAYLOAD_FIELDS,
    REQUIRED_PROVENANCE_FIELDS,
    REQUIRED_FDA_EXTRACTED_FIELDS,
    validate_export_payload,
    validate_quarantine_record,
)


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def make_valid_export_payload() -> dict:
    """
    Return a minimal contract-valid FDA warning letter export payload.

    Matches B-0 §4 shape. All required fields present. Optional fields omitted
    to keep the baseline minimal for surgical mutation in test cases.
    """
    return {
        "document_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "source_file": "fda_warning_letter_2024_001.pdf",
        "document_type": "fda_warning_letter",
        "routing_label": "regulatory_review",
        "extracted_fields": {
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "Acme Pharma Inc.",
            "recipient_name": None,
            "issue_date": "2024-03-15",
            "violation_type": ["Current Good Manufacturing Practice"],
            "corrective_action_requested": True,
        },
        "parsed_text_excerpt": "WARNING LETTER\n\nIssued: March 15, 2024\nTo: Acme Pharma Inc.",
        "provenance": {
            "ingested_at": "2024-03-20T14:30:00Z",
            "pipeline_run_id": "local-run-abc123",
            "extraction_model": "local_rule_extractor/v1",
            "classification_model": "local_rule_classifier/v1",
            "classification_confidence": 0.94,
            "schema_version": "v0.1.0",
        },
    }


def make_quarantine_gold_record() -> dict:
    """Return a correctly shaped quarantine Gold record per B-0 §6."""
    return {
        "document_id": "b2c3d4e5-f6a7-8901-bcde-fa2345678901",
        "bronze_record_id": "c3d4e5f6-a7b8-9012-cdef-ab3456789012",
        "extraction_id": "d4e5f6a7-b8c9-0123-defa-bc4567890123",
        "gold_record_id": "e5f6a7b8-c9d0-1234-efab-cd5678901234",
        "pipeline_run_id": "local-run-xyz456",
        "classified_at": "2024-03-20T15:00:00+00:00",
        "document_type_label": "unknown",
        "routing_label": "quarantine",
        "classification_confidence": 0.0,
        "classification_model": "local_rule_classifier/v1",
        "export_ready": False,
        "export_path": None,
        "schema_version": "v0.1.0",
    }


# ---------------------------------------------------------------------------
# ContractValidationResult — basic behavior
# ---------------------------------------------------------------------------


class TestContractValidationResult:
    def test_valid_result_repr(self):
        r = ContractValidationResult(valid=True)
        assert "VALID" in repr(r)

    def test_invalid_result_repr(self):
        r = ContractValidationResult(valid=False, errors=["missing field x"])
        assert "INVALID" in repr(r)
        assert "1 error" in repr(r)

    def test_defaults_empty_lists(self):
        r = ContractValidationResult(valid=True)
        assert r.errors == []
        assert r.warnings == []


# ---------------------------------------------------------------------------
# validate_export_payload — valid cases
# ---------------------------------------------------------------------------


class TestValidExportPayload:
    def test_contract_valid_fda_payload_passes(self):
        """A complete, contract-valid FDA export payload must pass."""
        payload = make_valid_export_payload()
        result = validate_export_payload(payload)
        assert result.valid is True
        assert result.errors == []

    def test_null_confidence_bootstrap_path_passes(self):
        """
        Null classification_confidence is explicitly allowed per B-0 §4.3 and §9.
        Bootstrap-origin records carry null confidence; this must not fail validation.
        """
        payload = make_valid_export_payload()
        payload["provenance"]["classification_confidence"] = None
        result = validate_export_payload(payload)
        assert result.valid is True, f"Unexpected errors: {result.errors}"

    def test_bootstrap_pipeline_run_id_passes(self):
        """
        'bootstrap_sql_v1' as pipeline_run_id is explicitly allowed per B-0 §9.
        """
        payload = make_valid_export_payload()
        payload["provenance"]["pipeline_run_id"] = "bootstrap_sql_v1"
        result = validate_export_payload(payload)
        assert result.valid is True

    def test_optional_fields_omitted_passes(self):
        """
        Optional fields (page_count, char_count, extraction_prompt_id) must not
        be required. Their absence must not produce a validation error.
        """
        payload = make_valid_export_payload()
        # Ensure none of the optional fields are present
        for opt_field in ("page_count", "char_count", "extraction_prompt_id"):
            payload.pop(opt_field, None)
        result = validate_export_payload(payload)
        assert result.valid is True

    def test_optional_extracted_fields_omitted_passes(self):
        """
        Optional FDA extracted_fields (recipient_name, cited_regulations,
        response_deadline_days, product_involved, summary) must not be required.
        """
        payload = make_valid_export_payload()
        # Remove all optional FDA fields, keeping only required ones
        payload["extracted_fields"] = {
            "issuing_office": "FDA Office",
            "recipient_company": "Test Corp",
            "issue_date": "2024-01-01",
            "violation_type": ["cGMP violation"],
            "corrective_action_requested": True,
        }
        result = validate_export_payload(payload)
        assert result.valid is True

    def test_confidence_at_boundary_values_passes(self):
        """Confidence exactly 0.0 and 1.0 must be accepted."""
        for boundary_val in (0.0, 1.0):
            payload = make_valid_export_payload()
            payload["provenance"]["classification_confidence"] = boundary_val
            result = validate_export_payload(payload)
            assert result.valid is True, (
                f"confidence={boundary_val} should pass; got errors: {result.errors}"
            )

    def test_fixture_file_is_contract_valid(self):
        """
        The committed contract_valid_fda_export_payload.json fixture must
        pass contract validation.
        """
        import json

        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "contract_valid_fda_export_payload.json"
        )
        assert fixture_path.exists(), f"Fixture not found: {fixture_path}"
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        # Strip the _note metadata field before validation
        payload.pop("_note", None)
        result = validate_export_payload(payload)
        assert result.valid is True, f"Fixture failed contract validation: {result.errors}"


# ---------------------------------------------------------------------------
# validate_export_payload — missing required top-level fields
# ---------------------------------------------------------------------------


class TestMissingRequiredPayloadFields:
    @pytest.mark.parametrize("missing_field", list(REQUIRED_PAYLOAD_FIELDS))
    def test_missing_required_field_fails(self, missing_field: str):
        """
        Removing any required top-level field must cause validation failure.
        Per B-0 §4.1: absence of any required field is grounds to reject the handoff unit.
        """
        payload = make_valid_export_payload()
        del payload[missing_field]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any(missing_field in e for e in result.errors), (
            f"Expected error mentioning '{missing_field}', got: {result.errors}"
        )

    def test_non_dict_payload_fails(self):
        """Non-dict input must fail immediately."""
        for bad_input in (None, "string", 42, []):
            result = validate_export_payload(bad_input)
            assert result.valid is False

    def test_empty_document_id_fails(self):
        """Empty string document_id must fail."""
        payload = make_valid_export_payload()
        payload["document_id"] = ""
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_empty_source_file_fails(self):
        """Empty string source_file must fail."""
        payload = make_valid_export_payload()
        payload["source_file"] = ""
        result = validate_export_payload(payload)
        assert result.valid is False


# ---------------------------------------------------------------------------
# validate_export_payload — document_type and routing_label contract rules
# ---------------------------------------------------------------------------


class TestDocumentTypeAndRoutingLabelRules:
    def test_unknown_document_type_fails(self):
        """
        document_type='unknown' signals quarantine and must not be a valid handoff unit.
        Per B-0 §3 condition 4: 'document_type_label != unknown'.
        """
        payload = make_valid_export_payload()
        payload["document_type"] = "unknown"
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("unknown" in e.lower() for e in result.errors)

    def test_quarantine_routing_label_fails(self):
        """
        routing_label='quarantine' means the record must not be treated as a handoff unit.
        Per B-0 §3: quarantine records have export_ready=False and produce no export file.
        """
        payload = make_valid_export_payload()
        payload["routing_label"] = "quarantine"
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("quarantine" in e.lower() for e in result.errors)


# ---------------------------------------------------------------------------
# validate_export_payload — provenance validation
# ---------------------------------------------------------------------------


class TestProvenanceValidation:
    @pytest.mark.parametrize("missing_prov_field", list(REQUIRED_PROVENANCE_FIELDS))
    def test_missing_provenance_field_fails(self, missing_prov_field: str):
        """
        Removing any required provenance field must fail.
        Per B-0 §4.3: all provenance fields are required (classification_confidence nullable).
        """
        payload = make_valid_export_payload()
        del payload["provenance"][missing_prov_field]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any(missing_prov_field in e for e in result.errors), (
            f"Expected error mentioning '{missing_prov_field}', got: {result.errors}"
        )

    def test_null_required_provenance_field_fails(self):
        """
        Required provenance fields (all except classification_confidence) must not be null.
        """
        non_nullable_fields = [
            f for f in REQUIRED_PROVENANCE_FIELDS if f != "classification_confidence"
        ]
        for prov_field in non_nullable_fields:
            payload = make_valid_export_payload()
            payload["provenance"][prov_field] = None
            result = validate_export_payload(payload)
            assert result.valid is False, (
                f"Null provenance.{prov_field} should fail; got valid=True"
            )

    def test_null_classification_confidence_passes(self):
        """
        classification_confidence = null is explicitly allowed per B-0 §4.3 and §9.
        The key must be present; the value may be null.
        """
        payload = make_valid_export_payload()
        payload["provenance"]["classification_confidence"] = None
        result = validate_export_payload(payload)
        assert result.valid is True, f"Null confidence should pass; got: {result.errors}"

    def test_out_of_range_confidence_fails(self):
        """Confidence values outside [0.0, 1.0] must fail."""
        for bad_val in (-0.01, 1.01, 2.0, -1.0):
            payload = make_valid_export_payload()
            payload["provenance"]["classification_confidence"] = bad_val
            result = validate_export_payload(payload)
            assert result.valid is False, (
                f"Confidence {bad_val} should fail range check"
            )

    def test_non_object_provenance_fails(self):
        """provenance must be an object, not a string or list."""
        payload = make_valid_export_payload()
        payload["provenance"] = "not an object"
        result = validate_export_payload(payload)
        assert result.valid is False

    def test_empty_provenance_object_fails(self):
        """An empty provenance dict is missing all required fields."""
        payload = make_valid_export_payload()
        payload["provenance"] = {}
        result = validate_export_payload(payload)
        assert result.valid is False
        assert len(result.errors) >= len(REQUIRED_PROVENANCE_FIELDS), (
            "Expected an error for every missing provenance field"
        )


# ---------------------------------------------------------------------------
# validate_export_payload — FDA warning letter extracted field validation
# ---------------------------------------------------------------------------


class TestFDAExtractedFieldsValidation:
    @pytest.mark.parametrize("missing_fda_field", list(REQUIRED_FDA_EXTRACTED_FIELDS))
    def test_missing_required_fda_field_fails(self, missing_fda_field: str):
        """
        Removing any required FDA warning letter extracted_field must fail.
        Per B-0 §4.4: issuing_office, recipient_company, issue_date, violation_type,
        corrective_action_requested are required for document_type='fda_warning_letter'.
        """
        payload = make_valid_export_payload()
        del payload["extracted_fields"][missing_fda_field]
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any(missing_fda_field in e for e in result.errors), (
            f"Expected error mentioning '{missing_fda_field}', got: {result.errors}"
        )

    def test_null_required_fda_field_fails(self):
        """Required FDA extracted_fields must not be null."""
        for req_field in REQUIRED_FDA_EXTRACTED_FIELDS:
            payload = make_valid_export_payload()
            payload["extracted_fields"][req_field] = None
            result = validate_export_payload(payload)
            assert result.valid is False, (
                f"Null extracted_fields.{req_field} should fail"
            )

    def test_empty_violation_type_array_fails(self):
        """violation_type must be a non-empty array per B-0 §4.4."""
        payload = make_valid_export_payload()
        payload["extracted_fields"]["violation_type"] = []
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("violation_type" in e for e in result.errors)

    def test_non_boolean_corrective_action_fails(self):
        """corrective_action_requested must be a boolean per B-0 §4.4."""
        payload = make_valid_export_payload()
        payload["extracted_fields"]["corrective_action_requested"] = "yes"
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("corrective_action_requested" in e for e in result.errors)

    def test_non_fda_document_type_skips_fda_field_checks(self):
        """
        FDA-specific extracted_field validation must only apply to
        document_type='fda_warning_letter'. Other document types must not be
        rejected for missing FDA fields.
        """
        payload = make_valid_export_payload()
        payload["document_type"] = "cisa_advisory"
        payload["routing_label"] = "security_ops"
        # Replace extracted_fields with non-FDA content
        payload["extracted_fields"] = {
            "advisory_id": "AA-2024-001",
            "severity_level": "High",
        }
        result = validate_export_payload(payload)
        # Should pass — FDA field checks must not fire for non-FDA document types.
        assert result.valid is True, (
            f"Non-FDA document type should not be rejected for missing FDA fields; "
            f"got: {result.errors}"
        )


# ---------------------------------------------------------------------------
# validate_quarantine_record — quarantine contract compliance
# ---------------------------------------------------------------------------


class TestQuarantineRecordValidation:
    def test_correctly_shaped_quarantine_record_passes(self):
        """
        A Gold record with routing_label='quarantine', export_ready=False,
        and export_path=None must pass quarantine record validation per B-0 §6.
        """
        record = make_quarantine_gold_record()
        result = validate_quarantine_record(record)
        assert result.valid is True
        assert result.errors == []

    def test_quarantine_with_export_ready_true_fails(self):
        """
        A quarantine record must never have export_ready=True.
        Per B-0 §6: routing_label='quarantine' always accompanies export_ready=False.
        """
        record = make_quarantine_gold_record()
        record["export_ready"] = True
        result = validate_quarantine_record(record)
        assert result.valid is False
        assert any("export_ready" in e.lower() for e in result.errors)

    def test_quarantine_with_non_quarantine_routing_label_fails(self):
        """
        A record claiming to be quarantine but with routing_label != 'quarantine'
        is inconsistent and must fail quarantine validation.
        """
        record = make_quarantine_gold_record()
        record["routing_label"] = "regulatory_review"
        result = validate_quarantine_record(record)
        assert result.valid is False
        assert any("routing_label" in e.lower() for e in result.errors)

    def test_quarantine_with_export_path_set_fails(self):
        """
        Quarantine records must not have an export_path. Only export_ready=True
        records produce export files per B-0 §7.
        """
        record = make_quarantine_gold_record()
        record["export_path"] = "output/gold/exports/quarantine/b2c3d4e5.json"
        result = validate_quarantine_record(record)
        assert result.valid is False
        assert any("export_path" in e.lower() for e in result.errors)

    def test_quarantine_record_fails_as_export_payload(self):
        """
        A record with routing_label='quarantine' must fail export_payload validation.
        Quarantine records are governance signals, not handoff units (B-0 §3).
        """
        record = make_quarantine_gold_record()
        # Construct a payload-shaped version of the quarantine record
        payload = {
            "document_id": record["document_id"],
            "source_file": "some_file.pdf",
            "document_type": record["document_type_label"],
            "routing_label": "quarantine",
            "extracted_fields": {},
            "parsed_text_excerpt": "",
            "provenance": {
                "ingested_at": "2024-01-01T00:00:00Z",
                "pipeline_run_id": "local-run-xyz",
                "extraction_model": "local_rule_extractor/v1",
                "classification_model": "local_rule_classifier/v1",
                "classification_confidence": None,
                "schema_version": "v0.1.0",
            },
        }
        result = validate_export_payload(payload)
        assert result.valid is False
        assert any("quarantine" in e.lower() for e in result.errors)

    def test_non_dict_quarantine_record_fails(self):
        """Non-dict input to quarantine validator must fail."""
        result = validate_quarantine_record(None)
        assert result.valid is False


# ---------------------------------------------------------------------------
# Integration-style: Gold schema alignment
# ---------------------------------------------------------------------------


class TestGoldSchemaContractAlignment:
    """
    Verify that ExportProvenance and GoldRecord from gold_schema.py correctly
    support Optional[float] classification_confidence, matching B-0 §4.3 and §9.
    """

    def test_export_provenance_accepts_null_confidence(self):
        """ExportProvenance must accept classification_confidence=None."""
        from src.schemas.gold_schema import ExportProvenance

        prov = ExportProvenance(
            ingested_at="2024-01-01T00:00:00Z",
            pipeline_run_id="bootstrap_sql_v1",
            extraction_model="ai_extract/v1",
            classification_model="ai_classify/v1",
            classification_confidence=None,
            schema_version="v0.1.0",
        )
        assert prov.classification_confidence is None

    def test_export_provenance_accepts_float_confidence(self):
        """ExportProvenance must accept a valid float classification_confidence."""
        from src.schemas.gold_schema import ExportProvenance

        prov = ExportProvenance(
            ingested_at="2024-01-01T00:00:00Z",
            pipeline_run_id="mlflow-run-abc",
            extraction_model="ai_extract/v1",
            classification_model="ai_classify/v1",
            classification_confidence=0.92,
            schema_version="v0.1.0",
        )
        assert prov.classification_confidence == 0.92

    def test_export_provenance_rejects_out_of_range_confidence(self):
        """ExportProvenance must reject classification_confidence outside [0.0, 1.0]."""
        from pydantic import ValidationError
        from src.schemas.gold_schema import ExportProvenance

        with pytest.raises(ValidationError):
            ExportProvenance(
                ingested_at="2024-01-01T00:00:00Z",
                pipeline_run_id="mlflow-run-abc",
                extraction_model="ai_extract/v1",
                classification_model="ai_classify/v1",
                classification_confidence=1.5,
                schema_version="v0.1.0",
            )

    def test_gold_record_accepts_null_confidence(self):
        """GoldRecord must accept classification_confidence=None (bootstrap path)."""
        from datetime import datetime, timezone
        from src.schemas.gold_schema import ExportPayload, ExportProvenance, GoldRecord

        provenance = ExportProvenance(
            ingested_at="2024-01-01T00:00:00Z",
            pipeline_run_id="bootstrap_sql_v1",
            extraction_model="ai_extract/v1",
            classification_model="ai_classify/v1",
            classification_confidence=None,
            schema_version="v0.1.0",
        )
        payload = ExportPayload(
            document_id="a1b2c3d4-0000-0000-0000-000000000001",
            source_file="test.pdf",
            document_type="fda_warning_letter",
            routing_label="regulatory_review",
            extracted_fields={"issuing_office": "FDA"},
            parsed_text_excerpt="test",
            provenance=provenance,
        )
        record = GoldRecord(
            document_id="a1b2c3d4-0000-0000-0000-000000000001",
            bronze_record_id="b2c3d4e5-0000-0000-0000-000000000002",
            extraction_id="c3d4e5f6-0000-0000-0000-000000000003",
            gold_record_id="d4e5f6a7-0000-0000-0000-000000000004",
            pipeline_run_id="bootstrap_sql_v1",
            classified_at=datetime.now(tz=timezone.utc),
            document_type_label="fda_warning_letter",
            routing_label="regulatory_review",
            classification_confidence=None,
            classification_model="ai_classify/v1",
            export_payload=payload,
            export_ready=True,
        )
        assert record.classification_confidence is None

    def test_no_bedrock_sdk_or_aws_import(self):
        """
        The bedrock_contract module must not contain import statements for any
        AWS/Bedrock SDK package. The module may reference 'Bedrock' in docstrings
        and comments but must not import live integration libraries.
        This test guards against accidentally introducing live integration.
        """
        import importlib.util
        import re

        import src.schemas.bedrock_contract as contract_module

        module_source = Path(
            contract_module.__file__
        ).read_text(encoding="utf-8")

        # Match only actual import lines (import X or from X import Y)
        import_lines = [
            line.strip()
            for line in module_source.splitlines()
            if re.match(r"^\s*(import|from)\s+", line)
        ]

        forbidden_packages = ["boto3", "botocore", "bedrock_runtime", "sagemaker", "awscrt"]
        for pkg in forbidden_packages:
            for imp_line in import_lines:
                assert pkg not in imp_line.lower(), (
                    f"bedrock_contract.py must not import '{pkg}' — "
                    f"no live Bedrock/AWS integration in B-1. Found: {imp_line!r}"
                )
