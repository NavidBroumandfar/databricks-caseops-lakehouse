"""
tests/test_b2_export_materialization.py — B-2 contract-enforced export materialization tests.

B-2 purpose: validate that the real Gold export-writing path obeys the B-1 contract.
Export artifacts are only written when the payload passes contract validation.
Invalid payloads are explicitly blocked. Quarantine records produce no export file.

What these tests cover:
- Valid export-ready payload is validated and written successfully
- Invalid export-ready payload is blocked from being written
- Quarantine record does not get written as a downstream export artifact
- Export path generation is deterministic and correct
- Contract block is visible and explicit in the pipeline summary
- No live Bedrock integration is implied or required

What these tests do NOT imply:
- No live Bedrock/AWS integration exists
- No S3, boto3, or Bedrock SDK usage
- These tests validate local pipeline materialization behavior only

Authoritative contract: docs/bedrock-handoff-contract.md
Contract validator: src/schemas/bedrock_contract.py (B-1)
Pipeline under test: src/pipelines/classify_gold.py (B-2 enforcement)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.pipelines.classify_gold import (
    EXPORT_CONFIDENCE_THRESHOLD,
    ROUTING_LABEL_QUARANTINE,
    assemble_gold_record,
    build_export_payload,
    resolve_bronze_metadata,
    run_classify_gold,
    write_gold_artifact,
)
from src.pipelines.export_handoff import write_export_artifact
from src.schemas.bedrock_contract import (
    validate_export_payload,
    validate_quarantine_record,
)


# ---------------------------------------------------------------------------
# Silver fixture helpers
# ---------------------------------------------------------------------------


def make_valid_silver_record() -> dict:
    """
    Return a minimal Silver record that the local FDA classifier will classify
    as export-ready (fda_warning_letter / regulatory_review).

    All required FDA extracted fields are present and non-null.
    field_coverage_pct and validation_status ensure export_ready=True.
    """
    return {
        "document_id": "aaaabbbb-0000-0000-0000-000000000001",
        "bronze_record_id": "aaaabbbb-0000-0000-0000-000000000002",
        "extraction_id": "aaaabbbb-0000-0000-0000-000000000003",
        "pipeline_run_id": "local-run-test-b2",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.85,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "TestCorp Pharma Inc.",
            "issue_date": "2024-06-01",
            "violation_type": ["Current Good Manufacturing Practice"],
            "corrective_action_requested": True,
            "recipient_name": "Jane Doe",
            "product_involved": "Acetaminophen Tablets 500 mg",
        },
    }


def make_contract_blocking_silver_record() -> dict:
    """
    Return a Silver record that:
    - The local classifier will classify as fda_warning_letter / export_ready=True
      (class_hint + 3 populated FDA fields + valid + coverage all fire)
    - BUT the assembled export payload will FAIL B-1 contract validation
      because violation_type is an empty array and corrective_action_requested is null

    This is the canonical B-2 test scenario: pipeline routing passes, contract
    validation catches the malformed payload and blocks the write.
    """
    return {
        "document_id": "ccccdddd-0000-0000-0000-000000000001",
        "bronze_record_id": "ccccdddd-0000-0000-0000-000000000002",
        "extraction_id": "ccccdddd-0000-0000-0000-000000000003",
        "pipeline_run_id": "local-run-test-b2-block",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.80,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Compliance",
            "recipient_company": "BlockedCorp Inc.",
            "issue_date": "2024-06-01",
            # violation_type is empty — passes routing (field present) but fails contract §4.4
            "violation_type": [],
            # corrective_action_requested is null — fails contract §4.4
            "corrective_action_requested": None,
        },
    }


def make_quarantine_silver_record() -> dict:
    """
    Return a Silver record whose document_class_hint produces an 'unknown' classification,
    routing to quarantine. No export file should be written.
    """
    return {
        "document_id": "eeeeffff-0000-0000-0000-000000000001",
        "bronze_record_id": "eeeeffff-0000-0000-0000-000000000002",
        "extraction_id": "eeeeffff-0000-0000-0000-000000000003",
        "pipeline_run_id": "local-run-test-b2-quarantine",
        "document_class_hint": None,
        "validation_status": "valid",
        "field_coverage_pct": 0.10,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {},  # no signals → classified as 'unknown' → quarantine
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_silver_fixture(tmp_path: Path, record: dict) -> Path:
    """Write a Silver fixture JSON to a temp directory and return the path."""
    p = tmp_path / f"{record['document_id']}.json"
    p.write_text(json.dumps(record), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# B-2 Test: valid export-ready payload is written
# ---------------------------------------------------------------------------


class TestValidExportIsWritten:
    def test_valid_silver_produces_export_artifact(self, tmp_path: Path):
        """
        A valid Silver record with all required FDA fields produces a Gold record
        with export_ready=True and an export artifact written to the correct path.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        assert len(summaries) == 1
        s = summaries[0]
        assert s["export_ready"] is True
        assert s["export_artifact_path"] is not None
        assert s["contract_validation_errors"] == []

        # Verify the export file actually exists on disk
        export_path = Path(s["export_artifact_path"])
        assert export_path.exists(), f"Export artifact not found at {export_path}"

        # Verify the file content is a valid JSON payload
        payload = json.loads(export_path.read_text(encoding="utf-8"))
        assert payload["document_id"] == "aaaabbbb-0000-0000-0000-000000000001"
        assert payload["routing_label"] == "regulatory_review"

    def test_valid_export_payload_passes_contract_validation(self, tmp_path: Path):
        """
        The export artifact written by the pipeline must pass contract validation.
        This confirms the write path produces contract-compliant artifacts.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert s["export_artifact_path"] is not None
        payload = json.loads(Path(s["export_artifact_path"]).read_text(encoding="utf-8"))

        result = validate_export_payload(payload)
        assert result.valid is True, (
            f"Written export payload failed contract validation: {result.errors}"
        )

    def test_gold_record_export_path_matches_export_artifact_path(self, tmp_path: Path):
        """
        The export_path field in the Gold record must match the actual export artifact path.
        The Gold record is the authoritative record of where the export was materialized.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        gold_record_data = json.loads(
            Path(s["gold_artifact_path"]).read_text(encoding="utf-8")
        )

        assert gold_record_data["export_path"] == s["export_artifact_path"]
        assert gold_record_data["export_ready"] is True


# ---------------------------------------------------------------------------
# B-2 Test: invalid export payload is blocked from being written
# ---------------------------------------------------------------------------


class TestContractBlockPreventsWrite:
    def test_invalid_payload_is_not_written(self, tmp_path: Path):
        """
        A Silver record that produces an export payload failing B-1 contract
        validation must NOT result in an export artifact being written to disk.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        assert len(summaries) == 1
        s = summaries[0]

        # Export artifact must not be written
        assert s["export_artifact_path"] is None, (
            "Invalid payload must not be written as a Bedrock handoff artifact"
        )
        assert s["export_ready"] is False, (
            "Contract block must demote export_ready to False"
        )

    def test_contract_block_surfaces_errors_in_summary(self, tmp_path: Path):
        """
        When contract validation blocks a write, the contract_validation_errors
        field in the summary must be non-empty and explicitly name the violations.
        Failure must be explicit, not silent.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert len(s["contract_validation_errors"]) > 0, (
            "Contract validation errors must be present and non-empty in the summary"
        )
        # The errors must mention the violated fields
        errors_str = " ".join(s["contract_validation_errors"])
        assert "violation_type" in errors_str or "corrective_action_requested" in errors_str, (
            f"Expected errors mentioning violated fields; got: {s['contract_validation_errors']}"
        )

    def test_gold_record_written_even_when_contract_blocks_export(self, tmp_path: Path):
        """
        A Gold record is always written (it is the full governance record).
        Contract blocking only prevents the export artifact from being written.
        The Gold record's export_ready=False and export_path=None reflect the block.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        gold_path = Path(s["gold_artifact_path"])
        assert gold_path.exists(), "Gold record must be written even when export is blocked"

        gold_data = json.loads(gold_path.read_text(encoding="utf-8"))
        assert gold_data["export_ready"] is False
        assert gold_data["export_path"] is None

    def test_export_dir_empty_when_contract_blocks(self, tmp_path: Path):
        """
        When contract validation blocks all exports, no files should exist under
        the export directory. No invalid payloads should appear on disk.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        # The export directory should not contain any JSON files
        if export_dir.exists():
            export_files = list(export_dir.rglob("*.json"))
            assert export_files == [], (
                f"No export files should exist when contract blocks write; "
                f"found: {export_files}"
            )


# ---------------------------------------------------------------------------
# B-2 Test: quarantine record not written as downstream export artifact
# ---------------------------------------------------------------------------


class TestQuarantineRecordNotWrittenAsExport:
    def test_quarantine_silver_produces_no_export_artifact(self, tmp_path: Path):
        """
        A Silver record classified as 'unknown' (quarantine) must not produce
        an export artifact. Quarantine records are governance signals, not
        Bedrock handoff units.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        assert len(summaries) == 1
        s = summaries[0]
        assert s["export_ready"] is False
        assert s["export_artifact_path"] is None
        assert s["routing_label"] == "quarantine"

    def test_quarantine_gold_record_has_correct_shape(self, tmp_path: Path):
        """
        The Gold record for a quarantine case must satisfy the B-1 quarantine
        shape contract: export_ready=False, routing_label='quarantine', export_path=None.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        gold_data = json.loads(
            Path(s["gold_artifact_path"]).read_text(encoding="utf-8")
        )

        quarantine_result = validate_quarantine_record(gold_data)
        assert quarantine_result.valid is True, (
            f"Quarantine Gold record failed shape validation: {quarantine_result.errors}"
        )

    def test_quarantine_gold_record_not_a_valid_handoff_unit(self, tmp_path: Path):
        """
        The export_payload embedded in a quarantine Gold record must fail
        validate_export_payload() — it must not pass as a valid handoff unit.
        This confirms quarantine records cannot accidentally be used as exports.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        gold_data = json.loads(
            Path(s["gold_artifact_path"]).read_text(encoding="utf-8")
        )

        # The embedded export_payload has routing_label='quarantine' — must fail handoff validation
        embedded_payload = gold_data.get("export_payload", {})
        handoff_result = validate_export_payload(embedded_payload)
        assert handoff_result.valid is False, (
            "Quarantine record's export_payload must not pass as a valid handoff unit"
        )
        assert any("quarantine" in e.lower() for e in handoff_result.errors)

    def test_quarantine_contract_errors_are_empty(self, tmp_path: Path):
        """
        Quarantine records are correctly routed by the pipeline — they are not
        contract-blocked. The contract_validation_errors field should be empty for
        quarantine records (the contract block applies only to export-ready records
        that then fail validation).
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        # No contract_validation_errors for quarantine — they are not attempted exports
        assert s["contract_validation_errors"] == [], (
            "Quarantine records should have no contract_validation_errors "
            "(they are not attempted as exports)"
        )


# ---------------------------------------------------------------------------
# B-2 Test: export path generation is deterministic
# ---------------------------------------------------------------------------


class TestExportPathGeneration:
    def test_export_path_follows_routing_label_document_id_pattern(self, tmp_path: Path):
        """
        Export artifact path must follow the deterministic pattern:
            <export_base_dir>/<routing_label>/<document_id>.json

        This pattern is defined in docs/bedrock-handoff-contract.md § 7 and
        ARCHITECTURE.md § Bedrock Handoff Design — Delivery Mechanism (V1).
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        silver = make_valid_silver_record()
        write_silver_fixture(silver_dir, silver)

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert s["export_artifact_path"] is not None

        export_path = Path(s["export_artifact_path"])
        # Path segments: .../exports/<routing_label>/<document_id>.json
        assert export_path.parent.name == s["routing_label"], (
            f"Expected export path parent to be '{s['routing_label']}'; "
            f"got '{export_path.parent.name}'"
        )
        assert export_path.name == f"{silver['document_id']}.json", (
            f"Expected export file name to be '{silver['document_id']}.json'; "
            f"got '{export_path.name}'"
        )

    def test_export_path_uses_routing_label_subdirectory(self, tmp_path: Path):
        """
        Export artifacts for regulatory_review routing must be written under
        the regulatory_review subdirectory. Bedrock consumers are expected to
        poll routing-label-specific subdirectories.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert s["routing_label"] == "regulatory_review"

        expected_export_subdir = export_dir / "regulatory_review"
        assert expected_export_subdir.exists(), (
            f"Expected regulatory_review subdirectory at {expected_export_subdir}"
        )

    def test_no_quarantine_subdir_created_for_quarantine_record(self, tmp_path: Path):
        """
        Quarantine records must not create a quarantine subdirectory under the
        export base. Quarantine records produce no export artifacts.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_quarantine_silver_record())

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        quarantine_export_dir = export_dir / "quarantine"
        assert not quarantine_export_dir.exists(), (
            "No quarantine export subdirectory should be created — "
            "quarantine records are not materialized as export files"
        )


# ---------------------------------------------------------------------------
# B-2 Test: summary contract_validation_errors field
# ---------------------------------------------------------------------------


class TestSummaryContractErrors:
    def test_valid_export_has_empty_contract_errors(self, tmp_path: Path):
        """
        Successful export summaries must have contract_validation_errors=[]
        (empty list). The field must always be present in the summary.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_valid_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert "contract_validation_errors" in s, (
            "contract_validation_errors must always be present in the summary"
        )
        assert s["contract_validation_errors"] == []

    def test_blocked_export_has_non_empty_contract_errors(self, tmp_path: Path):
        """
        When export is blocked by contract validation, contract_validation_errors
        must be a non-empty list of strings describing the violations.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        gold_dir = tmp_path / "gold"
        export_dir = tmp_path / "gold" / "exports"

        write_silver_fixture(silver_dir, make_contract_blocking_silver_record())

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        assert "contract_validation_errors" in s
        assert isinstance(s["contract_validation_errors"], list)
        assert len(s["contract_validation_errors"]) > 0
        assert all(isinstance(e, str) for e in s["contract_validation_errors"])


# ---------------------------------------------------------------------------
# B-2 Test: no live Bedrock/AWS integration in classify_gold.py
# ---------------------------------------------------------------------------


class TestNoBedrockIntegration:
    def test_classify_gold_has_no_aws_or_bedrock_imports(self):
        """
        classify_gold.py must not import any AWS/Bedrock SDK packages.
        This guards against accidentally introducing live integration into the
        upstream pipeline layer (B-2 boundary preservation).
        """
        import re

        pipeline_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "classify_gold.py"
        )
        source = pipeline_path.read_text(encoding="utf-8")
        import_lines = [
            line.strip()
            for line in source.splitlines()
            if re.match(r"^\s*(import|from)\s+", line)
        ]

        forbidden_packages = ["boto3", "botocore", "bedrock_runtime", "sagemaker", "awscrt"]
        for pkg in forbidden_packages:
            for imp_line in import_lines:
                assert pkg not in imp_line.lower(), (
                    f"classify_gold.py must not import '{pkg}' — "
                    f"no live Bedrock/AWS integration in B-2. Found: {imp_line!r}"
                )

    def test_contract_validator_imported_not_bedrock_sdk(self):
        """
        The B-2/B-3 enforcement path must use the local bedrock_contract validator
        (src/schemas/bedrock_contract.py), not any live SDK.

        After B-3: contract enforcement lives in export_handoff.py, which classify_gold.py
        delegates to via execute_export(). The structural validation chain is intact.
        """
        repo_root = Path(__file__).resolve().parents[1]

        # After B-3: export_handoff.py owns the contract validator imports.
        export_handoff_path = repo_root / "src" / "pipelines" / "export_handoff.py"
        handoff_source = export_handoff_path.read_text(encoding="utf-8")

        assert "from src.schemas.bedrock_contract import" in handoff_source, (
            "export_handoff.py must import from src.schemas.bedrock_contract "
            "(the B-1 contract validator) for B-2/B-3 enforcement"
        )
        assert "validate_export_payload" in handoff_source, (
            "validate_export_payload must be used in export_handoff.py (B-3 enforcement)"
        )
        assert "validate_quarantine_record" in handoff_source, (
            "validate_quarantine_record must be used in export_handoff.py (B-3 quarantine assertion)"
        )

        # classify_gold.py delegates export materialization to execute_export (B-3 boundary).
        pipeline_path = repo_root / "src" / "pipelines" / "classify_gold.py"
        pipeline_source = pipeline_path.read_text(encoding="utf-8")

        assert "from src.pipelines.export_handoff import" in pipeline_source, (
            "classify_gold.py must import from export_handoff (B-3 delegation boundary)"
        )
        assert "execute_export" in pipeline_source, (
            "classify_gold.py must delegate export materialization to execute_export (B-3)"
        )
