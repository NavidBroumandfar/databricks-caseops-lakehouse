"""
tests/test_b3_export_handoff.py — B-3 export packaging and handoff service boundary tests.

B-3 purpose: verify that the export/handoff materialization behavior extracted into
src/pipelines/export_handoff.py preserves B-2 behavior, has a clean module boundary,
and correctly separates classification from export packaging.

What these tests cover:
  - compute_export_path() is deterministic and follows the B-0 §7 pattern
  - write_export_artifact() writes to the correct path
  - execute_export() with a valid export-ready Gold record writes the artifact
  - execute_export() with a contract-blocked Gold record blocks the write and returns errors
  - execute_export() with a quarantine Gold record produces no export file
  - execute_export() with a non-export-ready (non-quarantine) record produces no export file
  - export_handoff.py contains no AWS/Bedrock SDK imports (B-2 boundary preserved)
  - classify_gold.py delegates to execute_export (integration via run_classify_gold)
  - ExportResult fields are correct in all cases

What these tests do NOT imply:
  - No live Bedrock/AWS integration exists or is required
  - No S3, boto3, or Bedrock SDK usage
  - These tests validate local module structure and materialization behavior only

Authoritative contract: docs/bedrock-handoff-contract.md
Contract validator: src/schemas/bedrock_contract.py (B-1)
Export module under test: src/pipelines/export_handoff.py (B-3)
Pipeline integration: src/pipelines/classify_gold.py (B-3 delegation)
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.pipelines.export_handoff import (
    ExportResult,
    compute_export_path,
    execute_export,
    write_export_artifact,
)
from src.pipelines.classify_gold import run_classify_gold
from src.schemas.gold_schema import (
    ExportPayload,
    ExportProvenance,
    GoldRecord,
    SCHEMA_VERSION,
)


# ---------------------------------------------------------------------------
# Gold record fixture helpers
# ---------------------------------------------------------------------------


def make_valid_export_gold_record(document_id: str = "aaaa0001-0000-0000-0000-000000000001") -> GoldRecord:
    """
    Return a GoldRecord in a valid export-ready state.

    All required FDA extracted_fields are present. routing_label is
    'regulatory_review'. export_ready=True. Contract validation will pass.
    """
    provenance = ExportProvenance(
        ingested_at="2024-03-20T14:30:00Z",
        pipeline_run_id="local-run-b3-test",
        extraction_model="local_rule_extractor/v1",
        classification_model="local_rule_classifier/v1",
        classification_confidence=0.90,
        schema_version=SCHEMA_VERSION,
    )
    payload = ExportPayload(
        document_id=document_id,
        source_file="fda_warning_letter_sample.pdf",
        document_type="fda_warning_letter",
        routing_label="regulatory_review",
        extracted_fields={
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "TestCorp Pharma Inc.",
            "issue_date": "2024-03-15",
            "violation_type": ["Current Good Manufacturing Practice"],
            "corrective_action_requested": True,
        },
        parsed_text_excerpt="WARNING LETTER\n\nIssued: March 15, 2024",
        provenance=provenance,
    )
    return GoldRecord(
        document_id=document_id,
        bronze_record_id="bbbb0001-0000-0000-0000-000000000001",
        extraction_id="cccc0001-0000-0000-0000-000000000001",
        gold_record_id="dddd0001-0000-0000-0000-000000000001",
        pipeline_run_id="local-run-b3-test",
        classified_at=datetime.now(tz=timezone.utc),
        document_type_label="fda_warning_letter",
        routing_label="regulatory_review",
        classification_confidence=0.90,
        classification_model="local_rule_classifier/v1",
        export_payload=payload,
        export_ready=True,
        export_path=None,
        schema_version=SCHEMA_VERSION,
    )


def make_contract_blocked_gold_record(
    document_id: str = "aaaa0002-0000-0000-0000-000000000001",
) -> GoldRecord:
    """
    Return a GoldRecord with export_ready=True but whose export_payload will
    FAIL B-1 contract validation (violation_type is an empty list,
    corrective_action_requested is None).

    This models the canonical B-2/B-3 contract block scenario.
    """
    provenance = ExportProvenance(
        ingested_at="2024-03-20T14:30:00Z",
        pipeline_run_id="local-run-b3-block",
        extraction_model="local_rule_extractor/v1",
        classification_model="local_rule_classifier/v1",
        classification_confidence=0.85,
        schema_version=SCHEMA_VERSION,
    )
    payload = ExportPayload(
        document_id=document_id,
        source_file="fda_warning_letter_blocked.pdf",
        document_type="fda_warning_letter",
        routing_label="regulatory_review",
        extracted_fields={
            "issuing_office": "Office of Compliance",
            "recipient_company": "BlockedCorp Inc.",
            "issue_date": "2024-03-15",
            "violation_type": [],              # empty — fails contract §4.4
            "corrective_action_requested": None,  # null — fails contract §4.4
        },
        parsed_text_excerpt="WARNING LETTER\n\nBlockedCorp Inc.",
        provenance=provenance,
    )
    return GoldRecord(
        document_id=document_id,
        bronze_record_id="bbbb0002-0000-0000-0000-000000000001",
        extraction_id="cccc0002-0000-0000-0000-000000000001",
        gold_record_id="dddd0002-0000-0000-0000-000000000001",
        pipeline_run_id="local-run-b3-block",
        classified_at=datetime.now(tz=timezone.utc),
        document_type_label="fda_warning_letter",
        routing_label="regulatory_review",
        classification_confidence=0.85,
        classification_model="local_rule_classifier/v1",
        export_payload=payload,
        export_ready=True,  # routing passed; contract will block
        export_path=None,
        schema_version=SCHEMA_VERSION,
    )


def make_quarantine_gold_record(
    document_id: str = "aaaa0003-0000-0000-0000-000000000001",
) -> GoldRecord:
    """
    Return a GoldRecord correctly shaped as a quarantine record.

    routing_label='quarantine', export_ready=False. No export file expected.
    """
    provenance = ExportProvenance(
        ingested_at="2024-03-20T14:30:00Z",
        pipeline_run_id="local-run-b3-quarantine",
        extraction_model="local_rule_extractor/v1",
        classification_model="local_rule_classifier/v1",
        classification_confidence=0.0,
        schema_version=SCHEMA_VERSION,
    )
    payload = ExportPayload(
        document_id=document_id,
        source_file="unknown_document.pdf",
        document_type="unknown",
        routing_label="quarantine",
        extracted_fields={},
        parsed_text_excerpt="",
        provenance=provenance,
    )
    return GoldRecord(
        document_id=document_id,
        bronze_record_id="bbbb0003-0000-0000-0000-000000000001",
        extraction_id="cccc0003-0000-0000-0000-000000000001",
        gold_record_id="dddd0003-0000-0000-0000-000000000001",
        pipeline_run_id="local-run-b3-quarantine",
        classified_at=datetime.now(tz=timezone.utc),
        document_type_label="unknown",
        routing_label="quarantine",
        classification_confidence=0.0,
        classification_model="local_rule_classifier/v1",
        export_payload=payload,
        export_ready=False,
        export_path=None,
        schema_version=SCHEMA_VERSION,
    )


def make_non_export_ready_gold_record(
    document_id: str = "aaaa0004-0000-0000-0000-000000000001",
) -> GoldRecord:
    """
    Return a GoldRecord with export_ready=False and routing_label != 'quarantine'.

    This covers the case where a non-quarantine record fails export readiness
    (e.g., low coverage or low confidence) without being routed to quarantine.
    """
    provenance = ExportProvenance(
        ingested_at="2024-03-20T14:30:00Z",
        pipeline_run_id="local-run-b3-low-coverage",
        extraction_model="local_rule_extractor/v1",
        classification_model="local_rule_classifier/v1",
        classification_confidence=0.60,
        schema_version=SCHEMA_VERSION,
    )
    payload = ExportPayload(
        document_id=document_id,
        source_file="fda_warning_letter_low_cov.pdf",
        document_type="fda_warning_letter",
        routing_label="regulatory_review",
        extracted_fields={
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "LowCovCorp Inc.",
            "issue_date": "2024-03-15",
            "violation_type": ["cGMP"],
            "corrective_action_requested": True,
        },
        parsed_text_excerpt="WARNING LETTER",
        provenance=provenance,
    )
    return GoldRecord(
        document_id=document_id,
        bronze_record_id="bbbb0004-0000-0000-0000-000000000001",
        extraction_id="cccc0004-0000-0000-0000-000000000001",
        gold_record_id="dddd0004-0000-0000-0000-000000000001",
        pipeline_run_id="local-run-b3-low-coverage",
        classified_at=datetime.now(tz=timezone.utc),
        document_type_label="fda_warning_letter",
        routing_label="regulatory_review",
        classification_confidence=0.60,
        classification_model="local_rule_classifier/v1",
        export_payload=payload,
        export_ready=False,  # failed export readiness threshold (not quarantine)
        export_path=None,
        schema_version=SCHEMA_VERSION,
    )


# ---------------------------------------------------------------------------
# Silver fixture helper (for run_classify_gold integration tests)
# ---------------------------------------------------------------------------


def write_silver_fixture(tmp_path: Path, record: dict) -> Path:
    """Write a Silver fixture JSON to a temp directory and return the path."""
    p = tmp_path / f"{record['document_id']}.json"
    p.write_text(json.dumps(record), encoding="utf-8")
    return p


def make_valid_silver_record() -> dict:
    """Minimal valid Silver record for integration tests via run_classify_gold."""
    return {
        "document_id": "aaaa1001-0000-0000-0000-000000000001",
        "bronze_record_id": "bbbb1001-0000-0000-0000-000000000001",
        "extraction_id": "cccc1001-0000-0000-0000-000000000001",
        "pipeline_run_id": "local-run-b3-integration",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.85,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "IntegrationCorp Inc.",
            "issue_date": "2024-06-01",
            "violation_type": ["Current Good Manufacturing Practice"],
            "corrective_action_requested": True,
        },
    }


# ---------------------------------------------------------------------------
# B-3 Test: compute_export_path is deterministic
# ---------------------------------------------------------------------------


class TestComputeExportPath:
    def test_path_follows_routing_label_document_id_pattern(self, tmp_path: Path):
        """
        compute_export_path must follow: <base>/<routing_label>/<document_id>.json
        This is the authoritative B-0 §7 delivery path pattern.
        """
        base = tmp_path / "exports"
        result = compute_export_path(base, "regulatory_review", "abc-123")
        assert result == base / "regulatory_review" / "abc-123.json"

    def test_path_is_deterministic(self, tmp_path: Path):
        """
        compute_export_path returns the same path for the same inputs every time.
        No randomness or timestamps in the path.
        """
        base = tmp_path / "exports"
        path1 = compute_export_path(base, "regulatory_review", "doc-xyz")
        path2 = compute_export_path(base, "regulatory_review", "doc-xyz")
        assert path1 == path2

    def test_different_routing_labels_produce_different_paths(self, tmp_path: Path):
        """Different routing labels produce different subdirectories."""
        base = tmp_path / "exports"
        p1 = compute_export_path(base, "regulatory_review", "doc-001")
        p2 = compute_export_path(base, "security_ops", "doc-001")
        assert p1 != p2
        assert p1.parent.name == "regulatory_review"
        assert p2.parent.name == "security_ops"

    def test_different_document_ids_produce_different_filenames(self, tmp_path: Path):
        """Different document IDs produce different file names within the same label dir."""
        base = tmp_path / "exports"
        p1 = compute_export_path(base, "regulatory_review", "doc-001")
        p2 = compute_export_path(base, "regulatory_review", "doc-002")
        assert p1.name == "doc-001.json"
        assert p2.name == "doc-002.json"
        assert p1.parent == p2.parent


# ---------------------------------------------------------------------------
# B-3 Test: write_export_artifact writes to the correct path
# ---------------------------------------------------------------------------


class TestWriteExportArtifact:
    def test_writes_to_deterministic_path(self, tmp_path: Path):
        """write_export_artifact places the file at the compute_export_path location."""
        record = make_valid_export_gold_record()
        base = tmp_path / "exports"

        written_path = write_export_artifact(record, base)

        expected_path = compute_export_path(base, record.routing_label, record.document_id)
        assert written_path == expected_path
        assert written_path.exists()

    def test_written_file_is_valid_json(self, tmp_path: Path):
        """The written export artifact must be valid JSON."""
        record = make_valid_export_gold_record()
        written_path = write_export_artifact(record, tmp_path / "exports")

        content = json.loads(written_path.read_text(encoding="utf-8"))
        assert isinstance(content, dict)
        assert content["document_id"] == record.document_id
        assert content["routing_label"] == record.routing_label

    def test_creates_routing_label_subdirectory(self, tmp_path: Path):
        """write_export_artifact creates the routing-label subdirectory if absent."""
        record = make_valid_export_gold_record()
        base = tmp_path / "exports"
        assert not base.exists()

        write_export_artifact(record, base)

        assert (base / record.routing_label).is_dir()


# ---------------------------------------------------------------------------
# B-3 Test: execute_export with valid export-ready record
# ---------------------------------------------------------------------------


class TestExecuteExportValid:
    def test_valid_record_writes_artifact(self, tmp_path: Path):
        """A valid export-ready Gold record must produce a written export artifact."""
        record = make_valid_export_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_artifact_path is not None
        assert result.export_artifact_path.exists()

    def test_valid_record_result_is_export_ready(self, tmp_path: Path):
        """ExportResult from a valid record must have export_ready=True."""
        record = make_valid_export_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_ready is True

    def test_valid_record_no_contract_errors(self, tmp_path: Path):
        """ExportResult from a valid record must have empty contract_validation_errors."""
        record = make_valid_export_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.contract_validation_errors == []

    def test_valid_record_export_path_matches_artifact_path(self, tmp_path: Path):
        """export_path (str) must match the export_artifact_path (Path) in the result."""
        record = make_valid_export_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_path is not None
        assert result.export_path == str(result.export_artifact_path)

    def test_valid_record_written_payload_is_contract_valid(self, tmp_path: Path):
        """The export payload written to disk must pass contract validation."""
        from src.schemas.bedrock_contract import validate_export_payload

        record = make_valid_export_gold_record()
        result = execute_export(record, tmp_path / "exports")

        payload = json.loads(result.export_artifact_path.read_text(encoding="utf-8"))
        contract_result = validate_export_payload(payload)
        assert contract_result.valid is True, (
            f"Written export artifact failed contract validation: {contract_result.errors}"
        )


# ---------------------------------------------------------------------------
# B-3 Test: execute_export with contract-blocked record
# ---------------------------------------------------------------------------


class TestExecuteExportContractBlocked:
    def test_blocked_record_does_not_write_artifact(self, tmp_path: Path):
        """A contract-blocked Gold record must NOT produce an export artifact on disk."""
        record = make_contract_blocked_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_artifact_path is None

        # No files should be on disk
        if (tmp_path / "exports").exists():
            assert list((tmp_path / "exports").rglob("*.json")) == []

    def test_blocked_record_result_is_not_export_ready(self, tmp_path: Path):
        """ExportResult from a contract-blocked record must have export_ready=False."""
        record = make_contract_blocked_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_ready is False

    def test_blocked_record_has_contract_errors(self, tmp_path: Path):
        """ExportResult from a blocked record must have non-empty contract_validation_errors."""
        record = make_contract_blocked_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert len(result.contract_validation_errors) > 0
        assert all(isinstance(e, str) for e in result.contract_validation_errors)

    def test_blocked_record_errors_name_violated_fields(self, tmp_path: Path):
        """Contract errors must name the specific violated fields."""
        record = make_contract_blocked_gold_record()
        result = execute_export(record, tmp_path / "exports")

        errors_str = " ".join(result.contract_validation_errors)
        assert "violation_type" in errors_str or "corrective_action_requested" in errors_str, (
            f"Expected errors mentioning the violated fields; got: {result.contract_validation_errors}"
        )

    def test_blocked_record_export_path_is_none(self, tmp_path: Path):
        """export_path must be None when the record is contract-blocked."""
        record = make_contract_blocked_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_path is None


# ---------------------------------------------------------------------------
# B-3 Test: execute_export with quarantine record
# ---------------------------------------------------------------------------


class TestExecuteExportQuarantine:
    def test_quarantine_record_does_not_write_artifact(self, tmp_path: Path):
        """A quarantine Gold record must not produce an export artifact."""
        record = make_quarantine_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_artifact_path is None

    def test_quarantine_record_result_is_not_export_ready(self, tmp_path: Path):
        """ExportResult from a quarantine record must have export_ready=False."""
        record = make_quarantine_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_ready is False

    def test_quarantine_record_no_contract_errors(self, tmp_path: Path):
        """Quarantine records are not attempted as exports — no contract errors expected."""
        record = make_quarantine_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.contract_validation_errors == []

    def test_quarantine_does_not_create_quarantine_export_subdir(self, tmp_path: Path):
        """No quarantine export subdirectory should be created."""
        record = make_quarantine_gold_record()
        execute_export(record, tmp_path / "exports")

        quarantine_dir = tmp_path / "exports" / "quarantine"
        assert not quarantine_dir.exists(), (
            "Quarantine records must not create an export subdirectory"
        )


# ---------------------------------------------------------------------------
# B-3 Test: execute_export with non-export-ready non-quarantine record
# ---------------------------------------------------------------------------


class TestExecuteExportNonExportReady:
    def test_non_export_ready_non_quarantine_no_artifact(self, tmp_path: Path):
        """
        A non-export-ready record that is also not quarantined must not write
        an export artifact or produce contract errors.
        """
        record = make_non_export_ready_gold_record()
        result = execute_export(record, tmp_path / "exports")

        assert result.export_artifact_path is None
        assert result.export_ready is False
        assert result.contract_validation_errors == []
        assert result.export_path is None


# ---------------------------------------------------------------------------
# B-3 Test: module boundary — no AWS/Bedrock imports in export_handoff.py
# ---------------------------------------------------------------------------


class TestModuleBoundary:
    def test_export_handoff_has_no_aws_or_bedrock_imports(self):
        """
        export_handoff.py must not import any AWS/Bedrock SDK packages.
        The module is strictly upstream export packaging — local validation and
        file write only. No live integration is permitted.
        """
        handoff_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "export_handoff.py"
        )
        source = handoff_path.read_text(encoding="utf-8")
        import_lines = [
            line.strip()
            for line in source.splitlines()
            if re.match(r"^\s*(import|from)\s+", line)
        ]

        forbidden_packages = ["boto3", "botocore", "bedrock_runtime", "sagemaker", "awscrt"]
        for pkg in forbidden_packages:
            for imp_line in import_lines:
                assert pkg not in imp_line.lower(), (
                    f"export_handoff.py must not import '{pkg}' — "
                    f"no live Bedrock/AWS integration in B-3. Found: {imp_line!r}"
                )

    def test_classify_gold_delegates_to_execute_export(self):
        """
        classify_gold.py must import and call execute_export from export_handoff.
        This confirms the B-3 delegation boundary is in place.
        """
        pipeline_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "classify_gold.py"
        )
        source = pipeline_path.read_text(encoding="utf-8")

        assert "from src.pipelines.export_handoff import" in source, (
            "classify_gold.py must import from export_handoff (B-3 boundary)"
        )
        assert "execute_export" in source, (
            "classify_gold.py must call execute_export (B-3 delegation)"
        )

    def test_export_handoff_uses_contract_validator(self):
        """
        export_handoff.py must use the B-1 contract validator (validate_export_payload)
        and quarantine validator (validate_quarantine_record), not any live SDK.
        """
        handoff_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "export_handoff.py"
        )
        source = handoff_path.read_text(encoding="utf-8")

        assert "validate_export_payload" in source
        assert "validate_quarantine_record" in source
        assert "from src.schemas.bedrock_contract import" in source

    def test_classify_gold_does_not_directly_call_contract_validators(self):
        """
        After B-3, classify_gold.py must not directly call validate_export_payload or
        validate_quarantine_record — those are owned by export_handoff.py.
        """
        pipeline_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "pipelines"
            / "classify_gold.py"
        )
        source = pipeline_path.read_text(encoding="utf-8")

        assert "validate_export_payload" not in source, (
            "classify_gold.py must not directly call validate_export_payload after B-3 — "
            "it delegates to execute_export in export_handoff.py"
        )
        assert "validate_quarantine_record" not in source, (
            "classify_gold.py must not directly call validate_quarantine_record after B-3 — "
            "it delegates to execute_export in export_handoff.py"
        )


# ---------------------------------------------------------------------------
# B-3 Integration Test: run_classify_gold still works via execute_export delegation
# ---------------------------------------------------------------------------


class TestRunClassifyGoldIntegration:
    """
    Verify that the classify_gold pipeline integration remains correct after
    the B-3 delegation refactor. These tests exercise the full pipeline path
    to confirm execute_export is wired correctly.
    """

    def test_valid_silver_produces_export_artifact_via_execute_export(self, tmp_path: Path):
        """
        Integration: valid Silver → classify_gold → execute_export → export artifact written.
        Confirms the delegation wiring works end-to-end.
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
        assert Path(s["export_artifact_path"]).exists()

    def test_pipeline_export_path_follows_deterministic_pattern(self, tmp_path: Path):
        """
        Integration: export path from run_classify_gold follows
        <export_dir>/<routing_label>/<document_id>.json pattern.
        """
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        silver = make_valid_silver_record()
        write_silver_fixture(silver_dir, silver)

        export_dir = tmp_path / "gold" / "exports"

        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(export_dir),
        )

        s = summaries[0]
        export_path = Path(s["export_artifact_path"])
        assert export_path.parent.name == s["routing_label"]
        assert export_path.name == f"{silver['document_id']}.json"
