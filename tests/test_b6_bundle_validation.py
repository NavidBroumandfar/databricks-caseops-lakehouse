"""
tests/test_b6_bundle_validation.py — B-6 handoff bundle integrity and consistency validation tests.

B-6 purpose: verify that a B-5 HandoffBatchManifest can be checked locally and
deterministically for structural correctness, count consistency, reference integrity,
identifier uniqueness, and filesystem path existence.

What these tests cover:

  --- Constants and check name vocabulary ---
  - All CHECK_* constants exist and are non-empty strings
  - ALL_CHECK_NAMES includes all structural, count, reference, uniqueness, path checks
  - CHECK_* constants are distinct (no duplicates in ALL_CHECK_NAMES)

  --- CheckResult model ---
  - CheckResult has check_name, passed, detail fields
  - CheckResult.to_dict() is JSON-serializable
  - passed=True result has correct shape
  - passed=False result carries detail

  --- BundleValidationResult model ---
  - BundleValidationResult has all required fields
  - BundleValidationResult.to_dict() is JSON-serializable
  - bundle_valid reflects failed_checks count
  - checks_run equals len(check_results)
  - checks_passed + checks_failed == checks_run

  --- validate_handoff_bundle_from_manifest() — valid bundle ---
  - Valid manifest passes all structural checks
  - Valid manifest passes all count checks
  - Valid manifest passes all reference checks
  - Valid manifest passes all uniqueness checks
  - bundle_valid is True for a valid manifest
  - checks_failed is 0 for a valid manifest
  - failed_checks is empty for a valid manifest
  - batch_id is set correctly in result
  - Empty-batch valid manifest (all zeros) passes validation

  --- validate_handoff_bundle_from_manifest() — structural failures ---
  - manifest_version mismatch fails CHECK_MANIFEST_VERSION_KNOWN
  - batch_id != pipeline_run_id fails CHECK_BATCH_ID_CONSISTENT
  - Both failures produce bundle_valid=False

  --- validate_handoff_bundle_from_manifest() — count mismatches ---
  - total_exported mismatch fails CHECK_TOTAL_EXPORTED_CONSISTENT
  - total_quarantined mismatch fails CHECK_TOTAL_QUARANTINED_CONSISTENT
  - total_contract_blocked mismatch fails CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT
  - total_skipped mismatch fails CHECK_TOTAL_SKIPPED_CONSISTENT
  - total_eligible mismatch fails CHECK_TOTAL_ELIGIBLE_CONSISTENT
  - total_records_processed mismatch fails CHECK_TOTAL_RECORDS_CONSISTENT
  - outcome_distribution mismatch fails CHECK_OUTCOME_DISTRIBUTION_CONSISTENT
  - count mismatch populates count_mismatches list in result
  - count mismatch sets bundle_valid=False

  --- validate_handoff_bundle_from_manifest() — reference contradictions ---
  - Exported record missing export_artifact_path fails CHECK_EXPORTED_HAVE_EXPORT_PATHS
  - Quarantined record with export_artifact_path fails CHECK_NON_EXPORTED_NO_EXPORT_PATHS
  - Contract-blocked record with export_artifact_path fails CHECK_NON_EXPORTED_NO_EXPORT_PATHS
  - Skipped record with export_artifact_path fails CHECK_NON_EXPORTED_NO_EXPORT_PATHS
  - Exported record with wrong outcome_category fails CHECK_EXPORTED_CORRECT_OUTCOME
  - Quarantined record with wrong outcome_category fails CHECK_QUARANTINED_CORRECT_OUTCOME
  - Contract-blocked record with wrong outcome_category fails CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME
  - Skipped record with wrong outcome_category fails CHECK_SKIPPED_CORRECT_OUTCOME
  - Quarantined record with non-quarantine routing_label fails CHECK_QUARANTINED_ROUTING
  - Exported record with routing_label='quarantine' fails CHECK_EXPORTED_NON_QUARANTINE_ROUTING
  - Contradiction populates contradictions list in result

  --- validate_handoff_bundle_from_manifest() — identifier uniqueness ---
  - Duplicate document_id across record lists fails CHECK_NO_DUPLICATE_DOCUMENT_IDS
  - Duplicate gold_record_id across record lists fails CHECK_NO_DUPLICATE_GOLD_RECORD_IDS
  - Duplicate identifier populates duplicate_identifiers list in result

  --- validate_handoff_bundle_from_manifest() — path checks ---
  - check_paths=False skips all filesystem path checks
  - check_paths=False adds observation about skipped path checks
  - Missing gold_artifact_path file fails CHECK_GOLD_ARTIFACT_PATHS_EXIST
  - Missing export_artifact_path file fails CHECK_EXPORT_ARTIFACT_PATHS_EXIST
  - Missing report_artifacts.json_path fails CHECK_REPORT_JSON_EXISTS
  - Missing report_artifacts.text_path fails CHECK_REPORT_TEXT_EXISTS
  - Present artifact files pass filesystem checks
  - Missing path is added to missing_paths list in result
  - No report_artifacts in manifest → report path checks pass (not applicable)

  --- validate_handoff_bundle() — file-based entry point ---
  - Valid bundle JSON file passes validation
  - Nonexistent bundle JSON file returns bundle_valid=False
  - Invalid (non-JSON) file returns bundle_valid=False
  - File with missing required fields returns bundle_valid=False
  - File-based validation reconstructs manifest from dict correctly

  --- write_validation_result() ---
  - Writes JSON artifact at expected path
  - Writes text artifact at expected path
  - JSON artifact is valid JSON containing all expected keys
  - Text artifact is non-empty string
  - Returns (json_path, text_path) tuple
  - Creates output_dir if not present

  --- format_validation_result_text() ---
  - Contains "=== B-6 Bundle Integrity Validation ===" header
  - Contains "VALID" when bundle_valid=True
  - Contains "INVALID" when bundle_valid=False
  - Contains "--- Failed Checks ---" section when checks fail
  - Contains "--- Count Mismatches ---" section when populated
  - Contains "--- Contradictions ---" section when populated
  - Contains "--- Check Detail ---" section
  - Contains PASS/FAIL annotations in check detail

  --- Module boundary ---
  - handoff_bundle_validation.py contains no AWS/Bedrock SDK imports
  - handoff_bundle_validation.py is a distinct module from handoff_bundle.py
  - handoff_bundle_validation.py does not import classify_gold

  --- Integration with real pipeline bundle output ---
  - Bundle written by run_classify_gold() + write_handoff_bundle() passes B-6 validation
  - Bundle with report_artifacts references present files passes path checks

What these tests do NOT imply:
  - No live Bedrock/AWS integration exists or is required
  - No S3, boto3, or Bedrock SDK usage
  - These tests validate local bundle integrity behavior only

Phase: B-6
Validation module: src/pipelines/handoff_bundle_validation.py
Bundle module: src/pipelines/handoff_bundle.py (B-5)
Pipeline integration: src/pipelines/classify_gold.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.pipelines.handoff_bundle import (
    MANIFEST_VERSION,
    HandoffBatchManifest,
    RecordArtifactRef,
    build_handoff_batch_manifest,
    write_handoff_bundle,
)
from src.pipelines.handoff_bundle_validation import (
    ALL_CHECK_NAMES,
    ALL_COUNT_CHECKS,
    ALL_PATH_CHECKS,
    ALL_REFERENCE_CHECKS,
    ALL_STRUCTURAL_CHECKS,
    ALL_UNIQUENESS_CHECKS,
    CHECK_BATCH_ID_CONSISTENT,
    CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME,
    CHECK_EXPORT_ARTIFACT_PATHS_EXIST,
    CHECK_EXPORTED_CORRECT_OUTCOME,
    CHECK_EXPORTED_HAVE_EXPORT_PATHS,
    CHECK_EXPORTED_NON_QUARANTINE_ROUTING,
    CHECK_GOLD_ARTIFACT_PATHS_EXIST,
    CHECK_MANIFEST_PARSES,
    CHECK_MANIFEST_VERSION_KNOWN,
    CHECK_NO_DUPLICATE_DOCUMENT_IDS,
    CHECK_NO_DUPLICATE_GOLD_RECORD_IDS,
    CHECK_NON_EXPORTED_NO_EXPORT_PATHS,
    CHECK_OUTCOME_DISTRIBUTION_CONSISTENT,
    CHECK_QUARANTINED_CORRECT_OUTCOME,
    CHECK_QUARANTINED_ROUTING,
    CHECK_REPORT_JSON_EXISTS,
    CHECK_REPORT_TEXT_EXISTS,
    CHECK_SKIPPED_CORRECT_OUTCOME,
    CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT,
    CHECK_TOTAL_ELIGIBLE_CONSISTENT,
    CHECK_TOTAL_EXPORTED_CONSISTENT,
    CHECK_TOTAL_QUARANTINED_CONSISTENT,
    CHECK_TOTAL_RECORDS_CONSISTENT,
    CHECK_TOTAL_SKIPPED_CONSISTENT,
    BundleValidationResult,
    CheckResult,
    format_validation_result_text,
    validate_handoff_bundle,
    validate_handoff_bundle_from_manifest,
    write_validation_result,
)
from src.pipelines.handoff_report import (
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
    REASON_CONTRACT_VALIDATION_FAILED,
    REASON_EXPORT_NOT_ATTEMPTED,
    REASON_NONE,
    REASON_ROUTING_QUARANTINE,
)


# ---------------------------------------------------------------------------
# Fixtures — helper builders
# ---------------------------------------------------------------------------


def _make_exported_ref(
    document_id: str = "doc-exported-0001",
    gold_record_id: str = "gold-exported-0001",
    routing_label: str = "regulatory_review",
    gold_artifact_path: str = "output/gold/gold-exported-0001.json",
    export_artifact_path: str = "output/gold/exports/regulatory_review/doc-exported-0001.json",
) -> RecordArtifactRef:
    return RecordArtifactRef(
        document_id=document_id,
        gold_record_id=gold_record_id,
        outcome_category=OUTCOME_EXPORTED,
        outcome_reason=REASON_NONE,
        routing_label=routing_label,
        gold_artifact_path=gold_artifact_path,
        export_artifact_path=export_artifact_path,
    )


def _make_quarantined_ref(
    document_id: str = "doc-quarantined-0001",
    gold_record_id: str = "gold-quarantined-0001",
) -> RecordArtifactRef:
    return RecordArtifactRef(
        document_id=document_id,
        gold_record_id=gold_record_id,
        outcome_category=OUTCOME_QUARANTINED,
        outcome_reason=REASON_ROUTING_QUARANTINE,
        routing_label="quarantine",
        gold_artifact_path="output/gold/gold-quarantined-0001.json",
        export_artifact_path=None,
    )


def _make_contract_blocked_ref(
    document_id: str = "doc-blocked-0001",
    gold_record_id: str = "gold-blocked-0001",
) -> RecordArtifactRef:
    return RecordArtifactRef(
        document_id=document_id,
        gold_record_id=gold_record_id,
        outcome_category=OUTCOME_CONTRACT_BLOCKED,
        outcome_reason=REASON_CONTRACT_VALIDATION_FAILED,
        routing_label="regulatory_review",
        gold_artifact_path="output/gold/gold-blocked-0001.json",
        export_artifact_path=None,
    )


def _make_skipped_ref(
    document_id: str = "doc-skipped-0001",
    gold_record_id: str = "gold-skipped-0001",
) -> RecordArtifactRef:
    return RecordArtifactRef(
        document_id=document_id,
        gold_record_id=gold_record_id,
        outcome_category=OUTCOME_SKIPPED_NOT_EXPORT_READY,
        outcome_reason=REASON_EXPORT_NOT_ATTEMPTED,
        routing_label="regulatory_review",
        gold_artifact_path="output/gold/gold-skipped-0001.json",
        export_artifact_path=None,
    )


def _make_valid_manifest(
    exported: int = 1,
    quarantined: int = 1,
    contract_blocked: int = 0,
    skipped: int = 0,
    ineligible: int = 0,
    pipeline_run_id: str = "test-run-b6-0001",
    report_artifacts: dict | None = None,
) -> HandoffBatchManifest:
    """Build a consistent, valid HandoffBatchManifest for testing."""
    exported_records = [
        _make_exported_ref(
            document_id=f"doc-exp-{i:04d}",
            gold_record_id=f"gold-exp-{i:04d}",
        )
        for i in range(exported)
    ]
    quarantined_records = [
        _make_quarantined_ref(
            document_id=f"doc-quar-{i:04d}",
            gold_record_id=f"gold-quar-{i:04d}",
        )
        for i in range(quarantined)
    ]
    contract_blocked_records = [
        _make_contract_blocked_ref(
            document_id=f"doc-blk-{i:04d}",
            gold_record_id=f"gold-blk-{i:04d}",
        )
        for i in range(contract_blocked)
    ]
    skipped_records = [
        _make_skipped_ref(
            document_id=f"doc-skip-{i:04d}",
            gold_record_id=f"gold-skip-{i:04d}",
        )
        for i in range(skipped)
    ]
    total_eligible = exported + quarantined + contract_blocked + skipped
    return HandoffBatchManifest(
        manifest_version=MANIFEST_VERSION,
        batch_id=pipeline_run_id,
        pipeline_run_id=pipeline_run_id,
        generated_at="2026-04-11T00:00:00+00:00",
        total_records_processed=total_eligible + ineligible,
        total_ineligible_skipped=ineligible,
        total_eligible=total_eligible,
        total_exported=exported,
        total_quarantined=quarantined,
        total_contract_blocked=contract_blocked,
        total_skipped_not_export_ready=skipped,
        outcome_distribution={
            OUTCOME_EXPORTED: exported,
            OUTCOME_QUARANTINED: quarantined,
            OUTCOME_CONTRACT_BLOCKED: contract_blocked,
            OUTCOME_SKIPPED_NOT_EXPORT_READY: skipped,
        },
        exported_records=exported_records,
        quarantined_records=quarantined_records,
        contract_blocked_records=contract_blocked_records,
        skipped_records=skipped_records,
        report_artifacts=report_artifacts,
        review_notes=["test batch"],
    )


# ---------------------------------------------------------------------------
# Constants and vocabulary
# ---------------------------------------------------------------------------


class TestCheckConstants:
    def test_all_check_names_is_tuple_of_strings(self):
        assert isinstance(ALL_CHECK_NAMES, tuple)
        for name in ALL_CHECK_NAMES:
            assert isinstance(name, str) and name

    def test_no_duplicate_check_names(self):
        assert len(ALL_CHECK_NAMES) == len(set(ALL_CHECK_NAMES))

    def test_structural_checks_are_subset_of_all(self):
        for name in ALL_STRUCTURAL_CHECKS:
            assert name in ALL_CHECK_NAMES

    def test_count_checks_are_subset_of_all(self):
        for name in ALL_COUNT_CHECKS:
            assert name in ALL_CHECK_NAMES

    def test_reference_checks_are_subset_of_all(self):
        for name in ALL_REFERENCE_CHECKS:
            assert name in ALL_CHECK_NAMES

    def test_uniqueness_checks_are_subset_of_all(self):
        for name in ALL_UNIQUENESS_CHECKS:
            assert name in ALL_CHECK_NAMES

    def test_path_checks_are_subset_of_all(self):
        for name in ALL_PATH_CHECKS:
            assert name in ALL_CHECK_NAMES

    def test_known_check_constants_exist(self):
        for const in [
            CHECK_MANIFEST_PARSES,
            CHECK_MANIFEST_VERSION_KNOWN,
            CHECK_BATCH_ID_CONSISTENT,
            CHECK_TOTAL_RECORDS_CONSISTENT,
            CHECK_TOTAL_ELIGIBLE_CONSISTENT,
            CHECK_TOTAL_EXPORTED_CONSISTENT,
            CHECK_TOTAL_QUARANTINED_CONSISTENT,
            CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT,
            CHECK_TOTAL_SKIPPED_CONSISTENT,
            CHECK_OUTCOME_DISTRIBUTION_CONSISTENT,
            CHECK_EXPORTED_HAVE_EXPORT_PATHS,
            CHECK_NON_EXPORTED_NO_EXPORT_PATHS,
            CHECK_EXPORTED_CORRECT_OUTCOME,
            CHECK_QUARANTINED_CORRECT_OUTCOME,
            CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME,
            CHECK_SKIPPED_CORRECT_OUTCOME,
            CHECK_QUARANTINED_ROUTING,
            CHECK_EXPORTED_NON_QUARANTINE_ROUTING,
            CHECK_NO_DUPLICATE_DOCUMENT_IDS,
            CHECK_NO_DUPLICATE_GOLD_RECORD_IDS,
            CHECK_REPORT_JSON_EXISTS,
            CHECK_REPORT_TEXT_EXISTS,
            CHECK_GOLD_ARTIFACT_PATHS_EXIST,
            CHECK_EXPORT_ARTIFACT_PATHS_EXIST,
        ]:
            assert isinstance(const, str) and const


# ---------------------------------------------------------------------------
# CheckResult model
# ---------------------------------------------------------------------------


class TestCheckResult:
    def test_passed_result_shape(self):
        r = CheckResult(check_name=CHECK_MANIFEST_VERSION_KNOWN, passed=True)
        assert r.check_name == CHECK_MANIFEST_VERSION_KNOWN
        assert r.passed is True
        assert r.detail is None

    def test_failed_result_carries_detail(self):
        r = CheckResult(
            check_name=CHECK_TOTAL_EXPORTED_CONSISTENT,
            passed=False,
            detail="total_exported (3) != len(exported_records) (2)",
        )
        assert r.passed is False
        assert "total_exported" in r.detail

    def test_to_dict_is_json_serializable(self):
        r = CheckResult(check_name="x", passed=True)
        d = r.to_dict()
        assert json.dumps(d)  # should not raise
        assert d["check_name"] == "x"
        assert d["passed"] is True
        assert d["detail"] is None

    def test_to_dict_with_detail(self):
        r = CheckResult(check_name="y", passed=False, detail="oops")
        d = r.to_dict()
        assert d["detail"] == "oops"


# ---------------------------------------------------------------------------
# BundleValidationResult model
# ---------------------------------------------------------------------------


class TestBundleValidationResult:
    def test_result_fields_exist(self):
        r = BundleValidationResult(
            bundle_valid=True,
            batch_id="run-001",
            checks_run=5,
            checks_passed=5,
            checks_failed=0,
        )
        assert r.bundle_valid is True
        assert r.batch_id == "run-001"
        assert r.checks_run == 5
        assert r.checks_passed == 5
        assert r.checks_failed == 0
        assert isinstance(r.check_results, list)
        assert isinstance(r.failed_checks, list)
        assert isinstance(r.missing_paths, list)
        assert isinstance(r.count_mismatches, list)
        assert isinstance(r.duplicate_identifiers, list)
        assert isinstance(r.contradictions, list)
        assert isinstance(r.observations, list)
        assert r.validated_at

    def test_to_dict_is_json_serializable(self):
        r = BundleValidationResult(
            bundle_valid=True,
            batch_id="run-001",
            checks_run=3,
            checks_passed=3,
            checks_failed=0,
            check_results=[
                CheckResult(check_name="a", passed=True),
                CheckResult(check_name="b", passed=True),
            ],
        )
        d = r.to_dict()
        assert json.dumps(d)
        assert d["bundle_valid"] is True
        assert d["checks_run"] == 3
        assert len(d["check_results"]) == 2

    def test_to_dict_keys(self):
        r = BundleValidationResult(
            bundle_valid=False, batch_id=None, checks_run=1, checks_passed=0, checks_failed=1
        )
        d = r.to_dict()
        for key in [
            "bundle_valid", "batch_id", "checks_run", "checks_passed", "checks_failed",
            "check_results", "failed_checks", "missing_paths", "count_mismatches",
            "duplicate_identifiers", "contradictions", "observations", "validated_at",
        ]:
            assert key in d


# ---------------------------------------------------------------------------
# validate_handoff_bundle_from_manifest() — valid bundle
# ---------------------------------------------------------------------------


class TestValidManifestPasses:
    def test_valid_manifest_bundle_valid_true(self):
        manifest = _make_valid_manifest(exported=2, quarantined=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is True

    def test_valid_manifest_zero_failed_checks(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.checks_failed == 0
        assert result.failed_checks == []

    def test_valid_manifest_batch_id_set(self):
        manifest = _make_valid_manifest(pipeline_run_id="my-run-abc")
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.batch_id == "my-run-abc"

    def test_valid_manifest_passes_all_structural_checks(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_STRUCTURAL_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_valid_manifest_passes_all_count_checks(self):
        manifest = _make_valid_manifest(exported=2, quarantined=1, contract_blocked=1, skipped=0, ineligible=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_COUNT_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_valid_manifest_passes_all_reference_checks(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_REFERENCE_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_valid_manifest_passes_all_uniqueness_checks(self):
        manifest = _make_valid_manifest(exported=2, quarantined=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_UNIQUENESS_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_empty_batch_valid_manifest(self):
        manifest = _make_valid_manifest(exported=0, quarantined=0, contract_blocked=0, skipped=0)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is True

    def test_empty_batch_observation_added(self):
        manifest = _make_valid_manifest(exported=0, quarantined=0)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert any("zero eligible" in obs for obs in result.observations)

    def test_checks_passed_plus_failed_equals_checks_run(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.checks_passed + result.checks_failed == result.checks_run

    def test_checks_run_equals_len_check_results(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.checks_run == len(result.check_results)


# ---------------------------------------------------------------------------
# Structural failures
# ---------------------------------------------------------------------------


class TestStructuralFailures:
    def test_manifest_version_mismatch_fails(self):
        manifest = _make_valid_manifest()
        manifest.manifest_version = "v9.9.9"
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_MANIFEST_VERSION_KNOWN in result.failed_checks

    def test_batch_id_pipeline_run_id_mismatch_fails(self):
        manifest = _make_valid_manifest(pipeline_run_id="run-001")
        manifest.batch_id = "run-DIFFERENT"
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_BATCH_ID_CONSISTENT in result.failed_checks

    def test_version_mismatch_detail_in_failed_check_result(self):
        manifest = _make_valid_manifest()
        manifest.manifest_version = "v0.0.0"
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        failed_detail = next(
            r.detail for r in result.check_results if r.check_name == CHECK_MANIFEST_VERSION_KNOWN
        )
        assert "v0.0.0" in failed_detail


# ---------------------------------------------------------------------------
# Count mismatch failures
# ---------------------------------------------------------------------------


class TestCountMismatches:
    def test_total_exported_mismatch_fails(self):
        manifest = _make_valid_manifest(exported=2)
        manifest.total_exported = 99
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_EXPORTED_CONSISTENT in result.failed_checks

    def test_total_quarantined_mismatch_fails(self):
        manifest = _make_valid_manifest(quarantined=2)
        manifest.total_quarantined = 0
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_QUARANTINED_CONSISTENT in result.failed_checks

    def test_total_contract_blocked_mismatch_fails(self):
        manifest = _make_valid_manifest(contract_blocked=1)
        manifest.total_contract_blocked = 5
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT in result.failed_checks

    def test_total_skipped_mismatch_fails(self):
        manifest = _make_valid_manifest(skipped=1)
        manifest.total_skipped_not_export_ready = 0
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_SKIPPED_CONSISTENT in result.failed_checks

    def test_total_eligible_mismatch_fails(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        # total_eligible says 10 but only 2 records exist
        manifest.total_eligible = 10
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_ELIGIBLE_CONSISTENT in result.failed_checks

    def test_total_records_processed_mismatch_fails(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1, ineligible=0)
        manifest.total_records_processed = 999
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_TOTAL_RECORDS_CONSISTENT in result.failed_checks

    def test_outcome_distribution_mismatch_fails(self):
        manifest = _make_valid_manifest(exported=2, quarantined=1)
        # outcome_distribution says 5 exported, but there are 2
        manifest.outcome_distribution = {
            OUTCOME_EXPORTED: 5,
            OUTCOME_QUARANTINED: 1,
            OUTCOME_CONTRACT_BLOCKED: 0,
            OUTCOME_SKIPPED_NOT_EXPORT_READY: 0,
        }
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert CHECK_OUTCOME_DISTRIBUTION_CONSISTENT in result.failed_checks

    def test_count_mismatch_populates_count_mismatches_list(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.total_exported = 99
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert len(result.count_mismatches) > 0

    def test_count_mismatch_sets_bundle_invalid(self):
        manifest = _make_valid_manifest(quarantined=1)
        manifest.total_quarantined = 0
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False


# ---------------------------------------------------------------------------
# Reference contradiction failures
# ---------------------------------------------------------------------------


class TestReferenceContradictions:
    def test_exported_record_missing_export_path_fails(self):
        manifest = _make_valid_manifest(exported=1)
        # Remove the export_artifact_path from the exported record
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0000.json",
            export_artifact_path=None,  # missing!
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_EXPORTED_HAVE_EXPORT_PATHS in result.failed_checks

    def test_quarantined_record_with_export_path_fails(self):
        manifest = _make_valid_manifest(quarantined=1)
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id="doc-quar-0000",
            gold_record_id="gold-quar-0000",
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-quar-0000.json",
            export_artifact_path="output/gold/exports/quarantine/doc-quar-0000.json",  # contradiction!
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NON_EXPORTED_NO_EXPORT_PATHS in result.failed_checks

    def test_contract_blocked_record_with_export_path_fails(self):
        manifest = _make_valid_manifest(contract_blocked=1)
        manifest.contract_blocked_records[0] = RecordArtifactRef(
            document_id="doc-blk-0000",
            gold_record_id="gold-blk-0000",
            outcome_category=OUTCOME_CONTRACT_BLOCKED,
            outcome_reason=REASON_CONTRACT_VALIDATION_FAILED,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-blk-0000.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-blk-0000.json",  # contradiction!
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NON_EXPORTED_NO_EXPORT_PATHS in result.failed_checks

    def test_skipped_record_with_export_path_fails(self):
        manifest = _make_valid_manifest(skipped=1)
        manifest.skipped_records[0] = RecordArtifactRef(
            document_id="doc-skip-0000",
            gold_record_id="gold-skip-0000",
            outcome_category=OUTCOME_SKIPPED_NOT_EXPORT_READY,
            outcome_reason=REASON_EXPORT_NOT_ATTEMPTED,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-skip-0000.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-skip-0000.json",  # contradiction!
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NON_EXPORTED_NO_EXPORT_PATHS in result.failed_checks

    def test_exported_record_wrong_outcome_category_fails(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_QUARANTINED,  # wrong!
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0000.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-exp-0000.json",
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_EXPORTED_CORRECT_OUTCOME in result.failed_checks

    def test_quarantined_record_wrong_outcome_category_fails(self):
        manifest = _make_valid_manifest(quarantined=1)
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id="doc-quar-0000",
            gold_record_id="gold-quar-0000",
            outcome_category=OUTCOME_EXPORTED,  # wrong!
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-quar-0000.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_QUARANTINED_CORRECT_OUTCOME in result.failed_checks

    def test_contract_blocked_wrong_outcome_category_fails(self):
        manifest = _make_valid_manifest(contract_blocked=1)
        manifest.contract_blocked_records[0] = RecordArtifactRef(
            document_id="doc-blk-0000",
            gold_record_id="gold-blk-0000",
            outcome_category=OUTCOME_EXPORTED,  # wrong!
            outcome_reason=REASON_CONTRACT_VALIDATION_FAILED,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-blk-0000.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME in result.failed_checks

    def test_skipped_record_wrong_outcome_category_fails(self):
        manifest = _make_valid_manifest(skipped=1)
        manifest.skipped_records[0] = RecordArtifactRef(
            document_id="doc-skip-0000",
            gold_record_id="gold-skip-0000",
            outcome_category=OUTCOME_QUARANTINED,  # wrong!
            outcome_reason=REASON_EXPORT_NOT_ATTEMPTED,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-skip-0000.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_SKIPPED_CORRECT_OUTCOME in result.failed_checks

    def test_quarantined_record_non_quarantine_routing_fails(self):
        manifest = _make_valid_manifest(quarantined=1)
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id="doc-quar-0000",
            gold_record_id="gold-quar-0000",
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="regulatory_review",  # should be 'quarantine'!
            gold_artifact_path="output/gold/gold-quar-0000.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_QUARANTINED_ROUTING in result.failed_checks

    def test_exported_record_quarantine_routing_fails(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="quarantine",  # contradictory — exported records can't be quarantine!
            gold_artifact_path="output/gold/gold-exp-0000.json",
            export_artifact_path="output/gold/exports/quarantine/doc-exp-0000.json",
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_EXPORTED_NON_QUARANTINE_ROUTING in result.failed_checks

    def test_contradiction_populates_contradictions_list(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0000.json",
            export_artifact_path=None,  # missing — contradiction
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert len(result.contradictions) > 0


# ---------------------------------------------------------------------------
# Identifier uniqueness failures
# ---------------------------------------------------------------------------


class TestIdentifierUniqueness:
    def test_duplicate_document_id_across_lists_fails(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        # Give quarantined record the same document_id as the exported one
        exported_doc_id = manifest.exported_records[0].document_id
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id=exported_doc_id,  # duplicate!
            gold_record_id="gold-quar-unique",
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-quar-unique.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NO_DUPLICATE_DOCUMENT_IDS in result.failed_checks

    def test_duplicate_gold_record_id_across_lists_fails(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        exported_gold_id = manifest.exported_records[0].gold_record_id
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id="doc-quar-unique",
            gold_record_id=exported_gold_id,  # duplicate!
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-quar-unique.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NO_DUPLICATE_GOLD_RECORD_IDS in result.failed_checks

    def test_duplicate_document_id_within_same_list_fails(self):
        manifest = _make_valid_manifest(exported=2)
        # Both exported records get the same document_id
        same_id = "doc-dup-0001"
        manifest.exported_records[0] = RecordArtifactRef(
            document_id=same_id,
            gold_record_id="gold-exp-0001",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0001.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-dup-0001.json",
        )
        manifest.exported_records[1] = RecordArtifactRef(
            document_id=same_id,
            gold_record_id="gold-exp-0002",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0002.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-dup-0001.json",
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_NO_DUPLICATE_DOCUMENT_IDS in result.failed_checks

    def test_duplicate_identifier_populates_duplicate_identifiers_list(self):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        manifest.quarantined_records[0] = RecordArtifactRef(
            document_id=manifest.exported_records[0].document_id,  # duplicate
            gold_record_id="gold-quar-unique",
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-quar-unique.json",
            export_artifact_path=None,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert len(result.duplicate_identifiers) > 0


# ---------------------------------------------------------------------------
# Path existence checks
# ---------------------------------------------------------------------------


class TestPathExistenceChecks:
    def test_check_paths_false_skips_path_checks(self):
        manifest = _make_valid_manifest(exported=1)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        executed_names = {r.check_name for r in result.check_results}
        for path_check in ALL_PATH_CHECKS:
            assert path_check not in executed_names

    def test_check_paths_false_adds_observation(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        assert any("check_paths=False" in obs for obs in result.observations)

    def test_missing_gold_artifact_fails(self, tmp_path):
        # export path doesn't exist on disk
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-path",
            gold_record_id="gold-exp-path",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path=str(tmp_path / "nonexistent_gold.json"),
            export_artifact_path=str(tmp_path / "nonexistent_export.json"),
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert result.bundle_valid is False
        assert CHECK_GOLD_ARTIFACT_PATHS_EXIST in result.failed_checks

    def test_missing_export_artifact_fails(self, tmp_path):
        # Create gold artifact but not the export artifact
        gold_path = tmp_path / "gold-exp-path.json"
        gold_path.write_text("{}", encoding="utf-8")
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-path",
            gold_record_id="gold-exp-path",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path=str(gold_path),
            export_artifact_path=str(tmp_path / "nonexistent_export.json"),
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert result.bundle_valid is False
        assert CHECK_EXPORT_ARTIFACT_PATHS_EXIST in result.failed_checks

    def test_missing_report_json_path_fails(self, tmp_path):
        manifest = _make_valid_manifest(
            report_artifacts={
                "json_path": str(tmp_path / "nonexistent_report.json"),
                "text_path": str(tmp_path / "nonexistent_report.txt"),
            }
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert result.bundle_valid is False
        assert CHECK_REPORT_JSON_EXISTS in result.failed_checks

    def test_missing_report_text_path_fails(self, tmp_path):
        json_path = tmp_path / "report.json"
        json_path.write_text("{}", encoding="utf-8")
        manifest = _make_valid_manifest(
            report_artifacts={
                "json_path": str(json_path),
                "text_path": str(tmp_path / "nonexistent_report.txt"),
            }
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert result.bundle_valid is False
        assert CHECK_REPORT_TEXT_EXISTS in result.failed_checks

    def test_present_files_pass_path_checks(self, tmp_path):
        # Create all referenced files
        gold = tmp_path / "gold.json"
        export = tmp_path / "export.json"
        report_json = tmp_path / "report.json"
        report_txt = tmp_path / "report.txt"
        for f in [gold, export, report_json, report_txt]:
            f.write_text("{}", encoding="utf-8")

        manifest = _make_valid_manifest(
            exported=1,
            quarantined=0,
            report_artifacts={
                "json_path": str(report_json),
                "text_path": str(report_txt),
            },
        )
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path=str(gold),
            export_artifact_path=str(export),
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert result.bundle_valid is True

    def test_missing_path_added_to_missing_paths_list(self, tmp_path):
        manifest = _make_valid_manifest(exported=1)
        fake_gold = str(tmp_path / "fake_gold.json")
        fake_export = str(tmp_path / "fake_export.json")
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path=fake_gold,
            export_artifact_path=fake_export,
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        assert len(result.missing_paths) > 0

    def test_no_report_artifacts_report_path_checks_pass(self, tmp_path):
        manifest = _make_valid_manifest(report_artifacts=None)
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=True)
        check_names = {r.check_name: r.passed for r in result.check_results}
        # Both report path checks should pass (not applicable)
        assert check_names.get(CHECK_REPORT_JSON_EXISTS) is True
        assert check_names.get(CHECK_REPORT_TEXT_EXISTS) is True


# ---------------------------------------------------------------------------
# validate_handoff_bundle() — file-based entry point
# ---------------------------------------------------------------------------


class TestFileBased:
    def test_nonexistent_file_returns_invalid(self, tmp_path):
        result = validate_handoff_bundle(tmp_path / "does_not_exist.json")
        assert result.bundle_valid is False
        assert CHECK_MANIFEST_PARSES in result.failed_checks

    def test_invalid_json_returns_invalid(self, tmp_path):
        bad_json = tmp_path / "bad.json"
        bad_json.write_text("NOT JSON {{{{", encoding="utf-8")
        result = validate_handoff_bundle(bad_json)
        assert result.bundle_valid is False
        assert CHECK_MANIFEST_PARSES in result.failed_checks

    def test_missing_required_fields_returns_invalid(self, tmp_path):
        partial = tmp_path / "partial.json"
        partial.write_text(json.dumps({"manifest_version": "v0.1.0"}), encoding="utf-8")
        result = validate_handoff_bundle(partial)
        assert result.bundle_valid is False
        assert CHECK_MANIFEST_PARSES in result.failed_checks

    def test_valid_bundle_file_passes(self, tmp_path):
        manifest = _make_valid_manifest(exported=1, quarantined=1)
        json_path, _ = write_handoff_bundle(manifest, tmp_path)
        result = validate_handoff_bundle(json_path, check_paths=False)
        assert result.bundle_valid is True

    def test_file_based_validation_reconstructs_batch_id(self, tmp_path):
        manifest = _make_valid_manifest(pipeline_run_id="my-file-run-001")
        json_path, _ = write_handoff_bundle(manifest, tmp_path)
        result = validate_handoff_bundle(json_path, check_paths=False)
        assert result.batch_id == "my-file-run-001"

    def test_file_based_detects_corrupt_counts(self, tmp_path):
        manifest = _make_valid_manifest(exported=2, quarantined=1)
        # Manually corrupt the JSON before writing
        d = manifest.to_dict()
        d["total_exported"] = 999
        corrupt_path = tmp_path / "corrupt.json"
        corrupt_path.write_text(json.dumps(d), encoding="utf-8")
        result = validate_handoff_bundle(corrupt_path, check_paths=False)
        assert result.bundle_valid is False
        assert CHECK_TOTAL_EXPORTED_CONSISTENT in result.failed_checks


# ---------------------------------------------------------------------------
# write_validation_result()
# ---------------------------------------------------------------------------


class TestWriteValidationResult:
    def test_writes_json_artifact(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        json_path, _ = write_validation_result(result, tmp_path)
        assert json_path.exists()

    def test_writes_text_artifact(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        _, text_path = write_validation_result(result, tmp_path)
        assert text_path.exists()

    def test_json_artifact_is_valid_json(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        json_path, _ = write_validation_result(result, tmp_path)
        parsed = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(parsed, dict)

    def test_json_artifact_contains_expected_keys(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        json_path, _ = write_validation_result(result, tmp_path)
        parsed = json.loads(json_path.read_text(encoding="utf-8"))
        for key in ["bundle_valid", "batch_id", "checks_run", "checks_passed",
                    "checks_failed", "failed_checks", "check_results",
                    "missing_paths", "count_mismatches", "contradictions", "validated_at"]:
            assert key in parsed

    def test_text_artifact_is_non_empty(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        _, text_path = write_validation_result(result, tmp_path)
        assert len(text_path.read_text(encoding="utf-8")) > 0

    def test_returns_json_text_path_tuple(self, tmp_path):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        paths = write_validation_result(result, tmp_path)
        assert len(paths) == 2
        assert paths[0].suffix == ".json"
        assert paths[1].suffix == ".txt"

    def test_creates_output_dir_if_absent(self, tmp_path):
        new_dir = tmp_path / "new_validation_dir"
        assert not new_dir.exists()
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        write_validation_result(result, new_dir)
        assert new_dir.exists()


# ---------------------------------------------------------------------------
# format_validation_result_text()
# ---------------------------------------------------------------------------


class TestFormatValidationResultText:
    def test_contains_header(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "=== B-6 Bundle Integrity Validation ===" in text

    def test_contains_valid_when_passed(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "VALID" in text

    def test_contains_invalid_when_failed(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.total_exported = 99
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "INVALID" in text

    def test_contains_failed_checks_section_when_failures(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.total_exported = 99
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "--- Failed Checks ---" in text

    def test_contains_count_mismatches_section_when_populated(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.total_exported = 5
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "--- Count Mismatches ---" in text

    def test_contains_contradictions_section_when_populated(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.exported_records[0] = RecordArtifactRef(
            document_id="doc-exp-0000",
            gold_record_id="gold-exp-0000",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-exp-0000.json",
            export_artifact_path=None,  # contradiction
        )
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "--- Contradictions ---" in text

    def test_contains_check_detail_section(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "--- Check Detail ---" in text

    def test_check_detail_contains_pass_fail_annotations(self):
        manifest = _make_valid_manifest()
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "[PASS]" in text

    def test_failed_check_shown_in_detail(self):
        manifest = _make_valid_manifest(exported=1)
        manifest.total_exported = 99
        result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)
        text = format_validation_result_text(result)
        assert "[FAIL]" in text


# ---------------------------------------------------------------------------
# Module boundary
# ---------------------------------------------------------------------------


class TestModuleBoundary:
    def test_no_aws_bedrock_sdk_imports(self):
        import re
        module_path = Path(__file__).resolve().parents[1] / "src/pipelines/handoff_bundle_validation.py"
        source = module_path.read_text(encoding="utf-8")
        # Check there are no SDK import statements for AWS/Bedrock libraries.
        # We check for import patterns rather than bare substrings because module
        # docstrings legitimately reference "bedrock-handoff-contract.md" and
        # "Bedrock CaseOps" as system names.
        for forbidden_import in ["boto3", "botocore", "aws_crt"]:
            assert forbidden_import not in source, (
                f"handoff_bundle_validation.py must not reference {forbidden_import}"
            )
        # No SDK import lines for bedrock (checking import statements specifically)
        assert not re.search(r"^\s*(import|from)\s+bedrock", source, re.MULTILINE), (
            "handoff_bundle_validation.py must not import bedrock SDK"
        )

    def test_module_is_distinct_from_handoff_bundle(self):
        val_path = Path(__file__).resolve().parents[1] / "src/pipelines/handoff_bundle_validation.py"
        bundle_path = Path(__file__).resolve().parents[1] / "src/pipelines/handoff_bundle.py"
        assert val_path != bundle_path
        assert val_path.exists()
        assert bundle_path.exists()

    def test_module_does_not_import_classify_gold(self):
        import re
        module_path = Path(__file__).resolve().parents[1] / "src/pipelines/handoff_bundle_validation.py"
        source = module_path.read_text(encoding="utf-8")
        # Check there are no import statements for classify_gold. Docstrings may
        # mention "classify_gold.py" in the module boundary description, which is fine.
        assert not re.search(r"^\s*(import|from)\s+.*classify_gold", source, re.MULTILINE), (
            "handoff_bundle_validation.py must not import from classify_gold"
        )


# ---------------------------------------------------------------------------
# Integration — validate real pipeline bundle output
# ---------------------------------------------------------------------------


class TestIntegrationWithPipeline:
    """
    Verify that a bundle produced by the real pipeline passes B-6 validation.
    Uses run_classify_gold() integration logic from B-5 test patterns.
    """

    def _run_pipeline(self, tmp_path):
        """Run classify_gold against the sample fixture and return bundle dir."""
        import uuid
        from datetime import datetime, timezone

        from src.pipelines.classify_gold import run_classify_gold

        silver_dir = tmp_path / "silver"
        bronze_dir = tmp_path / "bronze"
        gold_dir = tmp_path / "gold"
        report_dir = tmp_path / "reports"
        bundle_dir = tmp_path / "reports"
        silver_dir.mkdir()
        bronze_dir.mkdir()
        gold_dir.mkdir()
        report_dir.mkdir()

        doc_id = str(uuid.uuid4())
        bronze_id = str(uuid.uuid4())
        extraction_id = str(uuid.uuid4())

        bronze_rec = {
            "bronze_record_id": bronze_id,
            "document_id": doc_id,
            "source_path": "/volumes/caseops/raw/documents/fda/test.pdf",
            "file_hash": "abc123",
            "file_name": "test.pdf",
            "mime_type": "application/pdf",
            "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
            "parsed_at": datetime.now(tz=timezone.utc).isoformat(),
            "parse_status": "success",
            "parsed_text": "FDA WARNING LETTER. Issuing Office: Office of Quality. "
                           "Date: March 1, 2025. This is a warning letter. " * 20,
            "page_count": 1,
            "char_count": 500,
            "pipeline_run_id": "test-b6-int-run",
        }
        (bronze_dir / f"{bronze_id}.json").write_text(
            json.dumps(bronze_rec), encoding="utf-8"
        )

        silver_rec = {
            "extraction_id": extraction_id,
            "document_id": doc_id,
            "bronze_record_id": bronze_id,
            "extracted_at": datetime.now(tz=timezone.utc).isoformat(),
            "document_class_hint": "fda_warning_letter",
            "extracted_fields": {
                "issuing_office": "Office of Pharmaceutical Quality",
                "recipient_company": "Test Pharma Inc.",
                "issue_date": "2025-03-01",
                "violation_type": ["cGMP"],
                "corrective_action_requested": True,
                "product_names": ["TestDrug"],
                "warning_letter_number": "320-25-001",
                "response_deadline_days": 15,
                "response_deadline_date": "2025-03-16",
                "cited_regulations": ["21 CFR Part 211"],
            },
            "field_coverage_pct": 1.0,
            "validation_status": "valid",
            "validation_errors": [],
            "extraction_model": "local_rule_based_v1",
            "pipeline_run_id": "test-b6-int-run",
        }
        (silver_dir / f"{extraction_id}.json").write_text(
            json.dumps(silver_rec), encoding="utf-8"
        )

        # run_classify_gold takes string paths, not Path objects
        summaries = run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(gold_dir),
            export_dir=str(gold_dir / "exports"),
            bronze_dir=str(bronze_dir),
            report_dir=str(report_dir),
            bundle_dir=str(bundle_dir),
        )
        return summaries, bundle_dir, report_dir

    def test_pipeline_bundle_passes_b6_structural_checks(self, tmp_path):
        _, bundle_dir, _ = self._run_pipeline(tmp_path)
        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        assert bundle_files, "Pipeline should produce a bundle JSON"
        result = validate_handoff_bundle(bundle_files[0], check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_STRUCTURAL_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_pipeline_bundle_passes_b6_count_checks(self, tmp_path):
        _, bundle_dir, _ = self._run_pipeline(tmp_path)
        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        result = validate_handoff_bundle(bundle_files[0], check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_COUNT_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_pipeline_bundle_passes_b6_reference_checks(self, tmp_path):
        _, bundle_dir, _ = self._run_pipeline(tmp_path)
        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        result = validate_handoff_bundle(bundle_files[0], check_paths=False)
        check_names = {r.check_name: r.passed for r in result.check_results}
        for name in ALL_REFERENCE_CHECKS:
            assert check_names.get(name, False) is True, f"{name} should pass"

    def test_pipeline_bundle_overall_valid(self, tmp_path):
        _, bundle_dir, _ = self._run_pipeline(tmp_path)
        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        result = validate_handoff_bundle(bundle_files[0], check_paths=False)
        assert result.bundle_valid is True

    def test_pipeline_bundle_passes_with_path_checks_when_files_exist(self, tmp_path):
        _, bundle_dir, _ = self._run_pipeline(tmp_path)
        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        # check_paths=True but all files were written by the pipeline
        result = validate_handoff_bundle(bundle_files[0], check_paths=True)
        # Path checks for gold and export artifacts should pass (files were created)
        check_names = {r.check_name: r.passed for r in result.check_results}
        assert check_names.get(CHECK_GOLD_ARTIFACT_PATHS_EXIST) is True
        assert check_names.get(CHECK_EXPORT_ARTIFACT_PATHS_EXIST) is True
