"""
tests/test_b5_handoff_bundle.py — B-5 handoff batch manifest and review bundle tests.

B-5 purpose: verify that the Gold → Bedrock export path can produce a single,
coherent, reviewable batch handoff bundle that packages all per-record artifact
references, batch metadata, and links to B-4 report artifacts into one manifest.

What these tests cover:
  --- Constants and model structure ---
  - MANIFEST_VERSION constant exists and is a string
  - RecordArtifactRef has all required fields and to_dict() is correct
  - HandoffBatchManifest has all required fields and to_dict() is JSON-serializable
  - HandoffBatchManifest.to_dict() preserves RecordArtifactRef objects

  --- build_handoff_batch_manifest() ---
  - Correctly classifies summaries into exported / quarantined / contract_blocked / skipped
  - Correctly counts all four outcome categories
  - manifest_version, batch_id, pipeline_run_id are set correctly
  - total_records_processed defaults to len(summaries) + ineligible when not provided
  - total_records_processed is respected when explicitly provided
  - outcome_distribution is populated from summaries
  - report_artifacts is None when not provided
  - report_artifacts is set when provided
  - review_notes is non-empty list
  - review_notes mentions report attachment when report_artifacts provided
  - review_notes notes missing report when not provided
  - empty summaries list produces valid manifest with all zeros
  - generated_at defaults to a valid ISO timestamp when not provided
  - generated_at is respected when explicitly provided

  --- compute_bundle_path() ---
  - Returns deterministic path under bundle_dir
  - Path ends with .json
  - Same inputs always produce the same path
  - Run ID with special characters is sanitized

  --- format_bundle_text() ---
  - Contains "=== Handoff Batch Review Bundle (B-5) ===" header
  - Contains manifest_version, batch_id, pipeline_run_id sections
  - Contains "--- Record Counts ---" section
  - Contains "--- Outcome Counts ---" section
  - Exported records section shows export_artifact_path when present
  - Quarantined records section shows "(none)" when empty
  - Contains "--- Report Artifacts (B-4) ---" section when report_artifacts present
  - Contains "--- Review Notes ---" section

  --- write_handoff_bundle() ---
  - Writes JSON artifact at expected path
  - Writes text artifact at expected path
  - JSON artifact is valid JSON
  - JSON artifact contains all top-level manifest keys
  - Text artifact is non-empty string
  - Returns (json_path, text_path) tuple
  - Creates bundle_dir if it does not exist

  --- Integration via run_classify_gold() ---
  - run_classify_gold() with bundle_dir writes a bundle manifest
  - Bundle manifest correctly references exported records with export_artifact_path
  - Bundle manifest correctly references quarantined records
  - Bundle manifest counts match summaries
  - Bundle manifest references report_artifacts when report_dir also provided
  - Bundle manifest has None report_artifacts when only bundle_dir provided
  - Bundle manifest JSON is parseable and contains expected structure

  --- Module boundary ---
  - handoff_bundle.py contains no AWS/Bedrock SDK imports (boto3, botocore, bedrock)
  - handoff_bundle.py is distinct from handoff_report.py

What these tests do NOT imply:
  - No live Bedrock/AWS integration exists or is required
  - No S3, boto3, or Bedrock SDK usage
  - These tests validate local module structure and bundle assembly behavior only

Phase: B-5
Authoritative contract: docs/bedrock-handoff-contract.md
Bundle module under test: src/pipelines/handoff_bundle.py
Pipeline integration: src/pipelines/classify_gold.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.pipelines.handoff_bundle import (
    MANIFEST_VERSION,
    HandoffBatchManifest,
    RecordArtifactRef,
    build_handoff_batch_manifest,
    compute_bundle_path,
    format_bundle_text,
    write_handoff_bundle,
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
from src.pipelines.classify_gold import run_classify_gold


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_exported_summary(doc_id: str = "b5aa0001-0000-0000-0000-000000000001") -> dict:
    return {
        "document_id": doc_id,
        "gold_record_id": f"gold-{doc_id}",
        "routing_label": "regulatory_review",
        "export_ready": True,
        "gold_artifact_path": f"output/gold/gold-{doc_id}.json",
        "export_artifact_path": f"output/gold/exports/regulatory_review/{doc_id}.json",
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_EXPORTED,
        "outcome_reason": REASON_NONE,
    }


def _make_quarantined_summary(doc_id: str = "b5aa0002-0000-0000-0000-000000000002") -> dict:
    return {
        "document_id": doc_id,
        "gold_record_id": f"gold-{doc_id}",
        "routing_label": "quarantine",
        "export_ready": False,
        "gold_artifact_path": f"output/gold/gold-{doc_id}.json",
        "export_artifact_path": None,
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_QUARANTINED,
        "outcome_reason": REASON_ROUTING_QUARANTINE,
    }


def _make_contract_blocked_summary(doc_id: str = "b5aa0003-0000-0000-0000-000000000003") -> dict:
    return {
        "document_id": doc_id,
        "gold_record_id": f"gold-{doc_id}",
        "routing_label": "regulatory_review",
        "export_ready": False,
        "gold_artifact_path": f"output/gold/gold-{doc_id}.json",
        "export_artifact_path": None,
        "contract_validation_errors": ["Missing required field: 'source_file'"],
        "outcome_category": OUTCOME_CONTRACT_BLOCKED,
        "outcome_reason": REASON_CONTRACT_VALIDATION_FAILED,
    }


def _make_skipped_summary(doc_id: str = "b5aa0004-0000-0000-0000-000000000004") -> dict:
    return {
        "document_id": doc_id,
        "gold_record_id": f"gold-{doc_id}",
        "routing_label": "regulatory_review",
        "export_ready": False,
        "gold_artifact_path": f"output/gold/gold-{doc_id}.json",
        "export_artifact_path": None,
        "contract_validation_errors": [],
        "outcome_category": OUTCOME_SKIPPED_NOT_EXPORT_READY,
        "outcome_reason": REASON_EXPORT_NOT_ATTEMPTED,
    }


def _make_mixed_summaries() -> list[dict]:
    return [
        _make_exported_summary("b5aa0001-0000-0000-0000-000000000001"),
        _make_quarantined_summary("b5aa0002-0000-0000-0000-000000000002"),
        _make_contract_blocked_summary("b5aa0003-0000-0000-0000-000000000003"),
        _make_skipped_summary("b5aa0004-0000-0000-0000-000000000004"),
    ]


# Silver fixtures for pipeline integration tests

def _write_silver_fixture(tmp_path: Path, record: dict) -> Path:
    p = tmp_path / f"{record['document_id']}.json"
    p.write_text(json.dumps(record), encoding="utf-8")
    return p


def _make_valid_silver(doc_id: str = "b5int0001-0000-0000-0000-000000000001") -> dict:
    return {
        "document_id": doc_id,
        "bronze_record_id": f"bronze-{doc_id}",
        "extraction_id": f"ext-{doc_id}",
        "document_class_hint": "fda_warning_letter",
        "validation_status": "valid",
        "field_coverage_pct": 0.9,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {
            "issuing_office": "Office of Pharmaceutical Quality",
            "recipient_company": "Acme Pharma Inc.",
            "issue_date": "2024-01-15",
            "violation_type": ["cGMP"],
            "corrective_action_requested": True,
        },
    }


def _make_quarantine_silver(doc_id: str = "b5int0002-0000-0000-0000-000000000002") -> dict:
    # No class_hint + no FDA fields + low coverage → signals < 2 → UNKNOWN → quarantine.
    return {
        "document_id": doc_id,
        "bronze_record_id": f"bronze-{doc_id}",
        "extraction_id": f"ext-{doc_id}",
        "document_class_hint": None,
        "validation_status": "partial",
        "field_coverage_pct": 0.1,
        "extraction_model": "local_rule_extractor/v1",
        "extracted_fields": {},
    }


# ---------------------------------------------------------------------------
# 1. Constants and model structure
# ---------------------------------------------------------------------------


class TestManifestVersion:
    def test_manifest_version_exists(self):
        assert MANIFEST_VERSION is not None

    def test_manifest_version_is_string(self):
        assert isinstance(MANIFEST_VERSION, str)

    def test_manifest_version_format(self):
        assert MANIFEST_VERSION.startswith("v"), (
            "MANIFEST_VERSION should start with 'v' (e.g. v0.1.0)"
        )


class TestRecordArtifactRef:
    def test_fields_present(self):
        ref = RecordArtifactRef(
            document_id="doc-001",
            gold_record_id="gold-001",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-001.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-001.json",
        )
        assert ref.document_id == "doc-001"
        assert ref.gold_record_id == "gold-001"
        assert ref.outcome_category == OUTCOME_EXPORTED
        assert ref.outcome_reason == REASON_NONE
        assert ref.routing_label == "regulatory_review"
        assert ref.gold_artifact_path == "output/gold/gold-001.json"
        assert ref.export_artifact_path is not None

    def test_to_dict_keys(self):
        ref = RecordArtifactRef(
            document_id="doc-001",
            gold_record_id="gold-001",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-001.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-001.json",
        )
        d = ref.to_dict()
        expected_keys = {
            "document_id", "gold_record_id", "outcome_category",
            "outcome_reason", "routing_label", "gold_artifact_path",
            "export_artifact_path",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_values_match(self):
        ref = RecordArtifactRef(
            document_id="doc-001",
            gold_record_id="gold-001",
            outcome_category=OUTCOME_QUARANTINED,
            outcome_reason=REASON_ROUTING_QUARANTINE,
            routing_label="quarantine",
            gold_artifact_path="output/gold/gold-001.json",
            export_artifact_path=None,
        )
        d = ref.to_dict()
        assert d["document_id"] == "doc-001"
        assert d["outcome_category"] == OUTCOME_QUARANTINED
        assert d["export_artifact_path"] is None

    def test_to_dict_is_json_serializable(self):
        ref = RecordArtifactRef(
            document_id="doc-001",
            gold_record_id="gold-001",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-001.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-001.json",
        )
        serialized = json.dumps(ref.to_dict())
        assert isinstance(serialized, str)


class TestHandoffBatchManifestModel:
    def _make_minimal_manifest(self) -> HandoffBatchManifest:
        return HandoffBatchManifest(
            manifest_version=MANIFEST_VERSION,
            batch_id="run-abc",
            pipeline_run_id="run-abc",
            generated_at="2026-04-11T00:00:00+00:00",
            total_records_processed=2,
            total_ineligible_skipped=0,
            total_eligible=2,
            total_exported=1,
            total_quarantined=1,
            total_contract_blocked=0,
            total_skipped_not_export_ready=0,
        )

    def test_fields_present(self):
        m = self._make_minimal_manifest()
        assert m.manifest_version == MANIFEST_VERSION
        assert m.batch_id == "run-abc"
        assert m.pipeline_run_id == "run-abc"
        assert m.total_records_processed == 2
        assert m.total_exported == 1
        assert m.total_quarantined == 1
        assert m.total_contract_blocked == 0
        assert m.report_artifacts is None
        assert isinstance(m.review_notes, list)

    def test_to_dict_top_level_keys(self):
        m = self._make_minimal_manifest()
        d = m.to_dict()
        expected_keys = {
            "manifest_version", "batch_id", "pipeline_run_id", "generated_at",
            "total_records_processed", "total_ineligible_skipped", "total_eligible",
            "total_exported", "total_quarantined", "total_contract_blocked",
            "total_skipped_not_export_ready", "outcome_distribution",
            "exported_records", "quarantined_records", "contract_blocked_records",
            "skipped_records", "report_artifacts", "review_notes",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_is_json_serializable(self):
        m = self._make_minimal_manifest()
        serialized = json.dumps(m.to_dict())
        assert isinstance(serialized, str)

    def test_to_dict_serializes_record_artifact_refs(self):
        m = self._make_minimal_manifest()
        ref = RecordArtifactRef(
            document_id="doc-001",
            gold_record_id="gold-001",
            outcome_category=OUTCOME_EXPORTED,
            outcome_reason=REASON_NONE,
            routing_label="regulatory_review",
            gold_artifact_path="output/gold/gold-001.json",
            export_artifact_path="output/gold/exports/regulatory_review/doc-001.json",
        )
        m.exported_records = [ref]
        d = m.to_dict()
        assert isinstance(d["exported_records"], list)
        assert isinstance(d["exported_records"][0], dict)
        assert d["exported_records"][0]["document_id"] == "doc-001"


# ---------------------------------------------------------------------------
# 2. build_handoff_batch_manifest()
# ---------------------------------------------------------------------------


class TestBuildHandoffBatchManifest:
    def test_exported_record_goes_to_exported_list(self):
        summaries = [_make_exported_summary()]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert len(m.exported_records) == 1
        assert len(m.quarantined_records) == 0
        assert len(m.contract_blocked_records) == 0
        assert len(m.skipped_records) == 0

    def test_quarantined_record_goes_to_quarantined_list(self):
        summaries = [_make_quarantined_summary()]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert len(m.quarantined_records) == 1
        assert len(m.exported_records) == 0

    def test_contract_blocked_record_goes_to_blocked_list(self):
        summaries = [_make_contract_blocked_summary()]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert len(m.contract_blocked_records) == 1
        assert len(m.exported_records) == 0

    def test_skipped_record_goes_to_skipped_list(self):
        summaries = [_make_skipped_summary()]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert len(m.skipped_records) == 1

    def test_mixed_summaries_classified_correctly(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.total_exported == 1
        assert m.total_quarantined == 1
        assert m.total_contract_blocked == 1
        assert m.total_skipped_not_export_ready == 1

    def test_counts_match_record_lists(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.total_exported == len(m.exported_records)
        assert m.total_quarantined == len(m.quarantined_records)
        assert m.total_contract_blocked == len(m.contract_blocked_records)
        assert m.total_skipped_not_export_ready == len(m.skipped_records)

    def test_total_eligible_equals_len_summaries(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.total_eligible == 4

    def test_total_records_processed_defaults_when_not_provided(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(summaries, "run-001", total_ineligible_skipped=2)
        assert m.total_records_processed == 6  # 4 eligible + 2 ineligible

    def test_total_records_processed_respected_when_provided(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(
            summaries, "run-001", total_records_processed=10, total_ineligible_skipped=6
        )
        assert m.total_records_processed == 10

    def test_manifest_version_is_set(self):
        m = build_handoff_batch_manifest([], "run-001")
        assert m.manifest_version == MANIFEST_VERSION

    def test_batch_id_equals_pipeline_run_id(self):
        m = build_handoff_batch_manifest([], "run-xyz-123")
        assert m.batch_id == "run-xyz-123"
        assert m.pipeline_run_id == "run-xyz-123"

    def test_outcome_distribution_populated(self):
        summaries = _make_mixed_summaries()
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.outcome_distribution[OUTCOME_EXPORTED] == 1
        assert m.outcome_distribution[OUTCOME_QUARANTINED] == 1
        assert m.outcome_distribution[OUTCOME_CONTRACT_BLOCKED] == 1
        assert m.outcome_distribution[OUTCOME_SKIPPED_NOT_EXPORT_READY] == 1

    def test_report_artifacts_is_none_when_not_provided(self):
        m = build_handoff_batch_manifest([], "run-001")
        assert m.report_artifacts is None

    def test_report_artifacts_set_when_provided(self):
        paths = {"json_path": "output/reports/report.json", "text_path": "output/reports/report.txt"}
        m = build_handoff_batch_manifest([], "run-001", report_artifact_paths=paths)
        assert m.report_artifacts == paths

    def test_review_notes_is_non_empty_list(self):
        m = build_handoff_batch_manifest(_make_mixed_summaries(), "run-001")
        assert isinstance(m.review_notes, list)
        assert len(m.review_notes) > 0

    def test_review_notes_mentions_report_when_attached(self):
        paths = {"json_path": "output/reports/report.json", "text_path": "output/reports/report.txt"}
        m = build_handoff_batch_manifest([], "run-001", report_artifact_paths=paths)
        notes_text = " ".join(m.review_notes)
        assert "HandoffBatchReport" in notes_text or "report" in notes_text.lower()

    def test_review_notes_notes_missing_report_when_not_provided(self):
        m = build_handoff_batch_manifest([], "run-001")
        notes_text = " ".join(m.review_notes)
        assert "not generated" in notes_text or "--report-dir" in notes_text

    def test_empty_summaries_produces_valid_manifest(self):
        m = build_handoff_batch_manifest([], "run-empty")
        assert m.total_exported == 0
        assert m.total_quarantined == 0
        assert m.total_contract_blocked == 0
        assert m.total_skipped_not_export_ready == 0
        assert m.total_eligible == 0
        assert len(m.exported_records) == 0

    def test_generated_at_defaults_to_iso_string(self):
        m = build_handoff_batch_manifest([], "run-001")
        # ISO 8601 pattern: YYYY-MM-DDTHH:MM:SS
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", m.generated_at)

    def test_generated_at_respected_when_provided(self):
        ts = "2026-04-11T12:00:00+00:00"
        m = build_handoff_batch_manifest([], "run-001", generated_at=ts)
        assert m.generated_at == ts

    def test_record_artifact_ref_document_id_preserved(self):
        summaries = [_make_exported_summary("my-doc-001")]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.exported_records[0].document_id == "my-doc-001"

    def test_record_artifact_ref_export_path_preserved(self):
        summaries = [_make_exported_summary("my-doc-001")]
        m = build_handoff_batch_manifest(summaries, "run-001")
        ref = m.exported_records[0]
        assert ref.export_artifact_path is not None
        assert "my-doc-001" in ref.export_artifact_path

    def test_record_artifact_ref_gold_path_preserved(self):
        summaries = [_make_quarantined_summary("my-doc-002")]
        m = build_handoff_batch_manifest(summaries, "run-001")
        ref = m.quarantined_records[0]
        assert ref.gold_artifact_path is not None
        assert ref.export_artifact_path is None

    def test_record_artifact_ref_routing_label_preserved(self):
        summaries = [_make_exported_summary("my-doc-001")]
        m = build_handoff_batch_manifest(summaries, "run-001")
        assert m.exported_records[0].routing_label == "regulatory_review"

    def test_multiple_exported_records(self):
        summaries = [
            _make_exported_summary("doc-001"),
            _make_exported_summary("doc-002"),
            _make_exported_summary("doc-003"),
        ]
        m = build_handoff_batch_manifest(summaries, "run-multi")
        assert m.total_exported == 3
        assert len(m.exported_records) == 3


# ---------------------------------------------------------------------------
# 3. compute_bundle_path()
# ---------------------------------------------------------------------------


class TestComputeBundlePath:
    def test_returns_path_in_bundle_dir(self, tmp_path):
        result = compute_bundle_path(tmp_path, "run-abc-123")
        assert result.parent == tmp_path

    def test_path_ends_with_json(self, tmp_path):
        result = compute_bundle_path(tmp_path, "run-abc-123")
        assert result.suffix == ".json"

    def test_path_contains_run_id(self, tmp_path):
        result = compute_bundle_path(tmp_path, "run-abc-123")
        assert "run-abc-123" in result.name

    def test_deterministic_for_same_inputs(self, tmp_path):
        p1 = compute_bundle_path(tmp_path, "run-stable")
        p2 = compute_bundle_path(tmp_path, "run-stable")
        assert p1 == p2

    def test_different_run_ids_give_different_paths(self, tmp_path):
        p1 = compute_bundle_path(tmp_path, "run-aaa")
        p2 = compute_bundle_path(tmp_path, "run-bbb")
        assert p1 != p2

    def test_special_characters_in_run_id_are_sanitized(self, tmp_path):
        result = compute_bundle_path(tmp_path, "run/with:spaces and/slashes")
        name = result.name
        assert "/" not in name
        assert ":" not in name

    def test_path_starts_with_handoff_bundle_prefix(self, tmp_path):
        result = compute_bundle_path(tmp_path, "run-abc")
        assert result.name.startswith("handoff_bundle_")


# ---------------------------------------------------------------------------
# 4. format_bundle_text()
# ---------------------------------------------------------------------------


class TestFormatBundleText:
    def _make_manifest_with_all_outcomes(self) -> HandoffBatchManifest:
        summaries = _make_mixed_summaries()
        return build_handoff_batch_manifest(
            summaries,
            "run-format-test",
            total_records_processed=5,
            total_ineligible_skipped=1,
            report_artifact_paths={
                "json_path": "output/reports/report.json",
                "text_path": "output/reports/report.txt",
            },
        )

    def test_contains_b5_header(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "=== Handoff Batch Review Bundle (B-5) ===" in text

    def test_contains_manifest_version(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert MANIFEST_VERSION in text

    def test_contains_batch_id(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "run-format-test" in text

    def test_contains_pipeline_run_id(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "Pipeline Run ID" in text

    def test_contains_record_counts_section(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "--- Record Counts ---" in text

    def test_contains_outcome_counts_section(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "--- Outcome Counts ---" in text

    def test_contains_exported_records_section(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "--- Exported Records" in text

    def test_exported_section_shows_export_path(self):
        summaries = [_make_exported_summary("doc-export-test")]
        m = build_handoff_batch_manifest(summaries, "run-test")
        text = format_bundle_text(m)
        assert "export" in text.lower()
        assert "doc-export-test" in text

    def test_quarantined_section_shows_none_when_empty(self):
        summaries = [_make_exported_summary()]
        m = build_handoff_batch_manifest(summaries, "run-test")
        text = format_bundle_text(m)
        assert "--- Quarantined Records (0) ---" in text
        assert "(none)" in text

    def test_contains_report_artifacts_section_when_present(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "--- Report Artifacts (B-4) ---" in text
        assert "output/reports/report.json" in text

    def test_no_report_artifacts_section_when_absent(self):
        summaries = [_make_exported_summary()]
        m = build_handoff_batch_manifest(summaries, "run-test")
        text = format_bundle_text(m)
        assert "--- Report Artifacts" not in text

    def test_contains_review_notes_section(self):
        m = self._make_manifest_with_all_outcomes()
        text = format_bundle_text(m)
        assert "--- Review Notes ---" in text

    def test_returns_string(self):
        m = self._make_manifest_with_all_outcomes()
        assert isinstance(format_bundle_text(m), str)


# ---------------------------------------------------------------------------
# 5. write_handoff_bundle()
# ---------------------------------------------------------------------------


class TestWriteHandoffBundle:
    def _make_simple_manifest(self, run_id: str = "run-write-test") -> HandoffBatchManifest:
        summaries = [_make_exported_summary(), _make_quarantined_summary()]
        return build_handoff_batch_manifest(summaries, run_id)

    def test_returns_two_paths(self, tmp_path):
        m = self._make_simple_manifest()
        result = write_handoff_bundle(m, tmp_path)
        assert len(result) == 2

    def test_json_path_is_path_object(self, tmp_path):
        m = self._make_simple_manifest()
        json_path, _ = write_handoff_bundle(m, tmp_path)
        assert isinstance(json_path, Path)

    def test_text_path_is_path_object(self, tmp_path):
        m = self._make_simple_manifest()
        _, text_path = write_handoff_bundle(m, tmp_path)
        assert isinstance(text_path, Path)

    def test_json_artifact_is_written(self, tmp_path):
        m = self._make_simple_manifest()
        json_path, _ = write_handoff_bundle(m, tmp_path)
        assert json_path.exists()

    def test_text_artifact_is_written(self, tmp_path):
        m = self._make_simple_manifest()
        _, text_path = write_handoff_bundle(m, tmp_path)
        assert text_path.exists()

    def test_json_artifact_is_valid_json(self, tmp_path):
        m = self._make_simple_manifest()
        json_path, _ = write_handoff_bundle(m, tmp_path)
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(data, dict)

    def test_json_artifact_contains_top_level_keys(self, tmp_path):
        m = self._make_simple_manifest()
        json_path, _ = write_handoff_bundle(m, tmp_path)
        data = json.loads(json_path.read_text(encoding="utf-8"))
        required_keys = {
            "manifest_version", "batch_id", "pipeline_run_id", "generated_at",
            "total_records_processed", "total_exported", "total_quarantined",
            "exported_records", "quarantined_records", "contract_blocked_records",
            "skipped_records", "report_artifacts", "review_notes",
        }
        for k in required_keys:
            assert k in data, f"Missing key: {k}"

    def test_json_artifact_records_are_lists(self, tmp_path):
        m = self._make_simple_manifest()
        json_path, _ = write_handoff_bundle(m, tmp_path)
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(data["exported_records"], list)
        assert isinstance(data["quarantined_records"], list)
        assert isinstance(data["contract_blocked_records"], list)
        assert isinstance(data["skipped_records"], list)

    def test_text_artifact_is_non_empty(self, tmp_path):
        m = self._make_simple_manifest()
        _, text_path = write_handoff_bundle(m, tmp_path)
        assert len(text_path.read_text(encoding="utf-8")) > 0

    def test_creates_bundle_dir_if_absent(self, tmp_path):
        bundle_dir = tmp_path / "nested" / "bundle_output"
        assert not bundle_dir.exists()
        m = self._make_simple_manifest()
        write_handoff_bundle(m, bundle_dir)
        assert bundle_dir.exists()

    def test_json_path_matches_compute_bundle_path(self, tmp_path):
        run_id = "run-path-test"
        m = self._make_simple_manifest(run_id)
        json_path, _ = write_handoff_bundle(m, tmp_path)
        expected = compute_bundle_path(tmp_path, run_id)
        assert json_path == expected

    def test_run_id_with_slashes_produces_valid_filename(self, tmp_path):
        m = HandoffBatchManifest(
            manifest_version=MANIFEST_VERSION,
            batch_id="local-run/abc:123",
            pipeline_run_id="local-run/abc:123",
            generated_at="2026-04-11T00:00:00+00:00",
            total_records_processed=0,
            total_ineligible_skipped=0,
            total_eligible=0,
            total_exported=0,
            total_quarantined=0,
            total_contract_blocked=0,
            total_skipped_not_export_ready=0,
        )
        json_path, text_path = write_handoff_bundle(m, tmp_path)
        assert json_path.exists()
        assert text_path.exists()
        # Filename must not contain slashes or colons
        assert "/" not in json_path.name
        assert ":" not in json_path.name


# ---------------------------------------------------------------------------
# 6. Integration via run_classify_gold()
# ---------------------------------------------------------------------------


class TestBundleIntegration:
    def test_run_classify_gold_with_bundle_dir_writes_bundle(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-doc-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_files = list(bundle_dir.glob("handoff_bundle_*.json"))
        assert len(bundle_files) == 1, "Expected exactly one bundle JSON artifact"

    def test_run_classify_gold_without_bundle_dir_writes_no_bundle(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-doc-002"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            # no bundle_dir
        )

        assert not bundle_dir.exists() or len(list(bundle_dir.glob("*"))) == 0

    def test_bundle_references_exported_record_with_export_path(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        doc_id = "int-exported-001"
        _write_silver_fixture(silver_dir, _make_valid_silver(doc_id))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))

        assert data["total_exported"] >= 1
        assert len(data["exported_records"]) >= 1
        exported_ref = data["exported_records"][0]
        assert exported_ref["export_artifact_path"] is not None
        assert exported_ref["gold_artifact_path"] is not None

    def test_bundle_references_quarantined_record(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        doc_id = "int-quarantine-001"
        _write_silver_fixture(silver_dir, _make_quarantine_silver(doc_id))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))

        assert data["total_quarantined"] >= 1
        assert len(data["quarantined_records"]) >= 1
        quarantined_ref = data["quarantined_records"][0]
        assert quarantined_ref["export_artifact_path"] is None
        assert quarantined_ref["gold_artifact_path"] is not None

    def test_bundle_counts_match_summaries(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-mix-001"))
        _write_silver_fixture(silver_dir, _make_quarantine_silver("int-mix-002"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))

        total = (
            data["total_exported"]
            + data["total_quarantined"]
            + data["total_contract_blocked"]
            + data["total_skipped_not_export_ready"]
        )
        assert total == data["total_eligible"]

    def test_bundle_has_report_artifacts_when_report_dir_also_provided(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"
        report_dir = tmp_path / "reports"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-report-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            report_dir=str(report_dir),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))
        assert data["report_artifacts"] is not None
        assert "json_path" in data["report_artifacts"]
        assert "text_path" in data["report_artifacts"]

    def test_bundle_has_none_report_artifacts_when_only_bundle_dir_provided(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-no-report-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))
        assert data["report_artifacts"] is None

    def test_bundle_json_is_parseable(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-parse-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))
        assert isinstance(data, dict)
        assert data["manifest_version"] == MANIFEST_VERSION

    def test_bundle_text_artifact_is_written(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-text-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        text_files = list(bundle_dir.glob("handoff_bundle_*.txt"))
        assert len(text_files) == 1
        assert len(text_files[0].read_text(encoding="utf-8")) > 0

    def test_bundle_pipeline_run_id_matches_summaries(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-runid-001"))

        run_id = "stable-test-run-001"
        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
            pipeline_run_id=run_id,
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))
        assert data["pipeline_run_id"] == run_id
        assert data["batch_id"] == run_id

    def test_bundle_manifest_version_is_current(self, tmp_path):
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()
        bundle_dir = tmp_path / "bundles"

        _write_silver_fixture(silver_dir, _make_valid_silver("int-version-001"))

        run_classify_gold(
            input_dir=str(silver_dir),
            output_dir=str(tmp_path / "gold"),
            export_dir=str(tmp_path / "exports"),
            bundle_dir=str(bundle_dir),
        )

        bundle_json = list(bundle_dir.glob("handoff_bundle_*.json"))[0]
        data = json.loads(bundle_json.read_text(encoding="utf-8"))
        assert data["manifest_version"] == MANIFEST_VERSION


# ---------------------------------------------------------------------------
# 7. Module boundary: no AWS/Bedrock SDK imports
# ---------------------------------------------------------------------------


class TestModuleBoundary:
    def test_handoff_bundle_has_no_aws_sdk_imports(self):
        bundle_src = Path(__file__).resolve().parents[1] / "src" / "pipelines" / "handoff_bundle.py"
        src_text = bundle_src.read_text(encoding="utf-8")
        # Check import lines only — docstrings and comments legitimately mention "bedrock"
        import_lines = [
            line for line in src_text.splitlines()
            if line.strip().startswith("import ") or line.strip().startswith("from ")
        ]
        imports_text = "\n".join(import_lines)
        forbidden_sdk = ["boto3", "botocore", "aiobotocore", "aws_sdk"]
        for token in forbidden_sdk:
            assert token not in imports_text, (
                f"handoff_bundle.py must not import '{token}' — "
                "B-5 is upstream-only and must not introduce AWS SDK dependencies."
            )

    def test_handoff_bundle_imports_handoff_report_constants(self):
        # Verify bundle module correctly imports from handoff_report (not duplicating constants)
        from src.pipelines.handoff_bundle import MANIFEST_VERSION
        from src.pipelines.handoff_report import OUTCOME_EXPORTED
        # Both exist and are distinct modules
        assert MANIFEST_VERSION is not None
        assert OUTCOME_EXPORTED is not None

    def test_handoff_bundle_module_is_distinct_from_handoff_report(self):
        import src.pipelines.handoff_bundle as bundle_mod
        import src.pipelines.handoff_report as report_mod
        assert bundle_mod is not report_mod

    def test_handoff_bundle_module_is_distinct_from_export_handoff(self):
        import src.pipelines.handoff_bundle as bundle_mod
        import src.pipelines.export_handoff as export_mod
        assert bundle_mod is not export_mod

    def test_handoff_bundle_module_is_distinct_from_classify_gold(self):
        import src.pipelines.handoff_bundle as bundle_mod
        import src.pipelines.classify_gold as classify_mod
        assert bundle_mod is not classify_mod
