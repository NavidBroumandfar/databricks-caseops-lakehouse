"""
src/pipelines/handoff_bundle_validation.py — B-6 Handoff Bundle Integrity and Consistency Validation.

Validates a B-5 HandoffBatchManifest for structural correctness, count consistency,
reference integrity, identifier uniqueness, and (optionally) filesystem path existence.

This module exists to answer: "Is this B-5 bundle internally trustworthy and review-safe?"

It verifies:
  - Structural correctness: manifest parses, schema version is known, batch_id is consistent
  - Count consistency: declared totals match record list lengths and outcome_distribution
  - Reference consistency: exported records have export paths; quarantined/blocked/skipped do not
  - Outcome consistency: records in each list carry the correct outcome_category and routing_label
  - Identifier uniqueness: no duplicate document_ids or gold_record_ids across all record lists
  - Filesystem path existence: referenced artifact files exist on disk (optional, on by default)

This module does NOT:
  - Re-run classification or export logic
  - Make any Bedrock or AWS API calls
  - Promote the manifest into a new source of truth
  - Duplicate business logic from B-0 through B-5

The manifest remains a derived packaging artifact. B-6 confirms it is internally coherent
with the artifacts it references — nothing more.

Module boundary:
  classify_gold.py        → assembles GoldRecord → delegates to execute_export → writes Gold artifact
  export_handoff.py       → validates contract → writes export artifact → returns ExportResult
  handoff_report.py       → derives outcome categories → aggregates batch report → writes report
  handoff_bundle.py       → packages batch into manifest/review bundle → writes bundle artifacts
  handoff_bundle_validation.py  → validates the bundle is internally consistent and trustworthy

Usage:
    # Validate a written bundle by path
    from pathlib import Path
    from src.pipelines.handoff_bundle_validation import validate_handoff_bundle

    result = validate_handoff_bundle(Path("output/reports/handoff_bundle_run123.json"))
    if not result.bundle_valid:
        print("Bundle has integrity failures:")
        for name in result.failed_checks:
            print(f"  FAIL: {name}")

    # Validate an in-memory manifest (skip filesystem path checks)
    from src.pipelines.handoff_bundle_validation import validate_handoff_bundle_from_manifest

    result = validate_handoff_bundle_from_manifest(manifest, check_paths=False)

Phase: B-6
Architecture context: ARCHITECTURE.md § Bedrock Handoff Design
Authoritative contract: docs/bedrock-handoff-contract.md
Bundle module: src/pipelines/handoff_bundle.py (B-5)
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.pipelines.handoff_bundle import (
    MANIFEST_VERSION,
    HandoffBatchManifest,
    RecordArtifactRef,
)
from src.pipelines.handoff_report import (
    OUTCOME_CONTRACT_BLOCKED,
    OUTCOME_EXPORTED,
    OUTCOME_QUARANTINED,
    OUTCOME_SKIPPED_NOT_EXPORT_READY,
)


# ---------------------------------------------------------------------------
# Check name constants
# ---------------------------------------------------------------------------

# Structural checks
CHECK_MANIFEST_PARSES = "manifest_parses_correctly"
CHECK_MANIFEST_VERSION_KNOWN = "manifest_version_known"
CHECK_BATCH_ID_CONSISTENT = "batch_id_pipeline_run_id_consistent"

# Count consistency checks
CHECK_TOTAL_RECORDS_CONSISTENT = "total_records_processed_consistent"
CHECK_TOTAL_ELIGIBLE_CONSISTENT = "total_eligible_consistent"
CHECK_TOTAL_EXPORTED_CONSISTENT = "total_exported_consistent"
CHECK_TOTAL_QUARANTINED_CONSISTENT = "total_quarantined_consistent"
CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT = "total_contract_blocked_consistent"
CHECK_TOTAL_SKIPPED_CONSISTENT = "total_skipped_consistent"
CHECK_OUTCOME_DISTRIBUTION_CONSISTENT = "outcome_distribution_consistent"

# Reference consistency checks
CHECK_EXPORTED_HAVE_EXPORT_PATHS = "exported_records_have_export_paths"
CHECK_NON_EXPORTED_NO_EXPORT_PATHS = "non_exported_records_no_export_paths"

# Outcome category consistency checks
CHECK_EXPORTED_CORRECT_OUTCOME = "exported_records_correct_outcome_category"
CHECK_QUARANTINED_CORRECT_OUTCOME = "quarantined_records_correct_outcome_category"
CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME = "contract_blocked_records_correct_outcome_category"
CHECK_SKIPPED_CORRECT_OUTCOME = "skipped_records_correct_outcome_category"
CHECK_QUARANTINED_ROUTING = "quarantined_records_have_quarantine_routing"
CHECK_EXPORTED_NON_QUARANTINE_ROUTING = "exported_records_non_quarantine_routing"

# Identifier uniqueness checks
CHECK_NO_DUPLICATE_DOCUMENT_IDS = "no_duplicate_document_ids"
CHECK_NO_DUPLICATE_GOLD_RECORD_IDS = "no_duplicate_gold_record_ids"

# Filesystem path checks (optional — only run when check_paths=True)
CHECK_REPORT_JSON_EXISTS = "report_artifact_json_exists"
CHECK_REPORT_TEXT_EXISTS = "report_artifact_text_exists"
CHECK_GOLD_ARTIFACT_PATHS_EXIST = "gold_artifact_paths_exist"
CHECK_EXPORT_ARTIFACT_PATHS_EXIST = "export_artifact_paths_exist"

ALL_STRUCTURAL_CHECKS: tuple[str, ...] = (
    CHECK_MANIFEST_VERSION_KNOWN,
    CHECK_BATCH_ID_CONSISTENT,
)

ALL_COUNT_CHECKS: tuple[str, ...] = (
    CHECK_TOTAL_RECORDS_CONSISTENT,
    CHECK_TOTAL_ELIGIBLE_CONSISTENT,
    CHECK_TOTAL_EXPORTED_CONSISTENT,
    CHECK_TOTAL_QUARANTINED_CONSISTENT,
    CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT,
    CHECK_TOTAL_SKIPPED_CONSISTENT,
    CHECK_OUTCOME_DISTRIBUTION_CONSISTENT,
)

ALL_REFERENCE_CHECKS: tuple[str, ...] = (
    CHECK_EXPORTED_HAVE_EXPORT_PATHS,
    CHECK_NON_EXPORTED_NO_EXPORT_PATHS,
    CHECK_EXPORTED_CORRECT_OUTCOME,
    CHECK_QUARANTINED_CORRECT_OUTCOME,
    CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME,
    CHECK_SKIPPED_CORRECT_OUTCOME,
    CHECK_QUARANTINED_ROUTING,
    CHECK_EXPORTED_NON_QUARANTINE_ROUTING,
)

ALL_UNIQUENESS_CHECKS: tuple[str, ...] = (
    CHECK_NO_DUPLICATE_DOCUMENT_IDS,
    CHECK_NO_DUPLICATE_GOLD_RECORD_IDS,
)

ALL_PATH_CHECKS: tuple[str, ...] = (
    CHECK_REPORT_JSON_EXISTS,
    CHECK_REPORT_TEXT_EXISTS,
    CHECK_GOLD_ARTIFACT_PATHS_EXIST,
    CHECK_EXPORT_ARTIFACT_PATHS_EXIST,
)

ALL_CHECK_NAMES: tuple[str, ...] = (
    *ALL_STRUCTURAL_CHECKS,
    *ALL_COUNT_CHECKS,
    *ALL_REFERENCE_CHECKS,
    *ALL_UNIQUENESS_CHECKS,
    *ALL_PATH_CHECKS,
)


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """
    Result of a single bundle integrity check.

    Fields
    ------
    check_name
        One of the CHECK_* constants defined in this module.
    passed
        True if the check found no integrity problems.
    detail
        Human-readable explanation when the check fails (or passes with a note).
        None when passed=True and no note is needed.
    """

    check_name: str
    passed: bool
    detail: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "detail": self.detail,
        }


@dataclass
class BundleValidationResult:
    """
    Structured result of a B-6 bundle integrity and consistency validation pass.

    Fields
    ------
    bundle_valid
        True only if every executed check passed.
    batch_id
        The batch_id from the manifest being validated (None if manifest failed to parse).
    checks_run
        Total number of checks that were executed.
    checks_passed
        Number of checks that passed.
    checks_failed
        Number of checks that failed.
    check_results
        Full list of CheckResult objects, one per executed check.
    failed_checks
        Names of all checks that failed (subset of check_results).
    missing_paths
        Filesystem paths referenced in the manifest that do not exist on disk.
        Populated only when check_paths=True.
    count_mismatches
        Human-readable descriptions of count inconsistencies found.
    duplicate_identifiers
        Descriptions of duplicate document_id or gold_record_id values found.
    contradictions
        Descriptions of contradictory reference or outcome state found.
    observations
        Non-blocking notes about the bundle state.
    validated_at
        ISO 8601 UTC timestamp when this validation was performed.
    """

    bundle_valid: bool
    batch_id: Optional[str]
    checks_run: int
    checks_passed: int
    checks_failed: int
    check_results: list = field(default_factory=list)
    failed_checks: list = field(default_factory=list)
    missing_paths: list = field(default_factory=list)
    count_mismatches: list = field(default_factory=list)
    duplicate_identifiers: list = field(default_factory=list)
    contradictions: list = field(default_factory=list)
    observations: list = field(default_factory=list)
    validated_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        return {
            "bundle_valid": self.bundle_valid,
            "batch_id": self.batch_id,
            "checks_run": self.checks_run,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "check_results": [
                r.to_dict() if isinstance(r, CheckResult) else r
                for r in self.check_results
            ],
            "failed_checks": self.failed_checks,
            "missing_paths": self.missing_paths,
            "count_mismatches": self.count_mismatches,
            "duplicate_identifiers": self.duplicate_identifiers,
            "contradictions": self.contradictions,
            "observations": self.observations,
            "validated_at": self.validated_at,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_handoff_bundle(
    bundle_json_path: Path,
    check_paths: bool = True,
) -> BundleValidationResult:
    """
    Validate a B-5 bundle from its on-disk JSON manifest file.

    Loads the manifest JSON, reconstitutes a HandoffBatchManifest, and runs
    all integrity checks. When check_paths=True, also verifies that every
    referenced artifact file exists on disk.

    Args:
        bundle_json_path:
            Path to the bundle JSON artifact (written by write_handoff_bundle()).
        check_paths:
            When True (default), verify that referenced artifact paths exist
            on the filesystem. Set to False for structural-only validation
            when artifact files are not available locally.

    Returns:
        BundleValidationResult with full check detail.
    """
    validated_at = datetime.now(tz=timezone.utc).isoformat()

    # --- Attempt to load and parse the manifest ---
    try:
        raw = json.loads(bundle_json_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _failed_parse_result(
            f"Bundle file not found: {bundle_json_path}",
            validated_at=validated_at,
        )
    except (json.JSONDecodeError, OSError) as exc:
        return _failed_parse_result(
            f"Bundle file could not be parsed as JSON: {exc}",
            validated_at=validated_at,
        )

    manifest = _dict_to_manifest(raw)
    if manifest is None:
        return _failed_parse_result(
            "Bundle JSON is missing required manifest fields and cannot be validated.",
            validated_at=validated_at,
        )

    return validate_handoff_bundle_from_manifest(
        manifest,
        check_paths=check_paths,
        validated_at=validated_at,
    )


def validate_handoff_bundle_from_manifest(
    manifest: HandoffBatchManifest,
    check_paths: bool = True,
    validated_at: Optional[str] = None,
) -> BundleValidationResult:
    """
    Validate an in-memory HandoffBatchManifest for integrity and consistency.

    Runs all structural, count, reference, and uniqueness checks. When
    check_paths=True, also verifies that referenced artifact files exist
    on the filesystem.

    Args:
        manifest:
            The HandoffBatchManifest to validate.
        check_paths:
            When True, verify referenced artifact paths exist on disk.
            When False, skip filesystem checks (useful for unit tests that
            use in-memory manifests with synthetic artifact paths).
        validated_at:
            ISO 8601 UTC timestamp to use for this result. Defaults to now.

    Returns:
        BundleValidationResult with full check detail.
    """
    if validated_at is None:
        validated_at = datetime.now(tz=timezone.utc).isoformat()

    results: list[CheckResult] = []
    missing_paths: list[str] = []
    count_mismatches: list[str] = []
    duplicate_identifiers: list[str] = []
    contradictions: list[str] = []
    observations: list[str] = []

    # --- Structural checks ---
    results.append(_check_manifest_version(manifest))
    results.append(_check_batch_id_consistent(manifest))

    # --- Count consistency checks ---
    results.extend(
        _check_count_consistency(manifest, count_mismatches)
    )

    # --- Reference and outcome consistency checks ---
    results.extend(
        _check_reference_consistency(manifest, contradictions)
    )

    # --- Identifier uniqueness checks ---
    results.extend(
        _check_identifier_uniqueness(manifest, duplicate_identifiers)
    )

    # --- Filesystem path checks ---
    if check_paths:
        results.extend(
            _check_path_existence(manifest, missing_paths)
        )

    # --- Aggregate result ---
    failed = [r for r in results if not r.passed]
    failed_names = [r.check_name for r in failed]

    if manifest.total_eligible == 0:
        observations.append(
            "Bundle contains zero eligible records. "
            "All counts are zero — this is valid but worth confirming."
        )
    if manifest.total_contract_blocked > 0:
        observations.append(
            f"{manifest.total_contract_blocked} record(s) were contract-blocked. "
            "Review contract_blocked_records for field-level contract violation detail."
        )
    if not check_paths:
        observations.append(
            "Filesystem path existence checks were skipped (check_paths=False). "
            "Run with check_paths=True to verify all referenced artifact files exist."
        )

    return BundleValidationResult(
        bundle_valid=len(failed) == 0,
        batch_id=manifest.batch_id,
        checks_run=len(results),
        checks_passed=len(results) - len(failed),
        checks_failed=len(failed),
        check_results=results,
        failed_checks=failed_names,
        missing_paths=missing_paths,
        count_mismatches=count_mismatches,
        duplicate_identifiers=duplicate_identifiers,
        contradictions=contradictions,
        observations=observations,
        validated_at=validated_at,
    )


def write_validation_result(
    result: BundleValidationResult,
    output_dir: Path,
    batch_id: Optional[str] = None,
) -> tuple[Path, Path]:
    """
    Write a BundleValidationResult to JSON and text artifacts.

    Artifact names use the batch_id (sanitized for filesystem):
        <output_dir>/bundle_validation_<batch_id>.json
        <output_dir>/bundle_validation_<batch_id>.txt

    Args:
        result: The BundleValidationResult to write.
        output_dir: Directory to write artifacts into. Created if absent.
        batch_id: Override the batch_id used in filenames. Defaults to result.batch_id.

    Returns:
        (json_path, text_path): Paths of the written artifacts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = batch_id or result.batch_id or "unknown"
    safe_id = run_id.replace("/", "_").replace(":", "_").replace(" ", "_")
    json_path = output_dir / f"bundle_validation_{safe_id}.json"
    text_path = output_dir / f"bundle_validation_{safe_id}.txt"

    json_path.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
    text_path.write_text(format_validation_result_text(result), encoding="utf-8")

    return json_path, text_path


def format_validation_result_text(result: BundleValidationResult) -> str:
    """
    Format a BundleValidationResult as a human-readable text summary.

    Suitable for terminal output and the .txt validation artifact.
    """
    status = "VALID" if result.bundle_valid else "INVALID"
    lines = [
        "=== B-6 Bundle Integrity Validation ===",
        f"Bundle Valid       : {status}",
        f"Batch ID           : {result.batch_id or '(unknown)'}",
        f"Validated At       : {result.validated_at}",
        "",
        "--- Check Summary ---",
        f"Checks Run         : {result.checks_run}",
        f"Checks Passed      : {result.checks_passed}",
        f"Checks Failed      : {result.checks_failed}",
        "",
    ]

    if result.failed_checks:
        lines.append("--- Failed Checks ---")
        for name in result.failed_checks:
            lines.append(f"  FAIL: {name}")
        lines.append("")

    if result.count_mismatches:
        lines.append("--- Count Mismatches ---")
        for msg in result.count_mismatches:
            lines.append(f"  {msg}")
        lines.append("")

    if result.missing_paths:
        lines.append("--- Missing Artifact Paths ---")
        for p in result.missing_paths:
            lines.append(f"  {p}")
        lines.append("")

    if result.duplicate_identifiers:
        lines.append("--- Duplicate Identifiers ---")
        for msg in result.duplicate_identifiers:
            lines.append(f"  {msg}")
        lines.append("")

    if result.contradictions:
        lines.append("--- Contradictions ---")
        for msg in result.contradictions:
            lines.append(f"  {msg}")
        lines.append("")

    lines.append("--- Check Detail ---")
    for r in result.check_results:
        if isinstance(r, CheckResult):
            icon = "PASS" if r.passed else "FAIL"
            detail = f" — {r.detail}" if r.detail else ""
            lines.append(f"  [{icon}] {r.check_name}{detail}")
        else:
            lines.append(f"  {r}")
    lines.append("")

    if result.observations:
        lines.append("--- Observations ---")
        for obs in result.observations:
            lines.append(f"  - {obs}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal — structural checks
# ---------------------------------------------------------------------------


def _check_manifest_version(manifest: HandoffBatchManifest) -> CheckResult:
    """Check that manifest_version matches the known MANIFEST_VERSION."""
    if manifest.manifest_version == MANIFEST_VERSION:
        return CheckResult(
            check_name=CHECK_MANIFEST_VERSION_KNOWN,
            passed=True,
        )
    return CheckResult(
        check_name=CHECK_MANIFEST_VERSION_KNOWN,
        passed=False,
        detail=(
            f"manifest_version '{manifest.manifest_version}' does not match "
            f"known version '{MANIFEST_VERSION}'"
        ),
    )


def _check_batch_id_consistent(manifest: HandoffBatchManifest) -> CheckResult:
    """Check that batch_id == pipeline_run_id (they must be identical by design)."""
    if manifest.batch_id == manifest.pipeline_run_id:
        return CheckResult(check_name=CHECK_BATCH_ID_CONSISTENT, passed=True)
    return CheckResult(
        check_name=CHECK_BATCH_ID_CONSISTENT,
        passed=False,
        detail=(
            f"batch_id '{manifest.batch_id}' != pipeline_run_id '{manifest.pipeline_run_id}'"
        ),
    )


# ---------------------------------------------------------------------------
# Internal — count consistency checks
# ---------------------------------------------------------------------------


def _check_count_consistency(
    manifest: HandoffBatchManifest,
    count_mismatches: list[str],
) -> list[CheckResult]:
    """Run all count consistency checks against the manifest."""
    results: list[CheckResult] = []

    n_exported = len(manifest.exported_records)
    n_quarantined = len(manifest.quarantined_records)
    n_contract_blocked = len(manifest.contract_blocked_records)
    n_skipped = len(manifest.skipped_records)
    sum_all = n_exported + n_quarantined + n_contract_blocked + n_skipped

    # total_records_processed == total_eligible + total_ineligible_skipped
    expected_total = manifest.total_eligible + manifest.total_ineligible_skipped
    if manifest.total_records_processed == expected_total:
        results.append(CheckResult(check_name=CHECK_TOTAL_RECORDS_CONSISTENT, passed=True))
    else:
        msg = (
            f"total_records_processed ({manifest.total_records_processed}) != "
            f"total_eligible ({manifest.total_eligible}) + "
            f"total_ineligible_skipped ({manifest.total_ineligible_skipped}) = {expected_total}"
        )
        count_mismatches.append(msg)
        results.append(CheckResult(
            check_name=CHECK_TOTAL_RECORDS_CONSISTENT, passed=False, detail=msg
        ))

    # total_eligible == sum of all record lists
    if manifest.total_eligible == sum_all:
        results.append(CheckResult(check_name=CHECK_TOTAL_ELIGIBLE_CONSISTENT, passed=True))
    else:
        msg = (
            f"total_eligible ({manifest.total_eligible}) != "
            f"sum of record lists ({sum_all}): "
            f"exported={n_exported}, quarantined={n_quarantined}, "
            f"contract_blocked={n_contract_blocked}, skipped={n_skipped}"
        )
        count_mismatches.append(msg)
        results.append(CheckResult(
            check_name=CHECK_TOTAL_ELIGIBLE_CONSISTENT, passed=False, detail=msg
        ))

    # total_exported
    results.append(_simple_count_check(
        CHECK_TOTAL_EXPORTED_CONSISTENT,
        "total_exported", manifest.total_exported,
        "exported_records", n_exported,
        count_mismatches,
    ))

    # total_quarantined
    results.append(_simple_count_check(
        CHECK_TOTAL_QUARANTINED_CONSISTENT,
        "total_quarantined", manifest.total_quarantined,
        "quarantined_records", n_quarantined,
        count_mismatches,
    ))

    # total_contract_blocked
    results.append(_simple_count_check(
        CHECK_TOTAL_CONTRACT_BLOCKED_CONSISTENT,
        "total_contract_blocked", manifest.total_contract_blocked,
        "contract_blocked_records", n_contract_blocked,
        count_mismatches,
    ))

    # total_skipped_not_export_ready
    results.append(_simple_count_check(
        CHECK_TOTAL_SKIPPED_CONSISTENT,
        "total_skipped_not_export_ready", manifest.total_skipped_not_export_ready,
        "skipped_records", n_skipped,
        count_mismatches,
    ))

    # outcome_distribution matches record list lengths
    dist = manifest.outcome_distribution
    dist_mismatches = []
    for cat, n_actual in [
        (OUTCOME_EXPORTED, n_exported),
        (OUTCOME_QUARANTINED, n_quarantined),
        (OUTCOME_CONTRACT_BLOCKED, n_contract_blocked),
        (OUTCOME_SKIPPED_NOT_EXPORT_READY, n_skipped),
    ]:
        dist_count = dist.get(cat, 0)
        if dist_count != n_actual:
            dist_mismatches.append(
                f"outcome_distribution['{cat}']={dist_count} but {cat}_records has {n_actual} entries"
            )
    if not dist_mismatches:
        results.append(CheckResult(check_name=CHECK_OUTCOME_DISTRIBUTION_CONSISTENT, passed=True))
    else:
        for msg in dist_mismatches:
            count_mismatches.append(msg)
        results.append(CheckResult(
            check_name=CHECK_OUTCOME_DISTRIBUTION_CONSISTENT,
            passed=False,
            detail="; ".join(dist_mismatches),
        ))

    return results


def _simple_count_check(
    check_name: str,
    declared_field: str,
    declared_value: int,
    list_field: str,
    list_len: int,
    count_mismatches: list[str],
) -> CheckResult:
    if declared_value == list_len:
        return CheckResult(check_name=check_name, passed=True)
    msg = f"{declared_field} ({declared_value}) != len({list_field}) ({list_len})"
    count_mismatches.append(msg)
    return CheckResult(check_name=check_name, passed=False, detail=msg)


# ---------------------------------------------------------------------------
# Internal — reference and outcome consistency checks
# ---------------------------------------------------------------------------


def _check_reference_consistency(
    manifest: HandoffBatchManifest,
    contradictions: list[str],
) -> list[CheckResult]:
    """Check reference integrity and outcome consistency across all record lists."""
    results: list[CheckResult] = []

    # --- Exported records must have non-None export_artifact_path ---
    missing_export_paths = [
        _get_id(r)
        for r in manifest.exported_records
        if _get_export_path(r) is None
    ]
    if not missing_export_paths:
        results.append(CheckResult(check_name=CHECK_EXPORTED_HAVE_EXPORT_PATHS, passed=True))
    else:
        msg = (
            f"Exported records missing export_artifact_path: {missing_export_paths}"
        )
        contradictions.append(msg)
        results.append(CheckResult(
            check_name=CHECK_EXPORTED_HAVE_EXPORT_PATHS, passed=False, detail=msg
        ))

    # --- Non-exported records must NOT have an export_artifact_path ---
    unexpected_export_paths: list[str] = []
    for rec_list_name, rec_list in [
        ("quarantined_records", manifest.quarantined_records),
        ("contract_blocked_records", manifest.contract_blocked_records),
        ("skipped_records", manifest.skipped_records),
    ]:
        for r in rec_list:
            if _get_export_path(r) is not None:
                unexpected_export_paths.append(
                    f"{rec_list_name}[document_id={_get_id(r)}] has export_artifact_path={_get_export_path(r)!r}"
                )
    if not unexpected_export_paths:
        results.append(CheckResult(check_name=CHECK_NON_EXPORTED_NO_EXPORT_PATHS, passed=True))
    else:
        for msg in unexpected_export_paths:
            contradictions.append(msg)
        results.append(CheckResult(
            check_name=CHECK_NON_EXPORTED_NO_EXPORT_PATHS,
            passed=False,
            detail="; ".join(unexpected_export_paths),
        ))

    # --- Outcome category consistency for each record list ---
    results.append(
        _check_outcome_category_for_list(
            CHECK_EXPORTED_CORRECT_OUTCOME,
            manifest.exported_records,
            OUTCOME_EXPORTED,
            "exported_records",
            contradictions,
        )
    )
    results.append(
        _check_outcome_category_for_list(
            CHECK_QUARANTINED_CORRECT_OUTCOME,
            manifest.quarantined_records,
            OUTCOME_QUARANTINED,
            "quarantined_records",
            contradictions,
        )
    )
    results.append(
        _check_outcome_category_for_list(
            CHECK_CONTRACT_BLOCKED_CORRECT_OUTCOME,
            manifest.contract_blocked_records,
            OUTCOME_CONTRACT_BLOCKED,
            "contract_blocked_records",
            contradictions,
        )
    )
    results.append(
        _check_outcome_category_for_list(
            CHECK_SKIPPED_CORRECT_OUTCOME,
            manifest.skipped_records,
            OUTCOME_SKIPPED_NOT_EXPORT_READY,
            "skipped_records",
            contradictions,
        )
    )

    # --- Quarantined records must have routing_label == 'quarantine' ---
    bad_quarantine_routing = [
        _get_id(r)
        for r in manifest.quarantined_records
        if _get_routing_label(r) != "quarantine"
    ]
    if not bad_quarantine_routing:
        results.append(CheckResult(check_name=CHECK_QUARANTINED_ROUTING, passed=True))
    else:
        msg = (
            f"Quarantined records with non-quarantine routing_label: {bad_quarantine_routing}"
        )
        contradictions.append(msg)
        results.append(CheckResult(
            check_name=CHECK_QUARANTINED_ROUTING, passed=False, detail=msg
        ))

    # --- Exported records must NOT have routing_label == 'quarantine' ---
    quarantine_in_exported = [
        _get_id(r)
        for r in manifest.exported_records
        if _get_routing_label(r) == "quarantine"
    ]
    if not quarantine_in_exported:
        results.append(CheckResult(check_name=CHECK_EXPORTED_NON_QUARANTINE_ROUTING, passed=True))
    else:
        msg = (
            f"Exported records with routing_label='quarantine' (contradictory state): "
            f"{quarantine_in_exported}"
        )
        contradictions.append(msg)
        results.append(CheckResult(
            check_name=CHECK_EXPORTED_NON_QUARANTINE_ROUTING, passed=False, detail=msg
        ))

    return results


def _check_outcome_category_for_list(
    check_name: str,
    records: list,
    expected_category: str,
    list_label: str,
    contradictions: list[str],
) -> CheckResult:
    wrong = [
        _get_id(r)
        for r in records
        if _get_outcome_category(r) != expected_category
    ]
    if not wrong:
        return CheckResult(check_name=check_name, passed=True)
    msg = (
        f"{list_label} contains records with wrong outcome_category "
        f"(expected '{expected_category}'): {wrong}"
    )
    contradictions.append(msg)
    return CheckResult(check_name=check_name, passed=False, detail=msg)


# ---------------------------------------------------------------------------
# Internal — identifier uniqueness checks
# ---------------------------------------------------------------------------


def _check_identifier_uniqueness(
    manifest: HandoffBatchManifest,
    duplicate_identifiers: list[str],
) -> list[CheckResult]:
    """Check that document_ids and gold_record_ids are unique across all record lists."""
    results: list[CheckResult] = []
    all_records = (
        list(manifest.exported_records)
        + list(manifest.quarantined_records)
        + list(manifest.contract_blocked_records)
        + list(manifest.skipped_records)
    )

    # --- document_id uniqueness ---
    doc_ids = [_get_id(r) for r in all_records if _get_id(r)]
    seen: set[str] = set()
    dupes: list[str] = []
    for doc_id in doc_ids:
        if doc_id in seen:
            dupes.append(doc_id)
        seen.add(doc_id)
    if not dupes:
        results.append(CheckResult(check_name=CHECK_NO_DUPLICATE_DOCUMENT_IDS, passed=True))
    else:
        msg = f"Duplicate document_ids across record lists: {sorted(set(dupes))}"
        duplicate_identifiers.append(msg)
        results.append(CheckResult(
            check_name=CHECK_NO_DUPLICATE_DOCUMENT_IDS, passed=False, detail=msg
        ))

    # --- gold_record_id uniqueness ---
    gold_ids = [_get_gold_record_id(r) for r in all_records if _get_gold_record_id(r)]
    seen2: set[str] = set()
    dupes2: list[str] = []
    for gid in gold_ids:
        if gid in seen2:
            dupes2.append(gid)
        seen2.add(gid)
    if not dupes2:
        results.append(CheckResult(check_name=CHECK_NO_DUPLICATE_GOLD_RECORD_IDS, passed=True))
    else:
        msg = f"Duplicate gold_record_ids across record lists: {sorted(set(dupes2))}"
        duplicate_identifiers.append(msg)
        results.append(CheckResult(
            check_name=CHECK_NO_DUPLICATE_GOLD_RECORD_IDS, passed=False, detail=msg
        ))

    return results


# ---------------------------------------------------------------------------
# Internal — filesystem path existence checks
# ---------------------------------------------------------------------------


def _check_path_existence(
    manifest: HandoffBatchManifest,
    missing_paths: list[str],
) -> list[CheckResult]:
    """Check that every referenced artifact path exists on the filesystem."""
    results: list[CheckResult] = []

    # --- Report artifact paths ---
    results.append(
        _check_single_report_path(
            CHECK_REPORT_JSON_EXISTS,
            manifest.report_artifacts,
            "json_path",
            missing_paths,
        )
    )
    results.append(
        _check_single_report_path(
            CHECK_REPORT_TEXT_EXISTS,
            manifest.report_artifacts,
            "text_path",
            missing_paths,
        )
    )

    # --- Gold artifact paths ---
    all_records = (
        list(manifest.exported_records)
        + list(manifest.quarantined_records)
        + list(manifest.contract_blocked_records)
        + list(manifest.skipped_records)
    )
    missing_gold: list[str] = []
    for r in all_records:
        gp = _get_gold_artifact_path(r)
        if gp and not Path(gp).exists():
            missing_gold.append(gp)
    if not missing_gold:
        results.append(CheckResult(check_name=CHECK_GOLD_ARTIFACT_PATHS_EXIST, passed=True))
    else:
        for p in missing_gold:
            missing_paths.append(f"gold_artifact_path: {p}")
        results.append(CheckResult(
            check_name=CHECK_GOLD_ARTIFACT_PATHS_EXIST,
            passed=False,
            detail=f"{len(missing_gold)} gold artifact path(s) not found on disk",
        ))

    # --- Export artifact paths ---
    missing_export: list[str] = []
    for r in manifest.exported_records:
        ep = _get_export_path(r)
        if ep and not Path(ep).exists():
            missing_export.append(ep)
    if not missing_export:
        results.append(CheckResult(check_name=CHECK_EXPORT_ARTIFACT_PATHS_EXIST, passed=True))
    else:
        for p in missing_export:
            missing_paths.append(f"export_artifact_path: {p}")
        results.append(CheckResult(
            check_name=CHECK_EXPORT_ARTIFACT_PATHS_EXIST,
            passed=False,
            detail=f"{len(missing_export)} export artifact path(s) not found on disk",
        ))

    return results


def _check_single_report_path(
    check_name: str,
    report_artifacts: Optional[dict],
    key: str,
    missing_paths: list[str],
) -> CheckResult:
    """Check that a single report artifact path exists (or is not referenced)."""
    if not report_artifacts:
        # No report artifacts referenced — check is not applicable; pass it.
        return CheckResult(
            check_name=check_name,
            passed=True,
            detail="No report_artifacts referenced — path check not applicable.",
        )
    path_str = report_artifacts.get(key)
    if not path_str:
        return CheckResult(
            check_name=check_name,
            passed=True,
            detail=f"report_artifacts.{key} is not set — skipped.",
        )
    if Path(path_str).exists():
        return CheckResult(check_name=check_name, passed=True)
    missing_paths.append(f"report_artifacts.{key}: {path_str}")
    return CheckResult(
        check_name=check_name,
        passed=False,
        detail=f"report_artifacts.{key} path not found on disk: {path_str}",
    )


# ---------------------------------------------------------------------------
# Internal — helpers for accessing record fields (RecordArtifactRef or dict)
# ---------------------------------------------------------------------------


def _get_id(r: object) -> str:
    if isinstance(r, RecordArtifactRef):
        return r.document_id
    if isinstance(r, dict):
        return r.get("document_id", "")
    return ""


def _get_gold_record_id(r: object) -> str:
    if isinstance(r, RecordArtifactRef):
        return r.gold_record_id
    if isinstance(r, dict):
        return r.get("gold_record_id", "")
    return ""


def _get_outcome_category(r: object) -> str:
    if isinstance(r, RecordArtifactRef):
        return r.outcome_category
    if isinstance(r, dict):
        return r.get("outcome_category", "")
    return ""


def _get_routing_label(r: object) -> str:
    if isinstance(r, RecordArtifactRef):
        return r.routing_label
    if isinstance(r, dict):
        return r.get("routing_label", "")
    return ""


def _get_gold_artifact_path(r: object) -> Optional[str]:
    if isinstance(r, RecordArtifactRef):
        return r.gold_artifact_path
    if isinstance(r, dict):
        return r.get("gold_artifact_path")
    return None


def _get_export_path(r: object) -> Optional[str]:
    if isinstance(r, RecordArtifactRef):
        return r.export_artifact_path
    if isinstance(r, dict):
        return r.get("export_artifact_path")
    return None


# ---------------------------------------------------------------------------
# Internal — manifest reconstruction from dict
# ---------------------------------------------------------------------------


def _dict_to_manifest(raw: dict) -> Optional[HandoffBatchManifest]:
    """
    Reconstruct a HandoffBatchManifest from a parsed JSON dict.

    Returns None if required fields are missing and reconstruction is not possible.
    """
    required = {
        "manifest_version", "batch_id", "pipeline_run_id", "generated_at",
        "total_records_processed", "total_ineligible_skipped", "total_eligible",
        "total_exported", "total_quarantined", "total_contract_blocked",
        "total_skipped_not_export_ready",
    }
    if not required.issubset(raw.keys()):
        return None

    def _to_refs(items: list) -> list:
        """Convert list of dicts to RecordArtifactRef objects."""
        refs = []
        for item in items:
            if isinstance(item, RecordArtifactRef):
                refs.append(item)
            elif isinstance(item, dict):
                refs.append(RecordArtifactRef(
                    document_id=item.get("document_id", ""),
                    gold_record_id=item.get("gold_record_id", ""),
                    outcome_category=item.get("outcome_category", ""),
                    outcome_reason=item.get("outcome_reason", ""),
                    routing_label=item.get("routing_label", ""),
                    gold_artifact_path=item.get("gold_artifact_path"),
                    export_artifact_path=item.get("export_artifact_path"),
                ))
        return refs

    return HandoffBatchManifest(
        manifest_version=raw["manifest_version"],
        batch_id=raw["batch_id"],
        pipeline_run_id=raw["pipeline_run_id"],
        generated_at=raw["generated_at"],
        total_records_processed=raw["total_records_processed"],
        total_ineligible_skipped=raw["total_ineligible_skipped"],
        total_eligible=raw["total_eligible"],
        total_exported=raw["total_exported"],
        total_quarantined=raw["total_quarantined"],
        total_contract_blocked=raw["total_contract_blocked"],
        total_skipped_not_export_ready=raw["total_skipped_not_export_ready"],
        outcome_distribution=raw.get("outcome_distribution", {}),
        exported_records=_to_refs(raw.get("exported_records", [])),
        quarantined_records=_to_refs(raw.get("quarantined_records", [])),
        contract_blocked_records=_to_refs(raw.get("contract_blocked_records", [])),
        skipped_records=_to_refs(raw.get("skipped_records", [])),
        report_artifacts=raw.get("report_artifacts"),
        review_notes=raw.get("review_notes", []),
    )


# ---------------------------------------------------------------------------
# Internal — failure result factory
# ---------------------------------------------------------------------------


def _failed_parse_result(
    detail: str,
    validated_at: Optional[str] = None,
) -> BundleValidationResult:
    """Return a validation result indicating the manifest could not be loaded/parsed."""
    if validated_at is None:
        validated_at = datetime.now(tz=timezone.utc).isoformat()
    parse_failure = CheckResult(
        check_name=CHECK_MANIFEST_PARSES,
        passed=False,
        detail=detail,
    )
    return BundleValidationResult(
        bundle_valid=False,
        batch_id=None,
        checks_run=1,
        checks_passed=0,
        checks_failed=1,
        check_results=[parse_failure],
        failed_checks=[CHECK_MANIFEST_PARSES],
        contradictions=[detail],
        validated_at=validated_at,
    )
