"""
src/schemas/delivery_validation.py — C-2: Delivery-Layer Runtime Validation Result Schema.

Pydantic models for the structured runtime validation results produced
by the C-2 delivery-layer validation layer (src/pipelines/delivery_validation.py).

A DeliveryValidationResult captures the outcome of one validation run against
the C-1 delivery artifacts: the delivery event record, the Delta Share
preparation manifest, and the B-5 bundle artifact reference chain.

Validation status vocabulary
----------------------------
validated
    All required checks passed and runtime evidence was collected in a live
    Databricks workspace (workspace_mode = 'personal_databricks'). This status
    requires end_to_end scope. It MUST NOT be assigned for local-repo-only runs.

partially_validated
    All repo-side artifact checks passed; the Delta Share is not yet provisioned
    or runtime evidence was not collected. Producer-side layer is correct.
    Appropriate for local_repo_only runs with no check failures.

not_provisioned
    The Delta Share has not been created in Unity Catalog (manifest status =
    'designed'). Runtime validation cannot proceed until the setup_sql is
    executed in a Databricks workspace. Repo-side artifact checks may still pass.

failed
    One or more checks explicitly failed — schema version mismatch, ID
    inconsistency, missing required artifact, unparseable JSON, or evidence
    sufficiency violation.

Validation scope vocabulary
---------------------------
producer_side_only
    Only repo-side artifacts are validated. No live Databricks workspace queries
    are executed. This is the only scope supported by local execution.

end_to_end
    Full validation including runtime workspace evidence (SQL query results,
    share queryability, delivery event table row presence, payload conformance).
    Requires workspace_mode = 'personal_databricks'.

Workspace mode vocabulary
--------------------------
local_repo_only
    No Databricks workspace available. All checks operate on repo artifacts only.
    Status can be at most 'partially_validated' or 'not_provisioned' in this mode.

personal_databricks
    Validated against a personal (non-production) Databricks workspace. Sufficient
    for 'validated' status with end_to_end scope.

Phase: C-2
Architecture context: ARCHITECTURE.md § Phase C-1 Delivery Layer — Implementation Status
Design decision: docs/live-handoff-design.md § 11 (C-2 Validation Direction)
C-2 design: docs/delivery-runtime-validation.md
"""

from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Constants — validation status vocabulary
# ---------------------------------------------------------------------------

VALIDATION_SCHEMA_VERSION = "v0.2.0"

VALIDATION_STATUS_VALIDATED = "validated"
"""
All required checks passed with runtime evidence from a live workspace.
Requires end_to_end scope and personal_databricks workspace mode.
"""

VALIDATION_STATUS_PARTIALLY_VALIDATED = "partially_validated"
"""
Producer-side artifact checks passed; runtime validation evidence is
incomplete or the Delta Share has not been provisioned yet.
Appropriate for local_repo_only runs with no check failures.
"""

VALIDATION_STATUS_NOT_PROVISIONED = "not_provisioned"
"""
The Delta Share has not been provisioned in Unity Catalog.
Manifest status is 'designed'; runtime validation cannot proceed
until setup_sql is executed in a Databricks workspace.
"""

VALIDATION_STATUS_FAILED = "failed"
"""
One or more checks explicitly failed: schema version mismatch,
cross-artifact ID inconsistency, unparseable artifact, missing
required field, or evidence sufficiency violation.
"""

ALL_VALIDATION_STATUSES = (
    VALIDATION_STATUS_VALIDATED,
    VALIDATION_STATUS_PARTIALLY_VALIDATED,
    VALIDATION_STATUS_NOT_PROVISIONED,
    VALIDATION_STATUS_FAILED,
)


# ---------------------------------------------------------------------------
# Constants — validation scope vocabulary
# ---------------------------------------------------------------------------

VALIDATION_SCOPE_PRODUCER_SIDE_ONLY = "producer_side_only"
"""
Only repo-side artifacts are validated. No live workspace queries executed.
"""

VALIDATION_SCOPE_END_TO_END = "end_to_end"
"""
Full validation including runtime workspace evidence.
Requires personal_databricks workspace mode.
"""

ALL_VALIDATION_SCOPES = (
    VALIDATION_SCOPE_PRODUCER_SIDE_ONLY,
    VALIDATION_SCOPE_END_TO_END,
)


# ---------------------------------------------------------------------------
# Constants — workspace mode vocabulary
# ---------------------------------------------------------------------------

WORKSPACE_MODE_LOCAL_REPO_ONLY = "local_repo_only"
"""
No live Databricks workspace. All checks operate on repo artifacts.
Status is at most 'partially_validated' or 'not_provisioned'.
"""

WORKSPACE_MODE_PERSONAL_DATABRICKS = "personal_databricks"
"""
Validated in a personal (non-production) Databricks workspace.
Sufficient for 'validated' status with end_to_end scope.
"""

ALL_WORKSPACE_MODES = (
    WORKSPACE_MODE_LOCAL_REPO_ONLY,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
)


# ---------------------------------------------------------------------------
# Constants — check name vocabulary
# ---------------------------------------------------------------------------

CHECK_DELIVERY_EVENT_EXISTS = "delivery_event_exists"
CHECK_DELIVERY_EVENT_PARSEABLE = "delivery_event_parseable"
CHECK_DELIVERY_EVENT_SCHEMA_VERSION = "delivery_event_schema_version"
CHECK_DELIVERY_EVENT_STATUS_KNOWN = "delivery_event_status_known"
CHECK_DELIVERY_MECHANISM_KNOWN = "delivery_mechanism_known"
CHECK_SHARE_MANIFEST_EXISTS = "share_manifest_exists"
CHECK_SHARE_MANIFEST_PARSEABLE = "share_manifest_parseable"
CHECK_SHARE_MANIFEST_HAS_SETUP_SQL = "share_manifest_has_setup_sql"
CHECK_SHARE_MANIFEST_HAS_C2_QUERIES = "share_manifest_has_c2_queries"
CHECK_CROSS_ID_CONSISTENCY = "cross_id_consistency"
CHECK_BUNDLE_PATH_REFERENCED = "bundle_path_referenced"
CHECK_BUNDLE_PATH_EXISTS = "bundle_path_exists"
CHECK_ROUTING_LABELS_PRESENT = "routing_labels_present"
CHECK_EVIDENCE_SUFFICIENCY = "evidence_sufficiency"
CHECK_SHARE_PROVISIONING_ACKNOWLEDGED = "share_provisioning_acknowledged"

ALL_CHECK_NAMES = (
    CHECK_DELIVERY_EVENT_EXISTS,
    CHECK_DELIVERY_EVENT_PARSEABLE,
    CHECK_DELIVERY_EVENT_SCHEMA_VERSION,
    CHECK_DELIVERY_EVENT_STATUS_KNOWN,
    CHECK_DELIVERY_MECHANISM_KNOWN,
    CHECK_SHARE_MANIFEST_EXISTS,
    CHECK_SHARE_MANIFEST_PARSEABLE,
    CHECK_SHARE_MANIFEST_HAS_SETUP_SQL,
    CHECK_SHARE_MANIFEST_HAS_C2_QUERIES,
    CHECK_CROSS_ID_CONSISTENCY,
    CHECK_BUNDLE_PATH_REFERENCED,
    CHECK_BUNDLE_PATH_EXISTS,
    CHECK_ROUTING_LABELS_PRESENT,
    CHECK_EVIDENCE_SUFFICIENCY,
    CHECK_SHARE_PROVISIONING_ACKNOWLEDGED,
)


# ---------------------------------------------------------------------------
# Check result model
# ---------------------------------------------------------------------------


class CheckResult(BaseModel):
    """Result of a single named validation check."""

    check_name: str = Field(description="Name of the check. One of ALL_CHECK_NAMES.")
    passed: bool = Field(description="True if the check passed.")
    detail: Optional[str] = Field(
        default=None,
        description="Human-readable explanation of the check outcome.",
    )

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())


# ---------------------------------------------------------------------------
# Delivery validation result model
# ---------------------------------------------------------------------------


class DeliveryValidationResult(BaseModel):
    """
    Structured result of one delivery-layer runtime validation run.

    This is the authoritative C-2 artifact. It records what was checked,
    what passed, what failed, and what scope the validation covered. It
    is always honest: status 'validated' MUST NOT be assigned for
    local-repo-only runs, and status 'not_provisioned' is required when
    the Delta Share has not been executed in Unity Catalog.

    Fields
    ------
    validation_run_id
        UUID v4 uniquely identifying this validation run.
    pipeline_run_id
        Pipeline run ID of the batch being validated.
    delivery_event_id
        Delivery event ID being validated. Null if the delivery event
        artifact was not found or could not be parsed.
    validated_at
        UTC ISO 8601 timestamp when this validation run was performed.
    delivery_mechanism
        Delivery mechanism under validation. 'delta_sharing' for C-2.
    share_name
        Delta Share name configured in the delivery event.
    shared_object_name
        Shared table name configured in the delivery event.
    validation_scope
        Scope of this run. See VALIDATION_SCOPE_* constants.
    validation_status
        Overall result. See VALIDATION_STATUS_* constants.
    validation_reason
        Human-readable explanation of the validation_status. Must be
        explicit about what was and was not validated.
    checks_passed
        List of check names (CHECK_* constants) that passed.
    checks_failed
        List of check names (CHECK_* constants) that explicitly failed.
    check_details
        Per-check results with detail strings.
    observations
        Ordered human-readable observations from the validation run.
        Includes positive findings and explicit caveats.
    artifacts_checked
        Artifact paths that were opened and read during this run.
    queries_executed
        SQL query names or descriptors executed during this run.
        Empty when workspace_mode = 'local_repo_only'.
    workspace_mode
        Workspace context. See WORKSPACE_MODE_* constants.
    schema_version
        Data contract version. Always 'v0.2.0' for C-2 results.
    """

    validation_run_id: str = Field(
        description="UUID v4 uniquely identifying this validation run."
    )
    pipeline_run_id: str = Field(
        description="Pipeline run ID being validated."
    )
    delivery_event_id: Optional[str] = Field(
        default=None,
        description=(
            "Delivery event ID being validated. "
            "Null if the delivery event was not found or could not be parsed."
        ),
    )
    validated_at: str = Field(
        description="UTC ISO 8601 timestamp when this validation run was performed."
    )
    delivery_mechanism: str = Field(
        default="delta_sharing",
        description="Delivery mechanism under validation. 'delta_sharing' for C-2.",
    )
    share_name: Optional[str] = Field(
        default=None,
        description="Delta Share name from the delivery event or manifest.",
    )
    shared_object_name: Optional[str] = Field(
        default=None,
        description="Shared table name from the delivery event or manifest.",
    )
    validation_scope: str = Field(
        description=(
            "Scope of this validation run. "
            "'producer_side_only' = repo artifacts only. "
            "'end_to_end' = includes runtime workspace evidence."
        )
    )
    validation_status: str = Field(
        description=(
            "Overall validation status. "
            "See VALIDATION_STATUS_* constants in this module for the full vocabulary."
        )
    )
    validation_reason: str = Field(
        description=(
            "Human-readable explanation of the validation_status. "
            "Must be explicit about what was and was not validated."
        )
    )
    checks_passed: List[str] = Field(
        default_factory=list,
        description="Names of checks that passed (CHECK_* constants).",
    )
    checks_failed: List[str] = Field(
        default_factory=list,
        description="Names of checks that explicitly failed (CHECK_* constants).",
    )
    check_details: List[CheckResult] = Field(
        default_factory=list,
        description="Per-check results with full detail strings.",
    )
    observations: List[str] = Field(
        default_factory=list,
        description=(
            "Ordered list of human-readable observations from the validation run. "
            "Includes positive findings and explicit caveats."
        ),
    )
    artifacts_checked: List[str] = Field(
        default_factory=list,
        description="Artifact paths that were opened and read during this run.",
    )
    queries_executed: List[str] = Field(
        default_factory=list,
        description=(
            "SQL query names or descriptors executed during this run. "
            "Empty when workspace_mode = 'local_repo_only'."
        ),
    )
    workspace_mode: str = Field(
        description=(
            "Workspace context for this validation run. "
            "'local_repo_only' = no live workspace queries. "
            "'personal_databricks' = validated in personal workspace."
        )
    )
    schema_version: str = Field(
        default=VALIDATION_SCHEMA_VERSION,
        description="Data contract version. Always 'v0.2.0' for C-2 results.",
    )

    @field_validator("validation_status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in ALL_VALIDATION_STATUSES:
            raise ValueError(
                f"validation_status '{v}' is not a known status. "
                f"Known: {sorted(ALL_VALIDATION_STATUSES)}"
            )
        return v

    @field_validator("validation_scope")
    @classmethod
    def validate_scope(cls, v: str) -> str:
        if v not in ALL_VALIDATION_SCOPES:
            raise ValueError(
                f"validation_scope '{v}' is not a known scope. "
                f"Known: {sorted(ALL_VALIDATION_SCOPES)}"
            )
        return v

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, v: str) -> str:
        if v not in ALL_WORKSPACE_MODES:
            raise ValueError(
                f"workspace_mode '{v}' is not a known mode. "
                f"Known: {sorted(ALL_WORKSPACE_MODES)}"
            )
        return v

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != VALIDATION_SCHEMA_VERSION:
            raise ValueError(
                f"Delivery validation results require "
                f"schema_version='{VALIDATION_SCHEMA_VERSION}'. Got: '{v}'"
            )
        return v

    def to_json_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        """Return a formatted JSON string."""
        return json.dumps(self.to_json_dict(), indent=indent)
