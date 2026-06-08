"""
Phase 4 runtime smoke evidence package schema.

This schema validates the local, sanitized evidence package produced after an
operator runs the Databricks runtime smoke workflow. It does not execute
Databricks APIs or inspect live workspaces.
"""

from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


RUNTIME_SMOKE_SCHEMA_VERSION = "v0.1.0"
RUNTIME_SMOKE_PLAN_SCHEMA_VERSION = "v0.1.0"
RUNTIME_SMOKE_RUN_CONTEXT_SCHEMA_VERSION = "v0.1.0"
RUNTIME_SMOKE_PREFLIGHT_SCHEMA_VERSION = "v0.1.0"

SMOKE_STATUS_READY = "ready_for_phase4_closeout"
SMOKE_STATUS_INCOMPLETE = "incomplete"
SMOKE_STATUS_FAILED = "failed"
ALL_SMOKE_STATUSES = {
    SMOKE_STATUS_READY,
    SMOKE_STATUS_INCOMPLETE,
    SMOKE_STATUS_FAILED,
}

SMOKE_PREFLIGHT_STATUS_READY = "ready_for_workspace_smoke"
SMOKE_PREFLIGHT_STATUS_BLOCKED = "blocked"
ALL_SMOKE_PREFLIGHT_STATUSES = {
    SMOKE_PREFLIGHT_STATUS_READY,
    SMOKE_PREFLIGHT_STATUS_BLOCKED,
}

SMOKE_WORKSPACE_PERSONAL = "personal_databricks"
SMOKE_WORKSPACE_ENTERPRISE = "enterprise_databricks"
ALL_SMOKE_WORKSPACE_MODES = {
    SMOKE_WORKSPACE_PERSONAL,
    SMOKE_WORKSPACE_ENTERPRISE,
}

SMOKE_ENVIRONMENTS = {"dev", "staging", "prod"}


class RuntimeSmokeCheck(BaseModel):
    """One check result from the Phase 4 smoke package validator."""

    check_name: str = Field(description="Stable check identifier.")
    passed: bool = Field(description="Whether the check passed.")
    detail: Optional[str] = Field(default=None, description="Human-readable check detail.")


class RuntimeSmokeValidationResult(BaseModel):
    """Structured validation result for a Phase 4 runtime smoke evidence package."""

    validation_run_id: str = Field(description="UUID v4 identifying this validation run.")
    pipeline_run_id: str = Field(description="Pipeline run ID being validated.")
    environment: str = Field(description="Runtime CASEOPS environment validated.")
    workspace_mode: str = Field(description="Workspace context for the smoke evidence.")
    validated_at: str = Field(description="UTC ISO 8601 validation timestamp.")
    smoke_status: str = Field(description="Overall smoke package status.")
    smoke_reason: str = Field(description="Human-readable explanation of smoke_status.")
    checks_passed: List[str] = Field(default_factory=list)
    checks_failed: List[str] = Field(default_factory=list)
    check_details: List[RuntimeSmokeCheck] = Field(default_factory=list)
    artifacts_checked: List[str] = Field(default_factory=list)
    observations: List[str] = Field(default_factory=list)
    schema_version: str = Field(default=RUNTIME_SMOKE_SCHEMA_VERSION)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        if value not in SMOKE_ENVIRONMENTS:
            raise ValueError(f"environment must be one of {sorted(SMOKE_ENVIRONMENTS)}")
        return value

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, value: str) -> str:
        if value not in ALL_SMOKE_WORKSPACE_MODES:
            raise ValueError(
                f"workspace_mode must be one of {sorted(ALL_SMOKE_WORKSPACE_MODES)}"
            )
        return value

    @field_validator("smoke_status")
    @classmethod
    def validate_smoke_status(cls, value: str) -> str:
        if value not in ALL_SMOKE_STATUSES:
            raise ValueError(f"smoke_status must be one of {sorted(ALL_SMOKE_STATUSES)}")
        return value

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != RUNTIME_SMOKE_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be '{RUNTIME_SMOKE_SCHEMA_VERSION}'"
            )
        return value

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)


class RuntimeSmokePreflightResult(BaseModel):
    """
    Structured validation result for pre-workspace smoke coordination artifacts.

    This result validates whether a capture plan and run context are internally
    consistent before an operator runs Databricks. It is not runtime smoke
    evidence and cannot be used for Phase 4 closeout.
    """

    preflight_run_id: str = Field(description="UUID v4 identifying this preflight run.")
    pipeline_run_id: str = Field(description="Pipeline run ID being prepared.")
    environment: str = Field(description="Runtime CASEOPS environment prepared.")
    workspace_mode: str = Field(description="Workspace context expected for the smoke run.")
    checked_at: str = Field(description="UTC ISO 8601 preflight timestamp.")
    preflight_status: str = Field(description="Overall preflight status.")
    preflight_reason: str = Field(description="Human-readable explanation of preflight_status.")
    checks_passed: List[str] = Field(default_factory=list)
    checks_failed: List[str] = Field(default_factory=list)
    check_details: List[RuntimeSmokeCheck] = Field(default_factory=list)
    artifacts_checked: List[str] = Field(default_factory=list)
    observations: List[str] = Field(default_factory=list)
    schema_version: str = Field(default=RUNTIME_SMOKE_PREFLIGHT_SCHEMA_VERSION)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        if value not in SMOKE_ENVIRONMENTS:
            raise ValueError(f"environment must be one of {sorted(SMOKE_ENVIRONMENTS)}")
        return value

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, value: str) -> str:
        if value not in ALL_SMOKE_WORKSPACE_MODES:
            raise ValueError(
                f"workspace_mode must be one of {sorted(ALL_SMOKE_WORKSPACE_MODES)}"
            )
        return value

    @field_validator("preflight_status")
    @classmethod
    def validate_preflight_status(cls, value: str) -> str:
        if value not in ALL_SMOKE_PREFLIGHT_STATUSES:
            raise ValueError(
                f"preflight_status must be one of {sorted(ALL_SMOKE_PREFLIGHT_STATUSES)}"
            )
        return value

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != RUNTIME_SMOKE_PREFLIGHT_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be '{RUNTIME_SMOKE_PREFLIGHT_SCHEMA_VERSION}'"
            )
        return value

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)


class RuntimeSmokeExpectedArtifact(BaseModel):
    """One artifact that must be captured for a Phase 4 smoke package."""

    artifact_name: str = Field(description="Stable artifact identifier.")
    path: str = Field(description="Expected local-safe artifact path.")
    required: bool = Field(default=True, description="Whether closeout requires this artifact.")
    description: str = Field(description="Human-readable artifact purpose.")


class RuntimeSmokeEnvironmentVariable(BaseModel):
    """One non-secret environment variable for repeatable smoke execution."""

    name: str = Field(description="Environment variable name.")
    value: str = Field(description="Non-secret value to export for the smoke run.")
    required: bool = Field(default=True, description="Whether the smoke run expects this value.")
    description: str = Field(description="Human-readable variable purpose.")


class RuntimeSmokeRunContext(BaseModel):
    """
    Non-secret run context for a repeatable Phase 4 workspace smoke run.

    The context centralizes run-scoped IDs, expected artifact paths, and
    non-secret environment variables that an operator must keep consistent
    across the Databricks bundle run and local evidence validation.
    """

    context_id: str = Field(description="UUID v4 identifying this run context.")
    pipeline_run_id: str = Field(description="Pipeline run ID for this smoke run.")
    environment: str = Field(description="Runtime CASEOPS environment.")
    workspace_mode: str = Field(description="Workspace context expected for closeout evidence.")
    generated_at: str = Field(description="UTC ISO 8601 timestamp when the context was generated.")
    capture_plan_path: str = Field(description="JSON capture plan path for this run.")
    capture_plan_text_path: str = Field(description="Text capture plan path for this run.")
    environment_variables: List[RuntimeSmokeEnvironmentVariable] = Field(default_factory=list)
    observations: List[str] = Field(default_factory=list)
    schema_version: str = Field(default=RUNTIME_SMOKE_RUN_CONTEXT_SCHEMA_VERSION)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        if value not in SMOKE_ENVIRONMENTS:
            raise ValueError(f"environment must be one of {sorted(SMOKE_ENVIRONMENTS)}")
        return value

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, value: str) -> str:
        if value not in ALL_SMOKE_WORKSPACE_MODES:
            raise ValueError(
                f"workspace_mode must be one of {sorted(ALL_SMOKE_WORKSPACE_MODES)}"
            )
        return value

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != RUNTIME_SMOKE_RUN_CONTEXT_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be '{RUNTIME_SMOKE_RUN_CONTEXT_SCHEMA_VERSION}'"
            )
        return value

    def environment_variable(self, name: str) -> Optional[str]:
        for variable in self.environment_variables:
            if variable.name == name:
                return variable.value
        return None

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)


class RuntimeSmokeCapturePlan(BaseModel):
    """
    Run-specific capture plan for a repeatable Phase 4 workspace smoke run.

    The plan is generated before the Databricks run. It records expected output
    paths and local validation commands, but it is not evidence that a workspace
    smoke test has completed.
    """

    capture_plan_id: str = Field(description="UUID v4 identifying this capture plan.")
    pipeline_run_id: str = Field(description="Pipeline run ID the operator should use.")
    environment: str = Field(description="Runtime CASEOPS environment.")
    workspace_mode: str = Field(description="Workspace context expected for closeout evidence.")
    generated_at: str = Field(description="UTC ISO 8601 timestamp when the plan was generated.")
    expected_artifacts: List[RuntimeSmokeExpectedArtifact] = Field(default_factory=list)
    workspace_commands: List[str] = Field(default_factory=list)
    local_validation_commands: List[str] = Field(default_factory=list)
    sanitization_rules: List[str] = Field(default_factory=list)
    observations: List[str] = Field(default_factory=list)
    schema_version: str = Field(default=RUNTIME_SMOKE_PLAN_SCHEMA_VERSION)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        if value not in SMOKE_ENVIRONMENTS:
            raise ValueError(f"environment must be one of {sorted(SMOKE_ENVIRONMENTS)}")
        return value

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, value: str) -> str:
        if value not in ALL_SMOKE_WORKSPACE_MODES:
            raise ValueError(
                f"workspace_mode must be one of {sorted(ALL_SMOKE_WORKSPACE_MODES)}"
            )
        return value

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != RUNTIME_SMOKE_PLAN_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be '{RUNTIME_SMOKE_PLAN_SCHEMA_VERSION}'"
            )
        return value

    def artifact_path(self, artifact_name: str) -> Optional[str]:
        for artifact in self.expected_artifacts:
            if artifact.artifact_name == artifact_name:
                return artifact.path
        return None

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)
