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

SMOKE_STATUS_READY = "ready_for_phase4_closeout"
SMOKE_STATUS_INCOMPLETE = "incomplete"
SMOKE_STATUS_FAILED = "failed"
ALL_SMOKE_STATUSES = {
    SMOKE_STATUS_READY,
    SMOKE_STATUS_INCOMPLETE,
    SMOKE_STATUS_FAILED,
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
