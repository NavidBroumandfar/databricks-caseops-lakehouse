"""
Phase 3 runtime evidence schema for Delta Share validation.

This schema is for sanitized Databricks workspace evidence collected after a
human operator runs the generated Delta Share setup SQL and validation queries.
It stores query-level proof without credentials, activation links, workspace
URLs, or personal identifiers.
"""

from __future__ import annotations

import json
import re
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from src.schemas.delivery_validation import (
    VALIDATION_SCOPE_END_TO_END,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
)


RUNTIME_EVIDENCE_VERSION = "v0.1.0"

REQUIRED_RUNTIME_QUERY_NAMES = (
    "confirm_share_exists",
    "query_export_ready_records",
    "query_delivery_events",
    "verify_routing_label_transparency",
)

REQUIRED_RUNTIME_ASSERTIONS = (
    "share_exists",
    "gold_table_visible",
    "export_ready_records_visible",
    "delivery_event_row_visible",
    "routing_labels_visible",
    "schema_versions_visible",
)

_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"dbc-[a-z0-9-]+\.cloud\.databricks\.com", re.IGNORECASE),
    re.compile(r"dapi[a-f0-9]{32}", re.IGNORECASE),
    re.compile(r"activation\s+link", re.IGNORECASE),
    re.compile(r"workspace\s+url", re.IGNORECASE),
    re.compile(r"(token|secret|password)\s*[:=]", re.IGNORECASE),
)


class RuntimeEvidenceSanitization(BaseModel):
    """Operator assertions that sensitive runtime details were removed."""

    workspace_url_removed: bool = Field(
        description="True when Databricks workspace URLs were removed."
    )
    activation_links_removed: bool = Field(
        description="True when Delta Sharing recipient activation links were removed."
    )
    tokens_removed: bool = Field(
        description="True when tokens, PATs, or credentials were removed."
    )
    personal_identifiers_removed: bool = Field(
        description="True when personal names, emails, and account IDs were removed."
    )

    def all_confirmed(self) -> bool:
        return all(
            (
                self.workspace_url_removed,
                self.activation_links_removed,
                self.tokens_removed,
                self.personal_identifiers_removed,
            )
        )


class RuntimeEvidenceQueryResult(BaseModel):
    """Sanitized summary of one Databricks validation query result."""

    name: str = Field(description="Validation query name from c2_validation_queries.")
    executed: bool = Field(description="True if the query was executed in Databricks SQL.")
    passed: bool = Field(description="True if the result satisfied the expected condition.")
    row_count: int = Field(
        ge=0,
        description="Sanitized row count observed for the query result.",
    )
    observed_fields: List[str] = Field(
        default_factory=list,
        description="Column names observed in the sanitized query output.",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Short sanitized note about the query result.",
    )

    @field_validator("notes")
    @classmethod
    def notes_must_be_sanitized(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        for pattern in _SENSITIVE_TEXT_PATTERNS:
            if pattern.search(value):
                raise ValueError("runtime evidence notes contain unsanitized sensitive text")
        return value


class DeliveryRuntimeEvidence(BaseModel):
    """
    Sanitized evidence that the Delta Share delivery layer was runtime validated.

    This model intentionally stores summaries, counts, and assertion booleans
    instead of raw query outputs. Raw exports from Databricks may contain
    workspace/account-specific data and should remain under ignored local paths.
    """

    evidence_version: str = Field(
        default=RUNTIME_EVIDENCE_VERSION,
        description="Runtime evidence schema version.",
    )
    captured_at: str = Field(description="UTC ISO 8601 timestamp when evidence was captured.")
    workspace_mode: str = Field(
        default=WORKSPACE_MODE_PERSONAL_DATABRICKS,
        description="Must be personal_databricks for Phase 3 evidence.",
    )
    validation_scope: str = Field(
        default=VALIDATION_SCOPE_END_TO_END,
        description="Must be end_to_end for Phase 3 evidence.",
    )
    pipeline_run_id: str = Field(description="Pipeline run ID validated in the workspace.")
    delivery_event_id: Optional[str] = Field(
        default=None,
        description="Delivery event ID validated in the workspace.",
    )
    share_name: str = Field(description="Delta Share name validated in the workspace.")
    shared_object_name: str = Field(description="Shared Gold table object name.")
    manifest_status: str = Field(
        description="Expected to be provisioned after setup_sql is executed."
    )
    queries: List[RuntimeEvidenceQueryResult] = Field(
        default_factory=list,
        description="Sanitized validation query summaries.",
    )
    runtime_assertions: Dict[str, bool] = Field(
        default_factory=dict,
        description="Boolean assertions for the Phase 3 acceptance criteria.",
    )
    sanitization: RuntimeEvidenceSanitization = Field(
        description="Operator assertions that sensitive runtime values were removed."
    )
    notes: List[str] = Field(
        default_factory=list,
        description="Short sanitized notes. Do not include workspace URLs or credentials.",
    )

    @field_validator("evidence_version")
    @classmethod
    def validate_evidence_version(cls, value: str) -> str:
        if value != RUNTIME_EVIDENCE_VERSION:
            raise ValueError(
                f"runtime evidence requires evidence_version='{RUNTIME_EVIDENCE_VERSION}'"
            )
        return value

    @field_validator("workspace_mode")
    @classmethod
    def validate_workspace_mode(cls, value: str) -> str:
        if value != WORKSPACE_MODE_PERSONAL_DATABRICKS:
            raise ValueError("runtime evidence requires workspace_mode='personal_databricks'")
        return value

    @field_validator("validation_scope")
    @classmethod
    def validate_validation_scope(cls, value: str) -> str:
        if value != VALIDATION_SCOPE_END_TO_END:
            raise ValueError("runtime evidence requires validation_scope='end_to_end'")
        return value

    @field_validator("manifest_status")
    @classmethod
    def validate_manifest_status(cls, value: str) -> str:
        if value != "provisioned":
            raise ValueError("runtime evidence requires manifest_status='provisioned'")
        return value

    @field_validator("notes")
    @classmethod
    def notes_must_be_sanitized(cls, values: List[str]) -> List[str]:
        for note in values:
            for pattern in _SENSITIVE_TEXT_PATTERNS:
                if pattern.search(note):
                    raise ValueError("runtime evidence notes contain unsanitized sensitive text")
        return values

    def query_names(self) -> set[str]:
        return {query.name for query in self.queries}

    def missing_required_queries(self) -> list[str]:
        present = self.query_names()
        return [name for name in REQUIRED_RUNTIME_QUERY_NAMES if name not in present]

    def failed_required_queries(self) -> list[str]:
        failures = []
        for query in self.queries:
            if query.name in REQUIRED_RUNTIME_QUERY_NAMES:
                if not query.executed or not query.passed:
                    failures.append(query.name)
        return failures

    def missing_or_false_assertions(self) -> list[str]:
        return [
            name
            for name in REQUIRED_RUNTIME_ASSERTIONS
            if self.runtime_assertions.get(name) is not True
        ]

    def to_json_dict(self) -> dict:
        return json.loads(self.model_dump_json())

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_json_dict(), indent=indent)
