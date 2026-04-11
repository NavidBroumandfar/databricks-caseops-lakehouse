"""
src/pipelines/delta_share_handoff.py — C-1: Delta Sharing Producer-Side Preparation Layer.

Implements the producer-side abstraction for the Delta Sharing delivery
mechanism chosen in Phase C-0. This module defines the share configuration,
naming conventions, preparation manifest model, and SQL DDL templates that
a Databricks workspace operator would use to provision the Delta Share.

What this module IS:
  - The repo-side canonical definition of the Delta Share configuration
  - A deterministic, credential-free preparation manifest generator
  - SQL template generation for share setup in Unity Catalog
  - The handoff surface definition for the Gold → Bedrock delivery boundary

What this module IS NOT:
  - An execution layer that calls Unity Catalog or Delta Sharing APIs
  - A real share provisioning script (requires a live Databricks workspace)
  - An SDK client for Bedrock CaseOps consumption
  - Anything that needs production credentials to run

C-0 decision: Delta Sharing is the primary delivery mechanism, augmenting
(not replacing) the V1 file export path. See docs/live-handoff-design.md § 6.

C-1 implementation stance:
  - The preparation manifest is a structured, testable artifact documenting
    exactly what must be executed in Unity Catalog to activate the share
  - Runtime provisioning is a manual step or C-2 notebook execution
  - No credentials, tokens, or workspace URLs appear in this module or its output

Phase: C-1
Design decision: docs/live-handoff-design.md § 6, § 10, § 13
Architecture context: ARCHITECTURE.md § Delivery Mechanism (V2 — C-0)
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

from src.schemas.delivery_event import (
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CATALOG = "caseops"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_GOLD_TABLE = "ai_ready_assets"
DEFAULT_DELIVERY_EVENTS_TABLE = "delivery_events"
DEFAULT_RECIPIENT_NAME = "bedrock_caseops"

SHARE_MANIFEST_VERSION = "v0.1.0"

# Status constants for the preparation manifest.
SHARE_PREP_STATUS_DESIGNED = "designed"
"""
Share configuration is documented in this repo. No provisioning has
been executed in Unity Catalog yet. This is the C-1 status — the
design is ready; execution is pending (C-2 or manual provisioning).
"""

SHARE_PREP_STATUS_PROVISIONED = "provisioned"
"""
Share has been created and the Gold table has been added in Unity Catalog.
Set this status after running the C-1 setup SQL in the workspace.
"""


# ---------------------------------------------------------------------------
# Delta Share configuration
# ---------------------------------------------------------------------------


@dataclass
class DeltaShareConfig:
    """
    Producer-side configuration for the caseops_handoff Delta Share.

    This is the canonical definition of the share that must be created
    in Unity Catalog to activate the Gold → Bedrock delivery channel.

    Fields
    ------
    share_name
        Name of the Delta Share in Unity Catalog. Default: 'caseops_handoff'.
    catalog
        Unity Catalog catalog containing the Gold table. Default: 'caseops'.
    gold_schema
        Schema containing the Gold table. Default: 'gold'.
    gold_table
        Gold Delta table to share. Default: 'ai_ready_assets'.
    shared_as
        Alias name for the table within the share (consumer sees this name).
        Default: 'gold_ai_ready_assets'.
    delivery_events_table
        Delta table recording per-batch delivery events. Default: 'delivery_events'.
    recipient_name
        Name of the Delta Sharing recipient configured in Unity Catalog.
        Default: 'bedrock_caseops'. In a personal workspace, this can be
        a self-referential recipient or an open-sharing recipient.
    schema_version
        Contract version for payloads accessible via this share. 'v0.2.0'
        for C-1+ delivery.
    """

    share_name: str = DEFAULT_SHARE_NAME
    catalog: str = DEFAULT_CATALOG
    gold_schema: str = DEFAULT_GOLD_SCHEMA
    gold_table: str = DEFAULT_GOLD_TABLE
    shared_as: str = DEFAULT_SHARED_OBJECT_NAME
    delivery_events_table: str = DEFAULT_DELIVERY_EVENTS_TABLE
    recipient_name: str = DEFAULT_RECIPIENT_NAME
    schema_version: str = DELIVERY_SCHEMA_VERSION

    @property
    def fully_qualified_gold_table(self) -> str:
        """Fully qualified Gold table name: catalog.schema.table."""
        return f"{self.catalog}.{self.gold_schema}.{self.gold_table}"

    @property
    def fully_qualified_delivery_events_table(self) -> str:
        """Fully qualified delivery_events table name."""
        return f"{self.catalog}.{self.gold_schema}.{self.delivery_events_table}"

    def to_dict(self) -> dict:
        return {
            "share_name": self.share_name,
            "catalog": self.catalog,
            "gold_schema": self.gold_schema,
            "gold_table": self.gold_table,
            "shared_as": self.shared_as,
            "delivery_events_table": self.delivery_events_table,
            "recipient_name": self.recipient_name,
            "schema_version": self.schema_version,
            "fully_qualified_gold_table": self.fully_qualified_gold_table,
            "fully_qualified_delivery_events_table": (
                self.fully_qualified_delivery_events_table
            ),
        }


# ---------------------------------------------------------------------------
# Share preparation manifest
# ---------------------------------------------------------------------------


@dataclass
class SharePreparationManifest:
    """
    Structured producer-side manifest documenting the Delta Share configuration
    that must be provisioned in Unity Catalog to activate the C-1 delivery channel.

    This manifest is the repo-side artifact for the Delta Sharing handoff surface.
    It contains:
      - the share configuration (DeltaShareConfig)
      - the SQL DDL templates needed to create the share in Unity Catalog
      - the handoff surface definition (what is shared, how it is accessed)
      - a status indicating whether the share has been provisioned
      - the delivery events table DDL for creating the notification log

    C-1 status: 'designed' — configuration is complete in this repo; no
    Unity Catalog provisioning has been executed. C-2 validates that the
    provisioned share is functional.

    Fields
    ------
    manifest_version
        Schema version for this manifest structure.
    delivery_mechanism
        Always 'delta_sharing' for C-1+ manifests.
    config
        The DeltaShareConfig dataclass specifying share name, tables, recipient.
    status
        Provisioning status. 'designed' in C-1 until manually provisioned.
    generated_at
        UTC ISO 8601 timestamp when this manifest was computed.
    setup_sql
        SQL DDL string for creating the share and adding the Gold table.
        Can be executed in a Databricks SQL notebook or the Unity Catalog UI.
    delivery_events_ddl
        SQL DDL string for creating the caseops.gold.delivery_events table.
    handoff_surface
        Dict describing the handoff surface: what is shared, where it lives,
        how a Bedrock CaseOps consumer accesses it.
    c2_validation_queries
        SQL queries that C-2 validation should run to confirm delivery.
    notes
        Human-readable notes about the C-1 preparation state.
    """

    manifest_version: str
    delivery_mechanism: str
    config: DeltaShareConfig
    status: str
    generated_at: str
    setup_sql: str
    delivery_events_ddl: str
    handoff_surface: dict = field(default_factory=dict)
    c2_validation_queries: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "manifest_version": self.manifest_version,
            "delivery_mechanism": self.delivery_mechanism,
            "config": self.config.to_dict(),
            "status": self.status,
            "generated_at": self.generated_at,
            "setup_sql": self.setup_sql,
            "delivery_events_ddl": self.delivery_events_ddl,
            "handoff_surface": self.handoff_surface,
            "c2_validation_queries": self.c2_validation_queries,
            "notes": self.notes,
        }

    def to_json_str(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# SQL template generation
# ---------------------------------------------------------------------------


def generate_share_setup_sql(config: DeltaShareConfig) -> str:
    """
    Generate the Unity Catalog SQL DDL for creating the Delta Share.

    This SQL must be run in a Databricks workspace with Unity Catalog enabled
    and appropriate CREATE SHARE / ALTER SHARE privileges.

    No credentials or workspace URLs are embedded. Environment-specific
    values (workspace URL, recipient activation links) are set at runtime.

    This template is for Unity Catalog Delta Sharing (not open sharing).
    For personal workspace open sharing, the RECIPIENT block is optional.
    """
    return f"""-- ============================================================
-- C-1 Delta Share Setup — caseops_handoff
-- ============================================================
-- Run in a Databricks SQL notebook or Unity Catalog SQL editor.
-- Requires: CREATE SHARE privilege on the catalog.
-- Schema version: {config.schema_version}
-- Generated by: src/pipelines/delta_share_handoff.py
-- ============================================================

-- Step 1: Create the Delta Share
CREATE SHARE IF NOT EXISTS {config.share_name}
  COMMENT 'CaseOps Lakehouse Gold table share for Bedrock CaseOps consumption.
Schema version: {config.schema_version}. Delivery mechanism: delta_sharing.
Upstream owner: Databricks CaseOps Lakehouse.
Consumer: Bedrock CaseOps.';

-- Step 2: Add the Gold AI-ready assets table to the share
ALTER SHARE {config.share_name}
  ADD TABLE {config.fully_qualified_gold_table}
  AS {config.shared_as}
  COMMENT 'Gold-tier classified AI-ready assets. Export-ready records only.
Routing labels: regulatory_review (V1), security_ops and incident_management (V2+).
schema_version: {config.schema_version}.';

-- Step 3: Add the delivery_events table to the share (optional — allows
-- Bedrock CaseOps to poll the delivery notification log directly via the share)
ALTER SHARE {config.share_name}
  ADD TABLE {config.fully_qualified_delivery_events_table}
  AS delivery_events
  COMMENT 'Per-batch delivery event log. One row per pipeline run.
Bedrock CaseOps reads this to discover new batches and locate the B-5 manifest.';

-- Step 4: Create a recipient (skip if using open sharing in personal workspace)
CREATE RECIPIENT IF NOT EXISTS {config.recipient_name}
  COMMENT 'Bedrock CaseOps Delta Sharing recipient.
Recipient activation link is delivered out-of-band — not committed to this repo.';

-- Step 5: Grant the recipient access to the share
GRANT SELECT ON SHARE {config.share_name} TO RECIPIENT {config.recipient_name};

-- ============================================================
-- Verification queries (run after setup to confirm)
-- ============================================================

-- Confirm the share exists and shows the expected tables:
-- SHOW ALL IN SHARE {config.share_name};

-- Confirm the recipient can see the share:
-- SHOW GRANTS ON SHARE {config.share_name};
"""


def generate_delivery_events_ddl(config: DeltaShareConfig) -> str:
    """
    Generate the SQL DDL for creating the caseops.gold.delivery_events table.

    This table is the per-batch delivery notification log. Written by this
    repo (producer side) after each successful pipeline run with delivery.
    Read by Bedrock CaseOps to discover new batches.

    The table uses USING DELTA to align with the lakehouse architecture.
    Column comments follow the DeliveryEvent schema in src/schemas/delivery_event.py.
    """
    return f"""-- ============================================================
-- C-1 delivery_events Table DDL
-- ============================================================
-- Run in a Databricks SQL notebook or Unity Catalog SQL editor.
-- Creates the per-batch delivery event notification log.
-- Schema: {config.fully_qualified_delivery_events_table}
-- Generated by: src/pipelines/delta_share_handoff.py
-- ============================================================

CREATE TABLE IF NOT EXISTS {config.fully_qualified_delivery_events_table} (
  delivery_event_id     STRING    NOT NULL  COMMENT 'UUID v4 for this delivery event.',
  pipeline_run_id       STRING    NOT NULL  COMMENT 'Pipeline run ID of the producing batch.',
  batch_id              STRING    NOT NULL  COMMENT 'Stable batch ID. Equal to pipeline_run_id.',
  generated_at          TIMESTAMP NOT NULL  COMMENT 'UTC timestamp when this event was written.',
  delivery_mechanism    STRING    NOT NULL  COMMENT 'Delivery mechanism. Always delta_sharing for C-1+.',
  share_name            STRING              COMMENT 'Delta Share name. Default: caseops_handoff.',
  shared_object_name    STRING              COMMENT 'Shared table name. Default: gold_ai_ready_assets.',
  eligible_record_count INT       NOT NULL  COMMENT 'Total records that entered classification in this batch.',
  exported_record_count INT       NOT NULL  COMMENT 'Records successfully exported as Bedrock payloads.',
  quarantined_record_count INT    NOT NULL  COMMENT 'Records routed to quarantine.',
  contract_blocked_count INT      NOT NULL  COMMENT 'Records that failed B-1 contract validation.',
  routing_labels        ARRAY<STRING>       COMMENT 'Distinct routing labels in exported records.',
  bundle_artifact_path  STRING              COMMENT 'Path to B-5 HandoffBatchManifest JSON artifact.',
  report_artifact_path  STRING              COMMENT 'Path to B-4 HandoffBatchReport JSON artifact.',
  status                STRING    NOT NULL  COMMENT 'Producer-side status: prepared or failed.',
  status_reason         STRING              COMMENT 'Human-readable explanation of status.',
  schema_version        STRING    NOT NULL  COMMENT 'Data contract version. v0.2.0 for C-1+.',
  notes                 STRING              COMMENT 'Optional batch-level notes.'
)
USING DELTA
COMMENT 'Per-batch delivery event log for the Gold → Bedrock CaseOps handoff.
Written by Databricks CaseOps Lakehouse after each pipeline run with delivery enabled.
Read by Bedrock CaseOps to discover new batches and navigate to export payload files.
Schema version: {DELIVERY_SCHEMA_VERSION}. Owner: Databricks CaseOps Lakehouse.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'caseops.schema_version' = '{DELIVERY_SCHEMA_VERSION}',
  'caseops.phase' = 'C-1'
);
"""


# ---------------------------------------------------------------------------
# Handoff surface definition
# ---------------------------------------------------------------------------


def compute_handoff_surface(config: DeltaShareConfig) -> dict:
    """
    Compute the handoff surface definition for this Delta Share configuration.

    The handoff surface is the authoritative description of what is available
    to Bedrock CaseOps via the Delta Share and where each artifact lives.

    This is NOT an API response — it is a documentation artifact generated
    from the DeltaShareConfig that can be embedded in the preparation manifest
    and used for C-2 validation planning.
    """
    return {
        "delivery_mechanism": DELIVERY_MECHANISM_DELTA_SHARING,
        "share_name": config.share_name,
        "shared_tables": [
            {
                "source_table": config.fully_qualified_gold_table,
                "shared_as": config.shared_as,
                "description": (
                    "Gold-tier classified AI-ready assets. "
                    "Filter on export_ready = true to discover Bedrock handoff records. "
                    f"Filter on routing_label to scope to a domain. "
                    f"schema_version: {config.schema_version}."
                ),
                "filter_for_export_ready": "WHERE export_ready = true",
                "filter_example": "WHERE routing_label = 'regulatory_review' AND export_ready = true",
            },
            {
                "source_table": config.fully_qualified_delivery_events_table,
                "shared_as": "delivery_events",
                "description": (
                    "Per-batch delivery notification log. "
                    "One row per pipeline run. "
                    "Contains bundle_artifact_path to locate the B-5 manifest."
                ),
            },
        ],
        "export_payload_files": {
            "path_pattern": "/Volumes/caseops/gold/exports/<routing_label>/<document_id>.json",
            "description": (
                "Individual export payload files (V1 file export path — preserved and augmented in V2). "
                "Located via B-5 HandoffBatchManifest.exported_records[].export_artifact_path."
            ),
        },
        "consumer": config.recipient_name,
        "consumer_access_pattern": (
            "1. Query delivery_events table (via share or direct) for new batches. "
            "2. Open bundle_artifact_path to access the B-5 HandoffBatchManifest. "
            "3. Navigate exported_records in the manifest for per-record export file paths. "
            "4. Fetch each export payload file at the listed export_artifact_path."
        ),
        "schema_version": config.schema_version,
        "v1_file_export_retained": True,
        "v1_file_export_note": (
            "The V1 file export path is preserved. Delta Sharing augments it — "
            "it does not replace file-based export. "
            "Both delivery channels are active in V2-C."
        ),
    }


def compute_c2_validation_queries(config: DeltaShareConfig) -> list[dict]:
    """
    Generate the C-2 validation query targets for this share configuration.

    These are the queries that a C-2 validation notebook or script should
    run to confirm the Delta Share is functional end-to-end.

    Returns a list of named query descriptors — not live SQL results.
    """
    return [
        {
            "name": "confirm_share_exists",
            "description": "Confirm the share exists in Unity Catalog.",
            "sql": f"SHOW ALL IN SHARE {config.share_name};",
            "expected": f"Table '{config.shared_as}' visible in share.",
        },
        {
            "name": "query_export_ready_records",
            "description": (
                "Query the shared Gold table for export-ready records "
                "from a V2 batch."
            ),
            "sql": (
                f"SELECT document_id, routing_label, schema_version "
                f"FROM {config.share_name}.{config.shared_as} "
                f"WHERE export_ready = true "
                f"AND schema_version = '{config.schema_version}' "
                f"LIMIT 10;"
            ),
            "expected": "Rows present with schema_version = 'v0.2.0'.",
        },
        {
            "name": "query_delivery_events",
            "description": "Confirm at least one delivery event row is present.",
            "sql": (
                f"SELECT delivery_event_id, batch_id, status, exported_record_count "
                f"FROM {config.fully_qualified_delivery_events_table} "
                f"ORDER BY generated_at DESC LIMIT 5;"
            ),
            "expected": "Rows present with status = 'prepared'.",
        },
        {
            "name": "verify_routing_label_transparency",
            "description": "Confirm routing labels are visible in the shared table.",
            "sql": (
                f"SELECT DISTINCT routing_label "
                f"FROM {config.share_name}.{config.shared_as} "
                f"WHERE export_ready = true;"
            ),
            "expected": "At least 'regulatory_review' visible (V1 FDA domain).",
        },
    ]


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------


def compute_share_preparation_manifest(
    config: Optional[DeltaShareConfig] = None,
    pipeline_run_id: Optional[str] = None,
    delivery_event_id: Optional[str] = None,
) -> SharePreparationManifest:
    """
    Compute the producer-side Delta Share preparation manifest.

    This is a credential-free, locally executable function that documents
    the full Delta Share setup required for the C-1 delivery channel.

    The manifest can be written to disk as a reference artifact and
    used as input for C-2 validation planning.

    Parameters
    ----------
    config
        DeltaShareConfig. If not provided, default values are used.
    pipeline_run_id
        Optional pipeline run ID to include in manifest notes.
    delivery_event_id
        Optional delivery event ID to cross-reference in manifest notes.
    """
    cfg = config or DeltaShareConfig()
    generated_at = datetime.now(tz=timezone.utc).isoformat()

    handoff_surface = compute_handoff_surface(cfg)
    c2_queries = compute_c2_validation_queries(cfg)
    setup_sql = generate_share_setup_sql(cfg)
    delivery_events_ddl = generate_delivery_events_ddl(cfg)

    notes = [
        f"C-1 status: '{SHARE_PREP_STATUS_DESIGNED}'. "
        "Share configuration is documented in this repo. "
        "No Unity Catalog provisioning has been executed.",
        "To activate: run setup_sql in a Databricks SQL notebook with CREATE SHARE privileges.",
        f"Gold table to share: {cfg.fully_qualified_gold_table} → shared as '{cfg.shared_as}'.",
        f"Delivery events table: {cfg.fully_qualified_delivery_events_table}.",
        "Runtime validation (C-2) confirms the share is queryable and delivery events are visible.",
        "No production credentials, tokens, or workspace URLs are embedded in this manifest.",
    ]
    if pipeline_run_id:
        notes.append(f"Associated pipeline run: {pipeline_run_id}.")
    if delivery_event_id:
        notes.append(f"Associated delivery event: {delivery_event_id}.")

    return SharePreparationManifest(
        manifest_version=SHARE_MANIFEST_VERSION,
        delivery_mechanism=DELIVERY_MECHANISM_DELTA_SHARING,
        config=cfg,
        status=SHARE_PREP_STATUS_DESIGNED,
        generated_at=generated_at,
        setup_sql=setup_sql,
        delivery_events_ddl=delivery_events_ddl,
        handoff_surface=handoff_surface,
        c2_validation_queries=c2_queries,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------


def compute_share_manifest_path(output_dir: Path) -> Path:
    """Deterministic path for the share preparation manifest JSON artifact."""
    return output_dir / "delta_share_preparation_manifest.json"


def write_share_manifest(
    manifest: SharePreparationManifest,
    output_dir: Path,
) -> Path:
    """
    Write the share preparation manifest as a JSON artifact.

    Creates the directory if it does not exist.
    Returns the path to the written JSON artifact.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = compute_share_manifest_path(output_dir)
    manifest_path.write_text(manifest.to_json_str(), encoding="utf-8")
    return manifest_path


def format_share_manifest_text(manifest: SharePreparationManifest) -> str:
    """
    Format a share preparation manifest as a human-readable text summary.
    """
    cfg = manifest.config
    lines = [
        "=" * 70,
        "DELTA SHARE PREPARATION MANIFEST — DATABRICKS CASEOPS LAKEHOUSE (C-1)",
        "=" * 70,
        f"Manifest Version   : {manifest.manifest_version}",
        f"Generated At       : {manifest.generated_at}",
        f"Status             : {manifest.status}",
        f"Delivery Mechanism : {manifest.delivery_mechanism}",
        "",
        "SHARE CONFIGURATION",
        "-" * 40,
        f"Share Name         : {cfg.share_name}",
        f"Catalog            : {cfg.catalog}",
        f"Gold Table         : {cfg.fully_qualified_gold_table}",
        f"Shared As          : {cfg.shared_as}",
        f"Delivery Events    : {cfg.fully_qualified_delivery_events_table}",
        f"Recipient          : {cfg.recipient_name}",
        f"Schema Version     : {cfg.schema_version}",
        "",
        "HANDOFF SURFACE",
        "-" * 40,
        f"Consumer           : {manifest.handoff_surface.get('consumer', '(not set)')}",
        "V1 File Export     : retained and augmented (not replaced)",
        f"Export Path Pattern: /Volumes/caseops/gold/exports/<routing_label>/<document_id>.json",
        "",
        "C-1 STATUS",
        "-" * 40,
    ]
    for note in manifest.notes:
        lines.append(f"  • {note}")
    lines += [
        "",
        "C-2 VALIDATION QUERIES",
        "-" * 40,
    ]
    for q in manifest.c2_validation_queries:
        lines.append(f"  [{q['name']}] {q['description']}")
    lines += [
        "",
        "=" * 70,
        "To provision: copy setup_sql from the JSON manifest and run in Databricks SQL.",
        "To validate: run c2_validation_queries in a Databricks SQL notebook (Phase C-2).",
        "=" * 70,
    ]
    return "\n".join(lines)
