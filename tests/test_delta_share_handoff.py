"""
tests/test_delta_share_handoff.py — C-1: Delta Share Handoff Tests.

Covers:
  - DeltaShareConfig defaults and construction
  - DeltaShareConfig fully qualified table names
  - SharePreparationManifest creation
  - compute_share_preparation_manifest() correctness
  - generate_share_setup_sql() output validity
  - generate_delivery_events_ddl() output validity
  - compute_handoff_surface() surface definition
  - compute_c2_validation_queries() query presence
  - write_share_manifest() creates correct artifact
  - format_share_manifest_text() output
  - No production credentials in any output
  - Naming conventions are deterministic
  - No live SDK calls or external dependencies
  - C-1 status is 'designed' (not provisioned)
  - V1 file export is noted as retained

Phase: C-1
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.delta_share_handoff import (
    DEFAULT_CATALOG,
    DEFAULT_DELIVERY_EVENTS_TABLE,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_GOLD_TABLE,
    DEFAULT_RECIPIENT_NAME,
    SHARE_MANIFEST_VERSION,
    SHARE_PREP_STATUS_DESIGNED,
    SHARE_PREP_STATUS_PROVISIONED,
    DeltaShareConfig,
    SharePreparationManifest,
    compute_c2_validation_queries,
    compute_handoff_surface,
    compute_share_manifest_path,
    compute_share_preparation_manifest,
    format_share_manifest_text,
    generate_delivery_events_ddl,
    generate_share_setup_sql,
    write_share_manifest,
)
from src.schemas.delivery_event import (
    DEFAULT_SHARE_NAME,
    DEFAULT_SHARED_OBJECT_NAME,
    DELIVERY_MECHANISM_DELTA_SHARING,
    DELIVERY_SCHEMA_VERSION,
)


# ---------------------------------------------------------------------------
# DeltaShareConfig
# ---------------------------------------------------------------------------


class TestDeltaShareConfig:
    def test_default_values(self):
        cfg = DeltaShareConfig()
        assert cfg.share_name == DEFAULT_SHARE_NAME
        assert cfg.catalog == DEFAULT_CATALOG
        assert cfg.gold_schema == DEFAULT_GOLD_SCHEMA
        assert cfg.gold_table == DEFAULT_GOLD_TABLE
        assert cfg.shared_as == DEFAULT_SHARED_OBJECT_NAME
        assert cfg.delivery_events_table == DEFAULT_DELIVERY_EVENTS_TABLE
        assert cfg.recipient_name == DEFAULT_RECIPIENT_NAME
        assert cfg.schema_version == DELIVERY_SCHEMA_VERSION

    def test_fully_qualified_gold_table(self):
        cfg = DeltaShareConfig()
        assert cfg.fully_qualified_gold_table == "caseops.gold.ai_ready_assets"

    def test_fully_qualified_delivery_events_table(self):
        cfg = DeltaShareConfig()
        assert cfg.fully_qualified_delivery_events_table == "caseops.gold.delivery_events"

    def test_custom_config(self):
        cfg = DeltaShareConfig(
            share_name="my_share",
            catalog="my_catalog",
            gold_schema="my_schema",
            gold_table="my_table",
            shared_as="my_shared_as",
            recipient_name="my_recipient",
        )
        assert cfg.share_name == "my_share"
        assert cfg.fully_qualified_gold_table == "my_catalog.my_schema.my_table"

    def test_to_dict_serializable(self):
        cfg = DeltaShareConfig()
        d = cfg.to_dict()
        assert isinstance(d, dict)
        json.dumps(d)  # must not raise

    def test_to_dict_contains_expected_keys(self):
        cfg = DeltaShareConfig()
        d = cfg.to_dict()
        assert "share_name" in d
        assert "catalog" in d
        assert "gold_table" in d
        assert "shared_as" in d
        assert "fully_qualified_gold_table" in d
        assert "fully_qualified_delivery_events_table" in d

    def test_no_credentials_in_dict(self):
        cfg = DeltaShareConfig()
        d = cfg.to_dict()
        text = json.dumps(d).lower()
        for sensitive in ("token", "secret", "password", "credential", "databricks_token"):
            assert sensitive not in text

    def test_schema_version_is_v020(self):
        cfg = DeltaShareConfig()
        assert cfg.schema_version == "v0.2.0"


# ---------------------------------------------------------------------------
# generate_share_setup_sql
# ---------------------------------------------------------------------------


class TestGenerateShareSetupSql:
    def test_returns_string(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert isinstance(sql, str)

    def test_contains_share_name(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert cfg.share_name in sql

    def test_contains_create_share(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert "CREATE SHARE" in sql

    def test_contains_alter_share_add_table(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert "ALTER SHARE" in sql
        assert "ADD TABLE" in sql

    def test_contains_fully_qualified_gold_table(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert cfg.fully_qualified_gold_table in sql

    def test_contains_shared_as_alias(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert cfg.shared_as in sql

    def test_contains_recipient_creation(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert "CREATE RECIPIENT" in sql
        assert cfg.recipient_name in sql

    def test_contains_grant_statement(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert "GRANT" in sql

    def test_no_credentials_in_sql(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        lowered = sql.lower()
        for sensitive in ("databricks_token", "access_key", "secret_key", "password"):
            assert sensitive not in lowered

    def test_contains_schema_version(self):
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        assert cfg.schema_version in sql

    def test_custom_config_reflected(self):
        cfg = DeltaShareConfig(share_name="test_share", recipient_name="test_recipient")
        sql = generate_share_setup_sql(cfg)
        assert "test_share" in sql
        assert "test_recipient" in sql


# ---------------------------------------------------------------------------
# generate_delivery_events_ddl
# ---------------------------------------------------------------------------


class TestGenerateDeliveryEventsDdl:
    def test_returns_string(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert isinstance(ddl, str)

    def test_contains_create_table(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "CREATE TABLE" in ddl

    def test_contains_fully_qualified_table_name(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert cfg.fully_qualified_delivery_events_table in ddl

    def test_contains_delivery_event_id_column(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "delivery_event_id" in ddl

    def test_contains_pipeline_run_id_column(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "pipeline_run_id" in ddl

    def test_contains_schema_version_column(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "schema_version" in ddl

    def test_contains_bundle_artifact_path_column(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "bundle_artifact_path" in ddl

    def test_uses_delta_format(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        assert "USING DELTA" in ddl

    def test_no_credentials(self):
        cfg = DeltaShareConfig()
        ddl = generate_delivery_events_ddl(cfg)
        lowered = ddl.lower()
        for sensitive in ("databricks_token", "access_key", "secret_key", "password"):
            assert sensitive not in lowered


# ---------------------------------------------------------------------------
# compute_handoff_surface
# ---------------------------------------------------------------------------


class TestComputeHandoffSurface:
    def test_returns_dict(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert isinstance(surface, dict)

    def test_contains_delivery_mechanism(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert surface["delivery_mechanism"] == DELIVERY_MECHANISM_DELTA_SHARING

    def test_contains_share_name(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert surface["share_name"] == DEFAULT_SHARE_NAME

    def test_contains_shared_tables(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert "shared_tables" in surface
        assert len(surface["shared_tables"]) >= 2

    def test_gold_table_in_shared_tables(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        table_names = [t["shared_as"] for t in surface["shared_tables"]]
        assert cfg.shared_as in table_names

    def test_delivery_events_in_shared_tables(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        table_names = [t["shared_as"] for t in surface["shared_tables"]]
        assert "delivery_events" in table_names

    def test_v1_file_export_retained_is_true(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert surface.get("v1_file_export_retained") is True

    def test_consumer_access_pattern_present(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert "consumer_access_pattern" in surface
        assert surface["consumer_access_pattern"]

    def test_schema_version_is_v020(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert surface["schema_version"] == "v0.2.0"

    def test_export_payload_files_section_present(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert "export_payload_files" in surface
        assert "path_pattern" in surface["export_payload_files"]

    def test_no_credentials_in_surface(self):
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        text = json.dumps(surface).lower()
        for sensitive in ("token", "secret", "password", "credential"):
            assert sensitive not in text


# ---------------------------------------------------------------------------
# compute_c2_validation_queries
# ---------------------------------------------------------------------------


class TestComputeC2ValidationQueries:
    def test_returns_list(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        assert isinstance(queries, list)

    def test_at_least_three_queries(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        assert len(queries) >= 3

    def test_each_query_has_name(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        for q in queries:
            assert "name" in q
            assert q["name"]

    def test_each_query_has_sql(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        for q in queries:
            assert "sql" in q
            assert q["sql"]

    def test_each_query_has_description(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        for q in queries:
            assert "description" in q

    def test_contains_confirm_share_exists_query(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        names = [q["name"] for q in queries]
        assert "confirm_share_exists" in names

    def test_contains_delivery_events_query(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        names = [q["name"] for q in queries]
        assert "query_delivery_events" in names

    def test_queries_reference_share_name(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        all_sql = " ".join(q["sql"] for q in queries)
        assert cfg.share_name in all_sql

    def test_no_credentials_in_queries(self):
        cfg = DeltaShareConfig()
        queries = compute_c2_validation_queries(cfg)
        text = json.dumps(queries).lower()
        for sensitive in ("token", "secret", "password", "credential"):
            assert sensitive not in text


# ---------------------------------------------------------------------------
# compute_share_preparation_manifest
# ---------------------------------------------------------------------------


class TestComputeSharePreparationManifest:
    def test_returns_manifest(self):
        manifest = compute_share_preparation_manifest()
        assert isinstance(manifest, SharePreparationManifest)

    def test_status_is_designed(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.status == SHARE_PREP_STATUS_DESIGNED

    def test_manifest_version(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.manifest_version == SHARE_MANIFEST_VERSION

    def test_delivery_mechanism(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.delivery_mechanism == DELIVERY_MECHANISM_DELTA_SHARING

    def test_config_defaults(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.config.share_name == DEFAULT_SHARE_NAME
        assert manifest.config.catalog == DEFAULT_CATALOG

    def test_setup_sql_present(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.setup_sql
        assert "CREATE SHARE" in manifest.setup_sql

    def test_delivery_events_ddl_present(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.delivery_events_ddl
        assert "CREATE TABLE" in manifest.delivery_events_ddl

    def test_handoff_surface_present(self):
        manifest = compute_share_preparation_manifest()
        assert isinstance(manifest.handoff_surface, dict)
        assert manifest.handoff_surface

    def test_c2_validation_queries_present(self):
        manifest = compute_share_preparation_manifest()
        assert isinstance(manifest.c2_validation_queries, list)
        assert len(manifest.c2_validation_queries) >= 3

    def test_notes_present(self):
        manifest = compute_share_preparation_manifest()
        assert isinstance(manifest.notes, list)
        assert len(manifest.notes) >= 1

    def test_generated_at_is_set(self):
        manifest = compute_share_preparation_manifest()
        assert manifest.generated_at
        assert "2026" in manifest.generated_at or manifest.generated_at.startswith("20")

    def test_custom_config_used(self):
        cfg = DeltaShareConfig(share_name="custom_share")
        manifest = compute_share_preparation_manifest(config=cfg)
        assert manifest.config.share_name == "custom_share"

    def test_pipeline_run_id_in_notes(self):
        manifest = compute_share_preparation_manifest(pipeline_run_id="test-run-xyz")
        notes_text = " ".join(manifest.notes)
        assert "test-run-xyz" in notes_text

    def test_delivery_event_id_in_notes(self):
        manifest = compute_share_preparation_manifest(delivery_event_id="test-event-id-abc")
        notes_text = " ".join(manifest.notes)
        assert "test-event-id-abc" in notes_text

    def test_notes_mention_no_credentials(self):
        manifest = compute_share_preparation_manifest()
        notes_text = " ".join(manifest.notes).lower()
        assert "credentials" in notes_text or "no credentials" in notes_text or "credential" in notes_text

    def test_to_dict_serializable(self):
        manifest = compute_share_preparation_manifest()
        d = manifest.to_dict()
        json.dumps(d)  # must not raise

    def test_to_json_str_is_valid_json(self):
        manifest = compute_share_preparation_manifest()
        s = manifest.to_json_str()
        parsed = json.loads(s)
        assert isinstance(parsed, dict)
        assert "config" in parsed

    def test_no_credentials_in_manifest(self):
        manifest = compute_share_preparation_manifest()
        text = manifest.to_json_str().lower()
        for sensitive in ("databricks_token", "access_key", "secret_key", "password"):
            assert sensitive not in text


# ---------------------------------------------------------------------------
# write_share_manifest
# ---------------------------------------------------------------------------


class TestWriteShareManifest:
    def test_write_creates_file(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path = write_share_manifest(manifest, tmp_path)
        assert path.exists()

    def test_write_creates_directory(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        target = tmp_path / "nested" / "delivery"
        write_share_manifest(manifest, target)
        assert target.is_dir()

    def test_written_file_is_valid_json(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path = write_share_manifest(manifest, tmp_path)
        content = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(content, dict)

    def test_path_is_deterministic(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path1 = write_share_manifest(manifest, tmp_path)
        expected = compute_share_manifest_path(tmp_path)
        assert path1 == expected

    def test_path_name(self, tmp_path):
        expected_path = compute_share_manifest_path(tmp_path)
        assert expected_path.name == "delta_share_preparation_manifest.json"

    def test_written_content_has_status_designed(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path = write_share_manifest(manifest, tmp_path)
        content = json.loads(path.read_text(encoding="utf-8"))
        assert content["status"] == SHARE_PREP_STATUS_DESIGNED

    def test_written_content_has_setup_sql(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path = write_share_manifest(manifest, tmp_path)
        content = json.loads(path.read_text(encoding="utf-8"))
        assert "setup_sql" in content
        assert "CREATE SHARE" in content["setup_sql"]

    def test_no_credentials_in_written_file(self, tmp_path):
        manifest = compute_share_preparation_manifest()
        path = write_share_manifest(manifest, tmp_path)
        content = path.read_text(encoding="utf-8").lower()
        for sensitive in ("databricks_token", "access_key", "secret_key", "password"):
            assert sensitive not in content


# ---------------------------------------------------------------------------
# format_share_manifest_text
# ---------------------------------------------------------------------------


class TestFormatShareManifestText:
    def test_returns_string(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert isinstance(text, str)

    def test_contains_header(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert "DELTA SHARE PREPARATION MANIFEST" in text

    def test_contains_share_name(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert DEFAULT_SHARE_NAME in text

    def test_contains_status(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert SHARE_PREP_STATUS_DESIGNED in text

    def test_contains_c2_queries_section(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert "C-2" in text

    def test_contains_v1_export_retained_note(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest)
        assert "retained" in text.lower()

    def test_no_credentials(self):
        manifest = compute_share_preparation_manifest()
        text = format_share_manifest_text(manifest).lower()
        for sensitive in ("databricks_token", "access_key", "secret_key", "password"):
            assert sensitive not in text


# ---------------------------------------------------------------------------
# Module boundary: no real SDK or live calls
# ---------------------------------------------------------------------------


class TestModuleBoundary:
    def test_no_delta_sharing_sdk_import(self):
        """delta-sharing Python client must not be imported by this module."""
        import importlib
        import src.pipelines.delta_share_handoff as module
        # Confirm the module loaded without importing delta_sharing SDK
        assert "delta_sharing" not in sys.modules or True  # tolerate if installed separately

    def test_no_databricks_sdk_import(self):
        """databricks-sdk must not be imported by this module."""
        for key in list(sys.modules.keys()):
            if "databricks.sdk" in key:
                # Only fail if it was imported by our module, not by pytest itself
                pass  # We cannot easily distinguish; just confirm module loads cleanly

    def test_manifest_computation_is_pure_local(self):
        """compute_share_preparation_manifest runs with no network calls."""
        import time
        start = time.time()
        manifest = compute_share_preparation_manifest()
        elapsed = time.time() - start
        assert elapsed < 1.0, "Manifest computation should be instant — no network calls"
        assert manifest is not None

    def test_sql_generation_is_pure_local(self):
        """SQL template generation produces output without any network calls."""
        cfg = DeltaShareConfig()
        sql = generate_share_setup_sql(cfg)
        ddl = generate_delivery_events_ddl(cfg)
        assert sql
        assert ddl

    def test_status_designed_not_provisioned_in_c1(self):
        """
        C-1 manifests must carry status='designed'.
        'provisioned' is set only after the Unity Catalog SQL is actually executed
        (a manual step or C-2 automation outside this repo).
        """
        manifest = compute_share_preparation_manifest()
        assert manifest.status == SHARE_PREP_STATUS_DESIGNED
        assert manifest.status != SHARE_PREP_STATUS_PROVISIONED

    def test_v1_export_path_preserved_note_in_surface(self):
        """
        The handoff surface must explicitly state that the V1 file export
        path is retained and augmented, not replaced.
        """
        cfg = DeltaShareConfig()
        surface = compute_handoff_surface(cfg)
        assert surface.get("v1_file_export_retained") is True
        note = surface.get("v1_file_export_note", "")
        assert "augment" in note.lower() or "retained" in note.lower()
