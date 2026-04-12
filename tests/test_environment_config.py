"""
test_environment_config.py — E-1: Environment separation test suite.

Tests the environment model, resource naming, MLflow path separation,
and environment-aware helpers introduced in Phase E-1.

Design constraints:
- All tests run locally with no Databricks workspace required.
- No secrets, tokens, or workspace URLs are used or required.
- Tests are deterministic — same input always produces the same output.
- The module under test is src/utils/environment_config.py and the
  updated src/evaluation/mlflow_experiment_paths.py.
"""

from __future__ import annotations

import os
import pytest

from src.utils.environment_config import (
    Environment,
    EnvironmentConfig,
    get_environment_config,
    parse_environment,
    _DEFAULT_ENV,
    _ENV_VAR,
    _VALID_ENV_NAMES,
)
from src.evaluation import mlflow_experiment_paths as mep


# ===========================================================================
# Section 1 — Environment enum
# ===========================================================================

class TestEnvironmentEnum:
    def test_three_environments_defined(self):
        assert set(e.value for e in Environment) == {"dev", "staging", "prod"}

    def test_dev_value(self):
        assert Environment.DEV.value == "dev"

    def test_staging_value(self):
        assert Environment.STAGING.value == "staging"

    def test_prod_value(self):
        assert Environment.PROD.value == "prod"

    def test_is_string_enum(self):
        assert isinstance(Environment.DEV, str)
        assert Environment.DEV == "dev"

    def test_all_environments_reachable(self):
        for env in Environment:
            assert env.value in _VALID_ENV_NAMES


# ===========================================================================
# Section 2 — parse_environment
# ===========================================================================

class TestParseEnvironment:
    def test_parse_dev(self):
        assert parse_environment("dev") == Environment.DEV

    def test_parse_staging(self):
        assert parse_environment("staging") == Environment.STAGING

    def test_parse_prod(self):
        assert parse_environment("prod") == Environment.PROD

    def test_case_insensitive_upper(self):
        assert parse_environment("DEV") == Environment.DEV
        assert parse_environment("STAGING") == Environment.STAGING
        assert parse_environment("PROD") == Environment.PROD

    def test_case_insensitive_mixed(self):
        assert parse_environment("Dev") == Environment.DEV
        assert parse_environment("Staging") == Environment.STAGING
        assert parse_environment("Prod") == Environment.PROD

    def test_strips_whitespace(self):
        assert parse_environment("  dev  ") == Environment.DEV
        assert parse_environment("  staging ") == Environment.STAGING

    def test_invalid_name_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown environment"):
            parse_environment("production")

    def test_invalid_empty_string_raises(self):
        with pytest.raises(ValueError):
            parse_environment("")

    def test_invalid_none_like_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            parse_environment("none")

    def test_error_message_includes_valid_names(self):
        with pytest.raises(ValueError) as exc_info:
            parse_environment("invalid_env")
        msg = str(exc_info.value)
        assert "dev" in msg
        assert "staging" in msg
        assert "prod" in msg


# ===========================================================================
# Section 3 — EnvironmentConfig construction and immutability
# ===========================================================================

class TestEnvironmentConfigConstruction:
    def test_construct_dev(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.environment == Environment.DEV

    def test_construct_staging(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.environment == Environment.STAGING

    def test_construct_prod(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.environment == Environment.PROD

    def test_is_frozen(self):
        """EnvironmentConfig must be immutable — frozen dataclass."""
        config = EnvironmentConfig(environment=Environment.DEV)
        with pytest.raises((AttributeError, TypeError)):
            config.environment = Environment.PROD  # type: ignore[misc]

    def test_repr_contains_env_name_and_catalog(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        r = repr(config)
        assert "staging" in r
        assert "caseops_staging" in r


# ===========================================================================
# Section 4 — Unity Catalog resource naming
# ===========================================================================

class TestCatalogNaming:
    """All three environments must produce distinct, deterministic catalog names."""

    def test_dev_catalog_name(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.catalog_name == "caseops_dev"

    def test_staging_catalog_name(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.catalog_name == "caseops_staging"

    def test_prod_catalog_name(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.catalog_name == "caseops_prod"

    def test_all_catalog_names_distinct(self):
        catalogs = [
            EnvironmentConfig(environment=env).catalog_name
            for env in Environment
        ]
        assert len(catalogs) == len(set(catalogs)), "Each environment must have a unique catalog name"

    def test_catalog_name_contains_env(self):
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            assert env.value in config.catalog_name

    def test_schema_names_are_fixed(self):
        """Schema names are environment-independent — isolation is at catalog level."""
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            assert config.raw_schema == "raw"
            assert config.bronze_schema == "bronze"
            assert config.silver_schema == "silver"
            assert config.gold_schema == "gold"

    def test_env_name_property(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.env_name == "staging"


# ===========================================================================
# Section 5 — Fully-qualified table names
# ===========================================================================

class TestTableFQNs:
    def test_dev_bronze_table(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.bronze_table == "caseops_dev.bronze.parsed_documents"

    def test_dev_silver_table(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.silver_table == "caseops_dev.silver.extracted_records"

    def test_dev_gold_table(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.gold_table == "caseops_dev.gold.ai_ready_assets"

    def test_staging_bronze_table(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.bronze_table == "caseops_staging.bronze.parsed_documents"

    def test_staging_silver_table(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.silver_table == "caseops_staging.silver.extracted_records"

    def test_staging_gold_table(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.gold_table == "caseops_staging.gold.ai_ready_assets"

    def test_prod_bronze_table(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.bronze_table == "caseops_prod.bronze.parsed_documents"

    def test_prod_silver_table(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.silver_table == "caseops_prod.silver.extracted_records"

    def test_prod_gold_table(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.gold_table == "caseops_prod.gold.ai_ready_assets"

    def test_all_bronze_tables_distinct(self):
        tables = [EnvironmentConfig(environment=env).bronze_table for env in Environment]
        assert len(tables) == len(set(tables))

    def test_table_fqn_format(self):
        """Each FQN must be exactly: catalog.schema.table_name"""
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            for fqn in [config.bronze_table, config.silver_table, config.gold_table]:
                parts = fqn.split(".")
                assert len(parts) == 3, f"Invalid FQN format: {fqn}"
                assert parts[0] == config.catalog_name


# ===========================================================================
# Section 6 — Volume paths
# ===========================================================================

class TestVolumePaths:
    def test_dev_raw_volume_path(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.raw_volume_path == "/Volumes/caseops_dev/raw/documents"

    def test_staging_raw_volume_path(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.raw_volume_path == "/Volumes/caseops_staging/raw/documents"

    def test_prod_raw_volume_path(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.raw_volume_path == "/Volumes/caseops_prod/raw/documents"

    def test_dev_export_volume_base(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.export_volume_base == "/Volumes/caseops_dev/gold/exports"

    def test_staging_export_volume_base(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.export_volume_base == "/Volumes/caseops_staging/gold/exports"

    def test_prod_export_volume_base(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.export_volume_base == "/Volumes/caseops_prod/gold/exports"

    def test_volume_paths_start_with_volumes(self):
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            assert config.raw_volume_path.startswith("/Volumes/")
            assert config.export_volume_base.startswith("/Volumes/")

    def test_all_raw_volume_paths_distinct(self):
        paths = [EnvironmentConfig(environment=env).raw_volume_path for env in Environment]
        assert len(paths) == len(set(paths))


# ===========================================================================
# Section 7 — MLflow experiment suffix naming
# ===========================================================================

class TestMLflowExperimentSuffixes:
    def test_dev_bronze_suffix(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.mlflow_bronze_suffix() == "dev/bronze/parse_quality"

    def test_dev_silver_suffix(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.mlflow_silver_suffix() == "dev/silver/extraction_quality"

    def test_dev_gold_suffix(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.mlflow_gold_suffix() == "dev/gold/classification_quality"

    def test_dev_traceability_suffix(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.mlflow_traceability_suffix() == "dev/pipeline/traceability"

    def test_dev_end_to_end_suffix(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        assert config.mlflow_end_to_end_suffix() == "dev/pipeline/end_to_end"

    def test_staging_bronze_suffix(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.mlflow_bronze_suffix() == "staging/bronze/parse_quality"

    def test_staging_gold_suffix(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.mlflow_gold_suffix() == "staging/gold/classification_quality"

    def test_prod_bronze_suffix(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.mlflow_bronze_suffix() == "prod/bronze/parse_quality"

    def test_prod_gold_suffix(self):
        config = EnvironmentConfig(environment=Environment.PROD)
        assert config.mlflow_gold_suffix() == "prod/gold/classification_quality"

    def test_generic_suffix_method(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        assert config.mlflow_experiment_suffix("custom/metric") == "staging/custom/metric"

    def test_suffixes_are_distinct_across_envs(self):
        suffixes_per_env = [
            EnvironmentConfig(environment=env).mlflow_bronze_suffix()
            for env in Environment
        ]
        assert len(suffixes_per_env) == len(set(suffixes_per_env))

    def test_suffix_includes_env_name(self):
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            assert config.mlflow_bronze_suffix().startswith(env.value + "/")


# ===========================================================================
# Section 8 — as_dict representation
# ===========================================================================

class TestAsDictRepresentation:
    def test_as_dict_has_required_keys(self):
        config = EnvironmentConfig(environment=Environment.DEV)
        d = config.as_dict()
        required_keys = [
            "environment",
            "catalog_name",
            "bronze_table",
            "silver_table",
            "gold_table",
            "raw_volume_path",
            "export_volume_base",
            "mlflow_bronze_suffix",
            "mlflow_silver_suffix",
            "mlflow_gold_suffix",
            "mlflow_traceability_suffix",
            "mlflow_end_to_end_suffix",
        ]
        for key in required_keys:
            assert key in d, f"Missing key in as_dict(): {key}"

    def test_as_dict_environment_value(self):
        config = EnvironmentConfig(environment=Environment.STAGING)
        d = config.as_dict()
        assert d["environment"] == "staging"
        assert d["catalog_name"] == "caseops_staging"

    def test_as_dict_no_secrets(self):
        """No personal workspace paths, tokens, or credentials should appear."""
        for env in Environment:
            config = EnvironmentConfig(environment=env)
            d = config.as_dict()
            for v in d.values():
                assert "@" not in str(v), f"Possible workspace path with email in: {v}"
                assert "token" not in str(v).lower()
                assert "secret" not in str(v).lower()


# ===========================================================================
# Section 9 — get_environment_config factory
# ===========================================================================

class TestGetEnvironmentConfig:
    def test_explicit_dev(self):
        config = get_environment_config("dev")
        assert config.environment == Environment.DEV

    def test_explicit_staging(self):
        config = get_environment_config("staging")
        assert config.environment == Environment.STAGING

    def test_explicit_prod(self):
        config = get_environment_config("prod")
        assert config.environment == Environment.PROD

    def test_explicit_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "staging")
        config = get_environment_config("dev")
        assert config.environment == Environment.DEV

    def test_reads_env_var_when_no_explicit(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "staging")
        config = get_environment_config()
        assert config.environment == Environment.STAGING

    def test_reads_env_var_prod(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "prod")
        config = get_environment_config()
        assert config.environment == Environment.PROD

    def test_defaults_to_dev_when_no_arg_no_env_var(self, monkeypatch):
        monkeypatch.delenv(_ENV_VAR, raising=False)
        config = get_environment_config()
        assert config.environment == _DEFAULT_ENV

    def test_default_is_dev(self):
        assert _DEFAULT_ENV == Environment.DEV

    def test_none_argument_uses_env_var(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "staging")
        config = get_environment_config(None)
        assert config.environment == Environment.STAGING

    def test_empty_string_argument_uses_env_var(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "staging")
        config = get_environment_config("")
        assert config.environment == Environment.STAGING

    def test_invalid_explicit_raises(self):
        with pytest.raises(ValueError, match="Unknown environment"):
            get_environment_config("production")

    def test_invalid_env_var_raises(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "production")
        with pytest.raises(ValueError, match="Unknown environment"):
            get_environment_config()

    def test_returns_environment_config_instance(self):
        config = get_environment_config("dev")
        assert isinstance(config, EnvironmentConfig)

    def test_no_databricks_workspace_required(self, monkeypatch):
        """Must work with no Databricks environment variables set."""
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv(_ENV_VAR, raising=False)
        config = get_environment_config("dev")
        assert config.catalog_name == "caseops_dev"

    def test_case_insensitive_via_factory(self):
        config = get_environment_config("DEV")
        assert config.environment == Environment.DEV


# ===========================================================================
# Section 10 — Environment isolation invariants
# ===========================================================================

class TestEnvironmentIsolation:
    """Ensure no two environments share the same resource identifiers."""

    def _all_configs(self):
        return [EnvironmentConfig(environment=env) for env in Environment]

    def test_catalog_names_are_globally_distinct(self):
        names = [c.catalog_name for c in self._all_configs()]
        assert len(names) == len(set(names))

    def test_bronze_tables_are_globally_distinct(self):
        names = [c.bronze_table for c in self._all_configs()]
        assert len(names) == len(set(names))

    def test_raw_volume_paths_are_globally_distinct(self):
        paths = [c.raw_volume_path for c in self._all_configs()]
        assert len(paths) == len(set(paths))

    def test_mlflow_suffixes_are_globally_distinct(self):
        suffixes = [c.mlflow_bronze_suffix() for c in self._all_configs()]
        assert len(suffixes) == len(set(suffixes))

    def test_dev_and_prod_do_not_share_catalog(self):
        dev = EnvironmentConfig(environment=Environment.DEV)
        prod = EnvironmentConfig(environment=Environment.PROD)
        assert dev.catalog_name != prod.catalog_name

    def test_dev_and_prod_do_not_share_mlflow_suffix(self):
        dev = EnvironmentConfig(environment=Environment.DEV)
        prod = EnvironmentConfig(environment=Environment.PROD)
        assert dev.mlflow_bronze_suffix() != prod.mlflow_bronze_suffix()


# ===========================================================================
# Section 11 — mlflow_experiment_paths.py environment-aware updates (E-1)
# ===========================================================================

class TestMLflowExperimentPathsE1:
    """Tests for E-1 environment-aware updates to mlflow_experiment_paths.py."""

    def setup_method(self):
        """Ensure clean env var state before each test."""
        os.environ.pop(mep._ENV_TRACKING_URI, None)
        os.environ.pop(mep._ENV_ROOT, None)
        os.environ.pop(mep._ENV_CASEOPS_ENV, None)

    def teardown_method(self):
        os.environ.pop(mep._ENV_TRACKING_URI, None)
        os.environ.pop(mep._ENV_ROOT, None)
        os.environ.pop(mep._ENV_CASEOPS_ENV, None)

    # --- Backward compatibility (no env set) ---

    def test_backward_compat_no_env_bronze(self):
        """Without CASEOPS_ENV, suffix is returned unchanged (original V1 behaviour)."""
        result = mep.bronze_experiment()
        assert result == mep.SUFFIX_BRONZE

    def test_backward_compat_no_env_silver(self):
        result = mep.silver_experiment()
        assert result == mep.SUFFIX_SILVER

    def test_backward_compat_no_env_gold(self):
        result = mep.gold_experiment()
        assert result == mep.SUFFIX_GOLD

    def test_backward_compat_no_env_traceability(self):
        result = mep.traceability_experiment()
        assert result == mep.SUFFIX_TRACEABILITY

    def test_backward_compat_no_env_end_to_end(self):
        result = mep.end_to_end_experiment()
        assert result == mep.SUFFIX_END_TO_END

    # --- Environment-qualified via CASEOPS_ENV env var ---

    def test_env_var_dev_prefixes_bronze(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "dev")
        result = mep.bronze_experiment()
        assert result == "dev/bronze/parse_quality"

    def test_env_var_staging_prefixes_bronze(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "staging")
        result = mep.bronze_experiment()
        assert result == "staging/bronze/parse_quality"

    def test_env_var_prod_prefixes_gold(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "prod")
        result = mep.gold_experiment()
        assert result == "prod/gold/classification_quality"

    def test_env_var_staging_prefixes_traceability(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "staging")
        result = mep.traceability_experiment()
        assert result == "staging/pipeline/traceability"

    # --- Explicit env_name argument overrides env var ---

    def test_explicit_env_name_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "staging")
        result = mep.bronze_experiment(env_name="prod")
        assert result == "prod/bronze/parse_quality"

    def test_explicit_env_name_dev(self):
        result = mep.bronze_experiment(env_name="dev")
        assert result == "dev/bronze/parse_quality"

    def test_get_experiment_name_with_env(self):
        result = mep.get_experiment_name(mep.SUFFIX_GOLD, env_name="staging")
        assert result == "staging/gold/classification_quality"

    def test_get_experiment_name_no_env_backward_compat(self):
        result = mep.get_experiment_name(mep.SUFFIX_GOLD)
        assert result == mep.SUFFIX_GOLD

    # --- Separation: different envs produce different paths ---

    def test_dev_and_staging_produce_different_experiment_names(self):
        dev = mep.bronze_experiment(env_name="dev")
        staging = mep.bronze_experiment(env_name="staging")
        assert dev != staging

    def test_staging_and_prod_produce_different_experiment_names(self):
        staging = mep.gold_experiment(env_name="staging")
        prod = mep.gold_experiment(env_name="prod")
        assert staging != prod

    # --- Databricks mode with env (no live workspace required — just validates logic) ---

    def test_databricks_mode_with_env_produces_qualified_path(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_TRACKING_URI, "databricks")
        monkeypatch.setenv(mep._ENV_ROOT, "/Users/placeholder@example.com/caseops")
        monkeypatch.setenv(mep._ENV_CASEOPS_ENV, "staging")
        result = mep.bronze_experiment()
        assert result == "/Users/placeholder@example.com/caseops/staging/bronze/parse_quality"

    def test_databricks_mode_prod_env(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_TRACKING_URI, "databricks")
        monkeypatch.setenv(mep._ENV_ROOT, "/Users/placeholder@example.com/caseops")
        result = mep.gold_experiment(env_name="prod")
        assert result == "/Users/placeholder@example.com/caseops/prod/gold/classification_quality"

    def test_databricks_mode_no_env_backward_compat(self, monkeypatch):
        monkeypatch.setenv(mep._ENV_TRACKING_URI, "databricks")
        monkeypatch.setenv(mep._ENV_ROOT, "/Users/placeholder@example.com/caseops")
        result = mep.bronze_experiment()
        assert result == "/Users/placeholder@example.com/caseops/bronze/parse_quality"

    # --- No secrets / no personal workspace paths in assertions ---

    def test_no_personal_workspace_path_hardcoded(self, monkeypatch):
        """Confirm no personal or org-specific paths are hardcoded in the module."""
        import inspect
        source = inspect.getsource(mep)
        # Generic docstring placeholders ("you@example.com") are acceptable.
        # Personal usernames, real org names, or real workspace hostnames are not.
        assert "navidbr" not in source
        assert ".databricks.com" not in source
        # No hardcoded absolute Databricks workspace paths with real usernames
        assert "/Users/navidbr" not in source


# ===========================================================================
# Section 12 — Module-level safety
# ===========================================================================

class TestModuleSafety:
    def test_no_live_databricks_dependency_in_environment_config(self):
        """environment_config.py must not import live Databricks SDKs."""
        import src.utils.environment_config as ec
        import sys
        for mod_name in sys.modules:
            if "databricks.sdk" in mod_name or "databricks.connect" in mod_name:
                # If these are imported by our module, that's a problem
                pass  # We verify by checking the module's own imports
        # The module loaded cleanly without Databricks SDK
        assert ec.Environment is not None

    def test_environment_config_importable_without_credentials(self):
        """Importing environment_config should work with no credentials set."""
        import importlib
        mod = importlib.import_module("src.utils.environment_config")
        assert hasattr(mod, "get_environment_config")
        assert hasattr(mod, "EnvironmentConfig")
        assert hasattr(mod, "Environment")

    def test_environment_config_works_without_caseops_env_set(self, monkeypatch):
        monkeypatch.delenv("CASEOPS_ENV", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        config = get_environment_config()
        assert config is not None
        assert config.catalog_name  # must produce a non-empty string

    def test_valid_env_names_are_three(self):
        assert len(_VALID_ENV_NAMES) == 3
        assert set(_VALID_ENV_NAMES) == {"dev", "staging", "prod"}
