"""
tests/test_mlflow_experiment_paths.py — Unit tests for the MLflow experiment path helper.

Covers:
  - Local/non-Databricks path behaviour: suffix returned unchanged
  - Databricks path expansion: root + suffix joined correctly
  - Missing env var fails with clear RuntimeError when targeting Databricks
  - Malformed root (no leading slash) fails with clear RuntimeError
  - No personal workspace paths committed
  - All five per-evaluator convenience functions return expected logical suffixes
    in the local case and expected absolute paths in the Databricks case
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

# Make the evaluation package importable without installing
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "evaluation"))

import mlflow_experiment_paths as mep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reload() -> None:
    """Reload the module so env-var changes take effect cleanly."""
    importlib.reload(mep)


# ---------------------------------------------------------------------------
# Local / non-Databricks behaviour
# ---------------------------------------------------------------------------

class TestLocalBehaviour:
    """When MLFLOW_TRACKING_URI is not 'databricks', suffixes are used as-is."""

    def setup_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)
        _reload()

    def teardown_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)

    def test_get_experiment_name_returns_suffix_unchanged(self):
        assert mep.get_experiment_name(mep.SUFFIX_BRONZE) == mep.SUFFIX_BRONZE

    def test_is_databricks_tracking_false(self):
        assert mep.is_databricks_tracking() is False

    def test_bronze_experiment_is_suffix(self):
        assert mep.bronze_experiment() == mep.SUFFIX_BRONZE

    def test_silver_experiment_is_suffix(self):
        assert mep.silver_experiment() == mep.SUFFIX_SILVER

    def test_gold_experiment_is_suffix(self):
        assert mep.gold_experiment() == mep.SUFFIX_GOLD

    def test_traceability_experiment_is_suffix(self):
        assert mep.traceability_experiment() == mep.SUFFIX_TRACEABILITY

    def test_end_to_end_experiment_is_suffix(self):
        assert mep.end_to_end_experiment() == mep.SUFFIX_END_TO_END

    def test_mlflow_tracking_uri_empty_string_treated_as_local(self):
        os.environ["MLFLOW_TRACKING_URI"] = ""
        _reload()
        assert mep.is_databricks_tracking() is False
        assert mep.get_experiment_name(mep.SUFFIX_GOLD) == mep.SUFFIX_GOLD

    def test_mlflow_tracking_uri_http_treated_as_local(self):
        os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5000"
        _reload()
        assert mep.is_databricks_tracking() is False


# ---------------------------------------------------------------------------
# Databricks path expansion
# ---------------------------------------------------------------------------

class TestDatabricksBehaviour:
    """When MLFLOW_TRACKING_URI == 'databricks', absolute paths are constructed."""

    def setup_method(self):
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "/Users/testuser@example.com/caseops"
        _reload()

    def teardown_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)

    def test_is_databricks_tracking_true(self):
        assert mep.is_databricks_tracking() is True

    def test_bronze_experiment_absolute(self):
        result = mep.bronze_experiment()
        assert result == "/Users/testuser@example.com/caseops/bronze/parse_quality"

    def test_silver_experiment_absolute(self):
        result = mep.silver_experiment()
        assert result == "/Users/testuser@example.com/caseops/silver/extraction_quality"

    def test_gold_experiment_absolute(self):
        result = mep.gold_experiment()
        assert result == "/Users/testuser@example.com/caseops/gold/classification_quality"

    def test_traceability_experiment_absolute(self):
        result = mep.traceability_experiment()
        assert result == "/Users/testuser@example.com/caseops/pipeline/traceability"

    def test_end_to_end_experiment_absolute(self):
        result = mep.end_to_end_experiment()
        assert result == "/Users/testuser@example.com/caseops/pipeline/end_to_end"

    def test_root_trailing_slash_is_normalised(self):
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "/Users/testuser@example.com/caseops/"
        result = mep.get_experiment_name(mep.SUFFIX_BRONZE)
        assert result == "/Users/testuser@example.com/caseops/bronze/parse_quality"
        assert "//" not in result

    def test_experiment_names_start_with_slash(self):
        for fn in (
            mep.bronze_experiment,
            mep.silver_experiment,
            mep.gold_experiment,
            mep.traceability_experiment,
            mep.end_to_end_experiment,
        ):
            assert fn().startswith("/"), f"{fn.__name__}() did not start with '/'"


# ---------------------------------------------------------------------------
# Missing env var failures
# ---------------------------------------------------------------------------

class TestMissingRoot:
    """Missing CASEOPS_MLFLOW_EXPERIMENT_ROOT when targeting Databricks must fail fast."""

    def setup_method(self):
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)
        _reload()

    def teardown_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)

    def test_missing_root_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="CASEOPS_MLFLOW_EXPERIMENT_ROOT"):
            mep.get_experiment_name(mep.SUFFIX_BRONZE)

    def test_error_message_contains_set_instruction(self):
        with pytest.raises(RuntimeError) as exc_info:
            mep.get_experiment_name(mep.SUFFIX_BRONZE)
        msg = str(exc_info.value)
        assert "export" in msg.lower() or "set" in msg.lower()

    def test_empty_root_raises_runtime_error(self):
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "   "
        with pytest.raises(RuntimeError, match="CASEOPS_MLFLOW_EXPERIMENT_ROOT"):
            mep.get_experiment_name(mep.SUFFIX_SILVER)


# ---------------------------------------------------------------------------
# Malformed root failures
# ---------------------------------------------------------------------------

class TestMalformedRoot:
    """Root without leading slash must fail with a clear message."""

    def setup_method(self):
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        _reload()

    def teardown_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)

    def test_relative_root_raises_runtime_error(self):
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "caseops"
        with pytest.raises(RuntimeError, match="must start with '/'"):
            mep.get_experiment_name(mep.SUFFIX_BRONZE)

    def test_root_with_only_space_still_fails_as_empty(self):
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "  "
        with pytest.raises(RuntimeError):
            mep.get_experiment_name(mep.SUFFIX_BRONZE)

    def test_error_message_shows_example(self):
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "caseops/experiments"
        with pytest.raises(RuntimeError) as exc_info:
            mep.get_experiment_name(mep.SUFFIX_BRONZE)
        assert "example" in str(exc_info.value).lower() or "/Users" in str(exc_info.value)


# ---------------------------------------------------------------------------
# No personal values
# ---------------------------------------------------------------------------

class TestNoPersonalValues:
    """The helper module source must not contain hardcoded personal paths."""

    def test_no_hardcoded_personal_values_in_module_source(self):
        source = Path(__file__).resolve().parents[1] / "src" / "evaluation" / "mlflow_experiment_paths.py"
        text = source.read_text(encoding="utf-8")
        # Must not contain real personal identifiers — generic example addresses are fine.
        assert "navidbr" not in text, "personal username 'navidbr' found in module source"
        # Lines containing /Users/ must only have generic placeholder text (e.g. "you@" or "user@")
        users_lines = [ln for ln in text.splitlines() if "/Users/" in ln]
        for line in users_lines:
            assert "navidbr" not in line, f"personal path found in: {line}"
            assert "azuredatabricks.net" not in line, f"workspace URL found in: {line}"

    def test_suffixes_are_relative(self):
        for suffix in (
            mep.SUFFIX_BRONZE,
            mep.SUFFIX_SILVER,
            mep.SUFFIX_GOLD,
            mep.SUFFIX_TRACEABILITY,
            mep.SUFFIX_END_TO_END,
        ):
            assert not suffix.startswith("/"), f"Suffix '{suffix}' must be relative"


# ---------------------------------------------------------------------------
# Evaluator/orchestrator experiment-name consistency
# ---------------------------------------------------------------------------

class TestEvaluatorConstantConsistency:
    """
    Verifies that all evaluator modules expose MLFLOW_EXPERIMENT_NAME equal to
    the logical suffix, and that the helper resolves correctly for each in both
    the local and Databricks cases.
    """

    def setup_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)
        _reload()

    def teardown_method(self):
        os.environ.pop("MLFLOW_TRACKING_URI", None)
        os.environ.pop("CASEOPS_MLFLOW_EXPERIMENT_ROOT", None)

    def _import_eval(self, name: str):
        """Import an evaluator module from the evaluation package."""
        import importlib
        return importlib.import_module(name)

    def test_bronze_constant_matches_suffix(self):
        mod = self._import_eval("eval_bronze")
        assert mod.MLFLOW_EXPERIMENT_NAME == mep.SUFFIX_BRONZE

    def test_silver_constant_matches_suffix(self):
        mod = self._import_eval("eval_silver")
        assert mod.MLFLOW_EXPERIMENT_NAME == mep.SUFFIX_SILVER

    def test_gold_constant_matches_suffix(self):
        mod = self._import_eval("eval_gold")
        assert mod.MLFLOW_EXPERIMENT_NAME == mep.SUFFIX_GOLD

    def test_traceability_constant_matches_suffix(self):
        mod = self._import_eval("eval_traceability")
        assert mod.MLFLOW_EXPERIMENT_NAME == mep.SUFFIX_TRACEABILITY

    def test_run_evaluation_constant_matches_suffix(self):
        mod = self._import_eval("run_evaluation")
        assert mod.MLFLOW_EXPERIMENT_NAME == mep.SUFFIX_END_TO_END

    def test_databricks_resolves_all_to_absolute(self):
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        os.environ["CASEOPS_MLFLOW_EXPERIMENT_ROOT"] = "/Users/ci@example.com/caseops"
        _reload()

        root = "/Users/ci@example.com/caseops"
        assert mep.bronze_experiment() == f"{root}/{mep.SUFFIX_BRONZE}"
        assert mep.silver_experiment() == f"{root}/{mep.SUFFIX_SILVER}"
        assert mep.gold_experiment() == f"{root}/{mep.SUFFIX_GOLD}"
        assert mep.traceability_experiment() == f"{root}/{mep.SUFFIX_TRACEABILITY}"
        assert mep.end_to_end_experiment() == f"{root}/{mep.SUFFIX_END_TO_END}"
