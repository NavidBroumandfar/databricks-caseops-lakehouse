"""
mlflow_experiment_paths.py — Shared MLflow experiment path resolution.

Databricks MLflow requires experiment names to be absolute workspace paths
(e.g. /Users/user@example.com/caseops/bronze/parse_quality).  Relative names
like "caseops/bronze/parse_quality" are valid for local/non-Databricks MLflow
tracking only.

## Runtime configuration

When MLFLOW_TRACKING_URI == "databricks":
    Set CASEOPS_MLFLOW_EXPERIMENT_ROOT to an absolute workspace path before
    running any evaluator or the orchestrator.  Example:

        export CASEOPS_MLFLOW_EXPERIMENT_ROOT=/Users/you@example.com/caseops

    The helper will construct fully-qualified experiment paths by joining the
    root with each logical suffix (bronze/parse_quality, etc.).

When MLFLOW_TRACKING_URI != "databricks" (local or unset):
    CASEOPS_MLFLOW_EXPERIMENT_ROOT is ignored.  The logical suffix is used
    as-is (for local runs) or prefixed with the environment name when CASEOPS_ENV
    is set — see E-1 environment separation notes below.

## E-1 Environment separation (Phase E-1)

Set CASEOPS_ENV to isolate experiment paths per environment:

    export CASEOPS_ENV=staging

When CASEOPS_ENV is set:
    - Local/non-Databricks: experiment name becomes "<env>/<suffix>"
      (e.g. "staging/bronze/parse_quality")
    - Databricks: experiment name becomes "<root>/<env>/<suffix>"
      (e.g. "/Users/you@example.com/caseops/staging/bronze/parse_quality")

When CASEOPS_ENV is NOT set (backward-compatible default):
    - Local: suffix used as-is (original V1 behaviour)
    - Databricks: "<root>/<suffix>" (original V1 behaviour)

The per-evaluator convenience functions (bronze_experiment, etc.) automatically
pick up CASEOPS_ENV.  You can also pass an explicit env_name to get_experiment_name.

## Design notes
- No personal workspace paths are hardcoded here or anywhere in the repo.
- Validation fails fast with a clear message when the root is missing or
  malformed, preventing a confusing downstream MLflow error.
- This module has zero non-stdlib dependencies so it is safe to import
  unconditionally alongside the optional mlflow import in each evaluator.
- CASEOPS_ENV defaults to None (backward-compatible); set explicitly for
  environment-isolated experiment tracking.
"""

from __future__ import annotations

import os
from typing import Optional

# ---------------------------------------------------------------------------
# Logical experiment suffixes — single source of truth for all evaluators.
# These are the human-readable stage names, environment-agnostic.
# ---------------------------------------------------------------------------

SUFFIX_BRONZE = "bronze/parse_quality"
SUFFIX_SILVER = "silver/extraction_quality"
SUFFIX_GOLD = "gold/classification_quality"
SUFFIX_TRACEABILITY = "pipeline/traceability"
SUFFIX_END_TO_END = "pipeline/end_to_end"

# Env var names — runtime-only, never committed.
_ENV_ROOT = "CASEOPS_MLFLOW_EXPERIMENT_ROOT"
_ENV_TRACKING_URI = "MLFLOW_TRACKING_URI"
_ENV_CASEOPS_ENV = "CASEOPS_ENV"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def is_databricks_tracking() -> bool:
    """Return True when the MLflow tracking URI is set to 'databricks'."""
    return os.environ.get(_ENV_TRACKING_URI, "").strip().lower() == "databricks"


def _resolve_env_name(env_name: Optional[str]) -> Optional[str]:
    """
    Resolve the environment name from argument or CASEOPS_ENV env var.

    Returns None if neither is set (backward-compatible: no env prefix).
    """
    if env_name:
        return env_name.strip()
    from_env = os.environ.get(_ENV_CASEOPS_ENV, "").strip()
    return from_env if from_env else None


def _apply_env_prefix(suffix: str, env_name: Optional[str]) -> str:
    """Prefix suffix with environment name if one is resolved."""
    resolved = _resolve_env_name(env_name)
    if resolved:
        return f"{resolved}/{suffix}"
    return suffix


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_experiment_name(suffix: str, env_name: Optional[str] = None) -> str:
    """
    Return the MLflow experiment name for *suffix*.

    Resolution behaviour:
    - Non-Databricks, no env:  returns suffix unchanged (original V1 behaviour)
    - Non-Databricks, env set: returns "{env}/{suffix}"
    - Databricks, no env:      returns "{root}/{suffix}" (original V1 behaviour)
    - Databricks, env set:     returns "{root}/{env}/{suffix}"

    Args:
        suffix:   Logical stage suffix (e.g. SUFFIX_BRONZE = "bronze/parse_quality")
        env_name: Optional explicit environment name. If None, reads CASEOPS_ENV.

    Raises RuntimeError with a clear message if the Databricks root is missing or
    malformed when targeting Databricks.
    """
    qualified_suffix = _apply_env_prefix(suffix, env_name)

    if not is_databricks_tracking():
        return qualified_suffix

    root = os.environ.get(_ENV_ROOT, "").strip()

    if not root:
        raise RuntimeError(
            f"MLFLOW_TRACKING_URI is 'databricks' but {_ENV_ROOT} is not set.\n"
            f"Set it in your shell before running the evaluator:\n"
            f"  export {_ENV_ROOT}=/Users/you@example.com/caseops\n"
            f"Do NOT commit this value to the repo."
        )

    if not root.startswith("/"):
        raise RuntimeError(
            f"{_ENV_ROOT}='{root}' is not a valid Databricks workspace path.\n"
            f"Databricks experiment paths must start with '/'.\n"
            f"Example:  export {_ENV_ROOT}=/Users/you@example.com/caseops"
        )

    # Normalise: strip trailing slash so we always produce /root/.../suffix
    return f"{root.rstrip('/')}/{qualified_suffix}"


# ---------------------------------------------------------------------------
# Per-evaluator convenience functions (mirror the original constants interface)
# All functions accept an optional env_name; default reads CASEOPS_ENV.
# ---------------------------------------------------------------------------

def bronze_experiment(env_name: Optional[str] = None) -> str:
    """Return the resolved MLflow experiment name for Bronze parse quality."""
    return get_experiment_name(SUFFIX_BRONZE, env_name=env_name)


def silver_experiment(env_name: Optional[str] = None) -> str:
    """Return the resolved MLflow experiment name for Silver extraction quality."""
    return get_experiment_name(SUFFIX_SILVER, env_name=env_name)


def gold_experiment(env_name: Optional[str] = None) -> str:
    """Return the resolved MLflow experiment name for Gold classification quality."""
    return get_experiment_name(SUFFIX_GOLD, env_name=env_name)


def traceability_experiment(env_name: Optional[str] = None) -> str:
    """Return the resolved MLflow experiment name for pipeline traceability."""
    return get_experiment_name(SUFFIX_TRACEABILITY, env_name=env_name)


def end_to_end_experiment(env_name: Optional[str] = None) -> str:
    """Return the resolved MLflow experiment name for end-to-end pipeline evaluation."""
    return get_experiment_name(SUFFIX_END_TO_END, env_name=env_name)
