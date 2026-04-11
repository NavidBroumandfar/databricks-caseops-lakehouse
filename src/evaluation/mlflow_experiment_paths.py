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
    as-is, which is the original V1 behaviour.

## Design notes
- No personal workspace paths are hardcoded here or anywhere in the repo.
- Validation fails fast with a clear message when the root is missing or
  malformed, preventing a confusing downstream MLflow error.
- This module has zero non-stdlib dependencies so it is safe to import
  unconditionally alongside the optional mlflow import in each evaluator.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Logical experiment suffixes — single source of truth for all evaluators.
# These are the human-readable names that also match the original V1 constants.
# ---------------------------------------------------------------------------

SUFFIX_BRONZE = "bronze/parse_quality"
SUFFIX_SILVER = "silver/extraction_quality"
SUFFIX_GOLD = "gold/classification_quality"
SUFFIX_TRACEABILITY = "pipeline/traceability"
SUFFIX_END_TO_END = "pipeline/end_to_end"

# Env var name — runtime-only, never committed.
_ENV_ROOT = "CASEOPS_MLFLOW_EXPERIMENT_ROOT"
_ENV_TRACKING_URI = "MLFLOW_TRACKING_URI"


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def is_databricks_tracking() -> bool:
    """Return True when the MLflow tracking URI is set to 'databricks'."""
    return os.environ.get(_ENV_TRACKING_URI, "").strip().lower() == "databricks"


def get_experiment_name(suffix: str) -> str:
    """
    Return the MLflow experiment name for *suffix*.

    - Non-Databricks: returns the suffix unchanged (original V1 behaviour).
    - Databricks: validates CASEOPS_MLFLOW_EXPERIMENT_ROOT and returns
      '<root>/<suffix>'.

    Raises RuntimeError with a clear message if the root is missing or
    malformed when targeting Databricks.
    """
    if not is_databricks_tracking():
        return suffix

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

    # Normalise: strip trailing slash so we always produce /root/suffix
    return f"{root.rstrip('/')}/{suffix}"


# ---------------------------------------------------------------------------
# Per-evaluator convenience functions (mirror the original constants interface)
# ---------------------------------------------------------------------------

def bronze_experiment() -> str:
    """Return the resolved MLflow experiment name for Bronze parse quality."""
    return get_experiment_name(SUFFIX_BRONZE)


def silver_experiment() -> str:
    """Return the resolved MLflow experiment name for Silver extraction quality."""
    return get_experiment_name(SUFFIX_SILVER)


def gold_experiment() -> str:
    """Return the resolved MLflow experiment name for Gold classification quality."""
    return get_experiment_name(SUFFIX_GOLD)


def traceability_experiment() -> str:
    """Return the resolved MLflow experiment name for pipeline traceability."""
    return get_experiment_name(SUFFIX_TRACEABILITY)


def end_to_end_experiment() -> str:
    """Return the resolved MLflow experiment name for end-to-end pipeline evaluation."""
    return get_experiment_name(SUFFIX_END_TO_END)
