"""
environment_config.py — E-1: Environment-aware configuration and resource naming.

Implements the bounded environment model for the Databricks CaseOps Lakehouse pipeline.
Supports three named environments: dev, staging, prod.

All resource names are derived deterministically from the environment name — no
workspace-specific values, no secrets, no hardcoded paths.

## Usage

    from src.utils.environment_config import get_environment_config, Environment

    # Resolve from the CASEOPS_ENV env var (or default to 'dev')
    config = get_environment_config()

    # Resolve explicitly
    config = get_environment_config("staging")

    # Resource names
    config.catalog_name            # "caseops_staging"
    config.bronze_table            # "caseops_staging.bronze.parsed_documents"
    config.raw_volume_path         # "/Volumes/caseops_staging/raw/documents"
    config.mlflow_experiment_suffix("bronze/parse_quality")
    # "staging/bronze/parse_quality"

## Runtime configuration

Set CASEOPS_ENV before running any pipeline or evaluator:

    export CASEOPS_ENV=staging

Do NOT commit personal workspace values to the repo. This module derives all
resource names from the environment key only — no personal workspace detail needed.

## Design notes

- Three environments: dev, staging, prod
- Unity Catalog isolation: one catalog per environment (caseops_dev, caseops_staging,
  caseops_prod). Schema and table names are identical across environments — isolation
  is at the catalog level, which is the standard Unity Catalog multi-environment pattern.
- MLflow experiment isolation: environment name is prefixed to the experiment suffix
  (e.g. dev/bronze/parse_quality, staging/bronze/parse_quality).
- Backward compatibility: existing pipelines that do not pass an environment continue
  to work — the environment defaults to 'dev' (safe for local iteration).
- No live Databricks workspace required to use this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# ---------------------------------------------------------------------------
# Environment vocabulary — the bounded set of valid environments.
# ---------------------------------------------------------------------------

class Environment(str, Enum):
    """Named deployment environments for the CaseOps pipeline."""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


# All valid environment names for validation error messages.
_VALID_ENV_NAMES = [e.value for e in Environment]

# Env var name — set at runtime, never committed.
_ENV_VAR = "CASEOPS_ENV"

# Default environment for local development and local test runs.
_DEFAULT_ENV = Environment.DEV

# ---------------------------------------------------------------------------
# Schema and table name constants — identical across all environments.
# Environment isolation is at the catalog level.
# ---------------------------------------------------------------------------

_SCHEMA_RAW = "raw"
_SCHEMA_BRONZE = "bronze"
_SCHEMA_SILVER = "silver"
_SCHEMA_GOLD = "gold"

_TABLE_BRONZE = "parsed_documents"
_TABLE_SILVER = "extracted_records"
_TABLE_GOLD = "ai_ready_assets"
_VOLUME_RAW = "documents"

# ---------------------------------------------------------------------------
# EnvironmentConfig — all resource naming for a single environment.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EnvironmentConfig:
    """
    Deterministic resource configuration for one named environment.

    All properties are derived from `environment` — no additional inputs required.
    Frozen to prevent accidental mutation after construction.
    """

    environment: Environment

    # ------------------------------------------------------------------
    # Unity Catalog resource names
    # ------------------------------------------------------------------

    @property
    def catalog_name(self) -> str:
        """Unity Catalog catalog name for this environment.

        Convention: caseops_{env}
        Examples: caseops_dev, caseops_staging, caseops_prod
        """
        return f"caseops_{self.environment.value}"

    @property
    def raw_schema(self) -> str:
        return _SCHEMA_RAW

    @property
    def bronze_schema(self) -> str:
        return _SCHEMA_BRONZE

    @property
    def silver_schema(self) -> str:
        return _SCHEMA_SILVER

    @property
    def gold_schema(self) -> str:
        return _SCHEMA_GOLD

    # ------------------------------------------------------------------
    # Fully-qualified Unity Catalog table names
    # ------------------------------------------------------------------

    @property
    def bronze_table(self) -> str:
        """FQN for the Bronze Delta table in this environment."""
        return f"{self.catalog_name}.{_SCHEMA_BRONZE}.{_TABLE_BRONZE}"

    @property
    def silver_table(self) -> str:
        """FQN for the Silver Delta table in this environment."""
        return f"{self.catalog_name}.{_SCHEMA_SILVER}.{_TABLE_SILVER}"

    @property
    def gold_table(self) -> str:
        """FQN for the Gold Delta table in this environment."""
        return f"{self.catalog_name}.{_SCHEMA_GOLD}.{_TABLE_GOLD}"

    # ------------------------------------------------------------------
    # Unity Catalog Volume paths
    # ------------------------------------------------------------------

    @property
    def raw_volume_path(self) -> str:
        """Absolute Unity Catalog Volume path for raw documents in this environment."""
        return f"/Volumes/{self.catalog_name}/{_SCHEMA_RAW}/{_VOLUME_RAW}"

    @property
    def export_volume_base(self) -> str:
        """Base Volume path for Gold export artifacts in this environment."""
        return f"/Volumes/{self.catalog_name}/{_SCHEMA_GOLD}/exports"

    # ------------------------------------------------------------------
    # MLflow experiment naming
    # ------------------------------------------------------------------

    def mlflow_experiment_suffix(self, stage_suffix: str) -> str:
        """
        Return the environment-qualified MLflow experiment suffix.

        The returned value is a relative suffix — not a full Databricks workspace path.
        Use with mlflow_experiment_paths.get_experiment_name() to resolve the full path.

        Examples:
            config.mlflow_experiment_suffix("bronze/parse_quality")
            # "dev/bronze/parse_quality"

            config.mlflow_experiment_suffix("gold/classification_quality")
            # "staging/gold/classification_quality"
        """
        return f"{self.environment.value}/{stage_suffix}"

    def mlflow_bronze_suffix(self) -> str:
        """Environment-qualified MLflow suffix for Bronze parse quality."""
        return self.mlflow_experiment_suffix("bronze/parse_quality")

    def mlflow_silver_suffix(self) -> str:
        """Environment-qualified MLflow suffix for Silver extraction quality."""
        return self.mlflow_experiment_suffix("silver/extraction_quality")

    def mlflow_gold_suffix(self) -> str:
        """Environment-qualified MLflow suffix for Gold classification quality."""
        return self.mlflow_experiment_suffix("gold/classification_quality")

    def mlflow_traceability_suffix(self) -> str:
        """Environment-qualified MLflow suffix for pipeline traceability."""
        return self.mlflow_experiment_suffix("pipeline/traceability")

    def mlflow_end_to_end_suffix(self) -> str:
        """Environment-qualified MLflow suffix for end-to-end pipeline evaluation."""
        return self.mlflow_experiment_suffix("pipeline/end_to_end")

    # ------------------------------------------------------------------
    # Convenience / introspection
    # ------------------------------------------------------------------

    @property
    def env_name(self) -> str:
        """Environment name as a plain string."""
        return self.environment.value

    def as_dict(self) -> dict:
        """Return all resource names as a plain dict — useful for logging / debugging."""
        return {
            "environment": self.env_name,
            "catalog_name": self.catalog_name,
            "bronze_table": self.bronze_table,
            "silver_table": self.silver_table,
            "gold_table": self.gold_table,
            "raw_volume_path": self.raw_volume_path,
            "export_volume_base": self.export_volume_base,
            "mlflow_bronze_suffix": self.mlflow_bronze_suffix(),
            "mlflow_silver_suffix": self.mlflow_silver_suffix(),
            "mlflow_gold_suffix": self.mlflow_gold_suffix(),
            "mlflow_traceability_suffix": self.mlflow_traceability_suffix(),
            "mlflow_end_to_end_suffix": self.mlflow_end_to_end_suffix(),
        }

    def __repr__(self) -> str:
        return f"EnvironmentConfig(environment={self.env_name!r}, catalog={self.catalog_name!r})"


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

def parse_environment(env_name: str) -> Environment:
    """
    Parse a string to an Environment enum value.

    Case-insensitive. Raises ValueError with a clear message for unrecognised names.
    """
    normalised = env_name.strip().lower()
    try:
        return Environment(normalised)
    except ValueError:
        raise ValueError(
            f"Unknown environment: {env_name!r}. "
            f"Valid environments: {_VALID_ENV_NAMES}. "
            f"Set CASEOPS_ENV to one of these values at runtime — do not commit workspace URLs."
        )


def get_environment_config(env_name: Optional[str] = None) -> EnvironmentConfig:
    """
    Resolve and return the EnvironmentConfig for the named environment.

    Resolution order:
      1. Explicit `env_name` argument (if provided and non-empty)
      2. CASEOPS_ENV environment variable
      3. Default: 'dev'

    This function does not require a live Databricks workspace.

    Args:
        env_name: Optional explicit environment name ('dev', 'staging', 'prod').

    Returns:
        EnvironmentConfig for the resolved environment.

    Raises:
        ValueError: If the resolved name is not a recognised environment.
    """
    resolved = (env_name or "").strip() or os.environ.get(_ENV_VAR, "").strip() or _DEFAULT_ENV.value
    environment = parse_environment(resolved)
    return EnvironmentConfig(environment=environment)
