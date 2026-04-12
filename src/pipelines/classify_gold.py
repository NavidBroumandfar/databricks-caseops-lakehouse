"""
classify_gold.py — Gold classification and routing pipeline (Phase A-3 / B-3 / D-0 / D-1)

Reads Silver JSON artifacts produced by extract_silver.py, classifies each
record into a Gold record (document type label + routing label), assembles
the export payload, and writes:
  - one Gold JSON artifact per Silver input record  (output/gold/<gold_record_id>.json)
  - one export JSON artifact per export-ready record (output/gold/exports/<routing_label>/<document_id>.json)

Only Silver records with validation_status != 'invalid' are eligible for
classification. All eligible records are classified and written — no silent
drops. Records that do not meet export readiness criteria are still written
as Gold records with export_ready=False.

Responsibilities of this module (B-3 boundary):
  - Load and validate Silver artifacts for eligibility
  - Select and run the appropriate document classifier
  - Assemble the Gold record (routing label, export payload, export readiness)
  - Delegate all export/handoff materialization to src/pipelines/export_handoff.py
  - Write the final Gold artifact with its resolved export state
  - Assemble and return the pipeline summary

Export/handoff behavior (B-2 enforcement, B-3 boundary):
  All contract-gated export materialization lives in src/pipelines/export_handoff.py.
  Invalid export payloads are blocked before write. Quarantine records produce no
  export file. The Gold record is written once with its final resolved state.

Export path:
  <export_base_dir>/<routing_label>/<document_id>.json
  One file per export_ready=True record that passes contract validation.
  Deterministic: same document_id always maps to the same path within a run.

Classification uses a deterministic local baseline classifier (V1 single domain).
A placeholder adapter for Databricks ai_classify is included as an explicit
future boundary — it is not active in local execution.

Usage:
    # Process all Silver artifacts in the default directory
    python src/pipelines/classify_gold.py --input-dir output/silver

    # Process a single Silver artifact
    python src/pipelines/classify_gold.py --input output/silver/<record>.json

    # Specify output directory
    python src/pipelines/classify_gold.py --input-dir output/silver --output-dir output/gold

Outputs:
    output/gold/<gold_record_id>.json                            — Gold record per Silver input
    output/gold/exports/<routing_label>/<document_id>.json       — Export payload (if export-ready, contract-valid)

Authoritative contract: docs/data-contracts.md § Gold: AI-Ready Asset Contract
Bedrock handoff contract: docs/bedrock-handoff-contract.md
Contract validator: src/schemas/bedrock_contract.py (B-1)
Export/handoff boundary: src/pipelines/export_handoff.py (B-3)
Architecture context: ARCHITECTURE.md § Gold Layer
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from the repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.pipelines.delivery_events import (
    build_delivery_event,
    generate_delivery_event_id,
    write_delivery_event,
)
from src.pipelines.delta_share_handoff import (
    DeltaShareConfig,
    compute_share_preparation_manifest,
    write_share_manifest,
)
from src.pipelines.export_handoff import execute_export
from src.pipelines.handoff_bundle import (
    build_handoff_batch_manifest,
    write_handoff_bundle,
)
from src.pipelines.handoff_report import (
    build_handoff_batch_report,
    derive_outcome,
    write_handoff_report,
)
from src.schemas.gold_schema import (
    ExportPayload,
    ExportProvenance,
    GoldRecord,
    SCHEMA_VERSION,
    SCHEMA_VERSION_V2,
)
from src.utils.classification_taxonomy import (
    DOCUMENT_TYPE_CISA_ADVISORY,
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_INCIDENT_REPORT,
    DOCUMENT_TYPE_UNKNOWN,
    ROUTING_LABEL_QUARANTINE,
    resolve_routing_label,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_INPUT_DIR = "output/silver"
DEFAULT_OUTPUT_DIR = "output/gold"
DEFAULT_EXPORT_DIR = "output/gold/exports"
LOCAL_PIPELINE_RUN_PREFIX = "local-run"
LOCAL_CLASSIFICATION_MODEL = "local_rule_classifier/v1"
PARSED_TEXT_EXCERPT_LENGTH = 2000

# Export readiness thresholds (from docs/data-contracts.md § Downstream AI-Ready Asset Requirements)
EXPORT_CONFIDENCE_THRESHOLD = 0.70
EXPORT_SILVER_COVERAGE_THRESHOLD = 0.50


# ---------------------------------------------------------------------------
# Silver artifact loader
# ---------------------------------------------------------------------------


def load_silver_artifact(path: Path) -> dict:
    """Load and return a Silver record dict from a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def collect_silver_paths(input_dir: Optional[Path], input_file: Optional[Path]) -> list[Path]:
    """Resolve Silver artifact paths to process."""
    if input_file is not None:
        if not input_file.exists():
            raise ValueError(f"Input file does not exist: {input_file}")
        return [input_file]
    if input_dir is not None:
        if not input_dir.is_dir():
            raise ValueError(f"Input directory does not exist: {input_dir}")
        paths = sorted(input_dir.glob("*.json"))
        if not paths:
            raise ValueError(f"No JSON artifacts found in: {input_dir}")
        return paths
    raise ValueError("Provide --input or --input-dir.")


def is_eligible_for_classification(silver: dict) -> bool:
    """
    Return True if the Silver record should proceed to Gold classification.

    Eligibility: validation_status != 'invalid'.
    Invalid records have no extractable fields and cannot produce a meaningful
    classification output.
    """
    return silver.get("validation_status") != "invalid"


# ---------------------------------------------------------------------------
# Classifier abstraction
# ---------------------------------------------------------------------------


class DocumentClassifier(ABC):
    """
    Strategy interface for document classifiers.

    Implementations return a classification result dict with:
        - document_type_label: str
        - classification_confidence: float (0.0–1.0)

    The caller assembles the routing label and Gold record.
    """

    @abstractmethod
    def classify(self, silver: dict) -> dict:
        """
        Classify a Silver record.

        Returns a dict with keys:
            document_type_label: str
            classification_confidence: float
        """
        ...

    @property
    @abstractmethod
    def model_id(self) -> str:
        """Stable model identifier written into the Gold record."""
        ...


# ---------------------------------------------------------------------------
# Local deterministic FDA classifier
# ---------------------------------------------------------------------------


class LocalFDAWarningLetterClassifier(DocumentClassifier):
    """
    Deterministic rule-based classifier for FDA warning letters.

    Classification logic is based on explicit, readable heuristics:
      1. document_class_hint field from Silver
      2. Presence and shape of FDA-specific extracted fields
      3. Silver validation_status
      4. Silver field_coverage_pct

    Confidence scoring:
      - Base confidence: 0.60 if any positive signal is present
      - +0.20 if document_class_hint == 'fda_warning_letter'
      - +0.10 if validation_status is 'valid'
      - +0.10 if field_coverage_pct >= 0.70
      - Maximum: 1.0, minimum for a classified record: 0.60

    This heuristic is intentionally simple, explicit, and stable across runs.
    It is not statistically calibrated — it is a deterministic rule baseline.

    The active V1 model for local-safe execution.
    """

    _MODEL_ID = LOCAL_CLASSIFICATION_MODEL

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def classify(self, silver: dict) -> dict:
        extracted_fields = silver.get("extracted_fields") or {}
        class_hint = silver.get("document_class_hint") or ""
        validation_status = silver.get("validation_status") or ""
        coverage = silver.get("field_coverage_pct") or 0.0

        # --- FDA warning letter signal detection ---
        # A record is classified as fda_warning_letter if at least two of the
        # following signals are present. This avoids classifying a bare/empty
        # Silver record as FDA when there is no evidence.

        signals = []

        if class_hint == "fda_warning_letter":
            signals.append("class_hint_match")

        # FDA-specific field presence checks
        fda_fields = {
            "issuing_office": extracted_fields.get("issuing_office"),
            "recipient_company": extracted_fields.get("recipient_company"),
            "violation_type": extracted_fields.get("violation_type"),
            "corrective_action_requested": extracted_fields.get("corrective_action_requested"),
            "issue_date": extracted_fields.get("issue_date"),
        }
        populated_fda_fields = [
            k for k, v in fda_fields.items()
            if v is not None and v != [] and v != ""
        ]
        if len(populated_fda_fields) >= 3:
            signals.append("fda_fields_populated")

        if validation_status in ("valid", "partial"):
            signals.append("valid_or_partial_extraction")

        if coverage >= 0.50:
            signals.append("adequate_coverage")

        # --- Classification decision ---
        if len(signals) >= 2:
            document_type_label = DOCUMENT_TYPE_FDA_WARNING_LETTER
            confidence = self._compute_confidence(
                class_hint=class_hint,
                validation_status=validation_status,
                coverage=coverage,
            )
        else:
            document_type_label = DOCUMENT_TYPE_UNKNOWN
            confidence = 0.0

        return {
            "document_type_label": document_type_label,
            "classification_confidence": confidence,
        }

    def _compute_confidence(
        self,
        class_hint: str,
        validation_status: str,
        coverage: float,
    ) -> float:
        """
        Compute a deterministic confidence score.

        Base: 0.60 (minimum for a classified non-unknown record)
        +0.20 if class_hint == 'fda_warning_letter'
        +0.10 if validation_status == 'valid'
        +0.10 if field_coverage_pct >= 0.70

        Maximum: 1.0
        """
        score = 0.60
        if class_hint == "fda_warning_letter":
            score += 0.20
        if validation_status == "valid":
            score += 0.10
        if coverage >= 0.70:
            score += 0.10
        return round(min(score, 1.0), 4)


# ---------------------------------------------------------------------------
# Local deterministic CISA advisory classifier (D-1)
# ---------------------------------------------------------------------------


class LocalCISAAdvisoryClassifier(DocumentClassifier):
    """
    Deterministic rule-based classifier for CISA cybersecurity advisories.

    Classification logic is based on explicit, readable heuristics:
      1. document_class_hint field from Silver
      2. Presence and shape of CISA-specific extracted fields
         (advisory_id, severity_level, cve_ids, affected_products)
      3. Silver validation_status
      4. Silver field_coverage_pct

    Confidence scoring mirrors the FDA classifier pattern:
      - Base confidence: 0.60 if at least 2 positive signals present
      - +0.20 if document_class_hint == 'cisa_advisory'
      - +0.10 if validation_status is 'valid'
      - +0.10 if field_coverage_pct >= 0.70
      - Maximum: 1.0

    D-1 active domain. routing_label = 'security_ops'.
    """

    _MODEL_ID = LOCAL_CLASSIFICATION_MODEL

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def classify(self, silver: dict) -> dict:
        extracted_fields = silver.get("extracted_fields") or {}
        class_hint = silver.get("document_class_hint") or ""
        validation_status = silver.get("validation_status") or ""
        coverage = silver.get("field_coverage_pct") or 0.0

        signals = []

        if class_hint == "cisa_advisory":
            signals.append("class_hint_match")

        # CISA-specific field presence checks
        cisa_fields = {
            "advisory_id": extracted_fields.get("advisory_id"),
            "severity_level": extracted_fields.get("severity_level"),
            "cve_ids": extracted_fields.get("cve_ids"),
            "affected_products": extracted_fields.get("affected_products"),
            "remediation_available": extracted_fields.get("remediation_available"),
        }
        populated_cisa_fields = [
            k for k, v in cisa_fields.items()
            if v is not None and v != [] and v != ""
        ]
        if len(populated_cisa_fields) >= 2:
            signals.append("cisa_fields_populated")

        if validation_status in ("valid", "partial"):
            signals.append("valid_or_partial_extraction")

        if coverage >= 0.50:
            signals.append("adequate_coverage")

        if len(signals) >= 2:
            document_type_label = DOCUMENT_TYPE_CISA_ADVISORY
            confidence = self._compute_confidence(
                class_hint=class_hint,
                validation_status=validation_status,
                coverage=coverage,
            )
        else:
            document_type_label = DOCUMENT_TYPE_UNKNOWN
            confidence = 0.0

        return {
            "document_type_label": document_type_label,
            "classification_confidence": confidence,
        }

    def _compute_confidence(
        self,
        class_hint: str,
        validation_status: str,
        coverage: float,
    ) -> float:
        score = 0.60
        if class_hint == "cisa_advisory":
            score += 0.20
        if validation_status == "valid":
            score += 0.10
        if coverage >= 0.70:
            score += 0.10
        return round(min(score, 1.0), 4)


# ---------------------------------------------------------------------------
# Local deterministic incident report classifier (D-2)
# ---------------------------------------------------------------------------


class LocalIncidentReportClassifier(DocumentClassifier):
    """
    Deterministic rule-based classifier for incident reports and post-mortems.

    Classification logic is based on explicit, readable heuristics:
      1. document_class_hint field from Silver
      2. Presence and shape of incident-specific extracted fields
         (incident_date, incident_type, severity, status, affected_systems)
      3. Silver validation_status
      4. Silver field_coverage_pct

    Confidence scoring mirrors the FDA/CISA classifier pattern:
      - Base confidence: 0.60 if at least 2 positive signals present
      - +0.20 if document_class_hint == 'incident_report'
      - +0.10 if validation_status is 'valid'
      - +0.10 if field_coverage_pct >= 0.70
      - Maximum: 1.0

    D-2 active domain. routing_label = 'incident_management'.
    """

    _MODEL_ID = LOCAL_CLASSIFICATION_MODEL

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def classify(self, silver: dict) -> dict:
        extracted_fields = silver.get("extracted_fields") or {}
        class_hint = silver.get("document_class_hint") or ""
        validation_status = silver.get("validation_status") or ""
        coverage = silver.get("field_coverage_pct") or 0.0

        signals = []

        if class_hint == "incident_report":
            signals.append("class_hint_match")

        # Incident-specific field presence checks
        incident_fields = {
            "incident_date": extracted_fields.get("incident_date"),
            "incident_type": extracted_fields.get("incident_type"),
            "severity": extracted_fields.get("severity"),
            "status": extracted_fields.get("status"),
            "affected_systems": extracted_fields.get("affected_systems"),
        }
        populated_incident_fields = [
            k for k, v in incident_fields.items()
            if v is not None and v != [] and v != ""
        ]
        if len(populated_incident_fields) >= 2:
            signals.append("incident_fields_populated")

        if validation_status in ("valid", "partial"):
            signals.append("valid_or_partial_extraction")

        if coverage >= 0.50:
            signals.append("adequate_coverage")

        if len(signals) >= 2:
            document_type_label = DOCUMENT_TYPE_INCIDENT_REPORT
            confidence = self._compute_confidence(
                class_hint=class_hint,
                validation_status=validation_status,
                coverage=coverage,
            )
        else:
            document_type_label = DOCUMENT_TYPE_UNKNOWN
            confidence = 0.0

        return {
            "document_type_label": document_type_label,
            "classification_confidence": confidence,
        }

    def _compute_confidence(
        self,
        class_hint: str,
        validation_status: str,
        coverage: float,
    ) -> float:
        score = 0.60
        if class_hint == "incident_report":
            score += 0.20
        if validation_status == "valid":
            score += 0.10
        if coverage >= 0.70:
            score += 0.10
        return round(min(score, 1.0), 4)


# ---------------------------------------------------------------------------
# Databricks ai_classify adapter (placeholder)
# ---------------------------------------------------------------------------


class DatabricksAiClassifyAdapter(DocumentClassifier):
    """
    Adapter placeholder for Databricks ai_classify.

    In a live Databricks execution environment this class would call:
        spark.sql("SELECT ai_classify(parsed_text, :labels)", ...)

    It is intentionally not implemented here because:
    1. There is no Spark session available in a local run.
    2. No credentials or workspace URLs should live in this file.

    To enable Databricks execution, subclass this and inject a SparkSession
    and the label taxonomy definition.
    """

    _MODEL_ID = "ai_classify/v1"

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def classify(self, silver: dict) -> dict:
        raise NotImplementedError(
            "DatabricksAiClassifyAdapter requires a live Databricks runtime. "
            "Use LocalFDAWarningLetterClassifier for local execution, or inject "
            "a SparkSession and override this method for Databricks cluster execution."
        )


# ---------------------------------------------------------------------------
# Classifier selection
# ---------------------------------------------------------------------------


def select_classifier(document_class_hint: Optional[str]) -> DocumentClassifier:
    """
    Return the appropriate classifier for the given document class.

    Domain-registry routing (D-0 framework, D-1 CISA active, D-2 incident active):
      - None or 'fda_warning_letter' → LocalFDAWarningLetterClassifier (ACTIVE)
      - 'cisa_advisory'              → LocalCISAAdvisoryClassifier (ACTIVE, D-1)
      - 'incident_report'            → LocalIncidentReportClassifier (ACTIVE, D-2)
      - Unregistered domain keys     → raises ValueError with registry context

    FDA and CISA behavior is preserved exactly.
    """
    from src.utils.domain_registry import (
        DOMAIN_REGISTRY,
        DomainNotImplementedError,
        get_active_domains,
        is_domain_active,
    )

    # Treat None as the default FDA domain (backward compatible with V1 callers
    # that do not supply a document_class_hint).
    effective_key = document_class_hint if document_class_hint is not None else "fda_warning_letter"

    # FDA warning letter — ACTIVE, V1 classifier (unchanged)
    if effective_key == "fda_warning_letter":
        return LocalFDAWarningLetterClassifier()

    # CISA advisory — ACTIVE, D-1 classifier
    if effective_key == "cisa_advisory":
        return LocalCISAAdvisoryClassifier()

    # Incident report — ACTIVE, D-2 classifier
    if effective_key == "incident_report":
        return LocalIncidentReportClassifier()

    # Check registry status for all other keys
    if effective_key not in DOMAIN_REGISTRY:
        raise ValueError(
            f"No classifier registered for document_class_hint '{document_class_hint}'. "
            f"Active domains: {[d.domain_key for d in get_active_domains()]}."
        )

    if not is_domain_active(effective_key):
        raise DomainNotImplementedError(effective_key, "classification")

    raise ValueError(
        f"Domain '{effective_key}' is registered as ACTIVE in the domain registry "
        "but no classifier has been implemented yet. "
        "Add the classifier class and dispatch case in classify_gold.py."
    )


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def compute_routing_label(
    document_type_label: str,
    classification_confidence: Optional[float],
    silver_validation_status: str,
    silver_coverage: float,
) -> str:
    """
    Assign a routing label based on classification output and Silver quality signals.

    Quarantine if:
    - document_type_label == 'unknown'
    - confidence is non-null AND below export threshold (per B-0 §6: threshold
      applies only when classification_confidence is non-null)
    - Silver validation_status == 'invalid'
    - Silver field_coverage_pct < coverage threshold

    Otherwise: route via taxonomy map.
    """
    if document_type_label == DOCUMENT_TYPE_UNKNOWN:
        return ROUTING_LABEL_QUARANTINE
    # Per B-0 §6: confidence threshold applies only when classification_confidence is non-null.
    # Bootstrap-origin records (null confidence) are routed by label alone.
    if classification_confidence is not None and classification_confidence < EXPORT_CONFIDENCE_THRESHOLD:
        return ROUTING_LABEL_QUARANTINE
    if silver_validation_status == "invalid":
        return ROUTING_LABEL_QUARANTINE
    if silver_coverage < EXPORT_SILVER_COVERAGE_THRESHOLD:
        return ROUTING_LABEL_QUARANTINE
    return resolve_routing_label(document_type_label)


# ---------------------------------------------------------------------------
# Export readiness
# ---------------------------------------------------------------------------


def compute_export_ready(
    document_type_label: str,
    routing_label: str,
    classification_confidence: Optional[float],
    silver_validation_status: str,
    silver_coverage: float,
    export_payload: ExportPayload,
) -> bool:
    """
    Determine whether a Gold record meets all export readiness criteria.

    From docs/data-contracts.md § Downstream AI-Ready Asset Requirements and
    docs/bedrock-handoff-contract.md § 6:
      1. document_type_label != 'unknown'
      2. routing_label != 'quarantine'
      3. classification_confidence >= threshold (target-state; skipped when null — B-0 §6)
      4. Silver validation_status in ('valid', 'partial')
      5. Silver field_coverage_pct >= threshold
      6. export_payload is structurally valid (all required fields present)

    Per B-0 §6: the confidence threshold applies only when classification_confidence
    is non-null. Bootstrap-origin records (null confidence) are evaluated on the
    remaining criteria only.
    """
    if document_type_label == DOCUMENT_TYPE_UNKNOWN:
        return False
    if routing_label == ROUTING_LABEL_QUARANTINE:
        return False
    # Per B-0 §6: confidence threshold applied only when non-null.
    if classification_confidence is not None and classification_confidence < EXPORT_CONFIDENCE_THRESHOLD:
        return False
    if silver_validation_status not in ("valid", "partial"):
        return False
    if silver_coverage < EXPORT_SILVER_COVERAGE_THRESHOLD:
        return False
    # Structural validity: required payload fields must be present and non-empty
    if not export_payload.document_id:
        return False
    if not export_payload.document_type:
        return False
    if not export_payload.routing_label:
        return False
    return True


# ---------------------------------------------------------------------------
# Export payload builder
# ---------------------------------------------------------------------------


def build_export_payload(
    silver: dict,
    bronze_source_file: str,
    bronze_ingested_at: str,
    document_type_label: str,
    routing_label: str,
    classification_confidence: Optional[float],
    classification_model: str,
    pipeline_run_id: str,
    delivery_event_id: Optional[str] = None,
    delivery_mechanism: Optional[str] = None,
    delta_share_name: Optional[str] = None,
) -> ExportPayload:
    """
    Assemble the AI-ready export payload from Silver record fields and
    classification outputs.

    The parsed_text_excerpt is taken from the Silver record's bronze source
    text if available, defaulting to empty string. In a live Databricks run
    the Bronze parsed_text would be joined from the Delta table.

    C-1 delivery augmentation (v0.2.0):
    When delivery_event_id is provided (delivery_dir set in run_classify_gold),
    the provenance block is upgraded to schema_version v0.2.0 and the three
    new optional delivery fields are populated:
      - delivery_mechanism: 'delta_sharing'
      - delta_share_name: 'caseops_handoff'
      - delivery_event_id: UUID of the delivery event for this batch
    When delivery_event_id is not provided, v0.1.0 behavior is preserved exactly.
    """
    parsed_text = silver.get("parsed_text_excerpt") or silver.get("parsed_text") or ""
    excerpt = parsed_text[:PARSED_TEXT_EXCERPT_LENGTH]

    extracted_fields = silver.get("extracted_fields") or {}

    # C-1: bump to v0.2.0 and populate delivery provenance fields when delivery is active.
    schema_ver = SCHEMA_VERSION_V2 if delivery_event_id else SCHEMA_VERSION

    provenance = ExportProvenance(
        ingested_at=bronze_ingested_at,
        pipeline_run_id=pipeline_run_id,
        extraction_model=silver.get("extraction_model", "unknown"),
        classification_model=classification_model,
        classification_confidence=classification_confidence,
        schema_version=schema_ver,
        delivery_mechanism=delivery_mechanism,
        delta_share_name=delta_share_name,
        delivery_event_id=delivery_event_id,
    )

    return ExportPayload(
        document_id=silver["document_id"],
        source_file=bronze_source_file,
        document_type=document_type_label,
        routing_label=routing_label,
        extracted_fields=extracted_fields,
        parsed_text_excerpt=excerpt,
        provenance=provenance,
    )


# ---------------------------------------------------------------------------
# Gold record assembly
# ---------------------------------------------------------------------------


def assemble_gold_record(
    silver: dict,
    bronze_source_file: str,
    bronze_ingested_at: str,
    classification_result: dict,
    classifier_model_id: str,
    pipeline_run_id: str,
    delivery_event_id: Optional[str] = None,
    delivery_mechanism: Optional[str] = None,
    delta_share_name: Optional[str] = None,
) -> GoldRecord:
    """
    Build a GoldRecord from a Silver record dict and classification output.

    Always produces a record — no silent drops.
    """
    gold_record_id = str(uuid.uuid4())
    classified_at = datetime.now(tz=timezone.utc)

    document_type_label = classification_result["document_type_label"]
    classification_confidence = classification_result["classification_confidence"]

    silver_validation_status = silver.get("validation_status", "invalid")
    silver_coverage = silver.get("field_coverage_pct", 0.0)

    routing_label = compute_routing_label(
        document_type_label=document_type_label,
        classification_confidence=classification_confidence,
        silver_validation_status=silver_validation_status,
        silver_coverage=silver_coverage,
    )

    export_payload = build_export_payload(
        silver=silver,
        bronze_source_file=bronze_source_file,
        bronze_ingested_at=bronze_ingested_at,
        document_type_label=document_type_label,
        routing_label=routing_label,
        classification_confidence=classification_confidence,
        classification_model=classifier_model_id,
        pipeline_run_id=pipeline_run_id,
        delivery_event_id=delivery_event_id,
        delivery_mechanism=delivery_mechanism,
        delta_share_name=delta_share_name,
    )

    export_ready = compute_export_ready(
        document_type_label=document_type_label,
        routing_label=routing_label,
        classification_confidence=classification_confidence,
        silver_validation_status=silver_validation_status,
        silver_coverage=silver_coverage,
        export_payload=export_payload,
    )

    # C-1: use v0.2.0 schema version when delivery is active (delivery_event_id set).
    gold_schema_version = SCHEMA_VERSION_V2 if delivery_event_id else SCHEMA_VERSION

    return GoldRecord(
        document_id=silver["document_id"],
        bronze_record_id=silver["bronze_record_id"],
        extraction_id=silver["extraction_id"],
        gold_record_id=gold_record_id,
        pipeline_run_id=pipeline_run_id,
        classified_at=classified_at,
        document_type_label=document_type_label,
        routing_label=routing_label,
        classification_confidence=classification_confidence,
        classification_model=classifier_model_id,
        export_payload=export_payload,
        export_ready=export_ready,
        export_path=None,  # Set after export artifact is written
        schema_version=gold_schema_version,
    )


# ---------------------------------------------------------------------------
# Artifact writers
# ---------------------------------------------------------------------------


def write_gold_artifact(record: GoldRecord, output_dir: Path) -> Path:
    """Write a Gold record as a JSON artifact named <gold_record_id>.json."""
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / f"{record.gold_record_id}.json"
    artifact_path.write_text(record.to_json_str(), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# Bronze metadata resolution
# ---------------------------------------------------------------------------


def resolve_bronze_metadata(silver: dict, bronze_dir: Optional[Path]) -> tuple[str, str]:
    """
    Resolve the source_file and ingested_at values from the Bronze record.

    In a live Databricks run these would be joined from the Bronze Delta table.
    In local execution, we attempt to load the Bronze artifact from the output
    directory. If not found, we return placeholder values so the pipeline does
    not fail — the export payload will still be structurally valid.

    Returns (source_file, ingested_at).
    """
    bronze_record_id = silver.get("bronze_record_id")
    if bronze_dir and bronze_record_id:
        bronze_path = bronze_dir / f"{bronze_record_id}.json"
        if bronze_path.exists():
            try:
                bronze = json.loads(bronze_path.read_text(encoding="utf-8"))
                return (
                    bronze.get("file_name") or bronze.get("source_path") or "unknown",
                    bronze.get("ingested_at") or "",
                )
            except Exception:  # noqa: BLE001
                pass
    # Fallback — still writes a valid record
    return "unknown", ""


# ---------------------------------------------------------------------------
# Pipeline run ID
# ---------------------------------------------------------------------------


def generate_pipeline_run_id() -> str:
    """Generate a local pipeline run ID. Replaced by MLflow run ID in Databricks."""
    return f"{LOCAL_PIPELINE_RUN_PREFIX}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Top-level pipeline function
# ---------------------------------------------------------------------------


def run_classify_gold(
    input_dir: Optional[str] = None,
    input_file: Optional[str] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    export_dir: str = DEFAULT_EXPORT_DIR,
    bronze_dir: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    report_dir: Optional[str] = None,
    bundle_dir: Optional[str] = None,
    delivery_dir: Optional[str] = None,
) -> list[dict]:
    """
    Run the Gold classification and routing pipeline.

    Reads Silver JSON artifacts, classifies each eligible record, assembles
    export payloads, and writes Gold and export artifacts.

    Each per-record summary includes B-4 outcome fields:
        - outcome_category: one of the OUTCOME_* constants from handoff_report.py
        - outcome_reason: one of the REASON_* constants from handoff_report.py

    If report_dir is provided, a B-4 HandoffBatchReport is written as JSON + text
    artifacts under that directory after all records are processed.

    If bundle_dir is provided, a B-5 HandoffBatchManifest review bundle is written
    as JSON + text artifacts under that directory. The bundle references all
    per-record artifact paths and the B-4 report artifacts (if report_dir was
    also provided). The bundle_dir may be the same as report_dir.

    If delivery_dir is provided (C-1 delivery augmentation):
        - A delivery_event_id is generated before the classification loop.
        - Export payloads are written at schema_version v0.2.0 with the three
          new optional provenance fields (delivery_mechanism, delta_share_name,
          delivery_event_id) populated.
        - After the batch, a C-1 DeliveryEvent artifact is written to delivery_dir.
        - A Delta Share preparation manifest is written to delivery_dir.
        - The delivery_dir may be the same as bundle_dir or report_dir.
        - V1 export path behavior is fully preserved — this is additive augmentation.

    Returns a list of summary dicts (one per processed Silver record).
    """
    input_dir_path = Path(input_dir) if input_dir else None
    input_file_path = Path(input_file) if input_file else None
    output_dir_path = Path(output_dir)
    export_dir_path = Path(export_dir)
    bronze_dir_path = Path(bronze_dir) if bronze_dir else None
    report_dir_path = Path(report_dir) if report_dir else None
    bundle_dir_path = Path(bundle_dir) if bundle_dir else None
    delivery_dir_path = Path(delivery_dir) if delivery_dir else None
    run_id = pipeline_run_id or generate_pipeline_run_id()

    # C-1: Generate delivery_event_id before the loop so it can be embedded in
    # every export payload's provenance. This links each payload to its batch
    # delivery event. Only active when delivery_dir is provided.
    active_delivery_event_id: Optional[str] = None
    active_delivery_mechanism: Optional[str] = None
    active_delta_share_name: Optional[str] = None
    if delivery_dir_path is not None:
        from src.schemas.delivery_event import (
            DEFAULT_SHARE_NAME,
            DELIVERY_MECHANISM_DELTA_SHARING,
        )
        active_delivery_event_id = generate_delivery_event_id()
        active_delivery_mechanism = DELIVERY_MECHANISM_DELTA_SHARING
        active_delta_share_name = DEFAULT_SHARE_NAME
        print(
            f"[classify_gold] C-1 delivery active. "
            f"delivery_event_id={active_delivery_event_id} "
            f"share={active_delta_share_name}"
        )

    silver_paths = collect_silver_paths(input_dir_path, input_file_path)
    print(f"[classify_gold] Found {len(silver_paths)} Silver artifact(s). Run ID: {run_id}")

    summaries = []
    ineligible_count = 0

    for silver_path in silver_paths:
        silver = load_silver_artifact(silver_path)

        if not is_eligible_for_classification(silver):
            ineligible_count += 1
            print(
                f"[classify_gold] Skipping {silver_path.name} "
                f"(validation_status={silver.get('validation_status')})"
            )
            continue

        document_class_hint = silver.get("document_class_hint")
        classifier = select_classifier(document_class_hint)

        try:
            classification_result = classifier.classify(silver)
        except NotImplementedError as exc:
            print(f"[classify_gold] Classifier not available: {exc}", file=sys.stderr)
            classification_result = {
                "document_type_label": DOCUMENT_TYPE_UNKNOWN,
                "classification_confidence": 0.0,
            }
        except Exception as exc:  # noqa: BLE001
            print(f"[classify_gold] Classification error for {silver_path.name}: {exc}", file=sys.stderr)
            classification_result = {
                "document_type_label": DOCUMENT_TYPE_UNKNOWN,
                "classification_confidence": 0.0,
            }

        source_file, ingested_at = resolve_bronze_metadata(silver, bronze_dir_path)

        gold_record = assemble_gold_record(
            silver=silver,
            bronze_source_file=source_file,
            bronze_ingested_at=ingested_at,
            classification_result=classification_result,
            classifier_model_id=classifier.model_id,
            pipeline_run_id=run_id,
            delivery_event_id=active_delivery_event_id,
            delivery_mechanism=active_delivery_mechanism,
            delta_share_name=active_delta_share_name,
        )

        # B-3: Delegate all export/handoff materialization to the export_handoff module.
        # execute_export handles contract validation, quarantine shape assertion, and
        # artifact write. Returns ExportResult with the final export state.
        export_result = execute_export(gold_record, export_dir_path)

        # B-4: Derive explicit outcome category and reason code from the export result.
        # routing_label is read before model_copy since it is unchanged by the export.
        outcome_category, outcome_reason = derive_outcome(
            export_ready=export_result.export_ready,
            routing_label=gold_record.routing_label,
            contract_validation_errors=export_result.contract_validation_errors,
        )

        # Apply export result to the Gold record — written once with its final state.
        gold_record = gold_record.model_copy(update={
            "export_ready": export_result.export_ready,
            "export_path": export_result.export_path,
        })

        if export_result.contract_validation_errors:
            print(
                f"[classify_gold] CONTRACT BLOCK for {gold_record.document_id}: "
                "export payload failed B-1 validation — NOT written. "
                f"Errors: {export_result.contract_validation_errors}",
                file=sys.stderr,
            )

        export_artifact_path = export_result.export_artifact_path
        contract_validation_errors = export_result.contract_validation_errors

        # Write Gold artifact once with its final state (export_path and export_ready resolved).
        gold_artifact_path = write_gold_artifact(gold_record, output_dir_path)

        summary = {
            "document_id": gold_record.document_id,
            "bronze_record_id": gold_record.bronze_record_id,
            "extraction_id": gold_record.extraction_id,
            "gold_record_id": gold_record.gold_record_id,
            "document_type_label": gold_record.document_type_label,
            "routing_label": gold_record.routing_label,
            "classification_confidence": gold_record.classification_confidence,
            "export_ready": gold_record.export_ready,
            "pipeline_run_id": run_id,
            "gold_artifact_path": str(gold_artifact_path),
            "export_artifact_path": str(export_artifact_path) if export_artifact_path else None,
            "contract_validation_errors": contract_validation_errors,
            # B-4: explicit outcome vocabulary
            "outcome_category": outcome_category,
            "outcome_reason": outcome_reason,
        }
        summaries.append(summary)

        status_tag = "EXPORT-READY" if gold_record.export_ready else "quarantine"
        if contract_validation_errors:
            status_tag = "CONTRACT-BLOCKED"
        confidence_str = (
            f"{gold_record.classification_confidence:.2f}"
            if gold_record.classification_confidence is not None
            else "null"
        )
        print(
            f"[classify_gold] Gold artifact written → {gold_artifact_path} "
            f"(label={gold_record.document_type_label}, "
            f"routing={gold_record.routing_label}, "
            f"confidence={confidence_str}, "
            f"status={status_tag})"
        )
        if export_artifact_path:
            print(f"[classify_gold] Export artifact written → {export_artifact_path}")

    print(f"[classify_gold] Done. {len(summaries)} Gold artifact(s) written.")

    # B-4: Build and optionally write the handoff batch report.
    report_artifact_paths: Optional[dict] = None
    if report_dir_path is not None:
        batch_report = build_handoff_batch_report(
            summaries=summaries,
            pipeline_run_id=run_id,
            total_records_processed=len(silver_paths),
            total_ineligible_skipped=ineligible_count,
        )
        report_json_path, report_text_path = write_handoff_report(batch_report, report_dir_path)
        report_artifact_paths = {
            "json_path": str(report_json_path),
            "text_path": str(report_text_path),
        }
        print(f"[classify_gold] Handoff report (JSON) → {report_json_path}")
        print(f"[classify_gold] Handoff report (text) → {report_text_path}")

    # B-5: Build and optionally write the handoff batch manifest/review bundle.
    bundle_json_path: Optional[Path] = None
    if bundle_dir_path is not None:
        bundle_manifest = build_handoff_batch_manifest(
            summaries=summaries,
            pipeline_run_id=run_id,
            total_records_processed=len(silver_paths),
            total_ineligible_skipped=ineligible_count,
            report_artifact_paths=report_artifact_paths,
        )
        bundle_json_path, bundle_text_path = write_handoff_bundle(bundle_manifest, bundle_dir_path)
        print(f"[classify_gold] Handoff bundle (JSON) → {bundle_json_path}")
        print(f"[classify_gold] Handoff bundle (text) → {bundle_text_path}")

    # C-1: Write the delivery event and Delta Share preparation manifest.
    if delivery_dir_path is not None and active_delivery_event_id is not None:
        delivery_event = build_delivery_event(
            summaries=summaries,
            pipeline_run_id=run_id,
            delivery_event_id=active_delivery_event_id,
            bundle_artifact_path=str(bundle_json_path) if bundle_json_path else None,
            report_artifact_path=(
                report_artifact_paths["json_path"] if report_artifact_paths else None
            ),
            share_name=active_delta_share_name or "",
        )
        event_json_path, event_text_path = write_delivery_event(
            delivery_event, delivery_dir_path
        )
        print(f"[classify_gold] Delivery event (JSON) → {event_json_path}")
        print(f"[classify_gold] Delivery event (text) → {event_text_path}")

        # Write the Delta Share preparation manifest alongside the delivery event.
        share_manifest = compute_share_preparation_manifest(
            config=DeltaShareConfig(),
            pipeline_run_id=run_id,
            delivery_event_id=active_delivery_event_id,
        )
        share_manifest_path = write_share_manifest(share_manifest, delivery_dir_path)
        print(f"[classify_gold] Delta Share manifest → {share_manifest_path}")

    return summaries


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="classify_gold",
        description=(
            "Classify Silver JSON artifacts into Gold records with document type labels, "
            "routing labels, and export payloads. Processes FDA warning letters using "
            "local rule-based classification. Only records with validation_status != 'invalid' "
            "are processed."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        help="Directory containing Silver JSON artifacts to process.",
    )
    group.add_argument(
        "--input",
        metavar="FILE",
        help="Single Silver JSON artifact file to process.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory to write Gold JSON artifacts. Default: {DEFAULT_OUTPUT_DIR}",
    )
    p.add_argument(
        "--export-dir",
        default=DEFAULT_EXPORT_DIR,
        metavar="DIR",
        help=f"Directory to write export payload artifacts. Default: {DEFAULT_EXPORT_DIR}",
    )
    p.add_argument(
        "--bronze-dir",
        default="output/bronze",
        metavar="DIR",
        help="Directory containing Bronze JSON artifacts (for source_file and ingested_at lookup). "
             "Default: output/bronze",
    )
    p.add_argument(
        "--pipeline-run-id",
        default=None,
        metavar="RUN_ID",
        help="Optional pipeline run ID. Auto-generated if not provided.",
    )
    p.add_argument(
        "--report-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory to write B-4 handoff batch report artifacts "
            "(JSON + text). If omitted, no report is written."
        ),
    )
    p.add_argument(
        "--bundle-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory to write B-5 handoff batch manifest/review bundle artifacts "
            "(JSON + text). If omitted, no bundle is written. May be the same as "
            "--report-dir. When both are provided, the bundle references the report."
        ),
    )
    p.add_argument(
        "--delivery-dir",
        default=None,
        metavar="DIR",
        help=(
            "C-1: Directory to write delivery event artifacts (JSON + text) and the "
            "Delta Share preparation manifest. When provided, activates the C-1 delivery "
            "augmentation layer: export payloads are written at schema_version v0.2.0 "
            "with delivery provenance fields populated. If omitted, V1 export behavior "
            "is preserved (schema_version v0.1.0, no delivery event). "
            "May be combined with --bundle-dir and --report-dir."
        ),
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_classify_gold(
        input_dir=args.input_dir,
        input_file=args.input,
        output_dir=args.output_dir,
        export_dir=args.export_dir,
        bronze_dir=args.bronze_dir,
        pipeline_run_id=args.pipeline_run_id,
        report_dir=args.report_dir,
        bundle_dir=args.bundle_dir,
        delivery_dir=args.delivery_dir,
    )


if __name__ == "__main__":
    main()
