"""
ingest_bronze.py — Bronze ingestion and parsing pipeline (Phase A-1)

Entry point for turning a local source file into a Bronze JSON artifact.
Designed to run locally without any Databricks workspace connection.
The parser abstraction and record structure are compatible with Databricks
ai_parse_document when that integration is added for cluster execution.

Usage:
    python src/pipelines/ingest_bronze.py --input examples/fda_warning_letter_sample.md
    python src/pipelines/ingest_bronze.py --input examples/fda_warning_letter_sample.md \\
        --output-dir output/bronze \\
        --document-class-hint fda_warning_letter \\
        --source-system local_dev

Outputs:
    output/bronze/<document_id>.json  — Bronze record JSON artifact

Authoritative contract: docs/data-contracts.md § Bronze: Parse Output Contract
Architecture context: ARCHITECTURE.md § Bronze Layer
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running from the repo root without installing as a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.bronze_schema import BronzeRecord, ParseStatus, ALLOWED_EXTENSIONS, SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = "output/bronze"
LOCAL_PIPELINE_RUN_PREFIX = "local-run"


# ---------------------------------------------------------------------------
# File validation
# ---------------------------------------------------------------------------

def validate_input_file(file_path: Path) -> None:
    """Raise ValueError with a descriptive message if the file is not usable."""
    if not file_path.exists():
        raise ValueError(f"Input file does not exist: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"Input path is not a file: {file_path}")
    ext = file_path.suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"File extension '{ext}' is not supported. Allowed: {sorted(ALLOWED_EXTENSIONS)}"
        )


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_file_metadata(file_path: Path) -> dict:
    """
    Return source document provenance fields.
    These are captured before any parsing occurs.
    """
    stat = file_path.stat()
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type is None:
        mime_type = "application/octet-stream"

    return {
        "file_name": file_path.name,
        "file_extension": file_path.suffix.lower(),
        "file_size_bytes": stat.st_size,
        "mime_type": mime_type,
        "ingested_at": datetime.now(tz=timezone.utc),
    }


def compute_sha256(file_path: Path) -> str:
    """Return the SHA-256 hex digest of the file contents."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Parser abstraction
# ---------------------------------------------------------------------------

class DocumentParser(ABC):
    """
    Strategy interface for document parsers.

    Implementations return a dict with:
        - parsed_text: str or None
        - page_count: int or None
        - parse_model: str
    """

    @abstractmethod
    def parse(self, file_path: Path) -> dict:
        ...

    @property
    @abstractmethod
    def model_id(self) -> str:
        ...


class LocalTextParser(DocumentParser):
    """
    Read .txt and .md files directly as plain text.
    No external dependencies. Suitable for local demonstration and testing.
    """

    _MODEL_ID = "local_text_parser/v1"

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def parse(self, file_path: Path) -> dict:
        with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        return {
            "parsed_text": text,
            "page_count": None,   # plain text has no page structure
            "parse_model": self.model_id,
        }


class DatabricksAiParseAdapter(DocumentParser):
    """
    Adapter placeholder for Databricks ai_parse_document.

    In a live Databricks execution environment this class would call:
        spark.sql("SELECT ai_parse_document('/Volumes/...')")

    It is intentionally not implemented here because:
    1. There is no Spark session available in a local run.
    2. No credentials or workspace URLs should live in this file.

    To enable Databricks execution, subclass this and inject a SparkSession.
    """

    _MODEL_ID = "ai_parse_document/v1"

    @property
    def model_id(self) -> str:
        return self._MODEL_ID

    def parse(self, file_path: Path) -> dict:
        raise NotImplementedError(
            "DatabricksAiParseAdapter requires a live Databricks runtime. "
            "Use LocalTextParser for local execution, or inject a SparkSession "
            "and override this method for Databricks cluster execution."
        )


def select_parser(file_path: Path) -> DocumentParser:
    """
    Choose the appropriate parser for the given file extension.

    Routing:
        .txt / .md  → LocalTextParser (works locally, no dependencies)
        .pdf / .docx → DatabricksAiParseAdapter (requires Databricks runtime)

    Extend this function when adding new parsers.
    """
    ext = file_path.suffix.lower()
    if ext in (".txt", ".md"):
        return LocalTextParser()
    # PDF and DOCX require Databricks ai_parse_document for production.
    # Return the adapter; it will raise a clear error if called without a runtime.
    return DatabricksAiParseAdapter()


# ---------------------------------------------------------------------------
# Pipeline run ID
# ---------------------------------------------------------------------------

def generate_pipeline_run_id() -> str:
    """
    Generate a local pipeline run ID for traceability.
    Format: local-run-<uuid4>

    In Databricks execution, this would be replaced with the MLflow run ID
    obtained from mlflow.start_run() or passed in as a parameter.
    """
    return f"{LOCAL_PIPELINE_RUN_PREFIX}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Bronze record assembly
# ---------------------------------------------------------------------------

def build_bronze_record(
    file_path: Path,
    parse_result: Optional[dict],
    parse_error: Optional[str],
    pipeline_run_id: str,
    document_class_hint: Optional[str] = None,
    source_system: Optional[str] = None,
    ingested_by: Optional[str] = None,
) -> BronzeRecord:
    """
    Assemble a complete BronzeRecord from file metadata and parser output.

    Both the success path (parse_result populated) and the failure path
    (parse_error populated) produce a valid BronzeRecord — no silent drops.
    """
    document_id = str(uuid.uuid4())
    bronze_record_id = str(uuid.uuid4())
    metadata = extract_file_metadata(file_path)
    file_hash = compute_sha256(file_path)
    now = datetime.now(tz=timezone.utc)

    if parse_error is not None:
        return BronzeRecord(
            document_id=document_id,
            bronze_record_id=bronze_record_id,
            source_path=str(file_path.resolve()),
            file_hash=file_hash,
            pipeline_run_id=pipeline_run_id,
            parse_status=ParseStatus.failed,
            parse_failure_reason=parse_error,
            parsed_text=None,
            page_count=None,
            char_count=None,
            parse_model="unknown",
            parsed_at=now,
            schema_version=SCHEMA_VERSION,
            document_class_hint=document_class_hint,
            source_system=source_system,
            ingested_by=ingested_by,
            **metadata,
        )

    parsed_text: str = parse_result["parsed_text"] or ""
    char_count = len(parsed_text)

    parse_status = ParseStatus.success
    if char_count == 0:
        parse_status = ParseStatus.partial

    return BronzeRecord(
        document_id=document_id,
        bronze_record_id=bronze_record_id,
        source_path=str(file_path.resolve()),
        file_hash=file_hash,
        pipeline_run_id=pipeline_run_id,
        parse_status=parse_status,
        parse_failure_reason=None,
        parsed_text=parsed_text,
        page_count=parse_result.get("page_count"),
        char_count=char_count,
        parse_model=parse_result["parse_model"],
        parsed_at=now,
        schema_version=SCHEMA_VERSION,
        document_class_hint=document_class_hint,
        source_system=source_system,
        ingested_by=ingested_by,
        **metadata,
    )


# ---------------------------------------------------------------------------
# Artifact writer
# ---------------------------------------------------------------------------

def write_bronze_artifact(record: BronzeRecord, output_dir: Path) -> Path:
    """
    Write a Bronze record as a JSON artifact.
    Each call creates a new file named <bronze_record_id>.json.
    Append-only: existing artifacts are never modified.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / f"{record.bronze_record_id}.json"
    artifact_path.write_text(record.to_json_str(), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# Top-level pipeline function
# ---------------------------------------------------------------------------

def run_ingest_bronze(
    input_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    document_class_hint: Optional[str] = None,
    source_system: Optional[str] = None,
    ingested_by: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
) -> dict:
    """
    Run the Bronze ingestion pipeline for a single file.

    Returns a summary dict with the artifact path and record metadata.
    Raises no exceptions on parse failure — failures produce a Bronze record
    with parse_status='failed' and are written to the output directory.
    """
    file_path = Path(input_path)
    run_id = pipeline_run_id or generate_pipeline_run_id()

    # Validate file before attempting any parse
    try:
        validate_input_file(file_path)
    except ValueError as exc:
        print(f"[ingest_bronze] File validation failed: {exc}", file=sys.stderr)
        raise

    # Select and run parser
    parser = select_parser(file_path)
    parse_result = None
    parse_error = None

    try:
        parse_result = parser.parse(file_path)
        print(
            f"[ingest_bronze] Parse complete — "
            f"parser={parser.model_id}, "
            f"chars={len(parse_result.get('parsed_text') or '')}"
        )
    except NotImplementedError as exc:
        parse_error = str(exc)
        print(f"[ingest_bronze] Parser not available locally: {exc}", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001
        parse_error = f"Unexpected parser error: {exc}"
        print(f"[ingest_bronze] Parse error: {exc}", file=sys.stderr)

    # Assemble and write Bronze record (always — success or failure)
    record = build_bronze_record(
        file_path=file_path,
        parse_result=parse_result,
        parse_error=parse_error,
        pipeline_run_id=run_id,
        document_class_hint=document_class_hint,
        source_system=source_system,
        ingested_by=ingested_by,
    )

    artifact_path = write_bronze_artifact(record, Path(output_dir))

    summary = {
        "document_id": record.document_id,
        "bronze_record_id": record.bronze_record_id,
        "parse_status": record.parse_status.value,
        "char_count": record.char_count,
        "pipeline_run_id": run_id,
        "artifact_path": str(artifact_path),
    }

    print(f"[ingest_bronze] Bronze artifact written → {artifact_path}")
    print(f"[ingest_bronze] Summary: {json.dumps(summary, indent=2)}")
    return summary


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ingest_bronze",
        description=(
            "Ingest a single document file and produce a Bronze JSON artifact. "
            "Supports .txt and .md locally; .pdf and .docx require Databricks runtime."
        ),
    )
    p.add_argument(
        "--input",
        required=True,
        metavar="FILE",
        help="Path to the source document file.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory to write the Bronze JSON artifact. Default: {DEFAULT_OUTPUT_DIR}",
    )
    p.add_argument(
        "--document-class-hint",
        default=None,
        metavar="CLASS",
        help="Optional document class hint (e.g., fda_warning_letter).",
    )
    p.add_argument(
        "--source-system",
        default=None,
        metavar="SYSTEM",
        help="Optional source system label (e.g., local_dev, fda_portal).",
    )
    p.add_argument(
        "--ingested-by",
        default=None,
        metavar="PRINCIPAL",
        help="Optional identity of the operator or job that triggered ingest.",
    )
    p.add_argument(
        "--pipeline-run-id",
        default=None,
        metavar="RUN_ID",
        help="Optional pipeline run ID. Auto-generated if not provided.",
    )
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_ingest_bronze(
        input_path=args.input,
        output_dir=args.output_dir,
        document_class_hint=args.document_class_hint,
        source_system=args.source_system,
        ingested_by=args.ingested_by,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    main()
