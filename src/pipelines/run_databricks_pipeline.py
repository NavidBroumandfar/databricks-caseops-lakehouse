"""Databricks runtime orchestration for bronze/silver/gold pipeline stages.

This runner is intentionally workspace-first and metadata-safe:
- it consumes a Spark session provided by the Databricks runtime,
- it writes intermediate/final records through DeltaTableIO,
- it uses runtime adapters for ai_parse_document / ai_extract / ai_classify,
- and it never stores credentials, URLs, or tokens.

Supported stages:
- bronze: parse Volume input files -> write Bronze Delta table
- silver: read Bronze -> extract fields -> write Silver Delta table
- gold: read Silver -> classify -> write Gold Delta table
- all: run bronze -> silver -> gold with one shared pipeline run ID
- delivery_validation: validate delivery artifacts via existing local validator
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

# Support direct execution as documented and Databricks Git-source Python tasks,
# where __file__ may be unavailable.
def _resolve_repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path.cwd()


_REPO_ROOT = _resolve_repo_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.pipelines.databricks_runtime import (
    AI_EXTRACT_VERSION,
    ALLOWED_WRITE_MODES,
    DeltaTableIO,
    DeltaTableTargets,
    DatabricksRuntimeError,
    databricks_ai_extract_schema,
)
from src.pipelines.databricks_runtime import validate_table_name
from src.utils.environment_config import get_environment_config
from src.schemas.bronze_schema import BronzeRecord, ParseStatus
from src.schemas.bedrock_contract import validate_export_payload
from src.schemas.domain_schema_registry import build_fields_for_domain
from src.pipelines.ingest_bronze import DatabricksAiParseAdapter
from src.pipelines.extract_silver import (
    DatabricksAiExtractAdapter,
    assemble_silver_record,
    is_eligible_for_extraction,
)
from src.pipelines.classify_gold import (
    DatabricksAiClassifyAdapter,
    assemble_gold_record,
    is_eligible_for_classification,
)
from src.pipelines.delivery_validation import (
    WORKSPACE_MODE_LOCAL_REPO_ONLY,
    WORKSPACE_MODE_PERSONAL_DATABRICKS,
    validate_delivery_layer,
    write_validation_result,
)
from src.utils.domain_registry import DomainNotFoundError, DomainNotImplementedError, require_active_domain
from src.utils.extraction_prompts import get_prompt_for_domain


@dataclass(frozen=True)
class RuntimeFileMetadata:
    """Metadata extracted from Databricks volume binaryFile scan."""

    path: str
    file_size_bytes: int
    file_hash: Optional[str] = None


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict()
    if isinstance(row, dict):
        return dict(row)
    raise TypeError(f"Expected Spark row or mapping, got {type(row)!r}")


def _ensure_workspace_spark():
    """Resolve the active Spark session in Databricks, or raise a local-safe error."""
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # pragma: no cover - depends on local runtime
        raise DatabricksRuntimeError(
            "PySpark is not available. Run this entrypoint in Databricks Runtime "
            "with a live Spark session."
        ) from exc

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise DatabricksRuntimeError(
            "No active Spark session found. Submit this as a Databricks job / notebook "
            "step where SparkSession is active."
        )
    return spark


def _safe_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _ensure_raw_input_available(spark, input_paths: list[str], fallback_input: str) -> None:
    """Validate that bronze raw input can be discovered in this runtime."""
    _resolve_runtime_paths(
        spark,
        requested_inputs=input_paths,
        fallback_input=fallback_input,
    )


def _resolve_runtime_paths(
    spark,
    requested_inputs: list[str],
    fallback_input: str,
    require_file_hash: bool = False,
) -> tuple[list[str], dict[str, RuntimeFileMetadata]]:
    """Resolve input file paths from one or more Volume/glob values."""
    normalized_inputs = list(requested_inputs) if requested_inputs else [fallback_input]
    if not normalized_inputs:
        raise ValueError("No input paths provided and no fallback raw-volume path is configured.")

    try:
        binary_files = spark.read.format("binaryFile").load(*normalized_inputs)
        try:
            rows = binary_files.selectExpr(
                "path",
                "length",
                "sha2(content, 256) AS file_hash",
            ).collect()
        except AttributeError:
            rows = binary_files.select("path", "length", "content").collect()
        except Exception:
            rows = binary_files.select("path", "length", "content").collect()
    except Exception as exc:
        raise DatabricksRuntimeError(
            "Unable to resolve input paths in Databricks runtime. "
            "Verify CASEOPS_ENV raw_volume_path and job permissions to the Volume paths."
        ) from exc

    if not rows:
        raise ValueError(
            "No readable files were found for stage bronze input: "
            f"{normalized_inputs}."
        )

    files: list[str] = []
    metadata: dict[str, RuntimeFileMetadata] = {}
    for row in rows:
        payload = _row_to_dict(row)
        path = _safe_string(payload.get("path")).strip()
        if not path:
            continue
        size = payload.get("length")
        try:
            file_size = int(size)
        except (TypeError, ValueError):
            file_size = 0
        if file_size <= 0:
            # Keep records schema-valid; path-derived files must be non-empty.
            file_size = 1
        file_hash = _safe_string(payload.get("file_hash")).strip().lower() or None
        if file_hash is None and payload.get("content") is not None:
            file_hash = _sha256_content(payload["content"])
        if file_hash is not None and (
            len(file_hash) != 64 or any(c not in "0123456789abcdef" for c in file_hash)
        ):
            raise DatabricksRuntimeError(
                f"binaryFile scan returned an invalid SHA-256 file_hash for {path}."
            )
        if require_file_hash and file_hash is None:
            raise DatabricksRuntimeError(
                "Unable to compute raw-byte SHA-256 file_hash for runtime Bronze input. "
                "The Databricks binaryFile scan must expose either sha2(content, 256) "
                "or raw content bytes."
            )
        files.append(path)
        metadata[path] = RuntimeFileMetadata(
            path=path,
            file_size_bytes=file_size,
            file_hash=file_hash,
        )

    if not files:
        raise ValueError("Input discovery returned no usable file paths.")

    # De-duplicate deterministically for reproducible per-run summaries.
    deduped = sorted(set(files))
    return deduped, metadata


def _volume_metadata_for_path(
    metadata: dict[str, RuntimeFileMetadata],
    path: str,
) -> RuntimeFileMetadata:
    return metadata.get(path, RuntimeFileMetadata(path=path, file_size_bytes=1))


def _sha256_content(content: Any) -> str:
    """Compute SHA-256 from binaryFile content values returned by Spark."""
    if isinstance(content, memoryview):
        data = content.tobytes()
    elif isinstance(content, bytearray):
        data = bytes(content)
    elif isinstance(content, bytes):
        data = content
    else:
        data = bytes(content)
    return hashlib.sha256(data).hexdigest()


def _build_bronze_record(
    *,
    file_path: str,
    parse_result: Optional[dict[str, Any]],
    parse_error: Optional[str],
    parse_model: str,
    pipeline_run_id: str,
    metadata: RuntimeFileMetadata,
    document_class_hint: Optional[str],
    source_system: Optional[str],
    ingested_by: Optional[str],
) -> dict[str, Any]:
    if parse_error is not None:
        parsed_text: Optional[str] = None
        parse_status = ParseStatus.failed
        parse_failure_reason = parse_error
        char_count = None
    else:
        parsed_text = _safe_string(parse_result.get("parsed_text") if parse_result else "")
        char_count = len(parsed_text)
        parse_status = ParseStatus.success if char_count else ParseStatus.partial
        parse_failure_reason = None

    now = datetime.now(tz=timezone.utc)
    path_obj = Path(file_path)
    extension = path_obj.suffix.lower()
    if extension == ".":
        extension = ""
    if metadata.file_hash is None:
        raise DatabricksRuntimeError(
            f"Missing raw-byte SHA-256 file_hash for runtime Bronze input {file_path}."
        )

    record = BronzeRecord(
        document_id=str(uuid4()),
        bronze_record_id=str(uuid4()),
        source_path=file_path,
        file_name=path_obj.name,
        file_extension=extension,
        file_size_bytes=max(1, int(metadata.file_size_bytes)),
        file_hash=metadata.file_hash,
        mime_type="application/pdf" if extension == ".pdf" else "application/octet-stream",
        ingested_at=now,
        parsed_at=now,
        parse_status=parse_status,
        parse_failure_reason=parse_failure_reason,
        parsed_text=parsed_text,
        page_count=(parse_result or {}).get("page_count") if parse_error is None else None,
        char_count=char_count,
        parse_model=parse_model,
        pipeline_run_id=pipeline_run_id,
        document_class_hint=document_class_hint,
        source_system=source_system,
        ingested_by=ingested_by,
    )
    return record.to_json_dict()


def _build_extract_adapter(spark, domain_key: str, version: str) -> DatabricksAiExtractAdapter:
    require_active_domain(domain_key, operation="runtime extraction")
    prompt = get_prompt_for_domain(domain_key)
    fields_model = build_fields_for_domain(domain_key, {})
    extraction_schema = databricks_ai_extract_schema(fields_model.model_json_schema())
    return DatabricksAiExtractAdapter(
        spark=spark,
        extraction_schema=extraction_schema,
        instructions=prompt.description,
        version=version,
    )


def _sort_rows(rows: list[dict[str, Any]], *sort_fields: str) -> list[dict[str, Any]]:
    """Sort records deterministically for stable stage outputs during local and workspace runs."""
    return sorted(rows, key=lambda row: tuple(_safe_string(row.get(field)) for field in sort_fields))


def _run_bronze_stage(
    spark,
    targets: DeltaTableTargets,
    input_paths: list[str],
    fallback_input: str,
    write_mode: str,
    pipeline_run_id: str,
    document_class_hint: Optional[str],
    source_system: Optional[str],
    ingested_by: Optional[str],
    parse_version: str,
) -> list[dict[str, Any]]:
    runtime_file_paths, metadata_map = _resolve_runtime_paths(
        spark,
        requested_inputs=input_paths,
        fallback_input=fallback_input,
        require_file_hash=True,
    )

    parser = DatabricksAiParseAdapter(spark=spark, version=parse_version)
    io = DeltaTableIO(spark)
    parsed_rows = []
    for file_path in runtime_file_paths:
        metadata = _volume_metadata_for_path(metadata_map, file_path)
        try:
            parse_result = parser.parse(file_path)
            parse_error = None
        except Exception as exc:  # noqa: BLE001
            parse_error = f"{type(exc).__name__}: {exc}"
            parse_result = None

        parsed_rows.append(
            _build_bronze_record(
                file_path=file_path,
                parse_result=parse_result,
                parse_error=parse_error,
                parse_model=parser.model_id,
                pipeline_run_id=pipeline_run_id,
                metadata=metadata,
                document_class_hint=document_class_hint,
                source_system=source_system,
                ingested_by=ingested_by,
            )
        )

    io.write_records(records=parsed_rows, table_name=targets.bronze_table, mode=write_mode)
    return parsed_rows


def _run_silver_stage(
    spark,
    targets: DeltaTableTargets,
    write_mode: str,
    pipeline_run_id: str,
    source_pipeline_run_id: str,
    extract_version: str,
) -> list[dict[str, Any]]:
    io = DeltaTableIO(spark)
    bronze_rows = _sort_rows(
        io.read_table(
            targets.bronze_table,
            where_equals={"pipeline_run_id": source_pipeline_run_id},
        ),
        "bronze_record_id",
        "source_path",
        "document_id",
        "file_name",
    )

    extracted_rows: list[dict[str, Any]] = []
    for bronze_row in bronze_rows:
        bronze = dict(bronze_row)
        if not is_eligible_for_extraction(bronze):
            continue

        domain_key = bronze.get("document_class_hint") or "fda_warning_letter"
        parsed_text = _safe_string(bronze.get("parsed_text"))

        extraction_model = DatabricksAiExtractAdapter._MODEL_ID
        extracted_raw: dict[str, Any] = {}

        try:
            adapter = _build_extract_adapter(
                spark=spark,
                domain_key=domain_key,
                version=extract_version,
            )
            extraction_model = adapter.model_id
            extracted_raw = adapter.extract(parsed_text)
            if not isinstance(extracted_raw, dict):
                extracted_raw = {}
        except Exception:
            # Runtime path keeps records and records a failure in validation status.
            extracted_raw = {}

        silver = assemble_silver_record(
            bronze=bronze,
            extracted_raw=extracted_raw,
            extraction_model=extraction_model,
            pipeline_run_id=pipeline_run_id,
            domain_key=domain_key,
        )
        extracted_rows.append(silver.to_json_dict())

    io.write_records(records=extracted_rows, table_name=targets.silver_table, mode=write_mode)
    return extracted_rows


def _run_gold_stage(
    spark,
    targets: DeltaTableTargets,
    write_mode: str,
    pipeline_run_id: str,
    source_pipeline_run_id: str,
    classify_version: str,
) -> list[dict[str, Any]]:
    io = DeltaTableIO(spark)
    silver_rows = _sort_rows(
        io.read_table(
            targets.silver_table,
            where_equals={"pipeline_run_id": source_pipeline_run_id},
        ),
        "bronze_record_id",
        "silver_record_id",
        "document_id",
    )
    bronze_rows = io.read_table(
        targets.bronze_table,
        where_equals={"pipeline_run_id": source_pipeline_run_id},
    )
    bronze_by_record = {
        _row_to_dict(row).get("bronze_record_id"): _row_to_dict(row) for row in bronze_rows
    }

    classifier = DatabricksAiClassifyAdapter(spark=spark, version=classify_version)
    gold_rows: list[dict[str, Any]] = []

    for silver_row in silver_rows:
        silver = dict(silver_row)
        if not is_eligible_for_classification(silver):
            continue

        try:
            classification_result = classifier.classify(silver)
        except Exception:  # noqa: BLE001
            classification_result = {
                "document_type_label": "unknown",
                "classification_confidence": 0.0,
            }

        bronze_row = bronze_by_record.get(silver.get("bronze_record_id") or "")
        source_file = "unknown"
        ingested_at = ""
        if bronze_row:
            source_file = bronze_row.get("file_name") or bronze_row.get("source_path") or "unknown"
            ingested_at = _safe_string(bronze_row.get("ingested_at"))

        gold = assemble_gold_record(
            silver=silver,
            bronze_source_file=source_file,
            bronze_ingested_at=ingested_at,
            classification_result=classification_result,
            classifier_model_id=classifier.model_id,
            pipeline_run_id=pipeline_run_id,
        )

        # Keep in sync with B-1/B-3 local behavior: invalid payloads are quarantine-ready false.
        contract_result = validate_export_payload(gold.export_payload.model_dump())
        if not contract_result.valid:
            gold = gold.model_copy(update={"export_ready": False})

        gold_rows.append(gold.to_json_dict())

    io.write_records(records=gold_rows, table_name=targets.gold_table, mode=write_mode)
    return gold_rows


def _assert_valid_table_names(targets: DeltaTableTargets) -> None:
    validate_table_name(targets.bronze_table)
    validate_table_name(targets.silver_table)
    validate_table_name(targets.gold_table)


def _ensure_source_table_exists(spark, table_name: str) -> None:
    try:
        spark.table(table_name).limit(1).collect()
    except Exception as exc:  # noqa: BLE001
        raise DatabricksRuntimeError(
            f"Source table {table_name} is not readable in this runtime. "
            "Confirm the schema/table exists before running this stage."
        ) from exc


def _ensure_output_table_writable(spark, table_name: str, write_mode: str) -> None:
    """Best-effort validate write permission with a no-op operation."""
    if write_mode == "overwrite" and not spark.catalog.tableExists(table_name):
        # Overwrite can create the target table when absent; creation permissions are
        # environment-specific and not validated here.
        return

    if not spark.catalog.tableExists(table_name):
        raise DatabricksRuntimeError(
            f"Output table {table_name} does not exist for write mode {write_mode!r}. "
            "Create it first or use --output-mode overwrite."
        )

    probe_sql = f"INSERT INTO {table_name} SELECT * FROM {table_name} WHERE 1 = 0"
    try:
        spark.sql(probe_sql)
    except Exception as exc:  # noqa: BLE001
        raise DatabricksRuntimeError(
            f"Output table {table_name} is not writable for mode {write_mode!r}. "
            "Verify table write permissions for the runtime principal."
        ) from exc


def _run_delivery_validation_stage(args: argparse.Namespace) -> None:
    result = validate_delivery_layer(
        pipeline_run_id=args.delivery_pipeline_run_id,
        delivery_event_path=Path(args.delivery_event_path) if args.delivery_event_path else None,
        share_manifest_path=Path(args.share_manifest_path) if args.share_manifest_path else None,
        runtime_evidence_path=Path(args.runtime_evidence_path)
        if args.runtime_evidence_path
        else None,
        workspace_mode=args.workspace_mode,
    )
    if args.validation_output_dir:
        output_dir = Path(args.validation_output_dir)
        validation_json_path, validation_text_path = write_validation_result(result, output_dir)
        print(f"Delivery validation artifact: {validation_json_path}")
        print(f"Delivery validation text artifact: {validation_text_path}")
        if result.validation_run_id != result.pipeline_run_id:
            pipeline_json_path, pipeline_text_path = write_validation_result(
                result,
                output_dir,
                artifact_id=result.pipeline_run_id,
            )
            print(f"Pipeline-run delivery validation artifact: {pipeline_json_path}")
            print(f"Pipeline-run delivery validation text artifact: {pipeline_text_path}")
    print(f"Delivery validation status: {result.validation_status}")
    print(f"Validation reason: {result.validation_reason}")


def _validate_preflight_requirements(
    spark,
    stage: str,
    targets: DeltaTableTargets,
    input_paths: list[str],
    fallback_input: str,
    write_mode: str,
    check_output_permissions: bool = False,
) -> None:
    """Fail fast if required runtime resources are unavailable."""
    if stage == "bronze":
        _ensure_raw_input_available(spark, input_paths=input_paths, fallback_input=fallback_input)
        if check_output_permissions:
            _ensure_output_table_writable(spark, targets.bronze_table, write_mode)
        return
    if stage == "all":
        _ensure_raw_input_available(spark, input_paths=input_paths, fallback_input=fallback_input)
        if check_output_permissions:
            _ensure_output_table_writable(spark, targets.bronze_table, write_mode)
            _ensure_output_table_writable(spark, targets.silver_table, write_mode)
            _ensure_output_table_writable(spark, targets.gold_table, write_mode)
        return
    if stage == "silver":
        _ensure_source_table_exists(spark, targets.bronze_table)
        if check_output_permissions:
            _ensure_output_table_writable(spark, targets.silver_table, write_mode)
    if stage == "gold":
        _ensure_source_table_exists(spark, targets.silver_table)
        if check_output_permissions:
            _ensure_output_table_writable(spark, targets.gold_table, write_mode)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Databricks runtime bronze/silver/gold stages with runtime adapters "
            "and Delta table I/O."
        ),
    )

    parser.add_argument(
        "--stage",
        required=True,
        choices=["bronze", "silver", "gold", "all", "delivery_validation"],
        help="Pipeline stage to execute.",
    )
    parser.add_argument(
        "--environment",
        default=None,
        help="Optional explicit CASEOPS environment (dev, staging, prod). Falls back to CASEOPS_ENV or dev.",
    )
    parser.add_argument(
        "--pipeline-run-id",
        default=None,
        help="Optional run identifier. Auto-generated if omitted.",
    )
    parser.add_argument(
        "--source-pipeline-run-id",
        default=None,
        help=(
            "Upstream batch/run ID to read from Bronze/Silver tables for silver/gold stages. "
            "Defaults to --pipeline-run-id. Use the same run ID across separated runtime jobs."
        ),
    )
    parser.add_argument(
        "--input",
        action="append",
        default=None,
        help=(
            "One or more input files/directories for bronze parse. "
            "If omitted, fallback to raw_volume_path from environment config."
        ),
    )
    parser.add_argument(
        "--document-class-hint",
        default=None,
        help="Optional document_class_hint for bronze parse stage.",
    )
    parser.add_argument(
        "--source-system",
        default=None,
        help="Optional source_system tag for bronze stage provenance.",
    )
    parser.add_argument(
        "--ingested-by",
        default=None,
        help="Optional ingested_by tag for bronze stage provenance.",
    )
    parser.add_argument(
        "--output-mode",
        default="append",
        choices=sorted(ALLOWED_WRITE_MODES),
        help="Delta write mode for bronze/silver/gold stages.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate preflight requirements and exit without running writes.",
    )
    parser.add_argument(
        "--check-write-permissions",
        action="store_true",
        help="Also validate output-table write permissions during preflight checks.",
    )
    parser.add_argument(
        "--parse-version",
        default="2.0",
        help="ai_parse_document version parameter for runtime adapter.",
    )
    parser.add_argument(
        "--extract-version",
        default=AI_EXTRACT_VERSION,
        help="ai_extract version parameter for runtime adapter.",
    )
    parser.add_argument(
        "--classify-version",
        default="2.0",
        help="ai_classify version parameter for runtime adapter.",
    )

    parser.add_argument(
        "--delivery-pipeline-run-id",
        default=None,
        help="Pipeline run ID for delivery_validation stage (if different from --pipeline-run-id).",
    )
    parser.add_argument(
        "--delivery-event-path",
        default=None,
        help="Delivery event JSON path for delivery_validation stage.",
    )
    parser.add_argument(
        "--share-manifest-path",
        default=None,
        help="Delta share manifest JSON path for delivery_validation stage.",
    )
    parser.add_argument(
        "--runtime-evidence-path",
        default=None,
        help="Sanitized runtime evidence JSON for personal_databricks validation.",
    )
    parser.add_argument(
        "--workspace-mode",
        default=WORKSPACE_MODE_LOCAL_REPO_ONLY,
        choices=[WORKSPACE_MODE_LOCAL_REPO_ONLY, WORKSPACE_MODE_PERSONAL_DATABRICKS],
        help="Validation workspace context for delivery_validation stage.",
    )
    parser.add_argument(
        "--validation-output-dir",
        default="output/validation",
        help="Optional output dir for delivery validation JSON/text artifacts.",
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    environment_name = args.environment or None
    env_config = get_environment_config(environment_name)
    targets = DeltaTableTargets.from_environment_config(env_config)
    _assert_valid_table_names(targets)

    pipeline_run_id = args.pipeline_run_id
    if not pipeline_run_id:
        pipeline_run_id = f"local-run-{uuid4()}"
    source_pipeline_run_id = args.source_pipeline_run_id or pipeline_run_id

    if args.stage == "delivery_validation":
        validation_run_id = args.delivery_pipeline_run_id or pipeline_run_id
        args.delivery_pipeline_run_id = validation_run_id
        _run_delivery_validation_stage(args)
        return

    spark = _ensure_workspace_spark()
    _validate_preflight_requirements(
        spark=spark,
        stage=args.stage,
        targets=targets,
        input_paths=args.input or [],
        fallback_input=env_config.raw_volume_path,
        write_mode=args.output_mode,
        check_output_permissions=args.check_write_permissions,
    )
    if args.check_only:
        print(f"Preflight checks passed for stage={args.stage}, environment={env_config.env_name}")
        return

    if args.stage in {"bronze", "silver", "gold", "all"}:
        if args.output_mode not in ALLOWED_WRITE_MODES:
            raise ValueError(f"Unsupported output mode: {args.output_mode}")

    if args.stage == "bronze":
        records = _run_bronze_stage(
            spark=spark,
            targets=targets,
            input_paths=args.input or [],
            fallback_input=env_config.raw_volume_path,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            document_class_hint=args.document_class_hint,
            source_system=args.source_system,
            ingested_by=args.ingested_by,
            parse_version=args.parse_version,
        )
        print(f"Bronze stage completed: {len(records)} row(s) written to {targets.bronze_table}")
        return

    if args.stage == "silver":
        records = _run_silver_stage(
            spark=spark,
            targets=targets,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            source_pipeline_run_id=source_pipeline_run_id,
            extract_version=args.extract_version,
        )
        print(f"Silver stage completed: {len(records)} row(s) written to {targets.silver_table}")
        return

    if args.stage == "gold":
        records = _run_gold_stage(
            spark=spark,
            targets=targets,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            source_pipeline_run_id=source_pipeline_run_id,
            classify_version=args.classify_version,
        )
        print(f"Gold stage completed: {len(records)} row(s) written to {targets.gold_table}")
        return

    if args.stage == "all":
        bronze_records = _run_bronze_stage(
            spark=spark,
            targets=targets,
            input_paths=args.input or [],
            fallback_input=env_config.raw_volume_path,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            document_class_hint=args.document_class_hint,
            source_system=args.source_system,
            ingested_by=args.ingested_by,
            parse_version=args.parse_version,
        )
        silver_records = _run_silver_stage(
            spark=spark,
            targets=targets,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            source_pipeline_run_id=pipeline_run_id,
            extract_version=args.extract_version,
        )
        gold_records = _run_gold_stage(
            spark=spark,
            targets=targets,
            write_mode=args.output_mode,
            pipeline_run_id=pipeline_run_id,
            source_pipeline_run_id=pipeline_run_id,
            classify_version=args.classify_version,
        )

        print(
            "All-stage completed: "
            f"bronze={len(bronze_records)} silver={len(silver_records)} gold={len(gold_records)}"
        )


if __name__ == "__main__":
    main()
