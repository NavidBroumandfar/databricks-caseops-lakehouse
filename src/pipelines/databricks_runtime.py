"""
Databricks runtime helpers for Phase 2 productionization.

This module provides Spark-backed implementations for Databricks SQL AI
Functions and Delta table I/O without importing PySpark directly. The pipeline
modules inject a SparkSession-like object at runtime, while local tests use
small fakes so no Databricks workspace or credentials are required.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional


AI_FUNCTION_VERSION = "2.0"

DEFAULT_EXTRACTION_INSTRUCTIONS = (
    "Extract the requested structured fields from the parsed document content. "
    "Return only fields covered by the provided schema."
)
DEFAULT_CLASSIFICATION_INSTRUCTIONS = (
    "Classify the document type for the governed document preparation pipeline. "
    "Use the best matching label from the closed taxonomy."
)

DEFAULT_CLASSIFICATION_LABELS = {
    "fda_warning_letter": "FDA-issued warning letter to a regulated company",
    "cisa_advisory": "CISA-issued cybersecurity advisory or bulletin",
    "incident_report": "Internal or regulatory incident report",
    "unknown": "Document does not clearly match an active domain",
}

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
# Public runtime write modes: CLI + runtime write path.
ALLOWED_WRITE_MODES = frozenset({"append", "overwrite", "error", "errorifexists", "ignore"})


class DatabricksRuntimeError(RuntimeError):
    """Raised when a Databricks runtime adapter cannot complete its operation."""


def sql_string_literal(value: Any) -> str:
    """
    Return a Databricks SQL single-quoted string literal.

    Databricks SQL escapes single quotes by doubling them. Backslashes are left
    untouched because the generated SQL uses standard single-quoted literals.
    """
    text = "" if value is None else str(value)
    return "'" + text.replace("'", "''") + "'"


def validate_table_name(table_name: str) -> str:
    """Reject unsafe table names before interpolating them into SQL or writer calls."""
    if not _SAFE_TABLE_NAME_RE.match(table_name):
        raise ValueError(
            "table_name must be an unquoted one-, two-, or three-part identifier "
            "containing only letters, numbers, and underscores"
        )
    return table_name


def validate_identifier(identifier: str) -> str:
    """Reject unsafe column identifiers before interpolating them into filters."""
    if not _SAFE_IDENTIFIER_RE.match(identifier):
        raise ValueError(
            "identifier must contain only letters, numbers, and underscores, "
            "and must not be quoted or qualified"
        )
    return identifier


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def _as_jsonable_dict(record: Any) -> dict:
    if hasattr(record, "to_json_dict"):
        return record.to_json_dict()
    if hasattr(record, "model_dump_json"):
        return json.loads(record.model_dump_json())
    if hasattr(record, "model_dump"):
        return json.loads(json.dumps(record.model_dump(), default=str))
    if isinstance(record, Mapping):
        return dict(record)
    raise TypeError(f"Unsupported record type for Delta write: {type(record)!r}")


def _spark_scalar(value: Any) -> Any:
    """Coerce complex JSON values to deterministic strings before Spark infers schema."""
    if isinstance(value, Mapping):
        return _json_dumps(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return _json_dumps(list(value))
    return value


def _spark_row(record: Mapping[str, Any]) -> dict:
    return {key: _spark_scalar(value) for key, value in record.items()}


def _spark_schema_for_rows(rows: Sequence[Mapping[str, Any]]) -> Any:
    """
    Build a nullable Spark schema so all-null optional columns do not break inference.

    PySpark is imported lazily to keep local execution and tests workspace-free.
    """
    try:
        from pyspark.sql.types import (  # type: ignore[import-not-found]
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
    except Exception:
        return None

    ordered_keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in ordered_keys:
                ordered_keys.append(key)

    fields = []
    for key in ordered_keys:
        values = [row.get(key) for row in rows if row.get(key) is not None]
        if not values:
            data_type = StringType()
        elif all(isinstance(value, bool) for value in values):
            data_type = BooleanType()
        elif all(isinstance(value, int) and not isinstance(value, bool) for value in values):
            data_type = LongType()
        elif all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            data_type = DoubleType()
        elif all(isinstance(value, (datetime, date)) for value in values):
            data_type = TimestampType()
        else:
            data_type = StringType()
        fields.append(StructField(key, data_type, nullable=True))
    return StructType(fields)


def _decode_json_cell(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "{[":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _decode_json_cells(row: Mapping[str, Any]) -> dict:
    return {key: _decode_json_cell(value) for key, value in row.items()}


def _row_to_mapping(row: Any) -> dict:
    if hasattr(row, "asDict"):
        return _decode_json_cells(row.asDict(recursive=True))
    if isinstance(row, Mapping):
        return _decode_json_cells(row)
    try:
        return _decode_json_cells(dict(row))
    except (TypeError, ValueError) as exc:
        raise DatabricksRuntimeError(f"Unable to convert Spark row to dict: {row!r}") from exc


def _collect_one(dataframe: Any) -> dict:
    rows = dataframe.collect()
    if not rows:
        raise DatabricksRuntimeError("Databricks AI Function query returned no rows.")
    return _row_to_mapping(rows[0])


def _decode_variant(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _extract_first(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _extract_parse_text(payload: Any) -> str:
    payload = _decode_variant(payload)
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, Mapping):
        document = payload.get("document")
        if isinstance(document, Mapping):
            document_text = _extract_parse_text(document)
            if document_text:
                return document_text

        text = _extract_first(payload, ("parsed_text", "text", "content", "markdown", "value"))
        if text is not None:
            if isinstance(text, Mapping):
                nested_text = _extract_parse_text(text)
                if nested_text:
                    return nested_text
            return str(text)

        text_blocks = []
        for collection_name in ("pages", "elements"):
            collection = payload.get(collection_name)
            if not isinstance(collection, Sequence) or isinstance(collection, (str, bytes)):
                continue
            for item in collection:
                if isinstance(item, Mapping):
                    candidate = _extract_first(item, ("text", "content", "markdown", "value"))
                    if isinstance(candidate, Mapping):
                        candidate = _extract_parse_text(candidate)
                    if candidate:
                        text_blocks.append(str(candidate))
        if text_blocks:
            return "\n\n".join(text_blocks)
        return _json_dumps(payload)
    try:
        return _extract_parse_text(json.loads(str(payload)))
    except (TypeError, json.JSONDecodeError):
        pass
    return str(payload)


def _extract_page_count(mapping: Mapping[str, Any], payload: Any) -> Optional[int]:
    direct = mapping.get("page_count")
    if direct is not None:
        return int(direct)
    payload = _decode_variant(payload)
    if isinstance(payload, Mapping):
        nested = payload.get("page_count")
        if nested is not None:
            return int(nested)
        pages = payload.get("pages")
        if isinstance(pages, Sequence) and not isinstance(pages, (str, bytes)):
            return len(pages)
    return None


def _normalize_extraction_result(value: Any) -> dict:
    decoded = _decode_variant(value)
    if isinstance(decoded, Mapping):
        response = decoded.get("response")
        if isinstance(response, Mapping):
            return {str(key): _unwrap_extraction_field(field) for key, field in response.items()}
        return {str(key): _unwrap_extraction_field(field) for key, field in decoded.items()}
    raise DatabricksRuntimeError(
        "Databricks ai_extract result must decode to a JSON object or mapping."
    )


def _unwrap_extraction_field(value: Any) -> Any:
    decoded = _decode_variant(value)
    if isinstance(decoded, Mapping):
        if "value" in decoded:
            return _unwrap_extraction_field(decoded.get("value"))
        return {str(key): _unwrap_extraction_field(item) for key, item in decoded.items()}
    if isinstance(decoded, Sequence) and not isinstance(decoded, (str, bytes, bytearray)):
        return [_unwrap_extraction_field(item) for item in decoded]
    return decoded


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _label_from_response(response: Any) -> Optional[str]:
    if isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        if not response:
            return None
        first = response[0]
        if isinstance(first, Mapping):
            label = _extract_first(first, ("label", "document_type_label", "value"))
            return str(label) if label is not None else None
        return str(first)
    if isinstance(response, Mapping):
        label = _extract_first(response, ("label", "document_type_label", "value"))
        return str(label) if label is not None else None
    if response is not None:
        return str(response)
    return None


def _normalize_classification_result(mapping: Mapping[str, Any]) -> dict:
    if mapping.get("document_type_label") is not None:
        return {
            "document_type_label": str(mapping["document_type_label"]),
            "classification_confidence": _coerce_optional_float(
                _extract_first(mapping, ("classification_confidence", "confidence", "score"))
            ),
        }

    result = _decode_variant(
        _extract_first(mapping, ("classification_result", "document_type_result", "result"))
    )
    if isinstance(result, Mapping):
        response = result.get("response")
        label = _label_from_response(response)
        confidence = _coerce_optional_float(
            _extract_first(result, ("classification_confidence", "confidence", "score"))
        )
    else:
        label = _label_from_response(result)
        confidence = None

    if not label:
        label = "unknown"

    return {
        "document_type_label": label,
        "classification_confidence": confidence,
    }


class DatabricksAiParseRuntimeAdapter:
    """Spark-backed adapter for Databricks `ai_parse_document`."""

    model_id = "ai_parse_document/v1"

    def __init__(self, spark: Any, version: str = AI_FUNCTION_VERSION) -> None:
        self.spark = spark
        self.version = version

    def parse_volume_file(self, file_path: str) -> dict:
        """
        Parse one file from a Unity Catalog Volume path using `READ_FILES`.

        The adapter returns the local Bronze parser shape: `parsed_text`,
        `page_count`, and `parse_model`.
        """
        path_literal = sql_string_literal(str(file_path))
        version_literal = sql_string_literal(self.version)
        query = f"""
SELECT
  ai_parse_document(content, MAP('version', {version_literal})) AS parsed_content
FROM READ_FILES({path_literal}, format => 'binaryFile')
LIMIT 1
""".strip()
        mapping = _collect_one(self.spark.sql(query))
        payload = _extract_first(mapping, ("parsed_content", "parse_result", "result"))
        parsed_text = _extract_parse_text(payload)
        return {
            "parsed_text": parsed_text,
            "page_count": _extract_page_count(mapping, payload),
            "parse_model": self.model_id,
        }


class DatabricksAiExtractRuntimeAdapter:
    """Spark-backed adapter for Databricks `ai_extract`."""

    model_id = "ai_extract/v1"

    def __init__(
        self,
        spark: Any,
        extraction_schema: Any,
        instructions: str = DEFAULT_EXTRACTION_INSTRUCTIONS,
        version: str = AI_FUNCTION_VERSION,
    ) -> None:
        self.spark = spark
        self.extraction_schema = extraction_schema
        self.instructions = instructions
        self.version = version

    def extract(self, parsed_text: str) -> dict:
        schema_json = (
            self.extraction_schema
            if isinstance(self.extraction_schema, str)
            else _json_dumps(self.extraction_schema)
        )
        query = f"""
SELECT
  ai_extract(
    {sql_string_literal(parsed_text)},
    {sql_string_literal(schema_json)},
    MAP(
      'version', {sql_string_literal(self.version)},
      'instructions', {sql_string_literal(self.instructions)}
    )
  ) AS extraction_result
""".strip()
        mapping = _collect_one(self.spark.sql(query))
        result = _extract_first(mapping, ("extraction_result", "result"))
        return _normalize_extraction_result(result if result is not None else mapping)


class DatabricksAiClassifyRuntimeAdapter:
    """Spark-backed adapter for Databricks `ai_classify`."""

    model_id = "ai_classify/v1"

    def __init__(
        self,
        spark: Any,
        label_taxonomy: Optional[Any] = None,
        instructions: str = DEFAULT_CLASSIFICATION_INSTRUCTIONS,
        version: str = AI_FUNCTION_VERSION,
    ) -> None:
        self.spark = spark
        self.label_taxonomy = label_taxonomy or DEFAULT_CLASSIFICATION_LABELS
        self.instructions = instructions
        self.version = version

    def classify(self, silver: Mapping[str, Any]) -> dict:
        input_value = silver.get("extracted_fields") or silver
        input_json = _json_dumps(input_value)
        labels_json = (
            self.label_taxonomy
            if isinstance(self.label_taxonomy, str)
            else _json_dumps(self.label_taxonomy)
        )
        query = f"""
SELECT
  ai_classify(
    {sql_string_literal(input_json)},
    {sql_string_literal(labels_json)},
    MAP(
      'version', {sql_string_literal(self.version)},
      'instructions', {sql_string_literal(self.instructions)}
    )
  ) AS classification_result
""".strip()
        mapping = _collect_one(self.spark.sql(query))
        return _normalize_classification_result(mapping)


class DeltaTableIO:
    """Small Delta table reader/writer using an injected SparkSession-like object."""

    def __init__(self, spark: Any) -> None:
        self.spark = spark

    def write_records(
        self,
        records: Iterable[Any],
        table_name: str,
        mode: str = "append",
    ) -> int:
        """
        Write JSON-serializable records to a managed Delta table.

        Returns the number of records written. Empty batches are treated as a
        no-op so callers can safely write conditional pipeline outputs.
        """
        validated_table = validate_table_name(table_name)
        normalized_mode = mode.lower()
        if normalized_mode not in ALLOWED_WRITE_MODES:
            raise ValueError(f"Unsupported Delta write mode: {mode!r}")

        rows = [_spark_row(_as_jsonable_dict(record)) for record in records]
        if not rows:
            return 0

        schema = _spark_schema_for_rows(rows)
        if schema is None:
            dataframe = self.spark.createDataFrame(rows)
        else:
            dataframe = self.spark.createDataFrame(rows, schema=schema)
        (
            dataframe.write
            .format("delta")
            .mode(normalized_mode)
            .saveAsTable(validated_table)
        )
        return len(rows)

    def read_table(
        self,
        table_name: str,
        limit: Optional[int] = None,
        where_equals: Optional[Mapping[str, Any]] = None,
    ) -> list[dict]:
        """Read a Delta table into a list of JSON-like row dictionaries."""
        validated_table = validate_table_name(table_name)
        dataframe = self.spark.table(validated_table)
        if where_equals:
            for column_name, value in sorted(where_equals.items()):
                validated_column = validate_identifier(column_name)
                dataframe = dataframe.where(
                    f"{validated_column} = {sql_string_literal(value)}"
                )
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be non-negative when provided")
            dataframe = dataframe.limit(limit)
        return [_row_to_mapping(row) for row in dataframe.collect()]


@dataclass(frozen=True)
class DeltaTableTargets:
    """Fully-qualified Delta table names for one pipeline environment."""

    bronze_table: str
    silver_table: str
    gold_table: str

    @classmethod
    def from_environment_config(cls, config: Any) -> "DeltaTableTargets":
        return cls(
            bronze_table=validate_table_name(config.bronze_table),
            silver_table=validate_table_name(config.silver_table),
            gold_table=validate_table_name(config.gold_table),
        )
