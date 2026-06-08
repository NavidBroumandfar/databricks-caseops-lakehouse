"""
Phase 2 Databricks runtime adapter tests.

These tests use fake Spark objects only. They verify the repo can expose
Databricks runtime paths without requiring credentials, PySpark, workspace URLs,
or live Unity Catalog resources during local test runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

from src.pipelines import databricks_runtime as runtime_module
from src.pipelines.classify_gold import DatabricksAiClassifyAdapter
from src.pipelines.databricks_runtime import (
    DatabricksAiClassifyRuntimeAdapter,
    DatabricksAiExtractRuntimeAdapter,
    DatabricksAiParseRuntimeAdapter,
    DeltaTableIO,
    DeltaTableTargets,
    sql_string_literal,
)
from src.pipelines.extract_silver import DatabricksAiExtractAdapter
from src.pipelines.ingest_bronze import DatabricksAiParseAdapter
from src.utils.environment_config import Environment, EnvironmentConfig


class FakeRow(dict):
    def asDict(self, recursive: bool = True) -> dict:
        return dict(self)


class JsonRenderedVariant:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def __str__(self) -> str:
        return self.payload


class FakeSqlDataFrame:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = [FakeRow(row) for row in rows]

    def collect(self) -> list[FakeRow]:
        return self._rows


class FakeTableDataFrame(FakeSqlDataFrame):
    def __init__(self, rows: list[dict]) -> None:
        super().__init__(rows)
        self.limit_value = None

    def limit(self, value: int) -> "FakeTableDataFrame":
        self.limit_value = value
        limited = FakeTableDataFrame([dict(row) for row in self._rows[:value]])
        limited.limit_value = value
        return limited


class FakeWriter:
    def __init__(self, spark: "FakeSpark") -> None:
        self.spark = spark
        self.format_value = None
        self.mode_value = None

    def format(self, value: str) -> "FakeWriter":
        self.format_value = value
        return self

    def mode(self, value: str) -> "FakeWriter":
        self.mode_value = value
        return self

    def saveAsTable(self, table_name: str) -> None:
        self.spark.saved_table = table_name
        self.spark.saved_format = self.format_value
        self.spark.saved_mode = self.mode_value


class FakeWriteDataFrame:
    def __init__(self, spark: "FakeSpark", rows: list[dict]) -> None:
        self.spark = spark
        self.rows = rows
        self.write = FakeWriter(spark)


class FakeSpark:
    def __init__(
        self,
        sql_results: Optional[list[list[dict]]] = None,
        table_rows: Optional[list[dict]] = None,
    ) -> None:
        self.sql_results = list(sql_results or [])
        self.table_rows = list(table_rows or [])
        self.sql_queries = []
        self.table_names = []
        self.created_rows = None
        self.created_schema = None
        self.saved_table = None
        self.saved_format = None
        self.saved_mode = None

    def sql(self, query: str) -> FakeSqlDataFrame:
        self.sql_queries.append(query)
        rows = self.sql_results.pop(0) if self.sql_results else []
        return FakeSqlDataFrame(rows)

    def table(self, table_name: str) -> FakeTableDataFrame:
        self.table_names.append(table_name)
        return FakeTableDataFrame(self.table_rows)

    def createDataFrame(self, rows: list[dict], schema=None) -> FakeWriteDataFrame:
        self.created_rows = rows
        self.created_schema = schema
        return FakeWriteDataFrame(self, rows)


def test_sql_string_literal_escapes_single_quotes() -> None:
    assert sql_string_literal("FDA's warning") == "'FDA''s warning'"


def test_parse_runtime_adapter_uses_read_files_and_returns_local_parser_shape() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "parsed_content": {
                        "pages": [
                            {"text": "Page one text"},
                            {"text": "Page two text"},
                        ]
                    }
                }
            ]
        ]
    )

    result = DatabricksAiParseRuntimeAdapter(spark).parse_volume_file(
        "/Volumes/caseops_dev/raw/documents/fda/file.pdf"
    )

    assert result == {
        "parsed_text": "Page one text\n\nPage two text",
        "page_count": 2,
        "parse_model": "ai_parse_document/v1",
    }
    query = spark.sql_queries[0]
    assert "ai_parse_document" in query
    assert "READ_FILES('/Volumes/caseops_dev/raw/documents/fda/file.pdf'" in query
    assert "MAP('version', '2.0')" in query


def test_parse_runtime_adapter_extracts_v2_document_elements() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "parsed_content": {
                        "document": {
                            "elements": [
                                {"content": "FDA Warning Letter"},
                                {"text": "Corrective action requested within 15 days."},
                            ]
                        }
                    }
                }
            ]
        ]
    )

    result = DatabricksAiParseRuntimeAdapter(spark).parse_volume_file(
        "/Volumes/caseops_dev/raw/documents/fda/file.pdf"
    )

    assert result["parsed_text"] == (
        "FDA Warning Letter\n\nCorrective action requested within 15 days."
    )


def test_parse_runtime_adapter_decodes_json_rendered_variant() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "parsed_content": JsonRenderedVariant(
                        '{"document":{"elements":[{"content":"FDA Warning Letter"},'
                        '{"content":"Issuing office: CDER"}]}}'
                    )
                }
            ]
        ]
    )

    result = DatabricksAiParseRuntimeAdapter(spark).parse_volume_file(
        "/Volumes/caseops_dev/raw/documents/fda/file.pdf"
    )

    assert result["parsed_text"] == "FDA Warning Letter\n\nIssuing office: CDER"


def test_existing_parse_adapter_delegates_when_spark_is_injected() -> None:
    spark = FakeSpark(sql_results=[[{"parsed_content": {"text": "parsed"}}]])

    result = DatabricksAiParseAdapter(spark=spark).parse(Path("/Volumes/caseops/raw/doc.pdf"))

    assert result["parsed_text"] == "parsed"
    assert spark.sql_queries


def test_existing_parse_adapter_still_raises_without_spark() -> None:
    with pytest.raises(NotImplementedError, match="requires a live Databricks runtime"):
        DatabricksAiParseAdapter().parse(Path("/Volumes/caseops/raw/doc.pdf"))


def test_extract_runtime_adapter_calls_ai_extract_and_decodes_json_result() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "extraction_result": (
                        '{"recipient_company": "Acme Pharma", '
                        '"corrective_action_requested": true}'
                    )
                }
            ]
        ]
    )
    schema = {
        "recipient_company": {"type": "string"},
        "corrective_action_requested": {"type": "boolean"},
    }

    result = DatabricksAiExtractRuntimeAdapter(
        spark=spark,
        extraction_schema=schema,
        instructions="Extract FDA fields only.",
    ).extract("Warning letter text")

    assert result == {
        "recipient_company": "Acme Pharma",
        "corrective_action_requested": True,
    }
    query = spark.sql_queries[0]
    assert "ai_extract" in query
    assert "'Warning letter text'" in query
    assert '"recipient_company"' in query
    assert "'Extract FDA fields only.'" in query


def test_extract_runtime_adapter_unwraps_v2_field_value_response() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "extraction_result": {
                        "response": {
                            "recipient_company": {
                                "value": "Acme Pharma",
                                "citation_ids": [],
                            },
                            "violation_type": {
                                "value": ["CGMP"],
                                "citation_ids": [],
                            },
                            "corrective_action_requested": {
                                "value": True,
                                "citation_ids": [],
                            },
                        },
                        "error_message": None,
                    }
                }
            ]
        ]
    )

    result = DatabricksAiExtractRuntimeAdapter(spark=spark, extraction_schema={}).extract(
        "Warning letter text"
    )

    assert result == {
        "recipient_company": "Acme Pharma",
        "violation_type": ["CGMP"],
        "corrective_action_requested": True,
    }


def test_existing_extract_adapter_requires_schema_when_spark_is_injected() -> None:
    spark = FakeSpark(sql_results=[[{"extraction_result": "{}"}]])

    with pytest.raises(ValueError, match="requires an extraction_schema"):
        DatabricksAiExtractAdapter(spark=spark).extract("text")


def test_existing_extract_adapter_still_raises_without_spark() -> None:
    with pytest.raises(NotImplementedError, match="requires a live Databricks runtime"):
        DatabricksAiExtractAdapter().extract("text")


def test_classify_runtime_adapter_decodes_response_and_allows_null_confidence() -> None:
    spark = FakeSpark(
        sql_results=[
            [
                {
                    "classification_result": {
                        "response": ["cisa_advisory"],
                        "error_message": None,
                    }
                }
            ]
        ]
    )
    silver = {
        "extracted_fields": {
            "advisory_id": "AA24-001A",
            "severity_level": "High",
        }
    }

    result = DatabricksAiClassifyRuntimeAdapter(spark=spark).classify(silver)

    assert result == {
        "document_type_label": "cisa_advisory",
        "classification_confidence": None,
    }
    query = spark.sql_queries[0]
    assert "ai_classify" in query
    assert '"cisa_advisory"' in query
    assert '"incident_report"' in query


def test_existing_classify_adapter_delegates_when_spark_is_injected() -> None:
    spark = FakeSpark(
        sql_results=[[{"classification_result": {"response": ["incident_report"]}}]]
    )

    result = DatabricksAiClassifyAdapter(spark=spark).classify(
        {"extracted_fields": {"incident_type": "outage"}}
    )

    assert result["document_type_label"] == "incident_report"
    assert result["classification_confidence"] is None
    assert spark.sql_queries


def test_existing_classify_adapter_still_raises_without_spark() -> None:
    with pytest.raises(NotImplementedError, match="requires a live Databricks runtime"):
        DatabricksAiClassifyAdapter().classify({"extracted_fields": {}})


def test_delta_table_io_writes_records_with_delta_format() -> None:
    spark = FakeSpark()
    io = DeltaTableIO(spark)

    count = io.write_records(
        records=[{"document_id": "doc-1"}, {"document_id": "doc-2"}],
        table_name="caseops_dev.bronze.parsed_documents",
    )

    assert count == 2
    assert spark.created_rows == [{"document_id": "doc-1"}, {"document_id": "doc-2"}]
    assert spark.saved_format == "delta"
    assert spark.saved_mode == "append"
    assert spark.saved_table == "caseops_dev.bronze.parsed_documents"


def test_delta_table_io_serializes_complex_fields_for_spark_inference() -> None:
    spark = FakeSpark()
    io = DeltaTableIO(spark)

    count = io.write_records(
        records=[
            {
                "document_id": "doc-1",
                "extracted_fields": {
                    "issuing_office": None,
                    "violation_type": [],
                },
                "validation_errors": ["issuing_office: null"],
            }
        ],
        table_name="caseops_dev.silver.extracted_records",
    )

    assert count == 1
    assert spark.created_rows == [
        {
            "document_id": "doc-1",
            "extracted_fields": '{"issuing_office": null, "violation_type": []}',
            "validation_errors": '["issuing_office: null"]',
        }
    ]


def test_delta_table_io_uses_explicit_schema_when_available(monkeypatch) -> None:
    spark = FakeSpark()
    io = DeltaTableIO(spark)
    sentinel_schema = object()
    monkeypatch.setattr(runtime_module, "_spark_schema_for_rows", lambda rows: sentinel_schema)

    count = io.write_records(
        records=[{"document_id": "doc-1", "document_class_hint": None}],
        table_name="caseops_dev.bronze.parsed_documents",
        mode="overwrite",
    )

    assert count == 1
    assert spark.created_schema is sentinel_schema
    assert spark.created_rows == [{"document_id": "doc-1", "document_class_hint": None}]


def test_delta_table_io_empty_write_is_noop() -> None:
    spark = FakeSpark()

    count = DeltaTableIO(spark).write_records([], "caseops_dev.gold.ai_ready_assets")

    assert count == 0
    assert spark.created_rows is None
    assert spark.saved_table is None


def test_delta_table_io_reads_table_with_limit() -> None:
    spark = FakeSpark(
        table_rows=[
            {"document_id": "doc-1"},
            {"document_id": "doc-2"},
        ]
    )

    rows = DeltaTableIO(spark).read_table("caseops_dev.silver.extracted_records", limit=1)

    assert rows == [{"document_id": "doc-1"}]
    assert spark.table_names == ["caseops_dev.silver.extracted_records"]


def test_delta_table_io_decodes_json_cells_after_read() -> None:
    spark = FakeSpark(
        table_rows=[
            {
                "document_id": "doc-1",
                "extracted_fields": '{"issuing_office": "FDA", "violation_type": []}',
                "validation_errors": '["issue_date: null"]',
            }
        ]
    )

    rows = DeltaTableIO(spark).read_table("caseops_dev.silver.extracted_records")

    assert rows == [
        {
            "document_id": "doc-1",
            "extracted_fields": {
                "issuing_office": "FDA",
                "violation_type": [],
            },
            "validation_errors": ["issue_date: null"],
        }
    ]


def test_delta_table_io_rejects_unsafe_table_names() -> None:
    with pytest.raises(ValueError, match="table_name"):
        DeltaTableIO(FakeSpark()).read_table("caseops.gold.assets; DROP TABLE x")


def test_delta_table_targets_from_environment_config() -> None:
    targets = DeltaTableTargets.from_environment_config(
        EnvironmentConfig(environment=Environment.DEV)
    )

    assert targets.bronze_table == "caseops_dev.bronze.parsed_documents"
    assert targets.silver_table == "caseops_dev.silver.extracted_records"
    assert targets.gold_table == "caseops_dev.gold.ai_ready_assets"
