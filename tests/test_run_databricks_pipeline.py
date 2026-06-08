from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

from src.pipelines.databricks_runtime import DeltaTableTargets
from src.pipelines.run_databricks_pipeline import (
    DatabricksRuntimeError,
    RuntimeFileMetadata,
    _build_bronze_record,
    _resolve_repo_root,
    _resolve_runtime_paths,
    _run_gold_stage,
    _run_silver_stage,
    _validate_preflight_requirements,
)
from src.utils.environment_config import EnvironmentConfig, Environment


class FakeRow(dict):
    def asDict(self, recursive: bool = True):  # noqa: N802
        return dict(self)


class FakeSqlDataFrame:
    def __init__(self, rows: list[dict], should_raise: bool = False) -> None:
        self.rows = rows
        self.should_raise = should_raise

    def collect(self) -> list[FakeRow]:
        if self.should_raise:
            raise RuntimeError("simulate write-permission failure")
        return [FakeRow(row) for row in self.rows]


class FakeTableDataFrame(FakeSqlDataFrame):
    def __init__(
        self,
        rows: list[dict],
        should_raise: bool = False,
        where_calls: list[str] | None = None,
    ) -> None:
        super().__init__(rows=rows, should_raise=should_raise)
        self.where_calls = where_calls if where_calls is not None else []

    def where(self, clause: str) -> "FakeTableDataFrame":
        self.where_calls.append(clause)
        filtered = self.rows
        marker = "pipeline_run_id = '"
        if clause.startswith(marker) and clause.endswith("'"):
            run_id = clause.removeprefix(marker)[:-1].replace("''", "'")
            filtered = [row for row in self.rows if row.get("pipeline_run_id") == run_id]
        return FakeTableDataFrame(filtered, should_raise=self.should_raise, where_calls=self.where_calls)

    def limit(self, value: int) -> "FakeTableDataFrame":
        return FakeTableDataFrame(
            self.rows[:value],
            should_raise=self.should_raise,
            where_calls=self.where_calls,
        )


class FakeReader:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def format(self, value: str) -> "FakeReader":
        return self

    def load(self, *values: str) -> "FakeReader":
        return self

    def select(self, *values: str) -> "FakeReader":
        return self

    def collect(self) -> list[FakeRow]:
        return [FakeRow(row) for row in self.rows]


class FakeCatalog:
    def __init__(self, existing: set[str]) -> None:
        self.existing = existing

    def tableExists(self, table_name: str) -> bool:
        return table_name in self.existing


class FakeSpark:
    def __init__(
        self,
        *,
        raw_input_rows: list[dict] | None = None,
        table_rows: list[dict] | None = None,
        table_rows_by_name: dict[str, list[dict]] | None = None,
        existing_tables: set[str] | None = None,
        sql_raises_for_tables: set[str] | None = None,
    ) -> None:
        self.read = FakeReader(raw_input_rows or [])
        self.catalog = FakeCatalog(existing_tables or set())
        self.table_rows = table_rows or []
        self.table_rows_by_name = table_rows_by_name or {}
        self.sql_raises_for_tables = sql_raises_for_tables or set()
        self.table_calls: list[str] = []
        self.where_calls: list[str] = []
        self.sql_queries: list[str] = []
        self.created_dataframes: list[list[dict]] = []

    def table(self, table_name: str) -> FakeTableDataFrame:
        self.table_calls.append(table_name)
        rows = self.table_rows_by_name.get(table_name, self.table_rows)
        return FakeTableDataFrame(rows, where_calls=self.where_calls)

    def sql(self, query: str):
        self.sql_queries.append(query)
        table_target = query.split("INSERT INTO ", 1)[1].split(" ", 1)[0] if "INSERT INTO " in query else ""
        if table_target in self.sql_raises_for_tables:
            raise RuntimeError("simulate write-permission failure")
        return FakeSqlDataFrame(rows=[])

    def createDataFrame(self, rows: list[dict]):  # noqa: N802
        self.created_dataframes.append(rows)
        return FakeWritableDataFrame(rows)


class FakeWriter:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def format(self, value: str) -> "FakeWriter":
        return self

    def mode(self, value: str) -> "FakeWriter":
        return self

    def saveAsTable(self, table_name: str) -> None:  # noqa: N802
        return None


class FakeWritableDataFrame:
    def __init__(self, rows: list[dict]) -> None:
        self.write = FakeWriter(rows)


def _targets() -> DeltaTableTargets:
    config = EnvironmentConfig(environment=Environment.DEV)
    return DeltaTableTargets.from_environment_config(config)


def _input_rows() -> list[dict]:
    return [{"path": "/Volumes/caseops_dev/raw/documents/doc1.pdf", "length": 128}]


def test_resolve_repo_root_falls_back_to_cwd_without_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.pipelines.run_databricks_pipeline as runner

    repo_root = Path(__file__).resolve().parents[1]
    original_file = runner.__dict__.pop("__file__", None)
    monkeypatch.chdir(repo_root)
    try:
        assert _resolve_repo_root() == repo_root
    finally:
        if original_file is not None:
            runner.__dict__["__file__"] = original_file


def test_validate_preflight_all_stage_checks_raw_inputs_without_write_checks() -> None:
    targets = _targets()
    spark = FakeSpark(raw_input_rows=_input_rows())

    _validate_preflight_requirements(
        spark=spark,
        stage="all",
        targets=targets,
        input_paths=[],
        fallback_input="/Volumes/caseops_dev/raw/documents",
        write_mode="append",
        check_output_permissions=False,
    )

    assert spark.table_calls == []


def test_validate_preflight_all_stage_with_check_write_permissions_fails_when_append_and_missing_target() -> None:
    targets = _targets()
    spark = FakeSpark(raw_input_rows=_input_rows(), existing_tables=set())

    with pytest.raises(DatabricksRuntimeError, match="does not exist"):
        _validate_preflight_requirements(
            spark=spark,
            stage="all",
            targets=targets,
            input_paths=[],
            fallback_input="/Volumes/caseops_dev/raw/documents",
            write_mode="append",
            check_output_permissions=True,
        )


def test_validate_preflight_all_stage_with_check_write_permissions_allows_missing_when_overwrite() -> None:
    targets = _targets()
    spark = FakeSpark(raw_input_rows=_input_rows(), existing_tables=set())

    _validate_preflight_requirements(
        spark=spark,
        stage="all",
        targets=targets,
        input_paths=[],
        fallback_input="/Volumes/caseops_dev/raw/documents",
        write_mode="overwrite",
        check_output_permissions=True,
    )


def test_validate_preflight_silver_with_check_write_permissions_checks_target_writability() -> None:
    targets = _targets()
    # bronze read path exists for stage dependency, silver exists but write probe fails.
    spark = FakeSpark(
        raw_input_rows=_input_rows(),
        table_rows=[{"bronze_record_id": "id"}],
        existing_tables={targets.bronze_table, targets.silver_table},
        sql_raises_for_tables={targets.silver_table},
    )

    with pytest.raises(DatabricksRuntimeError, match="is not writable"):
        _validate_preflight_requirements(
            spark=spark,
            stage="silver",
            targets=targets,
            input_paths=[],
            fallback_input="/Volumes/caseops_dev/raw/documents",
            write_mode="append",
            check_output_permissions=True,
        )

    assert any("INSERT INTO" in q for q in spark.sql_queries)


def test_direct_script_invocation_works_outside_repo_root(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "src" / "pipelines" / "run_databricks_pipeline.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--stage",
            "delivery_validation",
            "--pipeline-run-id",
            "direct-cli-run",
            "--workspace-mode",
            "local_repo_only",
            "--validation-output-dir",
            str(tmp_path / "validation"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Delivery validation status:" in result.stdout
    pipeline_scoped_result = tmp_path / "validation" / "delivery_validation_direct-cli-run.json"
    assert pipeline_scoped_result.exists()
    assert "Pipeline-run delivery validation artifact:" in result.stdout


def test_runtime_bronze_file_hash_uses_raw_file_bytes() -> None:
    content = b"raw source bytes for sha256"
    spark = FakeSpark(
        raw_input_rows=[
            {
                "path": "/Volumes/caseops_dev/raw/documents/doc1.pdf",
                "length": len(content),
                "content": content,
            }
        ],
    )

    paths, metadata = _resolve_runtime_paths(
        spark,
        requested_inputs=["/Volumes/caseops_dev/raw/documents/doc1.pdf"],
        fallback_input="/Volumes/caseops_dev/raw/documents",
        require_file_hash=True,
    )

    expected_hash = hashlib.sha256(content).hexdigest()
    assert paths == ["/Volumes/caseops_dev/raw/documents/doc1.pdf"]
    assert metadata[paths[0]].file_hash == expected_hash

    bronze = _build_bronze_record(
        file_path=paths[0],
        parse_result={"parsed_text": "Parsed document text.", "page_count": 1},
        parse_error=None,
        parse_model="ai_parse_document/v1",
        pipeline_run_id="run-current",
        metadata=metadata[paths[0]],
        document_class_hint="fda_warning_letter",
        source_system="runtime_test",
        ingested_by="job",
    )

    assert bronze["file_hash"] == expected_hash


def test_runtime_bronze_requires_raw_byte_file_hash() -> None:
    with pytest.raises(DatabricksRuntimeError, match="Missing raw-byte SHA-256"):
        _build_bronze_record(
            file_path="/Volumes/caseops_dev/raw/documents/doc1.pdf",
            parse_result={"parsed_text": "Parsed document text.", "page_count": 1},
            parse_error=None,
            parse_model="ai_parse_document/v1",
            pipeline_run_id="run-current",
            metadata=RuntimeFileMetadata(
                path="/Volumes/caseops_dev/raw/documents/doc1.pdf",
                file_size_bytes=42,
            ),
            document_class_hint="fda_warning_letter",
            source_system="runtime_test",
            ingested_by="job",
        )


class FakeExtractAdapter:
    model_id = "ai_extract/v1"

    def extract(self, parsed_text: str) -> dict:
        return {
            "issuing_office": "FDA Office",
            "recipient_company": "Acme Pharma",
            "issue_date": "2026-01-01",
            "violation_type": ["CGMP"],
            "corrective_action_requested": True,
        }


def _bronze_row(run_id: str, document_id: str, bronze_record_id: str) -> dict:
    return {
        "document_id": document_id,
        "bronze_record_id": bronze_record_id,
        "pipeline_run_id": run_id,
        "parse_status": "success",
        "parsed_text": "FDA warning letter text",
        "document_class_hint": "fda_warning_letter",
        "file_name": f"{document_id}.pdf",
        "ingested_at": "2026-06-08T00:00:00+00:00",
    }


def test_runtime_silver_stage_reads_only_source_pipeline_run(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets()
    spark = FakeSpark(
        table_rows_by_name={
            targets.bronze_table: [
                _bronze_row("run-current", "doc-current", "bronze-current"),
                _bronze_row("run-old", "doc-old", "bronze-old"),
            ],
        },
    )

    monkeypatch.setattr(
        "src.pipelines.run_databricks_pipeline._build_extract_adapter",
        lambda **kwargs: FakeExtractAdapter(),
    )

    rows = _run_silver_stage(
        spark=spark,
        targets=targets,
        write_mode="append",
        pipeline_run_id="run-current",
        source_pipeline_run_id="run-current",
        extract_version="2.0",
    )

    assert len(rows) == 1
    assert rows[0]["document_id"] == "doc-current"
    assert spark.where_calls == ["pipeline_run_id = 'run-current'"]


class FakeClassifyAdapter:
    model_id = "ai_classify/v1"

    def __init__(self, *args, **kwargs) -> None:
        pass

    def classify(self, silver: dict) -> dict:
        return {
            "document_type_label": "fda_warning_letter",
            "classification_confidence": 0.99,
        }


def _silver_row(run_id: str, document_id: str, bronze_record_id: str) -> dict:
    return {
        "document_id": document_id,
        "bronze_record_id": bronze_record_id,
        "extraction_id": f"extract-{document_id}",
        "pipeline_run_id": run_id,
        "validation_status": "valid",
        "field_coverage_pct": 1.0,
        "extraction_model": "ai_extract/v1",
        "extracted_fields": {
            "issuing_office": "FDA Office",
            "recipient_company": "Acme Pharma",
            "issue_date": "2026-01-01",
            "violation_type": ["CGMP"],
            "corrective_action_requested": True,
        },
    }


def test_runtime_gold_stage_reads_only_source_pipeline_run(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets()
    spark = FakeSpark(
        table_rows_by_name={
            targets.silver_table: [
                _silver_row("run-current", "doc-current", "bronze-current"),
                _silver_row("run-old", "doc-old", "bronze-old"),
            ],
            targets.bronze_table: [
                _bronze_row("run-current", "doc-current", "bronze-current"),
                _bronze_row("run-old", "doc-old", "bronze-old"),
            ],
        },
    )
    monkeypatch.setattr(
        "src.pipelines.run_databricks_pipeline.DatabricksAiClassifyAdapter",
        FakeClassifyAdapter,
    )

    rows = _run_gold_stage(
        spark=spark,
        targets=targets,
        write_mode="append",
        pipeline_run_id="run-current",
        source_pipeline_run_id="run-current",
        classify_version="2.0",
    )

    assert len(rows) == 1
    assert rows[0]["document_id"] == "doc-current"
    assert spark.where_calls == [
        "pipeline_run_id = 'run-current'",
        "pipeline_run_id = 'run-current'",
    ]


def test_bundle_delivery_validation_jobs_have_explicit_artifact_paths() -> None:
    bundle_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "databricks-runtime-bundle"
        / "databricks.yml"
    )
    bundle_text = bundle_path.read_text(encoding="utf-8")

    assert "caseops-runtime-delivery-validation-local" in bundle_text
    assert "caseops-runtime-delivery-validation-provisioned" in bundle_text
    assert "--delivery-event-path" in bundle_text
    assert "${env:CASEOPS_DELIVERY_EVENT_PATH}" in bundle_text
    assert "--share-manifest-path" in bundle_text
    assert "${env:CASEOPS_SHARE_MANIFEST_PATH}" in bundle_text
    assert "--runtime-evidence-path" in bundle_text
    assert "${env:CASEOPS_RUNTIME_EVIDENCE_PATH}" in bundle_text
