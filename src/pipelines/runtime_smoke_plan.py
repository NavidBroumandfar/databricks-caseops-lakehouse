"""
Phase 4 runtime smoke capture plan generator.

This module produces a local-safe, run-scoped checklist before an operator runs
the Databricks workspace smoke workflow. It does not contact Databricks and does
not prove that runtime smoke evidence exists.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schemas.runtime_smoke import (
    SMOKE_WORKSPACE_PERSONAL,
    RuntimeSmokeCapturePlan,
    RuntimeSmokeExpectedArtifact,
)


ARTIFACT_DELIVERY_EVENT = "delivery_event"
ARTIFACT_SHARE_MANIFEST = "share_manifest"
ARTIFACT_RUNTIME_EVIDENCE = "runtime_evidence"
ARTIFACT_DELIVERY_VALIDATION = "delivery_validation_result"
ARTIFACT_RUNTIME_SMOKE_VALIDATION = "runtime_smoke_validation_result"


def _safe_pipeline_run_id(pipeline_run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", pipeline_run_id)


def _path_text(path: Path) -> str:
    return path.as_posix()


def build_runtime_smoke_capture_plan(
    *,
    pipeline_run_id: str,
    environment: str,
    output_root: Path = Path("output"),
    workspace_mode: str = SMOKE_WORKSPACE_PERSONAL,
    generated_at: Optional[str] = None,
) -> RuntimeSmokeCapturePlan:
    """
    Build a deterministic capture plan for one Phase 4 runtime smoke run.

    The returned plan names the expected artifacts and commands needed to
    validate them locally after the workspace run completes. It intentionally
    contains only placeholder-free local paths and no workspace identifiers.
    """
    safe_id = _safe_pipeline_run_id(pipeline_run_id)
    delivery_event_path = output_root / "delivery" / f"delivery_event_{safe_id}.json"
    share_manifest_path = output_root / "delivery" / "delta_share_preparation_manifest.json"
    runtime_evidence_path = (
        output_root / "validation" / "runtime_evidence" / f"runtime_evidence_{safe_id}.json"
    )
    delivery_validation_path = output_root / "validation" / f"delivery_validation_{safe_id}.json"
    smoke_validation_path = output_root / "validation" / f"runtime_smoke_validation_{safe_id}.json"

    local_validation_command = (
        ".venv/bin/python src/pipelines/runtime_smoke_validation.py "
        f"--pipeline-run-id {pipeline_run_id} "
        f"--environment {environment} "
        f"--delivery-event-path {_path_text(delivery_event_path)} "
        f"--share-manifest-path {_path_text(share_manifest_path)} "
        f"--runtime-evidence-path {_path_text(runtime_evidence_path)} "
        f"--delivery-validation-result-path {_path_text(delivery_validation_path)} "
        f"--output-dir {_path_text(output_root / 'validation')}"
    )

    return RuntimeSmokeCapturePlan(
        capture_plan_id=str(uuid.uuid4()),
        pipeline_run_id=pipeline_run_id,
        environment=environment,
        workspace_mode=workspace_mode,
        generated_at=generated_at or datetime.now(tz=timezone.utc).isoformat(),
        expected_artifacts=[
            RuntimeSmokeExpectedArtifact(
                artifact_name=ARTIFACT_DELIVERY_EVENT,
                path=_path_text(delivery_event_path),
                description="Delivery event JSON for the runtime batch.",
            ),
            RuntimeSmokeExpectedArtifact(
                artifact_name=ARTIFACT_SHARE_MANIFEST,
                path=_path_text(share_manifest_path),
                description="Delta Share preparation manifest after setup SQL is provisioned.",
            ),
            RuntimeSmokeExpectedArtifact(
                artifact_name=ARTIFACT_RUNTIME_EVIDENCE,
                path=_path_text(runtime_evidence_path),
                description="Sanitized Databricks SQL evidence captured from the workspace run.",
            ),
            RuntimeSmokeExpectedArtifact(
                artifact_name=ARTIFACT_DELIVERY_VALIDATION,
                path=_path_text(delivery_validation_path),
                description="C-2 delivery validation result in validated personal_databricks mode.",
            ),
            RuntimeSmokeExpectedArtifact(
                artifact_name=ARTIFACT_RUNTIME_SMOKE_VALIDATION,
                path=_path_text(smoke_validation_path),
                description="Local Phase 4 smoke package validation result.",
            ),
        ],
        workspace_commands=[
            (
                ".venv/bin/python src/pipelines/run_databricks_pipeline.py "
                f"--stage all --environment {environment} "
                f"--pipeline-run-id {pipeline_run_id} --output-mode append"
            ),
            (
                ".venv/bin/python src/pipelines/run_databricks_pipeline.py "
                f"--stage delivery_validation --environment {environment} "
                f"--delivery-pipeline-run-id {pipeline_run_id} "
                f"--delivery-event-path {_path_text(delivery_event_path)} "
                f"--share-manifest-path {_path_text(share_manifest_path)} "
                f"--runtime-evidence-path {_path_text(runtime_evidence_path)} "
                "--workspace-mode personal_databricks "
                f"--validation-output-dir {_path_text(output_root / 'validation')}"
            ),
        ],
        local_validation_commands=[local_validation_command],
        sanitization_rules=[
            "Remove Databricks workspace URLs before saving artifacts.",
            "Remove Delta Sharing activation links before saving artifacts.",
            "Remove PATs, tokens, passwords, and credential-like values before saving artifacts.",
            "Remove personal names, emails, account IDs, and customer identifiers before saving artifacts.",
        ],
        observations=[
            "This capture plan is not runtime evidence and must not be used as Phase 4 closeout proof.",
            "Phase 4 closeout still requires validated C-2 evidence and a ready_for_phase4_closeout smoke result.",
            "Keep generated evidence under output/ so it remains outside source control.",
        ],
    )


def format_runtime_smoke_capture_plan_text(plan: RuntimeSmokeCapturePlan) -> str:
    """Format a capture plan for operator review."""
    lines = [
        "Databricks Runtime Smoke Capture Plan",
        "=" * 52,
        f"capture_plan_id : {plan.capture_plan_id}",
        f"pipeline_run_id : {plan.pipeline_run_id}",
        f"environment     : {plan.environment}",
        f"workspace_mode  : {plan.workspace_mode}",
        f"generated_at    : {plan.generated_at}",
        "",
        "Expected Artifacts",
        "-" * 52,
    ]
    for artifact in plan.expected_artifacts:
        required = "required" if artifact.required else "optional"
        lines.append(f"- {artifact.artifact_name} ({required}): {artifact.path}")
        lines.append(f"  {artifact.description}")

    if plan.workspace_commands:
        lines.extend(["", "Workspace Commands", "-" * 52])
        lines.extend(f"- {command}" for command in plan.workspace_commands)

    if plan.local_validation_commands:
        lines.extend(["", "Local Validation", "-" * 52])
        lines.extend(f"- {command}" for command in plan.local_validation_commands)

    if plan.sanitization_rules:
        lines.extend(["", "Sanitization Rules", "-" * 52])
        lines.extend(f"- {rule}" for rule in plan.sanitization_rules)

    if plan.observations:
        lines.extend(["", "Observations", "-" * 52])
        lines.extend(f"- {observation}" for observation in plan.observations)

    return "\n".join(lines) + "\n"


def _plan_json_path(output_dir: Path, pipeline_run_id: str) -> Path:
    return output_dir / f"runtime_smoke_capture_plan_{_safe_pipeline_run_id(pipeline_run_id)}.json"


def _plan_text_path(output_dir: Path, pipeline_run_id: str) -> Path:
    return output_dir / f"runtime_smoke_capture_plan_{_safe_pipeline_run_id(pipeline_run_id)}.txt"


def write_runtime_smoke_capture_plan(
    plan: RuntimeSmokeCapturePlan,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write JSON and text capture plan artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = _plan_json_path(output_dir, plan.pipeline_run_id)
    text_path = _plan_text_path(output_dir, plan.pipeline_run_id)
    json_path.write_text(plan.to_json_str(indent=2) + "\n", encoding="utf-8")
    text_path.write_text(format_runtime_smoke_capture_plan_text(plan), encoding="utf-8")
    return json_path, text_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a local-safe Phase 4 Databricks runtime smoke capture plan.",
    )
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--output-root", default="output")
    parser.add_argument("--output-dir", default="output/validation")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    plan = build_runtime_smoke_capture_plan(
        pipeline_run_id=args.pipeline_run_id,
        environment=args.environment,
        output_root=Path(args.output_root),
    )
    json_path, text_path = write_runtime_smoke_capture_plan(plan, Path(args.output_dir))
    print(f"Wrote: {json_path}")
    print(f"Wrote: {text_path}")
    print("Capture plan generated. This is not runtime smoke evidence.")


if __name__ == "__main__":
    main()
