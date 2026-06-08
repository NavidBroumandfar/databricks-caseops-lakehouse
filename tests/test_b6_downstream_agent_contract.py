"""
tests/test_b6_downstream_agent_contract.py — Phase 6 downstream compatibility tests.

The module exercises a local, no-runtime consumer simulation against the exported
handoff payload contract.

These tests validate that downstream teams can implement ingestion deterministically
without Bedrock SDK/runtime assumptions:
- contract-valid payloads route to expected active consumers
- invalid payloads fail upstream contract gates
- routing/document-type mismatches are explicit and actionable
- planned/unsupported routing labels produce explicit failure semantics
- non-consumable governance signals (quarantine) are clearly surfaced
- additive/unknown fields can be surfaced as warnings, not hard failures

What these tests do NOT imply:
- No Bedrock SDK usage
- No live retrieval, indexing, vector store, or agent orchestration
- No AWS credentials or workspace URLs are introduced
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.schemas.downstream_consumer_contract import (
    SIMULATION_STATUS_ACCEPTED,
    SIMULATION_STATUS_REJECTED_CONTRACT,
    SIMULATION_STATUS_ROUTING_MISMATCH,
    SIMULATION_STATUS_ROUTING_UNKNOWN,
    SIMULATION_STATUS_ROUTING_PLANNED,
    SIMULATION_STATUS_QUARANTINE,
    simulate_downstream_consumption,
)
from src.utils.classification_taxonomy import (
    DOCUMENT_TYPE_CISA_ADVISORY,
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_INCIDENT_REPORT,
    DOCUMENT_TYPE_TECHNICAL_CASE,
    ROUTING_LABEL_INCIDENT_MANAGEMENT,
    ROUTING_LABEL_KNOWLEDGE_BASE,
    ROUTING_LABEL_REGULATORY_REVIEW,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_fixture(filename: str) -> dict:
    payload = json.loads(
        (REPO_ROOT / "examples" / filename).read_text(encoding="utf-8")
    )
    payload.pop("_note", None)
    return payload


class TestDownstreamActiveRoutingSimulation:
    """Active routing labels must map to active consumers."""

    def test_active_payloads_route_to_expected_downstream_consumer(self):
        fixtures = [
            (
                "contract_valid_fda_export_payload.json",
                ROUTING_LABEL_REGULATORY_REVIEW,
                "Bedrock regulatory intelligence index",
                DOCUMENT_TYPE_FDA_WARNING_LETTER,
            ),
            (
                "contract_valid_cisa_export_payload.json",
                "security_ops",
                "Bedrock security operations index",
                DOCUMENT_TYPE_CISA_ADVISORY,
            ),
            (
                "contract_valid_incident_export_payload.json",
                ROUTING_LABEL_INCIDENT_MANAGEMENT,
                "Bedrock incident management workflow",
                DOCUMENT_TYPE_INCIDENT_REPORT,
            ),
        ]

        for fixture_name, expected_routing, expected_consumer, expected_doc in fixtures:
            payload = _load_fixture(fixture_name)
            result = simulate_downstream_consumption(payload)
            assert result.accepted is True
            assert result.status == SIMULATION_STATUS_ACCEPTED
            assert result.routing_label == expected_routing
            assert result.downstream_consumer == expected_consumer
            assert payload["document_type"] == expected_doc

    def test_invalid_payload_fails_upstream_contract_gate(self):
        """Contract violations must fail before routing simulation runs."""
        payload = _load_fixture("invalid_export_payload_missing_fields.json")
        result = simulate_downstream_consumption(payload)

        assert result.accepted is False
        assert result.status == SIMULATION_STATUS_REJECTED_CONTRACT
        assert any("violation_type" in error for error in result.errors)
        assert any("corrective_action_requested" in error for error in result.errors)

    def test_routing_document_type_mismatch_is_rejected(self):
        """The same document_type must route to its documented routing target."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["routing_label"] = ROUTING_LABEL_INCIDENT_MANAGEMENT

        result = simulate_downstream_consumption(payload)

        assert result.accepted is False
        assert result.status == SIMULATION_STATUS_ROUTING_MISMATCH
        assert "incident_management" in result.errors[0]
        assert "document_type" in result.errors[0]

    def test_planned_routing_target_returns_explicit_not_ready_status(self):
        """Planned routing targets are explicit, deterministic, and non-blocking."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["document_type"] = DOCUMENT_TYPE_TECHNICAL_CASE
        payload["routing_label"] = ROUTING_LABEL_KNOWLEDGE_BASE
        payload["extracted_fields"] = {"notes": "planned-domain smoke fixture"}

        result = simulate_downstream_consumption(payload)

        assert result.accepted is False
        assert result.status == SIMULATION_STATUS_ROUTING_PLANNED
        assert "not active" in result.errors[0].lower()

    def test_unknown_routing_labels_are_rejected_with_actionable_reason(self):
        """Unknown routing labels must be rejected as contract-incompatible."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["routing_label"] = "security_threat_ops"

        result = simulate_downstream_consumption(payload)

        assert result.accepted is False
        assert result.status == SIMULATION_STATUS_ROUTING_UNKNOWN
        assert "not in the downstream routing contract" in result.errors[0]


class TestDownstreamFailureSemantics:
    """Validate recoverable consumer-facing failure behavior."""

    def test_quarantine_payloads_are_governance_signals(self):
        """routing_label='quarantine' is a governance-only signal and not forwarded."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["routing_label"] = "quarantine"
        result = simulate_downstream_consumption(payload)

        assert result.accepted is False
        assert result.status == SIMULATION_STATUS_QUARANTINE
        assert any("governance" in error for error in result.errors)

    def test_unknown_top_level_fields_emit_warnings_only(self):
        """Additive schema evolution should be forward-compatible at downstream boundary."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["debug_observed_at"] = "2034-01-01T00:00:00Z"

        result = simulate_downstream_consumption(payload)

        assert result.accepted is True
        assert result.status == SIMULATION_STATUS_ACCEPTED
        assert any("debug_observed_at" in w for w in result.warnings)

    def test_v0_2_0_delivery_fields_are_accepted_for_optional_compatibility(self):
        """v0.2.0 provenance delivery fields are optional and should not fail routing."""
        payload = _load_fixture("contract_valid_fda_export_payload.json")
        payload["provenance"].update(
            {
                "delivery_mechanism": "delta_sharing",
                "delta_share_name": "caseops_handoff",
                "delivery_event_id": "evt-1234",
            }
        )

        result = simulate_downstream_consumption(payload)

        assert result.accepted is True
        assert result.status == SIMULATION_STATUS_ACCEPTED
        assert result.downstream_consumer == "Bedrock regulatory intelligence index"

        # Ensure these known optional fields are not treated as hard errors.
        assert result.warnings == []
