"""
test_domain_registry.py — Test suite for Phase D-0 multi-domain framework

Updated for D-1: CISA advisory is now ACTIVE. incident_report is the sole PLANNED domain.

Covers:
    - Domain registry integrity (DOMAIN_REGISTRY shape, all entries valid)
    - DomainConfig field types and constraints
    - get_domain(), get_active_domains(), get_planned_domains()
    - is_domain_active(), require_active_domain()
    - DomainNotFoundError and DomainNotImplementedError semantics
    - Extraction prompt routing: get_prompt_for_domain()
    - Schema registry integrity and get_schema_info() / build_fields_for_domain()
    - Classification taxonomy D-0 extensions: DOMAIN_ROUTING_MAP, is_domain_executable()
      resolve_routing_label_for_domain()
    - FDA behavior preservation (no regression)
    - Planned domain failure modes (incident — clean errors, not silent)
    - extract_silver.select_extractor() domain-registry routing
    - classify_gold.select_classifier() domain-registry routing
    - No accidental activation of incident execution

Phase D-0/D-1 acceptance criteria alignment:
    - FDA domain resolves correctly                         [test_fda_*]
    - Unsupported domain resolution fails cleanly           [test_planned_domain_*]
    - Domain registry integrity                             [test_registry_integrity_*]
    - Prompt selection uses new framework correctly         [test_prompt_*]
    - Classification/routing framework preserves FDA        [test_taxonomy_*]
    - No accidental activation of incident (D-1 CISA is now intentionally active)
    - Existing behavior preserved                           [test_regression_*]
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Allow running from the repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.domain_registry import (
    DOMAIN_REGISTRY,
    DomainConfig,
    DomainNotFoundError,
    DomainNotImplementedError,
    DomainStatus,
    get_active_domains,
    get_domain,
    get_planned_domains,
    is_domain_active,
    list_domain_keys,
    require_active_domain,
)
from src.schemas.domain_schema_registry import (
    DomainSchemaInfo,
    _DOMAIN_SCHEMA_REGISTRY,
    build_fields_for_domain,
    get_schema_info,
    list_schema_domain_keys,
)
from src.utils.extraction_prompts import (
    FDA_WARNING_LETTER_PROMPT_ID,
    get_prompt,
    get_prompt_for_domain,
    list_prompt_ids,
)
from src.utils.classification_taxonomy import (
    DOMAIN_ROUTING_MAP,
    DOCUMENT_TYPE_CISA_ADVISORY,
    DOCUMENT_TYPE_FDA_WARNING_LETTER,
    DOCUMENT_TYPE_INCIDENT_REPORT,
    DOCUMENT_TYPE_UNKNOWN,
    ROUTING_LABEL_INCIDENT_MANAGEMENT,
    ROUTING_LABEL_QUARANTINE,
    ROUTING_LABEL_REGULATORY_REVIEW,
    ROUTING_LABEL_SECURITY_OPS,
    V1_ROUTING_MAP,
    is_domain_executable,
    is_valid_document_type,
    is_valid_routing_label,
    resolve_routing_label,
    resolve_routing_label_for_domain,
)


# ===========================================================================
# Domain registry integrity
# ===========================================================================


class TestRegistryIntegrity:
    """Verify DOMAIN_REGISTRY is well-formed and complete."""

    def test_registry_is_not_empty(self):
        assert len(DOMAIN_REGISTRY) > 0

    def test_registry_contains_fda(self):
        assert "fda_warning_letter" in DOMAIN_REGISTRY

    def test_registry_contains_cisa(self):
        assert "cisa_advisory" in DOMAIN_REGISTRY

    def test_registry_contains_incident(self):
        assert "incident_report" in DOMAIN_REGISTRY

    def test_all_entries_are_domain_config(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert isinstance(config, DomainConfig), f"{key} is not a DomainConfig"

    def test_all_domain_keys_match_dict_keys(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.domain_key == key, (
                f"domain_key mismatch: dict key={key!r}, config.domain_key={config.domain_key!r}"
            )

    def test_all_statuses_are_valid_enum_values(self):
        valid_statuses = {DomainStatus.ACTIVE, DomainStatus.PLANNED}
        for key, config in DOMAIN_REGISTRY.items():
            assert config.status in valid_statuses, (
                f"{key} has unexpected status: {config.status!r}"
            )

    def test_all_routing_labels_are_non_empty(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.routing_label, f"{key} has empty routing_label"

    def test_all_document_type_labels_are_non_empty(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.document_type_label, f"{key} has empty document_type_label"

    def test_all_schema_families_are_non_empty(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.schema_family, f"{key} has empty schema_family"

    def test_all_source_families_are_non_empty(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.source_family, f"{key} has empty source_family"

    def test_all_descriptions_are_non_empty(self):
        for key, config in DOMAIN_REGISTRY.items():
            assert config.description, f"{key} has empty description"

    def test_exactly_one_active_domain(self):
        active = get_active_domains()
        assert len(active) >= 1, "Expected at least one ACTIVE domain"
        active_keys = [d.domain_key for d in active]
        assert "fda_warning_letter" in active_keys

    def test_planned_domains_present(self):
        planned = get_planned_domains()
        planned_keys = [d.domain_key for d in planned]
        # D-1: cisa_advisory is now ACTIVE; incident_report remains PLANNED
        assert "incident_report" in planned_keys
        assert "cisa_advisory" not in planned_keys

    def test_active_domain_has_extraction_prompt_id(self):
        for domain in get_active_domains():
            assert domain.extraction_prompt_id is not None, (
                f"ACTIVE domain '{domain.domain_key}' has no extraction_prompt_id"
            )

    def test_planned_domains_have_no_extraction_prompt_id(self):
        for domain in get_planned_domains():
            assert domain.extraction_prompt_id is None, (
                f"PLANNED domain '{domain.domain_key}' unexpectedly has extraction_prompt_id "
                f"'{domain.extraction_prompt_id}' — prompt must not be registered until the "
                "domain's implementation phase (D-1 / D-2)."
            )

    def test_domain_keys_are_sorted_in_list(self):
        keys = list_domain_keys()
        assert keys == sorted(keys)

    def test_no_duplicate_routing_labels_among_active(self):
        active = get_active_domains()
        routing_labels = [d.routing_label for d in active]
        assert len(routing_labels) == len(set(routing_labels)), (
            "Active domains share a routing_label — each active domain needs a distinct target"
        )


# ===========================================================================
# FDA domain resolution
# ===========================================================================


class TestFDADomainResolution:
    """Verify FDA is correctly resolved as ACTIVE."""

    def test_get_domain_fda(self):
        domain = get_domain("fda_warning_letter")
        assert domain.domain_key == "fda_warning_letter"

    def test_fda_is_active(self):
        assert get_domain("fda_warning_letter").status == DomainStatus.ACTIVE

    def test_fda_routing_label(self):
        assert get_domain("fda_warning_letter").routing_label == "regulatory_review"

    def test_fda_extraction_prompt_id(self):
        assert get_domain("fda_warning_letter").extraction_prompt_id == "fda_warning_letter_extract_v1"

    def test_fda_schema_family(self):
        assert get_domain("fda_warning_letter").schema_family == "fda_warning_letter"

    def test_is_domain_active_fda(self):
        assert is_domain_active("fda_warning_letter") is True

    def test_require_active_domain_fda_returns_config(self):
        domain = require_active_domain("fda_warning_letter")
        assert domain.status == DomainStatus.ACTIVE

    def test_fda_in_active_domains_list(self):
        active_keys = [d.domain_key for d in get_active_domains()]
        assert "fda_warning_letter" in active_keys

    def test_fda_not_in_planned_domains_list(self):
        planned_keys = [d.domain_key for d in get_planned_domains()]
        assert "fda_warning_letter" not in planned_keys


# ===========================================================================
# Planned domain failure modes
# ===========================================================================


class TestPlannedDomainFailures:
    """Verify PLANNED domains fail cleanly — never silently execute.
    D-1: only incident_report is PLANNED. cisa_advisory is now ACTIVE.
    """

    # D-1: incident_report is the sole remaining PLANNED domain
    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_planned_domain_is_not_active(self, domain_key):
        assert is_domain_active(domain_key) is False

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_require_active_raises_for_planned(self, domain_key):
        with pytest.raises(DomainNotImplementedError) as exc_info:
            require_active_domain(domain_key)
        assert domain_key in str(exc_info.value)
        assert exc_info.value.domain_key == domain_key

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_require_active_error_mentions_planned_status(self, domain_key):
        with pytest.raises(DomainNotImplementedError) as exc_info:
            require_active_domain(domain_key)
        msg = str(exc_info.value)
        assert DomainStatus.PLANNED.value in msg

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_planned_domain_in_planned_list(self, domain_key):
        planned_keys = [d.domain_key for d in get_planned_domains()]
        assert domain_key in planned_keys

    def test_domain_not_found_raises_correct_error(self):
        with pytest.raises(DomainNotFoundError) as exc_info:
            get_domain("nonexistent_domain")
        assert "nonexistent_domain" in str(exc_info.value)

    def test_is_domain_active_unregistered_returns_false(self):
        assert is_domain_active("not_a_real_domain") is False

    def test_require_active_domain_unregistered_raises_not_found(self):
        with pytest.raises(DomainNotFoundError):
            require_active_domain("not_a_real_domain")

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_domain_not_implemented_error_has_operation(self, domain_key):
        with pytest.raises(DomainNotImplementedError) as exc_info:
            require_active_domain(domain_key, operation="test_op")
        assert exc_info.value.operation == "test_op"

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_get_domain_still_works_for_planned(self, domain_key):
        """get_domain returns config without error — status check is caller's job."""
        domain = get_domain(domain_key)
        assert domain.status == DomainStatus.PLANNED

    def test_cisa_is_active_not_planned(self):
        """D-1: cisa_advisory graduated from PLANNED to ACTIVE."""
        assert is_domain_active("cisa_advisory") is True
        planned_keys = [d.domain_key for d in get_planned_domains()]
        assert "cisa_advisory" not in planned_keys


# ===========================================================================
# Extraction prompt routing (D-0)
# ===========================================================================


class TestPromptRouting:
    """Verify get_prompt_for_domain() routes correctly through domain registry."""

    def test_fda_prompt_via_domain(self):
        prompt = get_prompt_for_domain("fda_warning_letter")
        assert prompt.prompt_id == FDA_WARNING_LETTER_PROMPT_ID

    def test_fda_prompt_domain_matches(self):
        prompt = get_prompt_for_domain("fda_warning_letter")
        assert prompt.document_domain == "fda_warning_letter"

    def test_fda_prompt_has_template(self):
        prompt = get_prompt_for_domain("fda_warning_letter")
        assert "{parsed_text}" in prompt.template

    def test_fda_prompt_direct_vs_domain_equivalent(self):
        """get_prompt_for_domain and get_prompt(FDA_WARNING_LETTER_PROMPT_ID) are equivalent."""
        via_domain = get_prompt_for_domain("fda_warning_letter")
        via_id = get_prompt(FDA_WARNING_LETTER_PROMPT_ID)
        assert via_domain == via_id

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_planned_domain_prompt_raises_not_implemented(self, domain_key):
        # D-1: cisa_advisory now has a registered prompt — only incident remains PLANNED
        with pytest.raises(DomainNotImplementedError):
            get_prompt_for_domain(domain_key)

    def test_unregistered_domain_prompt_raises_not_found(self):
        with pytest.raises(DomainNotFoundError):
            get_prompt_for_domain("unknown_domain_xyz")

    def test_prompt_ids_list_contains_fda(self):
        assert FDA_WARNING_LETTER_PROMPT_ID in list_prompt_ids()

    def test_existing_get_prompt_still_works(self):
        """Backward compatibility: existing get_prompt(id) call is unchanged."""
        prompt = get_prompt(FDA_WARNING_LETTER_PROMPT_ID)
        assert prompt.prompt_id == FDA_WARNING_LETTER_PROMPT_ID


# ===========================================================================
# Schema registry (D-0)
# ===========================================================================


class TestSchemaRegistry:
    """Verify domain_schema_registry integrity and routing."""

    def test_schema_registry_contains_fda(self):
        assert "fda_warning_letter" in _DOMAIN_SCHEMA_REGISTRY

    def test_schema_registry_contains_cisa(self):
        assert "cisa_advisory" in _DOMAIN_SCHEMA_REGISTRY

    def test_schema_registry_contains_incident(self):
        assert "incident_report" in _DOMAIN_SCHEMA_REGISTRY

    def test_all_entries_are_domain_schema_info(self):
        for key, info in _DOMAIN_SCHEMA_REGISTRY.items():
            assert isinstance(info, DomainSchemaInfo), f"{key} is not DomainSchemaInfo"

    def test_fda_schema_info_active(self):
        info = get_schema_info("fda_warning_letter")
        assert info.status == DomainStatus.ACTIVE

    def test_fda_required_fields_non_empty(self):
        info = get_schema_info("fda_warning_letter")
        assert len(info.required_fields) > 0

    def test_fda_optional_fields_non_empty(self):
        info = get_schema_info("fda_warning_letter")
        assert len(info.optional_fields) > 0

    def test_fda_all_fields_is_union(self):
        info = get_schema_info("fda_warning_letter")
        for f in info.required_fields:
            assert f in info.all_fields
        for f in info.optional_fields:
            assert f in info.all_fields

    def test_fda_required_includes_issuing_office(self):
        assert "issuing_office" in get_schema_info("fda_warning_letter").required_fields

    def test_fda_required_includes_corrective_action(self):
        assert "corrective_action_requested" in get_schema_info("fda_warning_letter").required_fields

    def test_cisa_required_fields_defined(self):
        info = get_schema_info("cisa_advisory")
        assert "advisory_id" in info.required_fields
        assert "severity_level" in info.required_fields

    def test_incident_required_fields_defined(self):
        info = get_schema_info("incident_report")
        assert "incident_date" in info.required_fields
        assert "severity" in info.required_fields

    def test_build_fields_for_fda_returns_model(self):
        raw = {
            "issuing_office": "FDA Chicago District",
            "recipient_company": "Acme Pharma",
            "issue_date": "2025-01-15",
            "violation_type": ["CGMP Violations"],
            "corrective_action_requested": True,
        }
        model = build_fields_for_domain("fda_warning_letter", raw)
        assert model.issuing_office == "FDA Chicago District"
        assert model.recipient_company == "Acme Pharma"

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_build_fields_for_planned_raises_not_implemented(self, domain_key):
        # D-1: cisa_advisory now builds fields successfully — only incident remains PLANNED
        with pytest.raises(DomainNotImplementedError):
            build_fields_for_domain(domain_key, {})

    def test_build_fields_for_unregistered_raises_not_found(self):
        with pytest.raises(DomainNotFoundError):
            build_fields_for_domain("nonexistent", {})

    def test_get_schema_info_planned_works_for_introspection(self):
        """get_schema_info on a PLANNED domain returns info — useful for docs/planning."""
        info = get_schema_info("incident_report")
        assert info.status == DomainStatus.PLANNED
        assert len(info.required_fields) > 0  # field contract defined even if model isn't

    def test_cisa_schema_info_active(self):
        """D-1: cisa_advisory schema is now ACTIVE — build_fields_for_domain works."""
        from src.schemas.silver_schema import CISAAdvisoryFields
        info = get_schema_info("cisa_advisory")
        assert info.status == DomainStatus.ACTIVE
        model = build_fields_for_domain("cisa_advisory", {
            "advisory_id": "ICSA-24-001-01",
            "title": "Test",
            "published_date": "2024-01-01",
            "severity_level": "High",
            "remediation_available": True,
        })
        assert isinstance(model, CISAAdvisoryFields)

    def test_list_schema_domain_keys_sorted(self):
        keys = list_schema_domain_keys()
        assert keys == sorted(keys)

    def test_schema_registry_keys_match_domain_registry(self):
        """Schema registry must cover all domains in DOMAIN_REGISTRY."""
        schema_keys = set(list_schema_domain_keys())
        registry_keys = set(list_domain_keys())
        assert registry_keys.issubset(schema_keys), (
            f"Domains in DOMAIN_REGISTRY without schema entries: "
            f"{registry_keys - schema_keys}"
        )


# ===========================================================================
# Classification taxonomy D-0 extensions
# ===========================================================================


class TestTaxonomyD0Extensions:
    """Verify D-0 additions to classification_taxonomy.py."""

    def test_domain_routing_map_contains_fda(self):
        assert DOCUMENT_TYPE_FDA_WARNING_LETTER in DOMAIN_ROUTING_MAP

    def test_domain_routing_map_fda_routes_regulatory(self):
        assert DOMAIN_ROUTING_MAP[DOCUMENT_TYPE_FDA_WARNING_LETTER] == ROUTING_LABEL_REGULATORY_REVIEW

    def test_domain_routing_map_contains_cisa(self):
        assert DOCUMENT_TYPE_CISA_ADVISORY in DOMAIN_ROUTING_MAP

    def test_domain_routing_map_cisa_routes_security_ops(self):
        assert DOMAIN_ROUTING_MAP[DOCUMENT_TYPE_CISA_ADVISORY] == ROUTING_LABEL_SECURITY_OPS

    def test_domain_routing_map_contains_incident(self):
        assert DOCUMENT_TYPE_INCIDENT_REPORT in DOMAIN_ROUTING_MAP

    def test_domain_routing_map_incident_routes_incident_management(self):
        assert DOMAIN_ROUTING_MAP[DOCUMENT_TYPE_INCIDENT_REPORT] == ROUTING_LABEL_INCIDENT_MANAGEMENT

    def test_is_domain_executable_fda(self):
        assert is_domain_executable("fda_warning_letter") is True

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_is_domain_executable_planned(self, domain_key):
        # D-1: cisa_advisory is now executable — only incident remains non-executable
        assert is_domain_executable(domain_key) is False

    def test_is_domain_executable_cisa_now_true(self):
        """D-1: cisa_advisory is now executable."""
        assert is_domain_executable("cisa_advisory") is True

    def test_is_domain_executable_unregistered(self):
        assert is_domain_executable("not_a_domain") is False

    def test_resolve_routing_label_for_domain_fda(self):
        result = resolve_routing_label_for_domain("fda_warning_letter", "fda_warning_letter")
        assert result == ROUTING_LABEL_REGULATORY_REVIEW

    def test_resolve_routing_label_for_domain_unknown_type(self):
        """Unknown document type → quarantine, regardless of domain."""
        result = resolve_routing_label_for_domain("fda_warning_letter", DOCUMENT_TYPE_UNKNOWN)
        assert result == ROUTING_LABEL_QUARANTINE

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_resolve_routing_label_for_planned_domain_raises(self, domain_key):
        # D-1: cisa_advisory now resolves to security_ops — only incident still raises
        with pytest.raises(DomainNotImplementedError):
            resolve_routing_label_for_domain(domain_key, domain_key)

    def test_resolve_routing_label_for_unregistered_domain_raises_not_found(self):
        with pytest.raises(DomainNotFoundError):
            resolve_routing_label_for_domain("not_registered", "fda_warning_letter")


# ===========================================================================
# V1 / FDA behavior preservation (regression tests)
# ===========================================================================


class TestFDABehaviorPreservation:
    """Verify all V1 FDA behavior is unchanged after D-0 refactor."""

    def test_v1_routing_map_preserved(self):
        assert V1_ROUTING_MAP[DOCUMENT_TYPE_FDA_WARNING_LETTER] == ROUTING_LABEL_REGULATORY_REVIEW

    def test_resolve_routing_label_fda_unchanged(self):
        assert resolve_routing_label("fda_warning_letter") == ROUTING_LABEL_REGULATORY_REVIEW

    def test_resolve_routing_label_unknown_quarantine(self):
        assert resolve_routing_label(DOCUMENT_TYPE_UNKNOWN) == ROUTING_LABEL_QUARANTINE

    def test_resolve_routing_label_unrecognized_quarantine(self):
        assert resolve_routing_label("something_else") == ROUTING_LABEL_QUARANTINE

    def test_is_valid_document_type_fda(self):
        assert is_valid_document_type("fda_warning_letter") is True

    def test_is_valid_routing_label_regulatory(self):
        assert is_valid_routing_label("regulatory_review") is True

    def test_is_valid_routing_label_quarantine(self):
        assert is_valid_routing_label("quarantine") is True

    def test_fda_prompt_unchanged(self):
        prompt = get_prompt(FDA_WARNING_LETTER_PROMPT_ID)
        assert "FDA warning letter" in prompt.description
        assert "{parsed_text}" in prompt.template


# ===========================================================================
# extract_silver.select_extractor() domain-registry routing
# ===========================================================================


class TestExtractSilverSelectExtractor:
    """Verify select_extractor() in extract_silver.py uses domain registry."""

    def test_fda_hint_returns_extractor(self):
        from src.pipelines.extract_silver import select_extractor, LocalFDAWarningLetterExtractor
        extractor = select_extractor("fda_warning_letter")
        assert isinstance(extractor, LocalFDAWarningLetterExtractor)

    def test_none_hint_defaults_to_fda(self):
        from src.pipelines.extract_silver import select_extractor, LocalFDAWarningLetterExtractor
        extractor = select_extractor(None)
        assert isinstance(extractor, LocalFDAWarningLetterExtractor)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_planned_domain_raises_not_implemented(self, domain_key):
        # D-1: cisa_advisory now has a real extractor — only incident still raises
        from src.pipelines.extract_silver import select_extractor
        with pytest.raises(DomainNotImplementedError):
            select_extractor(domain_key)

    def test_unregistered_domain_raises_value_error(self):
        from src.pipelines.extract_silver import select_extractor
        with pytest.raises(ValueError):
            select_extractor("totally_unknown_domain")

    def test_fda_extractor_model_id_unchanged(self):
        from src.pipelines.extract_silver import select_extractor
        extractor = select_extractor("fda_warning_letter")
        assert extractor.model_id == "local_rule_extractor/v1"


# ===========================================================================
# classify_gold.select_classifier() domain-registry routing
# ===========================================================================


class TestClassifyGoldSelectClassifier:
    """Verify select_classifier() in classify_gold.py uses domain registry."""

    def test_fda_hint_returns_classifier(self):
        from src.pipelines.classify_gold import select_classifier, LocalFDAWarningLetterClassifier
        classifier = select_classifier("fda_warning_letter")
        assert isinstance(classifier, LocalFDAWarningLetterClassifier)

    def test_none_hint_defaults_to_fda(self):
        from src.pipelines.classify_gold import select_classifier, LocalFDAWarningLetterClassifier
        classifier = select_classifier(None)
        assert isinstance(classifier, LocalFDAWarningLetterClassifier)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_planned_domain_raises_not_implemented(self, domain_key):
        # D-1: cisa_advisory now has a real classifier — only incident still raises
        from src.pipelines.classify_gold import select_classifier
        with pytest.raises(DomainNotImplementedError):
            select_classifier(domain_key)

    def test_unregistered_domain_raises_value_error(self):
        from src.pipelines.classify_gold import select_classifier
        with pytest.raises(ValueError):
            select_classifier("totally_unknown_domain")

    def test_fda_classifier_model_id_unchanged(self):
        from src.pipelines.classify_gold import select_classifier
        classifier = select_classifier("fda_warning_letter")
        assert classifier.model_id == "local_rule_classifier/v1"


# ===========================================================================
# No accidental activation of CISA / incident execution
# ===========================================================================


class TestNoAccidentalActivation:
    """
    Verify PLANNED domains cannot accidentally be executed.

    D-1 update: CISA advisory is now intentionally ACTIVE — it is no longer
    in these "planned guard" tests. incident_report is the sole remaining PLANNED
    domain. These tests confirm incident cannot run extraction, schema construction,
    classification, or routing without explicit D-2 implementation.
    """

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_extraction_not_runnable(self, domain_key):
        from src.pipelines.extract_silver import select_extractor
        with pytest.raises(DomainNotImplementedError):
            select_extractor(domain_key)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_classification_not_runnable(self, domain_key):
        from src.pipelines.classify_gold import select_classifier
        with pytest.raises(DomainNotImplementedError):
            select_classifier(domain_key)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_schema_construction_not_runnable(self, domain_key):
        with pytest.raises(DomainNotImplementedError):
            build_fields_for_domain(domain_key, {})

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_prompt_selection_not_runnable(self, domain_key):
        with pytest.raises(DomainNotImplementedError):
            get_prompt_for_domain(domain_key)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_routing_not_runnable(self, domain_key):
        with pytest.raises(DomainNotImplementedError):
            resolve_routing_label_for_domain(domain_key, domain_key)

    @pytest.mark.parametrize("domain_key", ["incident_report"])
    def test_require_active_domain_blocks_planned(self, domain_key):
        with pytest.raises(DomainNotImplementedError):
            require_active_domain(domain_key)

    def test_incident_is_not_in_active_domains(self):
        active_keys = [d.domain_key for d in get_active_domains()]
        assert "incident_report" not in active_keys

    def test_cisa_is_now_in_active_domains(self):
        """D-1: cisa_advisory graduated to ACTIVE — it IS in active domains now."""
        active_keys = [d.domain_key for d in get_active_domains()]
        assert "cisa_advisory" in active_keys


# ===========================================================================
# DomainConfig structural validation
# ===========================================================================


class TestDomainConfigStructure:
    """Verify DomainConfig is a NamedTuple and immutable."""

    def test_fda_config_is_named_tuple(self):
        domain = get_domain("fda_warning_letter")
        assert isinstance(domain, DomainConfig)
        assert isinstance(domain, tuple)

    def test_domain_config_is_immutable(self):
        domain = get_domain("fda_warning_letter")
        with pytest.raises((AttributeError, TypeError)):
            domain.status = DomainStatus.PLANNED  # type: ignore[misc]

    def test_domain_status_enum_values(self):
        assert DomainStatus.ACTIVE.value == "active"
        assert DomainStatus.PLANNED.value == "planned"

    def test_domain_config_fields(self):
        domain = get_domain("fda_warning_letter")
        assert hasattr(domain, "domain_key")
        assert hasattr(domain, "document_type_label")
        assert hasattr(domain, "routing_label")
        assert hasattr(domain, "source_family")
        assert hasattr(domain, "extraction_prompt_id")
        assert hasattr(domain, "schema_family")
        assert hasattr(domain, "status")
        assert hasattr(domain, "description")
