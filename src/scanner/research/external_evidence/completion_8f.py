from __future__ import annotations

from typing import Any, Mapping

from scanner.research.external_evidence.macro_exposure_8f import validate_exposure_mapping


SCHEMA_VERSION = "external_evidence_8f_completion_v1"


class Completion8FError(ValueError):
    pass


def _required_bool(mapping: Mapping[str, Any], key: str, expected: bool) -> None:
    if mapping.get(key) is not expected:
        raise Completion8FError(f"completion requirement {key} must be {expected}")


def evaluate_phase8f_completion(
    *,
    completion_config: Mapping[str, Any],
    macro_config: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
    macro_ledger: Mapping[str, Any] | None,
    exposure_domain_audit: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if completion_config.get("schema_version") != SCHEMA_VERSION:
        raise Completion8FError("unexpected Phase 8F completion schema_version")

    requirements = completion_config.get("requirements") or {}
    for key in (
        "foundation_contract_must_pass",
        "source_routing_contract_must_pass",
        "series_catalog_contract_must_pass",
        "market_proxy_contract_must_pass",
        "real_macro_ledger_required",
        "reviewed_exposure_mapping_required",
        "research_domain_coverage_must_be_explicit",
        "research_domain_must_be_pinned_to_pre8f_source_blob",
        "domain_subjects_must_be_accounted_for_as_mapped_or_explicitly_unmapped",
    ):
        _required_bool(requirements, key, True)
    for key in (
        "market_outcomes_may_be_read",
        "market_direction_may_be_assigned",
        "threshold_selection_enabled",
        "phase7_integration_enabled",
        "production_external_evidence_enabled",
    ):
        _required_bool(requirements, key, False)

    factor_ids = {
        str(item.get("factor_id"))
        for item in macro_config.get("factor_catalog") or []
        if item.get("factor_id")
    }
    mappings = exposure_map.get("mappings") or []
    normalized_mappings = [
        validate_exposure_mapping(item, allowed_factor_ids=factor_ids)
        for item in mappings
    ]
    active_reviewed = [item for item in normalized_mappings if item["review_status"] == "ACTIVE"]

    blockers: list[str] = []
    minimum_mappings = int(requirements.get("minimum_active_reviewed_mappings") or 0)
    if len(active_reviewed) < minimum_mappings:
        blockers.append(
            f"reviewed_exposure_mappings:{len(active_reviewed)}<{minimum_mappings}"
        )

    minimum_rows = int(requirements.get("minimum_knowable_ledger_rows") or 0)
    minimum_series = int(requirements.get("minimum_distinct_ledger_series") or 0)
    if macro_ledger is None:
        blockers.append("real_macro_ledger:missing")
        ledger_rows = 0
        ledger_series = 0
    else:
        if macro_ledger.get("schema_version") != "external_evidence_8f_macro_ledger_v1":
            raise Completion8FError("unexpected macro ledger schema_version")
        if macro_ledger.get("status") != "OUTCOME_BLIND_APPEND_ONLY_MACRO_LEDGER":
            raise Completion8FError("macro ledger is not an outcome-blind append-only ledger")
        guards = macro_ledger.get("guards") or {}
        if guards.get("market_outcomes_read") is not False:
            raise Completion8FError("macro ledger must remain outcome-blind")
        if guards.get("later_revision_overwrites_original") is not False:
            raise Completion8FError("macro ledger may not overwrite earlier revisions")
        ledger_rows = int(macro_ledger.get("knowable_row_count") or 0)
        ledger_series = int((macro_ledger.get("coverage") or {}).get("series_count") or 0)
        if ledger_rows < minimum_rows:
            blockers.append(f"knowable_ledger_rows:{ledger_rows}<{minimum_rows}")
        if ledger_series < minimum_series:
            blockers.append(f"ledger_series:{ledger_series}<{minimum_series}")

    domain_pinned = False
    if exposure_domain_audit is None:
        blockers.append("exposure_domain_audit:missing")
        domain_count = 0
        accounted_count = 0
    else:
        if exposure_domain_audit.get("schema_version") != "external_evidence_8f_exposure_domain_audit_v1":
            raise Completion8FError("unexpected exposure-domain audit schema_version")
        domain_count = int(exposure_domain_audit.get("domain_subject_count") or 0)
        mapped_count = int(exposure_domain_audit.get("mapped_subject_count") or 0)
        explicit_unmapped_count = int(exposure_domain_audit.get("explicit_unmapped_subject_count") or 0)
        accounted_count = mapped_count + explicit_unmapped_count
        domain_guards = exposure_domain_audit.get("guards") or {}
        domain_pinned = domain_guards.get("domain_pinned_to_pre8f_source_blob") is True
        if not domain_pinned:
            blockers.append("research_domain:not_pinned_to_pre8f_source_blob")
        if domain_count <= 0:
            blockers.append("research_domain:empty")
        if accounted_count != domain_count:
            blockers.append(
                f"domain_accounting:{accounted_count}!={domain_count}"
            )
        if exposure_domain_audit.get("market_outcomes_read") is not False:
            raise Completion8FError("exposure-domain audit must remain outcome-blind")

    passed = not blockers
    return {
        "schema_version": "external_evidence_8f_completion_gate_v1",
        "phase": "8F_macro_exposure_context",
        "status": "PASS_8F_COMPLETION" if passed else "BLOCKED_8F_COMPLETION",
        "freeze_allowed": passed,
        "blockers": blockers,
        "metrics": {
            "active_reviewed_mapping_count": len(active_reviewed),
            "knowable_ledger_row_count": ledger_rows,
            "ledger_series_count": ledger_series,
            "domain_subject_count": domain_count,
            "domain_accounted_count": accounted_count,
            "domain_pinned_to_pre8f_source_blob": domain_pinned,
        },
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }
