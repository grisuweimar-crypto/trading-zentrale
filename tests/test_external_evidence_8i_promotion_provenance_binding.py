from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.decision_binding_8i import (
    BOUND,
    MAIN_EFFECT_AUTH_SCHEMA,
    SOURCE_IDENTITY_CORRECTION_SCHEMA,
    UPSTREAM_IDENTITY_BLOCKED,
    ExternalEvidence8IBindingError,
    bind_promoted_component,
    current_binding_status,
    digest,
    record_main_effect_authorization,
    validate_binding_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_promotion_provenance_binding_v1.json"
A_CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_decision_extension_contract_v1.json"
PROMOTION_8G_PATH = ROOT / "configs" / "external_evidence_8g_promotion_review_v1.json"
PROMOTION_8H_PATH = ROOT / "configs" / "external_evidence_8h_interaction_promotion_review_v1.json"
CHALLENGER_PATH = ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json"
REGISTRY_8A_PATH = ROOT / "configs" / "external_source_registry_v1.json"
SOURCE_CANDIDATES_PATH = ROOT / "configs" / "external_evidence_8f_source_candidates_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _with_digest(row: dict, field: str) -> dict:
    out = dict(row)
    out[field] = digest(out)
    return out


def _8g_decision(component_id: str = "rates_policy_x_20t") -> dict:
    return _with_digest(
        {
            "schema_version": "external_evidence_8g_promotion_decision_v1",
            "phase": "8G-H",
            "hypothesis_id": component_id,
            "packet_sha256": "a" * 64,
            "decision": "APPROVE_FOR_8H_RESEARCH_ONLY",
            "state": "APPROVED_FOR_8H_RESEARCH_ONLY",
            "reviewer_identity": "synthetic-test-reviewer",
            "reviewed_at": "2026-09-28T12:00:00+00:00",
            "rationale": "Synthetic metadata-only test fixture.",
            "authorizes_8h_research": True,
            "authorizes_production": False,
            "authorizes_phase7_mutation": False,
            "authorizes_8i_integration": False,
            "authorizes_orders_or_trades": False,
            "rejection_authorizes_inverse_signal": False,
        },
        "decision_sha256",
    )


def _8h_decision(component_id: str = "rates_policy__fx_x_20t") -> dict:
    return _with_digest(
        {
            "schema_version": "external_evidence_8h_interaction_promotion_decision_v1",
            "phase": "8H-H",
            "hypothesis_id": component_id,
            "packet_sha256": "b" * 64,
            "decision": "APPROVE_FOR_8I_RESEARCH_ONLY",
            "state": "APPROVED_FOR_8I_RESEARCH_ONLY",
            "reviewer_identity": "synthetic-test-reviewer",
            "reviewed_at": "2026-09-28T12:05:00+00:00",
            "rationale": "Synthetic metadata-only test fixture.",
            "authorizes_8i_research_design": True,
            "authorizes_8i_integration": False,
            "authorizes_production": False,
            "authorizes_phase7_mutation": False,
            "authorizes_orders_or_trades": False,
            "rejection_authorizes_inverse_signal": False,
        },
        "decision_sha256",
    )


def _correction() -> dict:
    return _with_digest(
        {
            "schema_version": SOURCE_IDENTITY_CORRECTION_SCHEMA,
            "phase": "UPSTREAM-FORWARD-CORRECTION",
            "state": "RESOLVED_OUTCOME_BLIND_VERSIONED",
            "alias_to_canonical": {
                "FED_H15": "federal_reserve_board_h15",
                "ECB_EXR": "ecb_data_portal",
            },
            "outcomes_read": False,
            "retroactive_evidence_rewrite": False,
            "rationale": "Synthetic forward correction receipt used only to exercise the binding contract.",
        },
        "correction_sha256",
    )


def _source_row(source_id: str) -> dict:
    catalog = _load(SOURCE_CANDIDATES_PATH)
    return next(dict(row) for row in catalog["sources"] if row["source_id"] == source_id)


def _provenance(*, decision_sha: str, source_ids: list[str], component_artifact: dict, mapping_artifact: dict, auth_sha: str | None) -> dict:
    source_hashes = {source_id: digest(_source_row(source_id)) for source_id in source_ids}
    return {
        "model_or_spec_version": "synthetic-v1",
        "component_artifact_hash": digest(component_artifact),
        "upstream_promotion_receipt_sha256": decision_sha,
        "8i_authorization_receipt_sha256_if_required": auth_sha,
        "source_ids": source_ids,
        "source_identity_hashes": source_hashes,
        "as_of": "2026-09-28T13:00:00+00:00",
        "valid_from": "2026-09-28T12:30:00+00:00",
        "mapping_version": "synthetic-map-v1",
        "mapping_artifact_hash": digest(mapping_artifact),
        "pit_status": "PROSPECTIVE_SAFE",
        "evidence_consumption_status": "upstream_promotion_evidence_not_independent_8i_confirmation",
    }


def test_8i_b_contract_is_outcome_blind_binding_only() -> None:
    contract = _load(CONTRACT_PATH)
    validate_binding_contract(contract)
    assert contract["schema_version"] == "external_evidence_8i_promotion_provenance_binding_v1"
    assert contract["phase"] == "8I-B"
    assert contract["status"] == "FROZEN_OUTCOME_BLIND_BINDING_ONLY"
    assert contract["research_only"] is True
    assert contract["shadow_mode"] is True
    assert contract["real_decision_outcome_read_allowed"] is False
    assert contract["external_state_engine_enabled"] is False
    assert contract["extended_reliability_enabled"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False
    assert contract["outcome_blind_boundary"]["aggregation_belongs_to"] == "8I-C"


def test_8i_b_parent_and_upstream_contracts_are_exactly_bound() -> None:
    contract = _load(CONTRACT_PATH)
    parent = contract["parent_8i_a"]
    assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]
    assert _load(A_CONTRACT_PATH)["status"] == parent["required_status"]

    for name in (
        "8g_promotion_review",
        "8h_interaction_promotion_review",
        "8g_challenger_specs",
        "source_registry_8a",
        "source_candidates_8f",
    ):
        row = contract["upstream_contracts"][name]
        assert _git_blob_sha(row["path"]) == row["git_blob_sha"], name


def test_confirmed_upstream_source_identity_mismatch_is_classified_as_technical_not_empirical() -> None:
    contract = _load(CONTRACT_PATH)
    challenger = _load(CHALLENGER_PATH)
    registry = _load(REGISTRY_8A_PATH)
    candidates = _load(SOURCE_CANDIDATES_PATH)

    aliases = {
        challenger["factor_specs"]["rates_policy"]["source_id"],
        challenger["factor_specs"]["yield_curve"]["source_id"],
        challenger["factor_specs"]["fx"]["source_id"],
    }
    registry_ids = {row["source_id"] for row in registry["sources"]}
    candidate_ids = {row["source_id"] for row in candidates["sources"]}

    assert aliases == {"FED_H15", "ECB_EXR"}
    assert aliases.isdisjoint(registry_ids)
    assert {"federal_reserve_board_h15", "ecb_data_portal"}.issubset(candidate_ids)

    issue = contract["upstream_source_identity_issue"]
    assert issue["status"] == "OPEN_TECHNICAL_UPSTREAM_CONTRACT_BLOCKER"
    assert issue["classification"] == "TECHNICAL_SOURCE_IDENTITY_CONTRACT_ERROR_NOT_EMPIRICAL_EVIDENCE_FAILURE"
    assert issue["8i_b_may_treat_alias_map_as_retroactive_upstream_repair"] is False
    assert issue["8i_b_may_bind_8g_main_effect_while_upstream_identity_blocker_is_open"] is False
    assert issue["required_resolution"].startswith("Separate versioned upstream source-identity correction")


def test_current_repository_state_is_fail_closed_and_phase7_neutral() -> None:
    contract = _load(CONTRACT_PATH)
    status = current_binding_status(contract)
    assert status["state"] == UPSTREAM_IDENTITY_BLOCKED
    assert status["bound_8g_main_effect_ids"] == []
    assert status["bound_8h_interaction_ids"] == []
    assert status["external_evidence_state"] == "INSUFFICIENT_EXTERNAL"
    assert status["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert status["external_decision_influence_enabled"] is False
    assert status["phase7_mutation_authorized"] is False
    assert status["portfolio_action_change_authorized"] is False
    assert status["orders_or_trades_authorized"] is False


def test_8g_approval_is_not_transitively_promoted_into_8i() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8g_decision()
    component = {
        "component_type": "8g_main_effect",
        "component_id": "rates_policy_x_20t",
        "factor_id": "rates_policy",
        "horizon_sessions": 20,
    }
    component_artifact = {"spec": "synthetic-main-effect"}
    mapping_artifact = {"mapping": "synthetic"}
    provenance = _provenance(
        decision_sha=upstream["decision_sha256"],
        source_ids=["federal_reserve_board_h15"],
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        auth_sha=None,
    )
    with pytest.raises(ExternalEvidence8IBindingError, match="separate_authorization_required"):
        bind_promoted_component(
            contract=contract,
            component=component,
            upstream_promotion_decision=upstream,
            provenance=provenance,
            source_catalog=_load(SOURCE_CANDIDATES_PATH),
            component_artifact=component_artifact,
            mapping_artifact=mapping_artifact,
            source_identity_correction=_correction(),
        )


def test_main_effect_manual_authorization_is_research_binding_only() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8g_decision()
    receipt = record_main_effect_authorization(
        contract=contract,
        component_id="rates_policy_x_20t",
        upstream_promotion_decision=upstream,
        decision="AUTHORIZE_FOR_8I_RESEARCH_BINDING_ONLY",
        reviewer_identity="synthetic-reviewer",
        reviewed_at="2026-09-28T12:15:00+00:00",
        rationale="Synthetic governance test; no outcome values read.",
    )
    assert receipt["schema_version"] == MAIN_EFFECT_AUTH_SCHEMA
    assert receipt["state"] == "AUTHORIZED_FOR_8I_RESEARCH_BINDING_ONLY"
    assert receipt["authorizes_8i_research_binding"] is True
    assert receipt["authorizes_decision_influence"] is False
    assert receipt["authorizes_phase7_mutation"] is False
    assert receipt["authorizes_portfolio_action_change"] is False
    assert receipt["authorizes_orders_or_trades"] is False


def test_open_alias_issue_cannot_be_silently_waived() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8g_decision()
    auth = record_main_effect_authorization(
        contract=contract,
        component_id="rates_policy_x_20t",
        upstream_promotion_decision=upstream,
        decision="AUTHORIZE_FOR_8I_RESEARCH_BINDING_ONLY",
        reviewer_identity="synthetic-reviewer",
        reviewed_at="2026-09-28T12:15:00+00:00",
        rationale="Synthetic governance test.",
    )
    component = {
        "component_type": "8g_main_effect",
        "component_id": "rates_policy_x_20t",
        "factor_id": "rates_policy",
        "horizon_sessions": 20,
    }
    component_artifact = {"spec": "synthetic-main-effect"}
    mapping_artifact = {"mapping": "synthetic"}
    provenance = _provenance(
        decision_sha=upstream["decision_sha256"],
        source_ids=["federal_reserve_board_h15"],
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        auth_sha=auth["authorization_sha256"],
    )
    with pytest.raises(ExternalEvidence8IBindingError, match="source_identity_correction_required"):
        bind_promoted_component(
            contract=contract,
            component=component,
            upstream_promotion_decision=upstream,
            provenance=provenance,
            source_catalog=_load(SOURCE_CANDIDATES_PATH),
            component_artifact=component_artifact,
            mapping_artifact=mapping_artifact,
            main_effect_authorization=auth,
        )


def test_versioned_outcome_blind_correction_allows_research_binding_but_not_decision_influence() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8g_decision()
    auth = record_main_effect_authorization(
        contract=contract,
        component_id="rates_policy_x_20t",
        upstream_promotion_decision=upstream,
        decision="AUTHORIZE_FOR_8I_RESEARCH_BINDING_ONLY",
        reviewer_identity="synthetic-reviewer",
        reviewed_at="2026-09-28T12:15:00+00:00",
        rationale="Synthetic governance test.",
    )
    component = {
        "component_type": "8g_main_effect",
        "component_id": "rates_policy_x_20t",
        "factor_id": "rates_policy",
        "horizon_sessions": 20,
    }
    component_artifact = {"spec": "synthetic-main-effect"}
    mapping_artifact = {"mapping": "synthetic"}
    provenance = _provenance(
        decision_sha=upstream["decision_sha256"],
        source_ids=["federal_reserve_board_h15"],
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        auth_sha=auth["authorization_sha256"],
    )
    result = bind_promoted_component(
        contract=contract,
        component=component,
        upstream_promotion_decision=upstream,
        provenance=provenance,
        source_catalog=_load(SOURCE_CANDIDATES_PATH),
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        main_effect_authorization=auth,
        source_identity_correction=_correction(),
    )
    assert result["state"] == BOUND
    assert result["component_id"] == "rates_policy_x_20t"
    assert result["source_ids"] == ["federal_reserve_board_h15"]
    assert result["external_decision_influence_enabled"] is False
    assert result["decision_outcome_access_authorized"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["productive_integration_enabled"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False


def test_8h_approved_interaction_needs_no_second_manual_approval_but_still_needs_exact_binding() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8h_decision()
    component = {
        "component_type": "8h_interaction",
        "component_id": "rates_policy__fx_x_20t",
        "factor_id": "rates_policy+fx",
        "parent_factor_ids": ["rates_policy", "fx"],
        "horizon_sessions": 20,
    }
    component_artifact = {"spec": "synthetic-interaction"}
    mapping_artifact = {"mapping": "synthetic-interaction-map"}
    provenance = _provenance(
        decision_sha=upstream["decision_sha256"],
        source_ids=["federal_reserve_board_h15", "ecb_data_portal"],
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        auth_sha=None,
    )
    result = bind_promoted_component(
        contract=contract,
        component=component,
        upstream_promotion_decision=upstream,
        provenance=provenance,
        source_catalog=_load(SOURCE_CANDIDATES_PATH),
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        source_identity_correction=_correction(),
    )
    assert result["state"] == BOUND
    assert result["8i_authorization_receipt_sha256_if_required"] is None
    assert result["parent_factor_ids_if_applicable"] == ["rates_policy", "fx"]
    assert result["external_decision_influence_enabled"] is False


def test_binding_fails_closed_on_provenance_hash_or_pit_drift() -> None:
    contract = _load(CONTRACT_PATH)
    upstream = _8h_decision()
    component = {
        "component_type": "8h_interaction",
        "component_id": "rates_policy__fx_x_20t",
        "factor_id": "rates_policy+fx",
        "parent_factor_ids": ["rates_policy", "fx"],
        "horizon_sessions": 20,
    }
    component_artifact = {"spec": "synthetic-interaction"}
    mapping_artifact = {"mapping": "synthetic-interaction-map"}
    provenance = _provenance(
        decision_sha=upstream["decision_sha256"],
        source_ids=["federal_reserve_board_h15", "ecb_data_portal"],
        component_artifact=component_artifact,
        mapping_artifact=mapping_artifact,
        auth_sha=None,
    )

    bad_hash = copy.deepcopy(provenance)
    bad_hash["mapping_artifact_hash"] = "0" * 64
    with pytest.raises(ExternalEvidence8IBindingError, match="mapping_artifact_hash_mismatch"):
        bind_promoted_component(
            contract=contract,
            component=component,
            upstream_promotion_decision=upstream,
            provenance=bad_hash,
            source_catalog=_load(SOURCE_CANDIDATES_PATH),
            component_artifact=component_artifact,
            mapping_artifact=mapping_artifact,
            source_identity_correction=_correction(),
        )

    bad_pit = copy.deepcopy(provenance)
    bad_pit["pit_status"] = "UNKNOWN"
    with pytest.raises(ExternalEvidence8IBindingError, match="pit_status_not_allowed"):
        bind_promoted_component(
            contract=contract,
            component=component,
            upstream_promotion_decision=upstream,
            provenance=bad_pit,
            source_catalog=_load(SOURCE_CANDIDATES_PATH),
            component_artifact=component_artifact,
            mapping_artifact=mapping_artifact,
            source_identity_correction=_correction(),
        )


def test_8i_b_completion_is_technical_only_and_stops_before_8i_c() -> None:
    contract = _load(CONTRACT_PATH)
    gate = contract["completion_gate"]
    boundary = contract["authorization_boundary"]
    assert gate["technical_completion_requires_contract_implementation_tests_docs_and_ci"] is True
    assert gate["current_empty_or_blocked_empirical_state_is_valid_technical_completion"] is True
    assert gate["empirical_external_decision_influence_granted_by_8i_b"] is False
    assert gate["upstream_source_identity_blocker_may_not_be_hidden"] is True
    assert gate["next_subblock_after_manual_start"] == "8I-C_OUTCOME_BLIND_EXTERNAL_AGGREGATION_DESIGN"
    assert boundary["maximum_8i_b_state"] == "BOUND_FOR_8I_RESEARCH_ONLY"
    assert boundary["bound_component_allows_decision_outcome_access"] is False
    assert boundary["bound_component_allows_phase7_mutation"] is False
    assert boundary["bound_component_allows_production"] is False
    assert boundary["bound_component_allows_portfolio_action_change"] is False
    assert boundary["bound_component_allows_orders_or_trades"] is False
