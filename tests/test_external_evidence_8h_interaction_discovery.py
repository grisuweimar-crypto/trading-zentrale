from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanner.research.external_evidence.interaction_discovery_8h import (
    DISCOVERY_FAMILY_FROZEN,
    DISCOVERY_FAMILY_INSUFFICIENT,
    WAITING_FOR_DESIGNS,
    ExternalEvidence8HDiscoveryError,
    evaluate_interaction_discovery_family,
    freeze_interaction_split_manifest,
    validate_discovery_contract,
)
from scanner.research.external_evidence.interaction_eligibility_8h import bind_interaction_eligibility
from scanner.research.external_evidence.interaction_model_8h import construct_interaction_design_family
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
    freeze_interaction_specs,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_discovery_evaluation_v1.json").read_text())
MODEL_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_model_construction_v1.json").read_text())
SPEC_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_spec_freeze_v1.json").read_text())
BINDING_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_eligibility_binding_v1.json").read_text())
CHALLENGER = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
SOURCE_REGISTRY = json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text())


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode("ascii") + data).hexdigest()


def _spec(
    spec_id: str = "ix_discovery",
    *,
    horizon: int = 5,
    core_field: str = "score",
    external_field: str = "rates_policy_level_pct",
) -> dict:
    row = {
        "interaction_spec_id": spec_id,
        "interaction_class": "CORE_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {
                "kind": "CORE_NUMERIC",
                "component_id": core_field,
                "resolved_field": core_field,
                "horizon_sessions": horizon,
            },
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"rates_policy_x_{horizon}t",
                "factor_id": "rates_policy",
                "horizon_sessions": horizon,
                "feature_field": external_field,
                "component_identity_sha256": _digest({"field": external_field, "horizon": horizon}),
            },
        ],
        "operator": "ELEMENTWISE_PRODUCT",
        "eligibility_binding_sha256": _digest({"eligibility": spec_id}),
        "candidate_manifest_sha256": _digest({"manifest": spec_id}),
        "main_effects_retained": True,
        "exactly_one_new_interaction_term": True,
        "outcome_values_read_during_spec_freeze": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["interaction_spec_sha256"] = _digest(row)
    return row


def _freeze(*specs: dict) -> dict:
    ordered = sorted(specs, key=lambda row: row["interaction_spec_id"])
    family = [
        {
            "hypothesis_id": f"{spec['interaction_spec_id']}__peer_excess_{spec['horizon_sessions']}t",
            "interaction_spec_id": spec["interaction_spec_id"],
            "horizon_sessions": spec["horizon_sessions"],
            "primary_outcome": "peer_excess",
        }
        for spec in ordered
    ]
    row = {
        "schema_version": INTERACTION_FREEZE_RESULT_SCHEMA,
        "phase": "8H-C",
        "state": SPECS_FROZEN,
        "eligibility_binding_sha256": _digest({"eligibility": "bound"}),
        "candidate_manifest_sha256": _digest({"manifest": "frozen"}),
        "frozen_at": "2027-07-01T10:30:00Z",
        "frozen_interaction_specs": ordered,
        "confirmatory_family": family,
        "interaction_dataset_model_construction_authorized": True,
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "automatic_promotion_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-D_DATASET_MODEL_CONSTRUCTION",
        "wait_reason": None,
    }
    row["freeze_sha256"] = _digest(row)
    return row


def _dates(discovery_n: int = 30) -> tuple[list[pd.Timestamp], list[str]]:
    discovery = list(pd.date_range("2027-07-02", periods=discovery_n, freq="D", tz="UTC"))
    validation = list(pd.date_range("2027-08-10", periods=15, freq="D", tz="UTC"))
    holdout = list(pd.date_range("2027-09-05", periods=15, freq="D", tz="UTC"))
    dates = discovery + validation + holdout
    splits = ["DISCOVERY"] * len(discovery) + ["VALIDATION"] * len(validation) + ["HOLDOUT"] * len(holdout)
    return dates, splits


def _feature_frame(spec: dict, *, discovery_n: int = 30) -> tuple[pd.DataFrame, list[str]]:
    dates, _ = _dates(discovery_n)
    n = len(dates)
    idx = np.arange(n, dtype=float)
    score = np.sin(idx / 4.0) + idx / 100.0
    rates = np.cos(idx / 7.0) + 1.5
    other = np.sin(idx / 9.0) * 0.25
    risk = np.cos(idx / 5.0) * 0.5
    frame = pd.DataFrame(
        {
            "snapshot_id": [f"s{i:03d}" for i in range(n)],
            "as_of": [d.isoformat() for d in dates],
            "generated_at": [d.isoformat() for d in dates],
            "symbol": [f"SYM{i:03d}" for i in range(n)],
            "horizon_sessions": [int(spec["horizon_sessions"])] * n,
            "score": score,
            "risk": risk,
            "rates_policy_level_pct": rates,
            "other_main_effect": other,
        }
    )
    main = [str(spec["components"][0]["resolved_field"]), "rates_policy_level_pct", "other_main_effect"]
    return frame, main


def _construct(*specs: dict, discovery_n: int = 30):
    freeze = _freeze(*specs)
    frames: dict[str, pd.DataFrame] = {}
    columns: dict[str, list[str]] = {}
    for spec in specs:
        frame, main = _feature_frame(spec, discovery_n=discovery_n)
        frames[spec["interaction_spec_id"]] = frame
        columns[spec["interaction_spec_id"]] = main
    baselines, challengers, design = construct_interaction_design_family(
        contract=MODEL_CONTRACT,
        spec_contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        freeze_result=freeze,
        standardized_frames_by_spec=frames,
        main_effect_columns_by_spec=columns,
    )
    return freeze, baselines, challengers, design


def _manifest(freeze: dict, design: dict, *specs: dict, discovery_n: int = 30) -> dict:
    dates, splits = _dates(discovery_n)
    assignments = {
        spec["interaction_spec_id"]: [
            {
                "snapshot_id": f"s{i:03d}",
                "as_of": dates[i].isoformat(),
                "split": splits[i],
                "usable": True,
            }
            for i in range(len(dates))
        ]
        for spec in specs
    }
    return freeze_interaction_split_manifest(
        freeze_result=freeze,
        design_result=design,
        assignments_by_spec=assignments,
        author_identity="synthetic-outcome-blind-test",
        authored_at="2027-07-01T11:00:00Z",
    )


def _outcomes(baseline: pd.DataFrame, challenger: pd.DataFrame, *, horizon: int = 5, discovery_n: int = 30) -> pd.DataFrame:
    identity = ["snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions"]
    work = baseline.iloc[:discovery_n][identity].copy().reset_index(drop=True)
    as_of = pd.to_datetime(work["as_of"], utc=True)
    work["start_market_date"] = as_of.dt.date.astype(str)
    work[f"label_available_from_{horizon}t"] = (as_of + pd.Timedelta(days=4)).dt.isoformat()
    interaction = [c for c in challenger.columns if c.startswith("interaction__")]
    assert len(interaction) == 1
    y = (
        challenger.iloc[:discovery_n][interaction[0]].to_numpy(dtype=float)
        + 0.3 * baseline.iloc[:discovery_n]["other_main_effect"].to_numpy(dtype=float)
    )
    work[f"peer_excess_{horizon}t"] = y
    return work


def test_8h_e_contract_binds_exact_parent_blobs() -> None:
    validate_discovery_contract(CONTRACT, MODEL_CONTRACT)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "80ae64b2fa651f428290f4fb8b1d63fc7dcfcc8a"
    for path_key, sha_key in {
        "8h_a_contract": "8h_a_contract_git_blob_sha",
        "8h_c_contract": "8h_c_contract_git_blob_sha",
        "8h_d_contract": "8h_d_contract_git_blob_sha",
        "8h_d_implementation": "8h_d_implementation_git_blob_sha",
        "8g_challenger_specs": "8g_challenger_specs_git_blob_sha",
        "8g_evaluation_reference": "8g_evaluation_reference_git_blob_sha",
        "decision_research_dataset": "decision_research_dataset_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]


def test_current_repository_remains_waiting_and_validation_holdout_sealed() -> None:
    eligibility = bind_interaction_eligibility(
        contract=BINDING_CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
    )
    freeze = freeze_interaction_specs(
        contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
    )
    _, _, design = construct_interaction_design_family(
        contract=MODEL_CONTRACT,
        spec_contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        freeze_result=freeze,
    )
    result = evaluate_interaction_discovery_family(
        contract=CONTRACT,
        model_contract=MODEL_CONTRACT,
        freeze_result=freeze,
        design_result=design,
    )
    assert result["state"] == WAITING_FOR_DESIGNS
    assert result["confirmatory_family"] == []
    assert result["discovery_results"] == []
    assert result["frozen_model_pairs"] == []
    assert result["8h_f_validation_evaluation_eligible"] is False
    assert result["validation_outcomes_authorized"] is False
    assert result["holdout_outcomes_authorized"] is False


def test_valid_discovery_flow_freezes_full_model_family_without_changing_hypotheses() -> None:
    spec = _spec()
    freeze, baselines, challengers, design = _construct(spec)
    manifest = _manifest(freeze, design, spec)
    outcomes = {spec["interaction_spec_id"]: _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]])}
    result = evaluate_interaction_discovery_family(
        contract=CONTRACT,
        model_contract=MODEL_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        split_manifest=manifest,
        outcome_rows_by_spec=outcomes,
        research_as_of="2027-08-09T23:59:00Z",
    )
    assert result["state"] == DISCOVERY_FAMILY_FROZEN
    assert result["confirmatory_family"] == freeze["confirmatory_family"]
    assert result["family_shrunk_or_expanded"] is False
    assert result["holm_applied_in_discovery"] is False
    assert result["full_family_model_freeze_complete"] is True
    assert result["8h_f_validation_evaluation_eligible"] is True
    assert result["validation_outcomes_authorized"] is False
    assert result["holdout_outcomes_authorized"] is False
    discovery = result["discovery_results"][0]
    assert discovery["paired_n"] == 30
    assert discovery["minimum_evidence_met"] is True
    assert discovery["discovery_p_value_is_confirmatory"] is False
    assert discovery["family_membership_changed"] is False
    models = result["frozen_model_pairs"][0]
    assert models["baseline_model"]["alpha"] == 1.0
    assert models["challenger_model"]["alpha"] == 1.0
    assert models["hyperparameter_tuning_used"] is False
    assert models["validation_refit_allowed"] is False
    assert models["holdout_refit_allowed"] is False


def test_discovery_outcome_sign_or_p_value_cannot_cherry_pick_family() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    freeze, baselines, challengers, design = _construct(first, second)
    manifest = _manifest(freeze, design, first, second)
    outcomes = {
        spec["interaction_spec_id"]: _outcomes(
            baselines[spec["interaction_spec_id"]],
            challengers[spec["interaction_spec_id"]],
        )
        for spec in (first, second)
    }
    result = evaluate_interaction_discovery_family(
        contract=CONTRACT,
        model_contract=MODEL_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        split_manifest=manifest,
        outcome_rows_by_spec=outcomes,
        research_as_of="2027-08-09T23:59:00Z",
    )
    assert result["confirmatory_family"] == freeze["confirmatory_family"]
    assert [x["hypothesis_id"] for x in result["discovery_results"]] == [
        x["hypothesis_id"] for x in freeze["confirmatory_family"]
    ]
    assert len(result["frozen_model_pairs"]) == 2
    assert result["family_shrunk_or_expanded"] is False


def test_partial_empirical_family_is_rejected_before_outcome_access() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    freeze, baselines, challengers, design = _construct(first, second)
    manifest = _manifest(freeze, design, first, second)
    outcomes = {
        first["interaction_spec_id"]: _outcomes(
            baselines[first["interaction_spec_id"]], challengers[first["interaction_spec_id"]]
        )
    }
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="empirical_input_family_must_equal_full_frozen_family"):
        evaluate_interaction_discovery_family(
            contract=CONTRACT,
            model_contract=MODEL_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            split_manifest=manifest,
            outcome_rows_by_spec=outcomes,
            research_as_of="2027-08-09T23:59:00Z",
        )


def test_split_manifest_must_be_outcome_blind_full_family_and_chronological() -> None:
    spec = _spec()
    freeze, _, _, design = _construct(spec)
    dates, splits = _dates()
    assignments = {
        spec["interaction_spec_id"]: [
            {"snapshot_id": f"s{i:03d}", "as_of": dates[i].isoformat(), "split": splits[i], "usable": True}
            for i in range(len(dates))
        ]
    }
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="split_assignment_must_be_outcome_blind"):
        freeze_interaction_split_manifest(
            freeze_result=freeze,
            design_result=design,
            assignments_by_spec=assignments,
            author_identity="test",
            authored_at="2027-07-01T11:00:00Z",
            outcomes_read_while_assigning_splits=True,
        )
    bad = deepcopy(assignments)
    bad[spec["interaction_spec_id"]][1]["split"] = "HOLDOUT"
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="split_order_not_chronological"):
        freeze_interaction_split_manifest(
            freeze_result=freeze,
            design_result=design,
            assignments_by_spec=bad,
            author_identity="test",
            authored_at="2027-07-01T11:00:00Z",
        )


def test_validation_or_holdout_rows_cannot_be_substituted_for_discovery_outcomes() -> None:
    spec = _spec()
    freeze, baselines, challengers, design = _construct(spec)
    manifest = _manifest(freeze, design, spec)
    bad_outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]])
    validation_row = baselines[spec["interaction_spec_id"]].iloc[[30]][list(bad_outcome.columns[:5])].copy()
    validation_as_of = pd.to_datetime(validation_row["as_of"], utc=True)
    validation_row["start_market_date"] = validation_as_of.dt.date.astype(str)
    validation_row["label_available_from_5t"] = (validation_as_of + pd.Timedelta(days=4)).dt.isoformat()
    validation_row["peer_excess_5t"] = 0.0
    bad_outcome.iloc[-1] = validation_row.iloc[0]
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="outcome_identity_mismatch"):
        evaluate_interaction_discovery_family(
            contract=CONTRACT,
            model_contract=MODEL_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            split_manifest=manifest,
            outcome_rows_by_spec={spec["interaction_spec_id"]: bad_outcome},
            research_as_of="2027-08-20T23:59:00Z",
        )


def test_exact_discovery_validation_label_boundary_is_fail_closed() -> None:
    spec = _spec()
    freeze, baselines, challengers, design = _construct(spec)
    manifest = _manifest(freeze, design, spec)
    outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]])
    first_validation = manifest["streams"][spec["interaction_spec_id"]]["assignments"][30]["as_of"]
    outcome.loc[outcome.index[-1], "label_available_from_5t"] = first_validation
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="boundary_purge_required"):
        evaluate_interaction_discovery_family(
            contract=CONTRACT,
            model_contract=MODEL_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            split_manifest=manifest,
            outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
            research_as_of="2027-08-20T23:59:00Z",
        )


def test_unmatured_discovery_label_is_rejected() -> None:
    spec = _spec()
    freeze, baselines, challengers, design = _construct(spec)
    manifest = _manifest(freeze, design, spec)
    outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]])
    outcome.loc[0, "label_available_from_5t"] = "2027-08-09T12:00:00Z"
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="discovery_label_not_mature"):
        evaluate_interaction_discovery_family(
            contract=CONTRACT,
            model_contract=MODEL_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            split_manifest=manifest,
            outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
            research_as_of="2027-08-09T10:00:00Z",
        )


def test_insufficient_discovery_evidence_keeps_validation_sealed_and_family_intact() -> None:
    spec = _spec()
    freeze, baselines, challengers, design = _construct(spec, discovery_n=10)
    manifest = _manifest(freeze, design, spec, discovery_n=10)
    outcome = _outcomes(
        baselines[spec["interaction_spec_id"]],
        challengers[spec["interaction_spec_id"]],
        discovery_n=10,
    )
    result = evaluate_interaction_discovery_family(
        contract=CONTRACT,
        model_contract=MODEL_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        split_manifest=manifest,
        outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
        research_as_of="2027-08-09T23:59:00Z",
    )
    assert result["state"] == DISCOVERY_FAMILY_INSUFFICIENT
    assert result["confirmatory_family"] == freeze["confirmatory_family"]
    assert result["full_family_model_freeze_complete"] is False
    assert result["8h_f_validation_evaluation_eligible"] is False
    assert result["validation_outcomes_authorized"] is False
    assert result["holdout_outcomes_authorized"] is False
    assert result["discovery_results"][0]["minimum_evidence_met"] is False


def test_split_assignment_before_8h_c_freeze_is_rejected() -> None:
    spec = _spec()
    freeze, _, _, design = _construct(spec)
    dates, splits = _dates()
    assignments = {
        spec["interaction_spec_id"]: [
            {"snapshot_id": f"s{i:03d}", "as_of": dates[i].isoformat(), "split": splits[i], "usable": True}
            for i in range(len(dates))
        ]
    }
    assignments[spec["interaction_spec_id"]][0]["as_of"] = "2027-07-01T09:00:00+00:00"
    with pytest.raises(ExternalEvidence8HDiscoveryError, match="split_assignment_predates_spec_freeze"):
        freeze_interaction_split_manifest(
            freeze_result=freeze,
            design_result=design,
            assignments_by_spec=assignments,
            author_identity="test",
            authored_at="2027-07-01T11:00:00Z",
        )
