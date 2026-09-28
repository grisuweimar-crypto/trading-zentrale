from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanner.research.external_evidence.interaction_discovery_8h import (
    evaluate_interaction_discovery_family,
    freeze_interaction_split_manifest,
)
from scanner.research.external_evidence.interaction_eligibility_8h import bind_interaction_eligibility
from scanner.research.external_evidence.interaction_model_8h import construct_interaction_design_family
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
    freeze_interaction_specs,
)
from scanner.research.external_evidence.interaction_validation_8h import (
    VALIDATION_FAMILY_FROZEN,
    VALIDATION_FAMILY_INSUFFICIENT,
    WAITING_FOR_DISCOVERY_MODELS,
    ExternalEvidence8HValidationError,
    evaluate_interaction_validation_family,
    validate_validation_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_validation_evaluation_v1.json").read_text())
DISCOVERY_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_discovery_evaluation_v1.json").read_text())
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


def _spec(spec_id: str = "ix_validation", *, core_field: str = "score", horizon: int = 5) -> dict:
    row = {
        "interaction_spec_id": spec_id,
        "interaction_class": "CORE_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {"kind": "CORE_NUMERIC", "component_id": core_field, "resolved_field": core_field, "horizon_sessions": horizon},
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"rates_policy_x_{horizon}t",
                "factor_id": "rates_policy",
                "horizon_sessions": horizon,
                "feature_field": "rates_policy_level_pct",
                "component_identity_sha256": _digest({"field": "rates_policy_level_pct", "horizon": horizon}),
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
        "frozen_at": "2027-06-30T10:30:00Z",
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


def _dates(validation_n: int = 30) -> tuple[list[pd.Timestamp], list[str]]:
    discovery = list(pd.date_range("2027-07-01", periods=30, freq="D", tz="UTC"))
    validation = list(pd.date_range("2027-08-10", periods=validation_n, freq="D", tz="UTC"))
    holdout = list(pd.date_range("2027-09-20", periods=10, freq="D", tz="UTC"))
    dates = discovery + validation + holdout
    splits = ["DISCOVERY"] * len(discovery) + ["VALIDATION"] * len(validation) + ["HOLDOUT"] * len(holdout)
    return dates, splits


def _feature_frame(spec: dict, validation_n: int = 30) -> tuple[pd.DataFrame, list[str]]:
    dates, _ = _dates(validation_n)
    n = len(dates)
    idx = np.arange(n, dtype=float)
    frame = pd.DataFrame(
        {
            "snapshot_id": [f"s{i:03d}" for i in range(n)],
            "as_of": [d.isoformat() for d in dates],
            "generated_at": [d.isoformat() for d in dates],
            "symbol": [f"SYM{i:03d}" for i in range(n)],
            "horizon_sessions": [int(spec["horizon_sessions"])] * n,
            "score": np.sin(idx / 4.0) + idx / 100.0,
            "risk": np.cos(idx / 5.0) * 0.5,
            "rates_policy_level_pct": np.cos(idx / 7.0) + 1.5,
            "other_main_effect": np.sin(idx / 9.0) * 0.25,
        }
    )
    return frame, [str(spec["components"][0]["resolved_field"]), "rates_policy_level_pct", "other_main_effect"]


def _prepare(*specs: dict, validation_n: int = 30):
    freeze = _freeze(*specs)
    frames = {}
    columns = {}
    for spec in specs:
        frame, main = _feature_frame(spec, validation_n)
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
    dates, splits = _dates(validation_n)
    assignments = {
        spec["interaction_spec_id"]: [
            {"snapshot_id": f"s{i:03d}", "as_of": dates[i].isoformat(), "split": splits[i], "usable": True}
            for i in range(len(dates))
        ]
        for spec in specs
    }
    manifest = freeze_interaction_split_manifest(
        freeze_result=freeze,
        design_result=design,
        assignments_by_spec=assignments,
        author_identity="synthetic-outcome-blind-test",
        authored_at="2027-06-30T11:00:00Z",
    )
    discovery_outcomes = {
        spec["interaction_spec_id"]: _outcomes(
            baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]],
            start=0, count=30, effect=1.0,
        )
        for spec in specs
    }
    discovery = evaluate_interaction_discovery_family(
        contract=DISCOVERY_CONTRACT,
        model_contract=MODEL_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        split_manifest=manifest,
        outcome_rows_by_spec=discovery_outcomes,
        research_as_of="2027-08-09T23:59:00Z",
    )
    return freeze, baselines, challengers, design, manifest, discovery


def _outcomes(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    start: int,
    count: int,
    effect: float,
    horizon: int = 5,
) -> pd.DataFrame:
    identity = ["snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions"]
    work = baseline.iloc[start : start + count][identity].copy().reset_index(drop=True)
    as_of = pd.to_datetime(work["as_of"], utc=True)
    work["start_market_date"] = as_of.dt.date.astype(str)
    work[f"label_available_from_{horizon}t"] = (as_of + pd.Timedelta(days=4)).map(lambda x: x.isoformat())
    interaction = [c for c in challenger.columns if c.startswith("interaction__")]
    signal = challenger.iloc[start : start + count][interaction[0]].to_numpy(dtype=float)
    other = baseline.iloc[start : start + count]["other_main_effect"].to_numpy(dtype=float)
    work[f"peer_excess_{horizon}t"] = effect * signal + 0.25 * other
    return work


def test_8h_f_contract_binds_exact_8h_e_parent_blobs() -> None:
    validate_validation_contract(CONTRACT, DISCOVERY_CONTRACT)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "de0735a1889821110bd32421efcb4fbb69b3a48f"
    for path_key, sha_key in {
        "8h_c_contract": "8h_c_contract_git_blob_sha",
        "8h_d_contract": "8h_d_contract_git_blob_sha",
        "8h_e_contract": "8h_e_contract_git_blob_sha",
        "8h_e_implementation": "8h_e_implementation_git_blob_sha",
        "8h_e_tests": "8h_e_tests_git_blob_sha",
        "8g_evaluation_reference": "8g_evaluation_reference_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]


def test_current_repository_waits_and_does_not_open_validation_or_holdout() -> None:
    eligibility = bind_interaction_eligibility(
        contract=BINDING_CONTRACT, challenger_specs=CHALLENGER, source_registry=SOURCE_REGISTRY
    )
    freeze = freeze_interaction_specs(
        contract=SPEC_CONTRACT, challenger_specs=CHALLENGER, eligibility_result=eligibility
    )
    _, _, design = construct_interaction_design_family(
        contract=MODEL_CONTRACT, spec_contract=SPEC_CONTRACT, challenger_specs=CHALLENGER, freeze_result=freeze
    )
    discovery = evaluate_interaction_discovery_family(
        contract=DISCOVERY_CONTRACT, model_contract=MODEL_CONTRACT, freeze_result=freeze, design_result=design
    )
    result = evaluate_interaction_validation_family(
        contract=CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
    )
    assert result["state"] == WAITING_FOR_DISCOVERY_MODELS
    assert result["validation_results"] == []
    assert result["validation_family_receipt"] is None
    assert result["8h_g_holdout_evaluation_eligible"] is False
    assert result["validation_outcomes_opened"] is False
    assert result["holdout_outcomes_authorized"] is False


def test_valid_validation_freezes_full_family_without_refit_and_keeps_holdout_sealed() -> None:
    spec = _spec()
    freeze, baselines, challengers, design, manifest, discovery = _prepare(spec)
    outcomes = {spec["interaction_spec_id"]: _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=30, count=30, effect=1.0)}
    before = deepcopy(discovery["frozen_model_pairs"])
    result = evaluate_interaction_validation_family(
        contract=CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        split_manifest=manifest,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        outcome_rows_by_spec=outcomes,
        research_as_of="2027-09-19T23:59:00Z",
    )
    assert result["state"] == VALIDATION_FAMILY_FROZEN
    assert result["confirmatory_family"] == freeze["confirmatory_family"]
    assert result["holm_applied_to_full_family"] is True
    assert result["8h_g_holdout_evaluation_eligible"] is True
    assert result["validation_outcomes_opened"] is True
    assert result["holdout_outcomes_authorized"] is False
    assert result["model_refit"] is False
    assert result["family_shrunk_or_expanded"] is False
    assert result["validation_family_receipt"]["holdout_outcomes_opened"] is False
    assert result["validation_results"][0]["model_pair_sha256"] == before[0]["model_pair_sha256"]


def test_holm_uses_complete_family_and_negative_member_is_not_dropped() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    freeze, baselines, challengers, design, manifest, discovery = _prepare(first, second)
    outcomes = {
        first["interaction_spec_id"]: _outcomes(baselines[first["interaction_spec_id"]], challengers[first["interaction_spec_id"]], start=30, count=30, effect=1.0),
        second["interaction_spec_id"]: _outcomes(baselines[second["interaction_spec_id"]], challengers[second["interaction_spec_id"]], start=30, count=30, effect=-0.5),
    }
    result = evaluate_interaction_validation_family(
        contract=CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        split_manifest=manifest,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        outcome_rows_by_spec=outcomes,
        research_as_of="2027-09-19T23:59:00Z",
    )
    assert [x["hypothesis_id"] for x in result["validation_results"]] == [x["hypothesis_id"] for x in freeze["confirmatory_family"]]
    assert len(result["validation_results"]) == 2
    assert all(x["holm_adjusted_p"] is not None for x in result["validation_results"])
    assert result["family_shrunk_or_expanded"] is False
    assert result["8h_g_holdout_evaluation_eligible"] is True


def test_partial_validation_family_is_rejected() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    freeze, baselines, challengers, design, manifest, discovery = _prepare(first, second)
    outcomes = {first["interaction_spec_id"]: _outcomes(baselines[first["interaction_spec_id"]], challengers[first["interaction_spec_id"]], start=30, count=30, effect=1.0)}
    with pytest.raises(ExternalEvidence8HValidationError, match="empirical_input_family_must_equal_full_frozen_family"):
        evaluate_interaction_validation_family(
            contract=CONTRACT,
            discovery_contract=DISCOVERY_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            split_manifest=manifest,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec=outcomes,
            research_as_of="2027-09-19T23:59:00Z",
        )


def test_holdout_row_cannot_be_substituted_into_validation_outcomes() -> None:
    spec = _spec()
    freeze, baselines, challengers, design, manifest, discovery = _prepare(spec)
    outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=30, count=30, effect=1.0)
    holdout = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=60, count=1, effect=1.0)
    outcome.iloc[-1] = holdout.iloc[0]
    with pytest.raises(ExternalEvidence8HValidationError, match="outcome_identity_mismatch"):
        evaluate_interaction_validation_family(
            contract=CONTRACT,
            discovery_contract=DISCOVERY_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            split_manifest=manifest,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
            research_as_of="2027-09-19T23:59:00Z",
        )


def test_exact_validation_holdout_boundary_is_fail_closed() -> None:
    spec = _spec()
    freeze, baselines, challengers, design, manifest, discovery = _prepare(spec)
    outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=30, count=30, effect=1.0)
    first_holdout = min(pd.to_datetime(x["as_of"], utc=True) for x in manifest["streams"][spec["interaction_spec_id"]]["assignments"] if x["split"] == "HOLDOUT")
    outcome.loc[outcome.index[-1], "label_available_from_5t"] = first_holdout.isoformat()
    with pytest.raises(ExternalEvidence8HValidationError, match="exact_validation_holdout_boundary_purge_required"):
        evaluate_interaction_validation_family(
            contract=CONTRACT,
            discovery_contract=DISCOVERY_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            split_manifest=manifest,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
            research_as_of="2027-09-19T23:59:00Z",
        )


def test_tampered_frozen_model_pair_is_rejected_instead_of_refit() -> None:
    spec = _spec()
    freeze, baselines, challengers, design, manifest, discovery = _prepare(spec)
    tampered = deepcopy(discovery)
    tampered["frozen_model_pairs"][0]["baseline_model"]["intercept"] += 1.0
    tampered["discovery_sha256"] = _digest({k: v for k, v in tampered.items() if k != "discovery_sha256"})
    with pytest.raises(ExternalEvidence8HValidationError, match="model_pair_digest_mismatch|model_digest_mismatch"):
        evaluate_interaction_validation_family(
            contract=CONTRACT,
            discovery_contract=DISCOVERY_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=tampered,
            split_manifest=manifest,
        )


def test_insufficient_validation_evidence_does_not_open_holdout() -> None:
    spec = _spec()
    freeze, baselines, challengers, design, manifest, discovery = _prepare(spec, validation_n=10)
    outcome = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=30, count=10, effect=1.0)
    result = evaluate_interaction_validation_family(
        contract=CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        split_manifest=manifest,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        outcome_rows_by_spec={spec["interaction_spec_id"]: outcome},
        research_as_of="2027-09-19T23:59:00Z",
    )
    assert result["state"] == VALIDATION_FAMILY_INSUFFICIENT
    assert result["validation_results"][0]["minimum_evidence_met"] is False
    assert result["validation_family_receipt"] is None
    assert result["8h_g_holdout_evaluation_eligible"] is False
    assert result["holdout_outcomes_authorized"] is False
