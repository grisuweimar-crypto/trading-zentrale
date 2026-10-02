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
from scanner.research.external_evidence.interaction_holdout_8h import (
    HOLDOUT_FAMILY_COMPLETE,
    WAITING_FOR_VALIDATION,
    ExternalEvidence8HHoldoutError,
    evaluate_interaction_holdout_family,
    freeze_holdout_authorization,
    new_holdout_consumption_ledger,
    validate_holdout_contract,
)
from scanner.research.external_evidence.interaction_model_8h import construct_interaction_design_family
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
    freeze_interaction_specs,
)
from scanner.research.external_evidence.interaction_validation_8h import evaluate_interaction_validation_family


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_holdout_confirmation_v1.json").read_text())
VALIDATION_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_validation_evaluation_v1.json").read_text())
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


def _spec(spec_id: str = "ix_holdout", *, core_field: str = "score", horizon: int = 5) -> dict:
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


def _dates() -> tuple[list[pd.Timestamp], list[str]]:
    discovery = list(pd.date_range("2027-07-01", periods=30, freq="D", tz="UTC"))
    validation = list(pd.date_range("2027-08-10", periods=30, freq="D", tz="UTC"))
    holdout = list(pd.date_range("2027-09-20", periods=30, freq="D", tz="UTC"))
    dates = discovery + validation + holdout
    splits = ["DISCOVERY"] * 30 + ["VALIDATION"] * 30 + ["HOLDOUT"] * 30
    return dates, splits


def _feature_frame(spec: dict) -> tuple[pd.DataFrame, list[str]]:
    dates, _ = _dates()
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


def _availability(baseline: pd.DataFrame, *, start: int = 60, count: int = 30, horizon: int = 5) -> list[dict[str, str]]:
    rows = baseline.iloc[start : start + count][["snapshot_id", "as_of"]].copy().reset_index(drop=True)
    as_of = pd.to_datetime(rows["as_of"], utc=True)
    label = (as_of + pd.Timedelta(days=4)).map(lambda x: x.isoformat())
    return [
        {"snapshot_id": str(row.snapshot_id), "as_of": str(row.as_of), "label_available_from": str(label.iloc[i])}
        for i, row in enumerate(rows.itertuples(index=False))
    ]


def _prepare(*specs: dict):
    freeze = _freeze(*specs)
    frames: dict[str, pd.DataFrame] = {}
    columns: dict[str, list[str]] = {}
    for spec in specs:
        frame, main = _feature_frame(spec)
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
    dates, splits = _dates()
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
            baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=0, count=30, effect=1.0
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
    validation_outcomes = {
        spec["interaction_spec_id"]: _outcomes(
            baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=30, count=30, effect=1.0
        )
        for spec in specs
    }
    validation = evaluate_interaction_validation_family(
        contract=VALIDATION_CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        split_manifest=manifest,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        outcome_rows_by_spec=validation_outcomes,
        research_as_of="2027-09-19T23:59:00Z",
    )
    availability = {
        spec["interaction_spec_id"]: _availability(baselines[spec["interaction_spec_id"]])
        for spec in specs
    }
    authorization = freeze_holdout_authorization(
        contract=CONTRACT,
        validation_contract=VALIDATION_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        validation_result=validation,
        split_manifest=manifest,
        availability_rows_by_spec=availability,
        research_as_of="2027-10-24T12:00:00Z",
        author_identity="synthetic-outcome-blind-test",
        authored_at="2027-10-24T11:00:00Z",
    )
    ledger = new_holdout_consumption_ledger(authorization)
    return freeze, baselines, challengers, design, manifest, discovery, validation, authorization, ledger


def _evaluate(prepared, specs: tuple[dict, ...], effects: dict[str, float]):
    freeze, baselines, challengers, design, manifest, discovery, validation, authorization, ledger = prepared
    outcomes = {
        spec["interaction_spec_id"]: _outcomes(
            baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]],
            start=60, count=30, effect=effects[spec["interaction_spec_id"]],
        )
        for spec in specs
    }
    return evaluate_interaction_holdout_family(
        contract=CONTRACT,
        validation_contract=VALIDATION_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        validation_result=validation,
        split_manifest=manifest,
        authorization=authorization,
        ledger=ledger,
        baseline_frames_by_spec=baselines,
        challenger_frames_by_spec=challengers,
        outcome_rows_by_spec=outcomes,
        research_as_of="2027-10-24T12:00:00Z",
        opened_by="synthetic-terminal-test",
        opened_at="2027-10-24T11:30:00Z",
    )


def test_8h_g_contract_binds_exact_8h_f_parent_blobs() -> None:
    validate_holdout_contract(CONTRACT, VALIDATION_CONTRACT)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "010ccb54551290b7fbe116eb1d09539d62d70d59"
    for path_key, sha_key in {
        "8h_a_contract": "8h_a_contract_git_blob_sha",
        "8h_f_contract": "8h_f_contract_git_blob_sha",
        "8h_f_implementation": "8h_f_implementation_git_blob_sha",
        "8h_f_tests": "8h_f_tests_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]


def test_current_repository_waits_and_does_not_open_holdout() -> None:
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
    validation = evaluate_interaction_validation_family(
        contract=VALIDATION_CONTRACT,
        discovery_contract=DISCOVERY_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
    )
    result, consumed = evaluate_interaction_holdout_family(
        contract=CONTRACT,
        validation_contract=VALIDATION_CONTRACT,
        freeze_result=freeze,
        design_result=design,
        discovery_result=discovery,
        validation_result=validation,
    )
    assert result["state"] == WAITING_FOR_VALIDATION
    assert result["holdout_results"] == []
    assert result["holdout_completion"] is None
    assert result["holdout_outcomes_opened"] is False
    assert result["8h_h_promotion_review_eligible"] is False
    assert consumed is None


def test_holdout_authorization_rejects_outcome_values_and_partial_family() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    prepared = _prepare(first, second)
    freeze, baselines, _, design, manifest, discovery, validation, _, _ = prepared
    availability = {first["interaction_spec_id"]: _availability(baselines[first["interaction_spec_id"]])}
    with pytest.raises(ExternalEvidence8HHoldoutError, match="availability_family_must_equal_full_family"):
        freeze_holdout_authorization(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            availability_rows_by_spec=availability,
            research_as_of="2027-10-24T12:00:00Z",
            author_identity="test",
            authored_at="2027-10-24T11:00:00Z",
        )
    availability = {
        spec["interaction_spec_id"]: _availability(baselines[spec["interaction_spec_id"]])
        for spec in (first, second)
    }
    availability[first["interaction_spec_id"]][0]["peer_excess"] = 1.0
    with pytest.raises(ExternalEvidence8HHoldoutError, match="outcome_value_in_authorization_metadata"):
        freeze_holdout_authorization(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            availability_rows_by_spec=availability,
            research_as_of="2027-10-24T12:00:00Z",
            author_identity="test",
            authored_at="2027-10-24T11:00:00Z",
        )


def test_holdout_authorization_requires_all_labels_mature_before_open() -> None:
    spec = _spec()
    prepared = _prepare(spec)
    freeze, baselines, _, design, manifest, discovery, validation, _, _ = prepared
    availability = {spec["interaction_spec_id"]: _availability(baselines[spec["interaction_spec_id"]])}
    with pytest.raises(ExternalEvidence8HHoldoutError, match="labels_not_all_mature"):
        freeze_holdout_authorization(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            availability_rows_by_spec=availability,
            research_as_of="2027-10-20T00:00:00Z",
            author_identity="test",
            authored_at="2027-10-20T00:00:00Z",
        )


def test_valid_holdout_consumes_once_and_creates_manual_review_evidence() -> None:
    spec = _spec()
    prepared = _prepare(spec)
    result, consumed = _evaluate(prepared, (spec,), {spec["interaction_spec_id"]: 1.0})
    assert result["state"] == HOLDOUT_FAMILY_COMPLETE
    assert result["holdout_outcomes_opened"] is True
    assert result["holdout_open_count"] == 1
    assert result["model_refit"] is False
    assert result["family_shrunk_or_expanded"] is False
    assert result["holm_applied_to_full_family"] is True
    assert result["8h_h_promotion_review_eligible"] is True
    assert result["holdout_completion"]["empirically_complete"] is True
    assert result["holdout_completion"]["automatic_promotion_authorized"] is False
    assert result["holdout_completion"]["maximum_future_approval_scope"] == "APPROVED_FOR_8I_RESEARCH_ONLY"
    assert consumed is not None and consumed["state"] == "CONSUMED"
    assert consumed["holdout_open_count"] == 1


def test_negative_holdout_member_is_not_removed_and_full_family_holm_is_preserved() -> None:
    first = _spec("ix_a", core_field="score")
    second = _spec("ix_b", core_field="risk")
    prepared = _prepare(first, second)
    result, _ = _evaluate(
        prepared,
        (first, second),
        {first["interaction_spec_id"]: 1.0, second["interaction_spec_id"]: -1.0},
    )
    assert [row["hypothesis_id"] for row in result["holdout_results"]] == [
        row["hypothesis_id"] for row in result["confirmatory_family"]
    ]
    assert len(result["holdout_results"]) == 2
    assert all(row["holm_adjusted_p"] is not None for row in result["holdout_results"])
    assert result["family_shrunk_or_expanded"] is False
    assert set(result["hypothesis_states"].values()).issubset({"HOLDOUT_CONFIRMED", "HOLDOUT_NOT_CONFIRMED"})


def test_consumed_ledger_cannot_open_holdout_twice() -> None:
    spec = _spec()
    prepared = _prepare(spec)
    result, consumed = _evaluate(prepared, (spec,), {spec["interaction_spec_id"]: 1.0})
    assert result["holdout_outcomes_opened"] is True and consumed is not None
    freeze, baselines, challengers, design, manifest, discovery, validation, authorization, _ = prepared
    outcomes = {spec["interaction_spec_id"]: _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=60, count=30, effect=1.0)}
    with pytest.raises(ExternalEvidence8HHoldoutError, match="opened_once_only"):
        evaluate_interaction_holdout_family(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            authorization=authorization,
            ledger=consumed,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec=outcomes,
            research_as_of="2027-10-24T12:00:00Z",
            opened_by="second-open-forbidden",
            opened_at="2027-10-24T11:45:00Z",
        )


def test_holdout_outcomes_must_match_authorized_availability_metadata() -> None:
    spec = _spec()
    prepared = _prepare(spec)
    freeze, baselines, challengers, design, manifest, discovery, validation, authorization, ledger = prepared
    outcomes = _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=60, count=30, effect=1.0)
    outcomes.loc[outcomes.index[0], "label_available_from_5t"] = "2027-09-25T12:34:56+00:00"
    with pytest.raises(ExternalEvidence8HHoldoutError, match="availability_metadata_differs_from_authorization"):
        evaluate_interaction_holdout_family(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            authorization=authorization,
            ledger=ledger,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec={spec["interaction_spec_id"]: outcomes},
            research_as_of="2027-10-24T12:00:00Z",
            opened_by="test",
            opened_at="2027-10-24T11:30:00Z",
        )


def test_human_outcome_inspection_requires_explicit_log_fields() -> None:
    spec = _spec()
    prepared = _prepare(spec)
    freeze, baselines, challengers, design, manifest, discovery, validation, authorization, ledger = prepared
    outcomes = {spec["interaction_spec_id"]: _outcomes(baselines[spec["interaction_spec_id"]], challengers[spec["interaction_spec_id"]], start=60, count=30, effect=1.0)}
    with pytest.raises(ExternalEvidence8HHoldoutError, match="human_inspection_log_incomplete"):
        evaluate_interaction_holdout_family(
            contract=CONTRACT,
            validation_contract=VALIDATION_CONTRACT,
            freeze_result=freeze,
            design_result=design,
            discovery_result=discovery,
            validation_result=validation,
            split_manifest=manifest,
            authorization=authorization,
            ledger=ledger,
            baseline_frames_by_spec=baselines,
            challenger_frames_by_spec=challengers,
            outcome_rows_by_spec=outcomes,
            research_as_of="2027-10-24T12:00:00Z",
            opened_by="test",
            opened_at="2027-10-24T11:30:00Z",
            human_outcome_inspection_occurred=True,
        )
