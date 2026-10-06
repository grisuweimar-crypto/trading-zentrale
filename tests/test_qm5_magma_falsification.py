import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_magma_falsification_case_is_bound_to_current_runtime_evidence():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    row = next(item for item in runtime["rows"] if item["symbol"] == "MGMA.V")
    claim = contract["claim_under_test"]
    adverse = contract["adverse_current_evidence"]

    assert row["current"]["name"] == "MAGMA SILVER"
    assert row["decision"]["universal_stance_state"] == claim["observed_universal_stance"]
    assert row["decision"]["transition_status"] == claim["observed_transition_status"]
    assert row["decision"]["portfolio_action_state"] == claim["observed_portfolio_action"] == "HOLD"
    assert row["decision"]["portfolio_action_reason_code"] == claim["observed_reason_code"]
    assert row["current"]["score"] == adverse["score"]
    assert row["current"]["rank"] == adverse["rank"]
    assert runtime["row_count"] == adverse["universe_size"]
    assert row["current"]["rs3m"] == adverse["rs3m"]
    assert row["current"]["trend200"] == adverse["trend200"]
    assert row["decision"]["evidence_gap_count"] == adverse["evidence_gap_count"]
    assert row["decision"]["phase5_shadow_insufficient_evidence_horizons"] == adverse["phase5_shadow_insufficient_evidence_horizons"]


def test_magma_case_cannot_be_reinterpreted_as_buy_add_or_execution():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    row = next(item for item in runtime["rows"] if item["symbol"] == "MGMA.V")

    assert contract["interpretation"]["hold_does_not_claim_positive_forward_return"] is True
    assert contract["interpretation"]["hold_does_not_equal_buy_or_add"] is True
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert runtime["semantics"]["execution_semantics"] == "non_executable_research_watch"
    assert contract["promotion_or_semantic_change_performed"] is False
    assert runtime["diagnostics"]["scanner_scalar_fallback_used"] is False


def test_magma_phase5_shadow_insufficiency_has_zero_production_effect():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    row = next(item for item in runtime["rows"] if item["symbol"] == "MGMA.V")

    assert row["decision"]["phase5_shadow_status"] == "phase5_engineering_complete_collecting_evidence"
    assert row["decision"]["phase5_shadow_integration_mode"] == "shadow_only"
    assert row["decision"]["phase5_shadow_changes_portfolio_action"] is False
    assert row["decision"]["phase5_shadow_production_change_performed"] is False
