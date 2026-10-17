import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_magma_historical_case_is_preserved_and_live_reference_matches_its_scanner_snapshot():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    daily = json.loads((ROOT / "artifacts/research/daily_research.json").read_text())
    row = next(item for item in runtime["rows"] if item["symbol"] == "MGMA.V")
    claim = contract["claim_under_test"]
    historical = contract["adverse_current_evidence"]

    # Immutable older observed evidence, NOT expectations for a newer live
    # snapshot. Rank, score, directional stance and action can all move.
    assert historical["rank"] == 212
    assert historical["universe_size"] == 213
    assert claim["observed_universal_stance"] == "positive"
    assert claim["observed_portfolio_action"] == "HOLD"
    assert claim["observed_reason_code"] == "positive_confirmed_existing_long"
    assert contract["promotion_or_semantic_change_performed"] is False

    assert runtime["snapshot_id"] == daily["snapshot_id"] == runtime["diagnostics"]["snapshot_id"]
    assert runtime["row_count"] == len(runtime["rows"]) == daily["universe_size"]
    assert row["current"]["name"] == "MAGMA SILVER"
    live = daily["symbols"]["MGMA.V"]["current"]
    for field in ("rank", "rank_percentile", "score", "rs3m", "trend200", "confidence_label", "r_code"):
        assert row["current"][field] == live[field]
    assert 1 <= row["current"]["rank"] <= runtime["row_count"]
    assert runtime["diagnostics"]["scanner_scalar_fallback_used"] is False


def test_magma_historical_hold_is_not_a_buy_signal_or_a_live_execution_instruction():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    assert contract["interpretation"]["hold_does_not_claim_positive_forward_return"] is True
    assert contract["interpretation"]["hold_does_not_equal_buy_or_add"] is True
    assert contract["claim_under_test"]["observed_portfolio_action"] == "HOLD"
    assert runtime["semantics"]["reference_is_trade_instruction"] is False
    assert runtime["semantics"]["actual_holdings_included"] is False
    assert runtime["semantics"]["private_position_data_included"] is False
    assert contract["promotion_or_semantic_change_performed"] is False

def test_magma_phase5_shadow_insufficiency_has_zero_production_effect():
    contract = json.loads((ROOT / "configs/qm5_magma_falsification_v1.json").read_text())
    runtime = json.loads((ROOT / contract["source_artifact"]).read_text())
    row = next(item for item in runtime["rows"] if item["symbol"] == "MGMA.V")

    assert row["decision"]["phase5_shadow_status"] == "phase5_engineering_complete_collecting_evidence"
    assert row["decision"]["phase5_shadow_integration_mode"] == "shadow_only"
    assert row["decision"]["phase5_shadow_changes_portfolio_action"] is False
    assert row["decision"]["phase5_shadow_production_change_performed"] is False
