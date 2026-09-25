import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanner.research.decision_layer import (
    DecisionDatasetConfig,
    DecisionDatasetError,
    build_decision_research_dataset,
    validate_decision_dataset,
)


ROOT = Path(__file__).resolve().parents[1]


def _fixtures():
    days = pd.bdate_range("2026-04-15", periods=85)
    history_rows = []
    price_rows = []
    for s_idx, symbol in enumerate(("AAA", "BBB", "CCC")):
        base = 100.0 + s_idx * 10.0
        for i, day in enumerate(days):
            close = base + i * (0.35 + 0.05 * s_idx) + np.sin(i / 5.0 + s_idx)
            price_rows.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "close": close,
                "adj_close": close,
            })
            history_rows.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "name": symbol,
                "observation_type": "observed_scanner",
                "score": 30.0 + s_idx * 10.0 + (i % 9),
                "currency": "USD",
                "r_code": "R4" if s_idx == 2 else "R3",
                "score_status": "OK",
                "trend_ok": True,
                "liquidity_ok": True,
                "rs3m": i * (0.2 + 0.02 * s_idx),
                "trend200": -2.0 + i * 0.08 + s_idx * 0.1,
                "cycle": float((i * 3) % 100),
                "volatility": 0.15 + 0.03 * s_idx,
                "drawdown": 0.05 + 0.01 * s_idx,
                "confidence": 55.0 + s_idx * 5.0,
                "confidence_label": "MED",
            })
    catalog = {
        "schema_version": "phase1b_frozen_patterns_v1",
        "source_phase": "1B_timing_patterns",
        "horizons": {
            str(h): {
                "discovery_window": ["2026-04-15", "2026-05-15"],
                "frozen_patterns": [
                    {
                        "pattern": "rs3m_d1_up",
                        "conditions": ["rs3m_d1_up"],
                        "discovery_direction": "positive",
                    },
                    {
                        "pattern": "trend200_d1_up & rs3m_d1_up",
                        "conditions": ["trend200_d1_up", "rs3m_d1_up"],
                        "discovery_direction": "negative",
                    },
                ],
            }
            for h in (5, 20, 40, 60)
        },
    }
    config = DecisionDatasetConfig(
        stable_start="2026-04-15",
        legacy_replay_spent_through="2026-06-30",
        prospective_unspent_start="2026-07-01",
        timing_catalog_available_from="2026-06-15",
    )
    return pd.DataFrame(history_rows), pd.DataFrame(price_rows), catalog, config


def test_7b_contract_freezes_spent_and_prospective_boundaries():
    contract = json.loads((ROOT / "configs/decision_research_dataset_v1.json").read_text(encoding="utf-8"))
    assert contract["schema_version"] == "decision_research_dataset_v1"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["legacy_replay_spent_through"] == "2026-09-25"
    assert contract["prospective_unspent_start"] == "2026-09-26"
    assert contract["family_policy"]["probability"]["historical_mode"] == "not_retrojected"
    assert contract["family_policy"]["risk"]["historical_mode"] == "raw_point_in_time_state_only"
    assert contract["family_policy"]["confidence"]["historical_mode"] == "legacy_diagnostic_only"
    assert contract["family_policy"]["elliott"]["historical_mode"] == "not_retrojected"


def test_builds_point_in_time_rows_and_future_labels_separately():
    history, prices, catalog, config = _fixtures()
    frame, metadata = build_decision_research_dataset(history, prices, catalog, config)
    assert len(frame) > 0
    assert metadata["guards"]["future_labels_are_not_features"] is True
    assert metadata["guards"]["current_validation_knowledge_retrojected"] is False
    assert frame["future_labels_are_features"].eq(False).all()
    assert frame["selection_evidence_status"].eq("observed_point_in_time").all()
    assert frame["probability_status_5t"].eq(
        "not_retrojected_not_historically_archived_as_typed_claim"
    ).all()
    assert frame["confidence_vnext_status"].eq(
        "not_retrojected_legacy_confidence_is_diagnostic_only"
    ).all()
    assert frame["elliott_6h_status"].eq(
        "not_retrojected_not_historically_available_as_6h"
    ).all()


def test_frozen_timing_catalog_replays_but_marks_historical_availability():
    history, prices, catalog, config = _fixtures()
    frame, _ = build_decision_research_dataset(history, prices, catalog, config)
    before = frame.loc[pd.to_datetime(frame["obs_date"]) < pd.Timestamp("2026-06-15")]
    after = frame.loc[pd.to_datetime(frame["obs_date"]) >= pd.Timestamp("2026-06-15")]
    assert before["timing_catalog_status"].eq("retrospective_replay_spent").all()
    assert after["timing_catalog_status"].eq("available_as_of_observation").all()
    assert frame["timing_match_count_5t"].max() >= 1
    decoded = json.loads(frame.loc[frame["timing_match_count_5t"] > 0, "timing_matches_5t"].iloc[0])
    assert decoded[0]["horizon_sessions"] == 5
    assert decoded[0]["direction"] in {"positive", "negative"}


def test_spent_and_prospective_rows_are_kept_distinct():
    history, prices, catalog, config = _fixtures()
    frame, metadata = build_decision_research_dataset(history, prices, catalog, config)
    assert "legacy_replay_spent" in set(frame["research_partition"])
    assert "prospective_unspent" in set(frame["research_partition"])
    spent = frame.loc[frame["research_partition"].eq("legacy_replay_spent")]
    prospective = frame.loc[frame["research_partition"].eq("prospective_unspent")]
    assert pd.to_datetime(spent["obs_date"]).max() <= pd.Timestamp("2026-06-30")
    assert pd.to_datetime(prospective["obs_date"]).min() >= pd.Timestamp("2026-07-01")
    assert metadata["partitions"]["legacy_replay_spent"] == len(spent)
    assert metadata["partitions"]["prospective_unspent"] == len(prospective)


def test_label_available_from_never_precedes_observation():
    history, prices, catalog, config = _fixtures()
    frame, _ = build_decision_research_dataset(history, prices, catalog, config)
    obs = pd.to_datetime(frame["obs_date"])
    for horizon in (5, 20, 40, 60):
        available = pd.to_datetime(frame[f"label_available_from_{horizon}t"], errors="coerce")
        assert not (available.notna() & (available < obs)).any()
        last_rows = frame.tail(3)
        if horizon == 60:
            assert (~last_rows[f"label_mature_{horizon}t"]).all()


def test_raw_risk_and_legacy_confidence_remain_semantically_separate():
    history, prices, catalog, config = _fixtures()
    frame, _ = build_decision_research_dataset(history, prices, catalog, config)
    assert frame["risk_evidence_status"].eq("raw_point_in_time_state_only").all()
    assert frame["volatility"].notna().all()
    assert frame["drawdown"].notna().all()
    assert frame["legacy_confidence"].notna().all()
    assert "confidence_score_vnext" not in frame.columns


def test_unknown_frozen_pattern_condition_fails_closed():
    history, prices, catalog, config = _fixtures()
    catalog["horizons"]["5"]["frozen_patterns"][0]["conditions"] = ["future_magic"]
    with pytest.raises(DecisionDatasetError, match="timing_condition_columns_missing"):
        build_decision_research_dataset(history, prices, catalog, config)


def test_dataset_rejects_decision_columns():
    history, prices, catalog, config = _fixtures()
    frame, _ = build_decision_research_dataset(history, prices, catalog, config)
    frame["universal_stance"] = "BUY"
    with pytest.raises(DecisionDatasetError, match="forbidden_decision_columns"):
        validate_decision_dataset(frame, config)
