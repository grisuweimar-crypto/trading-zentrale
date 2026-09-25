import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanner.research.elliott_vnext.market_context import (
    MarketContextInputError,
    build_context_snapshot,
    build_market_context_history,
    normalize_context_assignments,
    normalize_context_registry,
    prepare_context_ohlcv_for_elliott,
    resolve_context_assignments,
    summarize_market_context,
)

ROOT = Path(__file__).resolve().parents[1]


def _registry(**overrides):
    row = {
        "context_id": "ctx_market",
        "context_type": "market",
        "name": "Broad Market",
        "symbol": "IDX",
        "proxy_kind": "official_or_broad_index",
        "source": "test_source",
        "context_quality": "sufficient",
        "price_basis": "raw",
        "valid_from": "2020-01-01",
        "valid_to": "",
        "retrieved_at": "2026-09-25T12:00:00Z",
        "notes": "fixture",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _assignments(**overrides):
    row = {
        "symbol": "AAA",
        "context_id": "ctx_market",
        "relationship": "market",
        "source": "classification_source",
        "assignment_quality": "sufficient",
        "pit_verified": True,
        "valid_from": "2024-01-01",
        "valid_to": "",
        "retrieved_at": "2026-09-25T12:00:00Z",
        "classification_version": "v1",
        "notes": "fixture",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _raw_rows(context_id="ctx_market", adjusted=False):
    rows = []
    for i, date in enumerate(pd.date_range("2026-09-01", periods=4, freq="D")):
        close = 100.0 + i
        rows.append(
            {
                "date": str(date.date()),
                "context_id": context_id,
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "adj_close": close * 0.5 if adjusted else np.nan,
                "volume": 1000 + i,
                "currency": "USD",
                "retrieved_at": "2026-09-25T12:00:00Z",
            }
        )
    return pd.DataFrame(rows)


def test_contract_and_artifacts_are_explicitly_separate_from_scanner_history():
    contract = json.loads(
        (ROOT / "configs" / "elliott_vnext_market_context_research_v1.json").read_text(encoding="utf-8")
    )
    assert contract["history_artifact"] == "artifacts/research/market_context_history.csv"
    assert contract["separate_from_scanner_history"] is True
    assert contract["scanner_peer_context_is_external_market_evidence"] is False
    assert set(contract["unusable_as_real_market_evidence"]) == {"unreliable", "unavailable"}
    assert (ROOT / "artifacts" / "research" / "market_context_history.csv").exists()
    assert (ROOT / "data" / "inputs" / "market_context_assignments.csv").exists()


def test_registry_still_contains_no_invented_proxy_rows():
    rows = [
        line
        for line in (ROOT / "data" / "inputs" / "market_context_registry.csv").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 1
    assert "price_basis" in rows[0]


def test_scanner_peer_context_is_never_real_external_evidence():
    reg = normalize_context_registry(
        _registry(
            context_id="scanner_peer",
            context_type="scanner_peer_context",
            proxy_kind="scanner_peer_context_only",
            context_quality="sufficient",
        )
    )
    assert bool(reg.iloc[0]["usable_as_real_market_evidence"]) is False


def test_unreliable_context_is_never_real_external_evidence():
    reg = normalize_context_registry(_registry(context_quality="unreliable"))
    assert bool(reg.iloc[0]["usable_as_real_market_evidence"]) is False


def test_registry_versions_may_not_overlap():
    reg = pd.concat(
        [
            _registry(valid_from="2020-01-01", valid_to="2025-12-31"),
            _registry(valid_from="2025-12-31", valid_to=""),
        ],
        ignore_index=True,
    )
    with pytest.raises(MarketContextInputError, match="overlapping_context_registry_validity"):
        normalize_context_registry(reg)


def test_history_is_cut_at_as_of_and_registry_metadata_is_authoritative():
    history = build_market_context_history(_raw_rows(), _registry(), as_of="2026-09-02")
    assert history["date"].max() == pd.Timestamp("2026-09-02")
    assert list(history["source"].unique()) == ["test_source"]
    assert history["usable_as_real_market_evidence"].all()


def test_conflicting_same_day_history_duplicates_fail_closed():
    raw = _raw_rows().iloc[[0]].copy()
    second = raw.copy()
    second["close"] = second["close"] + 1.0
    second["high"] = second[["high", "close"]].max(axis=1) + 0.1
    raw = pd.concat([raw, second], ignore_index=True)
    with pytest.raises(MarketContextInputError, match="conflicting_duplicate_context_history_row"):
        build_market_context_history(raw, _registry())


def test_adjusted_proxy_requires_adjusted_close_without_raw_fallback():
    reg = _registry(price_basis="adjusted", proxy_kind="broad_liquid_sector_or_theme_etf")
    with pytest.raises(MarketContextInputError, match="adjusted_context_missing_adj_close"):
        build_market_context_history(_raw_rows(adjusted=False), reg)


def test_adjusted_context_ohlc_is_scaled_coherently_for_elliott():
    reg = _registry(price_basis="adjusted", proxy_kind="broad_liquid_sector_or_theme_etf")
    history = build_market_context_history(_raw_rows(adjusted=True), reg)
    prepared, meta = prepare_context_ohlcv_for_elliott(history, context_id="ctx_market")
    assert meta["six_a_price_basis"] == "adjusted"
    assert prepared.iloc[0]["close"] == pytest.approx(50.0)
    assert prepared.iloc[0]["open"] == pytest.approx(49.75)
    assert prepared.iloc[0]["high"] == pytest.approx(50.5)
    assert prepared.iloc[0]["low"] == pytest.approx(49.5)


def test_unreliable_context_cannot_generate_real_market_elliott_series():
    reg = _registry(context_quality="unreliable")
    history = build_market_context_history(_raw_rows(), reg)
    with pytest.raises(MarketContextInputError, match="context_contains_unusable_real_market_evidence"):
        prepare_context_ohlcv_for_elliott(history, context_id="ctx_market")


def test_scanner_peer_context_cannot_generate_real_market_elliott_series():
    reg = _registry(
        context_id="scanner_peer",
        context_type="scanner_peer_context",
        proxy_kind="scanner_peer_context_only",
    )
    history = build_market_context_history(_raw_rows(context_id="scanner_peer"), reg)
    with pytest.raises(MarketContextInputError):
        prepare_context_ohlcv_for_elliott(history, context_id="scanner_peer")


def test_non_pit_assignment_is_preserved_but_not_historically_usable():
    assn = normalize_context_assignments(_assignments(pit_verified=False), _registry())
    assert bool(assn.iloc[0]["usable_for_historical_context"]) is False
    resolved = resolve_context_assignments(
        assn,
        _registry(),
        symbol="AAA",
        as_of="2026-09-02",
    )
    assert resolved.empty


def test_assignment_validity_prevents_today_classification_from_retrojection():
    assn = _assignments(valid_from="2026-09-10")
    resolved = resolve_context_assignments(
        assn,
        _registry(),
        symbol="AAA",
        as_of="2026-09-02",
    )
    assert resolved.empty


def test_snapshot_never_uses_future_context_row():
    history = build_market_context_history(_raw_rows(), _registry())
    snapshot = build_context_snapshot(
        history,
        _assignments(),
        _registry(),
        symbol="AAA",
        as_of="2026-09-02",
    )
    assert len(snapshot) == 1
    assert snapshot.iloc[0]["context_date"] == pd.Timestamp("2026-09-02")
    assert snapshot.iloc[0]["age_calendar_days"] == 0


def test_summary_is_coverage_only_not_predictive_claim():
    history = build_market_context_history(_raw_rows(), _registry())
    summary = summarize_market_context(history, _registry(), _assignments())
    assert summary["history_rows"] == 4
    assert summary["predictive_value_evaluated"] is False
    assert summary["incremental_value_evaluated"] is False
    assert summary["trade_decision_emitted"] is False
    assert summary["order_instruction_emitted"] is False


def test_market_context_schema_requires_quality_proxy_basis_and_real_evidence_flag():
    schema = json.loads(
        (ROOT / "configs" / "market_context_history_schema_v1.json").read_text(encoding="utf-8")
    )
    required = set(schema["required"])
    assert {"context_quality", "proxy_kind", "price_basis", "usable_as_real_market_evidence"} <= required


def test_assignment_schema_requires_pit_and_validity_interval():
    schema = json.loads(
        (ROOT / "configs" / "market_context_assignment_schema_v1.json").read_text(encoding="utf-8")
    )
    required = set(schema["required"])
    assert {"pit_verified", "valid_from", "valid_to", "assignment_quality"} <= required
