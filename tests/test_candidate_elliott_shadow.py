"""Fail-closed checks for the isolated non-universe Elliott candidate adapter."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from scripts import run_candidate_elliott_shadow as candidate


NOW = datetime(2026, 10, 10, 20, 0, tzinfo=timezone.utc)


def market_rows():
    day = datetime(2023, 1, 1, tzinfo=timezone.utc)
    rows = []
    while len(rows) < 340:
        if day.weekday() < 5:
            rows.append({
                "date": day.date().isoformat(), "symbol": "LULU", "currency": "USD",
                "open": "100", "high": "101", "low": "99", "close": "100",
                "adj_close": "99.5", "volume": "123456",
                "retrieved_at": NOW.isoformat(),
            })
        day += timedelta(days=1)
    return rows


def test_accepts_complete_adjusted_original_currency_and_source_hash():
    rows = market_rows()
    valid = candidate.validate_prices(rows, NOW)
    assert len(valid) == 340
    assert len(candidate.encode_prices(valid)) > 10000


@pytest.mark.parametrize("column,value,error", [
    ("symbol", "GOOGL", "wrong_listing"),
    ("currency", "EUR", "original_currency"),
    ("adj_close", "", "missing_adjusted"),
    ("date", "2026-10-10", "unconfirmed_current"),
])
def test_rejects_bad_price_evidence(column, value, error):
    rows = market_rows()
    rows[0][column] = value
    with pytest.raises(ValueError, match=error):
        candidate.validate_prices(rows, NOW)


def test_duplicate_price_session_fails_closed():
    rows = market_rows()
    rows.append(dict(rows[-1]))
    with pytest.raises(ValueError, match="price_validation_failed|duplicate_sessions"):
        candidate.validate_prices(rows, NOW)


def test_frozen_core_adapter_is_independent_of_external_and_scanner():
    rows = market_rows()
    as_of = rows[-1]["date"]
    routed = [{"symbol": "LULU", "as_of": as_of, "research_only": True}]
    output = {"symbol": "LULU", "as_of": as_of, "timeframe": "daily",
              "degree": "minor", "research_only": True, "output_id": "test"}
    with patch.object(candidate, "replay_symbol_states", return_value=(routed, {"status": "ok"})) as replay:
        with patch.object(candidate, "build_module_output", return_value=output) as build:
            with patch.object(candidate, "validate_module_output", return_value=output):
                result = candidate.build_capture(rows, NOW)
    replay.assert_called_once()
    assert replay.call_args.kwargs["as_of_dates"] == [as_of]
    assert replay.call_args.kwargs["keep_unchanged"] is True
    build.assert_called_once_with(routed[0])
    assert result["output_count"] == 1
    assert result["research_only"] is True
    assert result["external_count_used_as_model_input"] is False
    assert result["qm_g_empirical_evidence_eligible"] is False
    assert result["scanner_universe_unchanged"] is True
    assert result["decision_effect"] == "none"
    assert result["available_from"] == NOW.isoformat()


def test_no_elliott_structure_is_missing_evidence_not_success():
    rows = market_rows()
    with patch.object(candidate, "replay_symbol_states", return_value=([], {"status": "no_elliott_state_emitted"})):
        result = candidate.build_capture(rows, NOW)
    assert result["output_count"] == 0
    assert result["outputs"] == []
    assert result["coverage"]["status"] == "no_elliott_state_emitted"


def test_repeated_identical_market_prices_have_stable_hash_despite_new_retrieval():
    rows = market_rows()
    second = [dict(r, retrieved_at="2026-10-11T20:00:00+00:00") for r in rows]
    assert candidate.encode_prices(rows) == candidate.encode_prices(second)
    second[-1]["close"] = "100.50"
    assert candidate.encode_prices(rows) != candidate.encode_prices(second)
