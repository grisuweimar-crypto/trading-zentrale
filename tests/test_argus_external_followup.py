"""Tests for the independent ARGUS external-thesis follow-up (no market API)."""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location("followup",Path(__file__).resolve().parents[1]/"scripts/argus_external_followup.py")
follow=importlib.util.module_from_spec(spec)
spec.loader.exec_module(follow)


def sample():
    note={"symbol":"IFX.DE","source_kind":"observed_newsletter","source_date":"2026-10-08",
          "source_target":{"value":65,"currency":"EUR"}}
    market=[
      {"date":"2026-10-07","close":60.0,"adjusted_close":59.0,"high":62,"low":58},
      {"date":"2026-10-08","close":100,"adjusted_close":99,"high":150,"low":20},
      {"date":"2026-10-09","close":61.0,"adjusted_close":60.0,"high":64,"low":59},
      {"date":"2026-10-12","close":63.0,"adjusted_close":62.0,"high":65.5,"low":60},
      {"date":"2026-10-13","close":64.0,"adjusted_close":63.0,"high":66,"low":61},
    ]
    return note,market


def test_source_day_excluded_and_true_sessions_used():
    note,market=sample()
    x=follow.evaluate_one(note,market)
    assert x["baseline"]["date"]=="2026-10-07"
    assert x["windows"]["1"]["date"]=="2026-10-09"
    assert x["windows"]["1"]["target_touch"]["seen"] is False
    assert x["windows"]["5"]["state"]=="NOT_YET_DUE"
    assert x["provider_success"]=="NOT_ASSESSABLE_NO_EXPLICIT_TARGET_DEADLINE"
    assert x["scanner_comparison"]=="NO_VERIFIED_ASOF_SNAPSHOT"


def test_target_touch_is_not_prediction_success():
    note,market=sample()
    x=follow.evaluate_one(note,market)
    # The engine only publishes predefined windows; +5 is still pending.
    market.extend([dict(market[-1],date="2026-10-14"),
                   dict(market[-1],date="2026-10-15")])
    y=follow.evaluate_one(note,market)
    assert y["windows"]["5"]["target_touch"]["seen"] is True
    assert y["windows"]["5"]["target_touch"]["first_date"]=="2026-10-12"
    assert y["windows"]["5"]["target_touch"]["provider_success"] is None


def test_no_historical_price_means_no_fake_result():
    note,market=sample()
    x=follow.evaluate_one(note,market[1:])
    assert x["baseline"] is None
    assert all(v["state"]=="NO_PREPUBLICATION_PRICE" for v in x["windows"].values())


def test_original_currency_gate_and_partial_symbol_coverage():
    note,market=sample()
    bad=dict(note,source_target={"value":65,"currency":"USD"})
    with pytest.raises(ValueError,match="target_currency_mismatch"):
        follow.evaluate_one(bad,market)
    notes={"schema_version":"argus_external_thesis_source_notes_v1",
           "items":[note,dict(note,symbol="AAPL",source_target=None)]}
    x=follow.build(notes,{"IFX.DE":market},datetime(2026,10,15,tzinfo=timezone.utc))
    assert x["claim_count"]==2
    assert x["claims"][1]["state"]=="PRICE_SOURCE_UNAVAILABLE"
    assert x["scanner_changed"] is False
    assert x["provider_score_produced"] is False


def test_no_publication_day_lookahead_and_no_fake_external_verdict():
    note,market=sample()
    altered=[dict(row) for row in market]
    altered[1]["close"]=90000.0
    altered[1]["adjusted_close"]=90000.0
    result=follow.evaluate_one(note,altered)
    base=follow.evaluate_one(note,market)
    assert result["baseline"]==base["baseline"]
    assert result["windows"]==base["windows"]
    assert result["provider_success"]=="NOT_ASSESSABLE_NO_EXPLICIT_TARGET_DEADLINE"
