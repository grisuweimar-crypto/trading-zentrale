"""CY-03: deterministic synthetic observations, strict missingness and lag masks."""
from datetime import date, timedelta
import copy

import pytest

from scanner.reports.cycle_history import (
    VERSION, FORMULA, SOURCE, eligibility, lag_mask, coverage_rows, csv_bytes, FIELDS,
)


def row(day, asset="AAA", cycle="50", q="PROVISIONAL_REPLAYED", suffix="a"):
    d = date.fromisoformat(day)
    return dict(schema_version=VERSION, snapshot_id=day+"-"+suffix,
       run_id="test-"+day+"-"+suffix, asset_id=asset, listing_symbol=asset,
       price_symbol=asset, currency="USD", market_timezone="UNVERIFIED",
       as_of=day, generated_at=day+"T08:00:00+00:00",
       cycle_as_of=day+"T07:00:00+00:00", computed_at=day+"T07:01:00+00:00",
       last_bar=(d-timedelta(days=1)).isoformat(), cycle=cycle, quality="VALID",
       reason="COMPLETED_DAILY_BARS", source=SOURCE, formula=FORMULA,
       price_source="yfinance.download", price_basis="1d_auto_adjust_true_close",
       price_sha256="a"*64, watchlist_sha256="b"*64, bars_sha256="c"*64,
       currency_lineage="WATCHLIST_DECLARED_ONLY",
       session_time_quality="SESSION_DATE_CUTOFF_ONLY", availability=q)


def test_cycle_zero_and_fifty_are_real_when_provenance_is_valid():
    r = dict(cycle="0", cycle_quality="VALID", cycle_source=SOURCE,
         cycle_formula_version=FORMULA, cycle_price_sha256="d"*64,
         cycle_currency="USD", cycle_price_symbol="AAA",
         cycle_price_basis="1d_auto_adjust_true_close",
         cycle_currency_lineage="WATCHLIST_DECLARED_ONLY",
         cycle_session_time_quality="SESSION_DATE_CUTOFF_ONLY",
         cycle_as_of="2026-10-10T07:00:00+00:00",
         cycle_computed_at="2026-10-10T07:01:00+00:00",
         generated_at="2026-10-10T08:00:00+00:00",
         cycle_last_bar="2026-10-09")
    assert eligibility(r) == "PROVISIONAL_REPLAYED"
    r["cycle"] = "50"
    assert eligibility(r) == "PROVISIONAL_REPLAYED"
    r["cycle_quality"] = "MISSING_SOURCE"
    with pytest.raises(ValueError, match="invalid_cycle_resurrected"):
        eligibility(r)
    r["cycle"] = ""
    r["cycle_price_sha256"] = ""
    assert eligibility(r) == "EXCLUDED_MISSING_SOURCE"


def test_two_observations_produce_provisional_not_research_delta():
    a = row("2026-10-10", cycle="0")
    b = row("2026-10-11", cycle="50")
    mask = lag_mask([a,b])
    assert mask[0]["lag_1obs"] == "NO_PRIOR"
    assert mask[1]["lag_1obs"] == "PROVISIONAL_CHAIN"
    assert mask[1]["lag_5obs"] == "NO_PRIOR"
    assert mask[1]["research_status"] == "BLOCKED_EXTERNAL_VERIFICATION_269"
    assert coverage_rows(mask)[0]["research_eligible"] == "0"


def test_duplicate_same_day_run_selects_later_and_keeps_original():
    a = row("2026-10-10")
    older = row("2026-10-11", suffix="a")
    newer = row("2026-10-11", suffix="b")
    newer["generated_at"] = "2026-10-11T09:00:00+00:00"
    mask = lag_mask([a,older,newer])
    assert mask[1]["lag_1obs"] == "SUPERSEDED_SAME_DAY"
    assert mask[2]["lag_1obs"] == "PROVISIONAL_CHAIN"
    assert sum(int(r["observations"]) for r in coverage_rows(mask)) == 2


@pytest.mark.parametrize("changed", [
    {"formula":"future_cycle_v2"},
    {"price_symbol":"AAA.ALT"},
    {"currency":"EUR"},
    {"listing_symbol":"NEW"},
    {"price_basis":"unadjusted_close"},
])
def test_no_comparison_across_formula_alias_currency_or_listing(changed):
    a = row("2026-10-10")
    b = row("2026-10-11")
    b.update(changed)
    assert lag_mask([a,b])[1]["lag_1obs"] == "IDENTITY_OR_FORMULA_CHANGED"


def test_gap_and_universe_membership_and_invalid_previous():
    a = row("2026-10-10")
    b = row("2026-10-18")
    assert lag_mask([a,b])[1]["lag_1obs"] == "SCAN_GAP"
    missing = row("2026-10-11", asset="OTHER")
    end = row("2026-10-12")
    assert lag_mask([a,missing,end])[2]["lag_1obs"] == "UNIVERSE_OR_OBSERVATION_GAP"
    invalid = row("2026-10-11", q="EXCLUDED_STALE")
    assert lag_mask([a,invalid])[1]["lag_1obs"] == "INVALID_CURRENT"
    assert lag_mask([invalid,end])[1]["lag_1obs"] == "INVALID_CHAIN"


def test_future_timestamp_and_duplicate_snapshot_fail_closed():
    r = row("2026-10-10")
    r["generated_at"] = "2026-10-09T08:00:00+00:00"
    with pytest.raises(ValueError, match="future_scan_date"):
        lag_mask([r])
    with pytest.raises(ValueError, match="duplicate_asset_in_snapshot"):
        lag_mask([row("2026-10-10"),row("2026-10-10")])


def test_11_observations_provide_all_three_technical_lags_deterministically():
    start = date(2026,10,1)
    rows = [row((start+timedelta(days=i)).isoformat(),cycle=str(i))
            for i in range(11)]
    a = lag_mask(rows)
    b = lag_mask(copy.deepcopy(rows))
    assert a == b
    assert [a[-1][f"lag_{n}obs"] for n in (1,5,10)] == ["PROVISIONAL_CHAIN"]*3
    assert csv_bytes(FIELDS,rows) == csv_bytes(FIELDS,copy.deepcopy(rows))
    assert all(m["research_status"] != "ELIGIBLE" for m in a)


def test_unreliable_or_backdated_source_never_becomes_valid():
    r = dict(cycle="50",cycle_quality="VALID",cycle_source="LEGACY",
         cycle_formula_version="legacy",cycle_price_sha256="a"*64,
         cycle_currency="USD",cycle_price_symbol="AAA",cycle_price_basis="close",
         cycle_currency_lineage="WATCHLIST_DECLARED_ONLY",
         cycle_session_time_quality="SESSION_DATE_CUTOFF_ONLY",
         cycle_as_of="2026-10-10T07:00:00+00:00",
         cycle_computed_at="2026-10-10T07:01:00+00:00",
         generated_at="2026-10-10T08:00:00+00:00",
         cycle_last_bar="2026-10-10")
    with pytest.raises(ValueError,match="invalid_valid_provenance"):
        eligibility(r)
