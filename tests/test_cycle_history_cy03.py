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


def test_crypto_prefixed_asset_has_strict_calendar_daily_gap():
    """CRYPTO:BTC is the real scanner asset ID, not a -USD Yahoo ticker."""
    btc = "CRYPTO:BTC"
    first = row("2026-10-10", asset=btc)
    next_day = row("2026-10-11", asset=btc)
    two_days_later = row("2026-10-12", asset=btc)
    assert lag_mask([first, next_day])[1]["lag_1obs"] == "PROVISIONAL_CHAIN"
    assert lag_mask([first, two_days_later])[1]["lag_1obs"] == "SCAN_GAP"
    # Equity observation lags retain their documented five-calendar-day tolerance.
    assert lag_mask([row("2026-10-10"), row("2026-10-12")])[1]["lag_1obs"] == "PROVISIONAL_CHAIN"
    # Regardless of technical chain validity, Cycle v1 stays research-quarantined.
    assert lag_mask([first, next_day])[1]["research_status"] == "BLOCKED_EXTERNAL_VERIFICATION_269"


def test_crypto_gap_invalidates_longer_observation_chains_too():
    btc = "CRYPTO:ADA"
    start = date(2026, 10, 10)
    days = [0, 1, 2, 4, 5, 6]  # one missing calendar observation on 13 October
    rows = [row((start + timedelta(days=n)).isoformat(), asset=btc) for n in days]
    mask = lag_mask(rows)
    assert mask[-1]["lag_1obs"] == "PROVISIONAL_CHAIN"
    assert mask[-1]["lag_5obs"] == "SCAN_GAP"
    assert mask[-1]["research_status"] == "BLOCKED_EXTERNAL_VERIFICATION_269"


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


def test_file_record_is_append_only_idempotent_and_never_rewrites_legacy(tmp_path, monkeypatch):
    """Fixture mocks CY-02 replay only; live CI dry-run uses the real auditor."""
    import hashlib
    import json
    from pathlib import Path
    from scanner.reports import cycle_history
    from scanner.reports.cycle_history import record, csv_bytes

    monkeypatch.setattr(cycle_history, "audit_current", lambda *a, **k: None)
    def write(rel, data):
        path = tmp_path/rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path
    source = b"asset_id,cycle\\nAAA,50\\n"
    bars = b"synthetic-not-a-real-gzip-replay"
    write("artifacts/watchlist/watchlist_full.csv", source)
    write("artifacts/reports/cycle_input_bars.csv.gz", bars)
    protected = {
        "artifacts/research/history_analysis.csv": b"old research archive\\n",
        "artifacts/snapshots/score_history.csv": b"old score history\\n",
        "artifacts/research/history_recent.csv": b"old recent archive\\n",
    }
    for rel, data in protected.items():
        write(rel,data)
    sid = "11111111-1111-4111-8111-111111111111"
    def publish(sid, day, run_id, value):
        generated = day+"T08:00:00+00:00"
        observation = dict(symbol="AAA", snapshot_id=sid, run_id=run_id,
            as_of=day, generated_at=generated,
            cycle=str(value), cycle_quality="VALID", cycle_source=SOURCE,
            cycle_formula_version=FORMULA, cycle_price_sha256="a"*64,
            cycle_currency="USD", cycle_price_symbol="AAA",
            cycle_price_basis="1d_auto_adjust_true_close",
            cycle_price_source="yfinance.download",
            cycle_currency_lineage="WATCHLIST_DECLARED_ONLY",
            cycle_session_time_quality="SESSION_DATE_CUTOFF_ONLY",
            cycle_as_of=day+"T07:00:00+00:00",
            cycle_computed_at=day+"T07:01:00+00:00",
            cycle_last_bar=(date.fromisoformat(day)-timedelta(days=1)).isoformat(),
            cycle_quality_reason="COMPLETED_DAILY_BARS",
            observation_type="observed_scanner", data_source="scanner_run")
        raw = csv_bytes(tuple(observation), [observation])
        write("artifacts/research/latest_scanner.csv", raw)
        metadata = {"snapshot_id":sid,"as_of":day,"generated_at":generated,
            "latest_run_complete":True,"validation":{"status":"ok"},
            "daily_run":{"run_id":run_id},
            "latest_scanner":{"sha256":hashlib.sha256(raw).hexdigest()},
            "source":{"sha256":hashlib.sha256(source).hexdigest()}}
        write("artifacts/research/history_metadata.json",json.dumps(metadata).encode())
    publish(sid,"2026-10-10","run1",50)
    first = record(tmp_path)
    ledger = tmp_path/"artifacts/cycle_history/observations.csv"
    previous = ledger.read_bytes()
    assert first["observations"] == 1
    assert first["new_current_valid"] == 1
    assert first["research_eligible"] == 0
    assert record(tmp_path)["file_sha256"]["observations.csv"] == first["file_sha256"]["observations.csv"]
    assert ledger.read_bytes() == previous
    assert (tmp_path/"artifacts/cycle_history/bars"/(sid+".csv.gz")).read_bytes() == bars
    assert all((tmp_path/k).read_bytes()==v for k,v in protected.items())

    publish(sid,"2026-10-10","run1",60)
    with pytest.raises(ValueError,match="immutable_snapshot_changed"):
        record(tmp_path)
    assert ledger.read_bytes() == previous

    publish("22222222-2222-4222-8222-222222222222","2026-10-11","run2",75)
    second = record(tmp_path)
    assert second["observations"] == 2
    assert second["snapshots"] == 2
    assert second["provisional_lags"]["1"] == 1
    assert ledger.read_bytes().startswith(previous)
    assert all((tmp_path/k).read_bytes()==v for k,v in protected.items())
    # Never allow an older price-window archive to be reblessed by new metadata.
    first_bars = tmp_path/"artifacts/cycle_history/bars"/(sid+".csv.gz")
    first_bars.write_bytes(b"tampered historical bars")
    with pytest.raises(ValueError, match="previous_bar_archive_changed"):
        record(tmp_path)
    first_bars.write_bytes(bars)
    # A changed prior ledger or derived view is equally prohibited.
    mask_path = tmp_path/"artifacts/cycle_history/eligibility.csv"
    original_mask = mask_path.read_bytes()
    mask_path.write_bytes(original_mask+b"corrupt")
    with pytest.raises(ValueError, match="previous_history_integrity_changed:eligibility.csv"):
        record(tmp_path)
    mask_path.write_bytes(original_mask)
    assert record(tmp_path)["observations"] == 2
    # Append-only chain never modifies original scanner artifacts.
    assert all((tmp_path/k).read_bytes()==v for k,v in protected.items())


def test_current_snapshot_hash_tampering_is_blocked_before_write(tmp_path):
    from scanner.reports.cycle_history import record
    meta = tmp_path/"artifacts/research/history_metadata.json"
    meta.parent.mkdir(parents=True)
    meta.write_text('{"latest_run_complete":false,"validation":{"status":"incomplete"}}')
    with pytest.raises(ValueError,match="incomplete_run"):
        record(tmp_path)
    assert not (tmp_path/"artifacts/cycle_history").exists()
