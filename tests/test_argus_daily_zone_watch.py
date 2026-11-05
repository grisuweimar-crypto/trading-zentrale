"""Integration gates for the daily all-universe ARGUS research zone publication."""
import csv
from datetime import date, timedelta
import json
import math
from pathlib import Path

import pytest

from scripts.generate_argus_zone_watch import (
    _load_universe, _prior_contacts, _wave_counts, calculate, main, write_outputs,
)


def synthetic(symbol, n=170):
    start = date(2026, 1, 1)
    bars = []
    for i in range(n):
        price = 20 + 0.03*i + 2.2*math.sin(i/5)
        bars.append({"date": (start+timedelta(days=i)).isoformat(),
                     "symbol": symbol, "currency": "USD",
                     "open": f"{price:.6f}", "high": f"{price+0.50:.6f}",
                     "low": f"{price-0.50:.6f}", "close": f"{price:.6f}",
                     "adj_close": f"{price:.6f}", "volume": str(10000+i)})
    return bars


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        out = csv.DictWriter(f, fieldnames=fieldnames)
        out.writeheader()
        out.writerows(rows)


def fixture(tmp_path):
    symbols = ["UEC","XPEV","WPM"]
    bars = [b for s in symbols for b in synthetic(s)]
    asof = bars[169]["date"]
    market = tmp_path/"market.csv"
    write_csv(market, list(bars[0]), bars)
    scanner = tmp_path/"watch.csv"
    rows = [
        {"yahoo_symbol": s, "name": s, "asset_id": s, "currency": "USD",
         "score": "20", "score_status": "OK", "price_bar_date": asof,
         "price_bar_status": "CLOSED_PREVIOUS_SESSION"} for s in symbols
    ]
    write_csv(scanner, list(rows[0]), rows)
    return market,scanner,asof


def test_three_symbols_end_to_end_and_daily_history(tmp_path):
    market, watch, asof = fixture(tmp_path)
    json_path = tmp_path/"argus.json"
    html_path = tmp_path/"zones.html"
    history = tmp_path/"history"
    main(["--watchlist",str(watch),"--market",str(market),
          "--json",str(json_path),"--html",str(html_path),"--history",str(history)])
    data = json.loads(json_path.read_text())
    assert data["count"] == 3
    assert data["available"] == 3
    assert all(x["asof"] == asof and x["status"] == "RESEARCH_ONLY" for x in data["assets"])
    assert all(x["scanner_score"] == 20 for x in data["assets"])
    assert all(x["data_quality"] == "degraded" for x in data["assets"])
    assert all(x["empirical_probability"] is None for x in data["assets"])
    assert len(list(history.glob("*.json"))) == 1
    assert "ARGUS · Elliott" in html_path.read_text()
    assert "WPM" in html_path.read_text()
    # No new history record if input and output are identical.
    main(["--watchlist",str(watch),"--market",str(market),
          "--json",str(json_path),"--html",str(html_path),"--history",str(history)])
    assert len(list(history.glob("*.json"))) == 1


def test_unfinished_or_missing_session_never_produces_zone(tmp_path):
    market, watch, asof = fixture(tmp_path)
    entries = _load_universe(watch)
    entries["UEC"]["closed_bar_status"] = "INTRADAY"
    entries["UEC"]["asof"] = ""
    # No session exists after the actual archive last bar.
    entries["XPEV"]["asof"] = (date.fromisoformat(asof) + timedelta(days=1)).isoformat()
    results = {x["symbol"]:x for x in calculate(entries,market)}
    assert results["UEC"]["zones"] == []
    assert results["UEC"]["reason"] == "closed_daily_bar_not_proven"
    assert results["XPEV"]["zones"] == []
    assert results["XPEV"]["reason"] == "exact_scanner_closed_session_missing_in_archive"
    assert results["WPM"]["status"] == "RESEARCH_ONLY"


def test_scanner_identity_and_currency_fail_closed(tmp_path):
    market, watch, _ = fixture(tmp_path)
    entries = _load_universe(watch)
    entries["UEC"]["declared_currency"] = "EUR"
    results = {x["symbol"]:x for x in calculate(entries,market)}
    assert results["UEC"]["reason"] == "currency_mismatch"
    assert not results["UEC"]["wave_counts"]


def test_contact_episodes_exclude_pre_confirmation_and_current_bar():
    zone = {"lower": 9, "upper": 11, "known_at": "2026-10-02"}
    bars = [
        {"date":"2026-10-01","low":"9.5","high":"10.5"},
        {"date":"2026-10-02","low":"9.5","high":"10.5"},
        {"date":"2026-10-03","low":"9.5","high":"10.5"},
        {"date":"2026-10-04","low":"9.6","high":"10.4"},
        {"date":"2026-10-05","low":"13","high":"15"},
        {"date":"2026-10-06","low":"9.5","high":"10.5"},
        {"date":"2026-10-07","low":"9.7","high":"10.3"},
    ]
    x = _prior_contacts(zone,bars,"2026-10-07")
    assert x["episodes"] == 2
    assert x["most_recent"] == "2026-10-06"


def test_elliott_long_wave3_rule_and_nonoverlap():
    def five(values):
        kinds = ["high","low","high","low","high","low"]
        return [{"drivers":[{"family":"horizontal","anchors":[{
            "degree":"minor","kind":k,"pivot_time":f"2026-10-{i+1:02}",
            "known_at":f"2026-10-{i+7:02}","price":p}]}]}
            for i,(k,p) in enumerate(zip(kinds,values))]
    valid = _wave_counts(five([15,13,14,10,12,8]))
    assert valid and valid[0]["direction"] == "down"
    # W4 at 13.5 overlaps the W1 territory (13–15), so this is no impulse.
    assert not _wave_counts(five([15,13,14,10,13.5,8]))
    # W3 is the shortest: W1=2, W3=1, W5=3.
    assert not _wave_counts(five([15,13,14,13,13.5,10.5]))


def test_no_fabrication_if_closed_bar_receipt_absent(tmp_path):
    path = tmp_path/"watch.csv"
    write_csv(path,["yahoo_symbol","name"],[{"yahoo_symbol":"UEC","name":"UEC"}])
    with pytest.raises(ValueError,match="receipt"):
        _load_universe(path)


def test_html_script_injection_escaped(tmp_path):
    asset={"symbol":"ZZZ","name":"bad </script><script>alert(1)</script>",
           "status":"UNAVAILABLE","asof":"","reason":"missing"}
    j=tmp_path/"r.json";h=tmp_path/"zones.html"
    write_outputs([asset],out_json=j,out_html=h,history_dir=tmp_path/"history")
    content=h.read_text()
    assert "bad </script><script>alert(1)</script>" not in content
    assert r"\u003c/script>" in content
