#!/usr/bin/env python3
"""Isolated external-candidate Elliott observation; never modifies scanner admission or decisions.

The frozen 6A->6D causal replay and 6H assembler are reused without modification.
Only confirmed daily OHLCV prices from an explicitly identified provider are accepted.
An external HKCM count is NEVER an input to this evaluator.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
from pathlib import Path

from scanner.data.price_history import MARKET_COLUMNS, validated_rows
from scanner.research.elliott_vnext.output import build_module_output, validate_module_output
from scanner.research.elliott_vnext.validation_replay import replay_symbol_states

SYMBOL = "LULU"
SCHEMA = "external_candidate_elliott_shadow_v1"
SOURCE_START = "2018-01-01"


def collect_yahoo(now: datetime) -> list[dict[str, str]]:
    import yfinance as yf

    ticker = yf.Ticker(SYMBOL)
    history = ticker.history(start=SOURCE_START, interval="1d", auto_adjust=False, actions=False)
    if history is None or history.empty:
        raise ValueError("candidate_yahoo_no_observed_daily_prices")
    currency = str(ticker.fast_info.currency or "").strip().upper()
    if currency != "USD":
        raise ValueError("candidate_provider_currency_not_confirmed_usd")

    rows = []
    for timestamp, r in history.iterrows():
        session = timestamp.date().isoformat()
        # Disallow today's partial US market session, including on a manual run.
        if session >= now.date().isoformat():
            continue
        rows.append({
            "date": session, "symbol": SYMBOL, "currency": currency,
            "open": str(r["Open"]), "high": str(r["High"]),
            "low": str(r["Low"]), "close": str(r["Close"]),
            "adj_close": str(r["Adj Close"]), "volume": str(r["Volume"]),
            "retrieved_at": now.isoformat(),
        })
    return rows


def encode_prices(rows: list[dict[str, str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=MARKET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in sorted(rows, key=lambda x: x["date"]):
        writer.writerow({key: row.get(key, "") for key in MARKET_COLUMNS})
    return buf.getvalue().encode("utf-8")


def validate_prices(rows: list[dict[str, str]], now: datetime) -> list[dict[str, str]]:
    if not rows:
        raise ValueError("candidate_price_history_missing")
    if any(r.get("symbol") != SYMBOL or r.get("currency") != "USD" for r in rows):
        raise ValueError("candidate_wrong_listing_or_original_currency")
    if any(str(r.get("date", "")) >= now.date().isoformat() for r in rows):
        raise ValueError("candidate_unconfirmed_current_session")
    if any(not r.get("adj_close") for r in rows):
        raise ValueError("candidate_missing_adjusted_close_no_raw_fallback")
    valid, issues = validated_rows(rows)
    if issues or len(valid) != len(rows):
        raise ValueError("candidate_price_validation_failed")
    if len(valid) < 300:
        raise ValueError("candidate_insufficient_adjusted_price_history")
    if len({r["date"] for r in valid}) != len(valid):
        raise ValueError("candidate_duplicate_sessions")
    return sorted(valid, key=lambda x: x["date"])


def build_capture(rows: list[dict[str, str]], now: datetime) -> dict:
    prices = validate_prices(rows, now)
    source_bytes = encode_prices(prices)
    as_of = prices[-1]["date"]
    routed, coverage = replay_symbol_states(
        prices, SYMBOL, as_of_dates=[as_of], keep_unchanged=True
    )
    outputs = [validate_module_output(build_module_output(s)) for s in routed]
    if any(o.get("symbol") != SYMBOL or o.get("as_of") != as_of for o in outputs):
        raise ValueError("candidate_6h_symbol_or_date_mismatch")
    keys = [(o["symbol"], o["timeframe"], o["degree"]) for o in outputs]
    if len(keys) != len(set(keys)):
        raise ValueError("candidate_duplicate_elliott_degree")
    outputs.sort(key=lambda o: (o["timeframe"], o["degree"]))
    return {
        "schema_version": SCHEMA,
        "symbol": SYMBOL,
        "listing": "NASDAQ:LULU",
        "currency": "USD",
        "as_of": as_of,
        "captured_at": now.isoformat(),
        "available_from": now.isoformat(),
        "source": {
            "provider": "Yahoo Finance via yfinance",
            "historical_start": SOURCE_START,
            "market_data_sha256": sha256(source_bytes).hexdigest(),
            "market_data_sessions": len(prices),
            "raw_prices_and_adj_close_preserved": True,
            "live_source_capture_not_historical_pit_reconstruction": True,
        },
        "source_price_csv_sha256": sha256(source_bytes).hexdigest(),
        "coverage": coverage,
        "output_count": len(outputs),
        "outputs": outputs,
        "research_only": True,
        "scanner_universe_unchanged": True,
        "hkcm_count_used_as_model_input": False,
        "qm_g_empirical_evidence_eligible": False,
        "decision_effect": "none",
        "direct_ordering_allowed": False,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch-yahoo", action="store_true")
    source.add_argument("--prices", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--source-copy", type=Path, required=True)
    args = p.parse_args()
    now = datetime.now(timezone.utc)
    if args.fetch_yahoo:
        rows = collect_yahoo(now)
    else:
        with args.prices.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    rows = validate_prices(rows, now)
    result = build_capture(rows, now)
    source_bytes = encode_prices(rows)
    if sha256(source_bytes).hexdigest() != result["source_price_csv_sha256"]:
        raise ValueError("candidate_price_source_hash_changed")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.source_copy.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    args.source_copy.write_bytes(source_bytes)
    print(json.dumps({
        "symbol": SYMBOL, "as_of": result["as_of"],
        "source_sessions": len(rows), "output_count": result["output_count"],
        "research_only": True, "scanner_universe_unchanged": True
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
