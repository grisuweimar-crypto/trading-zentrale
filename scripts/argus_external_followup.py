#!/usr/bin/env python3
"""External thesis follow-up, read-only. No Scanner or QM authority."""
import argparse
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

WINDOWS = (1, 5, 10, 20, 40)
SCHEMA = "argus_external_thesis_followup_v1"


def bars_from_yahoo(symbol, original_currency, today):
    import yfinance as yf
    ticker = yf.Ticker(symbol)
    currency = str(ticker.fast_info.currency or "").upper()
    if currency != original_currency:
        raise ValueError("provider_currency_mismatch")
    df = ticker.history(start="2026-09-20", interval="1d",
                        auto_adjust=False, actions=False)
    if df is None or df.empty:
        raise ValueError("no_provider_prices")
    result = []
    for stamp, bar in df.iterrows():
        day = stamp.date().isoformat()
        if day >= today:
            continue  # current day might be incomplete
        values = [float(bar[k]) for k in ("Open", "High", "Low", "Close", "Adj Close")]
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("invalid_market_price")
        op, hi, lo, close, adj = values
        if lo > min(op, close) or hi < max(op, close):
            raise ValueError("invalid_ohlc")
        result.append({"date":day, "high":hi, "low":lo,
                       "close":close, "adjusted_close":adj})
    result.sort(key=lambda r:r["date"])
    if len({r["date"] for r in result}) != len(result):
        raise ValueError("duplicate_market_session")
    return result


def evaluate_one(note, bars):
    published = note["source_date"]
    before = [bar for bar in bars if bar["date"] < published]
    later = [bar for bar in bars if bar["date"] > published]
    original_currency = "EUR" if note["symbol"].endswith(".DE") else "USD"
    target = note.get("source_target")
    if target and target.get("currency") != original_currency:
        raise ValueError("target_currency_mismatch")
    initial = before[-1] if before else None
    windows = {}
    for count in WINDOWS:
        if initial is None:
            windows[str(count)] = {"state":"NO_PREPUBLICATION_PRICE"}
        elif len(later) < count:
            windows[str(count)] = {"state":"NOT_YET_DUE",
                                   "available_followup_sessions":len(later)}
        else:
            final = later[count-1]
            change = 100*(final["adjusted_close"]/initial["adjusted_close"]-1)
            entry = {"state":"OBSERVED", "date":final["date"],
                     "return_pct_adjusted":round(change, 4)}
            if target:
                level = float(target["value"])
                if not math.isfinite(level) or level <= 0:
                    raise ValueError("invalid_external_target")
                upward = level >= initial["close"]
                touched = next((b["date"] for b in later[:count]
                                if (b["high"] >= level if upward else b["low"] <= level)), None)
                entry["target_touch"] = {"seen":touched is not None, "first_date":touched,
                                          "provider_success":None}
            windows[str(count)] = entry
    return {"claim_id":"EXT-"+published.replace("-","")+"-"+note["symbol"],
            "symbol":note["symbol"],"publication_date":published,
            "currency":original_currency,
            "external_target":target["value"] if target else None,
            "target_deadline":None,
            "baseline":({"date":initial["date"],"close":round(initial["close"],4)}
                        if initial else None),
            "windows":windows,
            "source_capture_class":"retrospective_source_recovery",
            "provider_success":"NOT_ASSESSABLE_NO_EXPLICIT_TARGET_DEADLINE",
            "scanner_comparison":"NO_VERIFIED_ASOF_SNAPSHOT",
            "research_only":True}


def build(notes, prices, now):
    if notes.get("schema_version") != "argus_external_thesis_source_notes_v1":
        raise ValueError("unrecognized_notes_schema")
    claims = [n for n in notes["items"] if n.get("source_kind")=="observed_newsletter"]
    if not claims:
        raise ValueError("no_dated_newsletter_claims")
    out = []
    for note in claims:
        symbol = note["symbol"]
        if not note.get("source_date"):
            raise ValueError("newsletter_without_publication_date")
        if symbol not in prices:
            out.append({"symbol":symbol, "publication_date":note["source_date"],
                        "claim_id":"EXT-"+note["source_date"].replace("-","")+"-"+symbol,
                        "state":"PRICE_SOURCE_UNAVAILABLE",
                        "reason":"provider_failure_or_currency_unverified","research_only":True})
        else:
            out.append(evaluate_one(note, prices[symbol]))
    return {"schema_version":SCHEMA,"as_of":now.date().isoformat(),
            "generated_at":now.isoformat(),"research_only":True,
            "source_type":"yfinance_daily_original_currency",
            "time_windows_trading_sessions":[0,*WINDOWS],
            "source_date_only":True,
            "baseline_policy":"last_complete_close_strictly_before_publication_date",
            "outcome_policy":"completed_sessions_strictly_after_publication_date",
            "scanner_changed":False,"qm_changed":False,"provider_score_produced":False,
            "claims":out,"claim_count":len(out)}
