"""Research-only, reproducible one-symbol ARGUS zone CLI (M1–M3).

No Watch/Decision/Order code path. Source is either explicit local OHLCV
or the existing read-only Yahoo historical adapter, never video price levels.
A retrospective run cannot be claimed as originally first-seen on its asof.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path

from scanner.research.zones_v01 import build_zones, read_rows
from scanner.research.zones_m3 import write_readable_tables

# Instrument identity is pinned to the same pilot mapping as the private M5
# experiment. This is NOT a generic ticker-string-to-security resolver.
PILOT_IDENTITIES = {
    "AMD": ("XNAS", "XNAS:AMD:US", "USD"),
    "INTC": ("XNAS", "XNAS:INTC:US", "USD"),
    "ARM": ("XNAS", "XNAS:ARM:US", "USD"),
    "SMH": ("XNAS", "XNAS:SMH:US", "USD"),
    "NOW": ("XNYS", "XNYS:NOW:US", "USD"),
    # Original Nasdaq ticker since 2025-11-11 (FI on NYSE until 2025-11-10).
    # Official issuer announcement: investors.fiserv.com, 2025-10-29.
    "FISV": ("XNAS", "XNAS:FISV:US", "USD"),
}
COLUMNS=("date","symbol","currency","open","high","low","close","adj_close","volume")

# Venue aliases are provider-metadata mappings, NOT independent ISO-MIC proofs.
VENUE_CALENDARS = {
    "NMS": ("XNAS", "NASDAQ"), "NGM": ("XNAS", "NASDAQ"),
    "NCM": ("XNAS", "NASDAQ"), "NYQ": ("XNYS", "NYSE"),
    "GER": ("XETR", "XETR"), "ETR": ("XETR", "XETR"),
    "LSE": ("XLON", "LSE"), "TOR": ("XTSE", "TSX"),
    "HKG": ("XHKG", "HKEX"), "JPX": ("XTKS", "JPX"),
    "PAR": ("XPAR", "Euronext"), "AMS": ("XAMS", "Euronext"),
}

def resolve_equity_identity(symbol, *, metadata=None):
    """Resolve any provider-listed equity; fail closed on ambiguous identity."""
    import re
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\\-]{0,29}", symbol):
        raise ValueError("invalid_provider_ticker")
    provenance = "operator_supplied_metadata_unverified" if metadata is not None else "yfinance_history_metadata_live"
    if metadata is None:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        try:
            metadata = ticker.get_history_metadata() or {}
            if not metadata.get("instrumentType"):
                ticker.history(period="5d", auto_adjust=False)
                metadata = ticker.get_history_metadata() or {}
        except Exception as exc:
            raise ValueError("provider_identity_metadata_unavailable:" + type(exc).__name__) from exc
    if not isinstance(metadata, dict):
        raise ValueError("provider_identity_not_a_mapping")
    if str(metadata.get("symbol", symbol)).upper() != symbol:
        raise ValueError("ticker_redirect_requires_explicit_identity_selection")
    if str(metadata.get("instrumentType", "")).upper() != "EQUITY":
        raise ValueError("instrument_is_not_provider_verified_equity")
    currency = str(metadata.get("currency") or "").strip()
    venue = str(metadata.get("exchangeName") or "").upper()
    timezone_name = str(metadata.get("exchangeTimezoneName") or "")
    if not re.fullmatch(r"[A-Za-z]{3}", currency):
        raise ValueError("missing_or_invalid_provider_currency")
    if not re.fullmatch(r"[A-Z0-9]{2,8}", venue) or "/" not in timezone_name:
        raise ValueError("missing_or_invalid_provider_exchange_or_timezone")
    exchange, calendar = VENUE_CALENDARS.get(venue, ("YF-" + venue, None))
    # Preserve the already-established ARGUS identity for documented pilots;
    # the dynamic path works equally for every other provider-listed share.
    previous = PILOT_IDENTITIES.get(symbol)
    asset_id = (previous[1] if previous and previous[0] == exchange
                and previous[2] == currency else exchange + ":" + symbol + ":" + currency)
    return {
        "symbol": symbol, "exchange": exchange, "currency": currency,
        "asset_id": asset_id,
        "provider_exchange": venue, "exchange_timezone": timezone_name,
        "calendar": calendar, "identity_source": provenance,
        "mic_independently_verified": False,
    }


def fetch_provider(symbol, asof, *, identity=None):
    # Lazy import: offline CSV replays require no network adapter.
    from scripts.prefetch_market_history import _download
    end = (date.fromisoformat(asof)+timedelta(days=1)).isoformat()
    start = (date.fromisoformat(asof)-timedelta(days=930)).isoformat()
    frame=_download([symbol],start=start,end=end,threads=1)
    if frame.attrs.get("errors"):
        raise ValueError("provider_fetch_errors_for_selected_instrument")
    rows=[]
    for item in frame.to_dict("records"):
        if str(item.get("symbol"))!=symbol:
            raise ValueError("provider_returned_different_instrument")
        if str(item.get("date",""))[:10]>asof:continue
        row={"symbol":symbol,"currency":identity["currency"] if identity else PILOT_IDENTITIES[symbol][2],
             "date":str(item["date"])[:10]}
        for k in ("open","high","low","close","adj_close","volume"):
            value=float(item[k])
            if not math.isfinite(value) or value<0 or (k!="volume" and value<=0):
                raise ValueError("provider_invalid_"+k)
            row[k]=str(value)
        rows.append(row)
    return rows


def verify_completed(asof, now=None, *, identity=None):
    """Prevent unfinished NYSE/NASDAQ candles being used as complete."""
    import pandas_market_calendars as mcal
    now=now or datetime.now(timezone.utc)
    calendar = identity.get("calendar") if identity is not None else "NYSE"
    if calendar is None:
        # No invented NYSE close for an unknown foreign exchange. The provider
        # still must supply an actual bar dated asof, two days in the past.
        cutoff = datetime.combine(date.fromisoformat(asof), datetime.min.time(),
                                  tzinfo=timezone.utc) + timedelta(days=2)
        if cutoff > now:
            raise ValueError("unmapped_exchange_requires_48h_old_daily_bar")
        return
    try:
        session=mcal.get_calendar(calendar).schedule(
            start_date=asof, end_date=asof)
    except Exception as exc:
        raise ValueError("calendar_unavailable_for_exchange:"+str(calendar)) from exc
    if session.empty:
        raise ValueError("not_a_us_stock_market_session" if identity is None
                         else "not_a_completed_exchange_session")
    bar=session.iloc[0]["market_close"].to_pydatetime()
    if bar+timedelta(minutes=90)>now:
        raise ValueError("session_not_yet_closed_plus_safety_margin")


def analyze(symbol,asof,rows,*,source_kind,identity=None):
    symbol=symbol.upper()
    if identity is None:
        if symbol not in PILOT_IDENTITIES:
            raise ValueError("instrument_identity_unverified:"+symbol)
        exchange,asset_id,currency=PILOT_IDENTITIES[symbol]
        identity_source="legacy_verified_pilot_identity"
    else:
        if identity["symbol"]!=symbol:
            raise ValueError("instrument_metadata_ticker_mismatch")
        exchange,asset_id,currency=(identity["exchange"],identity["asset_id"],
                                   identity["currency"])
        identity_source=identity["identity_source"]
    date.fromisoformat(asof)
    sample=sorted((r for r in rows if r.get("date","")<=asof),key=lambda r:r["date"])
    if len(sample)<80 or sample[-1]["date"]!=asof:
        raise ValueError("closed_asof_bar_or_minimum_history_missing:"+symbol)
    if len({r["date"] for r in sample})!=len(sample):
        raise ValueError("duplicate_sessions")
    for r in sample:
        if r["symbol"]!=symbol or r["currency"]!=currency:
            raise ValueError("security_or_currency_mismatch")
        vals={}
        for k in ("open","high","low","close","adj_close"):
            try: vals[k]=float(r[k])
            except (KeyError,TypeError,ValueError):
                raise ValueError("invalid_market_price:"+k) from None
            if not math.isfinite(vals[k]) or vals[k]<=0:
                raise ValueError("nonpositive_or_nonfinite_market_price:"+k)
        if vals["low"]>min(vals["open"],vals["close"]) or vals["high"]<max(vals["open"],vals["close"]):
            raise ValueError("inconsistent_ohlc:"+r["date"])
    report=build_zones(sample,symbol,asof=asof,exchange=exchange,
                       asset_id=asset_id,source_sha=None)
    if report.get("data_quality")=="blocked" or report.get("last_completed_bar")!=asof:
        raise ValueError("zone_engine_data_quality_blocked")
    report["research_cli_receipt"]={
        "source_kind":source_kind,
        "source_rows_sha256":hashlib.sha256(json.dumps(
            sample,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest(),
        "source_sessions":len(sample),
        "source_asof":asof,
        "source_retrospective_pit_certified":False,
        "first_seen_prospective":False,
        "market_identity":identity_source,
        "resolved_instrument": identity if identity is not None else
            {"symbol":symbol,"exchange":exchange,"asset_id":asset_id,
             "currency":currency,"identity_source":identity_source,
             "mic_independently_verified":False},
        "provider_asset_identity_independently_certified":False,
        "can_trade":False,
        "empirical_probability":None,
    }
    return report


def run(symbol,asof,output_dir,*,input_file=None,downloader=fetch_provider,
        now=None,metadata=None):
    symbol=symbol.strip().upper()
    # For offline tests, previous documented pilots remain reproducible.
    # Any other equity can use a supplied issuer/provider metadata receipt.
    if metadata is None and input_file and symbol in PILOT_IDENTITIES:
        identity=None
    else:
        identity=resolve_equity_identity(symbol,metadata=metadata)
    verify_completed(asof,now=now,identity=identity)
    if input_file:
        data=read_rows(Path(input_file),{symbol})
        records=data[symbol]
        source="explicit_historical_ohlcv_csv"
    else:
        records=(fetch_provider(symbol,asof,identity=identity)
                 if downloader is fetch_provider else downloader(symbol,asof))
        source="read_only_yahoo_provider_adapter"
    report=analyze(symbol,asof,records,source_kind=source,identity=identity)
    output=Path(output_dir)
    output.mkdir(parents=True,exist_ok=True)
    dest=output/f"{symbol}_{asof}_zones.json"
    dest.write_text(json.dumps(report,indent=2,sort_keys=True,ensure_ascii=False,allow_nan=False)+"\n")
    write_readable_tables(report,dest)
    return dest,report


def main():
    parser=argparse.ArgumentParser(description="ARGUS independent research zone analysis for curated identity-verified instruments")
    parser.add_argument("--symbol",required=True,
                        help="Yahoo equity ticker (e.g., FISV, SAP.DE, 7203.T)")
    parser.add_argument("--asof",required=True)
    parser.add_argument("--metadata-json",type=Path,
                        help="Optional locally supplied Yahoo instrument metadata JSON; unverified provenance")
    parser.add_argument("--input",type=Path,default=None,
                        help="Optional OHLCV CSV for reproducible offline replay")
    parser.add_argument("--output-dir",required=True,type=Path)
    args=parser.parse_args()
    metadata=json.loads(args.metadata_json.read_text()) if args.metadata_json else None
    path,report=run(args.symbol,args.asof,args.output_dir,input_file=args.input,
                    metadata=metadata)
    print(json.dumps({
        "symbol":report["symbol"],"asof":report["asof"],
        "last_completed_bar":report["last_completed_bar"],
        "asset_id":report["asset_id"],"quality":report["data_quality"],
        "zones":len(report["zones"]),"clusters":len(report["zone_clusters"]),
        "plans":len(report["scenario_plans"]),"output":str(path),
        "research_only":True},sort_keys=True))


if __name__=="__main__":
    main()
