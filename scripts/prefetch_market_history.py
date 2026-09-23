"""Bootstrap/incrementally update observed Yahoo sessions for the latest universe."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time

import pandas as pd
import yfinance as yf

from scanner.data.enrich.yahoo_prices import ISIN_RE, _pick_symbol
from scanner.data.price_history import MARKET_COLUMNS, coverage, merge_prices, number, validated_rows
from scanner.reports.research_views import atomic_write, encode_csv, parse_csv

DEFAULT_DAYS = 300
CACHE = "artifacts/market_data/yahoo_ohlcv.csv"
STATE = "artifacts/market_data/price_fetch_state.json"


class ProviderSchemaError(ValueError):
    pass


def _csv(path):
    return parse_csv(path.read_bytes())[1] if path.exists() else []


def discover_universe(root):
    rows = _csv(root / "artifacts/research/latest_scanner.csv")
    symbols = [r.get("symbol", "").strip() for r in rows]
    if not symbols or any(not s for s in symbols) or len(set(symbols)) != len(symbols):
        raise ValueError("latest_scanner must contain a nonempty, unique complete symbol universe")
    meta = json.loads((root / "artifacts/research/history_metadata.json").read_text(encoding="utf-8"))
    if not meta.get("latest_run_complete"):
        raise ValueError("price update requires a complete scanner snapshot")
    if any(r.get("snapshot_id") != meta["snapshot_id"] or r.get("date") != meta["as_of"] for r in rows):
        raise ValueError("latest_scanner snapshot/date mismatch")
    import hashlib
    if hashlib.sha256((root / "artifacts/research/latest_scanner.csv").read_bytes()).hexdigest() != meta["latest_scanner"]["sha256"]:
        raise ValueError("latest_scanner hash mismatch")
    return rows, meta


def resolve_mappings(root, latest):
    """Use existing explicit identities; never guess a suffix or crypto quote."""
    master = _csv(root / "data/inputs/universe_master.csv")
    watchlist = _csv(root / "artifacts/watchlist/watchlist_full.csv")
    mapping_file = root / "data/inputs/symbol_map.csv"
    mapping_rows = []
    if mapping_file.exists():
        import csv
        with mapping_file.open(encoding="utf-8-sig", newline="") as handle:
            mapping_rows = list(csv.DictReader(handle, delimiter=";"))
    explicit = defaultdict(set)
    for row in mapping_rows:
        provider = row.get("YahooSymbol", "").strip()
        if provider:
            for key in ("ISIN", "symbol", "Symbol", "asset_id"):
                if row.get(key):
                    explicit[row[key].strip()].add(provider)
    # asset_id -> YahooSymbol resolves CRYPTO:* without guessing EUR/USD quotes.
    for row in watchlist + master:
        provider = _pick_symbol(pd.Series({
            "YahooSymbol": row.get("YahooSymbol") or row.get("yahoo_symbol"),
            "Yahoo": row.get("Yahoo"), "Symbol": row.get("Symbol") or row.get("symbol"),
            "Ticker": row.get("Ticker") or row.get("ticker"),
        }))
        if provider:
            for key in ("asset_id", "symbol", "Symbol", "isin", "ISIN"):
                if row.get(key):
                    explicit[row[key].strip()].add(provider)
    result = {}
    for row in latest:
        symbol = row["symbol"].strip()
        options = explicit.get(symbol, set())
        direct = row.get("YahooSymbol") or row.get("yahoo_symbol")
        if direct:
            options = options | {direct.strip()}
        if not options and not ISIN_RE.fullmatch(symbol.upper()) and re.fullmatch(r"[A-Za-z0-9^][A-Za-z0-9.^=\-]*", symbol):
            options = {symbol}  # exact stored provider-form symbol; no transformation
        if len(options) == 1:
            ticker = next(iter(options))
            if not ISIN_RE.fullmatch(ticker.upper()) and ":" not in ticker:
                result[symbol] = (ticker, None)
                continue
        result[symbol] = (None, "conflicting explicit provider mappings" if len(options) > 1 else "no explicit provider mapping for this identity")
    return result


def _download(tickers, *, period=None, start=None, end=None, threads=4):
    # yf.download aggregates errors. Snapshot the pinned 0.2.x error map now;
    # batches never overlap, including retries. Raw OHLCV stays unadjusted, but
    # Yahoo Adj Close is persisted separately for research return calculations.
    kwargs = dict(tickers=tickers, interval="1d", auto_adjust=False, repair=False,
                  group_by="column", threads=threads, progress=False, timeout=20,
                  end=end, multi_level_index=True)
    if start:
        kwargs["start"] = start
    else:
        kwargs["period"] = period or "2y"
    data = yf.download(**kwargs)
    from yfinance import shared
    errors = dict(getattr(shared, "_ERRORS", {}))
    rows = []
    for ticker in tickers:
        if data is None or data.empty:
            continue
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(1):
                continue
            frame = data.xs(ticker, axis=1, level=1)
        else:
            if len(tickers) != 1:
                raise ProviderSchemaError("provider returned an ambiguous multi-ticker schema")
            frame = data
        frame = frame.rename(columns=lambda value: str(value).strip().lower().replace(" ", "_"))
        if "close" not in frame:
            raise ProviderSchemaError("provider response has no close column")
        if "adj_close" not in frame:
            raise ProviderSchemaError("provider response has no adjusted close column")
        for timestamp, item in frame.iterrows():
            if pd.isna(item.get("close")):
                continue
            rows.append({"symbol": ticker, "date": timestamp.date().isoformat(),
                         **{field: "" if pd.isna(item.get(field)) else str(item[field])
                            for field in ("open", "high", "low", "close", "adj_close", "volume")}})
    result = pd.DataFrame(rows, columns=["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"])
    result.attrs["errors"] = {str(k): str(v) for k, v in errors.items()}
    return result


def _temporary(error):
    return any(term in error.lower() for term in ("ratelimit", "too many", "429", "timeout", "timed out", "connection", "curl", "500", "502", "503", "jsondecode", "unauthorized", "401", "403"))


def fetch_batch(tickers, request, *, retries=2, sleep=time.sleep):
    pending, frames, errors = list(tickers), [], {}
    for attempt in range(retries + 1):
        if attempt:
            sleep(2 ** attempt)
        try:
            frame = _download(pending, **request)
            reported = frame.attrs.get("errors", {})
            found = set(frame.get("symbol", pd.Series(dtype=str)).astype(str))
            frames.append(frame)
        except (ProviderSchemaError, TypeError, KeyError):
            raise  # structural contract/programming failure
        except Exception as exc:
            reported = {ticker: f"provider_error: {type(exc).__name__}: {exc}" for ticker in pending}
            found = set()
        retry = []
        for ticker in pending:
            if ticker in found:
                errors.pop(ticker, None)
                continue
            error = reported.get(ticker, "no daily prices returned by provider")
            errors[ticker] = error
            if _temporary(error) or error.startswith("provider_error:"):
                retry.append(ticker)
        pending = retry
        if not pending:
            break
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=MARKET_COLUMNS)
    return frame, errors


def prefetch_history(root, tickers=None, minimum_days=DEFAULT_DAYS, output=None, *, now=None,
                     batch_size=20, retries=2, sleep=time.sleep):
    root = Path(root)
    now = now or datetime.now(timezone.utc)
    if minimum_days < 1 or not 1 <= batch_size <= 50 or not 0 <= retries <= 5:
        raise ValueError("invalid price-fetch configuration")
    if tickers is None:
        latest, meta = discover_universe(root)
    else:
        symbols = list(dict.fromkeys(t.strip() for t in tickers if t.strip()))
        if not symbols:
            raise ValueError("At least one ticker is required")
        currencies = {r["symbol"]: r.get("currency", "") for r in _csv(root / "data/inputs/universe_master.csv")}
        latest = [{"symbol": s, "currency": currencies.get(s, "")} for s in symbols]
        meta = {"as_of": now.date().isoformat(), "snapshot_id": None}
    currencies = {row["symbol"]: row.get("currency", "") for row in latest}
    mappings = resolve_mappings(root, latest)
    target = output or root / CACHE
    state_path = root / STATE
    target.parent.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    existing_raw = target.read_bytes() if target.exists() else None
    if existing_raw:
        columns, original = parse_csv(existing_raw)
        required_raw = {"date", "symbol", "currency", "open", "high", "low", "close", "volume"}
        if not required_raw.issubset(columns):
            raise ValueError("market cache missing required OHLCV columns")
    else:
        original = []
    existing, existing_issues = validated_rows(original)
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    previous = state.get("symbols", {})
    details, requests, modes = {}, defaultdict(list), {}
    grouped = defaultdict(list)
    for row in existing:
        grouped[row["symbol"]].append(row)
    # Exclusive end avoids persisting unfinished daily bars. Existing legacy
    # same-day raw prices remain unchanged; missing adj_close is enrichment only.
    end = min(now.date(), date.fromisoformat(meta["as_of"]) + timedelta(days=1))
    for row in latest:
        symbol = row["symbol"]
        provider, error = mappings[symbol]
        prior = previous.get(symbol, {})
        detail = {"provider_symbol": provider, "last_attempt_at": now.isoformat(),
                  "validation_issues": existing_issues.get(symbol, {})}
        details[symbol] = detail
        if error:
            if grouped[symbol] and prior.get("provider_symbol"):
                detail["provider_symbol"] = prior["provider_symbol"]
            detail.update(status="ticker_mapping_missing", reason=error)
            continue
        if prior.get("provider_symbol") not in (None, provider) and grouped[symbol]:
            detail["provider_symbol"] = prior["provider_symbol"]
            detail["requested_provider_symbol"] = provider
            detail.update(status="ticker_mapping_missing", reason="provider mapping changed; refusing to mix price series")
            continue
        checked = prior.get("bootstrap_checked_at")
        exhausted = (prior.get("history_exhausted") and prior.get("minimum_sessions_target") == minimum_days
                     and checked and (now.date() - date.fromisoformat(checked)).days < 30)
        missing_adjusted = bool(grouped[symbol]) and any(
            number(item.get("adj_close")) is None or number(item.get("adj_close")) <= 0
            for item in grouped[symbol]
        )
        if missing_adjusted:
            # One-time migration of the historical research window. Two years is
            # comfortably wider than the 300-session target and all 60T labels.
            mode = "adjusted_close_migration"
            request = ("2y" if minimum_days <= 450 else "5y" if minimum_days <= 1100 else "max", None)
        elif len(grouped[symbol]) < minimum_days and not exhausted:
            mode = "bootstrap"
            request = ("2y" if minimum_days <= 450 else "5y" if minimum_days <= 1100 else "max", None)
        else:
            mode = "incremental"
            last = max((r["date"] for r in grouped[symbol]), default=(end - timedelta(days=7)).isoformat())
            request = (None, (date.fromisoformat(last) - timedelta(days=5)).isoformat())
            for key in ("bootstrap_checked_at", "history_exhausted", "minimum_sessions_target"):
                if key in prior:
                    detail[key] = prior[key]
        detail["update_mode"] = mode
        modes[symbol] = mode
        requests[request].append(symbol)
    incoming, successful_providers, provider_failures = [], set(), set()

    def acquire(symbols, period, start):
        providers = list(dict.fromkeys(mappings[s][0] for s in symbols))
        frame, errors = fetch_batch(providers, {"period": period, "start": start, "end": end.isoformat()}, retries=retries, sleep=sleep)
        records = frame.to_dict("records")
        for symbol in symbols:
            provider = mappings[symbol][0]
            source = [dict(r, symbol=symbol, currency=currencies[symbol], retrieved_at=now.isoformat())
                      for r in records if r.get("symbol") == provider]
            source = [dict(r, date=str(r["date"])[:10]) for r in source if str(r.get("date", ""))[:10] < end.isoformat()]
            valid, issues = validated_rows(source)
            details[symbol]["validation_issues"].update(issues.get(symbol, {}))
            incoming.extend(valid)
            error = errors.get(provider)
            if valid:
                successful_providers.add(provider)
                details[symbol].pop("reason", None)
                details[symbol].pop("status", None)
            elif error and (_temporary(error) or error.startswith("provider_error:")):
                provider_failures.add(provider)
                details[symbol].update(status="provider_error", reason=error)
            else:
                details[symbol]["reason"] = error or "provider returned no valid completed sessions in request window"

    for (period, start), symbols in requests.items():
        for offset in range(0, len(symbols), batch_size):
            chunk = symbols[offset:offset + batch_size]
            print(f"Price fetch {period or 'incremental from ' + start}: {', '.join(chunk)}", flush=True)
            acquire(chunk, period, start)
            sleep(0.5)
    merged, issues = merge_prices(original, incoming)
    counts = defaultdict(int)
    for row in merged:
        counts[row["symbol"]] += 1
    deeper = [s for s in modes if modes[s] == "bootstrap" and 0 < counts[s] < minimum_days
              and mappings[s][0] in successful_providers]
    for offset in range(0, len(deeper), batch_size):
        chunk = deeper[offset:offset + batch_size]
        print("Price fetch max-history for partial series: " + ", ".join(chunk), flush=True)
        acquire(chunk, "max", None)
        sleep(0.5)
    merged, issues = merge_prices(original, incoming)
    counts = defaultdict(int)
    for row in merged:
        counts[row["symbol"]] += 1
    for symbol, detail in details.items():
        detail["validation_issues"].update(issues.get(symbol, {}))
        if modes.get(symbol) in ("bootstrap", "adjusted_close_migration") and mappings[symbol][0] in successful_providers and detail.get("status") != "provider_error":
            detail.update(bootstrap_checked_at=now.date().isoformat(), history_exhausted=counts[symbol] < minimum_days,
                          minimum_sessions_target=minimum_days)
        detail.setdefault("status", "price_data_ok" if counts[symbol] >= minimum_days else "price_data_partial" if counts[symbol] else "price_data_unavailable")
    report = {"snapshot_id": meta["snapshot_id"], "as_of": meta["as_of"], "updated_at": now.isoformat(),
              "minimum_sessions_target": minimum_days, "symbols": details}
    atomic_write(state_path, (json.dumps(report, indent=2) + "\n").encode())
    if provider_failures and not successful_providers and len(provider_failures) == len({mappings[s][0] for s in modes}):
        raise RuntimeError("Price provider failed for every requested ticker; cached prices preserved")
    if not modes:
        raise RuntimeError("No resolvable provider mappings for the requested universe")
    if (target.read_bytes() if target.exists() else None) != existing_raw:
        raise RuntimeError("Market cache changed during price download")
    # Re-encode even when only the schema was extended; raw OHLCV remains intact.
    encoded = encode_csv(MARKET_COLUMNS, merged)
    if encoded != existing_raw:
        atomic_write(target, encoded)
    required = {r["symbol"] for r in latest}
    frame = pd.DataFrame([r for r in merged if r["symbol"] in required], columns=MARKET_COLUMNS)
    frame.attrs["coverage"] = coverage(required, merged, minimum_sessions=minimum_days, as_of=meta["as_of"], fetch_state=details)
    return frame


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tickers", nargs="*")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--minimum-days", "--minimum-sessions", dest="minimum_days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()
    yf.set_tz_cache_location(str(args.root / "artifacts/logs/yfinance_cache"))
    frame = prefetch_history(args.root, tuple(args.tickers) if args.tickers else None,
                             args.minimum_days, batch_size=args.batch_size, retries=args.retries)
    print(json.dumps({k: v for k, v in frame.attrs["coverage"].items() if k != "symbols"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
