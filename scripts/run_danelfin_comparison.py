"""Run an independent Danelfin-vs-scanner historical comparison."""
import argparse
from datetime import timedelta
import json
from pathlib import Path
import time

from dotenv import load_dotenv

from scanner.reports.danelfin_compare import (
    DanelfinClient,
    PRICE_OBSERVATION_TYPES,
    build_comparison_events,
    finite_number,
    infer_danelfin_market,
    parse_day,
    read_csv_rows,
    select_symbols,
    summarize_events,
    write_events_csv,
    write_summary,
)


def derive_missing_rank_percentiles(rows):
    """Derive point-in-time daily scanner percentiles when old rows lack them.

    The best scanner score receives percentile 0.0 and the worst 1.0. Ties use
    their average position. Existing stored percentiles are never overwritten.
    """
    by_day = {}
    for row in rows:
        if str(row.get("observation_type") or "") != "observed_scanner":
            continue
        day = parse_day(row.get("date"))
        symbol = str(row.get("symbol") or "").strip()
        score = finite_number(row.get("score"))
        if day is None or not symbol or score is None:
            continue
        by_day.setdefault(day, {}).setdefault(symbol, row)

    derived = 0
    for symbol_rows in by_day.values():
        entries = [
            (symbol, finite_number(row.get("score")), row)
            for symbol, row in symbol_rows.items()
        ]
        entries = [item for item in entries if item[1] is not None]
        entries.sort(key=lambda item: (-float(item[1]), item[0]))
        count = len(entries)
        if not count:
            continue
        pos = 0
        while pos < count:
            end = pos + 1
            while end < count and entries[end][1] == entries[pos][1]:
                end += 1
            average_zero_based_position = (pos + end - 1) / 2.0
            percentile = (
                average_zero_based_position / (count - 1)
                if count > 1
                else 0.0
            )
            for idx in range(pos, end):
                row = entries[idx][2]
                if finite_number(row.get("rank_percentile")) is None:
                    row["rank_percentile"] = f"{percentile:.12g}"
                    row["rank_percentile_source"] = "derived_daily_score_rank"
                    derived += 1
            pos = end
    return derived


def refresh_benchmark_from_yahoo(rows, benchmark_symbol, histories):
    """Add an isolated Yahoo benchmark series for exact-date alpha comparison."""
    if not benchmark_symbol:
        return {"source": None, "added_rows": 0}

    history_days = [
        parse_day(item.get("date"))
        for values in histories.values()
        for item in values
    ]
    history_days = [day for day in history_days if day is not None]
    if not history_days:
        return {"source": None, "added_rows": 0}

    available_days = [parse_day(row.get("date")) for row in rows]
    available_days = [day for day in available_days if day is not None]
    latest_available = max(available_days) if available_days else max(history_days)
    start = min(history_days) - timedelta(days=10)
    end = latest_available + timedelta(days=5)

    import yfinance as yf

    data = yf.download(
        benchmark_symbol,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if data is None or data.empty:
        return {"source": "yahoo", "added_rows": 0, "error": "empty benchmark download"}

    close = data["Close"]
    if getattr(close, "ndim", 1) > 1:
        close = close.iloc[:, 0]

    # For research-only alpha, use one consistent benchmark source rather than
    # mixing potentially different historical SPY providers in the same series.
    rows[:] = [
        row for row in rows
        if not (
            str(row.get("symbol") or "").strip() == benchmark_symbol
            and str(row.get("observation_type") or "") in PRICE_OBSERVATION_TYPES
        )
    ]
    added = 0
    for timestamp, value in close.dropna().items():
        numeric = finite_number(value)
        if numeric is None or numeric <= 0:
            continue
        rows.append({
            "date": timestamp.date().isoformat(),
            "symbol": benchmark_symbol,
            "close": str(numeric),
            "currency": "USD",
            "observation_type": "price_backfill",
            "provenance": "danelfin_comparison_yahoo_benchmark",
        })
        added += 1
    return {
        "source": "yahoo",
        "added_rows": added,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def fetch_histories_paced(client, mappings, *, min_interval_seconds=6.5, retry_wait_seconds=65.0):
    """Respect Danelfin Free's 10 requests/minute rate limit and retry one 429."""
    histories, errors = {}, {}
    last_request_started = None
    for mapping in mappings:
        symbol = mapping["symbol"]
        if last_request_started is not None:
            elapsed = time.monotonic() - last_request_started
            remaining = min_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)

        for attempt in range(2):
            last_request_started = time.monotonic()
            try:
                histories[symbol] = client.ranking_history(
                    mapping["ticker"], market=mapping["market"]
                )
                break
            except Exception as exc:
                message = str(exc)
                if "rate limit" in message.lower() and attempt == 0:
                    time.sleep(retry_wait_seconds)
                    continue
                errors[symbol] = message
                break
    return histories, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--history", type=Path)
    parser.add_argument("--prices", type=Path)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--markets", default="us", help="Comma-separated: us,europe")
    parser.add_argument("--symbols", help="Optional comma-separated scanner symbols")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--cooldown", type=int, default=5)
    parser.add_argument("--max-staleness-days", type=int, default=3)
    parser.add_argument("--danelfin-positive-min", type=float, default=8.0)
    parser.add_argument("--scanner-top-percentile", type=float, default=0.20)
    parser.add_argument("--danelfin-min-interval", type=float, default=6.5)
    parser.add_argument("--events-out", type=Path)
    parser.add_argument("--summary-out", type=Path)
    args = parser.parse_args()

    root = args.root.resolve()
    load_dotenv(root / ".env")
    history = args.history or root / "artifacts" / "research" / "history_analysis.csv"
    prices = args.prices or root / "artifacts" / "research" / "price_backfill.csv"
    events_out = args.events_out or root / "artifacts" / "research" / "danelfin_comparison_events.csv"
    summary_out = args.summary_out or root / "artifacts" / "research" / "danelfin_comparison.json"

    rows = read_csv_rows(history)
    if prices.exists() and prices.resolve() != history.resolve():
        rows.extend(read_csv_rows(prices))

    derived_percentiles = derive_missing_rank_percentiles(rows)

    markets = tuple(item.strip() for item in args.markets.split(",") if item.strip())
    if args.symbols:
        mappings = []
        for symbol in (item.strip() for item in args.symbols.split(",")):
            if not symbol:
                continue
            market = infer_danelfin_market(symbol)
            if market not in markets:
                continue
            mappings.append({"symbol": symbol, "ticker": symbol, "market": market})
    else:
        mappings = select_symbols(rows, limit=args.limit, markets=markets)

    client = DanelfinClient.from_env()
    histories, errors = fetch_histories_paced(
        client,
        mappings,
        min_interval_seconds=args.danelfin_min_interval,
    )

    benchmark_info = refresh_benchmark_from_yahoo(rows, args.benchmark or None, histories)

    events = build_comparison_events(
        rows,
        histories,
        benchmark_symbol=args.benchmark or None,
        cooldown_sessions=args.cooldown,
        max_scanner_staleness_days=args.max_staleness_days,
    )
    summary = summarize_events(
        events,
        danelfin_positive_min=args.danelfin_positive_min,
        scanner_top_percentile=args.scanner_top_percentile,
    )
    summary["requested_symbols"] = [item["symbol"] for item in mappings]
    summary["danelfin_symbols_with_history"] = sorted(
        symbol for symbol, values in histories.items() if values
    )
    summary["fetch_errors"] = errors
    summary["benchmark_symbol"] = args.benchmark or None
    summary["benchmark_research_source"] = benchmark_info
    summary["derived_rank_percentile_rows"] = derived_percentiles
    summary["cooldown_sessions"] = args.cooldown
    summary["max_scanner_staleness_days"] = args.max_staleness_days
    summary["danelfin_min_interval_seconds"] = args.danelfin_min_interval
    summary["api_key_source"] = "DANELFIN_API_KEY environment only"

    write_events_csv(events, events_out)
    write_summary(summary, summary_out)

    print(json.dumps({
        "requested_symbols": len(mappings),
        "danelfin_symbols_with_history": len(summary["danelfin_symbols_with_history"]),
        "events": len(events),
        "derived_rank_percentile_rows": derived_percentiles,
        "benchmark_rows_added": benchmark_info.get("added_rows", 0),
        "summary": str(summary_out),
        "events_csv": str(events_out),
        "fetch_error_count": len(errors),
    }, indent=2))


if __name__ == "__main__":
    main()
