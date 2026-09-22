"""Run an independent Danelfin-vs-scanner historical comparison."""
import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

from scanner.reports.danelfin_compare import (
    DanelfinClient,
    build_comparison_events,
    infer_danelfin_market,
    read_csv_rows,
    select_symbols,
    summarize_events,
    write_events_csv,
    write_summary,
)


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
    histories, errors = {}, {}
    for mapping in mappings:
        symbol = mapping["symbol"]
        try:
            histories[symbol] = client.ranking_history(
                mapping["ticker"], market=mapping["market"]
            )
        except Exception as exc:
            errors[symbol] = str(exc)

    events = build_comparison_events(
        rows,
        histories,
        benchmark_symbol=args.benchmark or None,
        cooldown_sessions=args.cooldown,
        max_scanner_staleness_days=args.max_staleness_days,
    )
    summary = summarize_events(events)
    summary["requested_symbols"] = [item["symbol"] for item in mappings]
    summary["danelfin_symbols_with_history"] = sorted(
        symbol for symbol, values in histories.items() if values
    )
    summary["fetch_errors"] = errors
    summary["benchmark_symbol"] = args.benchmark or None
    summary["cooldown_sessions"] = args.cooldown
    summary["max_scanner_staleness_days"] = args.max_staleness_days
    summary["api_key_source"] = "DANELFIN_API_KEY environment only"

    write_events_csv(events, events_out)
    write_summary(summary, summary_out)

    print(json.dumps({
        "requested_symbols": len(mappings),
        "danelfin_symbols_with_history": len(summary["danelfin_symbols_with_history"]),
        "events": len(events),
        "summary": str(summary_out),
        "events_csv": str(events_out),
        "fetch_error_count": len(errors),
    }, indent=2))


if __name__ == "__main__":
    main()
