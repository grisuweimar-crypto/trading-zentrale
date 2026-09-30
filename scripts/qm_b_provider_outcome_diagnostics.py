#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter, defaultdict
import argparse
import csv
from datetime import date
import json
from pathlib import Path

from scanner.research.governance.qm_b_provider_outcome_availability import (
    _price_index,
    _scanner_events,
    classify_forward_outcome,
    load_provider_outcome_contract,
)


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    parser = argparse.ArgumentParser(description="Decompose QM-B forward-outcome coverage gaps")
    parser.add_argument("--root", default=".")
    parser.add_argument("--contract", default="configs/qm_b_provider_outcome_availability_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    contract = load_provider_outcome_contract(args.contract)
    inputs = contract["inputs"]
    metadata = json.loads((root / inputs["metadata"]["path"]).read_text(encoding="utf-8"))
    audit_as_of = date.fromisoformat((metadata.get("price_coverage") or {}).get("as_of") or metadata["as_of"])
    events = _scanner_events(read_csv(root / inputs["history_analysis"]["path"]))
    price_dates, closes, _ = _price_index(read_csv(root / inputs["price_backfill"]["path"]), as_of=audit_as_of)

    missing_reasons = Counter()
    missing_by_symbol = Counter()
    missing_by_weekday = Counter()
    missing_by_month = Counter()
    no_series_by_symbol = Counter()
    exact_gap_by_symbol = Counter()
    start_available_by_symbol = Counter()

    for event_day, symbol in events:
        days = price_dates.get(symbol, [])
        exact = (symbol, event_day) in closes
        if exact:
            start_available_by_symbol[symbol] += 1
            continue
        missing_by_symbol[symbol] += 1
        missing_by_weekday[event_day.strftime("%A")] += 1
        missing_by_month[event_day.strftime("%Y-%m")] += 1
        if not days:
            reason = "NO_PRICE_SERIES_FOR_OBSERVED_IDENTIFIER"
            no_series_by_symbol[symbol] += 1
        else:
            reason = "EXACT_DATE_ABSENT_WITH_EXISTING_SERIES"
            exact_gap_by_symbol[symbol] += 1
        missing_reasons[reason] += 1

    unknown_by_horizon = {}
    for horizon in contract["outcome_availability"]["horizons_trading_sessions"]:
        unknown_dates = []
        unknown_symbols = Counter()
        for event_day, symbol in events:
            result = classify_forward_outcome(
                symbol=symbol,
                event_date=event_day,
                horizon=int(horizon),
                price_dates=price_dates,
                closes=closes,
                audit_as_of=audit_as_of,
            )
            if result["availability_status"] == "UNKNOWN":
                unknown_dates.append(event_day)
                unknown_symbols[symbol] += 1
        unknown_by_horizon[str(horizon)] = {
            "count": len(unknown_dates),
            "first_event_date": min(unknown_dates).isoformat() if unknown_dates else None,
            "last_event_date": max(unknown_dates).isoformat() if unknown_dates else None,
            "top_symbols": unknown_symbols.most_common(20),
        }

    report = {
        "audit_as_of": audit_as_of.isoformat(),
        "event_count": len(events),
        "start_price_available_count": sum(start_available_by_symbol.values()),
        "start_price_missing_count": sum(missing_by_symbol.values()),
        "missing_reason_counts": dict(sorted(missing_reasons.items())),
        "missing_weekday_counts": dict(sorted(missing_by_weekday.items())),
        "missing_month_counts": dict(sorted(missing_by_month.items())),
        "symbols_without_any_price_series_count": len(no_series_by_symbol),
        "symbols_without_any_price_series": no_series_by_symbol.most_common(),
        "top_missing_symbols": missing_by_symbol.most_common(40),
        "top_exact_date_gaps_with_series": exact_gap_by_symbol.most_common(40),
        "unknown_by_horizon": unknown_by_horizon,
    }
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
