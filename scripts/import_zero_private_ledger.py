#!/usr/bin/env python3
"""Private ZERO transaction ledger: dated fills, dividends and cash flows.

Amounts are broker EUR cash movements, not reconstructed gross trade prices.
A sell does not establish realized P/L without an explicit cost-basis convention.
"""
from __future__ import annotations
import argparse
import csv
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import re

ISIN = re.compile(r"\bISIN\s+([A-Z]{2}[A-Z0-9]{10})\b")
UNITS = re.compile(r"\bSTK\s+([0-9.]+(?:,[0-9]+)?)")
ACTION = re.compile(r"\b(Kauf|Verkauf)\b", re.I)

def decimal(text):
    return Decimal(str(text).strip().replace(".", "").replace(",", "."))

def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))

def parse(entries):
    events = []
    for n, row in enumerate(entries, 2):
        if row.get("Status", "").strip().lower() != "gebucht" or row.get("Betrag storniert", "").strip():
            continue
        memo = row["Verwendungszweck"].strip()
        amount = decimal(row["Betrag"])
        date = datetime.strptime(row["Datum"], "%d.%m.%Y").date().isoformat()
        valuta = datetime.strptime(row["Valuta"], "%d.%m.%Y").date().isoformat()
        isin_match, units_match, action_match = ISIN.search(memo), UNITS.search(memo), ACTION.search(memo)
        kind = "other"
        if "Coupons/Dividende:" in memo:
            kind = "dividend"
        elif "Steuerausgleich:" in memo:
            kind = "tax_adjustment"
        elif ("Order" in memo or "Sparplan" in memo) and action_match:
            kind = "buy" if action_match.group(1).lower() == "kauf" else "sell"
        elif amount > 0:
            kind = "credit_other"
        else:
            kind = "debit_other"
        if kind in {"buy", "sell"} and (not isin_match or not units_match):
            raise ValueError(f"unparseable_trade_row:{n}")
        if kind == "buy" and amount >= 0 or kind == "sell" and amount <= 0:
            raise ValueError(f"unexpected_trade_cash_direction:{n}")
        events.append({
            "row": n, "date": date, "value_date": valuta,
            "kind": kind, "isin": isin_match.group(1) if isin_match else None,
            "quantity": str(decimal(units_match.group(1))) if units_match and kind in {"buy", "sell"} else None,
            "amount_eur": str(amount), "memo": memo,
        })
    return events

def reconcile(events, stock_rows, crypto_rows):
    quantities = defaultdict(Decimal)
    first = {}
    trade_counts = defaultdict(lambda: {"buys": 0, "sells": 0})
    for event in events:
        if event["kind"] not in {"buy", "sell"}:
            continue
        isin = event["isin"]
        units = Decimal(event["quantity"])
        quantities[isin] += units if event["kind"] == "buy" else -units
        first[isin] = min(first.get(isin, event["date"]), event["date"])
        trade_counts[isin]["buys" if event["kind"] == "buy" else "sells"] += 1
    expected = {}
    for row in stock_rows + crypto_rows:
        isin = row["ISIN"].strip()
        if isin in expected:
            raise ValueError(f"duplicate_current_isin:{isin}")
        expected[isin] = decimal(row["Anzahl"])
    differences = [{
        "isin": isin, "recorded": str(quantities.get(isin, Decimal(0))),
        "current": str(qty),
    } for isin, qty in expected.items() if abs(quantities.get(isin, Decimal(0)) - qty) > Decimal("0.00000001")]
    orphan = [{"isin": isin, "reconstructed": str(qty)} for isin, qty in quantities.items()
              if isin not in expected and abs(qty) > Decimal("0.00000001")]
    return {
        "schema_version": "zero_private_transaction_reconciliation_v1",
        "event_count": len(events),
        "event_types": {kind: sum(e["kind"] == kind for e in events)
                        for kind in sorted({e["kind"] for e in events})},
        "current_position_count": len(expected),
        "matching_position_count": len(expected) - len(differences),
        "mismatches": differences, "unexplained_open_positions": orphan,
        "first_trade_dates": {isin: first.get(isin) for isin in expected},
        "trade_counts": {isin: trade_counts[isin] for isin in expected},
        "dividend_cash_eur": str(sum((Decimal(e["amount_eur"]) for e in events
                                      if e["kind"] == "dividend"), Decimal(0))),
        "cash_flow_taxonomy": "credits/debits other are not automatically assumed deposits/withdrawals",
        "realized_profit_calculated": False,
    }

def main():
    p = argparse.ArgumentParser()
    for name in ("transactions", "stocks", "crypto", "output"):
        p.add_argument("--" + name, required=True, type=Path)
    a = p.parse_args()
    root = Path(__file__).resolve().parents[1]
    if a.output.resolve().is_relative_to(root):
        p.error("private ledger output must be outside repository")
    events = parse(rows(a.transactions))
    report = reconcile(events, rows(a.stocks), rows(a.crypto))
    result = {"report": report, "events": events}
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    a.output.chmod(0o600)
    print(json.dumps({k: report[k] for k in ("event_count", "matching_position_count", "current_position_count", "mismatches", "unexplained_open_positions")}, ensure_ascii=False))
    if report["mismatches"] or report["unexplained_open_positions"]:
        raise SystemExit("ledger reconciliation incomplete")

if __name__ == "__main__":
    main()
