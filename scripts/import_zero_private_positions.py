#!/usr/bin/env python3
"""Convert private ZERO exports to the private Phase-7H position book.

No broker positions are ever written to the repository by this command.
Unmatched ISINs are rejected, never guessed from instrument names.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from pathlib import Path


def number(raw: str) -> float:
    try:
        return float(Decimal(str(raw).replace(".", "").replace(",", ".")))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid_numeric_value:{raw!r}") from exc


def read_zero(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter=";"))
    required = {"Name", "ISIN", "Art", "Anzahl", "Kaufkurs", "Kurs", "Kursdatum", "Wert"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError(f"missing_zero_columns_or_rows:{path.name}")
    return rows


def convert(stocks: Path, crypto: Path, symbol_map: dict[str, str], as_of: str) -> dict:
    observed = datetime.fromisoformat(as_of)
    if observed.tzinfo is None:
        raise ValueError("as_of_must_have_timezone")
    rows = read_zero(stocks) + read_zero(crypto)
    seen_isins: set[str] = set()
    seen_symbols: set[str] = set()
    positions = []
    unresolved = []
    for row in rows:
        isin = row["ISIN"].strip()
        if not isin or isin in seen_isins:
            raise ValueError(f"missing_or_duplicate_isin:{isin}")
        seen_isins.add(isin)
        symbol = symbol_map.get(isin, "").strip()
        if not symbol:
            unresolved.append({"isin": isin, "name": row["Name"]})
            continue
        if symbol in seen_symbols:
            raise ValueError(f"duplicate_scanner_symbol:{symbol}")
        seen_symbols.add(symbol)
        date = datetime.strptime(row["Kursdatum"].strip(), "%d.%m.%Y").date()
        if date > observed.date():
            raise ValueError(f"future_quote_date:{isin}")
        quantity = number(row["Anzahl"])
        entry = number(row["Kaufkurs"])
        current = number(row["Kurs"])
        value = number(row["Wert"])
        if min(quantity, entry, current, value) <= 0:
            raise ValueError(f"nonpositive_position:{isin}")
        positions.append({
            "schema_version": "decision_position_snapshot_v1",
            "symbol": symbol,
            "source_snapshot_id": f"zero:{as_of}:{isin}",
            "as_of": as_of,
            "position_state": "long",
            "quantity": quantity,
            "market_value": value,
            "average_entry_price": entry,
            "current_price": current,
            "currency": "EUR",
            "transaction_cost_bps": None,
            "broker_isin": isin,
            "broker_name": row["Name"],
            "broker_asset_class": row["Art"],
            "broker_quote_date": date.isoformat(),
        })
    if unresolved:
        raise ValueError("unresolved_ISIN_to_scanner_symbol:" + json.dumps(unresolved, ensure_ascii=False))
    digest = sha256(("|".join(sorted(seen_isins)) + "|" + as_of).encode()).hexdigest()[:16]
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": f"zero-private-{digest}",
        "as_of": as_of,
        "positions": positions,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Private ZERO CSV to Decision Depot Watch JSON")
    p.add_argument("--stocks", type=Path, required=True)
    p.add_argument("--crypto", type=Path, required=True)
    p.add_argument("--symbol-map", type=Path, required=True,
                   help="Private JSON mapping exact ZERO ISIN to exact Scanner symbol")
    p.add_argument("--as-of", required=True,
                   help="Explicit ISO-8601 snapshot timestamp with timezone, at or after quotes")
    p.add_argument("--output", type=Path, required=True,
                   help="PRIVATE destination outside the public Git working tree")
    args = p.parse_args()
    if args.output.resolve().is_relative_to(Path(__file__).resolve().parents[1]):
        p.error("output_must_be_outside_repository")
    mapping = json.loads(args.symbol_map.read_text(encoding="utf-8"))
    if not isinstance(mapping, dict):
        p.error("symbol_map_must_be_object")
    book = convert(args.stocks, args.crypto, mapping, args.as_of)
    from scanner.research.decision_layer.depot_watch import validate_position_book
    validate_position_book(book)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(book, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.output.chmod(0o600)
    print(f"Validated {len(book['positions'])} positions. Private output: {args.output}")


if __name__ == "__main__":
    main()
