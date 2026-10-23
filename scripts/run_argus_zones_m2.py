#!/usr/bin/env python3
"""Generate research-only, standalone ARGUS M2 zone JSON from archived candles."""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.research.zones.engine import build_zones, ZoneInputError


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--asof", required=True)
    p.add_argument("--bar-end-utc", required=True)
    p.add_argument("--published-at", default=None)
    p.add_argument("--price-basis", choices=("auto", "raw", "adjusted"), default="auto")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--manifest", type=Path, required=True)
    args = p.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "argus_zones_asset_identity_v1":
        raise SystemExit("M2_BLOCKED: invalid_manifest_version")
    published_at = args.published_at or datetime.now(timezone.utc).isoformat()
    symbols = manifest.get("assets")
    if not isinstance(symbols, list) or not symbols:
        raise SystemExit("M2_BLOCKED: missing_explicit_assets")
    names = {str(a.get("symbol", "")) for a in symbols}
    if len(names) != len(symbols):
        raise SystemExit("M2_BLOCKED: duplicate_symbol")
    with args.input.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not {"date", "symbol", "currency", "open", "high", "low", "close",
                "adj_close", "volume"}.issubset(reader.fieldnames or []):
            raise SystemExit("M2_BLOCKED: incomplete_ohlcv_archive_schema")
        data = [row for row in reader if row["symbol"] in names]
    # This script intentionally does not write to the productive scanner pipeline.
    output = []
    for asset in symbols:
        try:
            result = build_zones(
                data, symbol=asset["symbol"], exchange=asset["exchange"],
                asset_id=asset["asset_id"], currency=asset["currency"],
                asof=args.asof, bar_end_utc=args.bar_end_utc,
                published_at=published_at, price_basis=args.price_basis,
            )
        except (ZoneInputError, KeyError, ValueError) as exc:
            raise SystemExit("M2_BLOCKED:" + str(asset.get("symbol")) + ":" + str(exc))
        output.append(result)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for row in output:
        file = args.output_dir / (row["symbol"] + ".json")
        file.write_text(json.dumps(row, ensure_ascii=False, indent=2, sort_keys=True,
                                   allow_nan=False) + "\n", encoding="utf-8")
        print(json.dumps({
            "symbol": row["symbol"], "asof": row["asof"],
            "close": row["close"], "basis": row["price_basis"],
            "zones": len(row["zones"]), "family_counts": row["diagnostics"]["family_count"],
            "output": str(file), "evidence": "not_validated",
        }, sort_keys=True))
    print("M2_RESEARCH_GENERATED", len(output))


if __name__ == "__main__":
    main()
