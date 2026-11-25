"""Read-only audit of actual contemporaneous captured prices, not external PIT.

Compare all sixty archived cycle inputs to *earlier* normalized Yahoo responses.
A matching old receipt proves a client-side observation of that value, not
provider publication, freshness, quote unit, or independent source authority.
No outcomes, v1 mutations, or research release.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
import csv
import gzip
import json
import math
from pathlib import Path

from scanner.reports.cycle_history import ROOT, read_ledger, utc
from scanner.reports.cycle_prefetch_evidence import ARCHIVE_DIR, inspect_archived_witnesses
from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive


def positive(value):
    try:
        n = float(value)
        return n if math.isfinite(n) and n > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def capture_events(doc):
    """Only successful response rows, exact provider symbol and session date."""
    for attempt in doc["witness"]["attempts"]:
        if attempt.get("exception_type"):
            continue
        ended = utc(attempt["finished_at_utc"])
        if ended > utc(doc["generated_at"]):
            raise ValueError("capture_after_source_snapshot")
        rows = json.loads(gzip.decompress(base64.b64decode(
            attempt["frame_rows_gzip_base64"], validate=True)))
        for row in rows:
            if row["symbol"] not in attempt["requested_provider_symbols"]:
                continue
            price = positive(row["adj_close"])
            if price is not None:
                yield ended, row["symbol"], row["date"], price


def classify_window(window, symbol, available):
    """Exact float identity; no arbitrary tolerance, scaling or replacement."""
    counts = Counter()
    for day, value in window:
        price = positive(value)
        if price is None:
            raise ValueError("invalid_archived_price")
        seen = available.get((symbol, day), set())
        if not seen:
            counts["not_captured"] += 1
        elif price in seen:
            counts["exact_match"] += 1
            if len(seen) > 1:
                counts["other_vintage_also_seen"] += 1
        else:
            counts["different_only"] += 1
    return {k: counts[k] for k in ("exact_match", "not_captured",
                                    "different_only", "other_vintage_also_seen")}


def load_windows(root, sid):
    groups = defaultdict(list)
    with gzip.open(root / ROOT / "bars" / (sid + ".csv.gz"), "rt",
                   encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["symbol"], row["currency"], row["price_sha256"])
            groups[key].append(row)
    return groups


def audit(root):
    root = Path(root)
    checked = inspect_cycle_archive(root)
    ledger = read_ledger(root / ROOT / "observations.csv")
    receipt_audit = inspect_archived_witnesses(root, ledger)
    # Receipts are snapshot-bound by the existing verifier. Compare the actual
    # client response end time (not the later archive write) to the cutoff;
    # the client clock is not independently attested.
    events = []
    for path in sorted((root / ARCHIVE_DIR).glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        events.extend(capture_events(doc))
    events.sort(key=lambda x: x[0])
    eligible = sorted((r for r in ledger if r["availability"] == "PROVISIONAL_REPLAYED"),
                      key=lambda r: (utc(r["cycle_as_of"]), r["snapshot_id"], r["asset_id"]))
    pos = 0
    available = defaultdict(set)
    groups, previous = {}, None
    result = []
    for row in eligible:
        cutoff = utc(row["cycle_as_of"])
        while pos < len(events) and events[pos][0] <= cutoff:
            _, symbol, day, price = events[pos]
            available[(symbol, day)].add(price)
            pos += 1
        sid = row["snapshot_id"]
        if sid != previous:
            groups = load_windows(root, sid)
            previous = sid
        key = (row["price_symbol"], row["currency"], row["price_sha256"])
        bars = groups.get(key, [])
        if (len(bars) != 60 or len({b["session_date"] for b in bars}) != 60
                or any(b["formula_version"] != row["formula"] or
                       b["as_of"] != row["cycle_as_of"] for b in bars)):
            raise ValueError("unmatched_60_bar_window:" + sid + ":" + row["asset_id"])
        count = classify_window([(b["session_date"], b["close"]) for b in bars],
                                row["price_symbol"], available)
        result.append({"snapshot_id": sid, "asset_id": row["asset_id"],
                       "as_of": row["as_of"], **count})
    aggregate = {k: sum(r[k] for r in result) for k in
                 ("exact_match", "not_captured", "different_only", "other_vintage_also_seen")}
    return {
        "schema_version": "cycle_client_vintage_coverage_v1",
        "archive_latest_snapshot_id": checked["latest_snapshot_id"],
        "source_archive_manifest_hashes": checked["manifest_file_hashes"],
        "receipt_snapshots": receipt_audit["witness_snapshots"],
        "snapshots_without_receipt": receipt_audit["snapshots_without_prefetch_witness"],
        "valid_windows": len(result),
        "windows_all_60_seen": sum(r["exact_match"] == 60 for r in result),
        "windows_partial": sum(0 < r["exact_match"] < 60 for r in result),
        "windows_no_exact_matches": sum(r["exact_match"] == 0 for r in result),
        "windows_with_different_only": sum(r["different_only"] > 0 for r in result),
        "bar_counts": aggregate,
        "independent_publisher_pit_verified": False,
        "research_released": False,
        "rows": result,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.repo_root)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k:v for k,v in report.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
