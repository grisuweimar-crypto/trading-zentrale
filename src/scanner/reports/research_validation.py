"""Cross-file integrity gates shared by generation and CI publication."""
from collections import Counter
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from scanner.data.price_history import coverage, validated_rows

from scanner.reports.research_views import (
    OUTPUT, PRICE_COLUMNS, SCHEMA_VERSION, VIEW_COLUMNS, as_of, observed, parse_csv, score,
)

NAMES = ("history_analysis", "history_recent", "latest_scanner", "price_backfill")


def require(condition, message):
    if not condition:
        raise ValueError("research_integrity: " + message)


def validate_bundle(metadata, blobs, archive_before=None):
    """Validate serialized bytes before publishing the manifest; never repair data."""
    frames = {}
    for name in NAMES:
        data = blobs[name]
        require(metadata[name]["sha256"] == (hashlib.sha256(data).hexdigest() if data is not None else None),
                name + " hash mismatch")
        frames[name] = parse_csv(data) if data is not None else ([], [])
        require(metadata[name]["row_count"] == len(frames[name][1]), name + " row count mismatch")
    ac, archive = frames["history_analysis"]
    if archive_before is not None:
        old_columns, old_rows = parse_csv(archive_before)
        require(set(old_columns).issubset(ac), "archive columns removed")
        require([{c: r.get(c, "") for c in old_columns} for r in archive[:len(old_rows)]] == old_rows,
                "historical observations changed")
    pc, prices = frames["price_backfill"]
    require(pc == PRICE_COLUMNS, "price allowlist violated")
    require(all(r["observation_type"] == "price_backfill" for r in prices), "invalid price provenance")
    valid_prices, price_issues = validated_rows(prices)
    require(not price_issues and len(valid_prices) == len(prices), "invalid or duplicate price sessions")
    complete = metadata["latest_run_complete"]
    if complete:
        require(all(blobs[n] is not None for n in NAMES), "missing required view")
    for name in ("latest_scanner", "history_recent"):
        _, rows = frames[name]
        for row in rows:
            require(row.get("snapshot_id") == metadata["snapshot_id"], name + " snapshot_id mismatch")
            require(row.get("schema_version") == SCHEMA_VERSION, name + " schema_version mismatch")
            timestamp = datetime.fromisoformat(row["generated_at"])
            require(timestamp.utcoffset() == timedelta(0), name + " timestamp not UTC")
            require(observed(row), name + " contains non-scanner observations")
    _, latest = frames["latest_scanner"]
    if "price_coverage" in metadata:
        reported = metadata["price_coverage"]
        expected = coverage([r["symbol"] for r in latest], prices,
                            minimum_sessions=reported["minimum_sessions_target"],
                            as_of=reported["as_of"], fetch_state=reported["symbols"])
        require(reported == expected, "price coverage does not match stored sessions")
        require(not latest or reported["as_of"] == metadata["last_complete_scan"], "price coverage date mismatch")
    require(len({r["symbol"].strip() for r in latest}) == len(latest), "duplicate latest symbols")
    require(all(r["symbol"].strip() for r in latest), "empty latest symbol")
    require(all(as_of(r) == metadata["last_complete_scan"] for r in latest), "latest as_of mismatch")
    if complete:
        require(bool(latest) and metadata["as_of"] == metadata["last_complete_scan"], "complete run without current latest")
    ordered = sorted([score(r) for r in latest if score(r) is not None], reverse=True)
    for row in latest:
        value = score(row)
        rank = ordered.index(value) + 1 if value is not None else None
        require(row["universe_size"] == str(len(ordered)), "universe size mismatch")
        require(row["rank"] == (str(rank) if rank else ""), "rank mismatch")
        require(row["rank_percentile"] == (str(rank / len(ordered)) if rank else ""), "rank percentile mismatch")
    _, recent = frames["history_recent"]
    if recent or complete:
        scanner = [r for r in archive if observed(r)]
        newest = max(date.fromisoformat(as_of(r)) for r in scanner)
        cutoff = newest - timedelta(days=120)
        expected = [r for r in scanner if cutoff <= date.fromisoformat(as_of(r)) <= newest]
        keys = [c for c in ac if c not in VIEW_COLUMNS]
        project = lambda rows: Counter(tuple(r.get(c, "") for c in keys) for r in rows)
        require(project(recent) == project(expected), "recent not derived from archive")
        require(all(cutoff <= date.fromisoformat(as_of(r)) <= newest for r in recent), "recent outside window")
    return metadata


def validate_publication(root: Path):
    output = root / OUTPUT
    metadata = json.loads((output / "history_metadata.json").read_text(encoding="utf-8"))
    blobs = {n: (output / (n + ".csv")).read_bytes() if (output / (n + ".csv")).exists() else None for n in NAMES}
    return validate_bundle(metadata, blobs)
