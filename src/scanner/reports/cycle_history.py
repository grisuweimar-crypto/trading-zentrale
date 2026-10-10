"""CY-03: append-only, prospective cycle observations and research eligibility.

No legacy/backfilled cycle value is imported. The CY02-B01 external provenance
gate (#269) blocks research even for internally replayable observations.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re

from scanner.reports.research_views import atomic_write, parse_csv

VERSION = "cycle_observations_cy03_v1"
FORMULA = "cycle_detrended_sma20_range40_v1"
SOURCE = "YAHOO_PIT_CYCLE_V1"
ROOT = Path("artifacts/cycle_history")
FIELDS = ("schema_version", "snapshot_id", "run_id", "asset_id", "listing_symbol",
          "price_symbol", "currency", "market_timezone", "as_of", "generated_at",
          "cycle_as_of", "computed_at", "last_bar", "cycle", "quality", "reason",
          "source", "formula", "price_source", "price_basis", "price_sha256",
          "watchlist_sha256", "bars_sha256", "currency_lineage",
          "session_time_quality", "availability")
MASK = ("snapshot_id", "as_of", "asset_id", "run_id", "availability",
        "lag_1obs", "lag_5obs", "lag_10obs", "research_status")
COVERAGE = ("asset_id", "month", "observations", "valid_current",
            "lag_1obs", "lag_5obs", "lag_10obs", "research_eligible", "exclusions")
HASH = re.compile(r"^[0-9a-f]{64}$")
UUID = re.compile(r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def csv_bytes(columns, rows):
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows({c: row.get(c, "") for c in columns} for row in rows)
    return out.getvalue().encode("utf-8")


def read_ledger(path):
    if not path.exists():
        return []
    columns, rows = parse_csv(path.read_bytes())
    if tuple(columns) != FIELDS:
        raise ValueError("cy03:ledger_schema_mismatch")
    return rows


def utc(value):
    d = datetime.fromisoformat(value)
    if d.tzinfo is None or d.utcoffset() != timezone.utc.utcoffset(d):
        raise ValueError("cy03:timestamp_not_utc")
    return d


def eligibility(row):
    quality = row.get("cycle_quality", "")
    if quality != "VALID":
        if row.get("cycle", "").strip() or row.get("cycle_price_sha256", "").strip():
            raise ValueError("cy03:invalid_cycle_resurrected")
        return "EXCLUDED_" + (quality or "UNKNOWN")
    try:
        value = float(row["cycle"])
        ca, computed, generated = (utc(row[k]) for k in
                                    ("cycle_as_of", "cycle_computed_at", "generated_at"))
        bar = date.fromisoformat(row["cycle_last_bar"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("cy03:incomplete_valid_provenance") from exc
    if (not math.isfinite(value) or not 0 <= value <= 100
        or row.get("cycle_source") != SOURCE
        or row.get("cycle_formula_version") != FORMULA
        or not HASH.fullmatch(row.get("cycle_price_sha256", ""))
        or not row.get("cycle_currency") or not row.get("cycle_price_symbol")
        or not row.get("cycle_price_basis")
        or row.get("cycle_currency_lineage") != "WATCHLIST_DECLARED_ONLY"
        or row.get("cycle_session_time_quality") != "SESSION_DATE_CUTOFF_ONLY"
        or not bar < ca.date() or ca > computed or computed > generated):
        raise ValueError("cy03:invalid_valid_provenance")
    return "PROVISIONAL_REPLAYED"


def identity(row):
    return tuple(row[k] for k in ("asset_id", "listing_symbol", "price_symbol",
                "currency", "source", "formula", "price_source", "price_basis",
                "currency_lineage", "session_time_quality"))


def lag_mask(rows):
    """Latest same-day run is canonical. All intervening scan dates must match."""
    groups = defaultdict(dict)
    day_versions = {}
    seen = set()
    for row in rows:
        key = (row["snapshot_id"], row["asset_id"])
        if key in seen:
            raise ValueError("cy03:duplicate_asset_in_snapshot")
        seen.add(key)
        day = date.fromisoformat(row["as_of"])
        stamp = utc(row["generated_at"])
        if stamp.date() < day:
            raise ValueError("cy03:future_scan_date")
        groups[row["snapshot_id"]][row["asset_id"]] = row
        newer = (stamp.isoformat(), row["snapshot_id"])
        if row["as_of"] not in day_versions or newer > day_versions[row["as_of"]][0]:
            day_versions[row["as_of"]] = (newer, row["snapshot_id"])
    days = sorted(day_versions)
    latest = {d: day_versions[d][1] for d in days}
    index = {d: i for i, d in enumerate(days)}
    result = []
    for row in rows:
        day, sid, asset = row["as_of"], row["snapshot_id"], row["asset_id"]
        out = {"snapshot_id": sid, "as_of": day, "asset_id": asset,
               "run_id": row["run_id"], "availability": row["availability"],
               "research_status": "BLOCKED_EXTERNAL_VERIFICATION_269"}
        for count in (1, 5, 10):
            field = f"lag_{count}obs"
            if sid != latest[day]:
                status = "SUPERSEDED_SAME_DAY"
            elif row["availability"] != "PROVISIONAL_REPLAYED":
                status = "INVALID_CURRENT"
            elif index[day] < count:
                status = "NO_PRIOR"
            else:
                span = days[index[day]-count:index[day]+1]
                chain = [groups[latest[d]].get(asset) for d in span]
                if any(x is None for x in chain):
                    status = "UNIVERSE_OR_OBSERVATION_GAP"
                elif any(x["availability"] != "PROVISIONAL_REPLAYED" for x in chain):
                    status = "INVALID_CHAIN"
                elif any(identity(x) != identity(chain[0]) for x in chain):
                    status = "IDENTITY_OR_FORMULA_CHANGED"
                elif any((date.fromisoformat(b["as_of"])-date.fromisoformat(a["as_of"])).days >
                         (1 if asset.upper().endswith(("-USD", "-EUR", "-USDT", "-BTC")) else 5)
                         for a,b in zip(chain, chain[1:])):
                    status = "SCAN_GAP"
                elif any(b["cycle_as_of"] <= a["cycle_as_of"] or b["last_bar"] < a["last_bar"]
                         for a,b in zip(chain, chain[1:])):
                    status = "NONMONOTONIC_PROVENANCE"
                else:
                    status = "PROVISIONAL_CHAIN"
            out[field] = status
        result.append(out)
    return result


def coverage_rows(mask):
    grouped = defaultdict(Counter)
    for row in mask:
        if row["lag_1obs"] == "SUPERSEDED_SAME_DAY":
            continue
        c = grouped[(row["asset_id"], row["as_of"][:7])]
        c["observations"] += 1
        c["valid_current"] += row["availability"] == "PROVISIONAL_REPLAYED"
        for n in (1, 5, 10):
            status = row[f"lag_{n}obs"]
            c[f"lag_{n}obs"] += status == "PROVISIONAL_CHAIN"
            if status != "PROVISIONAL_CHAIN":
                c[status] += 1
    return [dict(asset_id=asset, month=month,
                 observations=str(c["observations"]), valid_current=str(c["valid_current"]),
                 lag_1obs=str(c["lag_1obs"]), lag_5obs=str(c["lag_5obs"]),
                 lag_10obs=str(c["lag_10obs"]), research_eligible="0",
                 exclusions=";".join(f"{k}:{v}" for k,v in sorted(c.items())
                                    if k not in ("observations","valid_current",
                                                 "lag_1obs","lag_5obs","lag_10obs")))
            for (asset,month),c in sorted(grouped.items())]


def record(root: Path, *, write=True):
    """Audit the exact current source and append once; --dry-run does not write."""
    from scripts.audit_cycle_current import audit_csv
    root = Path(root)
    meta = json.loads((root/"artifacts/research/history_metadata.json").read_text(encoding="utf-8"))
    if not meta.get("latest_run_complete") or meta.get("validation",{}).get("status") != "ok":
        raise ValueError("cy03:incomplete_run")
    sid = meta.get("snapshot_id", "")
    if not UUID.fullmatch(sid):
        raise ValueError("cy03:missing_snapshot_id")
    latest_bytes = (root/"artifacts/research/latest_scanner.csv").read_bytes()
    if sha(latest_bytes) != meta.get("latest_scanner",{}).get("sha256"):
        raise ValueError("cy03:latest_hash_mismatch")
    source = root/"artifacts/watchlist/watchlist_full.csv"
    if sha(source.read_bytes()) != meta.get("source",{}).get("sha256"):
        raise ValueError("cy03:source_hash_mismatch")
    bars_path = root/"artifacts/reports/cycle_input_bars.csv.gz"
    errors, _ = audit_csv(source, report_path=root/"artifacts/reports/cycle_quality.csv",
                           bars_path=bars_path)
    if errors:
        raise ValueError("cy03:current_replay_failed:"+errors[0])
    bars = bars_path.read_bytes()
    barhash = sha(bars)
    _, latest = parse_csv(latest_bytes)
    if not latest:
        raise ValueError("cy03:empty_latest")
    records = []
    assets = set()
    for row in sorted(latest, key=lambda x: x.get("symbol", "")):
        asset = row.get("symbol", "")
        if (not asset or asset in assets or row.get("snapshot_id") != sid
            or row.get("as_of") != meta["as_of"]
            or row.get("generated_at") != meta["generated_at"]
            or row.get("run_id") != meta["daily_run"]["run_id"]
            or row.get("observation_type") != "observed_scanner"
            or row.get("data_source") != "scanner_run"):
            raise ValueError("cy03:run_or_asset_identity_mismatch")
        assets.add(asset)
        status = eligibility(row)
        records.append(dict(schema_version=VERSION, snapshot_id=sid,
            run_id=row["run_id"], asset_id=asset, listing_symbol=asset,
            price_symbol=row.get("cycle_price_symbol",""),
            currency=row.get("cycle_currency",""), market_timezone="UNVERIFIED",
            as_of=row["as_of"], generated_at=row["generated_at"],
            cycle_as_of=row.get("cycle_as_of",""), computed_at=row.get("cycle_computed_at",""),
            last_bar=row.get("cycle_last_bar",""), cycle=row.get("cycle",""),
            quality=row.get("cycle_quality",""), reason=row.get("cycle_quality_reason",""),
            source=row.get("cycle_source",""), formula=row.get("cycle_formula_version",""),
            price_source=row.get("cycle_price_source",""),
            price_basis=row.get("cycle_price_basis",""),
            price_sha256=row.get("cycle_price_sha256",""),
            watchlist_sha256=meta["source"]["sha256"], bars_sha256=barhash,
            currency_lineage=row.get("cycle_currency_lineage",""),
            session_time_quality=row.get("cycle_session_time_quality",""),
            availability=status))
    dest = root/ROOT
    ledger_path = dest/"observations.csv"
    old = read_ledger(ledger_path)
    found = [r for r in old if r["snapshot_id"] == sid]
    if found and found != records:
        raise ValueError("cy03:immutable_snapshot_changed")
    combined = old if found else old + records
    mask = lag_mask(combined)
    coverage = coverage_rows(mask)
    outputs = {"observations.csv": csv_bytes(FIELDS, combined),
               "eligibility.csv": csv_bytes(MASK, mask),
               "coverage.csv": csv_bytes(COVERAGE, coverage)}
    manifest = {"schema_version": VERSION, "latest_snapshot_id": sid,
        "run_id": meta["daily_run"]["run_id"], "as_of": meta["as_of"],
        "observations": len(combined), "snapshots": len({r["snapshot_id"] for r in combined}),
        "new_current_valid": sum(r["availability"] == "PROVISIONAL_REPLAYED" for r in records),
        "new_current_excluded": sum(r["availability"] != "PROVISIONAL_REPLAYED" for r in records),
        "provisional_lags": {str(n):sum(m[f"lag_{n}obs"]=="PROVISIONAL_CHAIN" for m in mask)
                             for n in (1,5,10)},
        "research_eligible": 0, "research_gate":"BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269",
        "archive_bars_sha256": barhash,
        "source_watchlist_sha256": meta["source"]["sha256"],
        "file_sha256": {name:sha(data) for name,data in outputs.items()}}
    if write:
        dest.mkdir(parents=True, exist_ok=True)
        bar_archive = dest/"bars"/f"{sid}.csv.gz"
        bar_archive.parent.mkdir(parents=True, exist_ok=True)
        if bar_archive.exists() and bar_archive.read_bytes() != bars:
            raise ValueError("cy03:immutable_bar_evidence_changed")
        if not bar_archive.exists():
            atomic_write(bar_archive, bars)
        for name,data in outputs.items():
            target = dest/name
            if not target.exists() or target.read_bytes() != data:
                atomic_write(target,data)
        atomic_write(dest/"manifest.json",
                     (json.dumps(manifest,indent=2,sort_keys=True,ensure_ascii=False)+"\n").encode())
    return manifest
