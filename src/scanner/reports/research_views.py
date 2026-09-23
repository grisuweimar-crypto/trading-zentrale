"""Validated research views. Never infer scanner observations from prices.

All CSV values are strings; historical observations are append-only. The JSON
manifest is the publication marker and consumers must verify its hashes.
"""
from __future__ import annotations

from collections import Counter
import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import os
from pathlib import Path
import tempfile
from uuid import uuid4

from scanner.data.price_history import (
    LEGACY_PRICE_COLUMNS,
    MARKET_COLUMNS,
    PRICE_COLUMNS,
    coverage as price_coverage,
    merge_prices,
)

SCHEMA_VERSION = "research_views_v1"
SOURCE = "artifacts/snapshots/score_history.csv"
MARKET = "artifacts/market_data/yahoo_ohlcv.csv"
OUTPUT = "artifacts/research"
VIEW_COLUMNS = "as_of generated_at snapshot_id schema_version".split()
KNOWN_COLUMNS = set(("date symbol name score opportunity risk confidence confidence_label rs3m trend200 cycle r_code rank universe_size rank_percentile close currency sector pillar_primary cluster_official bucket_type scoring_version run_id universe_version config_version observation_type data_source liquidity_risk volatility drawdown roe growth margin debt_ratio market_regime_stock market_regime_crypto market_trend200_stock market_trend200_crypto scan_status".split()) + VIEW_COLUMNS)


@dataclass(frozen=True)
class ValidationPolicy:
    expected_symbol_count: int | None = None
    min_symbol_ratio: float = 1.0
    min_score_ratio: float = 0.90  # Existing scripts/test_pipeline.py coverage gate.
    baseline_runs: int = 20
    required_columns: tuple[str, ...] = ("date", "symbol", "score")

    def __post_init__(self):
        if (self.expected_symbol_count is not None and self.expected_symbol_count < 1
                or not 0 < self.min_symbol_ratio <= 1 or not 0 < self.min_score_ratio <= 1
                or self.baseline_runs < 1):
            raise ValueError("Invalid completeness policy")


def parse_csv(raw: bytes) -> tuple[list[str], list[dict[str, str]]]:
    records = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True))
    if not records or not records[0] or len(set(records[0])) != len(records[0]) or any(not c.strip() for c in records[0]):
        raise ValueError("invalid_csv_header")
    columns = records[0]
    if any(len(row) != len(columns) for row in records[1:]):
        raise ValueError("scan_aborted: incorrect CSV field count")
    return columns, [dict(zip(columns, row)) for row in records[1:]]


def encode_csv(columns, rows):
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows({c: row.get(c, "") for c in columns} for row in rows)
    return out.getvalue().encode("utf-8")


def atomic_write(path, raw):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp = Path(handle.name)
        try:
            handle.write(raw)
        except BaseException:
            handle.close()
            temp.unlink(missing_ok=True)
            raise
    try:
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def score(row):
    try:
        number = Decimal(row.get("score", ""))
        return number if number.is_finite() else None
    except InvalidOperation:
        return None


def observed(row):
    return row.get("observation_type", "") in ("", "observed_scanner") and row.get("data_source", "") in ("", "scanner_run")


def as_of(row):
    return row.get("as_of") or row.get("date", "")


def run_key(row):
    return as_of(row), row.get("run_id", "")


def validate_run(columns, rows, policy, baseline, today):
    errors = []
    if not set(policy.required_columns).issubset(columns):
        errors.append("missing_required_columns")
    symbols = [r.get("symbol", "").strip() for r in rows]
    if len(set(symbols)) != len(symbols):
        errors.append("duplicate_symbols")
    if not rows or not all(symbols):
        errors.append("scan_aborted")
    if any(not observed(r) for r in rows):
        errors.append("non_scanner_observations")
    if any(r.get("scan_status", "").lower() in ("aborted", "failed", "partial", "incomplete") for r in rows):
        errors.append("scan_aborted")
    try:
        dates = {date.fromisoformat(as_of(r)) for r in rows}
        if len(dates) != 1 or max(dates) > today:
            errors.append("invalid_as_of")
    except ValueError:
        errors.append("invalid_as_of")
    required = math.ceil(baseline * policy.min_symbol_ratio) if baseline else None
    if required is None:
        errors.append("missing_completeness_baseline")
    elif len(set(symbols)) < required:
        errors.append("symbol_count_below_threshold")
    count = sum(score(r) is not None for r in rows)
    if not count or count < math.ceil(len(rows) * policy.min_score_ratio):
        errors.append("invalid_score_data")
    return list(dict.fromkeys(errors)), {"baseline_symbol_count": baseline, "required_symbol_count": required,
                                        "symbol_count": len(set(symbols)), "numeric_score_count": count}


def build_views(root: Path, *, now=None, policy=None, daily_input=None):
    root = root.resolve()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    generated = now.astimezone(timezone.utc).isoformat()
    attempt_id = str(uuid4())
    policy = policy or ValidationPolicy()
    output = root / OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    names = ("history_analysis", "history_recent", "latest_scanner", "price_backfill")
    paths = {name: output / (name + ".csv") for name in names}
    meta_path = output / "history_metadata.json"
    for path in [*paths.values(), meta_path]:
        if path.is_symlink() or not path.resolve().is_relative_to(root / OUTPUT):
            raise ValueError(f"Unsafe output path: {path}")
    # Exclusive writer; readers use the manifest hashes to detect interrupted publication.
    lock = output / ".research_views.lock"
    with lock.open("x"):
        pass
    try:
        return _build(root, paths, meta_path, now, generated, attempt_id, policy, daily_input)
    finally:
        lock.unlink(missing_ok=True)


def _build(root, paths, meta_path, now, generated, attempt_id, policy, daily_input=None):
    original = {p: p.read_bytes() if p.exists() else None for p in [*paths.values(), meta_path]}
    def existing(name):
        raw = original[paths[name]]
        return parse_csv(raw) if raw is not None else ([], [])
    ac, archive = existing("history_analysis")
    lc, latest = existing("latest_scanner")
    pc, prices = existing("price_backfill")
    previous = json.loads(original[meta_path]) if original[meta_path] else {}
    warnings, errors = [], list(daily_input.errors) if daily_input else []
    source_relative = daily_input.source if daily_input else SOURCE
    source_path = root / source_relative
    raw = source_path.read_bytes() if source_path.exists() else None
    history_path = root / SOURCE
    history_raw = history_path.read_bytes() if history_path.exists() else None
    hc, history = parse_csv(history_raw) if history_raw and daily_input else ([], [])
    columns, source, current = [], [], []
    try:
        if raw is None:
            raise ValueError("missing_source")
        columns, source = parse_csv(raw)
        if daily_input:
            if hashlib.sha256(raw).hexdigest() != daily_input.source_sha256:
                raise ValueError("scanner_source_changed_during_preparation")
            columns, current = daily_input.columns, daily_input.rows
        elif source:
            newest = max(as_of(r) for r in source)
            key = run_key(next(r for r in reversed(source) if as_of(r) == newest))
            current = [r for r in source if run_key(r) == key]
    except (ValueError, csv.Error, UnicodeError) as exc:
        errors.append(str(exc))
    if not daily_input:
        hc, history = columns, source
    # Use only structurally sound prior scanner groups, never price rows.
    groups = {}
    for row in history + archive:
        if observed(row) and current and as_of(row) < as_of(current[0]):
            groups.setdefault(run_key(row), {})[row.get("symbol", "")] = row
    counts = [len(g) for _, g in sorted(groups.items())[-policy.baseline_runs:]
              if "" not in g and all(score(r) is not None for r in g.values())]
    baseline = policy.expected_symbol_count or max([len(latest), *counts], default=0) or None
    run_errors, diagnostics = validate_run(columns, current, policy, baseline, now.astimezone(timezone.utc).date())
    errors.extend(run_errors)
    if current and latest and as_of(current[0]) < as_of(latest[0]):
        errors.append("scan_older_than_last_complete")
    if raw and not raw.endswith((b"\n", b"\r")):
        errors.append("scan_aborted: missing final line ending")
    if set(columns) - KNOWN_COLUMNS:
        warnings.append("unknown_source_columns: " + ",".join(sorted(set(columns) - KNOWN_COLUMNS)))
    missing_previous = (set(ac) & KNOWN_COLUMNS) - set(columns) - set(VIEW_COLUMNS) - {"observation_type", "data_source"}
    if missing_previous:
        warnings.append("source_columns_missing_from_previous_archive: " + ",".join(sorted(missing_previous)))
    if any(not observed(r) for r in archive):
        warnings.append("legacy_non_scanner_rows_retained_in_archive_and_excluded_from_views")
    complete = not errors
    if not complete:
        # Never bless mismatched generations left by an interrupted earlier write.
        for name in ("latest_scanner", "history_recent"):
            data = original[paths[name]]
            expected = previous.get(name, {}).get("sha256")
            actual = hashlib.sha256(data).hexdigest() if data is not None else None
            if actual != expected:
                raise ValueError("inconsistent_retained_views: retry with a complete scanner source")
    pending = {}
    snapshot_id = previous.get("snapshot_id")
    view_generated = previous.get("views_generated_at")
    if complete:
        snapshot_id, view_generated = attempt_id, generated
        if daily_input:
            # Existing current-run R-code calculation, only after validation.
            current = daily_input.enrich(current)
            evaluated = sorted((score(r) for r in current if score(r) is not None), reverse=True)
            ranked = []
            for row in current:
                value = score(row)
                rank = evaluated.index(value) + 1 if value is not None else None
                ranked.append(dict(row, rank=str(rank) if rank else "", universe_size=str(len(evaluated)),
                                   rank_percentile=str(rank / len(evaluated)) if rank else ""))
            current = ranked
            columns = list(dict.fromkeys(columns + ["rank", "universe_size", "rank_percentile"]))
        incoming = [dict(r, observation_type="observed_scanner", data_source="scanner_run") for r in current]
        new_columns = list(dict.fromkeys(ac + columns + ["observation_type", "data_source"]))
        if ac and new_columns != ac:
            warnings.append("archive_schema_extended: " + ",".join(c for c in new_columns if c not in ac))
        # Multiset subtraction avoids duplicate imports without losing real duplicate
        # historical rows or rewriting a changed observation in an existing run.
        compare = [c for c in columns if c not in VIEW_COLUMNS + ["observation_type", "data_source"]]
        available = Counter(tuple(r.get(c, "") for c in compare) for r in archive if observed(r))
        for row in incoming:
            key = tuple(row.get(c, "") for c in compare)
            if available[key]:
                available[key] -= 1
            else:
                archive.append(row)
        if new_columns != ac or len(archive) != len(existing("history_analysis")[1]):
            pending[paths["history_analysis"]] = encode_csv(new_columns, archive)
        ac = new_columns
        ordered_scores = sorted((score(r) for r in current if score(r) is not None), reverse=True)
        latest = []
        for row in incoming:
            value = score(row)
            rank = ordered_scores.index(value) + 1 if value is not None else None
            latest.append(dict(row, as_of=as_of(row), generated_at=generated, snapshot_id=snapshot_id,
                               schema_version=SCHEMA_VERSION, rank=str(rank) if rank else "",
                               universe_size=str(len(ordered_scores)),
                               rank_percentile=str(rank / len(ordered_scores)) if rank else ""))
        lc = list(dict.fromkeys(columns + ["observation_type", "data_source"] + VIEW_COLUMNS + ["rank", "universe_size", "rank_percentile"]))
        # Recent contains only source fields plus explicitly documented view provenance.
        scanner_rows = [r for r in archive if observed(r)]
        newest = max(date.fromisoformat(as_of(r)) for r in scanner_rows)
        cutoff = newest - timedelta(days=120)
        recent = [dict(r, as_of=as_of(r), generated_at=generated, snapshot_id=snapshot_id, schema_version=SCHEMA_VERSION)
                  for r in scanner_rows if cutoff <= date.fromisoformat(as_of(r)) <= newest]
        pending[paths["history_recent"]] = encode_csv(list(dict.fromkeys(ac + VIEW_COLUMNS)), recent)
        pending[paths["latest_scanner"]] = encode_csv(lc, latest)
        if daily_input:
            # Daily history is append-only here. Same-day reruns retain previous
            # real observations; the report reader selects the latest run per day.
            history_columns = list(dict.fromkeys(hc + lc))
            compare = [c for c in history_columns if c not in VIEW_COLUMNS]
            available = Counter(tuple(r.get(c, "") for c in compare) for r in history)
            appended = []
            for row in latest:
                key = tuple(row.get(c, "") for c in compare)
                if available[key]:
                    available[key] -= 1
                else:
                    appended.append(row)
            if appended or history_columns != hc:
                pending[history_path] = encode_csv(history_columns, history + appended)
    elif latest:
        warnings.append("published_scanner_views_retained; attempt_id identifies the incomplete attempt")
    # Normal publication only accepts the canonical price schema. The exact
    # legacy schema is accepted exclusively inside refresh_price_backfill().
    if pc and pc != PRICE_COLUMNS:
        raise ValueError("price_backfill_schema_mismatch")
    market_path = root / MARKET
    market_raw = market_path.read_bytes() if market_path.exists() else None
    price_issues = {}
    if market_raw is not None:
        mc, market = parse_csv(market_raw)
        if not set(MARKET_COLUMNS).issubset(mc):
            raise ValueError("market_missing_required_columns")
        prices, price_issues = import_market_prices(prices, market)
        if price_issues:
            warnings.append("invalid_or_revised_price_rows_reported_in_price_coverage")
        if any(not r["retrieved_at"] for r in prices):
            warnings.append("price_retrieved_at_unknown_in_legacy_cache; not inferred from export time")
    if not pc or prices != existing("price_backfill")[1]:
        pending[paths["price_backfill"]] = encode_csv(PRICE_COLUMNS, prices)
    metadata = {"schema_version": SCHEMA_VERSION, "snapshot_id": snapshot_id, "attempt_id": attempt_id,
                "as_of": as_of(current[0]) if current else None, "generated_at": generated,
                "views_generated_at": view_generated, "latest_run_complete": complete,
                "last_complete_scan": as_of(latest[0]) if latest else None,
                "latest_run_error": errors or None,
                "source": {"file": source_relative, "sha256": hashlib.sha256(raw).hexdigest() if raw is not None else None},
                "validation": {"status": "ok" if complete else "incomplete", "errors": errors,
                               "warnings": warnings, "policy": asdict(policy), **diagnostics}}
    if daily_input:
        metadata["daily_run"] = daily_input.context
    fetch_state = read_price_fetch_state(root)
    fetch_details = fetch_state.get("symbols", {})
    for symbol, counts in price_issues.items():
        fetch_details.setdefault(symbol, {}).setdefault("validation_issues", {}).update(counts)
    metadata["price_coverage"] = price_coverage(
        [r["symbol"] for r in latest], prices, as_of=as_of(latest[0]) if latest else now.date().isoformat(),
        minimum_sessions=fetch_state.get("minimum_sessions_target", 300),
        fetch_state=fetch_details)
    for name, path in paths.items():
        data = pending.get(path, original[path])
        rows = parse_csv(data)[1] if data is not None else []
        dates = [as_of(r) for r in rows if as_of(r)]
        metadata[name] = {"path": path.relative_to(root).as_posix(),
                          "sha256": hashlib.sha256(data).hexdigest() if data is not None else None,
                          "row_count": len(rows), "symbol_count": len({r.get("symbol") for r in rows}),
                          "start_date": min(dates, default=None), "end_date": max(dates, default=None)}
    # Detect writers outside this module before publication. Metadata is always last.
    from scanner.reports.research_validation import validate_bundle
    blobs = {name: pending.get(path, original[path]) for name, path in paths.items()}
    validate_bundle(metadata, blobs, original[paths["history_analysis"]])
    for path, before in {**original, source_path: raw, history_path: history_raw, market_path: market_raw}.items():
        if (path.read_bytes() if path.exists() else None) != before:
            raise ValueError(f"Input changed during export: {path}")
    for path in [history_path, *paths.values()]:
        if path in pending:
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write(path, pending[path])
    atomic_write(meta_path, (json.dumps(metadata, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    return metadata


def import_market_prices(prices, market):
    # Explicit allowlist: neither provider nor scanner fields can contaminate
    # this price-only derived view. Preserve existing raw observations and carry
    # adjusted close as an explicit research-return field.
    raw_fields = [c for c in MARKET_COLUMNS if c != "retrieved_at"]
    incoming = [
        {
            **{c: row.get(c, "") for c in raw_fields},
            "source": "yahoo_ohlcv",
            "retrieved_at": row.get("retrieved_at", ""),
            "observation_type": "price_backfill",
        }
        for row in market
    ]
    return merge_prices(prices, incoming)


def read_price_fetch_state(root):
    path = root / "artifacts/market_data/price_fetch_state.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def refresh_price_backfill(root):
    """Refresh prices and coverage without re-running or rewriting any scanner view."""
    from scanner.reports.research_validation import NAMES, validate_bundle, validate_publication
    root = Path(root).resolve()
    output = root / OUTPUT
    lock = output / ".research_views.lock"
    with lock.open("x"):
        pass
    try:
        # This is the sole migration entry point allowed to read the exact legacy
        # price header. Final validation below is always canonical/strict.
        metadata = validate_publication(root, allow_legacy_price_schema=True)
        if not metadata["latest_run_complete"]:
            raise ValueError("price refresh requires a complete scanner snapshot")
        paths = {n: output / (n + ".csv") for n in NAMES}
        meta_path = output / "history_metadata.json"
        market_path = root / MARKET
        state_path = root / "artifacts/market_data/price_fetch_state.json"
        originals = {p: p.read_bytes() if p.exists() else None for p in [*paths.values(), meta_path, market_path, state_path]}
        if originals[market_path] is None:
            raise ValueError("market cache missing")
        columns, market = parse_csv(originals[market_path])
        if columns != MARKET_COLUMNS:
            raise ValueError("market_missing_required_columns")
        previous_columns, previous = parse_csv(originals[paths["price_backfill"]])
        if previous_columns not in (PRICE_COLUMNS, LEGACY_PRICE_COLUMNS):
            raise ValueError("price_backfill_schema_mismatch")
        prices, issues = import_market_prices(previous, market)
        latest = parse_csv(originals[paths["latest_scanner"]])[1]
        state = read_price_fetch_state(root)
        if state.get("snapshot_id") not in (None, metadata["snapshot_id"]):
            raise ValueError("price fetch belongs to a different scanner snapshot")
        details = state.get("symbols", {})
        for symbol, counts in issues.items():
            details.setdefault(symbol, {}).setdefault("validation_issues", {}).update(counts)
        metadata["price_coverage"] = price_coverage(
            [r["symbol"] for r in latest], prices, as_of=metadata["as_of"],
            minimum_sessions=state.get("minimum_sessions_target", 300), fetch_state=details)
        raw = encode_csv(PRICE_COLUMNS, prices)
        dates = [r["date"] for r in prices]
        metadata["price_backfill"] = {
            "path": "artifacts/research/price_backfill.csv", "sha256": hashlib.sha256(raw).hexdigest(),
            "row_count": len(prices), "symbol_count": len({r["symbol"] for r in prices}),
            "start_date": min(dates, default=None), "end_date": max(dates, default=None),
        }
        # Old daily output no longer represents the newly available price base.
        # Remove its publication marker until the next (required) daily step.
        metadata.pop("daily_research", None)
        blobs = {name: raw if name == "price_backfill" else originals[path] for name, path in paths.items()}
        validate_bundle(metadata, blobs)
        for path, before in originals.items():
            if (path.read_bytes() if path.exists() else None) != before:
                raise ValueError(f"Input changed during price publication: {path}")
        if raw != originals[paths["price_backfill"]]:
            atomic_write(paths["price_backfill"], raw)
        atomic_write(meta_path, (json.dumps(metadata, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
        return validate_publication(root)
    finally:
        lock.unlink(missing_ok=True)
