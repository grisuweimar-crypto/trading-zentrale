"""Bridge the freshly completed daily watchlist to the research publisher.

Does not call market APIs or change the scanner. The run receipt proves that
the watchlist was written after the scanner step began, including same-day reruns.
"""
from dataclasses import dataclass, replace
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

from scanner.reports.research_views import (
    ValidationPolicy, atomic_write, build_views, parse_csv,
)
from scanner.reports.research_validation import validate_publication

WATCHLIST = "artifacts/watchlist/watchlist_full.csv"
ALIASES = {
    "date": ("market_date", "MarketDate"),
    "symbol": ("asset_id", "symbol", "ticker_display", "ticker"),
    "name": ("name", "Name"), "score": ("score", "Score"),
    "opportunity": ("opportunity", "OpportunityScore"),
    "risk": ("risk", "RiskScore"), "confidence": ("confidence", "ConfidenceScore"),
    "confidence_label": ("confidence_label", "ConfidenceLabel"),
    "rs3m": ("rs3m", "RS3M"), "trend200": ("trend200", "Trend200"),
    "cycle": ("cycle", "Zyklus %"), "r_code": ("r_code",),
    "close": ("price", "close"), "currency": ("currency", "Currency"),
    "sector": ("sector", "Sector"), "pillar_primary": ("pillar_primary",),
    "cluster_official": ("cluster_official",), "bucket_type": ("bucket_type",),
    "volatility": ("volatility", "Volatility"), "drawdown": ("max_drawdown", "MaxDrawdown"),
    "roe": ("roe", "ROE %"), "growth": ("growth", "Growth %"),
    "margin": ("margin", "Margin %"), "debt_ratio": ("debt_ratio", "Debt/Equity"),
    "market_regime_stock": ("regime_stock", "MarketRegimeStock"),
    "market_regime_crypto": ("regime_crypto", "MarketRegimeCrypto"),
    "market_trend200_stock": ("trend200_stock", "MarketTrend200Stock"),
    "market_trend200_crypto": ("trend200_crypto", "MarketTrend200Crypto"),
    "universe_version": ("universe_version",), "config_version": ("config_version",),
}
REQUIRED = ("date", "symbol", "name", "score", "opportunity", "risk", "confidence", "rs3m", "trend200", "cycle")


def fingerprint(path):
    if not path.exists():
        return None
    info = path.stat()
    return [info.st_mtime_ns, info.st_size]


def begin_daily(root: Path, receipt_path: Path, *, run_id=None):
    receipt = {"run_id": run_id or str(uuid4()), "started_at": datetime.now(timezone.utc).isoformat(),
               "watchlist_before": fingerprint(root / WATCHLIST)}
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(receipt_path, (json.dumps(receipt) + "\n").encode("utf-8"))
    return receipt


@dataclass
class DailyInput:
    source: str
    source_sha256: str | None
    columns: list
    rows: list
    errors: list
    context: dict
    watchlist_raw: bytes

    def enrich(self, rows):
        # Reuse the project's established R-code derivation for this validated
        # current scan only. All other values remain verbatim source strings.
        import pandas as pd
        from scanner.reports.history_delta import _r_code_series, SCORING_VERSION
        frame = pd.read_csv(io.BytesIO(self.watchlist_raw), dtype=str, keep_default_na=False)
        codes = _r_code_series(frame).tolist()
        return [dict(row, r_code=row.get("r_code") or codes[i], scoring_version=SCORING_VERSION)
                for i, row in enumerate(rows)]


def generate_daily(root: Path, receipt_path: Path, *, scanner_status="success", policy=None, now=None):
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    errors = []
    if scanner_status != "success":
        errors.append("scan_aborted: scanner step " + scanner_status)
    path = root / WATCHLIST
    current_fingerprint = fingerprint(path)
    if current_fingerprint is None or current_fingerprint == receipt["watchlist_before"]:
        errors.append("stale_watchlist: not rewritten during this daily run")
    elif current_fingerprint[0] / 1e9 < int(datetime.fromisoformat(receipt["started_at"]).timestamp()):
        errors.append("stale_watchlist: older than run receipt")
    raw = path.read_bytes() if path.exists() else b""
    source_columns, source_rows = [], []
    try:
        source_columns, source_rows = parse_csv(raw)
    except (ValueError, UnicodeError, csv.Error) as exc:
        errors.append("invalid_watchlist: " + str(exc))
    columns = [c for c, aliases in ALIASES.items() if any(a in source_columns for a in aliases)]
    columns = list(dict.fromkeys(columns + ["r_code", "run_id", "scoring_version"]))
    rows = []
    for original in source_rows:
        row = {c: next((original[a] for a in ALIASES[c] if original.get(a, "").strip()), "")
               for c in columns if c in ALIASES}
        row["run_id"] = receipt["run_id"]
        rows.append(row)
        if original.get("ScoreError", "").strip():
            errors.append("scanner_error_rows")
    policy = replace(policy or ValidationPolicy(), required_columns=REQUIRED)
    daily = DailyInput(WATCHLIST, hashlib.sha256(raw).hexdigest() if raw else None,
                       columns, rows, sorted(set(errors)),
                       {"run_id": receipt["run_id"], "started_at": receipt["started_at"],
                        "scanner_status": scanner_status, "watchlist_rewritten": current_fingerprint != receipt["watchlist_before"]}, raw)
    metadata = build_views(root, now=now, policy=policy, daily_input=daily)
    validate_publication(root)
    return metadata


def _research_float(value):
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _daily_current(row):
    fields = (
        "score", "rank", "rank_percentile", "r_code", "rs3m", "trend200",
        "cycle", "confidence", "confidence_label", "close", "currency",
        "sector", "cluster", "cluster_official", "name",
    )
    result = {}
    for field in fields:
        value = row.get(field) or (row.get("cluster_official") if field == "cluster" else None)
        result[field] = value if field in {"r_code", "confidence_label", "currency", "sector", "cluster", "cluster_official", "name"} else _research_float(value)
    return result


def _daily_history_summary(symbol, current_row, history_rows):
    rows = [row for row in history_rows if row.get("symbol") == symbol]
    rows.sort(key=lambda row: row.get("date", ""))
    current_date = _parse_date(current_row.get("date") or current_row.get("as_of"))
    previous_rows = [row for row in rows if _parse_date(row.get("date")) and (current_date is None or _parse_date(row.get("date")) < current_date)]
    previous = previous_rows[-1] if previous_rows else None
    current_score = _research_float(current_row.get("score"))
    previous_score = _research_float(previous.get("score")) if previous else None
    older = {}
    for horizon in (5, 10, 20, 40):
        for candidate in reversed(previous_rows):
            candidate_date = _parse_date(candidate.get("date"))
            if candidate_date and _business_days_between(candidate_date, current_date) >= horizon:
                older[horizon] = candidate
                break
    def delta(field, horizon):
        current = _research_float(current_row.get(field))
        historic = _research_float(older.get(horizon, {}).get(field)) if older.get(horizon) else None
        return current - historic if current is not None and historic is not None else None
    return {
        "score_delta_1d": current_score - previous_score if current_score is not None and previous_score is not None else None,
        "score_delta_5d": delta("score", 5),
        "rank_delta_1d": delta("rank", 1) if previous else None,
        "rank_delta_5d": delta("rank", 5),
        "rank_percentile_delta_5d": delta("rank_percentile", 5),
        "rs3m_delta_5d": delta("rs3m", 5),
        "trend200_delta_5d": delta("trend200", 5),
        "r_code_previous": previous.get("r_code") if previous else None,
        "r_code_days_current_state": sum(1 for row in reversed(previous_rows) if row.get("r_code") == current_row.get("r_code")),
    }


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def _business_days_between(start, end):
    if start is None or end is None or end <= start:
        return 0
    return sum(1 for day in ((start + __import__("datetime").timedelta(days=offset)) for offset in range(1, (end - start).days)) if day.weekday() < 5)


def _forward_return(rows, index, horizon):
    current_date = _parse_date(rows[index].get("date"))
    current_close = _research_float(rows[index].get("close"))
    if current_date is None or current_close in (None, 0):
        return None
    for later in rows[index + 1:]:
        later_date = _parse_date(later.get("date"))
        later_close = _research_float(later.get("close"))
        if later_date and _business_days_between(current_date, later_date) >= horizon:
            return (later_close / current_close) - 1 if later_close not in (None, 0) else None
    return None


def _historical_match_summary(symbol, current_row, history_rows):
    rows = sorted((row for row in history_rows if row.get("symbol") == symbol and row.get("date") != current_row.get("date")), key=lambda row: row.get("date", ""))
    current_bucket = int((_research_float(current_row.get("rank_percentile")) or 0) * 10)
    candidates = [row for row in rows if int((_research_float(row.get("rank_percentile")) or -1) * 10) == current_bucket]
    kept = []
    last_date = None
    for row in candidates:
        row_date = _parse_date(row.get("date"))
        if last_date is None or _business_days_between(last_date, row_date) > 5:
            kept.append(row)
            last_date = row_date
    values = {horizon: [] for horizon in (5, 10, 20, 40)}
    for row in kept:
        index = rows.index(row)
        for horizon in values:
            value = _forward_return(rows, index, horizon)
            if value is not None:
                values[horizon].append(value)
    def summary(values_for_horizon):
        ordered = sorted(values_for_horizon)
        return {
            "N": len(ordered),
            "median_return": ordered[len(ordered) // 2] if ordered else None,
            "positive_count": sum(value > 0 for value in ordered),
            "positive_rate": (sum(value > 0 for value in ordered) / len(ordered)) if ordered else None,
        }
    return {
        "filter_id": "level_3" if kept else "none",
        "filter_description": "same rank percentile bucket; five trading-day cooldown" if kept else "no comparable historical rows",
        "cooldown_trading_days": 5,
        "N": len(kept),
        **{"forward_" + str(horizon) + "t": summary(values[horizon]) for horizon in values},
    }


def _classify_symbol(row):
    percentile = _research_float(row.get("rank_percentile"))
    rs3m = _research_float(row.get("rs3m"))
    trend = _research_float(row.get("trend200"))
    sector = str(row.get("sector") or row.get("cluster_official") or "").lower()
    return {
        "top_10_percent": percentile is not None and percentile <= 0.10,
        "top_20_percent": percentile is not None and percentile <= 0.20,
        "bottom_20_percent": percentile is not None and percentile >= 0.80,
        "bottom_10_percent": percentile is not None and percentile >= 0.90,
        "pullback_candidate": bool(percentile is not None and percentile <= 0.10 and rs3m is not None and rs3m < 0 and trend is not None and trend > 0),
        "dead_cat_warning": bool(rs3m is not None and rs3m > 0 and trend is not None and trend < 0),
        "overextension_warning": bool(rs3m is not None and rs3m >= 0.15),
        "mining_override_warning": bool(("mining" in sector or "gold" in sector) and percentile is not None and percentile <= 0.10 and rs3m is not None and rs3m < 0),
    }


def validate_daily_research(root: Path | str, payload=None):
    root = Path(root)
    research = root / "artifacts" / "research"
    metadata_path = research / "history_metadata.json"
    latest_path = research / "latest_scanner.csv"
    daily_path = research / "daily_research.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if payload is None:
        payload = json.loads(daily_path.read_text(encoding="utf-8"))
    with latest_path.open("r", encoding="utf-8", newline="") as handle:
        latest_rows = list(csv.DictReader(handle))
    with (research / "history_recent.csv").open("r", encoding="utf-8", newline="") as handle:
        recent_rows = list(csv.DictReader(handle))
    expected_symbols = {row.get("symbol", "").strip() for row in latest_rows if row.get("symbol", "").strip()}
    actual_symbols = set(payload.get("symbols", {}))
    if payload.get("snapshot_id") != metadata.get("snapshot_id"):
        raise ValueError("daily_research snapshot_id mismatch")
    if payload.get("as_of") != metadata.get("as_of"):
        raise ValueError("daily_research as_of mismatch")
    if actual_symbols != expected_symbols:
        raise ValueError("daily_research symbol set mismatch")
    if len(latest_rows) != len(expected_symbols) or payload.get("universe_size") != len(expected_symbols):
        raise ValueError("daily_research symbol count mismatch")
    for row in latest_rows:
        if row.get("snapshot_id") and row.get("snapshot_id") != metadata.get("snapshot_id"):
            raise ValueError("latest_scanner snapshot_id mismatch")
        if row.get("as_of") and row.get("as_of") != metadata.get("as_of"):
            raise ValueError("latest_scanner as_of mismatch")
    for row in recent_rows:
        if row.get("snapshot_id") and row.get("snapshot_id") != metadata.get("snapshot_id"):
            raise ValueError("history_recent snapshot_id mismatch")
    expected_hash = hashlib.sha256(daily_path.read_bytes()).hexdigest()
    if metadata.get("daily_research", {}).get("sha256") != expected_hash:
        raise ValueError("daily_research hash mismatch")
    return payload


def generate_daily_research(root: Path | str):
    """Build the compact daily view from the current, published research views."""
    root = Path(root)
    research = root / "artifacts" / "research"
    metadata_path = research / "history_metadata.json"
    latest_path = research / "latest_scanner.csv"
    history_path = research / "history_recent.csv"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    with latest_path.open("r", encoding="utf-8", newline="") as handle:
        latest_rows = list(csv.DictReader(handle))
    with history_path.open("r", encoding="utf-8", newline="") as handle:
        history_rows = list(csv.DictReader(handle))
    snapshot_id = metadata.get("snapshot_id")
    as_of = metadata.get("as_of")
    if not snapshot_id:
        raise ValueError("research metadata missing snapshot_id")
    if not as_of and any(row.get("as_of") or row.get("date") for row in latest_rows):
        raise ValueError("research metadata missing as_of")
    symbols = {}
    for row in latest_rows:
        symbol = row.get("symbol", "").strip()
        if not symbol:
            raise ValueError("latest_scanner contains an empty symbol")
        symbol_history = sorted((item for item in history_rows if item.get("symbol") == symbol), key=lambda item: item.get("date", ""))
        current_code = row.get("r_code") or ""
        code_counts = {code: sum(item.get("r_code") == code for item in symbol_history) for code in ("R1", "R2", "R3", "R4", "R5")}
        consecutive_code = 0
        for item in reversed(symbol_history):
            if item.get("r_code") == current_code:
                consecutive_code += 1
            else:
                break
        negative_rs3m = 0
        for item in reversed(symbol_history):
            if (_research_float(item.get("rs3m")) or 0) < 0:
                negative_rs3m += 1
            else:
                break
        negative_trend = 0
        for item in reversed(symbol_history):
            if (_research_float(item.get("trend200")) or 0) < 0:
                negative_trend += 1
            else:
                break
        symbols[symbol] = {
            "current": _daily_current(row),
            "dynamics": _daily_history_summary(symbol, row, history_rows),
            "persistence": {
                "days_r1": code_counts["R1"], "days_r2": code_counts["R2"],
                "days_r3": code_counts["R3"], "days_r4": code_counts["R4"],
                "days_r5": code_counts["R5"],
                "consecutive_days_current_r_code": consecutive_code,
                "consecutive_days_rs3m_negative": negative_rs3m,
                "consecutive_days_trend200_negative": negative_trend,
            },
            "classification": _classify_symbol(row),
            "historical_matches": _historical_match_summary(symbol, row, history_rows),
        }
    payload = {
        "schema_version": "daily_research_v1",
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_snapshot_id": snapshot_id,
        "universe_size": len(symbols),
        "symbols": symbols,
    }
    output_path = research / "daily_research.json"
    output_bytes = (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    atomic_write(output_path, output_bytes)
    metadata["daily_research"] = {
        "path": "artifacts/research/daily_research.json",
        "sha256": hashlib.sha256(output_bytes).hexdigest(),
        "symbol_count": len(symbols),
        "snapshot_id": snapshot_id,
        "as_of": as_of,
    }
    atomic_write(metadata_path, (json.dumps(metadata, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    validate_daily_research(root, payload)
    return payload
