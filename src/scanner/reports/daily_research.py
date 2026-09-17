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
