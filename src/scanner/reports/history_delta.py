from __future__ import annotations

"""History Delta (Score/Rank changes between latest two snapshots).

This module is explainability-only.
It reads existing CSV outputs and produces delta reports into artifacts/.
It must never influence scoring.

Inputs
------
- artifacts/watchlist/watchlist_full.csv (current run, canonical columns present)
- artifacts/snapshots/score_history.csv (append-only snapshot store)

Outputs
-------
- artifacts/reports/history_delta.json
- artifacts/reports/history_delta.csv

Notes
-----
We *upsert* today's snapshot (by date+symbol) into score_history.csv to keep the
pipeline deterministic per day (reruns don't duplicate rows).
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from scanner.data.io.paths import artifacts_dir, project_root
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 1


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def resolve_score_history_path() -> Path:
    a = artifacts_dir() / "snapshots" / "score_history.csv"
    if a.exists():
        return a
    # legacy fallback
    b = project_root() / "data" / "snapshots" / "score_history.csv"
    return b


def _first_nonempty(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        s = str(v).strip()
        if s:
            return v
    return None


def build_snapshot_from_watchlist(df_full: pd.DataFrame, date: str | None = None) -> pd.DataFrame:
    """Create a normalized daily snapshot frame from watchlist_full.csv."""
    if df_full is None or df_full.empty:
        return pd.DataFrame(columns=["date", "symbol", "name", "score"])

    # Prefer a stable market_date if present; else UTC today
    dt = date
    if not dt:
        if "market_date" in df_full.columns:
            md = df_full["market_date"].dropna().astype(str).str.strip()
            md = md[md != ""]
            if len(md) > 0:
                dt = str(md.iloc[0])[:10]
        dt = dt or _utc_today()

    def _col(name: str) -> pd.Series:
        if name not in df_full.columns:
            return pd.Series([pd.NA] * len(df_full), index=df_full.index)
        s = df_full[name]
        return s

    symbol = _col("symbol")
    ticker = _col("ticker")
    asset_id = _col("asset_id")
    ticker_display = _col("ticker_display")
    name = _col("name")
    score = pd.to_numeric(_col("score"), errors="coerce")
    confidence = pd.to_numeric(_col("confidence"), errors="coerce")
    rs3m = pd.to_numeric(_col("rs3m"), errors="coerce")
    trend200 = pd.to_numeric(_col("trend200"), errors="coerce")
    close = pd.to_numeric(_col("price"), errors="coerce")
    currency = _col("currency")
    sector = _col("sector")
    pillar_primary = _col("pillar_primary")
    cluster_official = _col("cluster_official")
    bucket_type = _col("bucket_type")

    # Normalize symbol key: prefer asset_id, then symbol, then ticker
    key = []
    for i in df_full.index:
        k = _first_nonempty(asset_id.loc[i], symbol.loc[i], ticker_display.loc[i], ticker.loc[i])
        key.append("" if k is None else str(k).strip())
    key_s = pd.Series(key, index=df_full.index)

    snap = pd.DataFrame(
        {
            "date": dt,
            "symbol": key_s,
            "name": name.astype(str).replace({"nan": ""}),
            "score": score,
            "confidence": confidence,
            "rs3m": rs3m,
            "trend200": trend200,
            "close": close,
            "currency": currency.astype(str).replace({"nan": ""}),
            "sector": sector.astype(str).replace({"nan": ""}),
            "pillar_primary": pillar_primary.astype(str).replace({"nan": ""}),
            "cluster_official": cluster_official.astype(str).replace({"nan": ""}),
            "bucket_type": bucket_type.astype(str).replace({"nan": ""}),
        }
    )

    snap["symbol"] = snap["symbol"].fillna("").astype(str).str.strip()
    snap = snap[snap["symbol"] != ""].copy()
    snap["name"] = snap["name"].fillna("").astype(str).str.strip()

    return snap


def upsert_daily_snapshot(score_history_path: Path, snapshot: pd.DataFrame) -> pd.DataFrame:
    """Upsert date+symbol into score_history.csv and return the combined DF."""
    score_history_path.parent.mkdir(parents=True, exist_ok=True)

    if snapshot is None or snapshot.empty:
        if score_history_path.exists():
            return pd.read_csv(score_history_path)
        return pd.DataFrame()

    dt = str(snapshot["date"].iloc[0])

    if score_history_path.exists():
        existing = pd.read_csv(score_history_path)
    else:
        existing = pd.DataFrame()

    if not existing.empty:
        # Drop rows for same date and symbols in snapshot
        existing["date"] = existing["date"].astype(str)
        snap_syms = set(snapshot["symbol"].astype(str).tolist())
        mask_drop = (existing["date"] == dt) & (existing["symbol"].astype(str).isin(snap_syms))
        existing = existing.loc[~mask_drop].copy()

    combined = pd.concat([existing, snapshot], ignore_index=True)

    # Stable sort: date asc then symbol asc
    if "date" in combined.columns and "symbol" in combined.columns:
        combined["date"] = combined["date"].astype(str)
        combined["symbol"] = combined["symbol"].astype(str)
        combined = combined.sort_values(["date", "symbol"], ascending=[True, True])

    to_csv_safely(combined, score_history_path, index=False)
    return combined


def _rank_by_score(df: pd.DataFrame) -> pd.Series:
    """Dense rank: 1 is best (highest score). NaN ranks last."""
    s = pd.to_numeric(df["score"], errors="coerce")
    # rank highest first; NaN -> bottom
    # Use method='min' so ties share best rank number
    return (-s).rank(method="min", na_option="bottom").astype("Int64")


def compute_history_delta(score_hist: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if score_hist is None or score_hist.empty:
        empty = pd.DataFrame(columns=["symbol", "name", "score_prev", "score_now", "score_delta", "rank_prev", "rank_now", "rank_delta", "status"])
        js = {
            "schema_version": SCHEMA_VERSION,
            "latest_date": None,
            "prev_date": None,
            "stats": {"total": 0, "new": 0, "dropped": 0, "changed": 0},
            "by_symbol": {},
            "movers_up": [],
            "movers_down": [],
            "new_symbols": [],
            "dropped_symbols": [],
        }
        return empty, js

    work = score_hist.copy()
    work["date"] = work["date"].astype(str)
    # Normalize symbol as str
    work["symbol"] = work["symbol"].astype(str)

    dates = sorted([d for d in work["date"].dropna().unique().tolist() if str(d).strip()])
    if len(dates) < 2:
        latest = dates[-1] if dates else None
        empty = pd.DataFrame(columns=["symbol", "name", "score_prev", "score_now", "score_delta", "rank_prev", "rank_now", "rank_delta", "status"])
        js = {
            "schema_version": SCHEMA_VERSION,
            "latest_date": latest,
            "prev_date": None,
            "stats": {"total": int(work["symbol"].nunique()), "new": 0, "dropped": 0, "changed": 0},
            "by_symbol": {},
            "movers_up": [],
            "movers_down": [],
            "new_symbols": [],
            "dropped_symbols": [],
        }
        return empty, js

    prev_date, latest_date = dates[-2], dates[-1]

    prev = work[work["date"] == prev_date].copy()
    now = work[work["date"] == latest_date].copy()

    # Rank within each snapshot
    prev["rank"] = _rank_by_score(prev)
    now["rank"] = _rank_by_score(now)

    # Reduce to last occurrence per symbol within date (in case of duplicates)
    prev = prev.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last")
    now = now.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last")

    prev = prev.set_index("symbol", drop=False)
    now = now.set_index("symbol", drop=False)

    all_syms = sorted(set(prev.index.tolist()) | set(now.index.tolist()))
    rows = []
    for sym in all_syms:
        p = prev.loc[sym] if sym in prev.index else None
        n = now.loc[sym] if sym in now.index else None

        name = ""
        if n is not None:
            name = str(n.get("name", "") or "")
        elif p is not None:
            name = str(p.get("name", "") or "")

        score_prev = float(p["score"]) if p is not None and pd.notna(p["score"]) else None
        score_now = float(n["score"]) if n is not None and pd.notna(n["score"]) else None

        rank_prev = int(p["rank"]) if p is not None and pd.notna(p["rank"]) else None
        rank_now = int(n["rank"]) if n is not None and pd.notna(n["rank"]) else None

        if p is None and n is not None:
            status = "new"
        elif p is not None and n is None:
            status = "dropped"
        else:
            status = "ok"

        score_delta = None
        if score_prev is not None and score_now is not None:
            score_delta = score_now - score_prev

        rank_delta = None
        if rank_prev is not None and rank_now is not None:
            # positive means moved UP (e.g., 10 -> 5 => +5)
            rank_delta = rank_prev - rank_now

        rows.append(
            {
                "symbol": sym,
                "name": name,
                "score_prev": score_prev,
                "score_now": score_now,
                "score_delta": score_delta,
                "rank_prev": rank_prev,
                "rank_now": rank_now,
                "rank_delta": rank_delta,
                "status": status,
            }
        )

    delta = pd.DataFrame(rows)

    # Movers: only those present in both snapshots
    both = delta[delta["status"] == "ok"].copy()
    both["rank_delta_num"] = pd.to_numeric(both["rank_delta"], errors="coerce").fillna(0)
    both["score_delta_num"] = pd.to_numeric(both["score_delta"], errors="coerce").fillna(0)

    movers_up = both.sort_values(["rank_delta_num", "score_delta_num"], ascending=[False, False]).head(10)
    movers_down = both.sort_values(["rank_delta_num", "score_delta_num"], ascending=[True, True]).head(10)

    def _pack(df: pd.DataFrame) -> list[dict[str, Any]]:
        out = []
        for _, r in df.iterrows():
            out.append(
                {
                    "symbol": r["symbol"],
                    "name": r.get("name", ""),
                    "rank_delta": None if pd.isna(r.get("rank_delta")) else int(r.get("rank_delta")),
                    "score_delta": None if pd.isna(r.get("score_delta")) else float(r.get("score_delta")),
                    "score_now": None if pd.isna(r.get("score_now")) else float(r.get("score_now")),
                    "rank_now": None if pd.isna(r.get("rank_now")) else int(r.get("rank_now")),
                }
            )
        return out

    new_syms = delta[delta["status"] == "new"]["symbol"].tolist()
    dropped_syms = delta[delta["status"] == "dropped"]["symbol"].tolist()


    # Full per-symbol map for UI (used for per-row dScore 1D display).
    # Key matches snapshot "symbol" key (asset_id > symbol > ticker_display > ticker).
    by_symbol: dict[str, Any] = {}
    for _, rr in delta.iterrows():
        sym = str(rr.get("symbol", "")).strip()
        if not sym:
            continue

        def _as_float(v: Any) -> float | None:
            try:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                if pd.isna(v):
                    return None
                return float(v)
            except Exception:
                return None

        def _as_int(v: Any) -> int | None:
            try:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                if pd.isna(v):
                    return None
                return int(v)
            except Exception:
                return None

        by_symbol[sym] = {
            "status": str(rr.get("status", "")).strip(),
            "score_prev": _as_float(rr.get("score_prev")),
            "score_now": _as_float(rr.get("score_now")),
            "score_delta": _as_float(rr.get("score_delta")),
            "rank_prev": _as_int(rr.get("rank_prev")),
            "rank_now": _as_int(rr.get("rank_now")),
            "rank_delta": _as_int(rr.get("rank_delta")),
        }

    js = {
        "schema_version": SCHEMA_VERSION,
        "latest_date": latest_date,
        "prev_date": prev_date,
        "stats": {
            "total": int(len(delta)),
            "new": int((delta["status"] == "new").sum()),
            "dropped": int((delta["status"] == "dropped").sum()),
            "changed": int(((delta["status"] == "ok") & (pd.to_numeric(delta["rank_delta"], errors="coerce").fillna(0) != 0)).sum()),
        },
        "by_symbol": by_symbol,
        "movers_up": _pack(movers_up),
        "movers_down": _pack(movers_down),
        "new_symbols": new_syms[:30],
        "dropped_symbols": dropped_syms[:30],
    }

    # Sort delta for CSV: best ranks first, then changes
    delta["_rank_now_sort"] = pd.to_numeric(delta["rank_now"], errors="coerce")
    delta = delta.sort_values(["status", "_rank_now_sort"], ascending=[True, True])
    delta = delta.drop(columns=["_rank_now_sort"])

    return delta, js


def write_history_delta_outputs(delta_df: pd.DataFrame, payload: dict[str, Any]) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "history_delta.json"
    p_csv = out_dir / "history_delta.csv"

    p_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    to_csv_safely(delta_df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
