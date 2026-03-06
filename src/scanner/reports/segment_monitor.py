from __future__ import annotations

"""Segment Monitor (pillar/cluster/bucket tracking + changes between snapshots).

Explainability-only. Reads existing CSV outputs and produces reports under artifacts/.
Does not influence scoring.

Inputs
------
- artifacts/watchlist/watchlist_full.csv
- artifacts/snapshots/segment_history.csv (append-only; created if missing)

Outputs
-------
- artifacts/reports/segment_monitor.json
- artifacts/reports/segment_monitor.csv
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from scanner.data.io.paths import artifacts_dir, project_root
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 1


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def resolve_segment_history_path() -> Path:
    a = artifacts_dir() / "snapshots" / "segment_history.csv"
    if a.exists():
        return a
    b = project_root() / "data" / "snapshots" / "segment_history.csv"
    return b


def build_segment_snapshot(df_full: pd.DataFrame, date: str | None = None) -> pd.DataFrame:
    if df_full is None or df_full.empty:
        return pd.DataFrame(columns=["date", "symbol", "pillar_primary", "cluster_official", "bucket_type"])

    dt = date
    if not dt and "market_date" in df_full.columns:
        md = df_full["market_date"].dropna().astype(str).str.strip()
        md = md[md != ""]
        if len(md) > 0:
            dt = str(md.iloc[0])[:10]
    dt = dt or _utc_today()

    def _col(name: str) -> pd.Series:
        if name not in df_full.columns:
            return pd.Series([pd.NA] * len(df_full), index=df_full.index)
        return df_full[name]

    sym = _col("asset_id")
    if sym.isna().all() and "symbol" in df_full.columns:
        sym = _col("symbol")
    if sym.isna().all() and "ticker" in df_full.columns:
        sym = _col("ticker")

    snap = pd.DataFrame(
        {
            "date": dt,
            "symbol": sym.astype(str),
            "pillar_primary": _col("pillar_primary").astype(str),
            "cluster_official": _col("cluster_official").astype(str),
            "bucket_type": _col("bucket_type").astype(str),
        }
    )

    for c in ["symbol", "pillar_primary", "cluster_official", "bucket_type"]:
        snap[c] = snap[c].replace({"nan": ""}).fillna("").astype(str).str.strip()

    snap = snap[snap["symbol"] != ""].copy()
    return snap


def upsert_segment_snapshot(path: Path, snapshot: pd.DataFrame) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    if snapshot is None or snapshot.empty:
        if path.exists():
            return pd.read_csv(path)
        return pd.DataFrame()

    dt = str(snapshot["date"].iloc[0])

    if path.exists():
        existing = pd.read_csv(path)
    else:
        existing = pd.DataFrame()

    if not existing.empty:
        existing["date"] = existing["date"].astype(str)
        syms = set(snapshot["symbol"].astype(str).tolist())
        drop = (existing["date"] == dt) & (existing["symbol"].astype(str).isin(syms))
        existing = existing.loc[~drop].copy()

    combined = pd.concat([existing, snapshot], ignore_index=True)
    if "date" in combined.columns and "symbol" in combined.columns:
        combined["date"] = combined["date"].astype(str)
        combined["symbol"] = combined["symbol"].astype(str)
        combined = combined.sort_values(["date", "symbol"], ascending=[True, True])

    to_csv_safely(combined, path, index=False)
    return combined


def compute_segment_monitor(seg_hist: pd.DataFrame, current_snapshot: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    # baseline current snapshot
    cur = current_snapshot.copy() if current_snapshot is not None else pd.DataFrame()
    if cur.empty:
        empty = pd.DataFrame(columns=["symbol", "pillar_primary", "cluster_official", "bucket_type", "changed"])
        payload = {
            "schema_version": SCHEMA_VERSION,
            "latest_date": None,
            "prev_date": None,
            "stats": {"total": 0, "changed": 0},
            "pillar_dist": [],
            "cluster_dist": [],
            "bucket_dist": [],
            "changes": [],
        }
        return empty, payload

    latest_date = str(cur["date"].iloc[0])

    # distributions
    def _dist(col: str, limit: int = 30) -> list[dict[str, Any]]:
        s = cur[col].fillna("").astype(str).replace({"nan": ""}).str.strip()
        s = s.replace({"": "∅"})
        vc = s.value_counts(dropna=False).head(limit)
        out = [{"key": k, "count": int(v)} for k, v in vc.items()]
        return out

    pillar_dist = _dist("pillar_primary")
    cluster_dist = _dist("cluster_official")
    bucket_dist = _dist("bucket_type")

    # changes vs previous snapshot (if any)
    prev_date = None
    changes = []
    changed_rows = pd.DataFrame(columns=["symbol", "pillar_primary", "cluster_official", "bucket_type", "changed"])

    if seg_hist is not None and not seg_hist.empty:
        work = seg_hist.copy()
        work["date"] = work["date"].astype(str)
        dates = sorted([d for d in work["date"].dropna().unique().tolist() if str(d).strip()])
        if len(dates) >= 2:
            prev_date = dates[-2]
            prev = work[work["date"] == prev_date].copy()
            prev = prev.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last").set_index("symbol", drop=False)
            now = cur.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last").set_index("symbol", drop=False)

            syms = sorted(set(prev.index.tolist()) & set(now.index.tolist()))
            for sym in syms:
                p = prev.loc[sym]
                n = now.loc[sym]
                changed = []
                for c in ["pillar_primary", "cluster_official", "bucket_type"]:
                    pv = str(p.get(c, "") or "").strip()
                    nv = str(n.get(c, "") or "").strip()
                    if pv != nv:
                        changed.append({"field": c, "from": pv, "to": nv})
                if changed:
                    changes.append({"symbol": sym, "changes": changed})

            # build csv frame
            rows = []
            for _, r in now.reset_index(drop=True).iterrows():
                sym = r["symbol"]
                changed_flag = any(x["symbol"] == sym for x in changes)
                rows.append(
                    {
                        "symbol": sym,
                        "pillar_primary": r.get("pillar_primary", ""),
                        "cluster_official": r.get("cluster_official", ""),
                        "bucket_type": r.get("bucket_type", ""),
                        "changed": bool(changed_flag),
                    }
                )
            changed_rows = pd.DataFrame(rows)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "latest_date": latest_date,
        "prev_date": prev_date,
        "stats": {"total": int(len(cur)), "changed": int(len(changes))},
        "pillar_dist": pillar_dist,
        "cluster_dist": cluster_dist,
        "bucket_dist": bucket_dist,
        "changes": changes[:50],
    }
    return changed_rows, payload


def write_segment_monitor_outputs(df: pd.DataFrame, payload: dict[str, Any]) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "segment_monitor.json"
    p_csv = out_dir / "segment_monitor.csv"

    p_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    to_csv_safely(df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
