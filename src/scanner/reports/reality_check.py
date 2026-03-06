from __future__ import annotations

"""Reality Check (data integrity / mapping sanity checks).

Explainability-only. Never influences scoring.

Inputs
------
- artifacts/watchlist/watchlist_full.csv

Outputs
-------
- artifacts/reports/reality_check.json
- artifacts/reports/reality_check.csv

Semantics
---------
We produce *signals* about data quality that help you trust/triage the scanner:
- missing identifiers (yahoo_symbol / isin / symbol)
- missing taxonomy (sector / cluster_official / pillar_primary)
- broken scoring rows (score_status / score is NaN)
- duplicates (asset_id / symbol)
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 1


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x).strip()


def build_reality_check(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if df is None or df.empty:
        empty = pd.DataFrame(columns=["symbol", "name", "severity", "reality_score", "problems"])
        payload = {
            "schema_version": SCHEMA_VERSION,
            "date": _utc_today(),
            "stats": {"total": 0, "ok": 0, "warn": 0, "error": 0},
            "top_issues": [],
        }
        return empty, payload

    work = df.copy()

    def col(name: str) -> pd.Series:
        return work[name] if name in work.columns else pd.Series([pd.NA] * len(work), index=work.index)

    symbol = col("asset_id")
    if symbol.isna().all():
        symbol = col("symbol")
    if symbol.isna().all():
        symbol = col("ticker")

    name = col("name")
    yahoo = col("yahoo_symbol")
    isin = col("isin")
    sector = col("sector")
    cluster = col("cluster_official")
    pillar = col("pillar_primary")
    bucket = col("bucket_type")
    score = pd.to_numeric(col("score"), errors="coerce")
    score_status = col("score_status")

    # duplicates
    dup_key = symbol.fillna("").astype(str).str.strip()
    dup_mask = dup_key.duplicated(keep=False) & (dup_key != "")

    rows = []
    for i in work.index:
        sym = _s(symbol.loc[i])
        nm = _s(name.loc[i])
        problems = []

        if not sym:
            problems.append("missing: symbol")
        if not _s(yahoo.loc[i]):
            problems.append("missing: yahoo_symbol")
        if not _s(isin.loc[i]):
            problems.append("missing: isin")
        if not _s(sector.loc[i]):
            problems.append("missing: sector")
        if not _s(cluster.loc[i]):
            problems.append("missing: cluster_official")
        if not _s(pillar.loc[i]):
            problems.append("missing: pillar_primary")

        st = _s(score_status.loc[i]).lower()
        sc = score.loc[i]
        if pd.isna(sc):
            problems.append("invalid: score NaN")
        if st in ("broken", "na", "error", "fail"):
            problems.append(f"score_status: {st}")

        if bool(dup_mask.loc[i]):
            problems.append("duplicate: symbol")

        # severity
        sev = "ok"
        if any(p.startswith("invalid: score") or p.startswith("score_status") for p in problems):
            sev = "error"
        elif any(p.startswith("missing: yahoo_symbol") or p.startswith("missing: symbol") for p in problems):
            sev = "error"
        elif problems:
            sev = "warn"

        # reality score (0..1)
        score_val = 1.0
        for p in problems:
            if p.startswith("invalid: score") or p.startswith("score_status"):
                score_val -= 0.35
            elif p.startswith("missing: yahoo_symbol") or p.startswith("missing: symbol"):
                score_val -= 0.25
            elif p.startswith("duplicate"):
                score_val -= 0.20
            else:
                score_val -= 0.10
        score_val = max(0.0, min(1.0, score_val))

        rows.append(
            {
                "symbol": sym,
                "name": nm,
                "severity": sev,
                "reality_score": round(score_val, 3),
                "score": None if pd.isna(sc) else float(sc),
                "bucket_type": _s(bucket.loc[i]),
                "pillar_primary": _s(pillar.loc[i]),
                "cluster_official": _s(cluster.loc[i]),
                "problems": "; ".join(problems),
            }
        )

    out = pd.DataFrame(rows)

    # stats
    stats = {
        "total": int(len(out)),
        "ok": int((out["severity"] == "ok").sum()),
        "warn": int((out["severity"] == "warn").sum()),
        "error": int((out["severity"] == "error").sum()),
    }

    # top issues: errors then low score
    top = out[out["severity"] != "ok"].copy()
    top = top.sort_values(["severity", "reality_score"], ascending=[True, True])  # error first? 'error' < 'warn' false; enforce
    # enforce severity order manually
    sev_rank = {"error": 0, "warn": 1, "ok": 2}
    top["_sev_rank"] = top["severity"].map(sev_rank).fillna(9).astype(int)
    top = top.sort_values(["_sev_rank", "reality_score"], ascending=[True, True]).drop(columns=["_sev_rank"])
    top_issues = []
    for _, r in top.head(30).iterrows():
        top_issues.append(
            {
                "symbol": r["symbol"],
                "name": r.get("name", ""),
                "severity": r["severity"],
                "reality_score": float(r["reality_score"]),
                "problems": str(r.get("problems", "") or ""),
            }
        )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date": _utc_today(),
        "stats": stats,
        "top_issues": top_issues,
    }
    return out, payload


def write_reality_check_outputs(df: pd.DataFrame, payload: dict[str, Any]) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "reality_check.json"
    p_csv = out_dir / "reality_check.csv"

    p_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    to_csv_safely(df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
