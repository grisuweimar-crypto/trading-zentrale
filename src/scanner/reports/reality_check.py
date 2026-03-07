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
        empty = pd.DataFrame(
            columns=[
                "symbol",
                "name",
                "severity",
                "signal",
                "verdict",
                "reality_score",
                "intern",
                "offiziell",
                "scanner",
                "market",
                "score",
                "bucket_type",
                "pillar_primary",
                "cluster_official",
                "problems",
            ]
        )
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
    industry = col("industry")
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

        # UI-facing columns for the Reality table.
        intern = _s(pillar.loc[i]) or _s(bucket.loc[i]) or "—"
        official = _s(industry.loc[i]) or _s(sector.loc[i]) or "—"
        scanner_view = _s(cluster.loc[i]) or _s(pillar.loc[i]) or "—"
        market_view = _s(sector.loc[i]) or _s(industry.loc[i]) or "—"

        if sev == "error":
            signal = "Kontra"
            verdict = "contra"
        elif sev == "warn":
            signal = "Warn"
            verdict = "warn"
        else:
            signal = "OK"
            verdict = "ok"

        rows.append(
            {
                "symbol": sym,
                "name": nm,
                "severity": sev,
                "signal": signal,
                "verdict": verdict,
                "reality_score": round(score_val, 3),
                "intern": intern,
                "offiziell": official,
                "scanner": scanner_view,
                "market": market_view,
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

    # Top comparison rows for UI table (intern vs offiziell), based on scanner scores.
    # This keeps the panel useful even before taxonomy is fully cleaned.
    cmp = pd.DataFrame(
        {
            "intern": out["intern"].astype(str),
            "offiziell": out["offiziell"].astype(str),
            "score": pd.to_numeric(out["score"], errors="coerce"),
            "severity": out["severity"].astype(str),
        }
    )
    # market proxy: prefer trend200 (roughly -1..+1), fallback rs3m, then normalized perf_pct.
    trend200 = pd.to_numeric(col("trend200"), errors="coerce") if "trend200" in work.columns else pd.Series(pd.NA, index=work.index)
    rs3m = pd.to_numeric(col("rs3m"), errors="coerce") if "rs3m" in work.columns else pd.Series(pd.NA, index=work.index)
    perf_pct = pd.to_numeric(col("perf_pct"), errors="coerce") if "perf_pct" in work.columns else pd.Series(pd.NA, index=work.index)
    market_proxy = trend200.copy()
    market_proxy = market_proxy.where(market_proxy.notna(), rs3m)
    market_proxy = market_proxy.where(market_proxy.notna(), perf_pct / 100.0)
    cmp["market_proxy"] = market_proxy
    cmp = cmp[
        cmp["intern"].str.strip().ne("")
        & cmp["intern"].str.strip().ne("—")
        & cmp["intern"].str.lower().str.strip().ne("none")
        & cmp["offiziell"].str.strip().ne("")
        & cmp["offiziell"].str.strip().ne("—")
    ].copy()

    if not cmp.empty:
        grouped = (
            cmp.groupby(["intern", "offiziell"], dropna=False)
            .agg(
                n=("score", "size"),
                scanner_mean=("score", "mean"),
                market_mean=("market_proxy", "mean"),
                warn_share=("severity", lambda s: float((s != "ok").mean()) if len(s) else 0.0),
            )
            .reset_index()
        )
    else:
        grouped = pd.DataFrame(columns=["intern", "offiziell", "n", "scanner_mean", "market_mean", "warn_share"])

    top_issues = []
    if not grouped.empty:
        # Keep only comparisons with enough samples to avoid random noise.
        grouped = grouped[grouped["n"] >= 3].copy()
        grouped = grouped.sort_values(["n", "scanner_mean"], ascending=[False, False], na_position="last")
        if not grouped.empty:
            sc_p33 = float(grouped["scanner_mean"].quantile(0.33))
            sc_p67 = float(grouped["scanner_mean"].quantile(0.67))
            mk_p33 = float(grouped["market_mean"].quantile(0.33))
            mk_p67 = float(grouped["market_mean"].quantile(0.67))
        else:
            sc_p33 = sc_p67 = mk_p33 = mk_p67 = 0.0

        for _, g in grouped.head(30).iterrows():
            scanner_mean = float(g["scanner_mean"]) if pd.notna(g["scanner_mean"]) else 0.0
            market_mean = float(g["market_mean"]) if pd.notna(g["market_mean"]) else 0.0
            warn_share = float(g["warn_share"]) if pd.notna(g["warn_share"]) else 0.0
            n = int(g["n"])

            scanner_band = "mittel"
            if scanner_mean >= sc_p67:
                scanner_band = "hoch"
            elif scanner_mean <= sc_p33:
                scanner_band = "niedrig"

            market_band = "neutral"
            if market_mean >= mk_p67:
                market_band = "positiv"
            elif market_mean <= mk_p33:
                market_band = "negativ"

            # Percentile-based, robust signal logic.
            if warn_share > 0.45 or market_band == "negativ":
                signal = "Kontra"
                severity = "warn"
                verdict = "contra"
                hint = "Konflikt: Markt/Qualität bremst."
            elif scanner_band == "hoch" and market_band in {"neutral", "positiv"} and warn_share <= 0.30:
                signal = "Scanner+"
                severity = "ok"
                verdict = "ok"
                hint = "Passend: intern stark, Markt trägt."
            else:
                signal = "Warn"
                severity = "warn"
                verdict = "warn"
                hint = "Gemischt: weiter prüfen."

            problems = [
                f"n={n}",
                f"warn%={warn_share * 100:.0f}%",
                f"bewertung={scanner_band}",
                f"markt={market_band}",
            ]
            top_issues.append(
                {
                    "symbol": "",
                    "name": "",
                    "severity": severity,
                    "signal": signal,
                    "verdict": verdict,
                    "reality_score": round(max(0.0, 1.0 - warn_share), 3),
                    "intern": str(g["intern"]),
                    "offiziell": str(g["offiziell"]),
                    "scanner": round(scanner_mean, 1),
                    "market": round(market_mean, 1),
                    "scanner_band": scanner_band,
                    "market_band": market_band,
                    "hint": hint,
                    "problems": problems,
                    "problems_text": "; ".join(problems),
                    "n": n,
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
