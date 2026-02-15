from __future__ import annotations

"""scanner.app.score_step

This is the *pipeline* scoring step (app-layer).

It takes the raw watchlist DataFrame (your DB-like table), runs the domain
scoring_engine for every row, and writes results back into the legacy columns
so the rest of the system stays compatible:

- Score
- OpportunityScore
- RiskScore
- ConfidenceScore / ConfidenceLabel / ConfidenceBreakdown
- ScoreError (empty on success)

Important: we do NOT silently set score=0 on errors. Errors are surfaced.
"""

from typing import Optional
import os
import json
import pandas as pd

from scanner.domain.scoring_engine.factors.universe_csv import load_universe
from scanner.domain.scoring_engine.engine import calculate_scores_v6_from_row


def _pick_identifier(row: pd.Series) -> str:
    """Pick the best identifier available for display / asset-class inference."""

    for col in ("YahooSymbol", "Yahoo", "Symbol", "Ticker", "ISIN"):
        v = row.get(col, None)
        if v is not None and str(v).strip() != "" and str(v).strip().lower() != "nan":
            return str(v).strip()
    return ""


def apply_scoring(df_raw: pd.DataFrame, *, universe_csv_path: Optional[str] = None) -> pd.DataFrame:
    """Compute scores for all rows.

    Args:
        df_raw: Raw watchlist table (as loaded from CSV).
        universe_csv_path: Optional path to the CSV to build the universe from.
            If omitted, scoring falls back to a small in-memory universe built from df_raw.

    Returns:
        DataFrame with scoring columns written/updated.
    """

    out = df_raw.copy()

    # Build universe once (fast + consistent)
    if universe_csv_path:
        universe = load_universe(universe_csv_path)
    else:
        # Fallback: write a minimal universe from df_raw
        # NOTE: This path is rarely used in vNext (we usually pass universe_csv_path).
        tmp = out.copy()
        # Reuse the same function by exporting to csv in memory is overkill; instead we
        # approximate by creating the Universe directly.
        from scanner.domain.scoring_engine.factors.universe_csv import Universe, _build_dist  # type: ignore

        numeric_cols = [
            "MC-Chance",
            "Growth %",
            "Margin %",
            "ROE %",
            "Debt/Equity",
            "CRV",
            "Zyklus %",
            "Perf %",
            "Score",
            "Volatility",
            "DownsideDev",
            "MaxDrawdown",
            "AvgVolume",
            "DollarVolume",
            "Trend200",
            "RS3M",
        ]
        dists = {c: _build_dist(tmp[c]) for c in numeric_cols if c in tmp.columns}
        universe = Universe(df=tmp, dists=dists)

    store_factors = os.getenv("SCANNER_STORE_SCORE_FACTORS", "0").strip() in {"1", "true", "yes"}

    # Prepare output columns
    score_list = []
    opp_list = []
    risk_list = []
    conf_list = []
    conf_label_list = []
    conf_breakdown_list = []
    err_list = []

    # meta (helps diagnose 0-scores)
    regime_list = []
    asset_class_list = []
    risk_mult_list = []
    opp_w_list = []
    risk_w_list = []
    mtrend200_list = []

    # optional factor payloads
    opp_factors_list = []
    risk_factors_list = []

    for _, row in out.iterrows():
        ident = _pick_identifier(row)
        res = calculate_scores_v6_from_row(row, universe, identifier=ident)

        if "error" in res:
            score_list.append(pd.NA)
            opp_list.append(pd.NA)
            risk_list.append(pd.NA)
            conf_list.append(pd.NA)
            conf_label_list.append(pd.NA)
            conf_breakdown_list.append(pd.NA)
            err_list.append(res["error"])

            regime_list.append(pd.NA)
            asset_class_list.append(pd.NA)
            risk_mult_list.append(pd.NA)
            opp_w_list.append(pd.NA)
            risk_w_list.append(pd.NA)
            mtrend200_list.append(pd.NA)
            if store_factors:
                opp_factors_list.append(pd.NA)
                risk_factors_list.append(pd.NA)
            continue

        score_list.append(res.get("score", pd.NA))
        opp_list.append(res.get("opportunity_score", pd.NA))
        risk_list.append(res.get("risk_score", pd.NA))
        conf_list.append(res.get("confidence_score", pd.NA))
        conf_label_list.append(res.get("confidence_label", pd.NA))

        bd = res.get("confidence_breakdown", {})
        try:
            conf_breakdown_list.append(json.dumps(bd, ensure_ascii=False))
        except Exception:
            conf_breakdown_list.append(str(bd))

        err_list.append("")

        meta = res.get("meta", {}) or {}
        regime_list.append(meta.get("market_regime", pd.NA))
        asset_class_list.append(meta.get("asset_class", pd.NA))
        risk_mult_list.append(meta.get("risk_mult", pd.NA))
        opp_w_list.append(meta.get("opp_w", pd.NA))
        risk_w_list.append(meta.get("risk_w", pd.NA))
        mtrend200_list.append(meta.get("market_trend200", pd.NA))

        if store_factors:
            fb = res.get("factor_breakdown", {}) or {}
            try:
                opp_factors_list.append(json.dumps(fb.get("opportunity", {}), ensure_ascii=False))
            except Exception:
                opp_factors_list.append(str(fb.get("opportunity", {})))
            try:
                risk_factors_list.append(json.dumps(fb.get("risk", {}), ensure_ascii=False))
            except Exception:
                risk_factors_list.append(str(fb.get("risk", {})))

    # Write back in legacy column names (so existing CSV users keep working)
    out["Score"] = pd.to_numeric(pd.Series(score_list), errors="coerce")
    out["OpportunityScore"] = pd.to_numeric(pd.Series(opp_list), errors="coerce")
    out["RiskScore"] = pd.to_numeric(pd.Series(risk_list), errors="coerce")

    out["ConfidenceScore"] = pd.to_numeric(pd.Series(conf_list), errors="coerce")
    out["ConfidenceLabel"] = pd.Series(conf_label_list, dtype="string")
    out["ConfidenceBreakdown"] = pd.Series(conf_breakdown_list, dtype="string")

    out["ScoreError"] = pd.Series(err_list, dtype="string")

    # meta columns (small + useful for health reports)
    out["ScoreMarketRegime"] = pd.Series(regime_list, dtype="string")
    out["ScoreAssetClass"] = pd.Series(asset_class_list, dtype="string")
    out["ScoreRiskMult"] = pd.to_numeric(pd.Series(risk_mult_list), errors="coerce")
    out["ScoreOppW"] = pd.to_numeric(pd.Series(opp_w_list), errors="coerce")
    out["ScoreRiskW"] = pd.to_numeric(pd.Series(risk_w_list), errors="coerce")
    out["ScoreMarketTrend200"] = pd.to_numeric(pd.Series(mtrend200_list), errors="coerce")

    if store_factors:
        out["ScoreOppFactors"] = pd.Series(opp_factors_list, dtype="string")
        out["ScoreRiskFactors"] = pd.Series(risk_factors_list, dtype="string")

    return out
