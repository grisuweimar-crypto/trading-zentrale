# scoring_engine/factors/universe_csv.py
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Universe:
    df: pd.DataFrame
    dists: Dict[str, List[float]]


def _to_float(x, default=None):
    try:
        if x is None:
            return default
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _build_dist(series: pd.Series) -> List[float]:
    vals = []
    for v in series.tolist():
        fv = _to_float(v, None)
        if fv is not None:
            vals.append(fv)
    vals.sort()
    return vals


def percentile_rank(value: float, dist: List[float], neutral: float = 0.5) -> float:
    if value is None or not math.isfinite(value) or not dist:
        return neutral
    # Anteil der Werte, die kleiner sind -> 0..1
    cnt = 0
    for d in dist:
        if d < value:
            cnt += 1
        else:
            break
    return cnt / len(dist)


def load_universe(csv_path: str) -> Universe:
    df = pd.read_csv(csv_path)
    # Dists für alle numerischen Spalten, die wir nutzen wollen:
    columns = [
        "MC-Chance",
        "Growth %",
        "Margin %",
        "ROE %",
        "Debt/Equity",
        "Div. Rendite %",
        "PE",
        "CRV",
        "Zyklus %",
        "Current Ratio",
        "Institutional Ownership %",
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
    dists = {c: _build_dist(df[c]) for c in columns if c in df.columns}
    return Universe(df=df, dists=dists)


def get_row_by_ticker(universe: Universe, identifier: str) -> Optional[pd.Series]:
    """Find a row by any reasonable identifier.

    Your legacy watchlist sometimes stores ISINs in `Ticker`, and you also have
    `ISIN`, `Symbol`, `Yahoo`, `YahooSymbol`. We therefore search multiple columns.
    """

    key = str(identifier).upper()
    df = universe.df

    candidate_cols = [
        "Ticker",
        "YahooSymbol",
        "Yahoo",
        "Symbol",
        "ISIN",
    ]

    for col in candidate_cols:
        if col not in df.columns:
            continue
        rows = df[df[col].astype(str).str.upper() == key]
        if not rows.empty:
            return rows.iloc[0]

    return None


def scale_from_universe(
    universe: Universe,
    column: str,
    value: Any,
    neutral: float = 0.5,
) -> float:
    """Scale a value to 0..1 using the universe distribution.

    vNext simplification: no external config dependencies; if the column isn't
    available (or the value is missing), we return `neutral`.
    """
    dist = universe.dists.get(column, [])
    v = _to_float(value, default=None)
    return max(0.0, min(1.0, percentile_rank(v, dist, neutral)))
