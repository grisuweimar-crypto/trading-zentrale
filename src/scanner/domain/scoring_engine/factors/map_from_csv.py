from __future__ import annotations
from typing import Dict, Any
import math


def _f(x, default=None):
    try:
        if x is None or x == "":
            return default
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def build_raw_from_csv_row(row) -> Dict[str, Any]:
    """
    Row ist pd.Series aus watchlist.csv.
    Liefert RAW-Features (noch NICHT 0..1 skaliert).
    Wichtig: fehlende Werte -> None (damit Normalisierung neutral werden kann)
    """

    crv = _f(row.get("CRV", None), default=None)
    # 0 oder negativ bedeutet in deiner Praxis: "kein Setup/keine Info"
    if crv is not None and crv <= 0:
        crv = None

    debt = _f(row.get("Debt/Equity", None), default=None)
    if debt is not None and debt < 0:
        debt = None

    # Price (we support both the native price column and the EUR convenience column)
    current_price = _f(row.get("Akt. Kurs", None), default=None)
    if current_price is None:
        current_price = _f(row.get("Akt. Kurs [€]", None), default=None)

    ell_target = _f(row.get("Elliott-Ausstieg", None), default=None)
    target_distance = None
    if current_price is not None and ell_target is not None and current_price > 0:
        # distance to target in pct (e.g. 0.25 for +25%)
        target_distance = (float(ell_target) / float(current_price)) - 1.0

    return {
        # Opportunity RAW
        "growth_pct": _f(row.get("Growth %", None), default=None),
        "roe_pct": _f(row.get("ROE %", None), default=None),
        "margin_pct": _f(row.get("Margin %", None), default=None),
        "mc_chance": _f(row.get("MC-Chance", None), default=None),

        "elliott_signal": str(row.get("Elliott-Signal", "")),
        "elliott_entry": _f(row.get("Elliott-Einstieg", None), default=None),
        "elliott_target": ell_target,

        # price helpers
        "current_price": current_price,
        "target_distance": target_distance,

        "volatility": _f(row.get("Volatility", None), default=None),
        "downside_dev": _f(row.get("DownsideDev", None), default=None),
        "max_drawdown": _f(row.get("MaxDrawdown", None), default=None),
        "avg_volume": _f(row.get("AvgVolume", None), default=None),
        "dollar_volume": _f(row.get("DollarVolume", None), default=None),
        "trend200": _f(row.get("Trend200", None), default=None),
        "rs3m": _f(row.get("RS3M", None), default=None),


        # Risk RAW
        "debt_to_equity": debt,
        "crv": crv,

        # Extras
        "cycle_pct": _f(row.get("Zyklus %", None), default=None),
        "perf_pct": _f(row.get("Perf %", None), default=None),
        "sector": str(row.get("Sektor", "")),
    }
