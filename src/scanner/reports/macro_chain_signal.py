from __future__ import annotations

"""Macro Chain Signal (Mega-Trend Signal).

Explainability-only report:
- computes compact chain signals from existing watchlist data
- writes deterministic artifacts under artifacts/reports
- never influences scoring

Inputs
------
- artifacts/watchlist/watchlist_full.csv

Outputs
-------
- artifacts/reports/macro_chain_signal.json
- artifacts/reports/macro_chain_signal.csv
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 1


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[name]


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _pillar_num(v: Any) -> int | None:
    s = str(v or "").strip()
    m = re.match(r"^S\s*([0-9]{1,2})\b", s, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))


def _status(strength: float | None, cov: float) -> str:
    if strength is None or cov <= 0.01:
        return "inactive"
    if strength >= 85 and cov >= 0.80:
        return "extended"
    if strength >= 70 and cov >= 0.60:
        return "confirmed"
    if strength >= 50 and cov >= 0.40:
        return "building"
    if strength >= 30 and cov >= 0.20:
        return "early"
    return "inactive"


def _interp(chain_key: str, pstats: list[dict[str, Any]], st: str) -> str:
    if not pstats:
        return "Noch zu wenig Daten fuer belastbares Signal."
    lead = [
        x
        for x in sorted(
            pstats,
            key=lambda z: float(z.get("strength")) if isinstance(z.get("strength"), (int, float)) else -1.0,
            reverse=True,
        )
        if isinstance(x.get("strength"), (int, float))
    ]
    lead = [f"S{int(x['pillar'])}" for x in lead[:2] if x.get("pillar") is not None]
    lag = [f"S{int(x['pillar'])}" for x in pstats if isinstance(x.get("strength"), (int, float)) and float(x["strength"]) < 45][:1]

    if chain_key == "fin":
        if st in {"confirmed", "extended"}:
            return "Kapitalfluss stuetzt Risikoappetit."
        if st in {"building", "early"}:
            return "Finanzsystem wird konstruktiver, aber noch nicht voll bestaetigt."
        return "Finanzsystem liefert aktuell keine klare Breite."

    if lead and lag:
        return f"{' + '.join(lead)} fuehren, {', '.join(lag)} hinkt noch hinterher."
    if lead:
        return f"{' + '.join(lead)} fuehren die aktuelle Bewegung."
    return "Bestaetigung ist noch schmal und uneinheitlich."


def build_macro_chain_signal(df_full: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if df_full is None or df_full.empty:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "date": _utc_today(),
            "universe_n": 0,
            "chains": [],
        }
        return pd.DataFrame(columns=["chain", "pillar", "n", "strength", "active"]), payload

    work = df_full.copy()
    work["pillar_num"] = _col(work, "pillar_primary").map(_pillar_num)
    work["score"] = _to_num(_col(work, "score"))
    work["dscore_1d"] = _to_num(_col(work, "dscore_1d"))
    work["perf_1d"] = _to_num(
        _col(work, "perf_1d").where(_col(work, "perf_1d").notna(), _col(work, "Perf 1D %"))
    )
    work["trend_ok"] = _col(work, "trend_ok").astype(str).str.lower().isin({"true", "1", "yes"})

    chains = [
        {"key": "tech", "name": "TechnoStack", "pillars": [4, 3, 2, 1, 5]},
        {"key": "civil", "name": "Civilization", "pillars": [16, 11, 13, 14, 15]},
        {"key": "fin", "name": "Finanzsystem", "pillars": [21]},
    ]

    pillar_rows: list[dict[str, Any]] = []
    chain_rows: list[dict[str, Any]] = []

    for ch in chains:
        pst: list[dict[str, Any]] = []
        for p in ch["pillars"]:
            sub = work[work["pillar_num"] == p].copy()
            n = int(len(sub))
            if n == 0:
                pst.append({"pillar": p, "n": 0, "strength": None, "active": False})
                pillar_rows.append({"chain": ch["key"], "pillar": p, "n": 0, "strength": None, "active": False})
                continue

            d_avg = float(sub["dscore_1d"].dropna().mean()) if sub["dscore_1d"].notna().any() else 0.0
            perf_base = sub["perf_1d"].notna().sum()
            pos_share = float((sub["perf_1d"] > 0).sum() / perf_base) if perf_base > 0 else 0.5
            trend_share = float(sub["trend_ok"].mean()) if n > 0 else 0.5
            sc_avg = float(sub["score"].dropna().mean()) if sub["score"].notna().any() else 0.0

            s = 0.0
            s += _clamp((d_avg + 2.0) / 4.0, 0.0, 1.0) * 35.0
            s += _clamp(pos_share, 0.0, 1.0) * 30.0
            s += _clamp(trend_share, 0.0, 1.0) * 25.0
            s += _clamp(sc_avg / 40.0, 0.0, 1.0) * 10.0
            strength = int(round(_clamp(s, 0.0, 100.0)))
            active = bool(strength >= 55)

            rec = {"pillar": p, "n": n, "strength": strength, "active": active}
            pst.append(rec)
            pillar_rows.append({"chain": ch["key"], **rec})

        known = [x["strength"] for x in pst if isinstance(x.get("strength"), (int, float))]
        chain_strength = int(round(sum(known) / len(known))) if known else None
        active_cnt = int(sum(1 for x in pst if x.get("active")))
        total = int(len(ch["pillars"]))
        cov = (active_cnt / total) if total else 0.0
        st = _status(chain_strength, cov)
        hint = _interp(ch["key"], pst, st)
        chain_rows.append(
            {
                "key": ch["key"],
                "name": ch["name"],
                "status": st,
                "strength": chain_strength,
                "coverage_active": active_cnt,
                "coverage_total": total,
                "coverage": f"{active_cnt}/{total}",
                "hint": hint,
            }
        )

    m = {x["key"]: x for x in chain_rows}
    tech_pos = m.get("tech", {}).get("status") in {"building", "confirmed", "extended"}
    civ_pos = m.get("civil", {}).get("status") in {"building", "confirmed", "extended"}
    fin_pos = m.get("fin", {}).get("status") in {"building", "confirmed", "extended"}
    cross_active = int(sum([tech_pos, civ_pos, fin_pos]))
    strengths = [x.get("strength") for x in chain_rows if isinstance(x.get("strength"), (int, float))]
    cross_strength = int(round(sum(strengths) / len(strengths))) if strengths else None
    cross_status = _status(cross_strength, cross_active / 3.0)
    cross_hint = "Keine breite Cross-Chain-Bestaetigung."
    if tech_pos and civ_pos and fin_pos:
        cross_hint = "Alle drei Ketten bestaetigen sich."
    elif tech_pos and fin_pos:
        cross_hint = "Tech + Finanzsystem bestaetigen sich gegenseitig."
    elif civ_pos and fin_pos:
        cross_hint = "Civilization + Finanzsystem wirken stabilisierend."
    elif tech_pos and civ_pos:
        cross_hint = "Tech + Civilization bauen Breite auf."

    chain_rows.append(
        {
            "key": "cross",
            "name": "Cross-Chain",
            "status": cross_status,
            "strength": cross_strength,
            "coverage_active": cross_active,
            "coverage_total": 3,
            "coverage": f"{cross_active}/3",
            "hint": cross_hint,
        }
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date": _utc_today(),
        "universe_n": int(len(work)),
        "chains": chain_rows,
    }
    return pd.DataFrame(pillar_rows), payload


def write_macro_chain_outputs(df: pd.DataFrame, payload: dict[str, Any]) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "macro_chain_signal.json"
    p_csv = out_dir / "macro_chain_signal.csv"
    p_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    to_csv_safely(df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
