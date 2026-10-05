from __future__ import annotations

"""Reality Check: scanner-internal segment movement vs official grouping movement.

This report is an explainability/validation layer. It does not decide whether
"the scanner" or "the market" is right and it never feeds back into scoring.
It compares the same 1D scanner-score movement through two different grouping
lenses:

- internal: scanner-owned pillar_primary
- official: cluster_official, with sector as a fallback label

The result is deliberately categorical and transparent rather than a synthetic
super-score.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scanner.data.io.paths import artifacts_dir
from scanner.data.io.safe_csv import to_csv_safely


SCHEMA_VERSION = 2
ALIGNMENT_TOLERANCE = 0.10
MIN_PAIR_VALID = 5
MIN_PAIR_COVERAGE = 0.50


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "<na>", "none"} else text


def _symbol_series(df: pd.DataFrame) -> pd.Series:
    for name in ("asset_id", "symbol", "ticker_display", "ticker"):
        if name in df.columns:
            values = df[name].fillna("").astype(str).str.strip()
            if values.ne("").any():
                return values
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _history_delta_map(
    history_delta: Mapping[str, Any] | None,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(history_delta, Mapping):
        return {}
    raw = history_delta.get("by_symbol")
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(symbol): row
        for symbol, row in raw.items()
        if isinstance(row, Mapping)
    }


def _metric_map(
    segment_monitor: Mapping[str, Any] | None,
    key: str,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(segment_monitor, Mapping):
        return {}
    raw = segment_monitor.get(key)
    if not isinstance(raw, list):
        return {}
    out: dict[str, Mapping[str, Any]] = {}
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        segment = _clean(row.get("segment"))
        if segment:
            out[segment] = row
    return out


def _movement(row: Mapping[str, Any] | None) -> float | None:
    if not isinstance(row, Mapping):
        return None
    try:
        value = float(row.get("average_dscore_1d"))
    except (TypeError, ValueError):
        return None
    return None if pd.isna(value) else value


def _category(
    internal: Mapping[str, Any] | None,
    official: Mapping[str, Any] | None,
    *,
    pair_n_valid: int,
    pair_coverage: float,
) -> tuple[str, str]:
    internal_value = _movement(internal)
    official_value = _movement(official)
    internal_valid = int(internal.get("n_valid") or 0) if isinstance(internal, Mapping) else 0
    official_valid = int(official.get("n_valid") or 0) if isinstance(official, Mapping) else 0
    internal_state = str(internal.get("sample_state") or "") if isinstance(internal, Mapping) else ""
    official_state = str(official.get("sample_state") or "") if isinstance(official, Mapping) else ""

    if (
        internal_value is None
        or official_value is None
        or pair_n_valid < MIN_PAIR_VALID
        or pair_coverage < MIN_PAIR_COVERAGE
        or internal_valid < MIN_PAIR_VALID
        or official_valid < MIN_PAIR_VALID
        or internal_state != "stable"
        or official_state != "stable"
    ):
        return "unclear", "Zu dünne oder unvollständige Vergleichsbasis."

    if internal_value * official_value < 0:
        return "contra_market", "Interne und offizielle Sicht laufen in entgegengesetzte Richtungen."

    difference = internal_value - official_value
    if abs(difference) <= ALIGNMENT_TOLERANCE:
        return "aligned", "Interne und offizielle Segmentbewegung sind ähnlich."
    if difference > 0:
        return "scanner_stronger", "Interne Scanner-Sicht ist stärker als die offizielle Gruppierung."
    return "scanner_weaker", "Interne Scanner-Sicht ist schwächer als die offizielle Gruppierung."


def build_reality_check(
    df: pd.DataFrame,
    *,
    history_delta: Mapping[str, Any] | None = None,
    segment_monitor: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [
        "intern",
        "offiziell",
        "scanner",
        "market",
        "signal",
        "verdict",
        "hint",
        "n",
        "n_valid",
        "coverage",
        "internal_sample_state",
        "official_sample_state",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns), {
            "schema_version": SCHEMA_VERSION,
            "date": _utc_today(),
            "stats": {
                "total": 0,
                "aligned": 0,
                "scanner_stronger": 0,
                "scanner_weaker": 0,
                "contra_market": 0,
                "unclear": 0,
            },
            "comparisons": [],
            "top_issues": [],
            "semantics": {
                "validation_layer_only": True,
                "absolute_truth_claimed": False,
                "mixed_super_metric_created": False,
            },
        }

    history_by_symbol = _history_delta_map(history_delta)
    internal_metrics = _metric_map(segment_monitor, "internal_segments")
    official_metrics = _metric_map(segment_monitor, "official_segments")

    work = df.copy()
    work["_symbol"] = _symbol_series(work)
    pillar = (
        work["pillar_primary"]
        if "pillar_primary" in work.columns
        else pd.Series([""] * len(work), index=work.index)
    )
    cluster = (
        work["cluster_official"]
        if "cluster_official" in work.columns
        else pd.Series([""] * len(work), index=work.index)
    )
    sector = (
        work["sector"]
        if "sector" in work.columns
        else pd.Series([""] * len(work), index=work.index)
    )

    pair_counts: dict[tuple[str, str], dict[str, int]] = {}
    for i in work.index:
        internal = _clean(pillar.loc[i])
        official = _clean(cluster.loc[i]) or _clean(sector.loc[i])
        if not internal or not official:
            continue
        key = (internal, official)
        rec = pair_counts.setdefault(key, {"n": 0, "n_valid": 0})
        rec["n"] += 1

        symbol = _clean(work.loc[i, "_symbol"])
        delta_row = history_by_symbol.get(symbol)
        if not isinstance(delta_row, Mapping) or str(delta_row.get("status") or "") != "ok":
            continue
        try:
            value = float(delta_row.get("score_delta"))
        except (TypeError, ValueError):
            continue
        if pd.isna(value):
            continue
        rec["n_valid"] += 1

    comparisons: list[dict[str, Any]] = []
    for (internal_name, official_name), counts in pair_counts.items():
        internal_row = internal_metrics.get(internal_name)
        official_row = official_metrics.get(official_name)
        n = int(counts["n"])
        n_valid = int(counts["n_valid"])
        coverage = (n_valid / n) if n else 0.0
        category, hint = _category(
            internal_row,
            official_row,
            pair_n_valid=n_valid,
            pair_coverage=coverage,
        )
        internal_value = _movement(internal_row)
        official_value = _movement(official_row)

        signal_labels = {
            "aligned": "Gleichgerichtet",
            "scanner_stronger": "Scanner stärker",
            "scanner_weaker": "Scanner schwächer",
            "contra_market": "Kontra Markt",
            "unclear": "Unklar",
        }
        comparisons.append(
            {
                "intern": internal_name,
                "offiziell": official_name,
                "scanner": internal_value,
                "market": official_value,
                "signal": signal_labels[category],
                "verdict": category,
                "hint": hint,
                "n": n,
                "n_valid": n_valid,
                "coverage": coverage,
                "internal_sample_state": (
                    internal_row.get("sample_state")
                    if isinstance(internal_row, Mapping)
                    else "unavailable"
                ),
                "official_sample_state": (
                    official_row.get("sample_state")
                    if isinstance(official_row, Mapping)
                    else "unavailable"
                ),
            }
        )

    priority = {
        "contra_market": 0,
        "scanner_weaker": 1,
        "scanner_stronger": 2,
        "aligned": 3,
        "unclear": 4,
    }
    comparisons.sort(
        key=lambda row: (
            priority.get(str(row["verdict"]), 9),
            -int(row["n_valid"]),
            -int(row["n"]),
            str(row["intern"]),
            str(row["offiziell"]),
        )
    )

    stats = {
        "total": len(comparisons),
        "aligned": sum(row["verdict"] == "aligned" for row in comparisons),
        "scanner_stronger": sum(row["verdict"] == "scanner_stronger" for row in comparisons),
        "scanner_weaker": sum(row["verdict"] == "scanner_weaker" for row in comparisons),
        "contra_market": sum(row["verdict"] == "contra_market" for row in comparisons),
        "unclear": sum(row["verdict"] == "unclear" for row in comparisons),
    }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date": _utc_today(),
        "history_latest_date": (
            history_delta.get("latest_date") if isinstance(history_delta, Mapping) else None
        ),
        "segment_latest_date": (
            segment_monitor.get("latest_date")
            if isinstance(segment_monitor, Mapping)
            else None
        ),
        "stats": stats,
        "comparisons": comparisons,
        # Backward-compatible key for older dashboard readers.
        "top_issues": comparisons,
        "rules": {
            "alignment_tolerance_dscore": ALIGNMENT_TOLERANCE,
            "min_pair_valid": MIN_PAIR_VALID,
            "min_pair_coverage": MIN_PAIR_COVERAGE,
        },
        "semantics": {
            "validation_layer_only": True,
            "internal_grouping": "pillar_primary",
            "official_grouping": "cluster_official_or_sector",
            "comparison_metric": "average_dscore_1d",
            "absolute_truth_claimed": False,
            "market_price_performance_claimed": False,
            "mixed_super_metric_created": False,
            "scanner_or_market_declared_superior": False,
            "thin_data_forces_hard_signal": False,
        },
    }
    return pd.DataFrame(comparisons, columns=columns), payload


def write_reality_check_outputs(
    df: pd.DataFrame, payload: dict[str, Any]
) -> dict[str, Path]:
    out_dir = artifacts_dir() / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    p_json = out_dir / "reality_check.json"
    p_csv = out_dir / "reality_check.csv"

    p_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    to_csv_safely(df, p_csv, index=False)
    return {"json": p_json, "csv": p_csv}
