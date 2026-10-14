"""Read-only A/B readiness audit for an exact uranium scanner snapshot.

No fabricated portfolio actions or challenger outcome statistics.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Mapping


def audit(daily: Mapping, w10: Mapping, lab: Mapping, symbols=("UEC", "UUUU")) -> dict:
    snapshot = str(daily.get("snapshot_id") or "")
    if not snapshot or snapshot != str(w10.get("snapshot_id") or ""):
        raise ValueError("scanner_w10_snapshot_mismatch")
    observations = daily.get("symbols")
    if not isinstance(observations, Mapping):
        raise ValueError("daily_symbols_missing")
    evidence = {}
    for symbol in symbols:
        row = observations.get(symbol)
        if not isinstance(row, Mapping):
            evidence[symbol] = {"status": "UNAVAILABLE"}
            continue
        current = row.get("current") or {}
        hist = row.get("historical_matches") or {}
        horizons = {}
        for h in (5, 10, 20, 40):
            sample = hist.get(f"forward_{h}t") or {}
            n = int(sample.get("N") or 0)
            count = int(sample.get("positive_count") or 0)
            if n < 0 or count < 0 or count > n:
                raise ValueError(f"invalid_outcomes:{symbol}:{h}")
            horizons[str(h)] = {
                "observations": n,
                "positive": count,
                "positive_rate": (count / n if n else None),
                "median_raw_return": sample.get("median_return") if n else None,
                "status": "DESCRIPTIVE_ONLY" if n >= 20 else "INSUFFICIENT_OR_MISSING",
            }
        evidence[symbol] = {
            "status": "DESCRIPTIVE_ONLY",
            "cluster": current.get("cluster_official"),
            "current": {k: current.get(k) for k in ("score", "confidence", "r_code", "rs3m", "trend200", "cycle")},
            "historical_filter_id": hist.get("filter_id"),
            "historical_conditions": hist.get("conditions"),
            "historical_horizons": horizons,
        }
    phase6 = (w10.get("stages") or {}).get("phase6_elliott") or {}
    result = {
        "schema_version": "uran_ab_readiness_v1",
        "snapshot_id": snapshot,
        "as_of": daily.get("as_of"),
        "research_only": True,
        "source_observation_count": lab.get("history_information", {}).get("observation_count"),
        "pattern_frozen_count": lab.get("laboratory_state", {}).get("frozen_pattern_count"),
        "pattern_claim_count": lab.get("laboratory_state", {}).get("claim_count"),
        "phase6_elliott_status": phase6.get("status"),
        "A_private_watch_supplied": False,
        "B_outcome_test_executed": False,
        "A_vs_B_incremental_value": "NOT_ESTIMABLE",
        "limitations": [
            "Different historical-match filters are non-comparable cohorts",
            "Raw returns are not cost-adjusted sector alpha",
            "No matched private A-Watch 7H position book supplied",
            "No frozen L5 pattern and matured prospective B outcomes supplied",
            "No backdated Elliott contexts",
        ],
        "symbols": evidence,
    }
    return result


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    root = args.root
    daily = json.loads((root / "artifacts/research/daily_research.json").read_text(encoding="utf-8"))
    w10 = json.loads((root / "artifacts/research/decision_snapshot_w10.json").read_text(encoding="utf-8"))
    pointer = json.loads((root / "artifacts/research/pattern_discovery/operations/latest.json").read_text(encoding="utf-8"))
    lab = json.loads((root / pointer["cycle_path"]).read_text(encoding="utf-8"))
    report = audit(daily, w10, lab)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
