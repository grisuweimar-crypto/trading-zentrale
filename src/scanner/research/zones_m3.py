"""M3: causal, deterministic research-zone clustering and conditional plans.

No probabilities, executions, order placement or automatic trading decisions.
All plans start *after* the as-of session: historical touches never retro-trigger.
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

M3_VERSION = "argus-zones-m3-v0.1-research"
MAX_CLUSTER_RELATIVE_WIDTH = 0.08
MAX_PLANS_PER_SIDE = 6


def _id(prefix, payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return prefix + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _anchor_key(anchor):
    """Ignore pivot 'degree': identical physical turns across degrees are related."""
    return (str(anchor["kind"]), str(anchor["pivot_time"]),
            round(float(anchor["price"]), 6))


def _anchor_set(zone):
    return {_anchor_key(a) for d in zone.get("drivers", [])
            for a in d.get("anchors", [])}


def _independent_anchor_groups(zones):
    """Connected components: shared pivots imply dependence, even across families."""
    keys = [_anchor_set(z) for z in zones]
    parents = list(range(len(keys)))

    def root(i):
        while parents[i] != i:
            i = parents[i]
        return i

    for i in range(len(keys)):
        for j in range(i):
            if keys[i] & keys[j]:
                parents[root(i)] = root(j)
    # Anchorless zones must not be misrepresented as corroboration.
    return len({root(i) for i in range(len(keys))})


def cluster_zones(zones, close):
    """Merge overlapping *same-role/side/timeframe* bands without chain sprawl."""
    raw = sorted(zones, key=lambda z: (z["role"], z["side"],
                 z["timeframe"], float(z["lower"]), float(z["upper"]), z["zone_id"]))
    groups = []
    for z in raw:
        lo, hi = float(z["lower"]), float(z["upper"])
        if not 0 < lo <= hi:
            raise ValueError("invalid_zone_price_band")
        if z.get("evidence_status") != "not_validated":
            raise ValueError("unexpected_zone_evidence_status")
        selected = None
        for g in reversed(groups):
            if (g["role"], g["side"], g["timeframe"]) != (
                    z["role"], z["side"], z["timeframe"]):
                continue
            union_lo, union_hi = min(g["lower"], lo), max(g["upper"], hi)
            midpoint = (union_lo + union_hi) / 2
            if (lo <= g["upper"] and hi >= g["lower"] and
                    (union_hi - union_lo) <= MAX_CLUSTER_RELATIVE_WIDTH * midpoint):
                selected = g
                break
        if selected is None:
            groups.append({"role": z["role"], "side": z["side"],
                           "timeframe": z["timeframe"], "lower": lo,
                           "upper": hi, "members": [z]})
        else:
            selected["lower"] = min(selected["lower"], lo)
            selected["upper"] = max(selected["upper"], hi)
            selected["members"].append(z)

    out = []
    for g in groups:
        members = sorted(g["members"], key=lambda z: z["zone_id"])
        ids = sorted({z["zone_id"] for z in members})
        families = sorted({d["family"] for z in members for d in z.get("drivers", [])})
        independent = _independent_anchor_groups(members)
        lo, hi = g["lower"], g["upper"]
        if g["role"] == "support" and close < lo:
            status = "broken"
        elif g["role"] == "resistance" and close > hi:
            status = "broken"
        elif any(z["status"] == "touched" for z in members):
            status = "touched"
        else:
            status = "watch"
        out.append({
            "cluster_id": _id("AC-", {"version": M3_VERSION, "members": ids}),
            "role": g["role"], "side": g["side"], "timeframe": g["timeframe"],
            "lower": round(lo, 6), "upper": round(hi, 6),
            "known_at": max(z["known_at"] for z in members),
            "status": status, "source_zone_ids": ids, "families": families,
            "distinct_family_count": len(families),
            "independent_anchor_groups": independent,
            "dependent_source_count": max(0, len(members) - independent),
            "technical_relevance": {
                "band_width_pct": round((hi - lo) / ((hi + lo) / 2) * 100, 4),
                "distinct_families": families,
                "independent_anchor_groups": independent,
                "note": "geometry only; related swings are not independent votes",
            },
            "empirical_probability": None, "evidence_status": "not_validated",
        })
    return sorted(out, key=lambda g: (g["lower"], g["role"], g["side"], g["cluster_id"]))


def _linked(entry, candidate, raw_by_id):
    e = set().union(*(_anchor_set(raw_by_id[x]) for x in entry["source_zone_ids"]))
    c = set().union(*(_anchor_set(raw_by_id[x]) for x in candidate["source_zone_ids"]))
    return bool(e & c)


def build_scenario_plans(clusters, raw_zones, *, asof, close):
    """Propose *future* two-step confirmations, never imply orders or fills.

    Touch must occur on a completed session strictly AFTER asof; confirmation
    is a subsequent completed session. Invalidation is a separately anchored
    origin *below* long entry or *above* short entry.
    """
    raw_by_id = {z["zone_id"]: z for z in raw_zones}
    out = []
    for side, entry_role in (("bull", "support"), ("bear", "resistance")):
        entries = [c for c in clusters if c["side"] == side and
                   c["role"] == entry_role and c["status"] != "broken"]
        entries.sort(key=lambda c: (abs((c["lower"]+c["upper"])/2 - close),
                                    c["cluster_id"]))
        for entry in entries:
            invalidations = [c for c in clusters
                if c["role"] == "invalidation" and c["side"] == side and
                (c["upper"] < entry["lower"] if side == "bull"
                 else c["lower"] > entry["upper"]) and
                _linked(entry, c, raw_by_id)]
            if not invalidations:
                continue
            invalidations.sort(key=lambda c: (
                entry["lower"] - c["upper"] if side == "bull"
                else c["lower"] - entry["upper"], c["cluster_id"]))
            invalid = invalidations[0]
            # A target and a structural opposing band are both possible exit
            # candidates, but must be strictly on the favorable side of entry.
            target_candidates = [c for c in clusters
                if ((c["role"] == "target" and c["side"] == side) or
                    (side == "bull" and c["role"] == "resistance") or
                    (side == "bear" and c["role"] == "support")) and
                (c["lower"] > max(entry["upper"], close) if side == "bull"
                 else c["upper"] < min(entry["lower"], close))]
            target_candidates.sort(key=lambda c: (
                c["lower"] - entry["upper"] if side == "bull"
                else entry["lower"] - c["upper"], c["cluster_id"]))
            targets = []
            for c in target_candidates:
                # Overlapping projection/structural bands are one price waypoint.
                if any(c["lower"] <= t["upper"] and c["upper"] >= t["lower"]
                       for t in targets):
                    continue
                targets.append(c)
                if len(targets) == 3:
                    break
            if not targets:
                continue
            touch = "low <= entry.upper AND high >= entry.lower"
            confirm = ("next completed close > entry.upper" if side == "bull"
                       else "next completed close < entry.lower")
            breach = ("completed close < invalidation.lower" if side == "bull"
                      else "completed close > invalidation.upper")
            ident = {"v": M3_VERSION, "asof": asof, "side": side,
                     "entry": entry["cluster_id"], "invalid": invalid["cluster_id"],
                     "targets": [c["cluster_id"] for c in targets]}
            out.append({
                "scenario_id": _id("AP-", ident), "side": side,
                "setup_type": ("support_rebound_confirmation" if side == "bull"
                               else "resistance_rejection_confirmation"),
                "state": "untriggered", "asof": asof,
                "known_at": max([entry["known_at"], invalid["known_at"]] +
                                [c["known_at"] for c in targets]),
                "entry_cluster_id": entry["cluster_id"],
                "entry_zone_ids": entry["source_zone_ids"],
                "target_cluster_ids": [c["cluster_id"] for c in targets],
                "target_zone_ids": [x for c in targets for x in c["source_zone_ids"]],
                "invalidation_cluster_id": invalid["cluster_id"],
                "invalidation_zone_ids": invalid["source_zone_ids"],
                "activation": {
                    "valid_from_exclusive": asof,
                    "touch": touch, "confirmation": confirm,
                    "order": "touch first, confirmation on a later completed daily bar",
                    "invalidation": breach,
                },
                "activation_rule": "untriggered; future closed-bar touch then confirmation",
                "horizon_bars": 20, "fill_assumption": None,
                "risk_reward_after_costs": None, "empirical_probability": None,
                "evidence_status": "not_validated",
            })
            if len([x for x in out if x["side"] == side]) >= MAX_PLANS_PER_SIDE:
                break
    return sorted(out, key=lambda p: (p["side"], p["entry_cluster_id"], p["scenario_id"]))


def enrich_m3(report):
    """In-place research-only output expansion; preserves all M2 raw zones."""
    report["m3_version"] = M3_VERSION
    report["zone_clusters"] = []
    report["scenario_plans"] = []
    report["m3_status"] = "no_valid_setup"
    if report.get("data_quality") == "blocked" or not report.get("zones"):
        return report
    close = report.get("asof_raw_close")
    if close is None or float(close) <= 0:
        return report
    clusters = cluster_zones(report["zones"], float(close))
    plans = build_scenario_plans(clusters, report["zones"],
                                 asof=report["asof"], close=float(close))
    report["zone_clusters"] = clusters
    report["scenario_plans"] = plans
    report["m3_status"] = "conditional_setups_unvalidated" if plans else "no_valid_setup"
    return report


def write_readable_tables(report, output_json_path):
    """Export M3 cluster/plan CSV plus a compact human-readable Markdown audit."""
    path = Path(output_json_path)
    clusters = report.get("zone_clusters", [])
    plans = report.get("scenario_plans", [])
    cpath = path.with_name(path.stem + "_clusters.csv")
    spath = path.with_name(path.stem + "_plans.csv")
    with cpath.open("w", newline="", encoding="utf-8") as f:
        fields = ["cluster_id", "role", "side", "lower", "upper", "status",
                  "known_at", "distinct_family_count", "independent_anchor_groups",
                  "dependent_source_count", "source_zone_ids", "families"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for c in clusters:
            w.writerow({k: (";".join(map(str, c[k])) if isinstance(c.get(k), list)
                             else c.get(k)) for k in fields})
    with spath.open("w", newline="", encoding="utf-8") as f:
        fields = ["scenario_id", "side", "setup_type", "state", "entry_cluster_id",
                  "invalidation_cluster_id", "target_cluster_ids", "horizon_bars"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for p in plans:
            w.writerow({k: (";".join(map(str, p[k])) if isinstance(p.get(k), list)
                             else p.get(k)) for k in fields})
    md = [
        "# ARGUS M3 – " + str(report["symbol"]) + " / " + str(report["asof"]),
        "",
        "Research only; **not validated**, no executed trades, no hit probability.",
        "Data quality: " + str(report["data_quality"]),
        "M3 status: " + str(report["m3_status"]),
        "",
        "| Typ | Seite | Unterkante | Oberkante | Status | Familien | Unabh. Ankergruppen |",
        "|---|---|---:|---:|---|---|---:|",
    ]
    for c in clusters:
        md.append("| " + " | ".join([
            c["role"], c["side"], str(c["lower"]), str(c["upper"]),
            c["status"], ", ".join(c["families"]), str(c["independent_anchor_groups"]),
        ]) + " |")
    md += ["", "## Unausgeloeste Szenarien", ""]
    for p in plans:
        md.append("- " + p["side"] + ": entry " + p["entry_cluster_id"] +
                  "; targets " + ", ".join(p["target_cluster_ids"]) +
                  "; invalidation " + p["invalidation_cluster_id"] +
                  "; touch then later close confirmation; no cost model")
    path.with_name(path.stem + "_M3.md").write_text(
        "\n".join(md) + "\n", encoding="utf-8")
