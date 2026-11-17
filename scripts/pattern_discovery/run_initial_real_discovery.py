#!/usr/bin/env python3
"""First governed real-data Pattern Discovery run using original L1-L4 engines.

This is intentionally RESEARCH ONLY. It does not promote a pattern, confirm
a signal, or modify scanner scores / decisions. The fixed initial hypothesis
family is RS3M/Trend200/Cycle, not scanner composite score or scanner rank.
Observations are derived from recorded history and mature, adjusted OHLCV.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path

import pandas as pd

from scanner.reports.timing_patterns import Phase1BConfig, build_timing_events, _add_peer_excess
from scanner.research.pattern_discovery import (
    FeatureLibrary, build_run_manifest, write_run_manifest,
    run_discovery_search, write_search_result,
    apply_statistical_guard, write_statistical_evidence,
    OperationsRegistry, load_operations_contract,
)

CUTOFF = "2026-11-17T00:00:00+00:00"
CUTOFF_DAY = "2026-11-16"
FAMILY_FIELDS = ("rs3m", "trend200", "cycle")
RESEARCH_OBSERVATION_PATH = (
    "artifacts/research/pattern_discovery/inputs/"
    "initial_rs3m_trend_cycle_2026-11-16.json"
)


def clean_number(value):
    if value is None or pd.isna(value):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def price_window(root):
    # Do not allow future sessions into the target builder.
    history_path = root / "artifacts/research/history_analysis.csv"
    prices_path = root / "artifacts/research/price_backfill.csv"
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    if "adj_close" not in prices:
        raise ValueError("adjusted_close_required")
    if "date" not in history or "date" not in prices:
        raise ValueError("raw_date_column_required")
    history = history.loc[pd.to_datetime(history["date"], errors="coerce") <= CUTOFF_DAY].copy()
    prices = prices.loc[pd.to_datetime(prices["date"], errors="coerce") <= CUTOFF_DAY].copy()
    return history, prices


def build_observations(root):
    history, prices = price_window(root)
    events, coverage = build_timing_events(
        history, prices, Phase1BConfig(stable_start="2026-04-15", cooldown_sessions=5))
    if events.empty:
        raise ValueError("no_historical_events")
    end = pd.to_datetime(events["end_date_20t"], errors="coerce")
    observation_date = pd.to_datetime(events["obs_date"], errors="coerce")
    eligible = events.loc[(observation_date <= CUTOFF_DAY) &
                          (end <= CUTOFF_DAY) &
                          pd.to_numeric(events["return_20t"], errors="coerce").notna()].copy()
    if eligible.empty:
        raise ValueError("no_mature_twenty_session_outcomes")
    eligible = _add_peer_excess(eligible, 20)
    # Stable input order and exact identity. Duplicate observations fail closed.
    eligible = eligible.sort_values(["symbol", "obs_date"], kind="mergesort")
    rows = []
    seen = set()
    for item in eligible.to_dict("records"):
        symbol = str(item["symbol"])
        at = pd.Timestamp(item["obs_date"]).strftime("%Y-%m-%dT12:00:00+00:00")
        final = pd.Timestamp(item["end_date_20t"]).strftime("%Y-%m-%dT12:00:00+00:00")
        if final > CUTOFF or at > CUTOFF:
            raise ValueError("future_observation_or_target")
        if (symbol, at) in seen:
            raise ValueError("duplicate_pit_identity")
        seen.add((symbol, at))
        outcomes = {}
        absolute = clean_number(item.get("return_20t"))
        relative = clean_number(item.get("peer_excess_20t"))
        if absolute is not None:
            outcomes["return_20t_gt_0"] = {"value":absolute,"end_at":final}
        if relative is not None:
            outcomes["peer_excess_20t_gt_0"] = {"value":relative,"end_at":final}
        if not outcomes:
            continue
        # Versioned L2 will reject all other missing scanner fields.
        # In particular rank, score and r_code never enter this experiment.
        row = {
            "symbol":symbol,"as_of":at, "outcomes":outcomes,
            "rs3m":clean_number(item.get("rs3m")),
            "trend200":clean_number(item.get("trend200")),
            "cycle":clean_number(item.get("cycle")),
        }
        for k in ("sector", "pillar_primary", "cluster_official"):
            value = item.get(k)
            row[k] = str(value) if value is not None and not pd.isna(value) and str(value).strip() else None
        rows.append(row)
    if len(rows) < 1000 or len({x["symbol"] for x in rows}) < 20:
        raise ValueError("too_few_mature_observations")
    return rows, {
        "source_history_rows":int(len(history)),
        "source_price_rows":int(len(prices)),
        "research_rows":len(rows),
        "symbols":len({x["symbol"] for x in rows}),
        "observation_days":len({x["as_of"] for x in rows}),
        "source_history_sha256":hashlib.sha256(
            (root/"artifacts/research/history_analysis.csv").read_bytes()).hexdigest(),
        "source_price_sha256":hashlib.sha256(
            (root/"artifacts/research/price_backfill.csv").read_bytes()).hexdigest(),
        "features_used":list(FAMILY_FIELDS), "no_scanner_score_or_rank":True,
    }


def preregistration(now, code_version):
    return {
        "declared_start_at":now,
        "data_cutoff":CUTOFF,
        "data_sources":[
            RESEARCH_OBSERVATION_PATH,
            "artifacts/research/history_analysis.csv",
            "artifacts/research/price_backfill.csv",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules":[
            "no_future_features", "missing_remains_missing",
            "no_retrofit_of_modern_features",
            "mature_adjusted_price_labels_only",
            "same_date_currency_leave_one_out_peer_baseline",
        ],
        "universe_version":"observed_equities_history_through_2026-11-16",
        "feature_library_version":"PDL-FEATURE-LIBRARY-v1",
        "allowed_transformations":["change_direction","threshold_crossing"],
        "pattern_complexity":{"min_atomic_conditions":1,"max_atomic_conditions":3},
        "pattern_types":["DIRECTIONAL","RELATIVE_ALPHA"],
        "targets":["return_20t_gt_0","peer_excess_20t_gt_0"],
        "horizons_sessions":[20],
        "baselines":{
            "return_20t_gt_0":"unconditional_20_session_adjusted_return",
            "peer_excess_20t_gt_0":"same_date_currency_leave_one_out_peer_median",
        },
        "minimum_criteria":{
            "minimum_raw_n":60,
            "minimum_temporal_support_regions":3,
            "minimum_effect_size":0.005,
            "minimum_baseline_lift":0.02,
        },
        "search_budget":{
            "max_tested_candidates_total":200,
            "max_tested_candidates_per_family":100,
        },
        "candidate_budget":{
            "max_frozen_candidates_total":12,
            "max_frozen_candidates_per_horizon":12,
        },
        "multiple_testing":{
            "primary_method":"HOLM_FWER","parameters":{"family_alpha":0.05},
        },
        "statistical_primary_method":"cluster_aware_effect_and_probability_estimation",
        "robustness_checks":[
            "moving_block_bootstrap","symbol_concentration","temporal_support"],
        "exclusion_rules":[
            "missing_required_feature","missing_forward_target",
            "unreliable_adjusted_close","crypto_excluded"],
        "dependency_rules":[
            "overlapping_events_not_independent","duplicate_specs_rejected"],
        "code_version":code_version,
        "determinism":{"randomness_allowed":False,"seed":None},
    }


def exclusive_json(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    body=json.dumps(value,sort_keys=True,ensure_ascii=True,allow_nan=False,
                    separators=(",",":"))+"\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != body:
            raise ValueError("immutable_research_input_collision:"+str(path))
        return
    with path.open("x",encoding="utf-8") as f:
        f.write(body)


def run(root, dry_run=False):
    if list((root/"artifacts/research/pattern_discovery/discovery_runs").glob("*/manifest.json")):
        raise RuntimeError("initial_discovery_already_preregistered; never rerun INITIAL as fresh discovery")
    latest=root/"artifacts/research/pattern_discovery/operations/latest.json"
    pointer=json.loads(latest.read_text(encoding="utf-8"))
    cycle=json.loads((root/pointer["cycle_path"]).read_text(encoding="utf-8"))
    trigger=cycle["discovery"]["trigger"]
    if trigger.get("mode") != "INITIAL" or trigger.get("eligible") is not True:
        raise RuntimeError("L14_initial_trigger_not_eligible:"+str(trigger))
    rows, coverage=build_observations(root)
    source=root/RESEARCH_OBSERVATION_PATH
    exclusive_json(source,rows)
    now=datetime.now(timezone.utc).isoformat()
    manifest=build_run_manifest(preregistration(
        now,os.environ.get("GITHUB_SHA","manual-research-version")),
        repo_root=root)
    # L1 MUST be immutable on disk before L3 or L4 search.
    write_run_manifest(root,manifest)
    library=FeatureLibrary(root/"configs/pattern_discovery/feature_library_v1.json")
    l3=run_discovery_search(manifest,rows,repo_root=root,feature_library=library)
    write_search_result(root,l3)
    l4=apply_statistical_guard(manifest,l3,rows,repo_root=root,feature_library=library)
    write_statistical_evidence(root,l4)
    summary={
        "schema_version":"argus_initial_discovery_evidence_summary_v1",
        "asof":"2026-11-16","run_id":manifest["run_id"],
        "l1_manifest_hash":manifest["manifest_hash"],"l3_result_hash":l3["result_hash"],
        "l4_evidence_hash":l4["evidence_hash"],
        "l14_trigger_cycle_hash":cycle["cycle_hash"],
        "sources":coverage,"l3_counts":l3["candidate_counts"],
        "l4_counts":l4["counts"],
        "rejected_reasons":{},
        "l5_freeze_performed":False,
        "prospective_confirmation":False,
        "models_are_trade_signals":False,
    }
    for item in l4["candidate_evidence"]:
        for reason in item["l4_rejection_reasons"]:
            summary["rejected_reasons"][reason]=summary["rejected_reasons"].get(reason,0)+1
    # This runner DOES NOT create fake QM-C1/C2 registration or L5 freeze.
    run_path=root/"artifacts/research/pattern_discovery/discovery_runs"/manifest["run_id"]
    exclusive_json(run_path/"initial_evidence_summary.json",summary)
    if not dry_run:
        ops=load_operations_contract()
        registry=OperationsRegistry(root/ops["storage"]["registry_path"])
        classification=registry.classify_discovery_run(
            run_id=manifest["run_id"],manifest_hash=manifest["manifest_hash"],
            mode="INITIAL",trigger_cycle_hash=cycle["cycle_hash"],
            recorded_at=now,actor_id="github-actions-initial-research",
            actor_role="research_automation")
        if classification.get("valid") is not True:
            raise RuntimeError("l14_run_classification_failed")
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False))
    return summary


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--repo-root",type=Path,default=Path("."))
    p.add_argument("--dry-run",action="store_true")
    args=p.parse_args()
    run(args.repo_root.resolve(),dry_run=args.dry_run)


if __name__ == "__main__":
    main()
