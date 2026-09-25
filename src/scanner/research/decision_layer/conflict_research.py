"""Phase 7C evidence confirmation/conflict research.

7C studies relationships among already-admitted evidence without creating a
stance or portfolio action. Historical replay is discovery-only. Independent
confirmation is reserved for the prospective-unspent partition frozen by 7B.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json
from typing import Mapping

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS, _spearman
from scanner.research.decision_layer.dataset import (
    DecisionDatasetConfig,
    build_decision_research_dataset,
    validate_decision_dataset,
)
from scanner.research.decision_layer.input_contract import (
    DIRECTIONAL_FAMILIES,
    validate_input_packet,
)


SCHEMA_VERSION = "decision_conflict_research_v1"
TIMING_TOPOLOGIES = ("none", "positive_only", "negative_only", "mixed_conflict")
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "universal_stance",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "buy_signal",
    "sell_signal",
    "position_size",
    "target_weight",
    "super_score",
})


class DecisionConflictResearchError(ValueError):
    """Raised when Phase-7C research violates its frozen boundary."""


@dataclass(frozen=True)
class ConflictResearchConfig:
    discovery_partition: str = "legacy_replay_spent"
    prospective_confirmation_partition: str = "prospective_unspent"
    prospective_confirmation_start: str = "2026-09-26"
    min_group_n: int = 30
    bootstrap_reps: int = 1000
    random_seed: int = 20260925

    def __post_init__(self) -> None:
        if self.discovery_partition == self.prospective_confirmation_partition:
            raise DecisionConflictResearchError("discovery_and_confirmation_partitions_must_differ")
        if self.min_group_n < 1:
            raise DecisionConflictResearchError("min_group_n_must_be_positive")
        if self.bootstrap_reps < 0:
            raise DecisionConflictResearchError("bootstrap_reps_must_be_nonnegative")

    @classmethod
    def from_contract(cls, contract: Mapping[str, object]) -> "ConflictResearchConfig":
        if contract.get("schema_version") != SCHEMA_VERSION:
            raise DecisionConflictResearchError("unsupported_conflict_research_contract")
        if contract.get("research_only") is not True:
            raise DecisionConflictResearchError("phase7c_must_be_research_only")
        if contract.get("productive_integration_enabled") is not False:
            raise DecisionConflictResearchError("productive_integration_must_remain_disabled")
        return cls(
            discovery_partition=str(contract["discovery_partition"]),
            prospective_confirmation_partition=str(contract["prospective_confirmation_partition"]),
            prospective_confirmation_start=str(contract["prospective_confirmation_start"]),
            min_group_n=int(contract["min_group_n"]),
            bootstrap_reps=int(contract["bootstrap_reps"]),
            random_seed=int(contract["random_seed"]),
        )


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def timing_topology(positive_count: object, negative_count: object) -> str:
    try:
        positive = int(positive_count)
        negative = int(negative_count)
    except (TypeError, ValueError) as exc:
        raise DecisionConflictResearchError("invalid_timing_match_counts") from exc
    if positive < 0 or negative < 0:
        raise DecisionConflictResearchError("negative_timing_match_count")
    if positive and negative:
        return "mixed_conflict"
    if positive:
        return "positive_only"
    if negative:
        return "negative_only"
    return "none"


def attach_timing_topology(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    pos_col = f"timing_positive_match_count_{horizon}t"
    neg_col = f"timing_negative_match_count_{horizon}t"
    missing = [name for name in (pos_col, neg_col) if name not in frame.columns]
    if missing:
        raise DecisionConflictResearchError(
            f"timing_topology_columns_missing:{horizon}:" + ",".join(missing)
        )
    work = frame.copy()
    work[f"timing_topology_{horizon}t"] = [
        timing_topology(p, n) for p, n in zip(work[pos_col], work[neg_col])
    ]
    return work


def _moving_date_blocks(
    frame: pd.DataFrame,
    horizon: int,
) -> tuple[list[pd.Timestamp], list[list[pd.Timestamp]], int]:
    dates = sorted(
        pd.Timestamp(day)
        for day in pd.to_datetime(frame["obs_date"], errors="coerce").dropna().unique()
    )
    block_length = max(1, 2 * int(horizon))
    if not dates:
        return [], [], block_length
    span = min(block_length, len(dates))
    blocks = [
        [dates[(start + offset) % len(dates)] for offset in range(span)]
        for start in range(len(dates))
    ]
    return dates, blocks, block_length


def _support_region_count(
    frame: pd.DataFrame,
    dates: list[pd.Timestamp],
    block_length: int,
) -> int:
    if frame.empty or not dates:
        return 0
    positions = {day: i for i, day in enumerate(dates)}
    occurrence_positions = sorted({
        positions[pd.Timestamp(day)]
        for day in pd.to_datetime(frame["obs_date"], errors="coerce").dropna().unique()
        if pd.Timestamp(day) in positions
    })
    count = 0
    last_start: int | None = None
    for position in occurrence_positions:
        if last_start is None or position - last_start >= block_length:
            count += 1
            last_start = position
    return count


def _block_bootstrap_difference(
    left: pd.DataFrame,
    right: pd.DataFrame,
    target: str,
    horizon: int,
    reps: int,
    seed: int,
) -> dict[str, object]:
    left = left[["obs_date", target]].dropna().copy()
    right = right[["obs_date", target]].dropna().copy()
    baseline = pd.concat([left, right], ignore_index=True)
    dates, blocks, block_length = _moving_date_blocks(baseline, horizon)
    left_support = _support_region_count(left, dates, block_length)
    right_support = _support_region_count(right, dates, block_length)
    diagnostics: dict[str, object] = {
        "block_length_sessions": block_length,
        "date_count": len(dates),
        "left_temporal_support_regions": left_support,
        "right_temporal_support_regions": right_support,
        "minimum_temporal_support_regions": 2,
    }
    if (
        reps <= 0
        or len(blocks) < 2
        or left.empty
        or right.empty
        or left_support < 2
        or right_support < 2
    ):
        return {
            **diagnostics,
            "mean_difference_95": None,
            "positive_rate_difference_95": None,
            "status": "insufficient_temporal_support",
        }

    left["obs_date"] = pd.to_datetime(left["obs_date"])
    right["obs_date"] = pd.to_datetime(right["obs_date"])
    left_by_day = {
        day: left.loc[left["obs_date"].eq(day), target].to_numpy(dtype=float)
        for day in dates
    }
    right_by_day = {
        day: right.loc[right["obs_date"].eq(day), target].to_numpy(dtype=float)
        for day in dates
    }
    rng = np.random.default_rng(seed)
    draws_per_rep = int(np.ceil(len(dates) / block_length))
    mean_diffs: list[float] = []
    rate_diffs: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for index in chosen for day in blocks[index]][: len(dates)]
        left_parts = [left_by_day[day] for day in sampled_dates if len(left_by_day[day])]
        right_parts = [right_by_day[day] for day in sampled_dates if len(right_by_day[day])]
        if not left_parts or not right_parts:
            continue
        l_values = np.concatenate(left_parts)
        r_values = np.concatenate(right_parts)
        if not len(l_values) or not len(r_values):
            continue
        mean_diffs.append(float(l_values.mean() - r_values.mean()))
        rate_diffs.append(float((l_values > 0).mean() - (r_values > 0).mean()))

    def interval(values: list[float]) -> list[float] | None:
        if len(values) < 20:
            return None
        low, high = np.quantile(np.asarray(values, dtype=float), [0.025, 0.975])
        return [float(low), float(high)]

    mean_interval = interval(mean_diffs)
    rate_interval = interval(rate_diffs)
    return {
        **diagnostics,
        "mean_difference_95": mean_interval,
        "positive_rate_difference_95": rate_interval,
        "status": "available" if mean_interval is not None else "insufficient_bootstrap_draws",
    }


def _group_summary(group: pd.DataFrame, horizon: int) -> dict[str, object]:
    peer = f"peer_excess_{horizon}t"
    adverse = f"adverse_excursion_{horizon}t"
    path_dd = f"path_max_drawdown_{horizon}t"
    values = (
        pd.to_numeric(group[peer], errors="coerce")
        if peer in group.columns
        else pd.Series(dtype=float)
    )
    out: dict[str, object] = {
        "N": int(len(group)),
        "symbols": int(group["symbol"].nunique()) if "symbol" in group.columns else 0,
        "days": int(pd.to_datetime(group["obs_date"], errors="coerce").nunique()) if "obs_date" in group.columns else 0,
        "mature_peer_excess_N": int(values.notna().sum()),
        "mean_peer_excess": float(values.mean()) if values.notna().any() else None,
        "median_peer_excess": float(values.median()) if values.notna().any() else None,
        "positive_peer_excess_rate": float((values.dropna() > 0).mean()) if values.notna().any() else None,
    }
    for column, key in (
        (adverse, "mean_adverse_excursion"),
        (path_dd, "mean_path_max_drawdown"),
    ):
        if column in group.columns:
            series = pd.to_numeric(group[column], errors="coerce")
            out[key] = float(series.mean()) if series.notna().any() else None
            out[f"{key}_N"] = int(series.notna().sum())
        else:
            out[key] = None
            out[f"{key}_N"] = 0
    return out


def _candidate_status(interval: list[float] | None, expected_direction: str) -> str:
    if interval is None:
        return "insufficient_temporal_support"
    low, high = interval
    if expected_direction == "positive" and low > 0:
        return "discovery_supported"
    if expected_direction == "negative" and high < 0:
        return "discovery_supported"
    if expected_direction not in {"positive", "negative"}:
        return "diagnostic_only"
    return "discovery_inconclusive"


def _comparison(
    frame: pd.DataFrame,
    horizon: int,
    left_name: str,
    right_name: str,
    expected_direction: str,
    config: ConflictResearchConfig,
) -> dict[str, object]:
    topology = f"timing_topology_{horizon}t"
    peer = f"peer_excess_{horizon}t"
    left = frame.loc[frame[topology].eq(left_name)].dropna(subset=[peer])
    right = frame.loc[frame[topology].eq(right_name)].dropna(subset=[peer])
    result: dict[str, object] = {
        "left": left_name,
        "right": right_name,
        "expected_direction": expected_direction,
        "left_N": int(len(left)),
        "right_N": int(len(right)),
        "minimum_group_n": config.min_group_n,
        "prospective_confirmation_required": True,
    }
    if len(left) < config.min_group_n or len(right) < config.min_group_n:
        result.update({
            "mean_peer_excess_difference": None,
            "positive_rate_difference": None,
            "bootstrap": None,
            "discovery_status": "insufficient_group_size",
        })
        return result

    mean_diff = float(left[peer].mean() - right[peer].mean())
    rate_diff = float((left[peer] > 0).mean() - (right[peer] > 0).mean())
    bootstrap = _block_bootstrap_difference(
        left,
        right,
        peer,
        horizon,
        config.bootstrap_reps,
        _stable_seed(config.random_seed, horizon, left_name, right_name),
    )
    result.update({
        "mean_peer_excess_difference": mean_diff,
        "positive_rate_difference": rate_diff,
        "bootstrap": bootstrap,
        "discovery_status": _candidate_status(
            bootstrap.get("mean_difference_95") if bootstrap else None,
            expected_direction,
        ),
    })
    return result


def _selection_context(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    topology = f"timing_topology_{horizon}t"
    out: dict[str, object] = {}
    if "quality_band" not in frame.columns:
        return out
    for band in sorted(str(x) for x in frame["quality_band"].dropna().unique()):
        band_rows = frame.loc[frame["quality_band"].astype(str).eq(band)]
        out[band] = {
            state: _group_summary(band_rows.loc[band_rows[topology].eq(state)], horizon)
            for state in TIMING_TOPOLOGIES
        }
    return out


def _risk_context(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    topology = f"timing_topology_{horizon}t"
    peer = f"peer_excess_{horizon}t"
    path_dd = f"path_max_drawdown_{horizon}t"
    risk_features = (
        "aggregate_risk",
        "volatility",
        "drawdown",
        "debt_ratio",
        "liquidity_risk",
        "downside_dev",
        "beta",
    )
    out: dict[str, object] = {}
    for feature in risk_features:
        if feature not in frame.columns:
            continue
        per_topology: dict[str, object] = {}
        for state in TIMING_TOPOLOGIES:
            group = frame.loc[frame[topology].eq(state)]
            feature_values = pd.to_numeric(group[feature], errors="coerce")
            peer_values = (
                pd.to_numeric(group[peer], errors="coerce")
                if peer in group.columns else pd.Series(dtype=float)
            )
            dd_values = (
                pd.to_numeric(group[path_dd], errors="coerce")
                if path_dd in group.columns else pd.Series(dtype=float)
            )
            return_sample = pd.DataFrame({"feature": feature_values, "target": peer_values}).dropna()
            protection_sample = pd.DataFrame({"feature": feature_values, "target": dd_values}).dropna()
            per_topology[state] = {
                "return_N": int(len(return_sample)),
                "protection_N": int(len(protection_sample)),
                "spearman_risk_vs_peer_excess": (
                    _spearman(return_sample["feature"], return_sample["target"])
                    if len(return_sample) >= 3 else None
                ),
                "spearman_risk_vs_path_max_drawdown": (
                    _spearman(protection_sample["feature"], protection_sample["target"])
                    if len(protection_sample) >= 3 else None
                ),
            }
        out[feature] = per_topology
    return out


def _timing_redundancy_diagnostic(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    pos = pd.to_numeric(frame[f"timing_positive_match_count_{horizon}t"], errors="coerce").fillna(0)
    neg = pd.to_numeric(frame[f"timing_negative_match_count_{horizon}t"], errors="coerce").fillna(0)
    total = pos + neg
    matched = total > 0
    return {
        "matched_rows": int(matched.sum()),
        "mean_pattern_matches_per_matched_row": float(total.loc[matched].mean()) if matched.any() else 0.0,
        "max_pattern_matches_one_row": int(total.max()) if len(total) else 0,
        "multiple_patterns_are_independent_votes": False,
        "note": "Frozen timing patterns share atoms and discovery history; count is redundancy/context only.",
    }


def analyze_conflicts(
    dataset: pd.DataFrame,
    contract: Mapping[str, object],
    config: ConflictResearchConfig | None = None,
) -> dict[str, object]:
    """Analyze spent 7B rows without converting evidence relations into a stance."""
    cfg = config or ConflictResearchConfig.from_contract(contract)
    validate_decision_dataset(dataset, DecisionDatasetConfig())
    forbidden_columns = sorted(FORBIDDEN_OUTPUT_KEYS.intersection(dataset.columns))
    if forbidden_columns:
        raise DecisionConflictResearchError(
            "decision_columns_present_in_7c_input:" + ",".join(forbidden_columns)
        )

    spent = dataset.loc[dataset["research_partition"].eq(cfg.discovery_partition)].copy()
    if spent.empty:
        raise DecisionConflictResearchError("no_spent_discovery_rows")

    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7C",
        "research_only": True,
        "productive_integration_enabled": False,
        "discovery_partition": cfg.discovery_partition,
        "prospective_confirmation_partition": cfg.prospective_confirmation_partition,
        "prospective_confirmation_start": cfg.prospective_confirmation_start,
        "rows_used_for_discovery": int(len(spent)),
        "symbols_used_for_discovery": int(spent["symbol"].nunique()),
        "historical_scope_limit": (
            "Legacy replay can study Timing topology with Selection/Risk context, "
            "but cannot fabricate historical Probability-vNext, Confidence-vNext, "
            "Elliott-6H or typed Selection-vs-Timing claim conflicts."
        ),
        "semantics": {
            "selection_is_context_not_auto_vote": True,
            "probability_is_claim_annotation_not_vote": True,
            "confidence_is_reliability_annotation_not_vote": True,
            "risk_is_non_directional_context": True,
            "elliott_is_research_only_context": True,
            "multiple_timing_patterns_are_not_independent_votes": True,
            "mixed_timing_is_conflict_state_not_neutral": True,
            "missing_evidence_is_neutral": False,
            "universal_stance_computed": False,
            "portfolio_action_computed": False,
        },
        "horizons": {},
        "promotion_gate": {
            "status": "prospective_confirmation_required",
            "independent_confirmation_available_at_freeze": False,
            "discovery_support_may_be_called_validated": False,
        },
    }

    preregistered = {
        str(item["id"]): item
        for item in contract.get("pre_registered_comparisons", [])
        if isinstance(item, Mapping)
    }

    for horizon in HORIZONS:
        work = attach_timing_topology(spent, horizon)
        peer = f"peer_excess_{horizon}t"
        mature = work.loc[work[peer].notna()].copy()
        topology = f"timing_topology_{horizon}t"
        summaries = {
            state: _group_summary(mature.loc[mature[topology].eq(state)], horizon)
            for state in TIMING_TOPOLOGIES
        }
        comparisons: dict[str, object] = {}
        for comparison_id, item in preregistered.items():
            if int(item.get("horizon_sessions", -1)) != horizon:
                continue
            comparisons[comparison_id] = _comparison(
                mature,
                horizon,
                str(item["left"]),
                str(item["right"]),
                str(item["expected_direction"]),
                cfg,
            )

        result["horizons"][str(horizon)] = {
            "mature_rows": int(len(mature)),
            "timing_topology": summaries,
            "pre_registered_comparisons": comparisons,
            "mixed_conflict": {
                "expected_direction": None,
                "status": "diagnostic_only_no_direction_assigned",
                "summary": summaries["mixed_conflict"],
            },
            "selection_context": _selection_context(mature, horizon),
            "risk_context": _risk_context(mature, horizon),
            "timing_redundancy": _timing_redundancy_diagnostic(mature, horizon),
        }

    return result


def _explicit_direction(row: Mapping[str, object]) -> str | None:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        return None
    value = str(payload.get("direction") or "").strip().lower()
    return value if value in {"positive", "negative"} else None


def packet_relation_graph(packet: Mapping[str, object]) -> dict[str, object]:
    """Describe confirmation/conflict topology in one validated prospective packet.

    Probability and Confidence remain annotations of claims. Risk and Elliott
    remain context. No relation is converted into a stance or action.
    """
    validated = validate_input_packet(packet)
    rows = validated.get("evidence", [])
    assert isinstance(rows, list)
    directional: list[dict[str, object]] = []
    annotations: list[dict[str, object]] = []
    context: list[dict[str, object]] = []
    unknown_direction: list[str] = []

    for row in rows:
        assert isinstance(row, Mapping)
        family = str(row["family"])
        claim_id = str(row["claim_id"])
        if family in DIRECTIONAL_FAMILIES:
            direction = _explicit_direction(row)
            directional.append({"claim_id": claim_id, "family": family, "direction": direction})
            if direction is None:
                unknown_direction.append(claim_id)
        elif family in {"probability", "confidence"}:
            annotations.append({
                "claim_id": claim_id,
                "family": family,
                "claim_ref": str(row.get("claim_ref") or ""),
            })
        elif family in {"risk", "elliott"}:
            context.append({"claim_id": claim_id, "family": family})

    known = [row for row in directional if row["direction"] in {"positive", "negative"}]
    positives = [row for row in known if row["direction"] == "positive"]
    negatives = [row for row in known if row["direction"] == "negative"]

    confirmations: list[dict[str, object]] = []
    for direction, group in (("positive", positives), ("negative", negatives)):
        if len(group) < 2:
            continue
        families = sorted({str(item["family"]) for item in group})
        confirmations.append({
            "direction": direction,
            "claim_ids": [str(item["claim_id"]) for item in group],
            "families": families,
            "relation_type": (
                "cross_family_confirmation"
                if len(families) > 1
                else "same_family_correlated_confirmation"
            ),
            "independent_votes": False,
        })

    conflicts: list[dict[str, object]] = []
    if positives and negatives:
        pos_families = {str(item["family"]) for item in positives}
        neg_families = {str(item["family"]) for item in negatives}
        conflicts.append({
            "positive_claim_ids": [str(item["claim_id"]) for item in positives],
            "negative_claim_ids": [str(item["claim_id"]) for item in negatives],
            "relation_type": (
                "cross_family_conflict"
                if pos_families != neg_families or len(pos_families | neg_families) > 1
                else "within_family_conflict"
            ),
            "resolved": False,
        })

    if conflicts:
        state = "conflict_present"
    elif confirmations:
        state = "confirmation_present"
    elif known:
        state = "single_direction_or_unopposed"
    else:
        state = "insufficient_directional_relation"

    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": validated["symbol"],
        "as_of": validated["as_of"],
        "source_snapshot_id": validated["source_snapshot_id"],
        "relation_state": state,
        "directional_claims": directional,
        "unknown_direction_claim_ids": unknown_direction,
        "confirmations": confirmations,
        "conflicts": conflicts,
        "annotations": annotations,
        "context": context,
        "probability_and_confidence_count_as_votes": False,
        "risk_and_elliott_count_as_votes": False,
        "missing_direction_is_neutral": False,
        "conflict_resolved": False,
        "universal_stance_computed": False,
        "portfolio_action_computed": False,
    }


def validate_conflict_report(report: Mapping[str, object]) -> None:
    if report.get("schema_version") != SCHEMA_VERSION:
        raise DecisionConflictResearchError("invalid_7c_report_schema")
    if report.get("research_only") is not True:
        raise DecisionConflictResearchError("7c_report_not_research_only")
    if report.get("productive_integration_enabled") is not False:
        raise DecisionConflictResearchError("7c_productive_integration_enabled")
    semantics = report.get("semantics")
    if not isinstance(semantics, Mapping):
        raise DecisionConflictResearchError("7c_semantics_missing")
    if semantics.get("universal_stance_computed") is not False:
        raise DecisionConflictResearchError("7c_must_not_compute_stance")
    if semantics.get("portfolio_action_computed") is not False:
        raise DecisionConflictResearchError("7c_must_not_compute_portfolio_action")


def run_conflict_research(
    history_path: str | Path,
    prices_path: str | Path,
    timing_catalog_path: str | Path,
    dataset_contract_path: str | Path,
    conflict_contract_path: str | Path,
    output_path: str | Path,
) -> dict[str, object]:
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    timing_catalog = json.loads(Path(timing_catalog_path).read_text(encoding="utf-8"))
    dataset_contract = json.loads(Path(dataset_contract_path).read_text(encoding="utf-8"))
    conflict_contract = json.loads(Path(conflict_contract_path).read_text(encoding="utf-8"))
    dataset_config = DecisionDatasetConfig.from_contract(dataset_contract)
    dataset, dataset_metadata = build_decision_research_dataset(
        history, prices, timing_catalog, dataset_config
    )
    report = analyze_conflicts(
        dataset,
        conflict_contract,
        ConflictResearchConfig.from_contract(conflict_contract),
    )
    report["dataset_metadata"] = {
        "schema_version": dataset_metadata.get("schema_version"),
        "rows": dataset_metadata.get("rows"),
        "symbols": dataset_metadata.get("symbols"),
        "date_min": dataset_metadata.get("date_min"),
        "date_max": dataset_metadata.get("date_max"),
        "partitions": dataset_metadata.get("partitions"),
    }
    validate_conflict_report(report)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return report
