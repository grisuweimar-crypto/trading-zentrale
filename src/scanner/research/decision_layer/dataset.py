"""Phase 7B point-in-time Decision Research Dataset.

The dataset is deliberately upstream of evidence fusion. Historical scanner
states are replayed only from information actually present at the observation
date. Forward returns and path-risk measures are labels with explicit maturity
dates, never features. Current Phase-2/3/5/6 knowledge is not retrojected.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
import json
from typing import Mapping

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    HORIZONS,
    Phase1AConfig,
    _r_score_backbone,
    build_events,
)
from scanner.reports.timing_patterns import (
    Phase1BConfig,
    _add_peer_excess,
    feature_rows,
)
from scanner.reports.risk_vnext import (
    RISK_FEATURE_ALIASES,
    Phase3Config,
    _attach_future_path_risk,
    risk_feature_rows,
)


DATASET_SCHEMA_VERSION = "decision_research_dataset_v1"
FORBIDDEN_DATASET_COLUMNS = frozenset({
    "universal_stance",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "buy_signal",
    "sell_signal",
    "position_size",
    "target_weight",
})


class DecisionDatasetError(ValueError):
    """Raised when the Phase-7B dataset violates its frozen research contract."""


@dataclass(frozen=True)
class DecisionDatasetConfig:
    stable_start: str = "2026-04-15"
    legacy_replay_spent_through: str = "2026-09-25"
    prospective_unspent_start: str = "2026-09-26"
    timing_catalog_available_from: str = "2026-09-23"

    def __post_init__(self) -> None:
        stable = pd.Timestamp(self.stable_start)
        spent = pd.Timestamp(self.legacy_replay_spent_through)
        prospective = pd.Timestamp(self.prospective_unspent_start)
        timing = pd.Timestamp(self.timing_catalog_available_from)
        if stable > spent:
            raise DecisionDatasetError("stable_start_after_spent_cutoff")
        if prospective <= spent:
            raise DecisionDatasetError("prospective_start_must_follow_spent_cutoff")
        if timing < stable:
            raise DecisionDatasetError("timing_catalog_before_stable_start")

    @classmethod
    def from_contract(cls, contract: Mapping[str, object]) -> "DecisionDatasetConfig":
        if contract.get("schema_version") != DATASET_SCHEMA_VERSION:
            raise DecisionDatasetError("unsupported_dataset_contract")
        if contract.get("research_only") is not True:
            raise DecisionDatasetError("dataset_contract_must_be_research_only")
        if contract.get("productive_integration_enabled") is not False:
            raise DecisionDatasetError("productive_integration_must_remain_disabled")
        return cls(
            stable_start=str(contract["stable_start"]),
            legacy_replay_spent_through=str(contract["legacy_replay_spent_through"]),
            prospective_unspent_start=str(contract["prospective_unspent_start"]),
            timing_catalog_available_from=str(contract["timing_catalog_available_from"]),
        )


def _first_present(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _json_compact(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _iso_day(value: object) -> str | None:
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        return None
    return pd.Timestamp(stamp).date().isoformat()


def _partition(obs_date: pd.Timestamp, config: DecisionDatasetConfig) -> str:
    spent = pd.Timestamp(config.legacy_replay_spent_through)
    prospective = pd.Timestamp(config.prospective_unspent_start)
    if obs_date <= spent:
        return "legacy_replay_spent"
    if obs_date >= prospective:
        return "prospective_unspent"
    return "embargo_gap"


def _validate_timing_catalog(catalog: Mapping[str, object]) -> None:
    if catalog.get("schema_version") != "phase1b_frozen_patterns_v1":
        raise DecisionDatasetError("unsupported_timing_catalog_schema")
    horizons = catalog.get("horizons")
    if not isinstance(horizons, Mapping):
        raise DecisionDatasetError("timing_catalog_horizons_missing")
    for horizon in HORIZONS:
        block = horizons.get(str(horizon))
        if not isinstance(block, Mapping):
            raise DecisionDatasetError(f"timing_catalog_horizon_missing:{horizon}")
        patterns = block.get("frozen_patterns")
        if not isinstance(patterns, list):
            raise DecisionDatasetError(f"timing_catalog_patterns_missing:{horizon}")
        for item in patterns:
            if not isinstance(item, Mapping):
                raise DecisionDatasetError("invalid_timing_pattern")
            if not str(item.get("pattern") or "").strip():
                raise DecisionDatasetError("timing_pattern_name_missing")
            if item.get("discovery_direction") not in {"positive", "negative"}:
                raise DecisionDatasetError("invalid_timing_pattern_direction")
            conditions = item.get("conditions")
            if not isinstance(conditions, list) or not conditions:
                raise DecisionDatasetError("timing_pattern_conditions_missing")


def _attach_timing_matches(
    frame: pd.DataFrame,
    catalog: Mapping[str, object],
    config: DecisionDatasetConfig,
) -> pd.DataFrame:
    result = frame.copy()
    available_from = pd.Timestamp(config.timing_catalog_available_from)
    result["timing_catalog_status"] = np.where(
        pd.to_datetime(result["obs_date"]) >= available_from,
        "available_as_of_observation",
        "retrospective_replay_spent",
    )

    horizons = catalog["horizons"]
    assert isinstance(horizons, Mapping)
    for horizon in HORIZONS:
        block = horizons[str(horizon)]
        assert isinstance(block, Mapping)
        patterns = block["frozen_patterns"]
        assert isinstance(patterns, list)
        matches: list[str] = []
        counts: list[int] = []
        positive_counts: list[int] = []
        negative_counts: list[int] = []
        for _, row in result.iterrows():
            row_matches: list[dict[str, object]] = []
            for item in patterns:
                assert isinstance(item, Mapping)
                conditions = [str(x) for x in item["conditions"]]
                missing = [condition for condition in conditions if condition not in result.columns]
                if missing:
                    raise DecisionDatasetError(
                        f"timing_condition_columns_missing:{horizon}:" + ",".join(sorted(missing))
                    )
                if all(bool(row.get(condition, False)) for condition in conditions):
                    pattern = str(item["pattern"])
                    pattern_id = sha256(f"{horizon}|{pattern}".encode("utf-8")).hexdigest()[:16]
                    row_matches.append({
                        "pattern_id": pattern_id,
                        "pattern": pattern,
                        "direction": str(item["discovery_direction"]),
                        "horizon_sessions": horizon,
                    })
            matches.append(_json_compact(row_matches))
            counts.append(len(row_matches))
            positive_counts.append(sum(item["direction"] == "positive" for item in row_matches))
            negative_counts.append(sum(item["direction"] == "negative" for item in row_matches))
        result[f"timing_matches_{horizon}t"] = matches
        result[f"timing_match_count_{horizon}t"] = counts
        result[f"timing_positive_match_count_{horizon}t"] = positive_counts
        result[f"timing_negative_match_count_{horizon}t"] = negative_counts
        result[f"probability_status_{horizon}t"] = "not_retrojected_not_historically_archived_as_typed_claim"
    return result


def _attach_selection_and_legacy_state(
    events: pd.DataFrame,
    timing_features: pd.DataFrame,
) -> pd.DataFrame:
    extra_candidates = [
        "score_status",
        "trend_ok",
        "liquidity_ok",
        "confidence",
        "confidence_label",
        "opportunity",
        "risk",
        "rs3m",
        "trend200",
        "cycle",
    ]
    atoms = [
        column for column in timing_features.columns
        if column.endswith(("_up", "_down")) or column.startswith("cycle_cross_")
    ]
    merge_cols = ["date", "symbol"] + [
        column for column in extra_candidates + atoms
        if column in timing_features.columns
        and column not in {"score", "score_pct_full", "r_code", "currency"}
    ]
    merge_cols = list(dict.fromkeys(merge_cols))
    merged = events.merge(
        timing_features[merge_cols],
        left_on=["obs_date", "symbol"],
        right_on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    ).drop(columns=["date"], errors="ignore")

    merged["quality_band"] = [
        _r_score_backbone(float(score), float(percentile))
        for score, percentile in zip(merged["score"], merged["score_pct_full"])
    ]
    merged["selection_evidence_status"] = "observed_point_in_time"
    merged["legacy_confidence"] = pd.to_numeric(
        merged.get("confidence", pd.Series(np.nan, index=merged.index)), errors="coerce"
    )
    if "confidence_label" in merged.columns:
        merged["legacy_confidence_label"] = merged["confidence_label"].astype("string")
    else:
        merged["legacy_confidence_label"] = pd.Series(pd.NA, index=merged.index, dtype="string")
    merged["confidence_vnext_status"] = "not_retrojected_legacy_confidence_is_diagnostic_only"
    merged["elliott_6h_status"] = "not_retrojected_not_historically_available_as_6h"
    return merged.drop(columns=["confidence", "confidence_label"], errors="ignore")


def _attach_risk_state(
    frame: pd.DataFrame,
    risk_features: pd.DataFrame,
) -> pd.DataFrame:
    merge_cols = ["date", "symbol", *RISK_FEATURE_ALIASES.keys()]
    available = [column for column in merge_cols if column in risk_features.columns]
    merged = frame.merge(
        risk_features[available],
        left_on=["obs_date", "symbol"],
        right_on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    ).drop(columns=["date"], errors="ignore")
    risk_columns = [name for name in RISK_FEATURE_ALIASES if name in merged.columns]
    merged["risk_observed_component_count"] = merged[risk_columns].notna().sum(axis=1)
    merged["risk_evidence_status"] = np.where(
        merged["risk_observed_component_count"] > 0,
        "raw_point_in_time_state_only",
        "unavailable",
    )
    return merged


def _attach_outcome_labels(frame: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    result = _attach_future_path_risk(frame, prices)
    for horizon in HORIZONS:
        result = _add_peer_excess(result, horizon)
        end_col = f"end_date_{horizon}t"
        return_col = f"return_{horizon}t"
        peer_col = f"peer_excess_{horizon}t"
        result[f"label_available_from_{horizon}t"] = result[end_col].map(_iso_day)
        mature = result[return_col].notna() & result[peer_col].notna()
        result[f"label_mature_{horizon}t"] = mature
        peer = pd.to_numeric(result[peer_col], errors="coerce")
        result[f"peer_direction_{horizon}t"] = np.where(
            peer.isna(), np.nan, np.where(peer > 0, 1.0, np.where(peer < 0, -1.0, 0.0))
        )
    return result


def validate_decision_dataset(
    frame: pd.DataFrame,
    config: DecisionDatasetConfig = DecisionDatasetConfig(),
) -> None:
    forbidden = sorted(FORBIDDEN_DATASET_COLUMNS.intersection(frame.columns))
    if forbidden:
        raise DecisionDatasetError("forbidden_decision_columns:" + ",".join(forbidden))
    required = {"symbol", "obs_date", "start_market_date", "research_partition", "quality_band"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise DecisionDatasetError("dataset_required_columns_missing:" + ",".join(missing))
    if frame.duplicated(["symbol", "obs_date", "start_market_date"]).any():
        raise DecisionDatasetError("duplicate_dataset_row_identity")

    obs = pd.to_datetime(frame["obs_date"], errors="coerce")
    if obs.isna().any():
        raise DecisionDatasetError("invalid_obs_date")
    if (obs < pd.Timestamp(config.stable_start)).any():
        raise DecisionDatasetError("row_before_stable_start")

    for horizon in HORIZONS:
        label_col = f"label_available_from_{horizon}t"
        if label_col not in frame.columns:
            raise DecisionDatasetError(f"label_availability_missing:{horizon}")
        available = pd.to_datetime(frame[label_col], errors="coerce")
        invalid = available.notna() & (available < obs)
        if invalid.any():
            raise DecisionDatasetError(f"future_label_available_before_observation:{horizon}")

    spent = pd.Timestamp(config.legacy_replay_spent_through)
    prospective = pd.Timestamp(config.prospective_unspent_start)
    expected = [
        "legacy_replay_spent" if day <= spent else "prospective_unspent" if day >= prospective else "embargo_gap"
        for day in obs
    ]
    if list(frame["research_partition"].astype(str)) != expected:
        raise DecisionDatasetError("research_partition_mismatch")


def build_decision_research_dataset(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    timing_catalog: Mapping[str, object],
    config: DecisionDatasetConfig = DecisionDatasetConfig(),
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Build the Phase-7B stock research dataset without retrojected evidence."""
    _validate_timing_catalog(timing_catalog)
    events = build_events(history, prices, Phase1AConfig(stable_start=config.stable_start))
    if events.empty:
        raise DecisionDatasetError("no_research_events")

    timing_features, timing_coverage = feature_rows(
        history, Phase1BConfig(stable_start=config.stable_start)
    )
    risk_features, risk_coverage = risk_feature_rows(
        history, Phase3Config(stable_start=config.stable_start, cluster_bootstrap_reps=0)
    )

    work = _attach_selection_and_legacy_state(events, timing_features)
    work = _attach_risk_state(work, risk_features)
    work = _attach_timing_matches(work, timing_catalog, config)
    work = _attach_outcome_labels(work, prices)
    work["obs_date"] = pd.to_datetime(work["obs_date"]).dt.normalize()
    work["start_market_date"] = pd.to_datetime(work["start_market_date"]).dt.normalize()
    work["research_partition"] = [
        _partition(pd.Timestamp(day), config) for day in work["obs_date"]
    ]
    work["dataset_schema_version"] = DATASET_SCHEMA_VERSION
    work["feature_time_rule"] = "point_in_time_only"
    work["future_labels_are_features"] = False
    work["crypto_in_scope"] = False

    work = work.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)
    validate_decision_dataset(work, config)

    metadata: dict[str, object] = {
        "schema_version": DATASET_SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "rows": int(len(work)),
        "symbols": int(work["symbol"].nunique()),
        "date_min": _iso_day(work["obs_date"].min()),
        "date_max": _iso_day(work["obs_date"].max()),
        "partitions": {str(k): int(v) for k, v in work["research_partition"].value_counts().items()},
        "horizons": {
            str(horizon): {
                "mature_peer_labels": int(work[f"label_mature_{horizon}t"].sum()),
                "timing_matches": int(work[f"timing_match_count_{horizon}t"].sum()),
                "rows_with_timing_match": int((work[f"timing_match_count_{horizon}t"] > 0).sum()),
            }
            for horizon in HORIZONS
        },
        "family_modes": {
            "selection": "point_in_time_replay",
            "timing": "frozen_catalog_replay_with_availability_marker",
            "probability": "not_retrojected",
            "risk": "raw_point_in_time_state_only",
            "confidence": "legacy_diagnostic_only_not_vnext",
            "elliott": "not_retrojected_requires_genuine_6h_packet",
        },
        "timing_feature_coverage": timing_coverage,
        "risk_feature_coverage": risk_coverage,
        "guards": {
            "future_labels_are_not_features": True,
            "missing_evidence_stays_missing": True,
            "current_validation_knowledge_retrojected": False,
            "overlapping_forward_windows_are_iid": False,
            "universal_stance_computed": False,
            "portfolio_action_computed": False,
            "crypto_deferred": True,
        },
    }
    return work, metadata


def _sha256_path(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_dataset_build(
    history_path: str | Path,
    prices_path: str | Path,
    timing_catalog_path: str | Path,
    output_path: str | Path,
    metadata_path: str | Path,
    config: DecisionDatasetConfig = DecisionDatasetConfig(),
) -> dict[str, object]:
    history_path = Path(history_path)
    prices_path = Path(prices_path)
    timing_catalog_path = Path(timing_catalog_path)
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    timing_catalog = json.loads(timing_catalog_path.read_text(encoding="utf-8"))
    frame, metadata = build_decision_research_dataset(history, prices, timing_catalog, config)

    output_path = Path(output_path)
    metadata_path = Path(metadata_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    serial = frame.copy()
    for column in ["obs_date", "start_market_date", *[f"end_date_{h}t" for h in HORIZONS]]:
        if column in serial.columns:
            serial[column] = pd.to_datetime(serial[column], errors="coerce").dt.strftime("%Y-%m-%d")
    serial.to_csv(output_path, index=False)

    metadata["generated_on"] = date.today().isoformat()
    metadata["inputs"] = {
        "history": {"path": str(history_path), "sha256": _sha256_path(history_path)},
        "prices": {"path": str(prices_path), "sha256": _sha256_path(prices_path)},
        "timing_catalog": {"path": str(timing_catalog_path), "sha256": _sha256_path(timing_catalog_path)},
    }
    metadata["output"] = {"path": str(output_path), "sha256": _sha256_path(output_path)}
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return metadata
