from __future__ import annotations

"""Phase 4B-D: empirical Confidence-vNext research registry.

Research-only. This module creates ordinal evidence states for Data Quality,
Statistical Confidence and Model Agreement. It deliberately does NOT create a
new 0-100 confidence score, HIGH/MED/LOW thresholds, or production behaviour.

The registry consumes already-frozen/validated Phase 1-3 research definitions.
Probability calibrates Selection/Timing evidence; it is not an independent
model vote. Risk is a downside-protection model; low risk never counts as a
positive return vote. Missing or immature evidence fails closed as unknown.
"""

from dataclasses import asdict, dataclass
from pathlib import Path
import json
import re

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS, _r_score_backbone, _scanner_rows
from scanner.reports.timing_patterns import Phase1BConfig, feature_rows


EVIDENCE_STATES = ("robust", "directional_only", "immature", "mixed", "unavailable")
PROVENANCE_FIELDS = ("run_id", "as_of", "snapshot_id", "data_source", "observation_type")
DELTA_ATOM_RE = re.compile(r"^(score|opportunity|risk|rs3m|trend200|cycle)_d(1|5|10)_(up|down)$")


@dataclass(frozen=True)
class Phase4ResearchConfig:
    evidence_version: str = "phase4_confidence_research_v1"
    stable_start: str = "2026-04-15"
    require_provenance_fields: tuple[str, ...] = PROVENANCE_FIELDS


def _present(value: object) -> bool:
    if value is None or value is pd.NA:
        return False
    if isinstance(value, float) and np.isnan(value):
        return False
    text = str(value).strip().lower()
    return text not in {"", "nan", "none", "null", "<na>"}


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _direction(value: object) -> str | None:
    number = _number(value)
    if number is None or number == 0:
        return None
    return "positive" if number > 0 else "negative"


def _interval_direction(interval: object) -> str | None:
    if not isinstance(interval, (list, tuple)) or len(interval) != 2:
        return None
    low = _number(interval[0])
    high = _number(interval[1])
    if low is None or high is None:
        return None
    if low > 0:
        return "positive"
    if high < 0:
        return "negative"
    return None


def _horizon_available(horizon_report: dict) -> bool:
    maturity = horizon_report.get("validation_maturity") or {}
    return maturity.get("status") == "available" and int(maturity.get("mature_target_events") or 0) > 0


def selection_evidence(phase2: dict, horizon: int, band: str) -> dict[str, object]:
    horizon_report = (phase2.get("horizons") or {}).get(str(horizon), {})
    if not _horizon_available(horizon_report):
        return {"state": "immature", "direction": None, "reason": "horizon_not_mature"}
    stats = (((horizon_report.get("selection") or {}).get("validation") or {}).get(band))
    if not isinstance(stats, dict):
        return {"state": "unavailable", "direction": None, "reason": "selection_band_not_available"}

    alpha_ci = stats.get("block_bootstrap_mean_peer_excess_95")
    probability_ci = stats.get("block_bootstrap_probability_advantage_95")
    alpha_ci_direction = _interval_direction(alpha_ci)
    probability_ci_direction = _interval_direction(probability_ci)
    point_alpha_direction = _direction(stats.get("mean_peer_excess"))
    point_probability_direction = _direction(stats.get("probability_advantage_vs_baseline"))
    support_regions = int(stats.get("bootstrap_occurrence_support_region_count") or 0)

    if alpha_ci is None or probability_ci is None or support_regions < 2:
        state = "immature"
        direction = None
        reason = "robust_uncertainty_or_temporal_support_unavailable"
    elif alpha_ci_direction and alpha_ci_direction == probability_ci_direction:
        state = "robust"
        direction = alpha_ci_direction
        reason = "alpha_and_probability_intervals_support_same_direction"
    elif point_alpha_direction and point_alpha_direction == point_probability_direction:
        state = "directional_only"
        direction = point_alpha_direction
        reason = "point_directions_agree_but_robust_intervals_do_not"
    else:
        state = "mixed"
        direction = None
        reason = "alpha_and_probability_evidence_mixed"

    return {
        "state": state,
        "direction": direction,
        "reason": reason,
        "N": int(stats.get("N") or 0),
        "days": int(stats.get("days") or 0),
        "symbols": int(stats.get("symbols") or 0),
        "top_symbol_share": stats.get("top_symbol_share"),
        "support_regions": support_regions,
        "mean_peer_excess": stats.get("mean_peer_excess"),
        "probability_advantage_vs_baseline": stats.get("probability_advantage_vs_baseline"),
        "alpha_interval_95": alpha_ci,
        "probability_advantage_interval_95": probability_ci,
    }


def timing_evidence(pattern: dict, horizon_report: dict) -> dict[str, object]:
    if not _horizon_available(horizon_report):
        return {"state": "immature", "direction": None, "reason": "horizon_not_mature"}
    validation = pattern.get("validation")
    if not isinstance(validation, dict):
        return {"state": "unavailable", "direction": None, "reason": "validation_not_available"}
    if not pattern.get("validation_sufficient"):
        return {"state": "immature", "direction": None, "reason": "validation_sample_insufficient"}

    alpha_ci = validation.get("block_bootstrap_mean_peer_excess_95")
    probability_ci = validation.get("block_bootstrap_probability_advantage_95")
    support_regions = int(validation.get("bootstrap_occurrence_support_region_count") or 0)
    discovery_direction = pattern.get("discovery_direction")

    if pattern.get("strong_validation"):
        state = "robust"
        direction = discovery_direction
        reason = "phase2_strong_validation"
    elif alpha_ci is None or probability_ci is None or support_regions < 2:
        state = "immature"
        direction = None
        reason = "robust_uncertainty_or_temporal_support_unavailable"
    elif pattern.get("joint_direction_confirmed"):
        state = "directional_only"
        direction = discovery_direction
        reason = "joint_point_direction_without_two_robust_intervals"
    else:
        state = "mixed"
        direction = None
        reason = "validation_direction_mixed"

    return {
        "state": state,
        "direction": direction,
        "reason": reason,
        "pattern": pattern.get("pattern"),
        "conditions": list(pattern.get("conditions") or []),
        "N": int(validation.get("N") or 0),
        "days": int(validation.get("days") or 0),
        "symbols": int(validation.get("symbols") or 0),
        "top_symbol_share": validation.get("top_symbol_share"),
        "support_regions": support_regions,
        "mean_peer_excess": validation.get("mean_peer_excess"),
        "probability_advantage_vs_baseline": validation.get("probability_advantage_vs_baseline"),
        "alpha_interval_95": alpha_ci,
        "probability_advantage_interval_95": probability_ci,
    }


def risk_evidence(risk_report: dict, horizon: int, feature: str) -> dict[str, object]:
    horizon_report = (risk_report.get("horizons") or {}).get(str(horizon), {})
    maturity = horizon_report.get("validation_maturity") or {}
    if maturity.get("status") != "available" or int(maturity.get("protection_mature_events") or 0) <= 0:
        return {"state": "immature", "direction": None, "reason": "protection_horizon_not_mature"}
    stats = ((horizon_report.get("validation") or {}).get(feature))
    if not isinstance(stats, dict) or int(stats.get("protection_N") or 0) == 0:
        return {"state": "unavailable", "direction": None, "reason": "risk_feature_not_available"}

    adverse_ci = stats.get("protection_gap_bootstrap_95")
    path_ci = stats.get("path_drawdown_gap_bootstrap_95")
    adverse_direction = _interval_direction(adverse_ci)
    path_direction = _interval_direction(path_ci)
    adverse_gap = _number(stats.get("protection_gap_high_minus_low_adverse_excursion"))
    path_gap = _number(stats.get("path_drawdown_gap_high_minus_low"))

    if adverse_ci is None or path_ci is None:
        state = "immature"
        direction = None
        reason = "robust_protection_intervals_unavailable"
    elif adverse_direction == "positive" and path_direction == "positive":
        state = "robust"
        direction = "higher_is_riskier"
        reason = "both_protection_intervals_confirm_higher_downside_risk"
    elif adverse_gap is not None and path_gap is not None and adverse_gap > 0 and path_gap > 0:
        state = "directional_only"
        direction = "higher_is_riskier"
        reason = "protection_point_estimates_agree_but_intervals_not_robust"
    else:
        state = "mixed"
        direction = None
        reason = "protection_evidence_mixed"

    return {
        "state": state,
        "direction": direction,
        "reason": reason,
        "feature": feature,
        "N": int(stats.get("protection_N") or 0),
        "days": int(stats.get("protection_days") or 0),
        "low_cutoff": stats.get("protection_low_risk_cutoff"),
        "high_cutoff": stats.get("protection_high_risk_cutoff"),
        "adverse_gap_high_minus_low": stats.get("protection_gap_high_minus_low_adverse_excursion"),
        "path_drawdown_gap_high_minus_low": stats.get("path_drawdown_gap_high_minus_low"),
        "adverse_gap_interval_95": adverse_ci,
        "path_drawdown_gap_interval_95": path_ci,
        "return_alpha_effect_kept_separate": True,
    }


def build_statistical_registry(phase2: dict, risk_report: dict) -> dict[str, object]:
    registry: dict[str, object] = {"horizons": {}}
    for horizon in HORIZONS:
        h2 = (phase2.get("horizons") or {}).get(str(horizon), {})
        selection_bands = (((h2.get("selection") or {}).get("validation") or {}).keys())
        selection = {band: selection_evidence(phase2, horizon, str(band)) for band in selection_bands}

        timing_rows = ((h2.get("timing_patterns") or {}).get("patterns") or [])
        timing = {
            str(row.get("pattern")): timing_evidence(row, h2)
            for row in timing_rows
            if row.get("pattern")
        }

        h3 = (risk_report.get("horizons") or {}).get(str(horizon), {})
        risk_features = (h3.get("validation") or {}).keys()
        risk = {feature: risk_evidence(risk_report, horizon, str(feature)) for feature in risk_features}

        registry["horizons"][str(horizon)] = {
            "selection": selection,
            "timing": timing,
            "risk": risk,
            "probability_role": "calibration_of_selection_and_timing_not_independent_vote",
            "regime_role": "unknown_unvalidated_not_a_vote",
        }
    return registry


def _atom_prerequisites(condition: str) -> tuple[str, ...]:
    match = DELTA_ATOM_RE.match(condition)
    if match:
        return (f"{match.group(1)}_d{match.group(2)}",)
    if condition in {"trend_cross_up", "trend_cross_down"}:
        return ("trend200", "trend_prev1")
    if condition.startswith("cycle_cross_up_") or condition.startswith("cycle_cross_down_"):
        return ("cycle", "cycle_prev1")
    if condition in {"r_upgrade", "r_downgrade"}:
        return ("r_d1",)
    if condition in {"r_high", "r_low"}:
        return ("r_num",)
    if condition in {"elliott_to_buy", "elliott_to_sell"}:
        return ("elliott_state", "elliott_prev1")
    if condition in {"elliott_buy", "elliott_sell"}:
        return ("elliott_state",)
    return (condition,)


def _provenance_quality(row: pd.Series, required: tuple[str, ...]) -> dict[str, object]:
    present = [field for field in required if field in row.index and _present(row.get(field))]
    return {
        "required": list(required),
        "present": present,
        "missing": [field for field in required if field not in present],
        "complete": len(present) == len(required),
    }


def _data_quality_state(required_fields: list[str], row: pd.Series, provenance: dict[str, object]) -> dict[str, object]:
    present = [field for field in required_fields if field in row.index and _present(row.get(field))]
    missing = [field for field in required_fields if field not in present]
    total = len(required_fields)
    if not provenance["complete"] or not present:
        state = "proxy_insufficient"
    elif not missing:
        state = "proxy_complete"
    else:
        state = "proxy_partial"
    return {
        "state": state,
        "required_fields": required_fields,
        "present_fields": present,
        "missing_fields": missing,
        "present_fraction": float(len(present) / total) if total else None,
        "provenance": provenance,
        "full_data_quality_claimed": False,
        "note": "Current presence/provenance proxy only; historical per-factor freshness/source/fetch quality is not archived.",
    }


def _latest_feature_rows(history: pd.DataFrame, latest_date: pd.Timestamp, config: Phase4ResearchConfig) -> pd.DataFrame:
    features, _ = feature_rows(history, Phase1BConfig(stable_start=config.stable_start))
    if features.empty:
        return features
    return features.loc[pd.to_datetime(features["date"]).dt.normalize().eq(latest_date.normalize())].copy()


def _matched_robust_timing(
    feature_row: pd.Series,
    timing_registry: dict[str, dict[str, object]],
) -> tuple[list[dict[str, object]], list[str]]:
    matches: list[dict[str, object]] = []
    unevaluable: list[str] = []
    for pattern_name, evidence in timing_registry.items():
        if evidence.get("state") != "robust":
            continue
        conditions = list(evidence.get("conditions") or [])
        prerequisites = sorted({p for condition in conditions for p in _atom_prerequisites(condition)})
        if any(p not in feature_row.index or not _present(feature_row.get(p)) for p in prerequisites):
            unevaluable.append(pattern_name)
            continue
        if all(bool(feature_row.get(condition, False)) for condition in conditions):
            matches.append(evidence)
    return matches, unevaluable


def _timing_model_state(matches: list[dict[str, object]]) -> dict[str, object]:
    if not matches:
        return {"state": "unknown", "direction": None, "matched_patterns": []}
    directions = {row.get("direction") for row in matches if row.get("direction") in {"positive", "negative"}}
    if len(directions) == 1:
        return {
            "state": "robust_claim",
            "direction": next(iter(directions)),
            "matched_patterns": [row.get("pattern") for row in matches],
        }
    return {
        "state": "internal_conflict",
        "direction": None,
        "matched_patterns": [row.get("pattern") for row in matches],
    }


def _risk_model_state(row: pd.Series, risk_registry: dict[str, dict[str, object]]) -> dict[str, object]:
    observed: list[dict[str, object]] = []
    for feature, evidence in risk_registry.items():
        if evidence.get("state") != "robust":
            continue
        value = _number(row.get(feature))
        low = _number(evidence.get("low_cutoff"))
        high = _number(evidence.get("high_cutoff"))
        if value is None or low is None or high is None:
            continue
        level = "high" if value >= high else "low" if value <= low else "middle"
        observed.append({"feature": feature, "value": value, "level": level, "low_cutoff": low, "high_cutoff": high})

    levels = {item["level"] for item in observed}
    if not observed:
        state = "unknown"
    elif "high" in levels and "low" in levels:
        state = "mixed"
    elif "high" in levels:
        state = "elevated"
    elif levels == {"low"}:
        state = "low"
    else:
        state = "middle"
    return {"state": state, "features": observed, "role": "downside_protection_only_not_return_vote"}


def _agreement_state(selection: dict[str, object], timing: dict[str, object], risk: dict[str, object]) -> dict[str, object]:
    conflicts: list[str] = []
    return_directions: list[str] = []
    if selection.get("state") == "robust":
        if selection.get("direction") in {"positive", "negative"}:
            return_directions.append(str(selection["direction"]))
    if timing.get("state") == "robust_claim":
        if timing.get("direction") in {"positive", "negative"}:
            return_directions.append(str(timing["direction"]))
    elif timing.get("state") == "internal_conflict":
        conflicts.append("timing_internal_conflict")

    if len(set(return_directions)) > 1:
        conflicts.append("selection_timing_return_conflict")
    positive_claim = "positive" in return_directions
    if positive_claim and risk.get("state") == "elevated":
        conflicts.append("positive_return_claim_vs_elevated_downside_risk")

    mature_return_models = len(return_directions)
    if conflicts:
        state = "conflict"
    elif mature_return_models >= 2:
        state = "compatible"
    elif mature_return_models == 1:
        state = "single_model"
    elif risk.get("state") == "elevated":
        state = "risk_only"
    else:
        state = "insufficient_evidence"

    return {
        "state": state,
        "mature_return_models": mature_return_models,
        "conflicts": conflicts,
        "probability_counted_as_independent_model": False,
        "low_risk_counted_as_positive_return_support": False,
        "regime_counted_as_model_vote": False,
    }


def _assert_pit_sources(latest: pd.DataFrame, phase2: dict, risk_report: dict) -> dict[str, object]:
    if latest.empty:
        raise ValueError("latest scanner is empty")
    current_as_of = pd.to_datetime(latest.get("as_of", latest["date"]), errors="coerce").max()
    if pd.isna(current_as_of):
        current_as_of = pd.to_datetime(latest["date"], errors="coerce").max()
    checks: dict[str, object] = {"current_as_of": current_as_of.isoformat() if not pd.isna(current_as_of) else None}
    for label, report in (("phase2", phase2), ("phase3", risk_report)):
        source_as_of = (report.get("source") or {}).get("as_of")
        parsed = pd.to_datetime(source_as_of, errors="coerce") if source_as_of else pd.NaT
        if not pd.isna(parsed) and not pd.isna(current_as_of) and parsed > current_as_of:
            raise ValueError(f"PIT violation: {label} evidence as_of {parsed} is after current scanner {current_as_of}")
        checks[label] = {
            "source_as_of": None if pd.isna(parsed) else parsed.isoformat(),
            "not_after_current_scan": bool(pd.isna(parsed) or pd.isna(current_as_of) or parsed <= current_as_of),
        }
    return checks


def current_registry(
    history: pd.DataFrame,
    latest: pd.DataFrame,
    phase2: dict,
    risk_report: dict,
    config: Phase4ResearchConfig = Phase4ResearchConfig(),
) -> dict[str, object]:
    current = _scanner_rows(latest)
    if current.empty:
        raise ValueError("no current scanner rows")
    latest_date = pd.to_datetime(current["date"], errors="coerce").max()
    current = current.loc[pd.to_datetime(current["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())].copy()
    current["selection_band"] = [
        _r_score_backbone(float(score), float(pct))
        for score, pct in zip(current["score"], current["score_pct_full"])
    ]

    feature_current = _latest_feature_rows(history, latest_date, config)
    feature_columns = [c for c in feature_current.columns if c not in current.columns or c in {"date", "symbol"}]
    if not feature_current.empty:
        merged = current.merge(
            feature_current[feature_columns],
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
            suffixes=("", "_feature"),
        )
    else:
        merged = current.copy()

    statistical = build_statistical_registry(phase2, risk_report)
    pit = _assert_pit_sources(merged, phase2, risk_report)
    rows: list[dict[str, object]] = []

    for row in merged.to_dict("records"):
        series = pd.Series(row)
        provenance = _provenance_quality(series, config.require_provenance_fields)
        for horizon in HORIZONS:
            h_registry = statistical["horizons"][str(horizon)]
            band = str(row["selection_band"])
            selection = dict((h_registry["selection"] or {}).get(band) or {"state": "unavailable", "direction": None})
            timing_matches, timing_unevaluable = _matched_robust_timing(series, h_registry["timing"])
            timing = _timing_model_state(timing_matches)
            timing["unevaluable_robust_patterns"] = timing_unevaluable
            risk = _risk_model_state(series, h_registry["risk"])

            selection_dq = _data_quality_state(["score", "score_pct_full"], series, provenance)
            matched_prereqs = sorted({
                p
                for evidence in timing_matches
                for condition in evidence.get("conditions", [])
                for p in _atom_prerequisites(str(condition))
            })
            timing_dq = (
                _data_quality_state(matched_prereqs, series, provenance)
                if timing_matches
                else {
                    "state": "proxy_partial_no_claim" if timing_unevaluable else "proxy_complete_no_claim",
                    "full_data_quality_claimed": False,
                    "unevaluable_robust_patterns": timing_unevaluable,
                    "note": "No robust timing claim matched; absence is not a negative timing signal.",
                }
            )
            robust_risk_fields = [
                feature for feature, evidence in h_registry["risk"].items() if evidence.get("state") == "robust"
            ]
            risk_dq = _data_quality_state(robust_risk_fields, series, provenance) if robust_risk_fields else {
                "state": "proxy_insufficient",
                "full_data_quality_claimed": False,
                "note": "No robust risk feature exists for this horizon.",
            }

            rows.append(
                {
                    "as_of": str(row.get("as_of") or row.get("date")),
                    "symbol": str(row["symbol"]),
                    "name": row.get("name"),
                    "horizon_sessions": int(horizon),
                    "selection_band": band,
                    "selection": selection,
                    "timing": timing,
                    "risk": risk,
                    "data_quality": {
                        "selection": selection_dq,
                        "timing": timing_dq,
                        "risk": risk_dq,
                    },
                    "model_agreement": _agreement_state(selection, timing, risk),
                    "regime": {"state": "unknown_unvalidated", "counted_as_model_vote": False},
                }
            )

    return {
        "as_of": latest_date.date().isoformat(),
        "scanner_rows": int(len(current)),
        "rows": rows,
        "pit_source_checks": pit,
        "statistical_registry": statistical,
    }


def analyze(
    history: pd.DataFrame,
    latest: pd.DataFrame,
    phase2: dict,
    risk_report: dict,
    config: Phase4ResearchConfig = Phase4ResearchConfig(),
) -> dict[str, object]:
    current = current_registry(history, latest, phase2, risk_report, config)
    agreement_counts = pd.Series([row["model_agreement"]["state"] for row in current["rows"]]).value_counts().to_dict()
    return {
        "phase": "4_confidence_vnext_empirical_research",
        "semantics": {
            "research_only": True,
            "production_confidence_changed": False,
            "production_score_changed": False,
            "risk_weights_changed": False,
            "r_codes_changed": False,
            "portfolio_logic_changed": False,
            "scalar_confidence_mapping_created": False,
            "confidence_thresholds_created": False,
            "probability_role": "calibration_not_independent_model_vote",
            "risk_role": "downside_protection_not_positive_return_vote",
            "regime_role": "unknown_unvalidated_not_model_vote",
            "missing_evidence": "unknown_or_insufficient_not_neutral",
            "phase2_phase3_holdout": "spent_not_used_to_tune_phase4_weights_or_thresholds",
            "historical_current_evidence_backfill": False,
        },
        "config": asdict(config),
        "current": current,
        "agreement_counts": {str(k): int(v) for k, v in agreement_counts.items()},
        "next_validation_gate": {
            "phase": "4E_walk_forward_reliability_validation",
            "purpose": "test whether pre-specified evidence/agreement states improve reliability on unspent/prospective outcomes",
            "requires_new_weights_or_thresholds": False,
            "production_change_allowed": False,
        },
    }


def run(
    history_path: str | Path,
    latest_path: str | Path,
    phase2_path: str | Path,
    risk_path: str | Path,
    output_path: str | Path,
    config: Phase4ResearchConfig = Phase4ResearchConfig(),
) -> dict[str, object]:
    history = pd.read_csv(history_path, low_memory=False)
    latest = pd.read_csv(latest_path, low_memory=False)
    phase2 = json.loads(Path(phase2_path).read_text(encoding="utf-8"))
    risk_report = json.loads(Path(risk_path).read_text(encoding="utf-8"))
    result = analyze(history, latest, phase2, risk_report, config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result
