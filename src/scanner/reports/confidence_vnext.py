from __future__ import annotations

"""Phase 4: Confidence vNext research audit and evidence contract.

Research-only.  This module audits the historical confidence implementation and
prepares the evidence contract for a replacement confidence layer.  It must not
change production scoring, risk weights, R-codes, watchlists or portfolio logic.

The core semantic rule is deliberately strict:

    Confidence describes how trustworthy a model claim is; it is not another
    opportunity score and it is not a low-risk reward.

Missing evidence fails closed as unknown/insufficient.  A neutral numeric value
must never be manufactured merely because data are missing.
"""

from dataclasses import asdict, dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS, _scanner_rows


@dataclass(frozen=True)
class Phase4Config:
    stable_start: str = "2026-04-15"
    confidence_alias_fix_date: str = "2026-09-22"
    min_required_fields_for_proxy: int = 1


# These are raw historical fields, not the normalized factors consumed by the
# legacy confidence calculation.  They are used only to audit availability.
LEGACY_RAW_INPUTS: dict[str, tuple[str, ...]] = {
    "data_coverage": (
        "growth",
        "roe",
        "margin",
        "debt_ratio",
        "volatility",
        "rs3m",
        "trend200",
    ),
    "signal_confluence": ("growth", "roe", "margin", "rs3m", "trend200"),
    "risk_cleanliness": ("volatility", "drawdown", "debt_ratio"),
    "regime_alignment": (
        "market_regime_stock",
        "market_regime_crypto",
        "volatility",
        "roe",
        "margin",
        "rs3m",
        "trend200",
    ),
    "liquidity_sanity": ("liquidity_risk",),
}


LEGACY_COMPONENT_AUDIT: dict[str, dict[str, object]] = {
    "data_coverage": {
        "legacy_weight": 0.25,
        "claim": "data completeness",
        "actual_measure": "presence of normalized factor keys",
        "verdict": "invalid_for_claim",
        "reasons": [
            "the production engine materializes every core normalized key",
            "missing raw values are normalized to numeric 0.5 before confidence sees them",
            "freshness, source quality, staleness, PIT provenance and price-history depth are absent",
        ],
    },
    "signal_confluence": {
        "legacy_weight": 0.25,
        "claim": "signal confluence",
        "actual_measure": "count of opportunity factors above 0.7",
        "verdict": "selection_strength_not_confidence",
        "reasons": [
            "growth, ROE, margin, relative strength and Trend200 are opportunity inputs",
            "the component rewards attractive factor levels rather than evidence reliability",
        ],
    },
    "risk_cleanliness": {
        "legacy_weight": 0.20,
        "claim": "risk cleanliness",
        "actual_measure": "count of risk factors below 0.6",
        "verdict": "risk_level_not_confidence",
        "reasons": [
            "volatility, max drawdown and debt are risk-model inputs",
            "missing raw inputs normalize to 0.5 and therefore pass the <0.6 clean-risk test",
        ],
    },
    "regime_alignment": {
        "legacy_weight": 0.20,
        "claim": "regime alignment",
        "actual_measure": "directional desirability heuristic conditioned on regime",
        "verdict": "model_signal_not_confidence",
        "reasons": [
            "bull reuses relative strength and Trend200",
            "bear reuses inverse volatility, ROE and margin",
            "neutral is hard-coded to 0.5 regardless of evidence",
            "the bull aliases were incompatible with canonical engine keys until the 2026-09-22 fix",
        ],
    },
    "liquidity_sanity": {
        "legacy_weight": 0.10,
        "claim": "liquidity sanity",
        "actual_measure": "one minus liquidity risk",
        "verdict": "risk_level_not_confidence",
        "reasons": [
            "it rewards a favorable liquidity-risk level rather than confidence in the estimate",
            "missing liquidity risk becomes 0.5",
        ],
    },
}


DOUBLE_COUNTING: dict[str, tuple[str, ...]] = {
    "selection": (
        "growth",
        "roe",
        "margin",
        "relative_strength/rs3m",
        "trend_200dma/trend200",
    ),
    "risk": (
        "volatility",
        "max_drawdown/drawdown",
        "debt_to_equity/debt_ratio",
        "liquidity_risk",
    ),
    "regime_component_reuses": (
        "relative_strength/rs3m",
        "trend_200dma/trend200",
        "volatility",
        "roe",
        "margin",
    ),
}


def _clean_text(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    return values.mask(values.str.lower().isin({"", "nan", "none", "null", "<na>"}))


def _nonnull(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").notna()
    return _clean_text(series).notna()


def _field_availability(frame: pd.DataFrame, field: str) -> dict[str, object]:
    if field not in frame.columns:
        return {
            "stored": False,
            "non_null_rows": 0,
            "non_null_rate": 0.0,
            "first_non_null_date": None,
            "last_non_null_date": None,
            "symbols": 0,
        }
    mask = _nonnull(frame[field])
    subset = frame.loc[mask]
    dates = pd.to_datetime(subset["date"], errors="coerce").dropna()
    return {
        "stored": True,
        "non_null_rows": int(mask.sum()),
        "non_null_rate": float(mask.mean()) if len(frame) else 0.0,
        "first_non_null_date": dates.min().date().isoformat() if len(dates) else None,
        "last_non_null_date": dates.max().date().isoformat() if len(dates) else None,
        "symbols": int(subset["symbol"].astype(str).nunique()) if len(subset) and "symbol" in subset else 0,
    }


def _formula_epoch(day: pd.Timestamp, fix_day: pd.Timestamp) -> str:
    normalized = pd.Timestamp(day).normalize()
    if normalized < fix_day:
        return "pre_alias_fix"
    if normalized == fix_day:
        # There may be runs from both code versions on the deployment day.  Do
        # not guess from date alone.
        return "transition_unknown"
    return "post_alias_fix"


def _legacy_formula_epochs(scanner: pd.DataFrame, config: Phase4Config) -> dict[str, dict[str, object]]:
    if scanner.empty:
        return {}
    work = scanner.copy()
    fix_day = pd.Timestamp(config.confidence_alias_fix_date).normalize()
    work["formula_epoch"] = work["date"].map(lambda d: _formula_epoch(pd.Timestamp(d), fix_day))
    work["confidence_num"] = pd.to_numeric(work.get("confidence"), errors="coerce")
    if "confidence_label" in work:
        work["confidence_label_clean"] = _clean_text(work["confidence_label"])
    else:
        work["confidence_label_clean"] = pd.Series(pd.NA, index=work.index, dtype="string")

    out: dict[str, dict[str, object]] = {}
    for epoch in ("pre_alias_fix", "transition_unknown", "post_alias_fix"):
        group = work.loc[work["formula_epoch"].eq(epoch)].copy()
        values = group["confidence_num"].dropna()
        labels = group["confidence_label_clean"].dropna().value_counts().to_dict()
        out[epoch] = {
            "rows": int(len(group)),
            "dates": int(group["date"].nunique()) if len(group) else 0,
            "confidence_non_null_rows": int(len(values)),
            "confidence_mean": float(values.mean()) if len(values) else None,
            "confidence_median": float(values.median()) if len(values) else None,
            "label_counts": {str(k): int(v) for k, v in labels.items()},
        }
    return out


def _presence_proxy(scanner: pd.DataFrame) -> dict[str, object]:
    """Describe raw-field presence only; this is not a vNext Data Quality score."""
    fields = ("growth", "roe", "margin", "volatility", "drawdown", "rs3m", "trend200")
    available = [name for name in fields if name in scanner.columns]
    if not available or scanner.empty:
        return {
            "status": "insufficient_evidence",
            "fields": available,
            "note": "raw presence proxy unavailable",
        }
    matrix = pd.DataFrame({name: _nonnull(scanner[name]) for name in available})
    fraction = matrix.mean(axis=1)
    return {
        "status": "diagnostic_only",
        "fields": available,
        "rows": int(len(fraction)),
        "mean_raw_presence_fraction": float(fraction.mean()),
        "complete_raw_presence_rate": float((fraction == 1.0).mean()),
        "note": (
            "This measures only whether selected archived raw values are present. "
            "It does not establish freshness, source quality, PIT provenance, or the "
            "historical normalized values consumed by legacy confidence."
        ),
    }


def audit_history(history: pd.DataFrame, config: Phase4Config = Phase4Config()) -> dict[str, object]:
    """Audit what confidence evidence is genuinely present in scanner history."""
    scanner = _scanner_rows(history)
    fields = {
        name: _field_availability(scanner, name)
        for name in (
            "confidence",
            "confidence_label",
            "rs3m",
            "trend200",
            "liquidity_risk",
            "volatility",
            "drawdown",
            "roe",
            "growth",
            "margin",
            "debt_ratio",
            "run_id",
            "universe_version",
            "config_version",
        )
    }
    first_confidence = fields["confidence"]["first_non_null_date"]
    return {
        "scanner_rows_after_same_day_symbol_dedup": int(len(scanner)),
        "scanner_dates": int(scanner["date"].nunique()) if len(scanner) else 0,
        "scanner_symbols": int(scanner["symbol"].astype(str).nunique()) if len(scanner) else 0,
        "stored_fields": fields,
        "aggregate_confidence": {
            "directly_archived": bool(fields["confidence"]["stored"]),
            "first_observed_non_null_date": first_confidence,
            "component_breakdown_archived": False,
            "normalized_factor_payload_archived_by_default": False,
            "formula_epochs": _legacy_formula_epochs(scanner, config),
            "warning": (
                "Legacy aggregate confidence is audit-only. It is not one invariant metric across "
                "the 2026-09-22 alias-fix transition, and the five component values were not "
                "persisted in score_history."
            ),
        },
        "legacy_component_inputs": {
            component: {
                name: fields.get(name, _field_availability(scanner, name))
                for name in names
            }
            for component, names in LEGACY_RAW_INPUTS.items()
        },
        "raw_presence_proxy": _presence_proxy(scanner),
    }


def testability_matrix() -> dict[str, dict[str, object]]:
    """PIT-aware researchability matrix. Unknown evidence is never neutralized."""
    return {
        "legacy_aggregate_confidence": {
            "status": "directly_testable_with_version_segmentation",
            "limitations": [
                "same-day reruns must be deduplicated by source order",
                "pre/post 2026-09-22 formula behavior must not be pooled",
                "post-fix forward windows need time to mature",
            ],
        },
        "legacy_component_breakdown": {
            "status": "not_directly_testable",
            "limitations": [
                "component values were not archived",
                "normalized factor payloads were not archived by the scheduled scanner",
                "raw values must not be substituted for normalized historical values",
            ],
        },
        "data_quality_presence": {
            "status": "partially_testable",
            "limitations": [
                "raw-field presence can be audited",
                "historical source timestamps, staleness and per-factor PIT provenance are missing",
                "full Data Quality requires prospective archival",
            ],
        },
        "statistical_confidence": {
            "status": "testable_only_as_of",
            "limitations": [
                "current Phase-2/3 validation evidence cannot be injected into older dates",
                "historical tests require expanding/walk-forward evidence using only matured outcomes as of each date",
                "overlapping outcomes require circular moving observation-date blocks of at least 2x horizon",
            ],
        },
        "model_agreement": {
            "status": "partially_testable",
            "limitations": [
                "Selection states are historically available",
                "Timing may use only frozen Phase-1B definitions and evidence known as of the observation date",
                "5T volatility/drawdown risk relations are usable; other risk factors remain PIT-immature",
                "unknown/immature model states must not be counted as agreement",
            ],
        },
    }


def research_contract(config: Phase4Config = Phase4Config()) -> dict[str, object]:
    return {
        "semantics": {
            "research_only": True,
            "production_confidence_changed": False,
            "production_score_changed": False,
            "opportunity_weights_changed": False,
            "risk_weights_changed": False,
            "r_codes_changed": False,
            "portfolio_logic_changed": False,
            "confidence_definition": "reliability_of_model_claim_not_expected_return",
            "missing_evidence": "unknown_or_insufficient_not_neutral",
        },
        "pillars": {
            "data_quality": {
                "question": "How trustworthy and complete is the data basis for this specific model claim?",
                "allowed_inputs": [
                    "required-field presence",
                    "source/fetch status",
                    "source timestamp and age",
                    "PIT-verification status",
                    "invalid/range checks",
                    "price-history session depth",
                    "run/universe/config/evidence versions",
                ],
                "forbidden_shortcuts": [
                    "good fundamentals",
                    "high opportunity factors",
                    "low risk factor values",
                    "automatic 0.5 for missing data",
                ],
            },
            "statistical_confidence": {
                "question": "How mature and robust is the empirical evidence behind this model claim and horizon?",
                "allowed_inputs": [
                    "validation N",
                    "unique observation dates",
                    "time-separated support regions",
                    "robust moving-block interval width and sign",
                    "discovery/validation direction consistency",
                    "calibration error",
                    "temporal stability",
                    "ticker concentration/effective names",
                    "evidence age and version",
                ],
                "uncertainty": {
                    "bootstrap": "circular_moving_observation_date",
                    "effective_block_length": "2_x_forward_horizon",
                    "full_date_clusters": True,
                    "same_date_sequence_for_occurrence_and_baseline": True,
                    "minimum_support_regions_for_robust_interval": 2,
                    "iid_intervals": "diagnostic_only",
                    "insufficient_support": None,
                },
            },
            "model_agreement": {
                "question": "Do independently conceived model claims tell a compatible story?",
                "candidate_models": ["selection", "timing", "probability", "risk", "regime_if_validated"],
                "rules": [
                    "agreement is compatibility of model states, not all numbers being high",
                    "a statistically immature model contributes unknown, not agreement",
                    "positive timing plus high short-term downside risk is tension, not agreement",
                    "strong selection plus clearly negative timing is conflict",
                ],
            },
        },
        "validation_targets": {
            "selection": "higher confidence should reduce rank/prediction error or strengthen score-to-future-peer-alpha reliability",
            "timing": "higher confidence should increase expected-direction confirmation for frozen timing claims",
            "probability": "higher confidence should reduce calibration error/Brier loss rather than simply increase positive outcome rate",
            "risk": "higher confidence should make protection forecasts more reliable, not merely select low-risk assets",
            "agreement": "compatible mature claims should fail less often than conflicted mature claims under external outcomes",
        },
        "horizons": {
            str(h): {
                "bootstrap_block_sessions": int(2 * h),
                "status_rule": "fail_closed_until_mature",
            }
            for h in HORIZONS
        },
        "prospective_archival": {
            "symbol_level_evidence": [
                "as_of",
                "run_id",
                "symbol",
                "confidence_evidence_version",
                "claim_id",
                "data_quality_state",
                "dq_required_count",
                "dq_present_count",
                "dq_fresh_count",
                "dq_pit_verified_count",
                "dq_invalid_count",
                "dq_price_history_sessions",
                "dq_source_failure_count",
                "model_agreement_state",
                "agreement_available_models",
                "agreement_mature_models",
                "agreement_conflicts",
            ],
            "model_horizon_evidence": [
                "as_of",
                "model_id",
                "claim_id",
                "horizon_sessions",
                "evidence_version",
                "validation_n",
                "unique_observation_dates",
                "support_regions",
                "robust_interval_low",
                "robust_interval_high",
                "calibration_error",
                "top_symbol_share",
                "effective_symbol_count",
                "evidence_state",
            ],
            "note": "Archive these prospectively; do not fabricate historical freshness/source/PIT metadata.",
        },
        "research_sequence": [
            "audit legacy confidence and formula versions",
            "freeze PIT availability matrix and confidence semantics",
            "start prospective evidence archival for unavailable fields",
            "build as-of Data Quality candidates without signal desirability",
            "build walk-forward Statistical Confidence from matured outcomes only",
            "define a pre-specified compatibility matrix for Model Agreement",
            "freeze discovery choices before holdout evaluation",
            "validate reliability with circular 2x-horizon moving-date blocks",
            "only after validation decide whether a scalar 0-100 mapping is justified",
            "change production confidence only in a separate production PR",
        ],
        "config": asdict(config),
    }


def run(
    history_path: Path,
    output_path: Path,
    config: Phase4Config = Phase4Config(),
    *,
    metadata_path: Path | None = None,
) -> dict[str, object]:
    history = pd.read_csv(history_path, low_memory=False)
    metadata: dict[str, object] | None = None
    if metadata_path is not None and metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    report: dict[str, object] = {
        "phase": "4_confidence_vnext_audit",
        "semantics": research_contract(config)["semantics"],
        "legacy_component_audit": LEGACY_COMPONENT_AUDIT,
        "double_counting": {k: list(v) for k, v in DOUBLE_COUNTING.items()},
        "history_audit": audit_history(history, config),
        "testability": testability_matrix(),
        "research_contract": research_contract(config),
        "source_metadata": {
            "path": str(metadata_path) if metadata_path is not None else None,
            "as_of": metadata.get("as_of") if isinstance(metadata, dict) else None,
            "last_complete_scan": metadata.get("last_complete_scan") if isinstance(metadata, dict) else None,
            "latest_run_complete": metadata.get("latest_run_complete") if isinstance(metadata, dict) else None,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    return report
