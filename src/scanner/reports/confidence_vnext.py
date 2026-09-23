from __future__ import annotations

"""Phase 4: Confidence vNext research audit and evidence contract.

Research-only. This module audits the historical confidence implementation and
prepares the evidence contract for a replacement confidence layer. It must not
change production scoring, risk weights, R-codes, watchlists or portfolio logic.

Confidence means reliability of a model claim. It is not another opportunity
score, and favorable risk levels are not confidence. Missing evidence fails
closed as unknown/insufficient.
"""

from dataclasses import asdict, dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import HORIZONS


@dataclass(frozen=True)
class Phase4Config:
    stable_start: str = "2026-04-15"
    confidence_alias_fix_date: str = "2026-09-22"
    # Proven from the repository/workflow history during the Phase-4 audit.
    known_post_fix_run_ids: tuple[str, ...] = ("github-35753072370-1",)
    known_post_fix_config_prefixes: tuple[str, ...] = ("git-9edc9be",)


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

# Every raw factor that can affect one of the five legacy confidence components.
# Regime labels are audited separately because they are metadata rather than
# per-asset factor values.
LEGACY_FACTOR_INPUTS: tuple[str, ...] = (
    "growth",
    "roe",
    "margin",
    "debt_ratio",
    "volatility",
    "drawdown",
    "rs3m",
    "trend200",
    "liquidity_risk",
)

NUMERIC_ARCHIVE_FIELDS = {
    "score",
    "opportunity",
    "risk",
    "confidence",
    "rs3m",
    "trend200",
    "liquidity_risk",
    "volatility",
    "drawdown",
    "roe",
    "growth",
    "margin",
    "debt_ratio",
    "cycle",
    "rank",
    "rank_percentile",
    "universe_size",
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
            "bull aliases were incompatible with canonical engine keys until the 2026-09-22 fix",
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


CALL_SITE_AUDIT: dict[str, dict[str, object]] = {
    "src/scanner/domain/scoring_engine/engine.py": {
        "role": "constructs normalized opportunity/risk factors and calls compute_confidence",
        "finding": "confidence receives already-neutralized numeric factors; missing raw values are no longer distinguishable",
    },
    "src/scanner/app/score_step.py": {
        "role": "writes ConfidenceScore, ConfidenceLabel and ConfidenceBreakdown to the runtime watchlist",
        "finding": (
            "optional ScoreOppFactors/ScoreRiskFactors archival is currently ineffective because score_step "
            "looks for factor_breakdown['opportunity'/'risk'] while compute_scores exposes "
            "'opportunity_factors_0_1'/'risk_factors_0_1'"
        ),
    },
    "src/scanner/data/schema/canonical.py": {
        "role": "maps ConfidenceScore to canonical confidence",
        "finding": "only aggregate confidence is canonicalized; the component breakdown is not a research field",
    },
    "src/scanner/reports/daily_research.py": {
        "role": "publishes validated scanner observations to research history",
        "finding": "archives aggregate confidence and label, not the five legacy component values",
    },
    "src/scanner/domain/scoring_engine/quality/snapshots.py": {
        "role": "legacy score-history snapshot/calibration helper",
        "finding": "stores aggregate confidence/label; old confidence-vs-return correlation is not a reliability definition",
    },
    "src/scanner/presets/presets.json": {
        "role": "uses confidence as a secondary sort key",
        "finding": "legacy confidence already influences presentation order although it has not been empirically validated as reliability",
    },
    "src/scanner/ui/generator.py": {
        "role": "displays confidence in table and detail drawer",
        "finding": "UI describes it as trust in scoring, which overstates what the current heuristic has established",
    },
}


def _clean_text(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    return values.mask(values.str.lower().isin({"", "nan", "none", "null", "<na>"}))


def _numeric_nonnull(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").notna()


def _nonnull(series: pd.Series, *, numeric: bool = False) -> pd.Series:
    return _numeric_nonnull(series) if numeric else _clean_text(series).notna()


def _confidence_scanner_rows(history: pd.DataFrame) -> pd.DataFrame:
    """Return PIT scanner observations, retaining unmarked legacy scanner rows.

    The research archive added observation_type/data_source prospectively. Older
    real scanner rows therefore have blanks in those columns. They remain valid
    scanner observations when they carry a numeric score. Known non-scanner
    sources are excluded. Same-day reruns keep the last appended observation.
    """
    frame = history.copy()
    frame["_source_order"] = np.arange(len(frame), dtype=np.int64)

    if "observation_type" in frame.columns:
        obs = _clean_text(frame["observation_type"])
        frame = frame.loc[obs.isna() | obs.eq("observed_scanner")].copy()
    if "data_source" in frame.columns:
        source = _clean_text(frame["data_source"])
        frame = frame.loc[source.isna() | source.eq("scanner_run")].copy()

    if "date" not in frame or "symbol" not in frame or "score" not in frame:
        return frame.iloc[0:0].copy()

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["symbol"] = _clean_text(frame["symbol"])
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "score"])
    frame = frame.drop_duplicates(["date", "symbol"], keep="last")
    return (
        frame.sort_values(["date", "_source_order"], kind="mergesort")
        .drop(columns=["_source_order"])
        .reset_index(drop=True)
    )


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
    mask = _nonnull(frame[field], numeric=field in NUMERIC_ARCHIVE_FIELDS)
    subset = frame.loc[mask]
    dates = pd.to_datetime(subset["date"], errors="coerce").dropna()
    return {
        "stored": True,
        "non_null_rows": int(mask.sum()),
        "non_null_rate": float(mask.mean()) if len(frame) else 0.0,
        "first_non_null_date": dates.min().date().isoformat() if len(dates) else None,
        "last_non_null_date": dates.max().date().isoformat() if len(dates) else None,
        "symbols": int(subset["symbol"].astype(str).nunique()) if len(subset) else 0,
    }


def _contains_prefix(value: object, prefixes: tuple[str, ...]) -> bool:
    text = str(value or "").strip().lower()
    return any(prefix.lower() in text for prefix in prefixes if prefix)


def _formula_epoch_and_reason(
    row: pd.Series,
    fix_day: pd.Timestamp,
    config: Phase4Config,
) -> tuple[str, str]:
    day = pd.Timestamp(row["date"]).normalize()
    if day < fix_day:
        return "pre_alias_fix", "date_before_fix"
    if day > fix_day:
        return "post_alias_fix", "date_after_fix"

    run_id = str(row.get("run_id", "") or "").strip()
    if run_id and run_id in config.known_post_fix_run_ids:
        return "post_alias_fix", "known_post_fix_run_id"

    config_version = row.get("config_version", "")
    if _contains_prefix(config_version, config.known_post_fix_config_prefixes):
        return "post_alias_fix", "known_post_fix_config_version"

    return "transition_unknown", "ambiguous_fix_date_provenance"


def _with_formula_epoch(scanner: pd.DataFrame, config: Phase4Config) -> pd.DataFrame:
    work = scanner.copy()
    if work.empty:
        work["formula_epoch"] = pd.Series(dtype="string")
        work["formula_epoch_reason"] = pd.Series(dtype="string")
        return work
    fix_day = pd.Timestamp(config.confidence_alias_fix_date).normalize()
    classified = work.apply(
        lambda row: _formula_epoch_and_reason(row, fix_day, config),
        axis=1,
        result_type="expand",
    )
    classified.columns = ["formula_epoch", "formula_epoch_reason"]
    work[["formula_epoch", "formula_epoch_reason"]] = classified
    return work


def _legacy_formula_epochs(scanner: pd.DataFrame, config: Phase4Config) -> dict[str, dict[str, object]]:
    work = _with_formula_epoch(scanner, config)
    if work.empty:
        return {}

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
        reasons = group["formula_epoch_reason"].value_counts().to_dict()
        out[epoch] = {
            "rows": int(len(group)),
            "dates": int(group["date"].nunique()) if len(group) else 0,
            "confidence_non_null_rows": int(len(values)),
            "confidence_mean": float(values.mean()) if len(values) else None,
            "confidence_median": float(values.median()) if len(values) else None,
            "label_counts": {str(k): int(v) for k, v in labels.items()},
            "classification_reasons": {str(k): int(v) for k, v in reasons.items()},
        }
    return out


def _presence_proxy(scanner: pd.DataFrame) -> dict[str, object]:
    """Audit completeness of all archived raw factor inputs, fail-closed."""
    if scanner.empty:
        return {
            "status": "insufficient_evidence",
            "required_factor_fields": list(LEGACY_FACTOR_INPUTS),
            "note": "raw factor presence proxy unavailable",
        }

    matrix: dict[str, pd.Series] = {}
    missing_columns: list[str] = []
    for field in LEGACY_FACTOR_INPUTS:
        if field in scanner.columns:
            matrix[field] = _nonnull(scanner[field], numeric=True)
        else:
            matrix[field] = pd.Series(False, index=scanner.index)
            missing_columns.append(field)

    presence = pd.DataFrame(matrix, index=scanner.index)
    fraction = presence.sum(axis=1) / len(LEGACY_FACTOR_INPUTS)
    return {
        "status": "diagnostic_only",
        "required_factor_fields": list(LEGACY_FACTOR_INPUTS),
        "missing_columns": missing_columns,
        "rows": int(len(fraction)),
        "mean_raw_factor_presence_fraction": float(fraction.mean()),
        "complete_raw_factor_presence_rate": float(presence.all(axis=1).mean()),
        "per_field_presence_rate": {
            field: float(presence[field].mean()) for field in LEGACY_FACTOR_INPUTS
        },
        "note": (
            "This is only archived raw-factor presence. It is not Data Quality: "
            "freshness, source quality, PIT provenance and historical normalized values "
            "were not archived. Missing columns count as absent rather than neutral."
        ),
    }


def _expected_display_label(value: float) -> str:
    if value >= 75.0:
        return "HIGH"
    if value >= 50.0:
        return "MED"
    return "LOW"


def _label_consistency(scanner: pd.DataFrame) -> dict[str, object]:
    if "confidence" not in scanner or "confidence_label" not in scanner:
        return {"compared_rows": 0, "display_score_label_mismatches": 0, "samples": []}

    work = scanner[["date", "symbol", "confidence", "confidence_label"]].copy()
    work["confidence_num"] = pd.to_numeric(work["confidence"], errors="coerce")
    work["label"] = _clean_text(work["confidence_label"])
    work = work.dropna(subset=["confidence_num", "label"])
    if work.empty:
        return {"compared_rows": 0, "display_score_label_mismatches": 0, "samples": []}

    work["expected_from_stored_score"] = work["confidence_num"].map(_expected_display_label)
    mismatch = work.loc[work["label"].ne(work["expected_from_stored_score"])].copy()
    samples = [
        {
            "date": pd.Timestamp(row.date).date().isoformat(),
            "symbol": str(row.symbol),
            "stored_confidence": float(row.confidence_num),
            "stored_label": str(row.label),
            "label_implied_by_stored_rounded_score": str(row.expected_from_stored_score),
        }
        for row in mismatch.head(20).itertuples(index=False)
    ]
    return {
        "compared_rows": int(len(work)),
        "display_score_label_mismatches": int(len(mismatch)),
        "mismatch_rate": float(len(mismatch) / len(work)),
        "samples": samples,
        "note": (
            "Legacy code assigns the label before rounding confidence to one decimal. "
            "A displayed 50.0/LOW or 75.0/MED can therefore be internally consistent "
            "with the unrounded value but is confusing at the stored/display boundary."
        ),
    }


def _spearman_pair(frame: pd.DataFrame, left: str, right: str) -> dict[str, object]:
    if left not in frame or right not in frame:
        return {"N": 0, "spearman": None}
    values = pd.DataFrame(
        {
            "left": pd.to_numeric(frame[left], errors="coerce"),
            "right": pd.to_numeric(frame[right], errors="coerce"),
        }
    ).dropna()
    if len(values) < 3 or values["left"].nunique() < 2 or values["right"].nunique() < 2:
        return {"N": int(len(values)), "spearman": None}
    rho = values["left"].corr(values["right"], method="spearman")
    return {"N": int(len(values)), "spearman": None if pd.isna(rho) else float(rho)}


def _relationship_diagnostics(scanner: pd.DataFrame, config: Phase4Config) -> dict[str, object]:
    """Quantify how much legacy confidence behaves like existing score layers."""
    work = _with_formula_epoch(scanner, config)
    targets = ("score", "opportunity", "risk", "rs3m", "trend200", "volatility", "drawdown")
    result: dict[str, object] = {}
    for epoch in ("pre_alias_fix", "transition_unknown", "post_alias_fix"):
        group = work.loc[work["formula_epoch"].eq(epoch)]
        result[epoch] = {
            target: _spearman_pair(group, "confidence", target)
            for target in targets
        }
    result["note"] = (
        "These are descriptive same-observation correlations, not predictive validation. "
        "High absolute correlation with score/opportunity/risk supports the double-counting audit."
    )
    return result


def audit_history(history: pd.DataFrame, config: Phase4Config = Phase4Config()) -> dict[str, object]:
    """Audit what confidence evidence is genuinely present in scanner history."""
    scanner = _confidence_scanner_rows(history)
    audited_fields = (
        "confidence",
        "confidence_label",
        "opportunity",
        "risk",
        "rs3m",
        "trend200",
        "liquidity_risk",
        "volatility",
        "drawdown",
        "roe",
        "growth",
        "margin",
        "debt_ratio",
        "market_regime_stock",
        "market_regime_crypto",
        "run_id",
        "universe_version",
        "config_version",
        "scoring_version",
    )
    fields = {name: _field_availability(scanner, name) for name in audited_fields}
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
            "display_label_consistency": _label_consistency(scanner),
            "warning": (
                "Legacy aggregate confidence is audit-only. It is not one invariant metric across "
                "the 2026-09-22 alias-fix transition, and the five component values were not "
                "persisted in score_history/history_analysis."
            ),
        },
        "legacy_component_inputs": {
            component: {
                name: fields.get(name, _field_availability(scanner, name))
                for name in names
            }
            for component, names in LEGACY_RAW_INPUTS.items()
        },
        "raw_factor_presence_proxy": _presence_proxy(scanner),
        "legacy_relationship_diagnostics": _relationship_diagnostics(scanner, config),
    }


def testability_matrix() -> dict[str, dict[str, object]]:
    """PIT-aware researchability matrix. Unknown evidence is never neutralized."""
    return {
        "legacy_aggregate_confidence": {
            "status": "directly_testable_with_version_segmentation",
            "limitations": [
                "same-day reruns must be deduplicated by append/source order",
                "pre/post 2026-09-22 formula behavior must not be pooled",
                "deployment-day rows require run/config/commit provenance",
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
                "historical liquidity-risk input is not archived",
                "full Data Quality requires prospective archival",
            ],
        },
        "statistical_confidence": {
            "status": "testable_only_as_of",
            "limitations": [
                "current Phase-2/3 validation evidence cannot be injected into older dates",
                "historical tests require expanding/walk-forward evidence using only outcomes matured as of each date",
                "the Phase-2/3 holdout is already used and must not tune Phase-4 mappings",
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
                "new weighting/threshold choices need fresh validation rather than reuse of spent holdout evidence",
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
                    "price-history session depth as observed at scan time",
                    "run/universe/config/evidence versions",
                ],
                "forbidden_shortcuts": [
                    "good fundamentals",
                    "high opportunity factors",
                    "low risk factor values",
                    "automatic 0.5 for missing data",
                    "today's metadata retroactively attached to historical scans",
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
                    "favorable risk level alone never increases confidence",
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
        "holdout_policy": {
            "phase2_phase3_holdout": "spent_for_prior_validation_not_for_phase4_weight_or_threshold_selection",
            "phase4_candidate_selection": "discovery_or_pre_specified_only",
            "production_gate": "requires_new_unspent_or_prospective_validation_evidence",
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
            "audit legacy confidence, call sites and formula versions",
            "freeze PIT availability matrix and confidence semantics",
            "start prospective evidence archival for unavailable fields",
            "build as-of Data Quality candidates without signal desirability",
            "build walk-forward Statistical Confidence from matured outcomes only",
            "define a pre-specified compatibility matrix for Model Agreement",
            "freeze discovery choices before any fresh holdout evaluation",
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

    contract = research_contract(config)
    report: dict[str, object] = {
        "phase": "4_confidence_vnext_audit",
        "semantics": contract["semantics"],
        "legacy_component_audit": LEGACY_COMPONENT_AUDIT,
        "double_counting": {k: list(v) for k, v in DOUBLE_COUNTING.items()},
        "call_site_audit": CALL_SITE_AUDIT,
        "history_audit": audit_history(history, config),
        "testability": testability_matrix(),
        "research_contract": contract,
        "source_metadata": {
            "path": str(metadata_path) if metadata_path is not None else None,
            "as_of": metadata.get("as_of") if isinstance(metadata, dict) else None,
            "last_complete_scan": metadata.get("last_complete_scan") if isinstance(metadata, dict) else None,
            "latest_run_complete": metadata.get("latest_run_complete") if isinstance(metadata, dict) else None,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    return report
