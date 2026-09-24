from __future__ import annotations

"""Phase 5A readiness and purged walk-forward contract.

Research-only. This module never changes production Confidence, Selection,
Timing, Probability, Risk, portfolio logic, or trading decisions.

Phase-4E shadow-v1 is accepted only as immutable audit evidence. It does not
archive an integrity-bound elapsed-market-session count for outcomes, so v1
outcomes are never certified as training/evaluation evidence by Phase 5A.
Phase 5B must add prospective integrity-bound Statistical Context and horizon
provenance before adaptive learning can begin.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_prospective import CLAIM_COLUMNS, OUTCOME_COLUMNS
from scanner.reports.selection_timing import HORIZONS


SCHEMA_VERSION = "phase5_walkforward_v1"
PHASE4E_SCHEMA_VERSION = "phase4e_shadow_v1"
PHASE5_FREEZE_COMMIT = "fb038c78682e4e9fe87f11a1c3c9472b763bd3c4"
PHASE5_FREEZE_TIME = "2026-09-24T01:12:14Z"
ORDINAL_EVIDENCE_STATES = ("robust", "directional_only", "immature", "mixed", "unavailable")
STATISTICAL_CONTEXT_COLUMNS = ("timing_statistical_state", "risk_statistical_state")
V1_HORIZON_PROVENANCE_BLOCKER = "phase4e_shadow_v1_lacks_integrity_bound_elapsed_session_count"
STATISTICAL_CONTEXT_BLOCKER = "claim_level_phase4c_timing_and_risk_states_not_archived"


@dataclass(frozen=True)
class Phase5WalkForwardConfig:
    schema_version: str = SCHEMA_VERSION
    uncertainty_block_multiplier: int = 2
    minimum_time_separated_support_regions: int = 2
    fixed_event_spacing_sessions: int = 5
    require_statistical_context_for_adaptation: bool = True


def _empty_claims() -> pd.DataFrame:
    return pd.DataFrame(columns=CLAIM_COLUMNS)


def _empty_outcomes() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTCOME_COLUMNS)


def read_shadow_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected Phase 4E shadow schema in {p}")
    return frame


def _required_utc(value: object, label: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ValueError(f"{label} must be a parseable UTC timestamp")
    return pd.Timestamp(parsed)


def _immutable_freeze_time(value: str | None = None) -> pd.Timestamp:
    expected = _required_utc(PHASE5_FREEZE_TIME, "PHASE5_FREEZE_TIME")
    if value:
        supplied = _required_utc(value, "freeze_time")
        if supplied != expected:
            raise ValueError("Phase 5 freeze_time is immutable")
    return expected


def _immutable_freeze_commit(value: str | None = None) -> str:
    if value and str(value).strip() != PHASE5_FREEZE_COMMIT:
        raise ValueError("Phase 5 freeze_commit is immutable")
    return PHASE5_FREEZE_COMMIT


def _validate_config(config: Phase5WalkForwardConfig) -> None:
    fixed = {
        "schema_version": SCHEMA_VERSION,
        "uncertainty_block_multiplier": 2,
        "minimum_time_separated_support_regions": 2,
        "fixed_event_spacing_sessions": 5,
        "require_statistical_context_for_adaptation": True,
    }
    observed = asdict(config)
    mismatches = {key: (observed.get(key), value) for key, value in fixed.items() if observed.get(key) != value}
    if mismatches:
        raise ValueError(f"Phase 5A contract constants are immutable: {mismatches}")


def _assert_exact_schema(claims: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    if list(claims.columns) != list(CLAIM_COLUMNS):
        raise ValueError("claims do not match the immutable Phase-4E claim schema")
    if list(outcomes.columns) != list(OUTCOME_COLUMNS):
        raise ValueError("outcomes do not match the immutable Phase-4E outcome schema")


def _date_span(values: pd.Series) -> dict[str, str | None]:
    if values.empty:
        return {"start": None, "end": None}
    parsed = pd.to_datetime(values, errors="coerce", utc=True).dropna()
    if parsed.empty:
        return {"start": None, "end": None}
    return {"start": parsed.min().isoformat(), "end": parsed.max().isoformat()}


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    values = frame[column].astype(str).replace("", "missing")
    return {str(key): int(value) for key, value in values.value_counts(dropna=False).sort_index().items()}


def _optional_float(value: object) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _claim_payload(row: pd.Series) -> dict[str, object]:
    """Reconstruct the exact canonical payload used by Phase 4E claim hashing."""

    payload: dict[str, object] = {}
    for column in CLAIM_COLUMNS:
        if column == "claim_id":
            continue
        value: object = row.get(column, "")
        if column == "horizon_sessions":
            try:
                value = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("claim horizon_sessions must be an integer") from exc
        elif column == "start_adjusted_close":
            value = _optional_float(value)
        else:
            value = "" if value is None else str(value)
        payload[column] = value
    return payload


def _expected_claim_id(row: pd.Series) -> str:
    canonical = json.dumps(
        _claim_payload(row),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _validate_claim_archive(claims: pd.DataFrame) -> None:
    if claims.empty:
        return
    if claims["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in Phase 4E claims")
    if not claims["schema_version"].astype(str).eq(PHASE4E_SCHEMA_VERSION).all():
        raise ValueError("unexpected Phase 4E claim schema_version")

    claim_horizons = pd.to_numeric(claims["horizon_sessions"], errors="coerce")
    unsupported_horizons = claim_horizons.isna() | ~claim_horizons.isin(HORIZONS)
    if unsupported_horizons.any():
        examples = claims.loc[unsupported_horizons, "horizon_sessions"].astype(str).head(3).tolist()
        raise ValueError(f"unsupported Phase 4E claim horizon_sessions: {examples}")

    expected = claims.apply(_expected_claim_id, axis=1)
    supplied = claims["claim_id"].astype(str)
    if not supplied.eq(expected).all():
        examples = supplied.loc[~supplied.eq(expected)].head(3).tolist()
        raise ValueError(f"Phase 4E claim hash mismatch: {examples}")

    freeze = _immutable_freeze_time()
    generated = pd.to_datetime(claims["generated_at"], errors="coerce", utc=True)
    as_of = pd.to_datetime(claims["as_of"], errors="coerce", utc=True)
    start = pd.to_datetime(claims["start_market_date"], errors="coerce", utc=True)
    eligibility = claims["outcome_eligibility"].astype(str)
    start_value = pd.to_numeric(claims["start_adjusted_close"], errors="coerce")

    eligible_valid = (
        eligibility.eq("eligible")
        & start.notna()
        & start_value.notna()
        & np.isfinite(start_value.to_numpy(dtype=float, na_value=np.nan))
        & start_value.gt(0)
    )
    unevaluable_valid = (
        eligibility.eq("unevaluable")
        & claims["outcome_unavailable_reason"].astype(str).str.strip().ne("")
    )
    invalid = (
        generated.isna()
        | as_of.isna()
        | generated.le(freeze)
        | ~generated.dt.normalize().eq(as_of.dt.normalize())
        | (~eligible_valid & ~unevaluable_valid)
        | (eligibility.eq("eligible") & start.dt.normalize().gt(as_of.dt.normalize()))
    )
    if invalid.any():
        examples = claims.loc[invalid, "claim_id"].astype(str).head(3).tolist()
        raise ValueError(f"invalid Phase 4E claim chronology/provenance: {examples}")


def _minimum_weekday_gap(start: pd.Timestamp, end: pd.Timestamp) -> int:
    if pd.isna(start) or pd.isna(end) or end <= start:
        return 0
    return max(0, len(pd.bdate_range(start.normalize(), end.normalize())) - 1)


def _validate_outcome_archive(claims: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    """Validate raw v1 outcome integrity without certifying its exact horizon."""

    if outcomes.empty:
        return
    if outcomes["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in Phase 4E outcomes")
    if not outcomes["schema_version"].astype(str).eq(PHASE4E_SCHEMA_VERSION).all():
        raise ValueError("unexpected Phase 4E outcome schema_version")

    unknown = set(outcomes["claim_id"].astype(str)) - set(claims["claim_id"].astype(str))
    if unknown:
        raise ValueError("Phase 4E outcomes exist without matching immutable claims")

    claim_info = claims[
        [
            "claim_id",
            "as_of",
            "generated_at",
            "symbol",
            "currency",
            "horizon_sessions",
            "start_market_date",
            "outcome_eligibility",
        ]
    ].copy()
    work = outcomes.merge(
        claim_info,
        on="claim_id",
        how="left",
        validate="one_to_one",
        suffixes=("_outcome", "_claim"),
    )

    evaluated = pd.to_datetime(work["evaluated_at"], errors="coerce", utc=True)
    generated = pd.to_datetime(work["generated_at"], errors="coerce", utc=True)
    start_claim = pd.to_datetime(work["start_market_date_claim"], errors="coerce", utc=True).dt.normalize()
    start_outcome = pd.to_datetime(work["start_market_date_outcome"], errors="coerce", utc=True).dt.normalize()
    end_market = pd.to_datetime(work["end_market_date"], errors="coerce", utc=True).dt.normalize()
    as_of_claim = pd.to_datetime(work["as_of_claim"], errors="coerce", utc=True).dt.normalize()
    as_of_outcome = pd.to_datetime(work["as_of_outcome"], errors="coerce", utc=True).dt.normalize()
    h_claim = pd.to_numeric(work["horizon_sessions_claim"], errors="coerce")
    h_outcome = pd.to_numeric(work["horizon_sessions_outcome"], errors="coerce")

    start_price = pd.to_numeric(work["start_adjusted_close"], errors="coerce")
    end_price = pd.to_numeric(work["end_adjusted_close"], errors="coerce")
    returns = pd.to_numeric(work["return"], errors="coerce")
    adverse = pd.to_numeric(work["adverse_excursion"], errors="coerce")
    drawdown = pd.to_numeric(work["path_max_drawdown"], errors="coerce")
    finite_numeric = (
        start_price.notna() & end_price.notna() & returns.notna() & adverse.notna() & drawdown.notna()
    )
    if finite_numeric.all():
        arrays_finite = (
            np.isfinite(start_price.to_numpy(dtype=float))
            & np.isfinite(end_price.to_numpy(dtype=float))
            & np.isfinite(returns.to_numpy(dtype=float))
            & np.isfinite(adverse.to_numpy(dtype=float))
            & np.isfinite(drawdown.to_numpy(dtype=float))
        )
        finite_numeric = finite_numeric & pd.Series(arrays_finite, index=work.index)

    expected_return = end_price / start_price - 1.0
    return_consistent = pd.Series(
        np.isclose(
            returns.to_numpy(dtype=float, na_value=np.nan),
            expected_return.to_numpy(dtype=float, na_value=np.nan),
            rtol=1e-9,
            atol=1e-12,
            equal_nan=False,
        ),
        index=work.index,
    )
    path_tolerance = 1e-12
    adverse_consistent = adverse + path_tolerance >= np.maximum(0.0, -returns)
    drawdown_consistent = drawdown + path_tolerance >= adverse
    weekday_feasible = pd.Series(
        [
            _minimum_weekday_gap(start, end) >= int(h) if pd.notna(h) else False
            for start, end, h in zip(start_outcome, end_market, h_outcome)
        ],
        index=work.index,
    )

    invalid = (
        ~work["outcome_eligibility"].astype(str).eq("eligible")
        | generated.isna()
        | evaluated.isna()
        | start_claim.isna()
        | start_outcome.isna()
        | end_market.isna()
        | as_of_claim.isna()
        | as_of_outcome.isna()
        | ~generated.dt.normalize().eq(as_of_claim)
        | ~start_claim.eq(start_outcome)
        | start_claim.gt(as_of_claim)
        | end_market.le(as_of_claim)
        | evaluated.dt.normalize().le(end_market)
        | ~as_of_claim.eq(as_of_outcome)
        | ~work["symbol_claim"].astype(str).eq(work["symbol_outcome"].astype(str))
        | ~work["currency_claim"].astype(str).eq(work["currency_outcome"].astype(str))
        | h_claim.isna()
        | h_outcome.isna()
        | ~h_claim.eq(h_outcome)
        | ~h_claim.isin(HORIZONS)
        | ~finite_numeric
        | start_price.le(0)
        | end_price.le(0)
        | adverse.lt(0)
        | adverse.gt(1)
        | drawdown.lt(0)
        | drawdown.gt(1)
        | ~return_consistent
        | ~adverse_consistent
        | ~drawdown_consistent
        | ~weekday_feasible
    )
    if invalid.any():
        examples = work.loc[invalid, "claim_id"].astype(str).head(3).tolist()
        raise ValueError(f"invalid Phase 4E outcome chronology/labels: {examples}")


def _statistical_context_complete(claims: pd.DataFrame) -> bool:
    if claims.empty:
        return False
    if not set(STATISTICAL_CONTEXT_COLUMNS).issubset(set(CLAIM_COLUMNS)):
        return False
    valid = set(ORDINAL_EVIDENCE_STATES)
    return all(claims[column].astype(str).isin(valid).all() for column in STATISTICAL_CONTEXT_COLUMNS)


def _distribution_audit(claims: pd.DataFrame) -> dict[str, object]:
    return {
        "data_quality": {
            "selection": _value_counts(claims, "dq_selection_state"),
            "timing": _value_counts(claims, "dq_timing_state"),
            "risk": _value_counts(claims, "dq_risk_state"),
        },
        "statistical_confidence": {
            "selection_phase4c_state": _value_counts(claims, "selection_state"),
            "timing_phase4c_state": {
                "status": "not_archived_claim_level",
                "reason": "phase4e_shadow_v1 does not integrity-bind claim-level Timing Phase-4C ordinal context",
            },
            "risk_phase4c_state": {
                "status": "not_archived_claim_level",
                "reason": "phase4e_shadow_v1 does not integrity-bind claim-level Risk Phase-4C ordinal context",
            },
        },
        "model_agreement": _value_counts(claims, "agreement_state"),
        "model_state_diagnostics_not_phase4c_confidence": {
            "timing_state": _value_counts(claims, "timing_state"),
            "risk_state": _value_counts(claims, "risk_state"),
        },
    }


def readiness_audit(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    freeze_commit: str | None = None,
    freeze_time: str | None = None,
    config: Phase5WalkForwardConfig = Phase5WalkForwardConfig(),
) -> dict[str, object]:
    """Audit shadow-v1 without treating unverifiable v1 outcomes as mature evidence."""

    _validate_config(config)
    _assert_exact_schema(claims, outcomes)
    effective_freeze_time = _immutable_freeze_time(freeze_time)
    effective_freeze_commit = _immutable_freeze_commit(freeze_commit)
    _validate_claim_archive(claims)
    _validate_outcome_archive(claims, outcomes)

    statistical_context_complete = _statistical_context_complete(claims)
    blockers: set[str] = {STATISTICAL_CONTEXT_BLOCKER, V1_HORIZON_PROVENANCE_BLOCKER}
    horizons: dict[str, object] = {}

    for horizon in HORIZONS:
        c = claims.loc[pd.to_numeric(claims["horizon_sessions"], errors="coerce").eq(horizon)].copy() if not claims.empty else _empty_claims()
        raw_o = outcomes.loc[pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)].copy() if not outcomes.empty else _empty_outcomes()
        eligibility = c["outcome_eligibility"].astype(str) if not c.empty else pd.Series(dtype=str)

        if c.empty:
            blockers.add(f"{horizon}T_no_claims")
        if raw_o.empty:
            blockers.add(f"{horizon}T_no_raw_outcomes")
        blockers.add(f"{horizon}T_no_verified_mature_outcomes")
        blockers.add(f"{horizon}T_insufficient_time_separated_support")

        horizons[str(horizon)] = {
            "claims": int(len(c)),
            "evaluable_claims": int(eligibility.eq("eligible").sum()),
            "unevaluable_claims": int(eligibility.eq("unevaluable").sum()),
            "raw_archive_outcomes": int(len(raw_o)),
            "mature_outcomes": 0,
            "verified_mature_outcomes": 0,
            "symbols": int(c["symbol"].astype(str).replace("", np.nan).nunique()) if not c.empty else 0,
            "mature_symbols": 0,
            "snapshots": int(c["snapshot_id"].astype(str).replace("", np.nan).nunique()) if not c.empty else 0,
            "mature_snapshots": 0,
            "claim_time_span": _date_span(c["as_of"]) if not c.empty else {"start": None, "end": None},
            "raw_outcome_evaluation_span": _date_span(raw_o["evaluated_at"]) if not raw_o.empty else {"start": None, "end": None},
            "mature_outcome_time_span": {"start": None, "end": None},
            "outcome_end_span": _date_span(raw_o["end_market_date"]) if not raw_o.empty else {"start": None, "end": None},
            "non_overlapping_outcome_support_regions": 0,
            "robust_uncertainty_block_length_sessions": int(2 * horizon),
            "fixed_event_spacing_sessions": 5,
            "distributions": _distribution_audit(c),
            "walkforward_evaluation_ready": False,
            "horizon_session_provenance_verified": False,
        }

    return {
        "phase": "5A_readiness_walkforward_contract",
        "schema_version": SCHEMA_VERSION,
        "status": "insufficient_evidence",
        "freeze": {
            "main_commit": effective_freeze_commit,
            "time": effective_freeze_time.isoformat(),
            "immutable": True,
            "enforced_on_claim_generated_at": True,
        },
        "shadow_archive": {
            "claims": int(len(claims)),
            "raw_outcomes": int(len(outcomes)),
            "mature_outcomes": 0,
            "claim_span": _date_span(claims["as_of"]) if not claims.empty else {"start": None, "end": None},
        },
        "horizons": horizons,
        "blockers": sorted(blockers),
        "contract": {
            "production_changes_allowed": False,
            "adaptive_weights_created": False,
            "scalar_confidence_mapping_created": False,
            "confidence_thresholds_created": False,
            "portfolio_or_depot_watch_changed": False,
            "training_requires_fully_mature_prior_outcomes": True,
            "evaluation_must_be_future_to_training_cutoff": True,
            "fixed_event_spacing_sessions": 5,
            "overlapping_forward_windows_treated_as_independent": False,
            "robust_uncertainty": "circular moving observation-date bootstrap with full date clusters and effective block length 2 x horizon",
            "shadow_v1_outcomes_certified_for_training": False,
            "unknown_or_insufficient_is_neutral": False,
        },
        "statistical_context_complete": statistical_context_complete,
        "config": asdict(config),
    }


def purged_training_pairs(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    horizon: int,
    training_cutoff: str,
    evaluation_start: str,
) -> pd.DataFrame:
    """Fail closed: shadow-v1 cannot prove exact elapsed market sessions."""

    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    _assert_exact_schema(claims, outcomes)
    cutoff = _required_utc(training_cutoff, "training_cutoff")
    evaluation = _required_utc(evaluation_start, "evaluation_start")
    if cutoff >= evaluation:
        raise ValueError("training_cutoff must be before evaluation_start")
    _validate_claim_archive(claims)
    _validate_outcome_archive(claims, outcomes)
    if outcomes.empty:
        return pd.DataFrame()
    raise ValueError(V1_HORIZON_PROVENANCE_BLOCKER)


def _fingerprint_scalar(value: object) -> dict[str, object]:
    missing = pd.isna(value)
    if isinstance(missing, (bool, np.bool_)) and bool(missing):
        return {"type": "null", "value": None}
    if isinstance(value, (bool, np.bool_)):
        return {"type": "bool", "value": bool(value)}
    if isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_)):
        return {"type": "int", "value": str(int(value))}
    if isinstance(value, (float, np.floating)):
        return {"type": "float", "value": repr(float(value))}
    if isinstance(value, pd.Timestamp):
        return {"type": "timestamp", "value": value.isoformat()}
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}", "value": str(value)}


def _fingerprint_dtype(series: pd.Series) -> dict[str, object]:
    dtype = series.dtype
    descriptor: dict[str, object] = {
        "class": f"{type(dtype).__module__}.{type(dtype).__qualname__}",
        "name": str(dtype),
        "repr": repr(dtype),
    }
    if isinstance(dtype, pd.CategoricalDtype):
        descriptor["ordered"] = bool(dtype.ordered)
        descriptor["categories"] = [_fingerprint_scalar(value) for value in dtype.categories.tolist()]
    return descriptor


def evidence_fingerprint(training_pairs: pd.DataFrame) -> str:
    if training_pairs.columns.duplicated().any():
        raise ValueError("training evidence contains duplicate column names")
    if any(not isinstance(column, str) for column in training_pairs.columns):
        raise ValueError("training evidence column names must be strings")

    columns = sorted(training_pairs.columns)
    schema = [{"name": column, "dtype": _fingerprint_dtype(training_pairs[column])} for column in columns]
    rows = [
        [_fingerprint_scalar(value) for value in row]
        for row in training_pairs.loc[:, columns].itertuples(index=False, name=None)
    ]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    payload = json.dumps({"schema": schema, "rows": rows}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(payload.encode("utf-8")).hexdigest()


def model_version_manifest(
    *,
    version_id: str,
    training_cutoff: str,
    training_pairs: pd.DataFrame,
    horizons: list[int],
    feature_definition: dict[str, object],
    parameters: dict[str, object],
    hyperparameters: dict[str, object],
    evaluation_start: str,
    evaluation_end: str,
    status: str = "candidate",
) -> dict[str, object]:
    if not version_id.strip():
        raise ValueError("version_id is required")
    invalid = sorted(set(horizons) - set(HORIZONS))
    if invalid:
        raise ValueError(f"unsupported horizons: {invalid}")
    cutoff = _required_utc(training_cutoff, "training_cutoff")
    start = _required_utc(evaluation_start, "evaluation_start")
    end = _required_utc(evaluation_end, "evaluation_end")
    if not cutoff < start <= end:
        raise ValueError("require training_cutoff < evaluation_start <= evaluation_end")
    if not training_pairs.empty:
        raise ValueError(V1_HORIZON_PROVENANCE_BLOCKER)

    return {
        "schema_version": SCHEMA_VERSION,
        "version_id": version_id,
        "training_cutoff": cutoff.isoformat(),
        "freeze_time": _immutable_freeze_time().isoformat(),
        "freeze_commit": _immutable_freeze_commit(),
        "evidence_fingerprint": evidence_fingerprint(training_pairs),
        "training_rows": 0,
        "horizons": sorted(int(h) for h in horizons),
        "feature_definition": feature_definition,
        "parameters": parameters,
        "hyperparameters": hyperparameters,
        "evaluation_start": start.isoformat(),
        "evaluation_end": end.isoformat(),
        "result": None,
        "promotion_status": status,
        "immutable_after_evaluation_start": True,
        "shadow_v1_training_allowed": False,
    }


def frozen_baseline_evaluator(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    horizon: int,
) -> dict[str, object]:
    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    _assert_exact_schema(claims, outcomes)
    _validate_claim_archive(claims)
    _validate_outcome_archive(claims, outcomes)
    raw = outcomes.loc[pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)] if not outcomes.empty else outcomes
    return {
        "horizon_sessions": horizon,
        "status": "insufficient_evidence",
        "N": 0,
        "raw_archive_outcomes": int(len(raw)),
        "support_regions": 0,
        "groups": {},
        "blockers": [V1_HORIZON_PROVENANCE_BLOCKER, STATISTICAL_CONTEXT_BLOCKER],
        "notes": {
            "frozen_baseline_only": True,
            "no_adaptive_weights": True,
            "no_scalar_confidence": True,
            "no_robust_interval_claimed": True,
            "shadow_v1_outcomes_not_certified": True,
        },
    }


def _has_multiple_genuine_walkforward_epochs(evaluations: list[dict[str, object]]) -> bool:
    if len(evaluations) < 2:
        return False
    parsed: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
    for evaluation in evaluations:
        version_id = str(evaluation.get("version_id") or "").strip()
        start = pd.to_datetime(evaluation.get("evaluation_start"), errors="coerce", utc=True)
        end = pd.to_datetime(evaluation.get("evaluation_end"), errors="coerce", utc=True)
        if not version_id or pd.isna(start) or pd.isna(end):
            return False
        start_day = pd.Timestamp(start).normalize()
        end_day = pd.Timestamp(end).normalize()
        if start_day > end_day:
            return False
        parsed.append((version_id, start_day, end_day))
    ids = [item[0] for item in parsed]
    if len(set(ids)) != len(ids):
        return False
    ordered = sorted(parsed, key=lambda item: (item[1], item[2], item[0]))
    return all(current[1] > previous[2] for previous, current in zip(ordered, ordered[1:]))


def promotion_assessment(
    *,
    walkforward_evaluations: list[dict[str, object]],
    pit_leakage_audit_passed: bool,
    reproducible_versions: bool,
    robust_uncertainty_available: bool,
    concentration_check_passed: bool,
    temporal_stability_check_passed: bool,
    baseline_advantage_demonstrated: bool,
) -> dict[str, object]:
    gates = {
        "multiple_walkforward_evaluations": _has_multiple_genuine_walkforward_epochs(walkforward_evaluations),
        "pit_leakage_audit_passed": bool(pit_leakage_audit_passed),
        "reproducible_versions": bool(reproducible_versions),
        "robust_uncertainty_available": bool(robust_uncertainty_available),
        "concentration_check_passed": bool(concentration_check_passed),
        "temporal_stability_check_passed": bool(temporal_stability_check_passed),
        "baseline_advantage_demonstrated": bool(baseline_advantage_demonstrated),
    }
    passed = all(gates.values())
    return {
        "status": "eligible_for_separate_promotion_review" if passed else "insufficient_evidence",
        "gates": gates,
        "production_change_performed": False,
    }
