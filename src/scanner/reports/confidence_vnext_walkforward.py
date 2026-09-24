from __future__ import annotations

"""Phase 5A readiness, purged walk-forward contract, and frozen-baseline evaluator.

Research-only. This module never changes production Confidence, Selection,
Timing, Probability, Risk, portfolio logic, or trading decisions.

The Phase-4E prospective stream is the only admissible outcome source. Training
uses only claims whose outcomes were fully mature and known by the model
training cutoff. Evaluation periods remain untouched until they end.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_prospective import (
    CLAIM_COLUMNS,
    OUTCOME_COLUMNS,
    derive_peer_labels,
)
from scanner.reports.selection_timing import HORIZONS


SCHEMA_VERSION = "phase5_walkforward_v1"
PHASE5_FREEZE_COMMIT = "fb038c78682e4e9fe87f11a1c3c9472b763bd3c4"
PHASE5_FREEZE_TIME = "2026-09-24T01:12:14Z"
ORDINAL_EVIDENCE_STATES = ("robust", "directional_only", "immature", "mixed", "unavailable")
STATISTICAL_CONTEXT_COLUMNS = ("timing_statistical_state", "risk_statistical_state")


@dataclass(frozen=True)
class Phase5WalkForwardConfig:
    """Immutable pre-algorithm contract; no adaptive weights or scalar thresholds."""

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


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    values = frame[column].astype(str).replace("", "missing")
    return {str(k): int(v) for k, v in values.value_counts(dropna=False).sort_index().items()}


def _date_span(values: pd.Series) -> dict[str, str | None]:
    if values.empty:
        return {"start": None, "end": None}
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    parsed = parsed.loc[parsed.notna()]
    if parsed.empty:
        return {"start": None, "end": None}
    return {"start": parsed.min().isoformat(), "end": parsed.max().isoformat()}


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


def _validate_phase5_contract_config(config: Phase5WalkForwardConfig) -> None:
    fixed = {
        "schema_version": SCHEMA_VERSION,
        "uncertainty_block_multiplier": 2,
        "minimum_time_separated_support_regions": 2,
        "fixed_event_spacing_sessions": 5,
        "require_statistical_context_for_adaptation": True,
    }
    observed = asdict(config)
    mismatches = {
        key: (observed.get(key), value)
        for key, value in fixed.items()
        if observed.get(key) != value
    }
    if mismatches:
        raise ValueError(f"Phase 5A contract constants are immutable: {mismatches}")


def _assert_exact_shadow_schema(claims: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    if list(claims.columns) != list(CLAIM_COLUMNS):
        raise ValueError("claims do not match the immutable Phase-4E claim schema")
    if list(outcomes.columns) != list(OUTCOME_COLUMNS):
        raise ValueError("outcomes do not match the immutable Phase-4E outcome schema")


def _assert_post_freeze_claims(claims: pd.DataFrame) -> None:
    """Reject any evidence not provably generated strictly after the Phase-5 freeze."""

    if claims.empty:
        return
    freeze = _immutable_freeze_time()
    generated = pd.to_datetime(claims["generated_at"], errors="coerce", utc=True)
    invalid = generated.isna() | generated.le(freeze)
    if invalid.any():
        examples = claims.loc[invalid, "claim_id"].astype(str).head(3).tolist()
        raise ValueError(f"pre-freeze or unprovable Phase 5 claims are forbidden: {examples}")


def _statistical_context_complete(claims: pd.DataFrame) -> bool:
    """Require Statistical Context to be part of the immutable claim schema.

    Phase4E shadow-v1 does not archive claim-level Timing/Risk Phase-4C ordinal
    states. Merely appending similarly named DataFrame columns must never turn
    that missing evidence into an integrity-bound claim. A future archive
    schema must add the fields to its immutable claim schema and claim hash
    before this gate can become true.
    """

    if claims.empty:
        return False
    if not set(STATISTICAL_CONTEXT_COLUMNS).issubset(set(CLAIM_COLUMNS)):
        return False
    valid = set(ORDINAL_EVIDENCE_STATES)
    return all(
        claims[column].astype(str).map(lambda value: value in valid).all()
        for column in STATISTICAL_CONTEXT_COLUMNS
    )


def _validate_outcome_archive(claims: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    """Validate that every stored outcome is a genuine matured Phase-4E label."""

    if outcomes.empty:
        return
    if claims["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in readiness evidence")
    if outcomes["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in matured outcomes")

    unknown = set(outcomes["claim_id"].astype(str)) - set(claims["claim_id"].astype(str))
    if unknown:
        raise ValueError("Phase 4E outcomes exist without matching immutable claims")

    claim_info = claims[
        [
            "claim_id",
            "as_of",
            "symbol",
            "horizon_sessions",
            "start_market_date",
            "outcome_eligibility",
        ]
    ].copy()
    joined = outcomes.merge(
        claim_info,
        on="claim_id",
        how="left",
        validate="one_to_one",
        suffixes=("_outcome", "_claim"),
    )

    evaluated = pd.to_datetime(joined["evaluated_at"], errors="coerce", utc=True)
    end_market = pd.to_datetime(joined["end_market_date"], errors="coerce", utc=True).dt.normalize()
    start_claim = pd.to_datetime(joined["start_market_date_claim"], errors="coerce", utc=True).dt.normalize()
    start_outcome = pd.to_datetime(joined["start_market_date_outcome"], errors="coerce", utc=True).dt.normalize()
    as_of_claim = pd.to_datetime(joined["as_of_claim"], errors="coerce", utc=True).dt.normalize()
    as_of_outcome = pd.to_datetime(joined["as_of_outcome"], errors="coerce", utc=True).dt.normalize()
    h_claim = pd.to_numeric(joined["horizon_sessions_claim"], errors="coerce")
    h_outcome = pd.to_numeric(joined["horizon_sessions_outcome"], errors="coerce")

    invalid = (
        ~joined["outcome_eligibility"].astype(str).eq("eligible")
        | evaluated.isna()
        | end_market.isna()
        | start_claim.isna()
        | start_outcome.isna()
        | as_of_claim.isna()
        | as_of_outcome.isna()
        | ~start_claim.eq(start_outcome)
        | start_claim.gt(as_of_claim)
        | end_market.le(as_of_claim)
        | evaluated.dt.normalize().le(end_market)
        | ~as_of_claim.eq(as_of_outcome)
        | ~joined["symbol_claim"].astype(str).eq(joined["symbol_outcome"].astype(str))
        | h_claim.isna()
        | h_outcome.isna()
        | ~h_claim.eq(h_outcome)
        | ~h_claim.isin(HORIZONS)
    )
    if invalid.any():
        examples = joined.loc[invalid, "claim_id"].astype(str).head(3).tolist()
        raise ValueError(f"invalid Phase 4E outcome chronology/provenance: {examples}")


def _apply_fixed_event_spacing(claims: pd.DataFrame, sessions: int = 5) -> pd.DataFrame:
    """Apply the inherited per-symbol five-session cooldown to eligible claims.

    Session positions come from immutable claim-time start sessions represented
    by that symbol in the archive, never from calendar-day distance. Missing
    scanner publications therefore make spacing conservative rather than
    inventing unobserved sessions. Multiple reruns on one market session share
    one position and only the first eligible occurrence can survive.
    """

    if claims.empty:
        return claims.copy()
    eligible = claims.loc[claims["outcome_eligibility"].astype(str).eq("eligible")].copy()
    if eligible.empty:
        return eligible
    eligible["_session"] = pd.to_datetime(
        eligible["start_market_date"], errors="coerce", utc=True
    ).dt.normalize()
    eligible = eligible.loc[eligible["_session"].notna()].copy()
    if eligible.empty:
        return eligible.drop(columns=["_session"], errors="ignore")

    keep: list[int] = []
    sort_columns = ["_session", "as_of", "snapshot_id", "claim_id"]
    for _, group in eligible.groupby("symbol", sort=False):
        session_dates = sorted(pd.Timestamp(day) for day in group["_session"].unique())
        positions = {day: pos for pos, day in enumerate(session_dates)}
        last_position: int | None = None
        for idx, row in group.sort_values(sort_columns, kind="mergesort").iterrows():
            position = positions.get(pd.Timestamp(row["_session"]))
            if position is None:
                continue
            if last_position is None or position - last_position >= sessions:
                keep.append(idx)
                last_position = position

    if not keep:
        return eligible.iloc[0:0].drop(columns=["_session"], errors="ignore")
    return (
        eligible.loc[keep]
        .drop(columns=["_session"], errors="ignore")
        .sort_values(["as_of", "symbol", "snapshot_id", "claim_id"], kind="mergesort")
        .reset_index(drop=True)
    )


def _time_separated_outcome_support_regions(
    spaced_claims: pd.DataFrame,
    spaced_outcomes: pd.DataFrame,
    block_length: int,
) -> int:
    """Count support regions after event spacing using observation positions."""

    if spaced_claims.empty or spaced_outcomes.empty or block_length < 1:
        return 0
    if spaced_claims["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in spaced readiness evidence")
    if spaced_outcomes["claim_id"].astype(str).duplicated().any():
        raise ValueError("duplicate claim_id in spaced matured outcomes")

    baseline_dates = sorted(
        pd.Timestamp(day).normalize()
        for day in pd.to_datetime(spaced_claims["as_of"], errors="coerce").dropna().unique()
    )
    if not baseline_dates:
        return 0
    positions = {day: index for index, day in enumerate(baseline_dates)}

    observed = spaced_claims[["claim_id", "as_of"]].copy()
    observed["_obs_date"] = pd.to_datetime(observed["as_of"], errors="coerce").dt.normalize()
    matured = spaced_outcomes[["claim_id", "start_market_date", "end_market_date"]].merge(
        observed[["claim_id", "_obs_date"]],
        on="claim_id",
        how="inner",
        validate="one_to_one",
    )
    matured["_start"] = pd.to_datetime(matured["start_market_date"], errors="coerce").dt.normalize()
    matured["_end"] = pd.to_datetime(matured["end_market_date"], errors="coerce").dt.normalize()
    matured = matured.loc[
        matured["_obs_date"].notna()
        & matured["_start"].notna()
        & matured["_end"].notna()
        & matured["_end"].ge(matured["_start"])
    ].copy()
    if matured.empty:
        return 0

    cohorts = (
        matured.groupby("_obs_date", as_index=False)
        .agg(_start=("_start", "min"), _end=("_end", "max"))
        .sort_values("_obs_date", kind="mergesort")
    )

    regions = 0
    last_position: int | None = None
    last_end: pd.Timestamp | None = None
    for obs_date_value, start_value, end_value in cohorts[["_obs_date", "_start", "_end"]].itertuples(
        index=False, name=None
    ):
        obs_date = pd.Timestamp(obs_date_value)
        position = positions.get(obs_date)
        if position is None:
            continue
        start = pd.Timestamp(start_value)
        end = pd.Timestamp(end_value)
        separated = last_position is None or position - last_position >= int(block_length)
        non_overlapping = last_end is None or start > last_end
        if separated and non_overlapping:
            regions += 1
            last_position = position
            last_end = end
    return regions


def _distribution_audit(claims: pd.DataFrame) -> dict[str, object]:
    statistical: dict[str, object] = {
        "selection_phase4c_state": _value_counts(claims, "selection_state"),
        "timing_phase4c_state": {
            "status": "not_archived_claim_level",
            "reason": "phase4e_shadow_v1 does not integrity-bind claim-level Timing Phase-4C ordinal state",
        },
        "risk_phase4c_state": {
            "status": "not_archived_claim_level",
            "reason": "phase4e_shadow_v1 does not integrity-bind claim-level Risk Phase-4C ordinal state",
        },
    }
    return {
        "data_quality": {
            "selection": _value_counts(claims, "dq_selection_state"),
            "timing": _value_counts(claims, "dq_timing_state"),
            "risk": _value_counts(claims, "dq_risk_state"),
        },
        "statistical_confidence": statistical,
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
    """Audit prospective readiness under the immutable Phase-5A contract."""

    _validate_phase5_contract_config(config)
    _assert_exact_shadow_schema(claims, outcomes)
    effective_freeze_time = _immutable_freeze_time(freeze_time)
    effective_freeze_commit = _immutable_freeze_commit(freeze_commit)
    _assert_post_freeze_claims(claims)
    _validate_outcome_archive(claims, outcomes)

    statistical_context_complete = _statistical_context_complete(claims)
    horizons: dict[str, object] = {}
    any_walkforward_ready = False
    blockers: set[str] = set()

    if not statistical_context_complete:
        blockers.add("claim_level_phase4c_timing_and_risk_states_not_archived")

    for horizon in HORIZONS:
        c = claims.loc[
            pd.to_numeric(claims["horizon_sessions"], errors="coerce").eq(horizon)
        ].copy() if not claims.empty else _empty_claims()
        o = outcomes.loc[
            pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)
        ].copy() if not outcomes.empty else _empty_outcomes()

        eligibility = c["outcome_eligibility"].astype(str) if not c.empty else pd.Series(dtype=str)
        spaced_claims = _apply_fixed_event_spacing(c, config.fixed_event_spacing_sessions)
        spaced_ids = set(spaced_claims["claim_id"].astype(str)) if not spaced_claims.empty else set()
        spaced_outcomes = o.loc[o["claim_id"].astype(str).isin(spaced_ids)].copy() if not o.empty else o
        mature_ids = set(spaced_outcomes["claim_id"].astype(str)) if not spaced_outcomes.empty else set()
        mature_claims = spaced_claims.loc[
            spaced_claims["claim_id"].astype(str).isin(mature_ids)
        ].copy() if not spaced_claims.empty else spaced_claims

        block_length = int(config.uncertainty_block_multiplier * horizon)
        regions = _time_separated_outcome_support_regions(
            spaced_claims,
            spaced_outcomes,
            block_length,
        )
        support_ok = regions >= config.minimum_time_separated_support_regions
        snapshots = int(c["snapshot_id"].astype(str).replace("", np.nan).nunique()) if not c.empty else 0
        mature_snapshots = int(
            mature_claims["snapshot_id"].astype(str).replace("", np.nan).nunique()
        ) if not mature_claims.empty else 0
        symbols = int(c["symbol"].astype(str).replace("", np.nan).nunique()) if not c.empty else 0
        mature_symbols = int(
            spaced_outcomes["symbol"].astype(str).replace("", np.nan).nunique()
        ) if not spaced_outcomes.empty else 0

        horizon_ready = bool(
            len(spaced_outcomes) > 0
            and mature_snapshots >= 2
            and mature_symbols >= 2
            and support_ok
            and statistical_context_complete
        )
        any_walkforward_ready = any_walkforward_ready or horizon_ready

        if len(c) == 0:
            blockers.add(f"{horizon}T_no_claims")
        elif len(o) == 0:
            blockers.add(f"{horizon}T_no_mature_outcomes")
        if mature_snapshots < 2:
            blockers.add(f"{horizon}T_insufficient_mature_snapshot_cohorts")
        if not support_ok:
            blockers.add(f"{horizon}T_insufficient_time_separated_support")

        horizons[str(horizon)] = {
            "claims": int(len(c)),
            "evaluable_claims": int(eligibility.eq("eligible").sum()),
            "unevaluable_claims": int(eligibility.eq("unevaluable").sum()),
            "mature_outcomes": int(len(o)),
            "spaced_evaluable_claims": int(len(spaced_claims)),
            "spaced_mature_outcomes": int(len(spaced_outcomes)),
            "symbols": symbols,
            "mature_symbols": mature_symbols,
            "snapshots": snapshots,
            "mature_snapshots": mature_snapshots,
            "claim_time_span": _date_span(c["as_of"]) if not c.empty else {"start": None, "end": None},
            "mature_outcome_time_span": _date_span(o["evaluated_at"]) if not o.empty else {"start": None, "end": None},
            "outcome_end_span": _date_span(o["end_market_date"]) if not o.empty else {"start": None, "end": None},
            "non_overlapping_outcome_support_regions": regions,
            "robust_uncertainty_block_length_sessions": block_length,
            "fixed_event_spacing_sessions": config.fixed_event_spacing_sessions,
            "distributions": _distribution_audit(c),
            "walkforward_evaluation_ready": horizon_ready,
        }

    return {
        "phase": "5A_readiness_walkforward_contract",
        "schema_version": config.schema_version,
        "status": "ready_for_walkforward_evaluation" if any_walkforward_ready else "insufficient_evidence",
        "freeze": {
            "main_commit": effective_freeze_commit,
            "time": effective_freeze_time.isoformat(),
            "immutable": True,
            "enforced_on_claim_generated_at": True,
        },
        "shadow_archive": {
            "claims": int(len(claims)),
            "mature_outcomes": int(len(outcomes)),
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
            "fixed_event_spacing_applied_before_evaluation": True,
            "overlapping_forward_windows_treated_as_independent": False,
            "robust_uncertainty": "circular moving observation-date bootstrap with full date clusters and effective block length 2 x horizon",
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
    """Return only post-freeze, spaced evidence fully knowable before evaluation."""

    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    _assert_exact_shadow_schema(claims, outcomes)
    cutoff = _required_utc(training_cutoff, "training_cutoff")
    eval_start = _required_utc(evaluation_start, "evaluation_start")
    if cutoff >= eval_start:
        raise ValueError("training_cutoff must be before evaluation_start")
    _assert_post_freeze_claims(claims)
    _validate_outcome_archive(claims, outcomes)
    if claims.empty or outcomes.empty:
        return pd.DataFrame()

    c = claims.loc[
        pd.to_numeric(claims["horizon_sessions"], errors="coerce").eq(horizon)
    ].copy()
    o = outcomes.loc[
        pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)
    ].copy()
    c = _apply_fixed_event_spacing(c, 5)
    if c.empty or o.empty:
        return pd.DataFrame()
    o = o.loc[o["claim_id"].astype(str).isin(set(c["claim_id"].astype(str)))].copy()
    if o.empty:
        return pd.DataFrame()

    merged = c.merge(
        o,
        on="claim_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_claim", "_outcome"),
    )
    evaluated = pd.to_datetime(merged["evaluated_at"], errors="coerce", utc=True)
    end_market = pd.to_datetime(merged["end_market_date"], errors="coerce", utc=True)
    generated = pd.to_datetime(merged["generated_at"], errors="coerce", utc=True)
    claim_as_of = pd.to_datetime(merged["as_of_claim"], errors="coerce", utc=True)
    freeze = _immutable_freeze_time()

    keep = (
        generated.notna()
        & generated.gt(freeze)
        & generated.le(cutoff)
        & evaluated.notna()
        & end_market.notna()
        & evaluated.dt.normalize().gt(end_market.dt.normalize())
        & evaluated.le(cutoff)
        & end_market.lt(eval_start.normalize())
        & claim_as_of.notna()
        & claim_as_of.lt(eval_start)
    )
    return merged.loc[keep].sort_values(
        ["as_of_claim", "snapshot_id", "symbol_claim"], kind="mergesort"
    ).reset_index(drop=True)


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
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "value": str(value),
    }


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
    """Hash every typed value and the exact column schema, including dtypes."""

    if training_pairs.columns.duplicated().any():
        raise ValueError("training evidence contains duplicate column names")
    if any(not isinstance(column, str) for column in training_pairs.columns):
        raise ValueError("training evidence column names must be strings")

    columns = sorted(training_pairs.columns)
    schema = [
        {"name": column, "dtype": _fingerprint_dtype(training_pairs[column])}
        for column in columns
    ]
    canonical_rows: list[list[dict[str, object]]] = []
    for row in training_pairs.loc[:, columns].itertuples(index=False, name=None):
        canonical_rows.append([_fingerprint_scalar(value) for value in row])
    canonical_rows.sort(
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    )
    payload = json.dumps(
        {"schema": schema, "rows": canonical_rows},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _validate_purged_training_pairs(
    training_pairs: pd.DataFrame,
    *,
    horizons: list[int],
    training_cutoff: pd.Timestamp,
    evaluation_start: pd.Timestamp,
) -> None:
    """Re-validate exact Phase-4 chronology at the immutable manifest boundary."""

    if training_pairs.empty:
        return
    required = {
        "claim_id",
        "generated_at",
        "evaluated_at",
        "end_market_date",
        "as_of_claim",
        "as_of_outcome",
        "start_market_date_claim",
        "start_market_date_outcome",
        "symbol_claim",
        "symbol_outcome",
        "horizon_sessions_claim",
        "horizon_sessions_outcome",
    }
    missing = required - set(training_pairs.columns)
    if missing:
        raise ValueError(f"training evidence cannot prove purging: missing {sorted(missing)}")
    if training_pairs["claim_id"].astype(str).duplicated().any():
        raise ValueError("training evidence violates purged walk-forward contract: duplicate claim_id")

    freeze = _immutable_freeze_time()
    generated = pd.to_datetime(training_pairs["generated_at"], errors="coerce", utc=True)
    evaluated = pd.to_datetime(training_pairs["evaluated_at"], errors="coerce", utc=True)
    end_market = pd.to_datetime(training_pairs["end_market_date"], errors="coerce", utc=True).dt.normalize()
    claim_as_of = pd.to_datetime(training_pairs["as_of_claim"], errors="coerce", utc=True).dt.normalize()
    outcome_as_of = pd.to_datetime(training_pairs["as_of_outcome"], errors="coerce", utc=True).dt.normalize()
    start_claim = pd.to_datetime(training_pairs["start_market_date_claim"], errors="coerce", utc=True).dt.normalize()
    start_outcome = pd.to_datetime(training_pairs["start_market_date_outcome"], errors="coerce", utc=True).dt.normalize()
    claim_horizons = pd.to_numeric(training_pairs["horizon_sessions_claim"], errors="coerce")
    outcome_horizons = pd.to_numeric(training_pairs["horizon_sessions_outcome"], errors="coerce")
    allowed_horizons = set(int(h) for h in horizons)

    claim_horizon_valid = claim_horizons.map(
        lambda value: pd.notna(value) and float(value).is_integer() and int(value) in allowed_horizons
    )
    outcome_horizon_valid = outcome_horizons.map(
        lambda value: pd.notna(value) and float(value).is_integer() and int(value) in allowed_horizons
    )

    invalid = (
        generated.isna()
        | generated.le(freeze)
        | generated.gt(training_cutoff)
        | evaluated.isna()
        | end_market.isna()
        | evaluated.dt.normalize().le(end_market)
        | evaluated.gt(training_cutoff)
        | end_market.ge(evaluation_start.normalize())
        | claim_as_of.isna()
        | outcome_as_of.isna()
        | claim_as_of.ge(evaluation_start.normalize())
        | ~claim_as_of.eq(outcome_as_of)
        | start_claim.isna()
        | start_outcome.isna()
        | ~start_claim.eq(start_outcome)
        | start_claim.gt(claim_as_of)
        | end_market.le(claim_as_of)
        | ~training_pairs["symbol_claim"].astype(str).eq(training_pairs["symbol_outcome"].astype(str))
        | ~claim_horizon_valid
        | ~outcome_horizon_valid
        | ~claim_horizons.eq(outcome_horizons)
    )
    if invalid.any():
        examples = training_pairs.loc[invalid, "claim_id"].astype(str).head(3).tolist()
        raise ValueError(f"training evidence violates purged walk-forward contract: {examples}")


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
    invalid_horizons = sorted(set(horizons) - set(HORIZONS))
    if invalid_horizons:
        raise ValueError(f"unsupported horizons: {invalid_horizons}")
    start = _required_utc(evaluation_start, "evaluation_start")
    end = _required_utc(evaluation_end, "evaluation_end")
    cutoff = _required_utc(training_cutoff, "training_cutoff")
    if not cutoff < start <= end:
        raise ValueError("require training_cutoff < evaluation_start <= evaluation_end")

    _validate_purged_training_pairs(
        training_pairs,
        horizons=horizons,
        training_cutoff=cutoff,
        evaluation_start=start,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "version_id": version_id,
        "training_cutoff": cutoff.isoformat(),
        "freeze_time": _immutable_freeze_time().isoformat(),
        "freeze_commit": _immutable_freeze_commit(),
        "evidence_fingerprint": evidence_fingerprint(training_pairs),
        "training_rows": int(len(training_pairs)),
        "horizons": sorted(int(h) for h in horizons),
        "feature_definition": feature_definition,
        "parameters": parameters,
        "hyperparameters": hyperparameters,
        "evaluation_start": start.isoformat(),
        "evaluation_end": end.isoformat(),
        "result": None,
        "promotion_status": status,
        "immutable_after_evaluation_start": True,
    }


def frozen_baseline_evaluator(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    horizon: int,
) -> dict[str, object]:
    """Descriptive Phase-4 frozen-baseline reliability metrics after cooldown."""

    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    _assert_exact_shadow_schema(claims, outcomes)
    _assert_post_freeze_claims(claims)
    _validate_outcome_archive(claims, outcomes)
    if claims.empty or outcomes.empty:
        return {"horizon_sessions": horizon, "status": "insufficient_evidence", "N": 0, "groups": {}}

    h_claims = claims.loc[
        pd.to_numeric(claims["horizon_sessions"], errors="coerce").eq(horizon)
    ].copy()
    h_outcomes = outcomes.loc[
        pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)
    ].copy()
    if h_claims.empty or h_outcomes.empty:
        return {"horizon_sessions": horizon, "status": "insufficient_evidence", "N": 0, "groups": {}}

    # Peer labels remain based on the complete matured immutable snapshot cohort.
    derived = derive_peer_labels(h_claims, h_outcomes)
    spaced_claims = _apply_fixed_event_spacing(h_claims, 5)
    spaced_ids = set(spaced_claims["claim_id"].astype(str))
    spaced_outcomes = h_outcomes.loc[h_outcomes["claim_id"].astype(str).isin(spaced_ids)].copy()

    merged = spaced_outcomes.merge(
        spaced_claims[
            [
                "claim_id",
                "selection_state",
                "agreement_state",
                "dq_selection_state",
                "dq_timing_state",
                "dq_risk_state",
            ]
        ],
        on="claim_id",
        how="inner",
        validate="one_to_one",
    )
    merged = merged.merge(
        derived[["claim_id", "signed_peer_excess", "direction_hit"]],
        on="claim_id",
        how="left",
        validate="one_to_one",
    )
    for column in ("return", "adverse_excursion", "path_max_drawdown", "signed_peer_excess", "direction_hit"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")

    def summarize(group: pd.DataFrame) -> dict[str, object]:
        directional = group.loc[group["direction_hit"].notna()]
        return {
            "N": int(len(group)),
            "directional_N": int(len(directional)),
            "direction_hit_rate": float(directional["direction_hit"].mean()) if len(directional) else None,
            "mean_signed_peer_excess": float(directional["signed_peer_excess"].mean()) if len(directional) else None,
            "mean_adverse_excursion": float(group["adverse_excursion"].mean()) if len(group) else None,
            "mean_path_max_drawdown": float(group["path_max_drawdown"].mean()) if len(group) else None,
        }

    groups: dict[str, object] = {}
    for column in (
        "selection_state",
        "agreement_state",
        "dq_selection_state",
        "dq_timing_state",
        "dq_risk_state",
    ):
        groups[column] = {
            str(key): summarize(group)
            for key, group in merged.groupby(column, dropna=False, sort=True)
        }

    block_length = int(2 * horizon)
    return {
        "horizon_sessions": horizon,
        "status": "descriptive_only",
        "N": int(len(merged)),
        "support_regions": _time_separated_outcome_support_regions(
            spaced_claims,
            spaced_outcomes,
            block_length,
        ),
        "groups": groups,
        "notes": {
            "frozen_baseline_only": True,
            "fixed_event_spacing_sessions": 5,
            "peer_labels_use_complete_snapshot_cohort_before_spacing": True,
            "no_adaptive_weights": True,
            "no_scalar_confidence": True,
            "no_robust_interval_claimed": True,
        },
    }


def _has_multiple_genuine_walkforward_epochs(
    walkforward_evaluations: list[dict[str, object]],
) -> bool:
    if len(walkforward_evaluations) < 2:
        return False
    parsed: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
    for evaluation in walkforward_evaluations:
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

    version_ids = [item[0] for item in parsed]
    if len(set(version_ids)) != len(version_ids):
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
        "multiple_walkforward_evaluations": _has_multiple_genuine_walkforward_epochs(
            walkforward_evaluations
        ),
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
