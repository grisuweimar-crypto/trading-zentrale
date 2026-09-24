from __future__ import annotations

"""Phase 5C progressive, horizon-specific research learning.

This module consumes only prospective Phase-5B shadow-v2 evidence.  It starts
learning separately for 5T/20T/40T/60T as each horizon accumulates enough of
its own fully matured outcomes.  Shorter-horizon labels are never substituted
for longer-horizon labels.

The first learning product is deliberately transparent: immutable, versioned
state-reliability tables.  Phase 5C does not create production Confidence,
production thresholds, portfolio actions, or a scalar 0-100 score.
"""

from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    SCHEMA_VERSION as V2_SCHEMA_VERSION,
    derive_peer_labels_v2,
    validate_v2_archives,
)
from scanner.reports.confidence_vnext_walkforward import evidence_fingerprint
from scanner.reports.selection_timing import HORIZONS


SCHEMA_VERSION = "phase5c_progressive_learning_v1"
FIXED_EVENT_SPACING_SESSIONS = 5
MIN_PILOT_OBSERVATION_DATES = 2
MIN_PILOT_DIRECTIONAL_ROWS = 20
MIN_PILOT_DIRECTIONAL_SYMBOLS = 10
MIN_ROBUST_SUPPORT_REGIONS = 2

SINGLE_STATE_FIELDS = (
    "selection_statistical_state",
    "agreement_state",
    "timing_model_state",
    "risk_model_state",
    "dq_selection_state",
    "dq_timing_state",
    "dq_risk_state",
)
MULTI_STATE_FIELDS = (
    "timing_matched_states",
    "risk_evidence_states",
)
TRAINING_FINGERPRINT_COLUMNS = (
    "claim_id",
    "snapshot_id",
    "as_of",
    "symbol",
    "horizon_sessions",
    "return",
    "peer_excess",
    "signed_peer_excess",
    "direction_hit",
    "adverse_excursion",
    "path_max_drawdown",
    *SINGLE_STATE_FIELDS,
    *MULTI_STATE_FIELDS,
)


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected Phase 5C source schema in {p}")
    return frame


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _forbidden_dates(value: object) -> set[str]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(item) for item in parsed}


def _required_utc(value: object, label: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ValueError(f"{label} must be a parseable UTC timestamp")
    return pd.Timestamp(parsed)


def claim_spacing_membership(claims: pd.DataFrame) -> set[str]:
    """Freeze 5-session membership from claims, never from outcome availability.

    A delayed outcome must not retroactively change which claim is eligible for
    learning.  Membership therefore depends only on immutable claim-time fields.
    """

    empty_outcomes = pd.DataFrame(columns=OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, empty_outcomes)
    if claims.empty:
        return set()

    work = claims.loc[
        claims["outcome_eligibility"].astype(str).eq("eligible")
        & claims["cooldown_context_complete"].map(_parse_bool)
    ].copy()
    work["_start"] = pd.to_datetime(work["start_market_date"], errors="coerce").dt.normalize()
    work = work.loc[work["_start"].notna()].copy()

    keep: set[str] = set()
    for (_, _), group in work.groupby(["horizon_sessions", "symbol"], sort=False):
        last_start: str | None = None
        ordered = group.sort_values(["_start", "snapshot_id", "claim_id"], kind="mergesort")
        for _, row in ordered.iterrows():
            start_text = pd.Timestamp(row["_start"]).date().isoformat()
            forbidden = _forbidden_dates(row.get("cooldown_forbidden_start_dates"))
            if last_start is None or last_start not in forbidden:
                keep.add(str(row["claim_id"]))
                last_start = start_text
    return keep


def progressive_training_frame(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    horizon: int,
    knowable_by: str | None = None,
) -> pd.DataFrame:
    """Return only claim-spaced, fully matured evidence knowable by the cutoff."""

    if horizon not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon}")
    validate_v2_archives(claims, outcomes)
    if claims.empty or outcomes.empty:
        return pd.DataFrame()

    membership = claim_spacing_membership(claims)
    derived = derive_peer_labels_v2(claims, outcomes)
    if derived.empty:
        return derived

    availability = outcomes[["claim_id", "evaluated_at", "end_market_date"]].copy()
    frame = derived.merge(availability, on="claim_id", how="inner", validate="one_to_one")
    frame = frame.loc[
        frame["claim_id"].astype(str).isin(membership)
        & pd.to_numeric(frame["horizon_sessions"], errors="coerce").eq(horizon)
    ].copy()
    frame["_evaluated_at"] = pd.to_datetime(frame["evaluated_at"], errors="coerce", utc=True)
    frame = frame.loc[frame["_evaluated_at"].notna()].copy()

    if knowable_by is not None:
        cutoff = _required_utc(knowable_by, "knowable_by")
        frame = frame.loc[frame["_evaluated_at"].le(cutoff)].copy()

    return (
        frame.sort_values(["_evaluated_at", "as_of", "symbol", "claim_id"], kind="mergesort")
        .drop(columns=["_evaluated_at"])
        .reset_index(drop=True)
    )


def _support_regions(frame: pd.DataFrame, horizon: int) -> int:
    """Conservative observation-date support under the inherited 2 x H rule."""

    if frame.empty:
        return 0
    dates = sorted(
        pd.Timestamp(value).normalize()
        for value in pd.to_datetime(frame["as_of"], errors="coerce").dropna().unique()
    )
    if not dates:
        return 0
    block_length = max(1, 2 * int(horizon))
    count = 0
    last: int | None = None
    for position in range(len(dates)):
        if last is None or position - last >= block_length:
            count += 1
            last = position
    return count


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce") if column in frame.columns else pd.Series(dtype=float)


def _state_summary(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    directional = frame.loc[_numeric(frame, "direction_hit").notna()].copy()
    signed = _numeric(directional, "signed_peer_excess").dropna()
    hit = _numeric(directional, "direction_hit").dropna()
    adverse = _numeric(frame, "adverse_excursion").dropna()
    drawdown = _numeric(frame, "path_max_drawdown").dropna()
    return {
        "N": int(len(frame)),
        "directional_N": int(len(directional)),
        "symbols": int(frame["symbol"].astype(str).nunique()) if len(frame) else 0,
        "observation_dates": int(pd.to_datetime(frame["as_of"], errors="coerce").nunique()) if len(frame) else 0,
        "support_regions": int(_support_regions(frame, horizon)),
        "direction_hit_rate": float(hit.mean()) if len(hit) else None,
        "mean_signed_peer_excess": float(signed.mean()) if len(signed) else None,
        "median_signed_peer_excess": float(signed.median()) if len(signed) else None,
        "mean_adverse_excursion": float(adverse.mean()) if len(adverse) else None,
        "mean_path_max_drawdown": float(drawdown.mean()) if len(drawdown) else None,
    }


def _single_state_table(frame: pd.DataFrame, field: str, horizon: int) -> dict[str, object]:
    if frame.empty:
        return {}
    values = frame[field].astype(str).replace("", "missing")
    return {
        state: _state_summary(frame.loc[values.eq(state)].copy(), horizon)
        for state in sorted(values.unique())
    }


def _multi_state_table(frame: pd.DataFrame, field: str, horizon: int) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for row in frame.to_dict("records"):
        raw = str(row.get(field) or "")
        states = sorted(set(value for value in raw.split("|") if value)) or ["missing"]
        for state in states:
            item = dict(row)
            item["_state"] = state
            rows.append(item)
    if not rows:
        return {}
    exploded = pd.DataFrame(rows)
    return {
        state: _state_summary(exploded.loc[exploded["_state"].eq(state)].copy(), horizon)
        for state in sorted(exploded["_state"].astype(str).unique())
    }


def horizon_readiness(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    directional = frame.loc[_numeric(frame, "direction_hit").notna()].copy() if not frame.empty else frame
    observation_dates = int(pd.to_datetime(frame["as_of"], errors="coerce").nunique()) if not frame.empty else 0
    directional_symbols = int(directional["symbol"].astype(str).nunique()) if not directional.empty else 0
    directional_rows = int(len(directional))
    support_regions = int(_support_regions(frame, horizon))

    pilot_ready = (
        observation_dates >= MIN_PILOT_OBSERVATION_DATES
        and directional_rows >= MIN_PILOT_DIRECTIONAL_ROWS
        and directional_symbols >= MIN_PILOT_DIRECTIONAL_SYMBOLS
    )
    robust_base = pilot_ready and support_regions >= MIN_ROBUST_SUPPORT_REGIONS

    if robust_base:
        stage = "robust_learning_base"
    elif pilot_ready:
        stage = "pilot_learning"
    elif len(frame):
        stage = "mature_outcomes_accumulating"
    else:
        stage = "collecting"

    return {
        "stage": stage,
        "pilot_ready": bool(pilot_ready),
        "robust_learning_base": bool(robust_base),
        "training_rows": int(len(frame)),
        "directional_rows": directional_rows,
        "directional_symbols": directional_symbols,
        "observation_dates": observation_dates,
        "support_regions": support_regions,
        "requirements": {
            "minimum_pilot_observation_dates": MIN_PILOT_OBSERVATION_DATES,
            "minimum_pilot_directional_rows": MIN_PILOT_DIRECTIONAL_ROWS,
            "minimum_pilot_directional_symbols": MIN_PILOT_DIRECTIONAL_SYMBOLS,
            "minimum_robust_support_regions": MIN_ROBUST_SUPPORT_REGIONS,
            "fixed_event_spacing_sessions": FIXED_EVENT_SPACING_SESSIONS,
            "robust_block_length_observation_dates": 2 * int(horizon),
        },
    }


def _training_fingerprint(frame: pd.DataFrame) -> str:
    if frame.empty:
        empty = pd.DataFrame(columns=TRAINING_FINGERPRINT_COLUMNS)
        return evidence_fingerprint(empty)
    missing = [column for column in TRAINING_FINGERPRINT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Phase 5C training columns missing: {missing}")
    return evidence_fingerprint(frame.loc[:, TRAINING_FINGERPRINT_COLUMNS].copy())


def _version_id(horizon: int, fingerprint: str) -> str:
    payload = f"{SCHEMA_VERSION}|{horizon}|{fingerprint}"
    suffix = sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"phase5c-{horizon}T-{suffix}"


def _learned_tables(frame: pd.DataFrame, horizon: int) -> dict[str, object]:
    tables = {
        field: _single_state_table(frame, field, horizon)
        for field in SINGLE_STATE_FIELDS
    }
    tables.update({
        field: _multi_state_table(frame, field, horizon)
        for field in MULTI_STATE_FIELDS
    })
    return tables


def build_candidate_version(frame: pd.DataFrame, *, horizon: int) -> dict[str, object] | None:
    readiness = horizon_readiness(frame, horizon)
    if not readiness["pilot_ready"]:
        return None

    fingerprint = _training_fingerprint(frame)
    evaluated = pd.to_datetime(frame["evaluated_at"], errors="coerce", utc=True).dropna()
    as_of = pd.to_datetime(frame["as_of"], errors="coerce", utc=True).dropna()
    if evaluated.empty or as_of.empty:
        raise ValueError("Phase 5C candidate requires valid evidence chronology")

    return {
        "schema_version": SCHEMA_VERSION,
        "version_id": _version_id(horizon, fingerprint),
        "horizon_sessions": int(horizon),
        "source_schema_version": V2_SCHEMA_VERSION,
        "training_cutoff": evaluated.max().isoformat(),
        "training_claim_start": as_of.min().date().isoformat(),
        "training_claim_end": as_of.max().date().isoformat(),
        "evidence_fingerprint": fingerprint,
        "training_rows": int(len(frame)),
        "readiness": readiness,
        "learned_state_reliability": _learned_tables(frame, horizon),
        "evaluation": {
            "status": "awaiting_strictly_future_evidence",
            "training_rows_may_never_be_reused_for_evaluation": True,
            "evaluation_claims_must_be_after_training_cutoff": True,
        },
        "semantics": {
            "research_only": True,
            "horizon_specific_labels_only": True,
            "shorter_horizon_labels_borrowed": False,
            "claim_first_spacing_membership": True,
            "state_names_assumed_ordinal": False,
            "scalar_confidence_mapping_created": False,
            "adaptive_production_weights_created": False,
            "production_confidence_changed": False,
            "portfolio_or_depot_watch_changed": False,
        },
    }


def _read_versions(path: str | Path) -> list[dict[str, object]]:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    versions: list[dict[str, object]] = []
    for line_no, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        item = json.loads(line)
        if item.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"unexpected Phase 5C version schema at line {line_no}")
        versions.append(item)
    ids = [str(item.get("version_id") or "") for item in versions]
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        raise ValueError("Phase 5C model-version archive is not unique")
    return versions


def _write_versions(path: str | Path, versions: list[dict[str, object]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(
        json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n"
        for item in versions
    )
    p.write_text(text, encoding="utf-8")


def run_progressive_learning(
    claims_path: str | Path,
    outcomes_path: str | Path,
    versions_path: str | Path,
    report_path: str | Path,
) -> dict[str, object]:
    claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, outcomes)
    versions = _read_versions(versions_path)
    known_ids = {str(item["version_id"]) for item in versions}

    horizons: dict[str, object] = {}
    new_versions: list[str] = []
    for horizon in HORIZONS:
        frame = progressive_training_frame(claims, outcomes, horizon=horizon)
        readiness = horizon_readiness(frame, horizon)
        candidate = build_candidate_version(frame, horizon=horizon)
        version_id = None
        if candidate is not None:
            version_id = str(candidate["version_id"])
            if version_id not in known_ids:
                versions.append(candidate)
                known_ids.add(version_id)
                new_versions.append(version_id)

        horizon_versions = [
            str(item["version_id"])
            for item in versions
            if int(item.get("horizon_sessions", -1)) == horizon
        ]
        horizons[str(horizon)] = {
            **readiness,
            "latest_candidate_version": horizon_versions[-1] if horizon_versions else None,
            "candidate_version_from_current_evidence": version_id,
            "version_count": int(len(horizon_versions)),
        }

    _write_versions(versions_path, versions)
    result = {
        "phase": "5C_progressive_learning",
        "schema_version": SCHEMA_VERSION,
        "status": "progressive_learning_active" if any(item["pilot_ready"] for item in horizons.values()) else "collecting_prospective_evidence",
        "source_schema_version": V2_SCHEMA_VERSION,
        "claims": int(len(claims)),
        "mature_outcomes": int(len(outcomes)),
        "horizons": horizons,
        "model_versions": int(len(versions)),
        "new_model_versions": new_versions,
        "semantics": {
            "research_only": True,
            "dynamic_horizon_specific_learning": True,
            "learning_waits_for_own_mature_horizon_labels": True,
            "shorter_horizon_labels_borrowed": False,
            "fixed_event_spacing_sessions": FIXED_EVENT_SPACING_SESSIONS,
            "claim_first_spacing_membership": True,
            "overlapping_forward_windows_treated_as_independent": False,
            "state_names_assumed_ordinal": False,
            "scalar_confidence_mapping_created": False,
            "adaptive_production_weights_created": False,
            "production_confidence_changed": False,
            "portfolio_or_depot_watch_changed": False,
        },
    }
    output = Path(report_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result
