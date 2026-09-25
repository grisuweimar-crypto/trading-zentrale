from __future__ import annotations

"""Phase 5D prospective walk-forward evaluation for frozen Phase-5C versions.

A Phase-5C candidate may only be evaluated on claims generated strictly after
its training cutoff.  A finalized epoch ends at the next version's training
cutoff for the same horizon.  The successor boundary is data-availability
based, not selected from performance.  The newest version remains provisional
until a successor exists.

Research-only: this module never changes production Confidence, scanner
weights, thresholds, R-codes, Depot-Watch, portfolio logic, or trading actions.
"""

from hashlib import sha256
from pathlib import Path
import json

import pandas as pd

from scanner.reports.confidence_vnext_frozen_baseline import (
    _baseline_dates,
    _bootstrap_summary,
)
from scanner.reports.confidence_vnext_progressive import (
    MULTI_STATE_FIELDS,
    SINGLE_STATE_FIELDS,
    SCHEMA_VERSION as PHASE5C_SCHEMA_VERSION,
    TRAINING_FINGERPRINT_COLUMNS,
    _learned_tables,
    _read_versions,
    progressive_training_frame,
)
from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    validate_v2_archives,
)
from scanner.reports.confidence_vnext_walkforward import evidence_fingerprint
from scanner.reports.selection_timing import HORIZONS


SCHEMA_VERSION = "phase5d_walkforward_evaluation_v1"
EVALUATION_FINGERPRINT_COLUMNS = (
    *TRAINING_FINGERPRINT_COLUMNS,
    "generated_at",
    "evaluated_at",
    "end_market_date",
)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha_payload(value: object) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected Phase 5D source schema in {p}")
    return frame


def _required_utc(value: object, label: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ValueError(f"{label} must be a parseable UTC timestamp")
    return pd.Timestamp(parsed)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _sign_persistent(left: object, right: object) -> bool | None:
    a = _optional_float(left)
    b = _optional_float(right)
    if a is None or b is None or a == 0.0 or b == 0.0:
        return None
    return bool((a > 0) == (b > 0))


def _delta(evaluation: object, training: object) -> float | None:
    e = _optional_float(evaluation)
    t = _optional_float(training)
    if e is None or t is None:
        return None
    return float(e - t)


def _state_persistence(
    training_tables: dict[str, object],
    evaluation_tables: dict[str, object],
) -> dict[str, object]:
    """Compare exact state groups without inventing an ordinal score."""

    result: dict[str, object] = {}
    for field in (*SINGLE_STATE_FIELDS, *MULTI_STATE_FIELDS):
        train = dict(training_tables.get(field) or {})
        test = dict(evaluation_tables.get(field) or {})
        field_result: dict[str, object] = {}
        for state in sorted(set(train) | set(test)):
            before = dict(train.get(state) or {})
            after = dict(test.get(state) or {})
            field_result[state] = {
                "training_N": int(before.get("N") or 0),
                "evaluation_N": int(after.get("N") or 0),
                "training_directional_N": int(before.get("directional_N") or 0),
                "evaluation_directional_N": int(after.get("directional_N") or 0),
                "direction_hit_rate_delta": _delta(
                    after.get("direction_hit_rate"),
                    before.get("direction_hit_rate"),
                ),
                "signed_peer_excess_sign_persistent": _sign_persistent(
                    before.get("mean_signed_peer_excess"),
                    after.get("mean_signed_peer_excess"),
                ),
                "mean_signed_peer_excess_delta": _delta(
                    after.get("mean_signed_peer_excess"),
                    before.get("mean_signed_peer_excess"),
                ),
                "mean_adverse_excursion_delta": _delta(
                    after.get("mean_adverse_excursion"),
                    before.get("mean_adverse_excursion"),
                ),
                "mean_path_max_drawdown_delta": _delta(
                    after.get("mean_path_max_drawdown"),
                    before.get("mean_path_max_drawdown"),
                ),
            }
        result[field] = field_result
    return result


def _overall_summary(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {
            "N": 0,
            "symbols": 0,
            "observation_dates": 0,
            "directional_N": 0,
            "direction_hit_rate": None,
            "mean_signed_peer_excess": None,
            "mean_adverse_excursion": None,
            "mean_path_max_drawdown": None,
        }

    work = frame.copy()
    for column in (
        "direction_hit",
        "signed_peer_excess",
        "adverse_excursion",
        "path_max_drawdown",
    ):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    directional = work.loc[work["direction_hit"].notna()].copy()
    signed = directional["signed_peer_excess"].dropna()
    return {
        "N": int(len(work)),
        "symbols": int(work["symbol"].astype(str).nunique()),
        "observation_dates": int(
            pd.to_datetime(work["as_of"], errors="coerce").nunique()
        ),
        "directional_N": int(len(directional)),
        "direction_hit_rate": (
            float(directional["direction_hit"].mean())
            if len(directional)
            else None
        ),
        "mean_signed_peer_excess": float(signed.mean()) if len(signed) else None,
        "mean_adverse_excursion": (
            float(work["adverse_excursion"].mean()) if len(work) else None
        ),
        "mean_path_max_drawdown": (
            float(work["path_max_drawdown"].mean()) if len(work) else None
        ),
    }


def _concentration(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {
            "max_symbol_share": None,
            "max_observation_date_share": None,
        }
    n = float(len(frame))
    symbol_share = frame["symbol"].astype(str).value_counts().max() / n
    dates = pd.to_datetime(frame["as_of"], errors="coerce").dt.normalize()
    date_share = dates.value_counts().max() / n if dates.notna().any() else None
    return {
        "max_symbol_share": float(symbol_share),
        "max_observation_date_share": (
            float(date_share) if date_share is not None else None
        ),
    }


def _evaluation_fingerprint(frame: pd.DataFrame) -> str:
    if frame.empty:
        return evidence_fingerprint(
            pd.DataFrame(columns=EVALUATION_FINGERPRINT_COLUMNS)
        )
    missing = [
        column
        for column in EVALUATION_FINGERPRINT_COLUMNS
        if column not in frame.columns
    ]
    if missing:
        raise ValueError(f"Phase 5D evaluation columns missing: {missing}")
    return evidence_fingerprint(
        frame.loc[:, EVALUATION_FINGERPRINT_COLUMNS].copy()
    )


def _epoch_frame(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    horizon: int,
    training_cutoff: str,
    evaluation_known_by: str,
) -> pd.DataFrame:
    """Select evidence strictly future to one frozen model version."""

    start = _required_utc(training_cutoff, "training_cutoff")
    end = _required_utc(evaluation_known_by, "evaluation_known_by")
    if end <= start:
        return pd.DataFrame()

    frame = progressive_training_frame(
        claims,
        outcomes,
        horizon=horizon,
        knowable_by=end.isoformat(),
    )
    if frame.empty:
        return frame

    claim_times = claims[["claim_id", "generated_at"]].copy()
    frame = frame.merge(
        claim_times,
        on="claim_id",
        how="left",
        validate="one_to_one",
    )
    generated = pd.to_datetime(frame["generated_at"], errors="coerce", utc=True)
    dependencies = pd.to_datetime(
        frame["peer_dependency_evaluated_at"], errors="coerce", utc=True
    )
    evaluated = pd.to_datetime(frame["evaluated_at"], errors="coerce", utc=True)
    if generated.isna().any() or dependencies.isna().any() or evaluated.isna().any():
        raise ValueError("Phase 5D evaluation chronology failure")

    frame = frame.loc[
        generated.gt(start)
        & generated.le(end)
        & dependencies.le(end)
        & evaluated.le(end)
    ].copy()
    return frame.sort_values(
        ["generated_at", "as_of", "symbol", "claim_id"], kind="mergesort"
    ).reset_index(drop=True)


def _evaluation_hash(record_without_hash: dict[str, object]) -> str:
    return _sha_payload(record_without_hash)


def _validate_evaluations(records: list[dict[str, object]]) -> None:
    expected_previous = ""
    pairs: set[tuple[str, str]] = set()
    for line_no, record in enumerate(records, start=1):
        if record.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"unexpected Phase 5D schema at line {line_no}")
        supplied = str(record.get("evaluation_sha256") or "")
        if len(supplied) != 64:
            raise ValueError(f"invalid Phase 5D hash at line {line_no}")
        payload = dict(record)
        payload.pop("evaluation_sha256", None)
        if supplied != _evaluation_hash(payload):
            raise ValueError(f"Phase 5D evaluation integrity failure at line {line_no}")
        if str(record.get("previous_evaluation_sha256") or "") != expected_previous:
            raise ValueError(f"Phase 5D evaluation chain failure at line {line_no}")
        pair = (
            str(record.get("version_id") or ""),
            str(record.get("successor_version_id") or ""),
        )
        if not pair[0] or not pair[1] or pair in pairs:
            raise ValueError("Phase 5D evaluation epoch identity failure")
        pairs.add(pair)
        expected_previous = supplied


def _read_evaluations(path: str | Path) -> list[dict[str, object]]:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    records: list[dict[str, object]] = []
    for line_no, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid Phase 5D evaluation JSON at line {line_no}"
            ) from exc
    _validate_evaluations(records)
    return records


def _write_evaluations(path: str | Path, records: list[dict[str, object]]) -> None:
    _validate_evaluations(records)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        "".join(_canonical_json(record) + "\n" for record in records),
        encoding="utf-8",
    )


def _validate_previous_report_anchor(
    report_path: str | Path,
    evaluations: list[dict[str, object]],
) -> None:
    path = Path(report_path)
    if not path.exists() or path.stat().st_size == 0:
        return
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("invalid previous Phase 5D report") from exc
    if previous.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected previous Phase 5D report schema")

    prior_count = int(previous.get("finalized_evaluations", 0))
    prior_tip = str(previous.get("evaluation_chain_tip") or "")
    if prior_count < 0 or len(evaluations) < prior_count:
        raise ValueError("Phase 5D evaluation archive was truncated")
    if prior_count == 0:
        if prior_tip:
            raise ValueError(
                "Phase 5D zero-evaluation report has a non-empty chain tip"
            )
        return
    observed_tip = str(
        evaluations[prior_count - 1].get("evaluation_sha256") or ""
    )
    if observed_tip != prior_tip:
        raise ValueError("Phase 5D historical evaluation prefix changed")


def _build_finalized_record(
    version: dict[str, object],
    successor: dict[str, object],
    frame: pd.DataFrame,
    *,
    previous_evaluation_sha256: str,
    bootstrap_reps: int,
    random_seed: int,
) -> dict[str, object] | None:
    if frame.empty:
        return None

    horizon = int(version["horizon_sessions"])
    fingerprint = _evaluation_fingerprint(frame)
    tables = _learned_tables(frame, horizon)
    dates = _baseline_dates(frame)
    robust = _bootstrap_summary(
        frame,
        dates,
        horizon,
        reps=bootstrap_reps,
        seed=random_seed + horizon,
    )
    status = (
        "robust_evaluation_available"
        if int(robust.get("support_regions") or 0) >= 2
        else "descriptive_evaluation"
    )
    generated = pd.to_datetime(frame["generated_at"], errors="coerce", utc=True)
    evaluated = pd.to_datetime(frame["evaluated_at"], errors="coerce", utc=True)
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "evaluation_id": _sha_payload(
            {
                "version_id": version["version_id"],
                "successor_version_id": successor["version_id"],
                "evidence_fingerprint": fingerprint,
            }
        ),
        "previous_evaluation_sha256": str(previous_evaluation_sha256 or ""),
        "version_id": str(version["version_id"]),
        "version_sha256": str(version["version_sha256"]),
        "successor_version_id": str(successor["version_id"]),
        "successor_version_sha256": str(successor["version_sha256"]),
        "horizon_sessions": horizon,
        "training_cutoff": str(version["training_cutoff"]),
        "evaluation_end_cutoff": str(successor["training_cutoff"]),
        "evaluation_claim_generated_start": generated.min().isoformat(),
        "evaluation_claim_generated_end": generated.max().isoformat(),
        "evaluation_outcome_known_end": evaluated.max().isoformat(),
        "evidence_fingerprint": fingerprint,
        "evaluation_rows": int(len(frame)),
        "status": status,
        "overall": _overall_summary(frame),
        "robust_uncertainty": robust,
        "concentration": _concentration(frame),
        "state_persistence": _state_persistence(
            dict(version.get("learned_state_reliability") or {}),
            tables,
        ),
        "semantics": {
            "research_only": True,
            "strictly_future_claims_only": True,
            "evaluation_claim_generated_at_after_training_cutoff": True,
            "evaluation_known_by_successor_training_cutoff": True,
            "successor_boundary_selected_from_performance": False,
            "training_rows_reused_for_same_version_evaluation": False,
            "shorter_horizon_labels_borrowed": False,
            "state_names_assumed_ordinal": False,
            "scalar_confidence_created": False,
            "production_change_performed": False,
        },
    }
    payload["evaluation_sha256"] = _evaluation_hash(payload)
    return payload


def _provisional_evaluation(
    version: dict[str, object],
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    as_known_at: str | None,
    bootstrap_reps: int,
    random_seed: int,
) -> dict[str, object]:
    horizon = int(version["horizon_sessions"])
    if as_known_at is None:
        return {
            "status": "awaiting_future_mature_evidence",
            "N": 0,
            "as_known_at": None,
        }
    frame = _epoch_frame(
        claims,
        outcomes,
        horizon=horizon,
        training_cutoff=str(version["training_cutoff"]),
        evaluation_known_by=as_known_at,
    )
    if frame.empty:
        return {
            "status": "awaiting_future_mature_evidence",
            "N": 0,
            "as_known_at": as_known_at,
        }
    dates = _baseline_dates(frame)
    robust = _bootstrap_summary(
        frame,
        dates,
        horizon,
        reps=bootstrap_reps,
        seed=random_seed + horizon + 1000,
    )
    return {
        "status": "provisional_not_promotion_evidence",
        "N": int(len(frame)),
        "as_known_at": as_known_at,
        "overall": _overall_summary(frame),
        "robust_uncertainty": robust,
        "concentration": _concentration(frame),
        "state_persistence": _state_persistence(
            dict(version.get("learned_state_reliability") or {}),
            _learned_tables(frame, horizon),
        ),
    }


def run_walkforward_evaluation(
    claims_path: str | Path,
    outcomes_path: str | Path,
    versions_path: str | Path,
    evaluations_path: str | Path,
    report_path: str | Path,
    *,
    bootstrap_reps: int = 200,
    random_seed: int = 20260925,
) -> dict[str, object]:
    claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, outcomes)
    versions = _read_versions(versions_path)
    evaluations = _read_evaluations(evaluations_path)
    _validate_previous_report_anchor(report_path, evaluations)

    archived_by_pair = {
        (
            str(record["version_id"]),
            str(record["successor_version_id"]),
        ): record
        for record in evaluations
    }
    current_known_at: str | None = None
    if not outcomes.empty:
        evaluated = pd.to_datetime(outcomes["evaluated_at"], errors="coerce", utc=True)
        if evaluated.isna().any():
            raise ValueError("Phase 5D current evidence chronology failure")
        current_known_at = evaluated.max().isoformat()

    horizon_reports: dict[str, object] = {}
    blockers: set[str] = set()
    new_evaluations: list[str] = []

    for horizon in HORIZONS:
        h_versions = [
            version
            for version in versions
            if int(version.get("horizon_sessions", -1)) == horizon
        ]
        finalized_status: list[dict[str, object]] = []
        for version, successor in zip(h_versions, h_versions[1:]):
            start = _required_utc(version["training_cutoff"], "training_cutoff")
            end = _required_utc(
                successor["training_cutoff"], "successor training_cutoff"
            )
            pair = (str(version["version_id"]), str(successor["version_id"]))
            if end <= start:
                blockers.add(f"{horizon}T_non_increasing_version_cutoff")
                finalized_status.append(
                    {
                        "version_id": pair[0],
                        "successor_version_id": pair[1],
                        "status": "no_strict_future_evaluation_window",
                    }
                )
                continue

            frame = _epoch_frame(
                claims,
                outcomes,
                horizon=horizon,
                training_cutoff=start.isoformat(),
                evaluation_known_by=end.isoformat(),
            )
            archived_record = archived_by_pair.get(pair)
            predecessor_hash = (
                str(archived_record["previous_evaluation_sha256"])
                if archived_record is not None
                else (
                    str(evaluations[-1]["evaluation_sha256"])
                    if evaluations
                    else ""
                )
            )
            record = _build_finalized_record(
                version,
                successor,
                frame,
                previous_evaluation_sha256=predecessor_hash,
                bootstrap_reps=bootstrap_reps,
                random_seed=random_seed,
            )
            if record is None:
                blockers.add(f"{horizon}T_successor_without_future_evaluation_rows")
                finalized_status.append(
                    {
                        "version_id": pair[0],
                        "successor_version_id": pair[1],
                        "status": "awaiting_or_missing_strictly_future_rows",
                    }
                )
                continue

            supplied_hash = str(record["evaluation_sha256"])
            if archived_record is None:
                evaluations.append(record)
                _validate_evaluations(evaluations)
                archived_by_pair[pair] = record
                new_evaluations.append(str(record["evaluation_id"]))
            elif str(archived_record["evaluation_sha256"]) != supplied_hash:
                raise ValueError(
                    f"immutable Phase 5D evaluation conflict for {pair}"
                )
            finalized_status.append(
                {
                    "version_id": pair[0],
                    "successor_version_id": pair[1],
                    "status": str(record["status"]),
                    "evaluation_id": str(record["evaluation_id"]),
                    "N": int(record["evaluation_rows"]),
                }
            )

        provisional = None
        if h_versions:
            provisional = _provisional_evaluation(
                h_versions[-1],
                claims,
                outcomes,
                as_known_at=current_known_at,
                bootstrap_reps=bootstrap_reps,
                random_seed=random_seed,
            )
        else:
            blockers.add(f"{horizon}T_no_phase5c_version")

        archived = [
            record
            for record in evaluations
            if int(record.get("horizon_sessions", -1)) == horizon
        ]
        horizon_reports[str(horizon)] = {
            "phase5c_versions": int(len(h_versions)),
            "finalized_evaluation_epochs": int(len(archived)),
            "finalized_status": finalized_status,
            "latest_provisional": provisional,
        }

    _write_evaluations(evaluations_path, evaluations)
    result = {
        "phase": "5D_walkforward_evaluation",
        "schema_version": SCHEMA_VERSION,
        "status": (
            "walkforward_evaluation_active"
            if versions
            else "awaiting_phase5c_candidate_versions"
        ),
        "source_phase5c_schema_version": PHASE5C_SCHEMA_VERSION,
        "claims": int(len(claims)),
        "mature_outcomes": int(len(outcomes)),
        "phase5c_versions": int(len(versions)),
        "finalized_evaluations": int(len(evaluations)),
        "new_finalized_evaluations": new_evaluations,
        "evaluation_chain_tip": (
            str(evaluations[-1]["evaluation_sha256"])
            if evaluations
            else None
        ),
        "as_known_at": current_known_at,
        "horizons": horizon_reports,
        "blockers": sorted(blockers),
        "promotion": {
            "status": "insufficient_evidence",
            "reason": (
                "Phase 5D records genuine future epochs but does not define or "
                "claim a scalar learner advantage over the frozen baseline."
            ),
            "production_change_performed": False,
        },
        "semantics": {
            "research_only": True,
            "strictly_future_claims_only": True,
            "same_version_training_rows_reused_for_evaluation": False,
            "finalized_epoch_boundary_is_next_same_horizon_training_cutoff": True,
            "epoch_boundary_selected_from_performance": False,
            "latest_version_evaluation_is_provisional": True,
            "provisional_counts_as_promotion_evidence": False,
            "shorter_horizon_labels_borrowed": False,
            "overlapping_forward_windows_treated_as_independent": False,
            "state_names_assumed_ordinal": False,
            "scalar_confidence_created": False,
            "adaptive_production_weights_created": False,
            "production_confidence_changed": False,
            "portfolio_or_depot_watch_changed": False,
            "evaluation_archive_hash_chained": True,
            "previous_report_anchors_append_only_prefix": True,
        },
    }
    output = Path(report_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return result