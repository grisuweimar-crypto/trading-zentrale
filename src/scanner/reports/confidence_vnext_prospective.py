from __future__ import annotations

"""Phase 4E prospective shadow validation infrastructure.

Research-only. This module freezes the contemporaneous Phase-4B-D evidence
state as immutable claims and matures raw outcomes only from later price
history. Peer-relative labels are derived from the full matured cohort at
analysis time rather than frozen when individual symbols happen to mature.

It does not create a scalar Confidence score, thresholds, production signals,
or portfolio actions.
"""

from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    HORIZONS,
    _norm_currency,
    _peer_median,
    _peer_medians,
    _price_rows,
    _scanner_rows,
)


SCHEMA_VERSION = "phase4e_shadow_v1"
CLAIM_COLUMNS = (
    "claim_id",
    "schema_version",
    "as_of",
    "generated_at",
    "run_id",
    "snapshot_id",
    "symbol",
    "currency",
    "horizon_sessions",
    "evidence_version",
    "evidence_fingerprint",
    "phase4_report_sha256",
    "phase2_sha256",
    "phase3_sha256",
    "risk_scale_sha256",
    "phase2_source_as_of",
    "phase3_source_as_of",
    "selection_band",
    "selection_state",
    "selection_direction",
    "timing_state",
    "timing_direction",
    "timing_patterns",
    "risk_state",
    "agreement_state",
    "agreement_conflicts",
    "return_claim_direction",
    "dq_selection_state",
    "dq_timing_state",
    "dq_risk_state",
    "volatility_application_status",
)
OUTCOME_COLUMNS = (
    "claim_id",
    "schema_version",
    "as_of",
    "evaluated_at",
    "symbol",
    "currency",
    "horizon_sessions",
    "start_market_date",
    "end_market_date",
    "start_adjusted_close",
    "end_adjusted_close",
    "return",
    "adverse_excursion",
    "path_max_drawdown",
)
DERIVED_PEER_COLUMNS = (
    "claim_id",
    "as_of",
    "symbol",
    "currency",
    "horizon_sessions",
    "return",
    "peer_median_return",
    "peer_excess",
    "return_claim_direction",
    "signed_peer_excess",
    "direction_hit",
    "agreement_state",
    "risk_state",
)


def _required_text(value: object, label: str) -> str:
    if value is None:
        raise ValueError(f"{label} is required")
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        raise ValueError(f"{label} is required")
    return text


def _sha_file(path: str | Path | None, *, required: bool) -> str:
    if path is None:
        if required:
            raise ValueError("required fingerprint source is missing")
        return "missing"
    p = Path(path)
    if not p.exists():
        if required:
            raise ValueError(f"required fingerprint source does not exist: {p}")
        return "missing"
    return sha256(p.read_bytes()).hexdigest()


def evidence_fingerprints(
    phase4_report_path: str | Path,
    phase2_path: str | Path,
    phase3_path: str | Path,
    risk_scale_path: str | Path | None = None,
) -> dict[str, str]:
    parts = {
        "phase4_report_sha256": _sha_file(phase4_report_path, required=True),
        "phase2_sha256": _sha_file(phase2_path, required=True),
        "phase3_sha256": _sha_file(phase3_path, required=True),
        "risk_scale_sha256": _sha_file(risk_scale_path, required=False),
    }
    parts["evidence_fingerprint"] = sha256(
        "|".join(parts[key] for key in sorted(parts)).encode("utf-8")
    ).hexdigest()
    return parts


def _return_claim_direction(row: dict) -> str:
    directions: list[str] = []
    selection = row.get("selection") or {}
    timing = row.get("timing") or {}
    if selection.get("state") == "robust" and selection.get("direction") in {"positive", "negative"}:
        directions.append(str(selection["direction"]))
    if timing.get("state") == "robust_claim" and timing.get("direction") in {"positive", "negative"}:
        directions.append(str(timing["direction"]))
    unique = set(directions)
    return next(iter(unique)) if len(unique) == 1 else ""


def _list_text(values: object) -> str:
    if not isinstance(values, (list, tuple, set)):
        return ""
    return "|".join(sorted(str(value) for value in values if str(value)))


def _current_stock_context(latest: pd.DataFrame) -> dict[str, dict[str, object]]:
    scanner = _scanner_rows(latest)
    if scanner.empty:
        raise ValueError("latest scanner is empty")
    latest_date = pd.to_datetime(scanner["date"], errors="coerce").max()
    current = scanner.loc[
        pd.to_datetime(scanner["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())
        & ~scanner["is_crypto"]
    ].copy()
    return {
        str(row["symbol"]): {"currency": _norm_currency(row.get("currency"))}
        for row in current.to_dict("records")
    }


def build_claim_rows(
    phase4_report: dict,
    latest: pd.DataFrame,
    metadata: dict,
    fingerprints: dict[str, str],
) -> pd.DataFrame:
    if metadata.get("latest_run_complete") is not True:
        raise ValueError("shadow claims require a complete scanner publication")
    snapshot_id = _required_text(metadata.get("snapshot_id"), "snapshot_id")
    generated_at = _required_text(metadata.get("generated_at"), "generated_at")
    as_of = _required_text(metadata.get("as_of"), "metadata as_of")
    daily = metadata.get("daily_run") or {}
    if daily.get("scanner_status") != "success":
        raise ValueError("shadow claims require scanner_status=success")
    run_id = _required_text(daily.get("run_id"), "run_id")

    current = phase4_report.get("current") or {}
    if _required_text(current.get("as_of"), "Phase 4 current as_of") != as_of:
        raise ValueError("Phase 4 current as_of does not match metadata as_of")
    if current.get("asset_scope") != "stocks_only":
        raise ValueError("Phase 4E currently accepts stock-only Phase 4 evidence")
    evidence_version = _required_text(
        (phase4_report.get("config") or {}).get("evidence_version"),
        "evidence_version",
    )
    pit = current.get("pit_source_checks") or {}
    phase2_source_as_of = _required_text(
        ((pit.get("phase2") or {}).get("source_as_of")),
        "phase2 source_as_of",
    )
    phase3_source_as_of = _required_text(
        ((pit.get("phase3") or {}).get("source_as_of")),
        "phase3 source_as_of",
    )
    volatility_status = _required_text(
        (((current.get("risk_metric_applicability") or {}).get("volatility") or {}).get("status")),
        "volatility application status",
    )
    context = _current_stock_context(latest)

    rows: list[dict[str, object]] = []
    for row in current.get("rows") or []:
        symbol = _required_text(row.get("symbol"), "symbol")
        if symbol not in context:
            raise ValueError(f"Phase 4 stock row missing from current scanner: {symbol}")
        horizon = int(row.get("horizon_sessions"))
        if horizon not in HORIZONS:
            raise ValueError(f"unsupported horizon: {horizon}")
        selection = row.get("selection") or {}
        timing = row.get("timing") or {}
        risk = row.get("risk") or {}
        agreement = row.get("model_agreement") or {}
        dq = row.get("data_quality") or {}
        payload = {
            "schema_version": SCHEMA_VERSION,
            "as_of": as_of,
            "generated_at": generated_at,
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "symbol": symbol,
            "currency": context[symbol]["currency"] or "",
            "horizon_sessions": horizon,
            "evidence_version": evidence_version,
            **fingerprints,
            "phase2_source_as_of": phase2_source_as_of,
            "phase3_source_as_of": phase3_source_as_of,
            "selection_band": str(row.get("selection_band") or ""),
            "selection_state": str(selection.get("state") or ""),
            "selection_direction": str(selection.get("direction") or ""),
            "timing_state": str(timing.get("state") or ""),
            "timing_direction": str(timing.get("direction") or ""),
            "timing_patterns": _list_text(timing.get("matched_patterns")),
            "risk_state": str(risk.get("state") or ""),
            "agreement_state": str(agreement.get("state") or ""),
            "agreement_conflicts": _list_text(agreement.get("conflicts")),
            "return_claim_direction": _return_claim_direction(row),
            "dq_selection_state": str(((dq.get("selection") or {}).get("state")) or ""),
            "dq_timing_state": str(((dq.get("timing") or {}).get("state")) or ""),
            "dq_risk_state": str(((dq.get("risk") or {}).get("state")) or ""),
            "volatility_application_status": volatility_status,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        payload["claim_id"] = sha256(canonical.encode("utf-8")).hexdigest()
        rows.append(payload)

    result = pd.DataFrame(rows, columns=CLAIM_COLUMNS)
    return result.sort_values(["symbol", "horizon_sessions"], kind="mergesort").reset_index(drop=True)


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected shadow schema in {p}")
    return frame


def _natural_claim_key(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["snapshot_id"].astype(str)
        + "|"
        + frame["symbol"].astype(str)
        + "|"
        + frame["horizon_sessions"].astype(str)
    )


def append_claims(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new.copy().reset_index(drop=True)
    if new.empty:
        return existing.copy().reset_index(drop=True)
    old = existing.copy()
    fresh = new.copy()
    old["_natural_key"] = _natural_claim_key(old)
    fresh["_natural_key"] = _natural_claim_key(fresh)
    old_by_key = dict(zip(old["_natural_key"], old["claim_id"]))
    keep: list[int] = []
    for idx, row in fresh.iterrows():
        key = row["_natural_key"]
        previous = old_by_key.get(key)
        if previous is None:
            keep.append(idx)
        elif str(previous) != str(row["claim_id"]):
            raise ValueError(f"immutable shadow claim conflict for {key}")
    combined = pd.concat(
        [old.drop(columns=["_natural_key"]), fresh.loc[keep].drop(columns=["_natural_key"])],
        ignore_index=True,
    )
    return combined.loc[:, CLAIM_COLUMNS]


def _price_groups(prices: pd.DataFrame) -> dict[str, pd.DataFrame]:
    price = _price_rows(prices)
    return {
        str(symbol): group.sort_values("date", kind="mergesort").reset_index(drop=True)
        for symbol, group in price.groupby("symbol", sort=False)
    }


def _mature_one(
    claim: dict[str, object],
    group: pd.DataFrame,
    evaluated_at: str,
) -> dict[str, object] | None:
    obs_date = pd.Timestamp(str(claim["as_of"])).normalize()
    dates = pd.to_datetime(group["date"], errors="coerce")
    pos = int(
        np.searchsorted(
            dates.values.astype("datetime64[ns]"),
            np.datetime64(obs_date),
            side="right",
        )
        - 1
    )
    if pos < 0:
        return None
    start_date = pd.Timestamp(dates.iloc[pos]).normalize()
    if (obs_date - start_date).days > 7:
        return None
    horizon = int(claim["horizon_sessions"])
    target = pos + horizon
    if target >= len(group):
        return None
    path = pd.to_numeric(
        group.iloc[pos : target + 1]["adj_close"],
        errors="coerce",
    ).to_numpy(dtype=float)
    if len(path) != horizon + 1 or not np.isfinite(path).all() or (path <= 0).any():
        return None
    start_value = float(path[0])
    end_value = float(path[-1])
    entry_returns = path / start_value - 1.0
    peaks = np.maximum.accumulate(path)
    drawdowns = path / peaks - 1.0
    return {
        "claim_id": str(claim["claim_id"]),
        "schema_version": SCHEMA_VERSION,
        "as_of": str(claim["as_of"]),
        "evaluated_at": evaluated_at,
        "symbol": str(claim["symbol"]),
        "currency": str(claim.get("currency") or ""),
        "horizon_sessions": horizon,
        "start_market_date": start_date.date().isoformat(),
        "end_market_date": pd.Timestamp(dates.iloc[target]).date().isoformat(),
        "start_adjusted_close": start_value,
        "end_adjusted_close": end_value,
        "return": end_value / start_value - 1.0,
        "adverse_excursion": max(0.0, -float(np.min(entry_returns))),
        "path_max_drawdown": max(0.0, -float(np.min(drawdowns))),
    }


def compute_mature_outcomes(
    claims: pd.DataFrame,
    prices: pd.DataFrame,
    existing_outcomes: pd.DataFrame,
    evaluated_at: str,
) -> pd.DataFrame:
    existing_ids = (
        set(existing_outcomes["claim_id"].astype(str))
        if not existing_outcomes.empty
        else set()
    )
    groups = _price_groups(prices)
    matured: list[dict[str, object]] = []
    for claim in claims.to_dict("records"):
        if str(claim["claim_id"]) in existing_ids:
            continue
        group = groups.get(str(claim["symbol"]))
        if group is None or group.empty:
            continue
        row = _mature_one(claim, group, evaluated_at)
        if row is not None:
            matured.append(row)
    if not matured:
        return pd.DataFrame(columns=OUTCOME_COLUMNS)
    return pd.DataFrame(matured, columns=OUTCOME_COLUMNS)


def append_outcomes(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new.copy().reset_index(drop=True)
    if new.empty:
        return existing.copy().reset_index(drop=True)
    duplicate = set(existing["claim_id"].astype(str)) & set(new["claim_id"].astype(str))
    if duplicate:
        raise ValueError(f"outcome rewrite attempted for {sorted(duplicate)[:3]}")
    return pd.concat([existing, new], ignore_index=True).loc[:, OUTCOME_COLUMNS]


def derive_peer_labels(claims: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    """Derive peer labels from all currently matured rows in each cohort.

    These labels are intentionally not part of the append-only raw outcome
    archive. If a slower market/symbol matures later, the peer cohort can become
    more complete without rewriting the original raw outcomes.
    """
    if claims.empty or outcomes.empty:
        return pd.DataFrame(columns=DERIVED_PEER_COLUMNS)
    claim_info = claims[
        ["claim_id", "return_claim_direction", "agreement_state", "risk_state"]
    ].copy()
    work = outcomes.merge(claim_info, on="claim_id", how="left", validate="one_to_one")
    work["obs_date"] = pd.to_datetime(work["as_of"], errors="coerce")
    work["return"] = pd.to_numeric(work["return"], errors="coerce")
    work["peer_median_return"] = np.nan
    work["peer_excess"] = np.nan
    work["signed_peer_excess"] = np.nan
    work["direction_hit"] = np.nan

    horizon_values = pd.to_numeric(work["horizon_sessions"], errors="coerce")
    for horizon in HORIZONS:
        subset = work.loc[horizon_values.eq(horizon)].copy()
        if subset.empty:
            continue
        baselines, fallback = _peer_medians(subset, "return")
        peer = subset.apply(lambda row: _peer_median(row, baselines, fallback), axis=1)
        work.loc[subset.index, "peer_median_return"] = peer.values
        work.loc[subset.index, "peer_excess"] = (
            subset["return"].to_numpy(dtype=float) - peer.to_numpy(dtype=float)
        )

    for idx, row in work.iterrows():
        direction = str(row.get("return_claim_direction") or "")
        peer_excess = row.get("peer_excess")
        if direction in {"positive", "negative"} and pd.notna(peer_excess):
            signed = float(peer_excess) if direction == "positive" else -float(peer_excess)
            work.at[idx, "signed_peer_excess"] = signed
            work.at[idx, "direction_hit"] = int(signed > 0)

    return work.loc[:, DERIVED_PEER_COLUMNS]


def _write_csv(frame: pd.DataFrame, path: str | Path, columns: tuple[str, ...]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.loc[:, columns].to_csv(output, index=False, lineterminator="\n")


def validation_summary(claims: pd.DataFrame, outcomes: pd.DataFrame) -> dict[str, object]:
    derived = derive_peer_labels(claims, outcomes)
    horizons: dict[str, dict[str, object]] = {}
    for horizon in HORIZONS:
        c = (
            claims.loc[
                pd.to_numeric(claims["horizon_sessions"], errors="coerce").eq(horizon)
            ]
            if not claims.empty
            else claims
        )
        o = (
            outcomes.loc[
                pd.to_numeric(outcomes["horizon_sessions"], errors="coerce").eq(horizon)
            ]
            if not outcomes.empty
            else outcomes
        )
        d = (
            derived.loc[
                pd.to_numeric(derived["horizon_sessions"], errors="coerce").eq(horizon)
            ]
            if not derived.empty
            else derived
        )
        horizons[str(horizon)] = {
            "claims": int(len(c)),
            "mature_outcomes": int(len(o)),
            "derived_peer_labels": int(pd.to_numeric(d.get("peer_excess"), errors="coerce").notna().sum()) if len(d) else 0,
            "directional_peer_labels": int(pd.to_numeric(d.get("signed_peer_excess"), errors="coerce").notna().sum()) if len(d) else 0,
            "observation_dates": int(pd.to_datetime(o["as_of"], errors="coerce").nunique()) if len(o) else 0,
            "block_length_sessions_for_future_inference": int(2 * horizon),
        }
    return {
        "phase": "4E_prospective_shadow_validation",
        "schema_version": SCHEMA_VERSION,
        "status": "collecting_prospective_evidence",
        "claims": int(len(claims)),
        "mature_outcomes": int(len(outcomes)),
        "horizons": horizons,
        "semantics": {
            "research_only": True,
            "claims_are_immutable": True,
            "raw_outcomes_are_append_only": True,
            "peer_labels_are_derived_not_frozen_early": True,
            "production_confidence_changed": False,
            "scalar_confidence_mapping_created": False,
            "confidence_thresholds_created": False,
            "spent_phase2_phase3_holdout_used_for_phase4e_selection": False,
        },
        "validation_contract": {
            "return_reliability": "compare pre-specified compatible versus single_model sign-normalized peer-excess reliability",
            "risk_reliability": "compare pre-specified risk-tension states on future adverse excursion and path max drawdown",
            "peer_baseline": "derive from all currently matured leave-one-symbol-out peers for the same observation cohort; same currency first, global fallback",
            "fixed_cooldown_sessions": 5,
            "uncertainty": "circular moving observation-date blocks with effective length 2x horizon; full dates stay clustered",
            "minimum_independent_support": "fail closed until at least two time-separated support regions exist",
            "weights_or_thresholds_may_be_tuned_on_this_stream": False,
        },
    }


def run(
    phase4_report_path: str | Path,
    latest_path: str | Path,
    metadata_path: str | Path,
    prices_path: str | Path,
    phase2_path: str | Path,
    phase3_path: str | Path,
    claims_path: str | Path,
    outcomes_path: str | Path,
    summary_path: str | Path,
    risk_scale_path: str | Path | None = None,
) -> dict[str, object]:
    phase4_report = json.loads(Path(phase4_report_path).read_text(encoding="utf-8"))
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    latest = pd.read_csv(latest_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    fingerprints = evidence_fingerprints(
        phase4_report_path,
        phase2_path,
        phase3_path,
        risk_scale_path,
    )

    existing_claims = _read_csv(claims_path, CLAIM_COLUMNS)
    new_claims = build_claim_rows(phase4_report, latest, metadata, fingerprints)
    claims = append_claims(existing_claims, new_claims)
    _write_csv(claims, claims_path, CLAIM_COLUMNS)

    existing_outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS)
    evaluated_at = _required_text(metadata.get("generated_at"), "generated_at")
    new_outcomes = compute_mature_outcomes(
        claims,
        prices,
        existing_outcomes,
        evaluated_at,
    )
    outcomes = append_outcomes(existing_outcomes, new_outcomes)
    _write_csv(outcomes, outcomes_path, OUTCOME_COLUMNS)

    summary = validation_summary(claims, outcomes)
    summary["current_snapshot"] = {
        "snapshot_id": metadata.get("snapshot_id"),
        "as_of": metadata.get("as_of"),
        "run_id": (metadata.get("daily_run") or {}).get("run_id"),
        "new_claims": int(len(claims) - len(existing_claims)),
        "new_mature_outcomes": int(len(outcomes) - len(existing_outcomes)),
        "evidence_fingerprint": fingerprints["evidence_fingerprint"],
    }
    output = Path(summary_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return summary
