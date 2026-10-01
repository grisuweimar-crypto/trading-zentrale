"""Build current Phase-7A packets from the authoritative scanner snapshot.

This adapter is intentionally conservative. It binds the exact current scanner
snapshot to Phase-7A without inventing a Selection direction. The only automatic
directional claims are matches of the already-frozen Phase-1B timing catalogue.
Frozen Phase-2 probability calibration is attached as non-directional annotation
through claim_ref. Missing Risk, Confidence, Elliott or external-evidence adapters
stay missing rather than being reconstructed from convenient scanner scalars.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping

import pandas as pd

from scanner.reports.daily_research import validate_daily_research
from scanner.reports.selection_timing import HORIZONS, _r_score_backbone, _score_percentile
from scanner.reports.timing_patterns import Phase1BConfig, feature_rows

from .evidence_archive import (
    load_evidence_archive,
    packet_identity,
    write_normalized_archive,
)
from .input_contract import build_input_packet, validate_input_packet
from .phase2_probability import (
    build_phase2_probability_rows,
    load_probability_calibration,
    probability_calibration_digest,
    validate_probability_calibration,
)


CURRENT_PACKET_SET_SCHEMA_VERSION = "decision_current_packet_set_7a_v1"
DEFAULT_LATEST = "artifacts/research/latest_scanner.csv"
DEFAULT_HISTORY = "artifacts/research/history_analysis.csv"
DEFAULT_TIMING_CATALOG = "artifacts/research/timing_patterns_1b_frozen.json"
DEFAULT_PROBABILITY_CALIBRATION = "artifacts/research/probability_calibration_2.json"
DEFAULT_ARCHIVE = "artifacts/research/decision_evidence_7a.jsonl"
DEFAULT_OUTPUT = "artifacts/research/current_decision_packets_7a.json"


class CurrentDecisionEvidenceError(ValueError):
    """Raised when the current snapshot cannot be bound without inference."""


def _aware(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise CurrentDecisionEvidenceError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CurrentDecisionEvidenceError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CurrentDecisionEvidenceError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _clean(value: object) -> object | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value


def _require_current_identity(daily: Mapping[str, object], latest: pd.DataFrame) -> tuple[str, str, datetime]:
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    daily_as_of = str(daily.get("as_of") or "").strip()
    generated_at = str(daily.get("generated_at") or "").strip()
    if not snapshot_id:
        raise CurrentDecisionEvidenceError("daily_snapshot_id_required")
    if not daily_as_of:
        raise CurrentDecisionEvidenceError("daily_as_of_required")
    decision_time = _aware(generated_at, "daily_generated_at")

    required = {
        "date", "symbol", "score", "snapshot_id", "generated_at",
        "scoring_version", "schema_version",
    }
    missing = sorted(required.difference(latest.columns))
    if missing:
        raise CurrentDecisionEvidenceError("latest_scanner_columns_missing:" + ",".join(missing))
    if latest.empty:
        raise CurrentDecisionEvidenceError("latest_scanner_empty")
    if latest["symbol"].astype(str).duplicated().any():
        raise CurrentDecisionEvidenceError("latest_scanner_duplicate_symbol")

    snapshots = set(latest["snapshot_id"].dropna().astype(str))
    if snapshots != {snapshot_id}:
        raise CurrentDecisionEvidenceError("latest_scanner_snapshot_mismatch")
    dates = set(latest["date"].dropna().astype(str))
    if dates != {daily_as_of}:
        raise CurrentDecisionEvidenceError("latest_scanner_as_of_mismatch")

    latest_symbols = set(latest["symbol"].astype(str))
    daily_symbols_raw = daily.get("symbols")
    if not isinstance(daily_symbols_raw, Mapping):
        raise CurrentDecisionEvidenceError("daily_symbols_required")
    daily_symbols = set(map(str, daily_symbols_raw.keys()))
    if latest_symbols != daily_symbols:
        raise CurrentDecisionEvidenceError("latest_scanner_daily_universe_mismatch")

    source_snapshot_id = str(daily.get("source_snapshot_id") or snapshot_id)
    if source_snapshot_id != snapshot_id:
        raise CurrentDecisionEvidenceError("daily_source_snapshot_mismatch")

    for value in latest["generated_at"].dropna().astype(str):
        if _aware(value, "latest_generated_at") > decision_time:
            raise CurrentDecisionEvidenceError("latest_scanner_generated_after_daily")
    return snapshot_id, daily_as_of, decision_time


def _load_timing_catalog(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") != "phase1b_frozen_patterns_v1":
        raise CurrentDecisionEvidenceError("unsupported_timing_catalog")
    horizons = value.get("horizons")
    if not isinstance(horizons, Mapping):
        raise CurrentDecisionEvidenceError("timing_catalog_horizons_required")
    for horizon in HORIZONS:
        block = horizons.get(str(horizon))
        if not isinstance(block, Mapping) or not isinstance(block.get("frozen_patterns"), list):
            raise CurrentDecisionEvidenceError(f"timing_catalog_horizon_invalid:{horizon}")
        for item in block["frozen_patterns"]:
            if not isinstance(item, Mapping):
                raise CurrentDecisionEvidenceError("timing_pattern_invalid")
            if item.get("discovery_direction") not in {"positive", "negative"}:
                raise CurrentDecisionEvidenceError("timing_pattern_direction_invalid")
            conditions = item.get("conditions")
            if not isinstance(conditions, list) or not conditions:
                raise CurrentDecisionEvidenceError("timing_pattern_conditions_invalid")
    return value


def _current_features(history: pd.DataFrame, latest: pd.DataFrame, daily_as_of: str, snapshot_id: str) -> pd.DataFrame:
    work = history.copy()
    if "date" not in work.columns:
        raise CurrentDecisionEvidenceError("history_date_required")
    dates = pd.to_datetime(work["date"], errors="coerce")
    work = work.loc[dates.isna() | (dates <= pd.Timestamp(daily_as_of))].copy()
    combined = pd.concat([work, latest], ignore_index=True, sort=False)
    features, _ = feature_rows(combined, Phase1BConfig())
    if "snapshot_id" not in features.columns:
        raise CurrentDecisionEvidenceError("timing_features_snapshot_identity_missing")
    current = features.loc[features["snapshot_id"].astype(str).eq(snapshot_id)].copy()
    if current["symbol"].astype(str).duplicated().any():
        raise CurrentDecisionEvidenceError("timing_features_duplicate_current_symbol")
    return current


def _selection_row(row: Mapping[str, object], score_percentile: float, packet_as_of: str) -> dict[str, object]:
    symbol = str(row.get("symbol") or "")
    score = float(row["score"])
    payload: dict[str, object] = {
        "score": score,
        "score_percentile": float(score_percentile),
        "quality_band": _r_score_backbone(score, float(score_percentile)),
    }
    for key in ("r_code", "score_status", "trend_ok", "liquidity_ok"):
        value = _clean(row.get(key))
        if value is not None:
            payload[key] = value

    source_version = f"scanner:{row.get('scoring_version')}:{row.get('schema_version')}"
    return {
        "family": "selection",
        "claim_id": f"selection:{symbol}:{row.get('snapshot_id')}",
        "as_of": str(row.get("generated_at")),
        "available_from": str(row.get("generated_at")),
        "source_version": source_version,
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": payload,
    }


def _timing_rows(
    *,
    symbol: str,
    feature: Mapping[str, object] | None,
    catalog: Mapping[str, object],
    packet_as_of: str,
    catalog_digest: str,
) -> list[dict[str, object]]:
    if feature is None:
        return []
    horizons = catalog["horizons"]
    assert isinstance(horizons, Mapping)
    rows: list[dict[str, object]] = []
    for horizon in HORIZONS:
        block = horizons[str(horizon)]
        assert isinstance(block, Mapping)
        patterns = block["frozen_patterns"]
        assert isinstance(patterns, list)
        for item in patterns:
            assert isinstance(item, Mapping)
            conditions = [str(value) for value in item["conditions"]]
            missing = [condition for condition in conditions if condition not in feature]
            if missing:
                raise CurrentDecisionEvidenceError(
                    f"timing_condition_missing:{horizon}:" + ",".join(sorted(missing))
                )
            if not all(bool(feature.get(condition, False)) for condition in conditions):
                continue
            pattern = str(item["pattern"])
            pattern_id = sha256(f"{horizon}|{pattern}".encode("utf-8")).hexdigest()[:16]
            rows.append({
                "family": "timing",
                "claim_id": f"timing:{symbol}:{pattern_id}:{horizon}T",
                "as_of": packet_as_of,
                "available_from": packet_as_of,
                "source_version": f"phase1b_frozen_patterns_v1:{catalog_digest[:16]}",
                "coverage_state": "available",
                "maturity_state": "directional_but_immature",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {
                    "pattern_id": pattern_id,
                    "pattern": pattern,
                    "direction": str(item["discovery_direction"]),
                    "horizon_sessions": int(horizon),
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
            })
    return rows


def build_current_packet_set_from_frames(
    *,
    daily: Mapping[str, object],
    latest: pd.DataFrame,
    history: pd.DataFrame,
    timing_catalog: Mapping[str, object],
    probability_calibration: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build one validated 7A packet per current scanner symbol.

    Selection stays current context without inferred direction. Frozen Timing is
    matched from point-in-time features. When supplied, frozen Phase-2
    probability calibration is attached to those exact claims as annotation;
    it is never converted into an additional directional vote.
    """
    snapshot_id, daily_as_of, decision_time = _require_current_identity(daily, latest)
    packet_as_of = str(daily.get("generated_at"))
    catalog_raw = json.dumps(timing_catalog, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    catalog_digest = sha256(catalog_raw.encode("utf-8")).hexdigest()

    horizons = timing_catalog.get("horizons")
    if timing_catalog.get("schema_version") != "phase1b_frozen_patterns_v1" or not isinstance(horizons, Mapping):
        raise CurrentDecisionEvidenceError("unsupported_timing_catalog")
    for horizon in HORIZONS:
        block = horizons.get(str(horizon))
        if not isinstance(block, Mapping) or not isinstance(block.get("frozen_patterns"), list):
            raise CurrentDecisionEvidenceError(f"timing_catalog_horizon_invalid:{horizon}")

    validated_probability = None
    probability_digest = None
    if probability_calibration is not None:
        validated_probability = validate_probability_calibration(probability_calibration)
        probability_digest = probability_calibration_digest(validated_probability)

    current_features = _current_features(history, latest, daily_as_of, snapshot_id)
    feature_index = {
        str(row["symbol"]): row.to_dict()
        for _, row in current_features.iterrows()
    }

    current = latest.copy()
    current["score"] = pd.to_numeric(current["score"], errors="coerce")
    if current["score"].isna().any():
        raise CurrentDecisionEvidenceError("latest_scanner_score_missing")
    current["score_pct_full"] = _score_percentile(current["score"])

    packets: list[dict[str, object]] = []
    timing_claims = 0
    symbols_with_timing = 0
    probability_claims = 0
    symbols_with_probability = 0
    for _, series in current.sort_values("symbol", kind="mergesort").iterrows():
        row = series.to_dict()
        symbol = str(row["symbol"])
        selection = _selection_row(row, float(row["score_pct_full"]), packet_as_of)
        timing = _timing_rows(
            symbol=symbol,
            feature=feature_index.get(symbol),
            catalog=timing_catalog,
            packet_as_of=packet_as_of,
            catalog_digest=catalog_digest,
        )
        if timing:
            symbols_with_timing += 1
            timing_claims += len(timing)

        probability: list[dict[str, object]] = []
        if validated_probability is not None:
            probability = build_phase2_probability_rows(
                symbol=symbol,
                selection=selection,
                timing=timing,
                calibration=validated_probability,
                packet_as_of=packet_as_of,
                source_digest=probability_digest,
            )
        if probability:
            symbols_with_probability += 1
            probability_claims += len(probability)

        packet = build_input_packet(
            symbol=symbol,
            as_of=packet_as_of,
            source_snapshot_id=snapshot_id,
            evidence=[selection, *timing, *probability],
        )
        packets.append(packet)

    return {
        "schema_version": CURRENT_PACKET_SET_SCHEMA_VERSION,
        "phase": "7A-current-orchestration",
        "snapshot_id": snapshot_id,
        "as_of": packet_as_of,
        "daily_as_of": daily_as_of,
        "packet_count": len(packets),
        "timing_claim_count": timing_claims,
        "symbols_with_timing_claims": symbols_with_timing,
        "probability_claim_count": probability_claims,
        "symbols_with_probability_claims": symbols_with_probability,
        "packets": packets,
        "semantics": {
            "exact_daily_snapshot_bound": True,
            "selection_direction_inferred": False,
            "timing_direction_source": "frozen_phase1b_discovery_direction",
            "timing_match_from_pit_features": True,
            "probability_reconstructed": False,
            "phase2_probability_annotations_attached": validated_probability is not None,
            "probability_is_directional_vote": False,
            "probability_source": "phase2_probability_calibration" if validated_probability is not None else None,
            "risk_reconstructed": False,
            "confidence_reconstructed": False,
            "elliott_reconstructed": False,
            "external_evidence_phase8_activated": False,
            "missing_evidence_treated_as_neutral": False,
            "portfolio_state_used": False,
            "stance_computed": False,
            "portfolio_action_computed": False,
        },
        "validation": {
            "research_only": True,
            "productive_decision_integration_enabled": False,
            "packet_as_of_utc": decision_time.isoformat(),
        },
    }


def build_current_packet_set(root: str | Path) -> dict[str, object]:
    root = Path(root)
    daily = validate_daily_research(root)
    latest = pd.read_csv(root / DEFAULT_LATEST)
    history = pd.read_csv(root / DEFAULT_HISTORY, low_memory=False)
    catalog = _load_timing_catalog(root / DEFAULT_TIMING_CATALOG)
    probability = load_probability_calibration(root / DEFAULT_PROBABILITY_CALIBRATION)
    return build_current_packet_set_from_frames(
        daily=daily,
        latest=latest,
        history=history,
        timing_catalog=catalog,
        probability_calibration=probability,
    )


def _compare_packet(packet: Mapping[str, object]) -> str:
    normalized = validate_input_packet(packet)
    normalized.pop("archive_partition", None)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def merge_packet_set_into_archive(
    archive_path: str | Path,
    packet_set: Mapping[str, object],
) -> dict[str, object]:
    """Idempotently persist a complete current packet set into the 7A archive."""
    if packet_set.get("schema_version") != CURRENT_PACKET_SET_SCHEMA_VERSION:
        raise CurrentDecisionEvidenceError("unsupported_current_packet_set")
    raw_packets = packet_set.get("packets")
    if not isinstance(raw_packets, list):
        raise CurrentDecisionEvidenceError("current_packet_set_packets_required")

    existing, _ = load_evidence_archive(archive_path, missing_ok=True)
    by_identity = {packet_identity(packet): packet for packet in existing}
    added = 0
    skipped = 0
    for raw in raw_packets:
        if not isinstance(raw, Mapping):
            raise CurrentDecisionEvidenceError("current_packet_must_be_object")
        packet = validate_input_packet(raw)
        identity = packet_identity(packet)
        previous = by_identity.get(identity)
        if previous is not None:
            if _compare_packet(previous) != _compare_packet(packet):
                raise CurrentDecisionEvidenceError("archive_identity_collision_with_changed_packet:" + "|".join(identity))
            skipped += 1
            continue
        existing.append(packet)
        by_identity[identity] = packet
        added += 1

    metadata = write_normalized_archive(archive_path, existing)
    return {
        **metadata,
        "current_snapshot_id": packet_set.get("snapshot_id"),
        "current_packets_added": added,
        "current_packets_already_present": skipped,
    }
