"""Attach frozen Phase-2 probability calibration to Phase-7A claims.

Probability remains a non-directional annotation.  This module never creates a
Selection/Timing direction, vote, stance or portfolio action; it only binds
already-computed Phase-2 calibration statistics to the exact claim they
calibrate.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping, Sequence

from scanner.reports.selection_timing import HORIZONS


PHASE2_SOURCE_VERSION = "phase2_probability_calibration"


class Phase2ProbabilityAnnotationError(ValueError):
    """Raised when frozen Phase-2 calibration cannot be mapped safely."""


def validate_probability_calibration(value: Mapping[str, object]) -> dict[str, object]:
    if value.get("phase") != "2_probability_calibration":
        raise Phase2ProbabilityAnnotationError("phase2_probability_identity_invalid")
    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise Phase2ProbabilityAnnotationError("phase2_probability_semantics_required")
    required_true = (
        "research_only",
        "selection_and_timing_kept_separate",
        "timing_candidates_are_frozen_from_phase1b",
    )
    if any(semantics.get(key) is not True for key in required_true):
        raise Phase2ProbabilityAnnotationError("phase2_probability_semantics_invalid")
    if semantics.get("score_is_trade_signal") is not False or semantics.get("r_code_is_trade_signal") is not False:
        raise Phase2ProbabilityAnnotationError("phase2_probability_trade_signal_guard_invalid")
    if not str(semantics.get("probability_target") or "").strip():
        raise Phase2ProbabilityAnnotationError("phase2_probability_target_required")

    horizons = value.get("horizons")
    if not isinstance(horizons, Mapping):
        raise Phase2ProbabilityAnnotationError("phase2_probability_horizons_required")
    for horizon in HORIZONS:
        block = horizons.get(str(horizon))
        if not isinstance(block, Mapping):
            raise Phase2ProbabilityAnnotationError(f"phase2_probability_horizon_missing:{horizon}")
        selection = block.get("selection")
        timing = block.get("timing_patterns")
        if not isinstance(selection, Mapping):
            raise Phase2ProbabilityAnnotationError(f"phase2_selection_calibration_invalid:{horizon}")
        for split in ("discovery", "validation"):
            if not isinstance(selection.get(split), Mapping):
                raise Phase2ProbabilityAnnotationError(f"phase2_selection_split_invalid:{horizon}:{split}")
        if not isinstance(timing, Mapping) or not isinstance(timing.get("patterns"), list):
            raise Phase2ProbabilityAnnotationError(f"phase2_timing_calibration_invalid:{horizon}")
        seen: set[str] = set()
        for row in timing["patterns"]:
            if not isinstance(row, Mapping):
                raise Phase2ProbabilityAnnotationError(f"phase2_timing_pattern_invalid:{horizon}")
            pattern = str(row.get("pattern") or "").strip()
            if not pattern or pattern in seen:
                raise Phase2ProbabilityAnnotationError(f"phase2_timing_pattern_identity_invalid:{horizon}")
            seen.add(pattern)
    return deepcopy(dict(value))


def load_probability_calibration(path: str | Path) -> dict[str, object]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise Phase2ProbabilityAnnotationError("phase2_probability_root_invalid")
    return validate_probability_calibration(raw)


def probability_calibration_digest(value: Mapping[str, object]) -> str:
    normalized = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _statistics(source: object) -> dict[str, object] | None:
    return deepcopy(dict(source)) if isinstance(source, Mapping) else None


def _selection_rows(
    *,
    symbol: str,
    selection: Mapping[str, object],
    calibration: Mapping[str, object],
    packet_as_of: str,
    source_version: str,
) -> list[dict[str, object]]:
    payload = selection.get("payload")
    if not isinstance(payload, Mapping):
        raise Phase2ProbabilityAnnotationError("selection_payload_required")
    band = str(payload.get("quality_band") or "").strip()
    if not band:
        raise Phase2ProbabilityAnnotationError("selection_quality_band_required")
    claim_ref = str(selection.get("claim_id") or "").strip()
    if not claim_ref:
        raise Phase2ProbabilityAnnotationError("selection_claim_id_required")

    horizons = calibration["horizons"]
    semantics = calibration["semantics"]
    assert isinstance(horizons, Mapping) and isinstance(semantics, Mapping)
    rows: list[dict[str, object]] = []
    for horizon in HORIZONS:
        block = horizons[str(horizon)]
        assert isinstance(block, Mapping)
        selection_cal = block["selection"]
        assert isinstance(selection_cal, Mapping)
        validation = selection_cal["validation"]
        discovery = selection_cal["discovery"]
        assert isinstance(validation, Mapping) and isinstance(discovery, Mapping)
        stats = _statistics(validation.get(band))
        split = "validation"
        coverage = "available"
        if stats is None:
            stats = _statistics(discovery.get(band))
            split = "discovery_only"
            coverage = "limited"
        if stats is None:
            continue
        rows.append({
            "family": "probability",
            "claim_id": f"probability:{symbol}:phase2-selection:{horizon}T",
            "claim_ref": claim_ref,
            "as_of": packet_as_of,
            "available_from": packet_as_of,
            "source_version": source_version,
            "coverage_state": coverage,
            "maturity_state": "not_yet_mature",
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": {
                "horizon_sessions": int(horizon),
                "probability_target": str(semantics["probability_target"]),
                "calibration_scope": "selection_band",
                "calibration_split": split,
                "selection_band": band,
                "statistics": stats,
                "annotation_is_directional_vote": False,
            },
        })
    return rows


def _timing_rows(
    *,
    symbol: str,
    timing: Sequence[Mapping[str, object]],
    calibration: Mapping[str, object],
    packet_as_of: str,
    source_version: str,
) -> list[dict[str, object]]:
    horizons = calibration["horizons"]
    semantics = calibration["semantics"]
    assert isinstance(horizons, Mapping) and isinstance(semantics, Mapping)
    rows: list[dict[str, object]] = []
    for claim in timing:
        payload = claim.get("payload")
        if not isinstance(payload, Mapping):
            raise Phase2ProbabilityAnnotationError("timing_payload_required")
        horizon = int(payload.get("horizon_sessions") or 0)
        pattern = str(payload.get("pattern") or "").strip()
        pattern_id = str(payload.get("pattern_id") or "").strip()
        claim_ref = str(claim.get("claim_id") or "").strip()
        if horizon not in HORIZONS or not pattern or not pattern_id or not claim_ref:
            raise Phase2ProbabilityAnnotationError("timing_claim_identity_invalid")
        block = horizons[str(horizon)]
        assert isinstance(block, Mapping)
        timing_cal = block["timing_patterns"]
        assert isinstance(timing_cal, Mapping)
        matches = [row for row in timing_cal["patterns"] if isinstance(row, Mapping) and row.get("pattern") == pattern]
        if len(matches) != 1:
            raise Phase2ProbabilityAnnotationError(f"phase2_timing_mapping_not_unique:{horizon}:{pattern}")
        calibrated = matches[0]
        stats = _statistics(calibrated.get("validation"))
        split = "validation"
        coverage = "available"
        if stats is None:
            stats = _statistics(calibrated.get("discovery"))
            split = "discovery_only"
            coverage = "limited"
        if stats is None:
            continue
        strong = bool(split == "validation" and calibrated.get("strong_validation") is True)
        rows.append({
            "family": "probability",
            "claim_id": f"probability:{symbol}:phase2-timing:{pattern_id}:{horizon}T",
            "claim_ref": claim_ref,
            "as_of": packet_as_of,
            "available_from": packet_as_of,
            "source_version": source_version,
            "coverage_state": coverage,
            "maturity_state": "robust" if strong else "not_yet_mature",
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": {
                "horizon_sessions": horizon,
                "probability_target": str(semantics["probability_target"]),
                "calibration_scope": "timing_pattern",
                "calibration_split": split,
                "pattern_id": pattern_id,
                "pattern": pattern,
                "statistics": stats,
                "strong_validation": strong,
                "annotation_is_directional_vote": False,
            },
        })
    return rows


def build_phase2_probability_rows(
    *,
    symbol: str,
    selection: Mapping[str, object],
    timing: Sequence[Mapping[str, object]],
    calibration: Mapping[str, object],
    packet_as_of: str,
    source_digest: str | None = None,
) -> list[dict[str, object]]:
    validated = validate_probability_calibration(calibration)
    digest = source_digest or probability_calibration_digest(validated)
    source_version = f"{PHASE2_SOURCE_VERSION}:{digest[:16]}"
    return [
        *_selection_rows(
            symbol=symbol,
            selection=selection,
            calibration=validated,
            packet_as_of=packet_as_of,
            source_version=source_version,
        ),
        *_timing_rows(
            symbol=symbol,
            timing=timing,
            calibration=validated,
            packet_as_of=packet_as_of,
            source_version=source_version,
        ),
    ]
