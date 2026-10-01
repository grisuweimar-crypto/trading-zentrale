"""W11 real end-to-end acceptance contract for one Depot-Watch snapshot.

W11 adds no investment logic. It verifies that one already sealed W10 snapshot
can traverse the existing 7A->7H chain without violating snapshot identity,
point-in-time, missing-evidence, privacy or Phase-8 boundaries.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, Sequence

from .input_contract import validate_input_packet
from .w10_orchestration import validate_sealed_manifest


SCHEMA_VERSION = "decision_watch_e2e_acceptance_w11_v1"
FORBIDDEN_SUPER_SCORE_KEYS = frozenset({
    "super_score",
    "superscore",
    "combined_score",
    "composite_score",
    "aggregate_score",
    "decision_score",
    "overall_score",
})


class W11EndToEndError(ValueError):
    """Raised when a real snapshot does not satisfy the W11 acceptance gate."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise W11EndToEndError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise W11EndToEndError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise W11EndToEndError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _forbidden_score_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).lower()
            child = f"{path}.{key}"
            if key_text in FORBIDDEN_SUPER_SCORE_KEYS:
                found.append(child)
            found.extend(_forbidden_score_paths(item, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_score_paths(item, f"{path}[{index}]"))
    return found


def _require_false(mapping: Mapping[str, object], key: str) -> None:
    if mapping.get(key) is not False:
        raise W11EndToEndError(f"guard_must_be_false:{key}")


def _validate_final_watch_shape(watch: Mapping[str, object]) -> dict[str, object]:
    """Validate final 7H semantics after W6/W7/W8 sidecars were attached.

    ``build_depot_watch`` seals and validates the base 7H object before the
    existing orchestrator adds review/history/policy sidecars. W11 therefore
    validates the final semantic contract directly instead of pretending that
    the original base ``watch_id`` hashes those later sidecars too.
    """
    if watch.get("schema_version") != "decision_depot_watch_v1":
        raise W11EndToEndError("unsupported_depot_watch_schema")
    if watch.get("phase") != "7H":
        raise W11EndToEndError("invalid_depot_watch_phase")
    if watch.get("watch_status") not in {"complete", "partial", "unavailable"}:
        raise W11EndToEndError("invalid_watch_status")
    if not str(watch.get("source_snapshot_id") or "").strip():
        raise W11EndToEndError("watch_source_snapshot_id_required")
    if not isinstance(watch.get("rows"), list):
        raise W11EndToEndError("watch_rows_required")
    if not str(watch.get("watch_id") or "").strip():
        raise W11EndToEndError("base_watch_id_required")
    return deepcopy(dict(watch))


def _current_packets(
    archive_packets: Sequence[Mapping[str, object]], *, snapshot_id: str
) -> list[dict[str, object]]:
    current: list[dict[str, object]] = []
    seen_symbols: set[str] = set()
    seen_claims: set[str] = set()
    for raw in archive_packets:
        packet = validate_input_packet(raw)
        if str(packet.get("source_snapshot_id")) != snapshot_id:
            continue
        symbol = str(packet.get("symbol") or "")
        if symbol in seen_symbols:
            raise W11EndToEndError(f"duplicate_current_packet_symbol:{symbol}")
        seen_symbols.add(symbol)
        evidence = packet.get("evidence")
        assert isinstance(evidence, list)
        families = {str(row.get("family")) for row in evidence if isinstance(row, Mapping)}
        if "selection" not in families:
            raise W11EndToEndError(f"phase1_selection_missing:{symbol}")
        coverage = packet.get("coverage")
        if not isinstance(coverage, Mapping) or coverage.get("missing_is_neutral") is not False:
            raise W11EndToEndError(f"missing_evidence_neutralized:{symbol}")
        for row in evidence:
            assert isinstance(row, Mapping)
            claim_id = str(row.get("claim_id") or "")
            if claim_id in seen_claims:
                raise W11EndToEndError(f"duplicate_current_claim:{claim_id}")
            seen_claims.add(claim_id)
            if row.get("pit_state") not in {"verified", "partial"}:
                raise W11EndToEndError(f"pit_not_acceptable:{symbol}:{claim_id}")
        current.append(packet)
    if not current:
        raise W11EndToEndError("current_7a_packets_missing")
    return current


def validate_w11_end_to_end(
    *,
    manifest: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
    watch: Mapping[str, object],
    diagnostics: Mapping[str, object],
) -> dict[str, object]:
    """Validate one complete real W11 snapshot and return a sanitized receipt."""
    sealed = validate_sealed_manifest(manifest)
    snapshot_id = str(sealed["snapshot_id"])
    validated_watch = _validate_final_watch_shape(watch)
    if str(validated_watch.get("source_snapshot_id")) != snapshot_id:
        raise W11EndToEndError("watch_snapshot_mismatch")
    if str(diagnostics.get("snapshot_id") or "") != snapshot_id:
        raise W11EndToEndError("diagnostics_snapshot_mismatch")
    if validated_watch.get("watch_status") != "complete":
        raise W11EndToEndError("watch_not_complete")

    stages = sealed.get("stages")
    assert isinstance(stages, Mapping)
    phase6 = stages.get("phase6_elliott")
    if not isinstance(phase6, Mapping):
        raise W11EndToEndError("phase6_resolution_missing")
    if phase6.get("status") == "not_supplied" and phase6.get("missing_is_neutral_evidence") is not False:
        raise W11EndToEndError("phase6_missing_treated_as_neutral")

    current = _current_packets(archive_packets, snapshot_id=snapshot_id)
    final_7a = stages.get("final_7a")
    if not isinstance(final_7a, Mapping):
        raise W11EndToEndError("final_7a_stage_missing")
    decision_time = _utc(final_7a.get("available_from"), "final_7a_available_from")
    sealed_at = _utc(sealed.get("sealed_at"), "sealed_at")
    diagnostic_time = _utc(diagnostics.get("decision_as_of"), "diagnostics_decision_as_of")
    if diagnostic_time != decision_time:
        raise W11EndToEndError("decision_time_mismatch")
    if decision_time > sealed_at:
        raise W11EndToEndError("future_final_7a")

    for packet in current:
        symbol = str(packet["symbol"])
        packet_time = _utc(packet.get("as_of"), "packet_as_of")
        if packet_time != decision_time:
            raise W11EndToEndError(f"current_packet_time_mismatch:{symbol}")
        evidence = packet.get("evidence")
        assert isinstance(evidence, list)
        for row in evidence:
            assert isinstance(row, Mapping)
            if _utc(row.get("as_of"), "evidence_as_of") > decision_time:
                raise W11EndToEndError(f"future_evidence_as_of:{symbol}")
            if _utc(row.get("available_from"), "evidence_available_from") > decision_time:
                raise W11EndToEndError(f"future_evidence_available_from:{symbol}")

    _require_false(diagnostics, "scanner_scalar_fallback_used")
    _require_false(diagnostics, "private_position_data_persisted")
    _require_false(diagnostics, "phase8_external_evidence_activated")
    _require_false(diagnostics, "selection_direction_inferred")
    _require_false(diagnostics, "phase5_shadow_changes_portfolio_action")
    _require_false(diagnostics, "elliott_changed_universal_stance")
    _require_false(diagnostics, "elliott_direction_used_as_vote")

    semantics = validated_watch.get("semantics")
    privacy = validated_watch.get("privacy")
    presentation = validated_watch.get("presentation")
    if not isinstance(semantics, Mapping) or not isinstance(privacy, Mapping) or not isinstance(presentation, Mapping):
        raise W11EndToEndError("watch_contract_sections_missing")
    _require_false(semantics, "scanner_scalar_replaced_missing_decision_evidence")
    _require_false(semantics, "numeric_reliability_score_created")
    _require_false(semantics, "holdings_ranked")
    if privacy.get("public_repository_persistence_default") is not False:
        raise W11EndToEndError("private_persistence_guard_invalid")
    if privacy.get("autopilot_public_commit_enabled") is not False:
        raise W11EndToEndError("private_autopilot_commit_guard_invalid")
    if presentation.get("grouping_is_ranking") is not False or presentation.get("action_or_reliability_ranking_performed") is not False:
        raise W11EndToEndError("watch_ranking_guard_invalid")

    forbidden = sorted(set(
        _forbidden_score_paths(current)
        + _forbidden_score_paths(validated_watch)
        + _forbidden_score_paths(diagnostics)
    ))
    if forbidden:
        raise W11EndToEndError("super_score_forbidden:" + ",".join(forbidden))

    guards = sealed.get("guards")
    downstream = sealed.get("downstream_contract")
    if not isinstance(guards, Mapping) or not isinstance(downstream, Mapping):
        raise W11EndToEndError("w10_contract_sections_missing")
    _require_false(guards, "later_evidence_backdating_allowed")
    _require_false(guards, "phase6_missing_is_neutral_evidence")
    _require_false(guards, "private_position_data_persisted")
    if downstream.get("requires_same_snapshot_id") is not True:
        raise W11EndToEndError("downstream_snapshot_guard_missing")
    if downstream.get("private_position_data_persisted") is not False:
        raise W11EndToEndError("downstream_private_persistence_guard_invalid")

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed",
        "snapshot_id": snapshot_id,
        "chain": ["scanner", "1", "2", "3", "4/5", "6", "7A", "7D", "7E", "7F", "7G", "7H"],
        "checks": {
            "same_snapshot_identity": True,
            "pit_correct": True,
            "no_future_evidence": True,
            "missing_evidence_not_neutral": True,
            "scanner_scalar_fallback_disabled": True,
            "private_depot_data_not_persisted": True,
            "no_duplicate_current_evidence": True,
            "no_super_score": True,
            "no_unauthorized_phase8_effect": True,
        },
        "w10_manifest_status": "sealed",
        "watch_status": "complete",
        "receipt_contains_position_rows": False,
    }
