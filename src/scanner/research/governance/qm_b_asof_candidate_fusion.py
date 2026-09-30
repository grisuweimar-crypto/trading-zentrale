"""QM-B PIT-safe fusion of identity, membership and listing evidence.

Research-only.  This layer creates pre-strict as-of universe candidates.  It never
promotes project investability, market tradability, negative membership or productive
scanner behavior.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "qm_b_asof_candidate_fusion_result_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_asof_candidate_fusion_v1.json"


class AsOfCandidateFusionError(ValueError):
    """Raised when evidence cannot be fused without violating PIT invariants."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise AsOfCandidateFusionError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AsOfCandidateFusionError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise AsOfCandidateFusionError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_fusion_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsOfCandidateFusionError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_asof_candidate_fusion_v1":
        raise AsOfCandidateFusionError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise AsOfCandidateFusionError("contract_scope_invalid")
    time_rules = payload.get("time_rules")
    if not isinstance(time_rules, Mapping):
        raise AsOfCandidateFusionError("time_rules_missing")
    if time_rules.get("historical_retrojection_permitted") is not False:
        raise AsOfCandidateFusionError("retrojection_must_be_forbidden")
    if time_rules.get("candidate_valid_from_is_max_of_input_evidence_times") is not True:
        raise AsOfCandidateFusionError("candidate_time_rule_missing")
    strict = payload.get("strict_bundle_promotion")
    if not isinstance(strict, Mapping) or strict.get("performed_by_this_package") is not False:
        raise AsOfCandidateFusionError("strict_bundle_promotion_must_be_disabled")
    return payload


def _validate_membership_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise AsOfCandidateFusionError("membership_snapshot_schema_invalid")
    if snapshot.get("historical_retrojection_permitted") is not False:
        raise AsOfCandidateFusionError("membership_snapshot_retrojection_guard_missing")
    if snapshot.get("absence_interpreted_as_out_of_scope") is not False:
        raise AsOfCandidateFusionError("membership_snapshot_absence_rule_invalid")
    claims = snapshot.get("claims")
    if not isinstance(claims, list):
        raise AsOfCandidateFusionError("membership_claims_must_be_list")
    _utc(snapshot.get("universe_observed_at"), field="membership.universe_observed_at")
    if _clean(snapshot.get("membership_valid_from")) != _clean(snapshot.get("universe_observed_at")):
        raise AsOfCandidateFusionError("membership_valid_from_mismatch")


def _validate_listing_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema_version") != "qm_b_prospective_listing_snapshot_v1":
        raise AsOfCandidateFusionError("listing_snapshot_schema_invalid")
    if snapshot.get("historical_retrojection_permitted") is not False:
        raise AsOfCandidateFusionError("listing_snapshot_retrojection_guard_missing")
    if _clean(snapshot.get("valid_from")) != _clean(snapshot.get("retrieved_at")):
        raise AsOfCandidateFusionError("listing_valid_from_must_equal_retrieved_at")
    if not isinstance(snapshot.get("records"), list):
        raise AsOfCandidateFusionError("listing_records_must_be_list")
    _utc(snapshot.get("valid_from"), field="listing.valid_from")


def _validate_identity_match(match: Mapping[str, Any]) -> None:
    if match.get("schema_version") != "qm_b_prospective_identity_match_result_v1":
        raise AsOfCandidateFusionError("identity_match_schema_invalid")
    if match.get("historical_retrojection_permitted") is not False:
        raise AsOfCandidateFusionError("identity_match_retrojection_guard_missing")
    if not isinstance(match.get("matches"), list):
        raise AsOfCandidateFusionError("identity_matches_must_be_list")
    _utc(match.get("identity_match_valid_from"), field="identity.identity_match_valid_from")


def _membership_by_instrument(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(snapshot.get("claims", []), start=1):
        if not isinstance(raw, Mapping):
            raise AsOfCandidateFusionError(f"membership_claim_not_object:{index}")
        instrument_id = _clean(raw.get("instrument_id"))
        if not instrument_id:
            raise AsOfCandidateFusionError(f"membership_instrument_id_missing:{index}")
        if instrument_id in result:
            raise AsOfCandidateFusionError(f"duplicate_membership_instrument:{instrument_id}")
        result[instrument_id] = dict(raw)
    return result


def _candidate_base(instrument_id: str, membership: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "instrument_id": instrument_id,
        "canonical_isin": membership.get("canonical_isin"),
        "membership_observation_status": membership.get("membership_observation_status"),
        "membership_valid_from": membership.get("membership_valid_from"),
        "candidate_status": None,
        "reason_codes": [],
        "source_symbol": None,
        "listing_venue": None,
        "listing_venue_namespace": None,
        "listing_state": None,
        "identity_valid_from": None,
        "listing_valid_from": None,
        "candidate_valid_from": None,
        "identity_membership_listing_fused": False,
        "strict_bundle_promotion_ready": False,
        "strict_bundle_promotion_performed": False,
        "market_tradability_status": "UNKNOWN",
        "project_investability_status": "UNKNOWN",
    }


def fuse_asof_candidates(
    membership_snapshot: Mapping[str, Any],
    *,
    listing_snapshot: Mapping[str, Any] | None = None,
    identity_match: Mapping[str, Any] | None = None,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Fuse three already PIT-controlled evidence classes without over-promotion."""
    contract = load_fusion_contract(contract_path)
    _validate_membership_snapshot(membership_snapshot)
    memberships = _membership_by_instrument(membership_snapshot)
    required_membership = str(contract["required_membership_status"])

    # A missing listing source is a valid fail-closed audit result, not an exception.
    if listing_snapshot is None or identity_match is None:
        rows = []
        for instrument_id, membership in sorted(memberships.items()):
            row = _candidate_base(instrument_id, membership)
            if membership.get("membership_observation_status") != required_membership:
                row["candidate_status"] = "BLOCKED_MEMBERSHIP_NOT_POSITIVE"
                row["reason_codes"] = ["MEMBERSHIP_NOT_POSITIVE"]
            else:
                row["candidate_status"] = "BLOCKED_NO_LISTING_EVIDENCE"
                row["reason_codes"] = ["NO_PIT_LISTING_SNAPSHOT_AND_IDENTITY_MATCH"]
            rows.append(row)
        return _result(membership_snapshot, rows, listing_snapshot=None, identity_match=None)

    _validate_listing_snapshot(listing_snapshot)
    _validate_identity_match(identity_match)

    if _clean(identity_match.get("source_snapshot_id")) != _clean(listing_snapshot.get("snapshot_id")):
        raise AsOfCandidateFusionError("listing_identity_source_snapshot_mismatch")
    if _clean(identity_match.get("source_id")) != _clean(listing_snapshot.get("source_id")):
        raise AsOfCandidateFusionError("listing_identity_source_id_mismatch")
    if _clean(identity_match.get("universe_snapshot_id")) != _clean(membership_snapshot.get("universe_snapshot_id")):
        raise AsOfCandidateFusionError("identity_membership_universe_snapshot_mismatch")
    if _clean(identity_match.get("universe_sha256")).lower() != _clean(membership_snapshot.get("universe_sha256")).lower():
        raise AsOfCandidateFusionError("identity_membership_universe_hash_mismatch")
    if _utc(identity_match.get("universe_observed_at"), field="identity.universe_observed_at") != _utc(
        membership_snapshot.get("universe_observed_at"), field="membership.universe_observed_at"
    ):
        raise AsOfCandidateFusionError("identity_membership_universe_time_mismatch")

    listing_records = listing_snapshot["records"]
    matches = identity_match["matches"]
    if len(matches) != len(listing_records):
        raise AsOfCandidateFusionError("listing_identity_record_count_mismatch")

    matched_by_instrument: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for position, raw_match in enumerate(matches, start=1):
        if not isinstance(raw_match, Mapping):
            raise AsOfCandidateFusionError(f"identity_match_row_not_object:{position}")
        source_index = raw_match.get("source_record_index")
        if source_index != position:
            raise AsOfCandidateFusionError(f"identity_source_record_index_invalid:{position}")
        if raw_match.get("match_status") != contract["required_identity_match_status"]:
            continue
        instrument_id = _clean(raw_match.get("candidate_instrument_id"))
        if not instrument_id:
            raise AsOfCandidateFusionError(f"matched_identity_missing_instrument:{position}")
        listing_record = listing_records[position - 1]
        if not isinstance(listing_record, Mapping):
            raise AsOfCandidateFusionError(f"listing_record_not_object:{position}")
        matched_by_instrument.setdefault(instrument_id, []).append((dict(raw_match), dict(listing_record)))

    rows: list[dict[str, Any]] = []
    listing_time = _utc(listing_snapshot.get("valid_from"), field="listing.valid_from")
    membership_time = _utc(membership_snapshot.get("membership_valid_from"), field="membership.membership_valid_from")

    for instrument_id, membership in sorted(memberships.items()):
        base = _candidate_base(instrument_id, membership)
        if membership.get("membership_observation_status") != required_membership:
            base["candidate_status"] = "BLOCKED_MEMBERSHIP_NOT_POSITIVE"
            base["reason_codes"] = ["MEMBERSHIP_NOT_POSITIVE"]
            rows.append(base)
            continue

        evidence_pairs = matched_by_instrument.get(instrument_id, [])
        if not evidence_pairs:
            base["candidate_status"] = "BLOCKED_NO_IDENTITY_MATCH"
            base["reason_codes"] = ["NO_MATCHED_LISTING_RECORD_FOR_MEMBER_INSTRUMENT"]
            rows.append(base)
            continue

        by_venue: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        no_venue: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for pair in evidence_pairs:
            venue = _clean(pair[1].get("venue_code")).upper()
            if venue:
                by_venue.setdefault(venue, []).append(pair)
            else:
                no_venue.append(pair)

        for match_row, listing_record in no_venue:
            row = dict(base)
            row["source_symbol"] = listing_record.get("source_symbol")
            row["listing_state"] = listing_record.get("listing_state")
            row["candidate_status"] = "BLOCKED_VENUE_MISSING"
            row["reason_codes"] = ["LISTING_RECORD_HAS_NO_VENUE_CODE"]
            rows.append(row)

        for venue, pairs in sorted(by_venue.items()):
            if len(pairs) > 1:
                row = dict(base)
                row["listing_venue"] = venue
                row["candidate_status"] = "REVIEW_REQUIRED_DUPLICATE_LISTING_VENUE"
                row["reason_codes"] = ["MULTIPLE_MATCHED_LISTING_RECORDS_FOR_SAME_INSTRUMENT_AND_VENUE"]
                rows.append(row)
                continue

            match_row, listing_record = pairs[0]
            row = dict(base)
            row["source_symbol"] = listing_record.get("source_symbol")
            row["listing_venue"] = venue
            row["listing_venue_namespace"] = listing_record.get("venue_namespace")
            row["listing_state"] = listing_record.get("listing_state")
            row["listing_valid_from"] = listing_snapshot.get("valid_from")
            row["identity_valid_from"] = match_row.get("identity_valid_from")

            if listing_record.get("listing_state") != contract["required_listing_state"]:
                row["candidate_status"] = "BLOCKED_LISTING_NOT_POSITIVE"
                row["reason_codes"] = ["LISTING_STATE_NOT_POSITIVE"]
                rows.append(row)
                continue
            if _clean(match_row.get("candidate_instrument_id")) != instrument_id:
                row["candidate_status"] = "BLOCKED_IDENTITY_MISMATCH"
                row["reason_codes"] = ["MATCHED_IDENTITY_DIFFERS_FROM_MEMBERSHIP_IDENTITY"]
                rows.append(row)
                continue

            identity_time = _utc(match_row.get("identity_valid_from"), field="match.identity_valid_from")
            candidate_time = max(membership_time, listing_time, identity_time)
            row["candidate_valid_from"] = candidate_time.isoformat()
            row["candidate_status"] = "FUSED_LISTED_MEMBER_CANDIDATE"
            row["reason_codes"] = [
                "STABLE_IDENTITY_MATCHED",
                "PROJECT_MEMBERSHIP_OBSERVED",
                "LISTING_PRESENT_IN_PIT_SNAPSHOT",
                "INVESTABILITY_STILL_UNKNOWN",
            ]
            row["identity_membership_listing_fused"] = True
            row["strict_bundle_promotion_ready"] = False
            rows.append(row)

    return _result(membership_snapshot, rows, listing_snapshot=listing_snapshot, identity_match=identity_match)


def _result(
    membership_snapshot: Mapping[str, Any],
    rows: list[dict[str, Any]],
    *,
    listing_snapshot: Mapping[str, Any] | None,
    identity_match: Mapping[str, Any] | None,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row["candidate_status"])
        counts[status] = counts.get(status, 0) + 1
    fused = counts.get("FUSED_LISTED_MEMBER_CANDIDATE", 0)
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": membership_snapshot.get("membership_snapshot_id"),
        "membership_universe_snapshot_id": membership_snapshot.get("universe_snapshot_id"),
        "listing_snapshot_id": listing_snapshot.get("snapshot_id") if listing_snapshot else None,
        "identity_source_snapshot_id": identity_match.get("source_snapshot_id") if identity_match else None,
        "candidate_count": len(rows),
        "fused_candidate_count": fused,
        "status_counts": dict(sorted(counts.items())),
        "historical_retrojection_permitted": False,
        "negative_membership_promotion_performed": False,
        "tradability_promotion_performed": False,
        "project_investability_promotion_performed": False,
        "strict_bundle_promotion_performed": False,
        "strict_bundle_blocker": "PROJECT_INVESTABILITY_UNRESOLVED",
        "candidates": rows,
    }


def load_json(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsOfCandidateFusionError(f"json_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise AsOfCandidateFusionError(f"json_not_object:{path}")
    return payload
