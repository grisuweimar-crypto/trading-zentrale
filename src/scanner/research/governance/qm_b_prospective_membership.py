"""QM-B prospective project-universe membership evidence ledger.

Research-only.  This layer records what the project universe configuration explicitly
showed at a known observation time.  It does not infer exchange listing, tradability,
investability, historical membership, or negative membership from absence.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_b_identity_reconciliation import (
    is_valid_isin,
    stable_isin_instrument_id,
)


SNAPSHOT_SCHEMA_VERSION = "qm_b_prospective_membership_snapshot_v1"
LEDGER_EVENT_SCHEMA_VERSION = "qm_b_prospective_membership_ledger_event_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_prospective_membership_v1.json"


class ProspectiveMembershipError(ValueError):
    """Raised when project-universe membership evidence cannot be handled safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProspectiveMembershipError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProspectiveMembershipError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveMembershipError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _valid_sha256(value: Any) -> str:
    digest = _clean(value).lower()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ProspectiveMembershipError("universe_sha256_invalid")
    return digest


def load_membership_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveMembershipError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_prospective_membership_v1":
        raise ProspectiveMembershipError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProspectiveMembershipError("contract_scope_invalid")
    rules = payload.get("time_rules")
    if not isinstance(rules, Mapping):
        raise ProspectiveMembershipError("time_rules_missing")
    if rules.get("membership_valid_from_equals_universe_observed_at") is not True:
        raise ProspectiveMembershipError("membership_valid_from_rule_missing")
    if rules.get("historical_retrojection_permitted") is not False:
        raise ProspectiveMembershipError("retrojection_must_be_forbidden")
    if rules.get("absence_can_close_prior_positive_membership") is not False:
        raise ProspectiveMembershipError("absence_negative_inference_must_be_forbidden")
    return payload


def _parse_active(value: Any, contract: Mapping[str, Any]) -> bool | None:
    text = _clean(value).lower()
    if text in {str(v).lower() for v in contract["row_active_true_values"]}:
        return True
    if text in {str(v).lower() for v in contract["row_active_false_values"]}:
        return False
    return None


def build_membership_snapshot(
    universe_rows: list[Mapping[str, Any]],
    *,
    universe_snapshot_id: str,
    universe_observed_at: str,
    universe_sha256: str,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build one immutable prospective membership-evidence snapshot.

    Positive membership is aggregated at stable ISIN security identity.  Explicit
    inactive rows are retained, but never promoted to a negative OUT_OF_SCOPE claim.
    """
    contract = load_membership_contract(contract_path)
    observed = _utc(universe_observed_at, field="universe_observed_at")
    observed_iso = observed.isoformat()
    snapshot_source_id = _clean(universe_snapshot_id)
    if not snapshot_source_id:
        raise ProspectiveMembershipError("universe_snapshot_id_required")
    digest = _valid_sha256(universe_sha256)
    supported = {str(v).lower() for v in contract["supported_asset_types"]}

    grouped: dict[str, list[dict[str, Any]]] = {}
    unresolved: list[dict[str, Any]] = []

    for row_number, raw in enumerate(universe_rows, start=2):
        if not isinstance(raw, Mapping):
            raise ProspectiveMembershipError(f"universe_row_not_object:{row_number}")
        symbol = _upper(raw.get("symbol"))
        isin = _upper(raw.get("isin"))
        asset_type = _clean(raw.get("asset_type")).lower()
        active = _parse_active(raw.get("active"), contract)
        row_view = {
            "source_row_number": row_number,
            "symbol": symbol or None,
            "isin": isin or None,
            "asset_type": asset_type or None,
            "active_raw": _clean(raw.get("active")),
            "name": _clean(raw.get("name")) or None,
            "country": _clean(raw.get("country")) or None,
            "currency": _clean(raw.get("currency")) or None,
        }

        if asset_type not in supported:
            unresolved.append({**row_view, "status": "UNSUPPORTED_ASSET_TYPE"})
            continue
        if not is_valid_isin(isin):
            unresolved.append({**row_view, "status": "IDENTITY_UNRESOLVED_INVALID_OR_MISSING_ISIN"})
            continue
        if active is None:
            unresolved.append({**row_view, "status": "INVALID_ACTIVE_FLAG"})
            continue

        grouped.setdefault(isin, []).append({**row_view, "active": active})

    claims: list[dict[str, Any]] = []
    for isin, rows in sorted(grouped.items()):
        active_rows = [row for row in rows if row["active"] is True]
        inactive_rows = [row for row in rows if row["active"] is False]
        active_symbols = sorted({str(row["symbol"]) for row in active_rows if row.get("symbol")})
        all_symbols = sorted({str(row["symbol"]) for row in rows if row.get("symbol")})
        quality_flags: list[str] = []
        if len(active_rows) > 1:
            quality_flags.append("MULTIPLE_ACTIVE_ROWS_SAME_ISIN")
        if len(all_symbols) > 1:
            quality_flags.append("MULTIPLE_SYMBOLS_SAME_ISIN")
        if active_rows and inactive_rows:
            quality_flags.append("MIXED_ACTIVE_AND_INACTIVE_ROWS_SAME_ISIN")

        if active_rows:
            status = "OBSERVED_IN_PROJECT_UNIVERSE"
            positive_verified = True
        else:
            status = "EXPLICIT_INACTIVE_ROWS_ONLY"
            positive_verified = False

        claims.append(
            {
                "instrument_id": stable_isin_instrument_id(isin),
                "canonical_isin": isin,
                "membership_observation_status": status,
                "membership_valid_from": observed_iso,
                "active_symbols": active_symbols,
                "all_source_symbols": all_symbols,
                "active_row_count": len(active_rows),
                "inactive_row_count": len(inactive_rows),
                "source_rows": rows,
                "quality_flags": quality_flags,
                "positive_project_membership_verified": positive_verified,
                "negative_out_of_scope_verified": False,
                "listing_status": "UNKNOWN",
                "listing_venue": None,
                "tradability_status": "UNKNOWN",
                "project_investability_status": "UNKNOWN",
            }
        )

    status_counts: dict[str, int] = {}
    for claim in claims:
        key = str(claim["membership_observation_status"])
        status_counts[key] = status_counts.get(key, 0) + 1
    unresolved_counts: dict[str, int] = {}
    for row in unresolved:
        key = str(row["status"])
        unresolved_counts[key] = unresolved_counts.get(key, 0) + 1

    identity = {
        "universe_snapshot_id": snapshot_source_id,
        "universe_observed_at": observed_iso,
        "universe_sha256": digest,
    }
    membership_snapshot_id = "qmbm_" + _sha256(_canonical_json_bytes(identity))[:24]
    body: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": membership_snapshot_id,
        "universe_snapshot_id": snapshot_source_id,
        "universe_observed_at": observed_iso,
        "membership_valid_from": observed_iso,
        "universe_sha256": digest,
        "source_row_count": len(universe_rows),
        "stable_instrument_claim_count": len(claims),
        "unresolved_row_count": len(unresolved),
        "claim_status_counts": dict(sorted(status_counts.items())),
        "unresolved_status_counts": dict(sorted(unresolved_counts.items())),
        "claims": claims,
        "unresolved_rows": unresolved,
        "historical_retrojection_permitted": False,
        "absence_interpreted_as_out_of_scope": False,
        "negative_membership_promotion_performed": False,
        "strict_qm_b_membership_bundle_promotion_performed": False,
        "listing_status_promotion_performed": False,
        "tradability_promotion_performed": False,
        "project_investability_promotion_performed": False,
    }
    body["membership_snapshot_sha256"] = _sha256(_canonical_json_bytes(body))
    return body


def _verify_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise ProspectiveMembershipError("membership_snapshot_schema_invalid")
    if snapshot.get("historical_retrojection_permitted") is not False:
        raise ProspectiveMembershipError("membership_snapshot_retrojection_guard_missing")
    if snapshot.get("absence_interpreted_as_out_of_scope") is not False:
        raise ProspectiveMembershipError("membership_snapshot_absence_rule_invalid")
    if snapshot.get("negative_membership_promotion_performed") is not False:
        raise ProspectiveMembershipError("negative_membership_promotion_forbidden")
    supplied = _clean(snapshot.get("membership_snapshot_sha256")).lower()
    base = dict(snapshot)
    base.pop("membership_snapshot_sha256", None)
    if supplied != _sha256(_canonical_json_bytes(base)):
        raise ProspectiveMembershipError("membership_snapshot_hash_invalid")
    if snapshot.get("membership_valid_from") != snapshot.get("universe_observed_at"):
        raise ProspectiveMembershipError("membership_valid_from_not_observation_time")
    _utc(snapshot.get("universe_observed_at"), field="membership_snapshot.universe_observed_at")


def _event_hash(event: Mapping[str, Any]) -> str:
    return _sha256(_canonical_json_bytes(event))


def _read_ledger(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProspectiveMembershipError(f"ledger_invalid_json:{number}") from exc
        if not isinstance(value, dict):
            raise ProspectiveMembershipError(f"ledger_event_not_object:{number}")
        events.append(value)
    return events


def verify_membership_ledger(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    events = _read_ledger(target)
    previous: str | None = None
    snapshot_ids: list[str] = []
    observed_values: list[str] = []

    for expected_sequence, event in enumerate(events, start=1):
        if event.get("schema_version") != LEDGER_EVENT_SCHEMA_VERSION:
            raise ProspectiveMembershipError(f"ledger_schema_invalid:{expected_sequence}")
        if event.get("sequence") != expected_sequence:
            raise ProspectiveMembershipError(f"ledger_sequence_invalid:{expected_sequence}")
        if event.get("previous_event_hash") != previous:
            raise ProspectiveMembershipError(f"ledger_chain_invalid:{expected_sequence}")
        supplied = _clean(event.get("event_hash")).lower()
        body = dict(event)
        body.pop("event_hash", None)
        if supplied != _event_hash(body):
            raise ProspectiveMembershipError(f"ledger_event_hash_invalid:{expected_sequence}")
        snapshot = event.get("snapshot")
        if not isinstance(snapshot, Mapping):
            raise ProspectiveMembershipError(f"ledger_snapshot_missing:{expected_sequence}")
        _verify_snapshot(snapshot)
        snapshot_id = _clean(snapshot.get("membership_snapshot_id"))
        observed_at = _clean(snapshot.get("universe_observed_at"))
        if snapshot_id in snapshot_ids:
            raise ProspectiveMembershipError(f"ledger_duplicate_snapshot:{snapshot_id}")
        if observed_at in observed_values:
            raise ProspectiveMembershipError(f"ledger_duplicate_observation_time:{observed_at}")
        snapshot_ids.append(snapshot_id)
        observed_values.append(observed_at)
        previous = supplied

    return {
        "schema_version": "qm_b_prospective_membership_ledger_verification_v1",
        "valid": True,
        "event_count": len(events),
        "head_hash": previous,
        "membership_snapshot_ids": snapshot_ids,
        "observed_at_values": observed_values,
    }


def _acquire_lock(path: Path) -> int:
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError as exc:
        raise ProspectiveMembershipError(f"ledger_lock_exists:{lock_path}") from exc


def _release_lock(path: Path, fd: int) -> None:
    lock_path = path.with_suffix(path.suffix + ".lock")
    try:
        os.close(fd)
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def append_membership_snapshot(path: str | Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    target = Path(path)
    _verify_snapshot(snapshot)
    lock_fd = _acquire_lock(target)
    try:
        state = verify_membership_ledger(target)
        snapshot_id = _clean(snapshot.get("membership_snapshot_id"))
        observed_at = _clean(snapshot.get("universe_observed_at"))
        if snapshot_id in state["membership_snapshot_ids"]:
            raise ProspectiveMembershipError(f"duplicate_membership_snapshot:{snapshot_id}")
        if observed_at in state["observed_at_values"]:
            raise ProspectiveMembershipError(f"duplicate_membership_observation_time:{observed_at}")
        event: dict[str, Any] = {
            "schema_version": LEDGER_EVENT_SCHEMA_VERSION,
            "sequence": state["event_count"] + 1,
            "event_type": "PROJECT_UNIVERSE_MEMBERSHIP_SNAPSHOT_OBSERVED",
            "previous_event_hash": state["head_hash"],
            "snapshot": dict(snapshot),
        }
        event["event_hash"] = _event_hash(event)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(target, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
        try:
            os.write(fd, (_canonical_json_bytes(event) + b"\n"))
            os.fsync(fd)
        finally:
            os.close(fd)
        return verify_membership_ledger(target)
    finally:
        _release_lock(target, lock_fd)


def membership_as_of(path: str | Path, *, instrument_id: str, as_of: str) -> dict[str, Any]:
    """Query only the latest snapshot observed at or before ``as_of``.

    An instrument missing from that latest snapshot is UNKNOWN.  Older positive
    evidence is surfaced for audit context but is never carried forward as current.
    """
    target = Path(path)
    verify_membership_ledger(target)
    query_time = _utc(as_of, field="as_of")
    instrument_id = _clean(instrument_id)
    if not instrument_id:
        raise ProspectiveMembershipError("instrument_id_required")

    eligible: list[dict[str, Any]] = []
    for event in _read_ledger(target):
        snapshot = event["snapshot"]
        observed = _utc(snapshot["universe_observed_at"], field="ledger.snapshot.universe_observed_at")
        if observed <= query_time:
            eligible.append(event)
    if not eligible:
        return {
            "instrument_id": instrument_id,
            "as_of": query_time.isoformat(),
            "status": "UNKNOWN_NO_SNAPSHOT_AT_OR_BEFORE_QUERY",
            "out_of_scope_verified": False,
            "positive_membership_carried_forward": False,
        }

    eligible.sort(key=lambda event: (_utc(event["snapshot"]["universe_observed_at"], field="observed"), event["sequence"]))
    latest = eligible[-1]["snapshot"]
    latest_claim = next((claim for claim in latest["claims"] if claim.get("instrument_id") == instrument_id), None)
    if latest_claim is not None:
        return {
            "instrument_id": instrument_id,
            "as_of": query_time.isoformat(),
            "latest_membership_snapshot_id": latest["membership_snapshot_id"],
            "latest_snapshot_observed_at": latest["universe_observed_at"],
            "status": latest_claim["membership_observation_status"],
            "claim": latest_claim,
            "out_of_scope_verified": False,
            "positive_membership_carried_forward": False,
        }

    prior_positive = None
    for event in reversed(eligible[:-1]):
        claim = next(
            (
                row
                for row in event["snapshot"]["claims"]
                if row.get("instrument_id") == instrument_id
                and row.get("membership_observation_status") == "OBSERVED_IN_PROJECT_UNIVERSE"
            ),
            None,
        )
        if claim is not None:
            prior_positive = {
                "membership_snapshot_id": event["snapshot"]["membership_snapshot_id"],
                "observed_at": event["snapshot"]["universe_observed_at"],
                "claim": claim,
            }
            break

    return {
        "instrument_id": instrument_id,
        "as_of": query_time.isoformat(),
        "latest_membership_snapshot_id": latest["membership_snapshot_id"],
        "latest_snapshot_observed_at": latest["universe_observed_at"],
        "status": "UNKNOWN_ABSENT_FROM_LATEST_SNAPSHOT",
        "prior_positive_evidence": prior_positive,
        "out_of_scope_verified": False,
        "positive_membership_carried_forward": False,
    }
