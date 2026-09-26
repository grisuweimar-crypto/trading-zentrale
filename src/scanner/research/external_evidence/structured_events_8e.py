from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "external_evidence_8e_event_ledger_v1"


class StructuredEvent8EError(ValueError):
    pass


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise StructuredEvent8EError(f"missing {field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise StructuredEvent8EError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StructuredEvent8EError(f"{field} must be timezone-aware: {value!r}")
    return parsed


def _optional_aware_datetime(value: Any, *, field: str) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    return _aware_datetime(value, field=field)


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise StructuredEvent8EError(f"missing {field}")
    return value


def _validate_sha256(value: str) -> None:
    if len(value) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in value):
        raise StructuredEvent8EError("content_sha256 must be a 64-character hex digest")


def validate_event_evidence(
    row: Mapping[str, Any],
    *,
    allowed_event_types: set[str],
    source_ranks: Mapping[str, int],
) -> dict[str, Any]:
    event_key = _required_text(row, "canonical_event_key")
    event_type = _required_text(row, "event_type").upper()
    if event_type not in allowed_event_types:
        raise StructuredEvent8EError(f"unsupported event_type: {event_type}")

    source_class = _required_text(row, "source_class")
    if source_class not in source_ranks:
        raise StructuredEvent8EError(f"unsupported source_class: {source_class}")

    content_sha256 = _required_text(row, "content_sha256")
    _validate_sha256(content_sha256)

    valid_from = _aware_datetime(row.get("valid_from"), field="valid_from")
    ingested_at = _aware_datetime(row.get("ingested_at"), field="ingested_at")
    historical_time_proven = row.get("historical_publication_time_independently_proven") is True
    if valid_from < ingested_at and not historical_time_proven:
        raise StructuredEvent8EError(
            "valid_from cannot precede actual ingestion unless historical publication timing is independently proven"
        )

    published_at = _optional_aware_datetime(row.get("published_at"), field="published_at")
    proof_status = _required_text(row, "public_release_proof_status").upper()
    if proof_status not in {"PROVEN", "INGESTION_ONLY", "UNPROVEN"}:
        raise StructuredEvent8EError(f"unsupported public_release_proof_status: {proof_status}")
    if proof_status == "PROVEN" and published_at is None:
        raise StructuredEvent8EError("PROVEN public release requires published_at")
    if published_at is not None and valid_from < published_at:
        raise StructuredEvent8EError("valid_from cannot precede published_at")
    if historical_time_proven and published_at is None:
        raise StructuredEvent8EError(
            "historical publication timing cannot be proven without published_at"
        )

    strict_pit = row.get("strict_pit_eligible") is True
    discovery_only = row.get("discovery_only") is True
    if discovery_only and strict_pit:
        raise StructuredEvent8EError("discovery-only evidence cannot be strict PIT evidence")

    return {
        "canonical_event_key": event_key,
        "event_type": event_type,
        "subject_id": _required_text(row, "subject_id"),
        "source_id": _required_text(row, "source_id"),
        "source_class": source_class,
        "source_rank": int(source_ranks[source_class]),
        "source_event_id": _required_text(row, "source_event_id"),
        "event_state": _required_text(row, "event_state").upper(),
        "authority_scope": _required_text(row, "authority_scope"),
        "published_at": published_at,
        "valid_from": valid_from,
        "ingested_at": ingested_at,
        "content_sha256": content_sha256.lower(),
        "strict_pit_eligible": strict_pit,
        "discovery_only": discovery_only,
        "public_release_proof_status": proof_status,
        "historical_publication_time_independently_proven": historical_time_proven,
    }


def _serialize_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in ("published_at", "valid_from", "ingested_at"):
        value = result.get(field)
        if isinstance(value, datetime):
            result[field] = value.isoformat()
    return result


def resolve_event_group(rows: list[Mapping[str, Any]], *, as_of: datetime) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise StructuredEvent8EError("as_of must be timezone-aware")
    if not rows:
        raise StructuredEvent8EError("event group is empty")

    keys = {row["canonical_event_key"] for row in rows}
    types = {row["event_type"] for row in rows}
    subjects = {row["subject_id"] for row in rows}
    if len(keys) != 1 or len(types) != 1 or len(subjects) != 1:
        raise StructuredEvent8EError("event group mixes canonical keys, event types, or subjects")

    available = [
        row
        for row in rows
        if row["strict_pit_eligible"] and row["valid_from"] <= as_of
    ]
    discovery = [row for row in rows if row["discovery_only"]]

    if not available:
        return {
            "canonical_event_key": next(iter(keys)),
            "event_type": next(iter(types)),
            "subject_id": next(iter(subjects)),
            "as_of": as_of.isoformat(),
            "status": "UNCONFIRMED_DISCOVERY_ONLY" if discovery else "INSUFFICIENT_PIT_EVIDENCE",
            "first_public_release_at": None,
            "valid_from": None,
            "event_state": None,
            "authoritative_source_class": None,
            "source_conflict": False,
            "evidence": [_serialize_evidence(row) for row in rows],
        }

    proven_release = [
        row
        for row in available
        if row["public_release_proof_status"] == "PROVEN" and row["published_at"] is not None
    ]
    first_public_release_at = (
        min(row["published_at"] for row in proven_release) if proven_release else None
    )
    valid_from = min(row["valid_from"] for row in available)

    best_rank = min(row["source_rank"] for row in available)
    authoritative = [row for row in available if row["source_rank"] == best_rank]
    latest_authoritative_valid_from = max(row["valid_from"] for row in authoritative)
    current_authoritative = [
        row
        for row in authoritative
        if row["valid_from"] == latest_authoritative_valid_from
    ]
    states = {row["event_state"] for row in current_authoritative}
    source_conflict = len(states) != 1
    event_state = None if source_conflict else next(iter(states))

    if source_conflict:
        status = "CONFLICTING_SOURCES"
    elif first_public_release_at is None:
        status = "INSUFFICIENT_FIRST_PUBLIC_RELEASE_PROOF"
    else:
        status = "KNOWN"

    return {
        "canonical_event_key": next(iter(keys)),
        "event_type": next(iter(types)),
        "subject_id": next(iter(subjects)),
        "as_of": as_of.isoformat(),
        "status": status,
        "first_public_release_at": (
            first_public_release_at.isoformat() if first_public_release_at else None
        ),
        "valid_from": valid_from.isoformat(),
        "event_state": event_state,
        "authoritative_source_class": current_authoritative[0]["source_class"] if not source_conflict else None,
        "source_conflict": source_conflict,
        "evidence": [_serialize_evidence(row) for row in rows],
    }


def build_event_ledger(
    *,
    evidence_rows: Iterable[Mapping[str, Any]],
    as_of: datetime,
    allowed_event_types: Iterable[str],
    source_ranks: Mapping[str, int],
) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    allowed = {str(value).upper() for value in allowed_event_types}
    for raw in evidence_rows:
        normalized.append(
            validate_event_evidence(raw, allowed_event_types=allowed, source_ranks=source_ranks)
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in normalized:
        grouped[row["canonical_event_key"]].append(row)

    resolved = [
        resolve_event_group(rows, as_of=as_of)
        for _, rows in sorted(grouped.items())
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_A_structured_events_first_public_release",
        "status": "OUTCOME_BLIND_EVENT_LEDGER",
        "as_of": as_of.isoformat(),
        "event_count": len(resolved),
        "events": resolved,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "generic_sentiment_enabled": False,
            "fuzzy_event_deduplication_enabled": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }
