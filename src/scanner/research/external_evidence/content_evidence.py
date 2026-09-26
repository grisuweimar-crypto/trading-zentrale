from __future__ import annotations

from hashlib import sha256
from typing import Any, Iterable, Mapping


class ContentEvidenceError(ValueError):
    """Raised when issuer-content evidence cannot satisfy the 8C-E contract."""


def excerpt_sha256(text: str) -> str:
    return sha256(str(text).encode("utf-8")).hexdigest()


def _require_same(previous: Mapping[str, Any], current: Mapping[str, Any], fields: Iterable[str]) -> None:
    for field in fields:
        if previous.get(field) != current.get(field):
            raise ContentEvidenceError(
                f"Non-comparable evidence for {field}: {previous.get(field)!r} != {current.get(field)!r}"
            )


def _range(record: Mapping[str, Any]) -> tuple[float, float]:
    low = record.get("lower")
    high = record.get("upper")
    if low is None or high is None:
        raise ContentEvidenceError("Guidance comparison requires both lower and upper bounds")
    low_f = float(low)
    high_f = float(high)
    if low_f > high_f:
        raise ContentEvidenceError("Guidance lower bound exceeds upper bound")
    return low_f, high_f


def classify_guidance_change(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare issuer guidance without assigning market direction."""
    _require_same(previous, current, ("metric", "period", "unit", "basis"))
    old_low, old_high = _range(previous)
    new_low, new_high = _range(current)

    if new_low == old_low and new_high == old_high:
        change = "UNCHANGED"
    elif new_low >= old_low and new_high >= old_high and (new_low > old_low or new_high > old_high):
        change = "RAISE"
    elif new_low <= old_low and new_high <= old_high and (new_low < old_low or new_high < old_high):
        change = "CUT"
    else:
        change = "MIXED"

    return {
        "event_type": "GUIDANCE_CHANGE",
        "semantic_change": change,
        "previous_lower": old_low,
        "previous_upper": old_high,
        "current_lower": new_low,
        "current_upper": new_high,
        "metric": current.get("metric"),
        "period": current.get("period"),
        "unit": current.get("unit"),
        "basis": current.get("basis"),
        "market_direction": "UNKNOWN",
        "outcome_research": "NOT_RUN",
    }


def classify_dividend_change(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare regular dividends on an explicitly comparable basis."""
    _require_same(previous, current, ("security_class", "currency", "frequency"))
    if previous.get("is_special") is True or current.get("is_special") is True:
        raise ContentEvidenceError("Special dividends must be modeled separately")
    old = previous.get("amount_per_share")
    new = current.get("amount_per_share")
    if old is None or new is None:
        raise ContentEvidenceError("Dividend comparison requires amount_per_share")
    old_f = float(old)
    new_f = float(new)
    if new_f > old_f:
        change = "INCREASE"
    elif new_f < old_f:
        change = "CUT"
    else:
        change = "UNCHANGED"
    return {
        "event_type": "DIVIDEND_CHANGE",
        "semantic_change": change,
        "previous_amount_per_share": old_f,
        "current_amount_per_share": new_f,
        "security_class": current.get("security_class"),
        "currency": current.get("currency"),
        "frequency": current.get("frequency"),
        "market_direction": "UNKNOWN",
        "outcome_research": "NOT_RUN",
    }


def validate_issuer_action(
    record: Mapping[str, Any],
    *,
    event_type: str,
    allowed_actions: Iterable[str],
    allowed_instruments: Iterable[str] | None = None,
) -> dict[str, Any]:
    action = str(record.get("action") or "").strip().upper()
    allowed = {str(value).upper() for value in allowed_actions}
    if action not in allowed:
        raise ContentEvidenceError(f"Unsupported {event_type} action: {action!r}")

    output = {
        "event_type": event_type,
        "action": action,
        "market_direction": "UNKNOWN",
        "outcome_research": "NOT_RUN",
    }
    if allowed_instruments is not None:
        instrument = str(record.get("instrument") or "").strip().upper()
        allowed_i = {str(value).upper() for value in allowed_instruments}
        if instrument not in allowed_i:
            raise ContentEvidenceError(f"Unsupported {event_type} instrument: {instrument!r}")
        output["instrument"] = instrument
    if event_type == "CAPITAL_RAISE_ACTION" and record.get("is_shelf_registration") is True and action == "COMPLETED":
        raise ContentEvidenceError("Shelf registration alone may not be labeled as completed capital raise")
    return output


def require_consensus_for_beat_miss(*, consensus_pit_status: str | None) -> None:
    if str(consensus_pit_status or "").upper() != "SAFE":
        raise ContentEvidenceError(
            "Earnings beat/miss requires a SAFE historical point-in-time analyst consensus source"
        )


def build_evidence_record(
    *,
    event_type: str,
    cik: str,
    accession_number: str,
    source_valid_from: str,
    source_document: str,
    evidence_excerpt: str,
    extraction_method: str,
    parser_version: str,
    semantic_status: str,
    reason_codes: Iterable[str] = (),
) -> dict[str, Any]:
    required = {
        "event_type": event_type,
        "cik": cik,
        "accession_number": accession_number,
        "source_valid_from": source_valid_from,
        "source_document": source_document,
        "evidence_excerpt": evidence_excerpt,
        "extraction_method": extraction_method,
        "parser_version": parser_version,
        "semantic_status": semantic_status,
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        raise ContentEvidenceError("Missing evidence provenance: " + ", ".join(missing))
    return {
        "event_type": event_type,
        "cik": cik,
        "accession_number": accession_number,
        "source_valid_from": source_valid_from,
        "source_document": source_document,
        "evidence_excerpt_sha256": excerpt_sha256(evidence_excerpt),
        "extraction_method": extraction_method,
        "parser_version": parser_version,
        "semantic_status": semantic_status,
        "market_direction": "UNKNOWN",
        "reason_codes": sorted(set(str(code) for code in reason_codes)),
    }


def validate_evidence_record(record: Mapping[str, Any], *, required_fields: Iterable[str]) -> None:
    missing = [field for field in required_fields if field not in record]
    if missing:
        raise ContentEvidenceError("Evidence record missing fields: " + ", ".join(missing))
    if record.get("market_direction") != "UNKNOWN":
        raise ContentEvidenceError("8C-E may not assign market direction")
    digest = str(record.get("evidence_excerpt_sha256") or "")
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest.lower()):
        raise ContentEvidenceError("Invalid evidence excerpt SHA-256")
