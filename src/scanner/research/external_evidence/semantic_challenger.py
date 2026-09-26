from __future__ import annotations

import re
from typing import Any, Iterable, Mapping

from .content_evidence import (
    ContentEvidenceError,
    build_evidence_record,
    validate_issuer_action,
)


_DIVIDEND_RE = re.compile(
    r"\b(?:board(?:\s+of\s+directors)?\s+)?(?:declared|approved)\b"
    r".{0,120}?\bquarterly(?:\s+cash)?\s+dividend\b"
    r".{0,80}?(?:\bof\b\s+)?"
    r"(?P<currency>USD|US\$|U\.S\.\s*\$|\$)\s*"
    r"(?P<amount>\d{1,4}(?:\.\d{1,6})?)\s*"
    r"(?:per|a)\s+(?:common\s+)?share\b",
    re.IGNORECASE | re.DOTALL,
)

_BUYBACK_AFTER_RE = re.compile(
    r"\b(?:board(?:\s+of\s+directors)?\s+)?(?:authorized|approved)\b"
    r".{0,160}?\b(?:share|stock)\s+repurchase(?:\s+(?:program|plan|authorization))?\b"
    r".{0,120}?\b(?:up\s+to|maximum\s+of|aggregate\s+amount\s+of)\b\s*"
    r"(?P<currency>USD|US\$|U\.S\.\s*\$|\$)\s*"
    r"(?P<amount>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<scale>million|billion)\b",
    re.IGNORECASE | re.DOTALL,
)

_BUYBACK_BEFORE_RE = re.compile(
    r"\b(?:board(?:\s+of\s+directors)?\s+)?(?:authorized|approved)\b"
    r".{0,100}?\b(?:a\s+new\s+)?"
    r"(?P<currency>USD|US\$|U\.S\.\s*\$|\$)\s*"
    r"(?P<amount>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<scale>million|billion)\s+"
    r"(?:share|stock)\s+repurchase(?:\s+(?:program|plan|authorization))?\b",
    re.IGNORECASE | re.DOTALL,
)

_SPECIAL_DIVIDEND_RE = re.compile(r"\bspecial\s+(?:cash\s+)?dividend\b", re.IGNORECASE)


class SemanticChallengerError(ValueError):
    """Raised when challenger output violates the Phase 8C-H contract."""


def _currency(token: str) -> tuple[str, str, bool]:
    normalized = re.sub(r"\s+", "", str(token or "").upper())
    if normalized in {"USD", "US$", "U.S.$"}:
        return "USD", "EXPLICIT", True
    if normalized == "$":
        return "UNKNOWN", "AMBIGUOUS_SYMBOL", False
    return "UNKNOWN", "UNKNOWN", False


def _scaled_amount(amount: str, scale: str) -> float:
    base = float(str(amount).replace(",", ""))
    factor = {"million": 1_000_000.0, "billion": 1_000_000_000.0}[scale.lower()]
    return base * factor


def _base_evidence(anchor: Mapping[str, Any], *, event_type: str, parser_version: str) -> dict[str, Any]:
    return build_evidence_record(
        event_type=event_type,
        cik=str(anchor.get("cik") or ""),
        accession_number=str(anchor.get("accession_number") or ""),
        source_valid_from=str(anchor.get("source_valid_from") or ""),
        source_document=str(anchor.get("filename") or anchor.get("source_document_file") or ""),
        evidence_excerpt=str(anchor.get("excerpt") or ""),
        extraction_method="DETERMINISTIC_HIGH_PRECISION_RULE",
        parser_version=parser_version,
        semantic_status="EXTRACTED_CANDIDATE",
        reason_codes=(),
    )


def _dividend_candidate(anchor: Mapping[str, Any], *, parser_version: str) -> tuple[dict[str, Any] | None, str | None]:
    excerpt = str(anchor.get("excerpt") or "")
    if _SPECIAL_DIVIDEND_RE.search(excerpt):
        return None, "SPECIAL_DIVIDEND_OUT_OF_SCOPE"
    match = _DIVIDEND_RE.search(excerpt)
    if not match:
        return None, "NO_HIGH_PRECISION_DIVIDEND_PATTERN"

    currency, currency_status, comparison_eligible = _currency(match.group("currency"))
    record = _base_evidence(
        anchor,
        event_type="DIVIDEND_DECLARATION_OBSERVATION",
        parser_version=parser_version,
    )
    record.update(
        {
            "symbol": anchor.get("symbol"),
            "family": "DIVIDEND",
            "frequency": "quarterly",
            "dividend_kind": "regular",
            "amount_per_share": float(match.group("amount")),
            "currency": currency,
            "currency_status": currency_status,
            "comparison_eligible": comparison_eligible,
            "security_class": "UNKNOWN",
            "matched_rule": "QUARTERLY_DIVIDEND_EXPLICIT_ACTION_AMOUNT_PER_SHARE_V1",
        }
    )
    if not comparison_eligible:
        record["reason_codes"] = ["AMBIGUOUS_CURRENCY_SYMBOL"]
    return record, None


def _buyback_candidate(
    anchor: Mapping[str, Any],
    *,
    parser_version: str,
    allowed_actions: Iterable[str],
) -> tuple[dict[str, Any] | None, str | None]:
    excerpt = str(anchor.get("excerpt") or "")
    match = _BUYBACK_AFTER_RE.search(excerpt) or _BUYBACK_BEFORE_RE.search(excerpt)
    if not match:
        return None, "NO_HIGH_PRECISION_BUYBACK_PATTERN"

    action = validate_issuer_action(
        {"action": "NEW_AUTHORIZATION"},
        event_type="BUYBACK_ACTION",
        allowed_actions=allowed_actions,
    )
    currency, currency_status, _ = _currency(match.group("currency"))
    record = _base_evidence(anchor, event_type="BUYBACK_ACTION", parser_version=parser_version)
    record.update(
        {
            "symbol": anchor.get("symbol"),
            "family": "BUYBACK",
            "action": action["action"],
            "authorization_amount": _scaled_amount(match.group("amount"), match.group("scale")),
            "authorization_amount_raw": match.group("amount"),
            "authorization_scale": match.group("scale").lower(),
            "currency": currency,
            "currency_status": currency_status,
            "matched_rule": "NEW_REPURCHASE_AUTHORIZATION_EXPLICIT_AMOUNT_V1",
        }
    )
    if currency_status != "EXPLICIT":
        record["reason_codes"] = ["AMBIGUOUS_CURRENCY_SYMBOL"]
    return record, None


def extract_semantic_candidates(
    anchors: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    parser_version = str(contract.get("parser_version") or "")
    if not parser_version:
        raise SemanticChallengerError("parser_version is required")
    supported = contract.get("supported_families") or {}
    buyback_actions = (
        ((supported.get("BUYBACK") or {}).get("allowed_action"),)
        if (supported.get("BUYBACK") or {}).get("allowed_action")
        else ()
    )

    candidates: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for anchor in anchors:
        family = str(anchor.get("family") or "").upper()
        if family not in {"DIVIDEND", "BUYBACK"}:
            continue
        if str(anchor.get("semantic_status") or "") != "ANCHOR_ONLY":
            rejections.append(
                {
                    "family": family,
                    "accession_number": anchor.get("accession_number"),
                    "reason": "INPUT_NOT_ANCHOR_ONLY",
                }
            )
            continue
        try:
            if family == "DIVIDEND":
                candidate, reason = _dividend_candidate(anchor, parser_version=parser_version)
            else:
                candidate, reason = _buyback_candidate(
                    anchor,
                    parser_version=parser_version,
                    allowed_actions=buyback_actions,
                )
        except (ContentEvidenceError, ValueError) as exc:
            candidate = None
            reason = "PROVENANCE_OR_VALUE_ERROR"
            rejections.append(
                {
                    "family": family,
                    "accession_number": anchor.get("accession_number"),
                    "reason": reason,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:500],
                }
            )
            continue

        if candidate is None:
            rejections.append(
                {
                    "family": family,
                    "accession_number": anchor.get("accession_number"),
                    "reason": reason,
                }
            )
            continue

        key = (
            candidate.get("event_type"),
            candidate.get("accession_number"),
            candidate.get("evidence_excerpt_sha256"),
            candidate.get("amount_per_share"),
            candidate.get("authorization_amount"),
        )
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)

    return {
        "candidates": candidates,
        "rejections": rejections,
        "candidate_count": len(candidates),
        "rejection_count": len(rejections),
        "market_outcomes_read": False,
        "market_direction_assigned": False,
    }


def validate_semantic_candidates(
    candidates: Iterable[Mapping[str, Any]],
    *,
    required_fields: Iterable[str],
) -> None:
    required = list(required_fields)
    allowed_events = {"DIVIDEND_DECLARATION_OBSERVATION", "BUYBACK_ACTION"}
    for index, row in enumerate(candidates):
        missing = [field for field in required if field not in row]
        if missing:
            raise SemanticChallengerError(
                f"Semantic candidate {index} missing fields: {', '.join(missing)}"
            )
        if row.get("event_type") not in allowed_events:
            raise SemanticChallengerError(f"Unsupported challenger event: {row.get('event_type')!r}")
        if row.get("semantic_status") != "EXTRACTED_CANDIDATE":
            raise SemanticChallengerError("8C-H outputs must remain EXTRACTED_CANDIDATE")
        if row.get("market_direction") != "UNKNOWN":
            raise SemanticChallengerError("8C-H may not assign market direction")
        if row.get("family") == "DIVIDEND" and row.get("comparison_eligible") is True:
            if row.get("currency") == "UNKNOWN":
                raise SemanticChallengerError("Dividend comparison may not be enabled with unknown currency")
