from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping


class StructuredEventContractError(ValueError):
    """Raised when a filing cannot be represented safely by the 8C-D contract."""


def _normalize_form(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_items(value: Any) -> list[str]:
    if value is None:
        return []
    raw = value if isinstance(value, list) else [value]
    output: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if text.lower().startswith("item "):
            text = text[5:].strip()
        if text:
            output.append(text)
    return output


def build_event_candidates(
    filing_rows: Iterable[Mapping[str, Any]],
    *,
    item_mapping: Mapping[str, str],
    allowed_forms: Iterable[str] = ("8-K", "8-K/A", "6-K", "6-K/A"),
) -> list[dict[str, Any]]:
    """Create non-directional event candidates from normalized SEC filing rows.

    The mapping is intentionally limited to filing metadata. It does not inspect
    market outcomes and does not infer guidance, dividend, buyback, beat/miss or
    other content semantics that require filing-text evidence.
    """
    allowed = {_normalize_form(form) for form in allowed_forms}
    output: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for filing in filing_rows:
        form = _normalize_form(filing.get("form"))
        if form not in allowed:
            continue

        accession = str(filing.get("accession_number") or "").strip() or None
        cik = filing.get("cik")
        items = _normalize_items(filing.get("items"))
        publication_stage = str(filing.get("publication_stage") or "UNKNOWN")
        base_reasons = list(filing.get("reason_codes") or [])
        valid_from = filing.get("valid_from")
        published_at = filing.get("published_at")
        filing_status = str(filing.get("status") or "UNKNOWN")

        candidates: list[tuple[str | None, str, str, list[str]]] = []
        if form in {"8-K", "8-K/A"}:
            mapped_any = False
            for item in items:
                event_type = item_mapping.get(item)
                if event_type:
                    mapped_any = True
                    candidates.append((item, str(event_type), "FILING_ITEM_ONLY", []))
                else:
                    candidates.append(
                        (
                            item,
                            "UNMAPPED_8K_ITEM",
                            "UNKNOWN",
                            ["UNMAPPED_8K_ITEM"],
                        )
                    )
            if not items:
                candidates.append(
                    (
                        None,
                        "UNCLASSIFIED_CURRENT_REPORT",
                        "UNKNOWN",
                        ["NO_8K_ITEM_CODES"],
                    )
                )
            elif not mapped_any and not candidates:
                candidates.append(
                    (
                        None,
                        "UNCLASSIFIED_CURRENT_REPORT",
                        "UNKNOWN",
                        ["NO_MAPPED_8K_ITEM"],
                    )
                )
        else:
            candidates.append(
                (
                    None,
                    "FOREIGN_CURRENT_REPORT_UNCLASSIFIED",
                    "UNKNOWN",
                    ["6K_REQUIRES_CONTENT_EVIDENCE"],
                )
            )

        for item_code, event_type, semantic_state, extra_reasons in candidates:
            reasons = [*base_reasons, *extra_reasons]
            status = filing_status
            if not accession:
                status = "UNKNOWN"
                reasons.append("MISSING_ACCESSION")
            if not valid_from:
                status = "UNKNOWN"
                reasons.append("MISSING_VALID_FROM")

            row = {
                "source": filing.get("source") or "sec_edgar_submissions_8k_6k",
                "cik": cik,
                "accession_number": accession,
                "form": filing.get("form"),
                "item_code": item_code,
                "candidate_event_type": event_type,
                "semantic_state": semantic_state,
                "direction": "UNKNOWN",
                "published_at": published_at,
                "valid_from": valid_from,
                "revision_id": filing.get("revision_id") or accession,
                "publication_stage": publication_stage,
                "status": status,
                "reason_codes": sorted(set(reasons)),
            }
            key = (
                row["cik"],
                row["accession_number"],
                row["form"],
                row["item_code"],
                row["candidate_event_type"],
            )
            if key in seen:
                continue
            seen.add(key)
            output.append(row)

    return output


def event_coverage(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    records = [dict(row) for row in rows]
    by_type = Counter(str(row.get("candidate_event_type") or "UNKNOWN") for row in records)
    by_form = Counter(_normalize_form(row.get("form")) or "UNKNOWN" for row in records)
    by_semantic = Counter(str(row.get("semantic_state") or "UNKNOWN") for row in records)
    by_stage = Counter(str(row.get("publication_stage") or "UNKNOWN") for row in records)
    status = Counter(str(row.get("status") or "UNKNOWN") for row in records)
    amendment_count = sum(
        1
        for row in records
        if _normalize_form(row.get("form")).endswith("/A")
        or str(row.get("publication_stage") or "").upper() == "AMENDMENT"
    )
    unknown_direction_count = sum(1 for row in records if row.get("direction") == "UNKNOWN")
    return {
        "event_count": len(records),
        "candidate_event_type_counts": dict(sorted(by_type.items())),
        "form_counts": dict(sorted(by_form.items())),
        "semantic_state_counts": dict(sorted(by_semantic.items())),
        "publication_stage_counts": dict(sorted(by_stage.items())),
        "status_counts": dict(sorted(status.items())),
        "amendment_count": amendment_count,
        "unknown_direction_count": unknown_direction_count,
        "all_directions_unknown": unknown_direction_count == len(records),
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
    }


def validate_event_contract(
    rows: Iterable[Mapping[str, Any]],
    *,
    required_fields: Iterable[str],
) -> None:
    required = list(required_fields)
    for index, row in enumerate(rows):
        missing = [field for field in required if field not in row]
        if missing:
            raise StructuredEventContractError(
                f"Event row {index} missing required fields: {', '.join(missing)}"
            )
        if row.get("direction") != "UNKNOWN":
            raise StructuredEventContractError(
                f"Event row {index} assigns forbidden direction {row.get('direction')!r}"
            )
        if str(row.get("candidate_event_type") or "") in {
            "GUIDANCE_RAISE",
            "GUIDANCE_CUT",
            "DIVIDEND_INCREASE",
            "DIVIDEND_CUT",
            "BUYBACK_AUTHORIZATION",
            "BUYBACK_EXPANSION",
            "CAPITAL_RAISE",
            "EARNINGS_BEAT",
            "EARNINGS_MISS",
        }:
            raise StructuredEventContractError(
                "Content-dependent event semantics may not be emitted by filing-metadata extraction"
            )
