from __future__ import annotations

import html
import re
from hashlib import sha256
from typing import Any, Iterable, Mapping


class ContentParserError(ValueError):
    """Raised when SEC full-submission content cannot satisfy the 8C-G parser contract."""


_DOCUMENT_RE = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.IGNORECASE | re.DOTALL)
_TAG_LINE_RE = {
    "document_type": re.compile(r"<TYPE>\s*([^\r\n<]+)", re.IGNORECASE),
    "sequence": re.compile(r"<SEQUENCE>\s*([^\r\n<]+)", re.IGNORECASE),
    "filename": re.compile(r"<FILENAME>\s*([^\r\n<]+)", re.IGNORECASE),
    "description": re.compile(r"<DESCRIPTION>\s*([^\r\n<]+)", re.IGNORECASE),
}
_TEXT_RE = re.compile(r"<TEXT>(.*?)</TEXT>", re.IGNORECASE | re.DOTALL)
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def _field(block: str, name: str) -> str | None:
    match = _TAG_LINE_RE[name].search(block)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def visible_text(raw: str) -> str:
    """Convert SEC HTML/plain-text content to stable visible text without semantics."""
    value = _SCRIPT_STYLE_RE.sub(" ", str(raw or ""))
    value = _HTML_TAG_RE.sub(" ", value)
    value = html.unescape(value)
    return _WHITESPACE_RE.sub(" ", value).strip()


def split_sec_documents(full_submission: bytes | str) -> list[dict[str, Any]]:
    """Split SEC full-submission text into deterministic document blocks.

    A filing may contain its primary document plus many exhibits. The parser retains
    metadata for each block and does not choose a semantically preferred exhibit.
    """
    if isinstance(full_submission, bytes):
        source = full_submission.decode("utf-8", errors="replace")
    else:
        source = str(full_submission)

    blocks = list(_DOCUMENT_RE.finditer(source))
    if not blocks:
        raise ContentParserError("SEC full submission contains no <DOCUMENT> blocks")

    output: list[dict[str, Any]] = []
    for index, match in enumerate(blocks, start=1):
        block = match.group(1)
        text_match = _TEXT_RE.search(block)
        raw_text = text_match.group(1) if text_match else ""
        output.append(
            {
                "document_index": index,
                "document_type": _field(block, "document_type"),
                "sequence": _field(block, "sequence"),
                "filename": _field(block, "filename"),
                "description": _field(block, "description"),
                "visible_text": visible_text(raw_text),
            }
        )
    return output


def is_textual_candidate(
    document: Mapping[str, Any],
    *,
    excluded_type_prefixes: Iterable[str],
    minimum_visible_characters: int,
) -> bool:
    document_type = str(document.get("document_type") or "").strip().upper()
    excluded = tuple(str(value).strip().upper() for value in excluded_type_prefixes)
    if document_type and any(document_type.startswith(prefix) for prefix in excluded if prefix):
        return False
    text = str(document.get("visible_text") or "")
    return len(text) >= int(minimum_visible_characters)


def _excerpt(text: str, start: int, end: int, radius: int) -> tuple[str, int, int]:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    value = text[left:right].strip()
    return value, left, right


def extract_evidence_anchors(
    documents: Iterable[Mapping[str, Any]],
    *,
    anchor_families: Mapping[str, Mapping[str, Any]],
    parser_version: str,
    excluded_type_prefixes: Iterable[str] = (),
    minimum_visible_characters: int = 20,
    excerpt_radius: int = 240,
) -> list[dict[str, Any]]:
    """Find literal evidence anchors without assigning issuer or market semantics."""
    if excerpt_radius < 0:
        raise ContentParserError("excerpt_radius must be >= 0")

    anchors: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for document in documents:
        if not is_textual_candidate(
            document,
            excluded_type_prefixes=excluded_type_prefixes,
            minimum_visible_characters=minimum_visible_characters,
        ):
            continue
        text = str(document.get("visible_text") or "")
        lower = text.casefold()
        for family, family_spec in anchor_families.items():
            phrases = family_spec.get("phrases") or []
            for raw_phrase in phrases:
                phrase = str(raw_phrase or "").strip()
                if not phrase:
                    continue
                needle = phrase.casefold()
                offset = 0
                while True:
                    position = lower.find(needle, offset)
                    if position < 0:
                        break
                    end = position + len(needle)
                    excerpt, excerpt_start, excerpt_end = _excerpt(
                        text,
                        position,
                        end,
                        excerpt_radius,
                    )
                    digest = sha256(excerpt.encode("utf-8")).hexdigest()
                    key = (
                        str(family),
                        phrase.casefold(),
                        document.get("document_index"),
                        position,
                        digest,
                    )
                    if key not in seen:
                        seen.add(key)
                        anchors.append(
                            {
                                "family": str(family),
                                "matched_phrase": phrase,
                                "document_type": document.get("document_type"),
                                "sequence": document.get("sequence"),
                                "filename": document.get("filename"),
                                "description": document.get("description"),
                                "excerpt": excerpt,
                                "excerpt_sha256": digest,
                                "character_start": position,
                                "character_end": end,
                                "excerpt_start": excerpt_start,
                                "excerpt_end": excerpt_end,
                                "semantic_status": "ANCHOR_ONLY",
                                "market_direction": "UNKNOWN",
                                "parser_version": parser_version,
                            }
                        )
                    offset = max(end, position + 1)
    anchors.sort(
        key=lambda row: (
            str(row.get("family") or ""),
            str(row.get("filename") or ""),
            int(row.get("character_start") or 0),
            str(row.get("matched_phrase") or ""),
        )
    )
    return anchors


def validate_anchor_contract(
    rows: Iterable[Mapping[str, Any]],
    *,
    required_fields: Iterable[str],
) -> None:
    required = list(required_fields)
    for index, row in enumerate(rows):
        missing = [field for field in required if field not in row]
        if missing:
            raise ContentParserError(
                f"Anchor row {index} missing required fields: {', '.join(missing)}"
            )
        if row.get("semantic_status") != "ANCHOR_ONLY":
            raise ContentParserError("8C-G may emit only ANCHOR_ONLY semantic status")
        if row.get("market_direction") != "UNKNOWN":
            raise ContentParserError("8C-G may not assign market direction")
        digest = str(row.get("excerpt_sha256") or "")
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest.lower()):
            raise ContentParserError("Invalid anchor excerpt SHA-256")


def anchor_coverage(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    records = list(rows)
    family_counts: dict[str, int] = {}
    document_files: set[str] = set()
    for row in records:
        family = str(row.get("family") or "UNKNOWN")
        family_counts[family] = family_counts.get(family, 0) + 1
        if row.get("filename"):
            document_files.add(str(row["filename"]))
    return {
        "anchor_count": len(records),
        "family_counts": dict(sorted(family_counts.items())),
        "document_count_with_anchors": len(document_files),
        "all_semantic_status_anchor_only": all(
            row.get("semantic_status") == "ANCHOR_ONLY" for row in records
        ),
        "all_market_directions_unknown": all(
            row.get("market_direction") == "UNKNOWN" for row in records
        ),
        "outcome_research": "NOT_RUN",
    }
