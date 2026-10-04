"""Prospective Phase-7A evidence archive for the Phase-7B research package.

The historical Decision Research Dataset and the prospective typed evidence are
kept as separate layers.  A packet is never retrojected onto an older scanner
row merely because it is available today.  Later research may align packets to
scanner rows only through a sufficiently strong snapshot identity.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
from typing import Mapping, Sequence

from .input_contract import validate_input_packet


ARCHIVE_SCHEMA_VERSION = "decision_evidence_archive_7a_v1"
DEFAULT_PROSPECTIVE_START = "2026-09-26"


class EvidenceArchiveError(ValueError):
    """Raised when the prospective typed-evidence archive is invalid."""


def _utc(value: object) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise EvidenceArchiveError("packet_as_of_required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceArchiveError("invalid_packet_as_of") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def packet_identity(packet: Mapping[str, object]) -> tuple[str, str, str]:
    """Return the immutable archive identity for one validated 7A packet."""
    return (
        str(packet.get("source_snapshot_id") or ""),
        str(packet.get("symbol") or ""),
        _utc(packet.get("as_of")).isoformat(),
    )


def evidence_partition(packet: Mapping[str, object], prospective_start: str) -> str:
    start = datetime.fromisoformat(prospective_start).replace(tzinfo=timezone.utc)
    return "prospective_unspent" if _utc(packet.get("as_of")).date() >= start.date() else "legacy_replay_spent"


def validate_archive_packets(
    packets: Sequence[Mapping[str, object]],
    *,
    prospective_start: str = DEFAULT_PROSPECTIVE_START,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Validate, deduplicate and summarize typed 7A packets without fusing them."""
    normalized: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    family_counts: Counter[str] = Counter()
    admission_counts: Counter[str] = Counter()
    partition_counts: Counter[str] = Counter()

    for raw in packets:
        if not isinstance(raw, Mapping):
            raise EvidenceArchiveError("archive_packet_must_be_object")
        packet = validate_input_packet(raw)
        identity = packet_identity(packet)
        if identity in seen:
            raise EvidenceArchiveError("duplicate_archive_packet:" + "|".join(identity))
        seen.add(identity)
        partition = evidence_partition(packet, prospective_start)
        packet["archive_partition"] = partition
        normalized.append(packet)
        partition_counts[partition] += 1
        coverage = packet.get("coverage", {})
        if isinstance(coverage, Mapping):
            admission_counts[str(coverage.get("admission_state"))] += 1
        evidence = packet.get("evidence", [])
        if isinstance(evidence, list):
            for row in evidence:
                if isinstance(row, Mapping):
                    family_counts[str(row.get("family"))] += 1

    normalized.sort(key=lambda p: (_utc(p["as_of"]), str(p["symbol"]), str(p["source_snapshot_id"])))
    symbols = sorted({str(packet["symbol"]) for packet in normalized})
    snapshots = sorted({str(packet["source_snapshot_id"]) for packet in normalized})
    as_of_values = [_utc(packet["as_of"]) for packet in normalized]
    metadata: dict[str, object] = {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "research_only": True,
        "packet_count": len(normalized),
        "symbol_count": len(symbols),
        "snapshot_count": len(snapshots),
        "as_of_min": min(as_of_values).isoformat() if as_of_values else None,
        "as_of_max": max(as_of_values).isoformat() if as_of_values else None,
        "prospective_unspent_start": prospective_start,
        "partitions": dict(sorted(partition_counts.items())),
        "families": dict(sorted(family_counts.items())),
        "admission_states": dict(sorted(admission_counts.items())),
        "historical_retrojection_permitted": False,
        "packet_to_scanner_alignment_requires_snapshot_identity": True,
        "evidence_fusion_performed": False,
        "universal_stance_computed": False,
        "portfolio_action_computed": False,
    }
    return normalized, metadata


def _archive_text(path: Path) -> tuple[str, Path]:
    source = path
    if not source.exists() and source.suffix == ".gz":
        legacy = source.with_suffix("")
        if legacy.exists():
            source = legacy
    if not source.exists():
        raise FileNotFoundError(source)
    if source.suffix == ".gz":
        try:
            return gzip.decompress(source.read_bytes()).decode("utf-8"), source
        except (OSError, UnicodeDecodeError) as exc:
            raise EvidenceArchiveError("evidence_archive_gzip_invalid") from exc
    return source.read_text(encoding="utf-8"), source


def load_evidence_archive(
    path: str | Path,
    *,
    prospective_start: str = DEFAULT_PROSPECTIVE_START,
    missing_ok: bool = True,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Read JSONL archive. Missing archive is an explicit zero-coverage state."""
    path = Path(path)
    try:
        text, source_path = _archive_text(path)
    except FileNotFoundError:
        if not missing_ok:
            raise EvidenceArchiveError("evidence_archive_missing")
        return [], {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "research_only": True,
            "status": "archive_not_present_yet",
            "packet_count": 0,
            "symbol_count": 0,
            "snapshot_count": 0,
            "as_of_min": None,
            "as_of_max": None,
            "prospective_unspent_start": prospective_start,
            "partitions": {},
            "families": {},
            "admission_states": {},
            "historical_retrojection_permitted": False,
            "packet_to_scanner_alignment_requires_snapshot_identity": True,
            "evidence_fusion_performed": False,
            "universal_stance_computed": False,
            "portfolio_action_computed": False,
        }

    packets: list[Mapping[str, object]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EvidenceArchiveError(f"invalid_jsonl_line:{number}") from exc
        if not isinstance(value, Mapping):
            raise EvidenceArchiveError(f"archive_line_not_object:{number}")
        packets.append(value)
    normalized, metadata = validate_archive_packets(packets, prospective_start=prospective_start)
    metadata["status"] = "available" if normalized else "empty_archive"
    metadata["archive_source_path"] = str(source_path)
    metadata["archive_compressed"] = source_path.suffix == ".gz"
    metadata["legacy_fallback_used"] = source_path != path
    return normalized, metadata


def write_normalized_archive(
    path: str | Path,
    packets: Sequence[Mapping[str, object]],
    *,
    prospective_start: str = DEFAULT_PROSPECTIVE_START,
) -> dict[str, object]:
    """Rewrite archive deterministically after validation; no inference is added."""
    normalized, metadata = validate_archive_packets(packets, prospective_start=prospective_start)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(packet, sort_keys=True, separators=(",", ":"), ensure_ascii=True) for packet in normalized]
    raw = (("\n".join(lines) + "\n") if lines else "").encode("utf-8")
    if target.suffix == ".gz":
        target.write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
        metadata["archive_compressed"] = True
    else:
        target.write_bytes(raw)
        metadata["archive_compressed"] = False
    metadata["archive_source_path"] = str(target)
    metadata["legacy_fallback_used"] = False
    return metadata


def append_prospective_packet(
    path: str | Path,
    packet: Mapping[str, object],
    *,
    prospective_start: str = DEFAULT_PROSPECTIVE_START,
    allow_spent: bool = False,
) -> dict[str, object]:
    """Append one validated packet, failing closed on duplicate or spent evidence."""
    target = Path(path)
    existing, _ = load_evidence_archive(target, prospective_start=prospective_start, missing_ok=True)
    validated = validate_input_packet(packet)
    if evidence_partition(validated, prospective_start) != "prospective_unspent" and not allow_spent:
        raise EvidenceArchiveError("spent_packet_cannot_enter_prospective_archive")
    existing.append(validated)
    return write_normalized_archive(target, existing, prospective_start=prospective_start)
