"""QM-B prospective listing-evidence snapshots.

Research-only infrastructure for archiving future listing/source snapshots without
retrojecting them into historical scanner states.  The project-known ``valid_from``
is always the actual retrieval timestamp.  Source effective dates and source file
creation markers are retained as evidence fields but can never move ``valid_from``
backwards.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import threading
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "qm_b_prospective_listing_snapshot_v1"
LEDGER_SCHEMA_VERSION = "qm_b_listing_snapshot_ledger_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_prospective_listing_snapshots_v1.json"


class ProspectiveListingSnapshotError(ValueError):
    """Raised when prospective listing evidence is ambiguous or unsafe."""


_PROCESS_LOCK = threading.Lock()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProspectiveListingSnapshotError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProspectiveListingSnapshotError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveListingSnapshotError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_snapshot_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveListingSnapshotError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_prospective_listing_snapshots_v1":
        raise ProspectiveListingSnapshotError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProspectiveListingSnapshotError("contract_scope_invalid")
    rules = payload.get("time_rules")
    if not isinstance(rules, Mapping) or rules.get("valid_from_equals_retrieved_at") is not True:
        raise ProspectiveListingSnapshotError("valid_from_rule_missing")
    if rules.get("historical_retrojection_permitted") is not False:
        raise ProspectiveListingSnapshotError("retrojection_must_be_forbidden")
    promotion = payload.get("promotion")
    if not isinstance(promotion, Mapping) or promotion.get("productive_scanner_integration") is not False:
        raise ProspectiveListingSnapshotError("productive_integration_must_be_disabled")
    return payload


def _strip_row(row: Mapping[str, Any]) -> dict[str, str]:
    return {_clean(key): _clean(value) for key, value in row.items() if key is not None}


def _file_creation_marker(lines: Sequence[str]) -> str | None:
    for line in reversed(lines):
        first = line.split("|", 1)[0].strip()
        if first.lower().startswith("file creation time:"):
            return first.split(":", 1)[1].strip() or None
    return None


def parse_nasdaq_symbol_directory(raw_payload: bytes, *, filename: str) -> dict[str, Any]:
    """Parse current Nasdaq symbol-directory text without inferring missing semantics."""
    name = Path(filename).name.lower()
    if name not in {"nasdaqlisted.txt", "otherlisted.txt"}:
        raise ProspectiveListingSnapshotError(f"nasdaq_symbol_directory_file_unsupported:{filename}")
    try:
        text = raw_payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw_payload.decode("latin-1")
        except UnicodeDecodeError as exc:  # pragma: no cover - latin-1 is total
            raise ProspectiveListingSnapshotError("nasdaq_symbol_directory_decode_failed") from exc
    lines = text.splitlines()
    if not lines:
        raise ProspectiveListingSnapshotError("nasdaq_symbol_directory_empty")
    creation_raw = _file_creation_marker(lines)
    data_lines = [line for line in lines if not line.split("|", 1)[0].strip().lower().startswith("file creation time:")]
    reader = csv.DictReader(io.StringIO("\n".join(data_lines)), delimiter="|")
    if not reader.fieldnames:
        raise ProspectiveListingSnapshotError("nasdaq_symbol_directory_header_missing")
    fields = {_clean(field) for field in reader.fieldnames if _clean(field)}

    if name == "nasdaqlisted.txt":
        required = {"Symbol", "Security Name", "Market Category", "Test Issue", "Financial Status"}
        if not required.issubset(fields):
            raise ProspectiveListingSnapshotError("nasdaqlisted_schema_invalid")
        records: list[dict[str, Any]] = []
        for raw in reader:
            row = _strip_row(raw)
            symbol = _clean(row.get("Symbol"))
            if not symbol:
                continue
            records.append(
                {
                    "source_symbol": symbol,
                    "security_name": _clean(row.get("Security Name")),
                    "venue_namespace": "nasdaq_symbol_directory",
                    "venue_code": "NASDAQ",
                    "listing_state": "LISTED_IN_SNAPSHOT",
                    "market_category": _clean(row.get("Market Category")) or None,
                    "financial_status": _clean(row.get("Financial Status")) or None,
                    "test_issue": _clean(row.get("Test Issue")) or None,
                    "listing_start": None,
                    "listing_end": None,
                    "market_tradability": "UNKNOWN",
                    "project_investability": "UNKNOWN",
                }
            )
    else:
        required = {"ACT Symbol", "Security Name", "Exchange", "Test Issue"}
        if not required.issubset(fields):
            raise ProspectiveListingSnapshotError("otherlisted_schema_invalid")
        records = []
        for raw in reader:
            row = _strip_row(raw)
            symbol = _clean(row.get("ACT Symbol"))
            if not symbol:
                continue
            records.append(
                {
                    "source_symbol": symbol,
                    "security_name": _clean(row.get("Security Name")),
                    "venue_namespace": "nasdaq_symbol_directory_exchange_code",
                    "venue_code": _clean(row.get("Exchange")) or None,
                    "listing_state": "LISTED_IN_SNAPSHOT",
                    "cqs_symbol": _clean(row.get("CQS Symbol")) or None,
                    "nasdaq_symbol": _clean(row.get("NASDAQ Symbol")) or None,
                    "test_issue": _clean(row.get("Test Issue")) or None,
                    "listing_start": None,
                    "listing_end": None,
                    "market_tradability": "UNKNOWN",
                    "project_investability": "UNKNOWN",
                }
            )

    records.sort(key=lambda row: (str(row.get("source_symbol")), str(row.get("venue_code"))))
    return {
        "parser_id": "nasdaq_symbol_directory_v1",
        "parser_version": "1",
        "source_generated_at_raw": creation_raw,
        "source_generated_timezone": None,
        "filename": Path(filename).name,
        "record_count": len(records),
        "records": records,
        "inferences": {
            "file_creation_timezone_inferred": False,
            "listing_dates_inferred": False,
            "delisting_from_absence_inferred": False,
            "project_investability_computed": False,
        },
    }


def build_snapshot_envelope(
    *,
    source_id: str,
    source_url: str,
    retrieved_at: str,
    raw_payload: bytes,
    parser_result: Mapping[str, Any],
    raw_archive_path: str,
    normalized_archive_path: str,
    http_etag: str | None = None,
    http_last_modified: str | None = None,
) -> dict[str, Any]:
    source_id = _clean(source_id)
    source_url = _clean(source_url)
    if not source_id or not source_url:
        raise ProspectiveListingSnapshotError("source_identity_required")
    retrieved = _utc(retrieved_at, field="retrieved_at").isoformat()
    raw_hash = sha256_bytes(bytes(raw_payload))
    normalized_records = parser_result.get("records")
    if not isinstance(normalized_records, list):
        raise ProspectiveListingSnapshotError("parser_records_required")
    normalized_payload = {
        "parser_id": _clean(parser_result.get("parser_id")),
        "parser_version": _clean(parser_result.get("parser_version")),
        "source_generated_at_raw": parser_result.get("source_generated_at_raw"),
        "source_generated_timezone": parser_result.get("source_generated_timezone"),
        "records": normalized_records,
    }
    normalized_hash = sha256_bytes(_canonical_json_bytes(normalized_payload))
    identity = {"source_id": source_id, "retrieved_at": retrieved, "raw_payload_sha256": raw_hash}
    snapshot_id = "qmbls_" + sha256_bytes(_canonical_json_bytes(identity))[:24]
    envelope = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "snapshot_id": snapshot_id,
        "source_id": source_id,
        "source_url": source_url,
        "retrieved_at": retrieved,
        "valid_from": retrieved,
        "raw_payload_sha256": raw_hash,
        "raw_payload_bytes": len(raw_payload),
        "parser_id": normalized_payload["parser_id"],
        "parser_version": normalized_payload["parser_version"],
        "source_generated_at_raw": normalized_payload["source_generated_at_raw"],
        "source_generated_timezone": normalized_payload["source_generated_timezone"],
        "normalized_payload_sha256": normalized_hash,
        "record_count": len(normalized_records),
        "raw_archive_path": _clean(raw_archive_path),
        "normalized_archive_path": _clean(normalized_archive_path),
        "http_etag": _clean(http_etag) or None,
        "http_last_modified": _clean(http_last_modified) or None,
        "historical_retrojection_permitted": False,
        "effective_dates_can_move_valid_from_backward": False,
        "records": normalized_records,
    }
    required = load_snapshot_contract()["required_snapshot_fields"]
    missing = [field for field in required if envelope.get(field) in {None, ""}]
    if missing:
        raise ProspectiveListingSnapshotError("snapshot_required_fields_missing:" + ",".join(missing))
    return envelope


def _event_hash(event_without_hash: Mapping[str, Any]) -> str:
    return sha256_bytes(_canonical_json_bytes(event_without_hash))


def verify_snapshot_ledger(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {"schema_version": LEDGER_SCHEMA_VERSION, "valid": True, "event_count": 0, "head_hash": None, "snapshot_ids": []}
    previous: str | None = None
    snapshot_ids: list[str] = []
    for number, line in enumerate(target.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProspectiveListingSnapshotError(f"ledger_invalid_json:{number}") from exc
        if event.get("schema_version") != LEDGER_SCHEMA_VERSION:
            raise ProspectiveListingSnapshotError(f"ledger_schema_invalid:{number}")
        expected_sequence = len(snapshot_ids) + 1
        if event.get("sequence") != expected_sequence:
            raise ProspectiveListingSnapshotError(f"ledger_sequence_invalid:{number}")
        if event.get("previous_event_hash") != previous:
            raise ProspectiveListingSnapshotError(f"ledger_chain_invalid:{number}")
        supplied = _clean(event.get("event_hash"))
        base = dict(event)
        base.pop("event_hash", None)
        calculated = _event_hash(base)
        if supplied != calculated:
            raise ProspectiveListingSnapshotError(f"ledger_hash_invalid:{number}")
        snapshot_id = _clean(event.get("snapshot_id"))
        if not snapshot_id:
            raise ProspectiveListingSnapshotError(f"ledger_snapshot_id_missing:{number}")
        if snapshot_id in snapshot_ids:
            raise ProspectiveListingSnapshotError(f"ledger_duplicate_snapshot:{snapshot_id}")
        snapshot_ids.append(snapshot_id)
        previous = supplied
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "valid": True,
        "event_count": len(snapshot_ids),
        "head_hash": previous,
        "snapshot_ids": snapshot_ids,
    }


def _write_once(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if existing != content:
            raise ProspectiveListingSnapshotError(f"archive_path_collision:{path}")
        return
    path.write_bytes(content)


def archive_snapshot(
    *,
    ledger_path: str | Path,
    raw_payload: bytes,
    envelope: Mapping[str, Any],
    repository_root: str | Path,
) -> dict[str, Any]:
    """Persist raw+normalized snapshot once, then append hash-chained metadata."""
    root = Path(repository_root)
    snapshot_id = _clean(envelope.get("snapshot_id"))
    if not snapshot_id:
        raise ProspectiveListingSnapshotError("snapshot_id_required")
    if sha256_bytes(bytes(raw_payload)) != envelope.get("raw_payload_sha256"):
        raise ProspectiveListingSnapshotError("raw_payload_hash_mismatch")
    normalized_content = _canonical_json_bytes(
        {
            "snapshot_id": snapshot_id,
            "source_id": envelope.get("source_id"),
            "retrieved_at": envelope.get("retrieved_at"),
            "valid_from": envelope.get("valid_from"),
            "records": envelope.get("records"),
        }
    )
    raw_target = root / str(envelope.get("raw_archive_path"))
    normalized_target = root / str(envelope.get("normalized_archive_path"))
    ledger_target = Path(ledger_path)

    with _PROCESS_LOCK:
        state = verify_snapshot_ledger(ledger_target)
        if snapshot_id in state["snapshot_ids"]:
            raise ProspectiveListingSnapshotError(f"duplicate_snapshot_identity:{snapshot_id}")
        _write_once(raw_target, bytes(raw_payload))
        _write_once(normalized_target, normalized_content)
        if sha256_bytes(_canonical_json_bytes({
            "parser_id": envelope.get("parser_id"),
            "parser_version": envelope.get("parser_version"),
            "source_generated_at_raw": envelope.get("source_generated_at_raw"),
            "source_generated_timezone": envelope.get("source_generated_timezone"),
            "records": envelope.get("records"),
        })) != envelope.get("normalized_payload_sha256"):
            raise ProspectiveListingSnapshotError("normalized_payload_hash_mismatch")

        event = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "sequence": state["event_count"] + 1,
            "previous_event_hash": state["head_hash"],
            "event_type": "LISTING_SNAPSHOT_ARCHIVED",
            "snapshot_id": snapshot_id,
            "source_id": envelope.get("source_id"),
            "source_url": envelope.get("source_url"),
            "retrieved_at": envelope.get("retrieved_at"),
            "valid_from": envelope.get("valid_from"),
            "raw_payload_sha256": envelope.get("raw_payload_sha256"),
            "normalized_payload_sha256": envelope.get("normalized_payload_sha256"),
            "record_count": envelope.get("record_count"),
            "raw_archive_path": envelope.get("raw_archive_path"),
            "normalized_archive_path": envelope.get("normalized_archive_path"),
            "historical_retrojection_permitted": False,
        }
        event["event_hash"] = _event_hash(event)
        ledger_target.parent.mkdir(parents=True, exist_ok=True)
        with ledger_target.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n")
        final = verify_snapshot_ledger(ledger_target)
    return {
        "snapshot_id": snapshot_id,
        "event_count": final["event_count"],
        "head_hash": final["head_hash"],
        "valid_from": envelope.get("valid_from"),
        "historical_retrojection_permitted": False,
    }
