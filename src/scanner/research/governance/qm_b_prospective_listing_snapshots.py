"""QM-B prospective listing-evidence snapshots.

Research-only infrastructure for archiving future listing/source snapshots without
retrojecting them into historical scanner states. Project-known ``valid_from`` is
always the actual retrieval timestamp. Source effective dates and source file
creation markers are retained as evidence fields but can never move ``valid_from``
backwards.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "qm_b_prospective_listing_snapshot_v1"
LEDGER_SCHEMA_VERSION = "qm_b_listing_snapshot_ledger_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_prospective_listing_snapshots_v1.json"


class ProspectiveListingSnapshotError(ValueError):
    """Raised when prospective listing evidence is ambiguous or unsafe."""


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
    """Parse Nasdaq symbol-directory text without inferring absent semantics."""
    name = Path(filename).name.lower()
    if name not in {"nasdaqlisted.txt", "otherlisted.txt"}:
        raise ProspectiveListingSnapshotError(f"nasdaq_symbol_directory_file_unsupported:{filename}")
    try:
        text = raw_payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_payload.decode("latin-1")
    lines = text.splitlines()
    if not lines:
        raise ProspectiveListingSnapshotError("nasdaq_symbol_directory_empty")
    creation_raw = _file_creation_marker(lines)
    data_lines = [
        line
        for line in lines
        if not line.split("|", 1)[0].strip().lower().startswith("file creation time:")
    ]
    reader = csv.DictReader(io.StringIO("\n".join(data_lines)), delimiter="|")
    if not reader.fieldnames:
        raise ProspectiveListingSnapshotError("nasdaq_symbol_directory_header_missing")
    fields = {_clean(field) for field in reader.fieldnames if _clean(field)}

    records: list[dict[str, Any]] = []
    if name == "nasdaqlisted.txt":
        required = {"Symbol", "Security Name", "Market Category", "Test Issue", "Financial Status"}
        if not required.issubset(fields):
            raise ProspectiveListingSnapshotError("nasdaqlisted_schema_invalid")
        for raw in reader:
            row = _strip_row(raw)
            symbol = _clean(row.get("Symbol"))
            if not symbol:
                continue
            records.append({
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
            })
    else:
        required = {"ACT Symbol", "Security Name", "Exchange", "Test Issue"}
        if not required.issubset(fields):
            raise ProspectiveListingSnapshotError("otherlisted_schema_invalid")
        for raw in reader:
            row = _strip_row(raw)
            symbol = _clean(row.get("ACT Symbol"))
            if not symbol:
                continue
            records.append({
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
            })

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



def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_first_child_text(parent: ET.Element | None, child_name: str) -> str | None:
    if parent is None:
        return None
    for child in list(parent):
        if _xml_local_name(child.tag) == child_name:
            value = _clean(child.text)
            return value or None
    return None


def _xml_first_descendant(parent: ET.Element, name: str) -> ET.Element | None:
    for node in parent.iter():
        if _xml_local_name(node.tag) == name:
            return node
    return None


def _esma_firds_xml_payload(raw_payload: bytes, *, filename: str) -> tuple[bytes, str]:
    name = Path(filename).name
    lower = name.lower()
    if lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_payload)) as archive:
                members = [
                    member for member in archive.namelist()
                    if not member.endswith("/") and member.lower().endswith(".xml")
                ]
                if len(members) != 1:
                    raise ProspectiveListingSnapshotError(
                        f"esma_firds_zip_requires_single_xml:{len(members)}"
                    )
                return archive.read(members[0]), Path(members[0]).name
        except zipfile.BadZipFile as exc:
            raise ProspectiveListingSnapshotError("esma_firds_zip_invalid") from exc
    if lower.endswith(".xml"):
        return bytes(raw_payload), name
    raise ProspectiveListingSnapshotError(f"esma_firds_file_unsupported:{filename}")


def _esma_firds_record(ref_data: ET.Element, *, event_type: str, file_type: str) -> dict[str, Any]:
    general = _xml_first_descendant(ref_data, "FinInstrmGnlAttrbts")
    venue = _xml_first_descendant(ref_data, "TradgVnRltdAttrbts")
    isin = _xml_first_child_text(general, "Id")
    mic = _xml_first_child_text(venue, "Id")
    if not isin or not mic:
        raise ProspectiveListingSnapshotError("esma_firds_record_identity_missing")
    first_trade = _xml_first_child_text(venue, "FrstTradDt")
    termination = _xml_first_child_text(venue, "TermntnDt")
    request_admission = _xml_first_child_text(venue, "ReqForAdmssnDt")
    if event_type == "TermntdRcrd":
        listing_state = "TERMINATED_EVENT"
    elif event_type == "CancRcrd":
        listing_state = "CANCELLED_EVENT"
    elif event_type == "NewRcrd":
        listing_state = "NEW_RECORD_EVENT"
    elif event_type == "ModfdRcrd":
        listing_state = "MODIFIED_RECORD_EVENT"
    elif file_type == "FULINS":
        listing_state = "LISTED_IN_SOURCE_FULL_FILE"
    elif file_type == "INVINS":
        listing_state = "INVALID_OR_SUPERSEDED_RECORD"
    else:
        listing_state = "REFERENCE_RECORD"
    return {
        "instrument_id": isin,
        "source_isin": isin,
        "venue_namespace": "iso_10383_mic",
        "venue_code": mic,
        "listing_state": listing_state,
        "listing_start": first_trade,
        "listing_end": termination,
        "request_for_admission": request_admission,
        "source_event_type": event_type,
        "source_file_type": file_type,
        "market_tradability": "UNKNOWN",
        "project_investability": "UNKNOWN",
    }


def parse_esma_firds_reference_file(raw_payload: bytes, *, filename: str) -> dict[str, Any]:
    """Parse one official ESMA FIRDS reference file without inferring tradability.

    Both the distributed ZIP wrapper and an extracted XML member are accepted.
    Full files contain active reference records; Delta/Invalid files can carry
    additions, modifications, terminations, cancellations and superseded data.
    """
    xml_bytes, member_name = _esma_firds_xml_payload(raw_payload, filename=filename)
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ProspectiveListingSnapshotError("esma_firds_xml_invalid") from exc

    upper_name = member_name.upper()
    if upper_name.startswith("FULINS_"):
        file_type = "FULINS"
    elif upper_name.startswith("DLTINS_"):
        file_type = "DLTINS"
    elif upper_name.startswith("INVINS_"):
        file_type = "INVINS"
    else:
        raise ProspectiveListingSnapshotError(
            f"esma_firds_member_type_unsupported:{member_name}"
        )

    creation = None
    for node in root.iter():
        if _xml_local_name(node.tag) == "CreDt":
            creation = _clean(node.text) or None
            break

    records: list[dict[str, Any]] = []
    event_names = {"NewRcrd", "ModfdRcrd", "TermntdRcrd", "CancRcrd"}
    event_nodes = [node for node in root.iter() if _xml_local_name(node.tag) in event_names]
    if event_nodes:
        for event_node in event_nodes:
            event_type = _xml_local_name(event_node.tag)
            for ref_data in event_node.iter():
                if _xml_local_name(ref_data.tag) == "RefData":
                    records.append(
                        _esma_firds_record(
                            ref_data,
                            event_type=event_type,
                            file_type=file_type,
                        )
                    )
    else:
        for ref_data in root.iter():
            if _xml_local_name(ref_data.tag) == "RefData":
                records.append(
                    _esma_firds_record(
                        ref_data,
                        event_type="FULL_RECORD" if file_type == "FULINS" else "REFERENCE_RECORD",
                        file_type=file_type,
                    )
                )

    if not records:
        raise ProspectiveListingSnapshotError("esma_firds_no_reference_records")
    records.sort(
        key=lambda row: (
            str(row.get("instrument_id")),
            str(row.get("venue_code")),
            str(row.get("source_event_type")),
        )
    )
    return {
        "parser_id": "esma_firds_reference_v1",
        "parser_version": "1",
        "source_generated_at_raw": creation,
        "source_generated_timezone": None,
        "filename": Path(filename).name,
        "member_filename": member_name,
        "file_type": file_type,
        "record_count": len(records),
        "records": records,
        "inferences": {
            "publication_time_inferred_from_filename": False,
            "listing_dates_inferred": False,
            "tradability_inferred_from_listing": False,
            "project_investability_computed": False,
        },
    }



def _normalized_payload_from_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "parser_id": envelope.get("parser_id"),
        "parser_version": envelope.get("parser_version"),
        "source_generated_at_raw": envelope.get("source_generated_at_raw"),
        "source_generated_timezone": envelope.get("source_generated_timezone"),
        "records": envelope.get("records"),
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
    records = parser_result.get("records")
    if not isinstance(records, list):
        raise ProspectiveListingSnapshotError("parser_records_required")
    parser_id = _clean(parser_result.get("parser_id"))
    parser_version = _clean(parser_result.get("parser_version"))
    if not parser_id or not parser_version:
        raise ProspectiveListingSnapshotError("parser_identity_required")
    raw_hash = sha256_bytes(bytes(raw_payload))
    normalized_payload = {
        "parser_id": parser_id,
        "parser_version": parser_version,
        "source_generated_at_raw": parser_result.get("source_generated_at_raw"),
        "source_generated_timezone": parser_result.get("source_generated_timezone"),
        "records": records,
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
        "parser_id": parser_id,
        "parser_version": parser_version,
        "source_generated_at_raw": parser_result.get("source_generated_at_raw"),
        "source_generated_timezone": parser_result.get("source_generated_timezone"),
        "normalized_payload_sha256": normalized_hash,
        "record_count": len(records),
        "raw_archive_path": _clean(raw_archive_path),
        "normalized_archive_path": _clean(normalized_archive_path),
        "http_etag": _clean(http_etag) or None,
        "http_last_modified": _clean(http_last_modified) or None,
        "historical_retrojection_permitted": False,
        "effective_dates_can_move_valid_from_backward": False,
        "records": records,
    }
    required = load_snapshot_contract()["required_snapshot_fields"]
    missing = [field for field in required if envelope.get(field) is None or envelope.get(field) == ""]
    if missing:
        raise ProspectiveListingSnapshotError("snapshot_required_fields_missing:" + ",".join(missing))
    return envelope


def _event_hash(value: Mapping[str, Any]) -> str:
    return sha256_bytes(_canonical_json_bytes(value))


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
        if event.get("sequence") != len(snapshot_ids) + 1:
            raise ProspectiveListingSnapshotError(f"ledger_sequence_invalid:{number}")
        if event.get("previous_event_hash") != previous:
            raise ProspectiveListingSnapshotError(f"ledger_chain_invalid:{number}")
        supplied = _clean(event.get("event_hash"))
        body = dict(event)
        body.pop("event_hash", None)
        if supplied != _event_hash(body):
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


def _resolve_inside_root(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    root_resolved = root.resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise ProspectiveListingSnapshotError(f"archive_path_outside_root:{relative}")
    return candidate


def _check_write_compatible(path: Path, content: bytes) -> None:
    if path.exists() and path.read_bytes() != content:
        raise ProspectiveListingSnapshotError(f"archive_path_collision:{path}")


def _write_once(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    try:
        os.write(fd, content)
        os.fsync(fd)
    finally:
        os.close(fd)


def _acquire_ledger_lock(ledger: Path) -> tuple[Path, int]:
    ledger.parent.mkdir(parents=True, exist_ok=True)
    lock_path = ledger.with_suffix(ledger.suffix + ".lock")
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError as exc:
        raise ProspectiveListingSnapshotError(f"ledger_lock_exists:{lock_path}") from exc
    return lock_path, fd


def _release_ledger_lock(lock_path: Path, fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


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
    if _clean(envelope.get("valid_from")) != _clean(envelope.get("retrieved_at")):
        raise ProspectiveListingSnapshotError("valid_from_must_equal_retrieved_at")
    if sha256_bytes(bytes(raw_payload)) != envelope.get("raw_payload_sha256"):
        raise ProspectiveListingSnapshotError("raw_payload_hash_mismatch")

    normalized_payload = _normalized_payload_from_envelope(envelope)
    normalized_content = _canonical_json_bytes(normalized_payload)
    if sha256_bytes(normalized_content) != envelope.get("normalized_payload_sha256"):
        raise ProspectiveListingSnapshotError("normalized_payload_hash_mismatch")
    records = envelope.get("records")
    if not isinstance(records, list) or len(records) != envelope.get("record_count"):
        raise ProspectiveListingSnapshotError("record_count_mismatch")

    raw_target = _resolve_inside_root(root, _clean(envelope.get("raw_archive_path")))
    normalized_target = _resolve_inside_root(root, _clean(envelope.get("normalized_archive_path")))
    _check_write_compatible(raw_target, bytes(raw_payload))
    _check_write_compatible(normalized_target, normalized_content)

    ledger_target = Path(ledger_path)
    lock_path, lock_fd = _acquire_ledger_lock(ledger_target)
    try:
        state = verify_snapshot_ledger(ledger_target)
        if snapshot_id in state["snapshot_ids"]:
            raise ProspectiveListingSnapshotError(f"duplicate_snapshot_identity:{snapshot_id}")

        _write_once(raw_target, bytes(raw_payload))
        _write_once(normalized_target, normalized_content)
        if sha256_bytes(raw_target.read_bytes()) != envelope.get("raw_payload_sha256"):
            raise ProspectiveListingSnapshotError("raw_archive_hash_mismatch_after_write")
        if sha256_bytes(normalized_target.read_bytes()) != envelope.get("normalized_payload_sha256"):
            raise ProspectiveListingSnapshotError("normalized_archive_hash_mismatch_after_write")

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
        line = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8") + b"\n"
        fd = os.open(ledger_target, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)
        final = verify_snapshot_ledger(ledger_target)
    finally:
        _release_ledger_lock(lock_path, lock_fd)

    return {
        "snapshot_id": snapshot_id,
        "event_count": final["event_count"],
        "head_hash": final["head_hash"],
        "valid_from": envelope.get("valid_from"),
        "historical_retrojection_permitted": False,
    }
