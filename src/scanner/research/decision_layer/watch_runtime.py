"""Compact public transport for the private Depot Watch.

The full 7A archive is the audit/source-of-truth artifact.  It is deliberately
not required at Watch-consumption time: W10 publishes a compact, sharded runtime
projection containing only the latest revision per symbol/snapshot, with payload
fields reduced to the exact semantics consumed by 7D->7H.

This module adds no investment logic.  Every compact packet is revalidated and
must produce the exact same 7D Universal Stance as its full source packet.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping, Sequence

from .input_contract import build_input_packet, validate_input_packet
from .integrated_evidence import PATH_CONTEXT_TYPE
from .phase5_shadow import PHASE5_CONTEXT_TYPE
from .universal_stance import compute_universal_stance
from .w10_orchestration import validate_sealed_manifest


MANIFEST_SCHEMA_VERSION = "decision_watch_runtime_manifest_v1"
SHARD_SCHEMA_VERSION = "decision_watch_runtime_shard_v1"
DEFAULT_RUNTIME_DIR = "artifacts/research/watch_runtime"
DEFAULT_RUNTIME_MANIFEST = f"{DEFAULT_RUNTIME_DIR}/manifest.json"
SHARD_IDS = tuple("0123456789abcdef")


class WatchRuntimeError(ValueError):
    """Raised when the compact transport cannot preserve Decision semantics."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise WatchRuntimeError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise WatchRuntimeError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonical_sha256(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(raw.encode("utf-8")).hexdigest()


def _simple_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Keep exactly the scalar facts 7G can expose plus required typed fields."""
    out: dict[str, object] = {}
    for raw_key, value in payload.items():
        key = str(raw_key)
        if isinstance(value, (bool, int, float)) or value is None:
            out[key] = deepcopy(value)
        elif isinstance(value, str) and len(value) <= 160:
            out[key] = value
    return out


def _compact_payload(family: str, payload: Mapping[str, object]) -> dict[str, object]:
    # W7/W8 consume the complete scanner path state.
    if family == "risk" and payload.get("context_type") == PATH_CONTEXT_TYPE:
        return deepcopy(dict(payload))
    # W5 is an explicit typed governance sidecar and must remain intact.
    if family == "confidence" and payload.get("context_type") == PHASE5_CONTEXT_TYPE:
        return deepcopy(dict(payload))
    # Elliott's validator requires its complete integration contract.
    if family == "elliott":
        return deepcopy(dict(payload))
    return _simple_payload(payload)


def compact_packet(packet: Mapping[str, object]) -> dict[str, object]:
    """Project a full 7A packet to the smallest downstream-equivalent packet."""
    full = validate_input_packet(packet)
    compact_rows: list[dict[str, object]] = []
    for raw in full["evidence"]:
        assert isinstance(raw, Mapping)
        family = str(raw["family"])
        payload = raw.get("payload")
        assert isinstance(payload, Mapping)
        row = {
            "family": family,
            "claim_id": str(raw["claim_id"]),
            "as_of": str(raw["as_of"]),
            "available_from": str(raw["available_from"]),
            "source_version": str(raw["source_version"]),
            "coverage_state": raw["coverage_state"],
            "maturity_state": raw["maturity_state"],
            "pit_state": raw["pit_state"],
            "integration_mode": raw["integration_mode"],
            "payload": _compact_payload(family, payload),
        }
        if "claim_ref" in raw:
            row["claim_ref"] = raw.get("claim_ref")
        compact_rows.append(row)

    compact = build_input_packet(
        symbol=str(full["symbol"]),
        as_of=str(full["as_of"]),
        source_snapshot_id=str(full["source_snapshot_id"]),
        evidence=compact_rows,
    )
    # Transport is only admissible if 7D is byte-for-byte semantically equal.
    if compute_universal_stance(full) != compute_universal_stance(compact):
        raise WatchRuntimeError(f"compact_stance_mismatch:{full['symbol']}:{full['source_snapshot_id']}")
    return compact


def _latest_revision_packets(
    archive_packets: Sequence[Mapping[str, object]],
    *,
    current_packets: Sequence[Mapping[str, object]],
    snapshot_id: str,
    decision_as_of: str,
) -> dict[str, list[dict[str, object]]]:
    decision_time = _utc(decision_as_of, "decision_as_of")
    current_by_symbol: dict[str, dict[str, object]] = {}
    for raw in current_packets:
        packet = validate_input_packet(raw)
        if str(packet["source_snapshot_id"]) != snapshot_id:
            raise WatchRuntimeError(f"current_packet_snapshot_mismatch:{packet['symbol']}")
        if _utc(packet["as_of"], "current_packet_as_of") != decision_time:
            raise WatchRuntimeError(f"current_packet_time_mismatch:{packet['symbol']}")
        symbol = str(packet["symbol"])
        if symbol in current_by_symbol:
            raise WatchRuntimeError(f"duplicate_current_packet:{symbol}")
        current_by_symbol[symbol] = packet

    wanted = set(current_by_symbol)
    by_symbol_snapshot: dict[str, dict[str, dict[str, object]]] = {symbol: {} for symbol in wanted}
    for raw in archive_packets:
        packet = validate_input_packet(raw)
        symbol = str(packet["symbol"])
        if symbol not in wanted:
            continue
        packet_time = _utc(packet["as_of"], "archive_packet_as_of")
        if packet_time > decision_time:
            continue
        source_snapshot_id = str(packet["source_snapshot_id"])
        previous = by_symbol_snapshot[symbol].get(source_snapshot_id)
        if previous is None:
            by_symbol_snapshot[symbol][source_snapshot_id] = packet
            continue
        previous_time = _utc(previous["as_of"], "archive_packet_as_of")
        if packet_time == previous_time:
            raise WatchRuntimeError(
                f"duplicate_symbol_snapshot_revision:{symbol}:{source_snapshot_id}"
            )
        if packet_time > previous_time:
            by_symbol_snapshot[symbol][source_snapshot_id] = packet

    # The authoritative final current packet wins for the W10 snapshot.
    for symbol, packet in current_by_symbol.items():
        by_symbol_snapshot[symbol][snapshot_id] = packet

    result: dict[str, list[dict[str, object]]] = {}
    for symbol in sorted(wanted):
        packets = sorted(
            by_symbol_snapshot[symbol].values(),
            key=lambda item: _utc(item["as_of"], "archive_packet_as_of"),
        )
        if not packets:
            raise WatchRuntimeError(f"runtime_history_missing:{symbol}")
        latest = packets[-1]
        if str(latest["source_snapshot_id"]) != snapshot_id:
            raise WatchRuntimeError(f"runtime_current_packet_not_latest:{symbol}")
        result[symbol] = [compact_packet(packet) for packet in packets]
    return result


def _shard_for_symbol(symbol: str) -> str:
    return sha256(symbol.encode("utf-8")).hexdigest()[0]


def build_watch_runtime(
    *,
    current_packet_set: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
    w10_manifest: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    """Build manifest + 16 compact shards from sealed public W10 evidence."""
    sealed = validate_sealed_manifest(w10_manifest)
    snapshot_id = str(sealed["snapshot_id"])
    if str(current_packet_set.get("snapshot_id") or "") != snapshot_id:
        raise WatchRuntimeError("runtime_packet_set_snapshot_mismatch")
    raw_current = current_packet_set.get("packets")
    if not isinstance(raw_current, list) or not raw_current:
        raise WatchRuntimeError("runtime_current_packets_required")
    decision_as_of = str(current_packet_set.get("as_of") or "")
    _utc(decision_as_of, "runtime_decision_as_of")

    histories = _latest_revision_packets(
        archive_packets,
        current_packets=raw_current,
        snapshot_id=snapshot_id,
        decision_as_of=decision_as_of,
    )

    shard_packets: dict[str, list[dict[str, object]]] = {key: [] for key in SHARD_IDS}
    symbol_shards: dict[str, str] = {}
    for symbol, packets in histories.items():
        shard_id = _shard_for_symbol(symbol)
        shard_packets[shard_id].extend(packets)
        symbol_shards[symbol] = f"shard_{shard_id}.json"

    shards: dict[str, dict[str, object]] = {}
    for shard_id in SHARD_IDS:
        packets = sorted(
            shard_packets[shard_id],
            key=lambda item: (str(item["symbol"]), _utc(item["as_of"], "packet_as_of")),
        )
        symbols = sorted({str(packet["symbol"]) for packet in packets})
        shards[shard_id] = {
            "schema_version": SHARD_SCHEMA_VERSION,
            "snapshot_id": snapshot_id,
            "decision_as_of": decision_as_of,
            "shard_id": shard_id,
            "symbols": symbols,
            "packet_count": len(packets),
            "packets": packets,
            "private_position_data_included": False,
        }

    stages = sealed.get("stages")
    assert isinstance(stages, Mapping)
    final_7a = stages.get("final_7a")
    archive = stages.get("phase7a_archive")
    if not isinstance(final_7a, Mapping) or not isinstance(archive, Mapping):
        raise WatchRuntimeError("w10_7a_provenance_missing")

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "decision_as_of": decision_as_of,
        "w10_status": "sealed",
        "w10_sealed_at": str(sealed["sealed_at"]),
        "source_current_7a_sha256": str(final_7a.get("artifact_sha256") or ""),
        "source_archive_7a_sha256": str(archive.get("artifact_sha256") or ""),
        "symbol_count": len(histories),
        "packet_count": sum(len(value) for value in histories.values()),
        "shard_count": len(SHARD_IDS),
        "symbol_shards": dict(sorted(symbol_shards.items())),
        "private_position_data_included": False,
        "decision_logic_changed": False,
        "runtime_projection_sha256": _canonical_sha256({
            key: _canonical_sha256(shards[key]) for key in SHARD_IDS
        }),
    }
    return manifest, shards


def write_watch_runtime(
    output_dir: Path,
    *,
    manifest: Mapping[str, object],
    shards: Mapping[str, Mapping[str, object]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for old in output_dir.glob("shard_*.json"):
        old.unlink()
    for shard_id in SHARD_IDS:
        shard = shards.get(shard_id)
        if not isinstance(shard, Mapping):
            raise WatchRuntimeError(f"runtime_shard_missing:{shard_id}")
        (output_dir / f"shard_{shard_id}.json").write_text(
            json.dumps(shard, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def validate_runtime_manifest(
    manifest: Mapping[str, object],
    *,
    w10_manifest: Mapping[str, object] | None = None,
    expected_snapshot_id: str | None = None,
) -> dict[str, object]:
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise WatchRuntimeError("unsupported_runtime_manifest_schema")
    snapshot_id = str(manifest.get("snapshot_id") or "")
    if not snapshot_id:
        raise WatchRuntimeError("runtime_snapshot_id_required")
    if expected_snapshot_id is not None and snapshot_id != str(expected_snapshot_id):
        raise WatchRuntimeError("runtime_snapshot_mismatch")
    if manifest.get("private_position_data_included") is not False:
        raise WatchRuntimeError("runtime_private_data_guard_invalid")
    if manifest.get("decision_logic_changed") is not False:
        raise WatchRuntimeError("runtime_decision_logic_guard_invalid")
    symbol_shards = manifest.get("symbol_shards")
    if not isinstance(symbol_shards, Mapping) or not symbol_shards:
        raise WatchRuntimeError("runtime_symbol_shards_required")
    if int(manifest.get("symbol_count") or 0) != len(symbol_shards):
        raise WatchRuntimeError("runtime_symbol_count_mismatch")
    if w10_manifest is not None:
        sealed = validate_sealed_manifest(w10_manifest, expected_snapshot_id=snapshot_id)
        stages = sealed["stages"]
        assert isinstance(stages, Mapping)
        final_7a = stages["final_7a"]
        archive = stages["phase7a_archive"]
        assert isinstance(final_7a, Mapping) and isinstance(archive, Mapping)
        if str(manifest.get("source_current_7a_sha256") or "") != str(final_7a.get("artifact_sha256") or ""):
            raise WatchRuntimeError("runtime_current_7a_hash_mismatch")
        if str(manifest.get("source_archive_7a_sha256") or "") != str(archive.get("artifact_sha256") or ""):
            raise WatchRuntimeError("runtime_archive_7a_hash_mismatch")
    return deepcopy(dict(manifest))


def load_runtime_packets_for_symbols(
    runtime_dir: Path,
    symbols: Sequence[str],
    *,
    w10_manifest: Mapping[str, object] | None = None,
    expected_snapshot_id: str | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    manifest_path = runtime_dir / "manifest.json"
    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WatchRuntimeError("runtime_manifest_missing") from exc
    if not isinstance(raw_manifest, Mapping):
        raise WatchRuntimeError("runtime_manifest_must_be_object")
    manifest = validate_runtime_manifest(
        raw_manifest,
        w10_manifest=w10_manifest,
        expected_snapshot_id=expected_snapshot_id,
    )
    symbol_shards = manifest["symbol_shards"]
    assert isinstance(symbol_shards, Mapping)

    wanted = sorted({str(symbol) for symbol in symbols if str(symbol) in symbol_shards})
    needed_files = sorted({str(symbol_shards[symbol]) for symbol in wanted})
    packets: list[dict[str, object]] = []
    for filename in needed_files:
        value = json.loads((runtime_dir / filename).read_text(encoding="utf-8"))
        if not isinstance(value, Mapping) or value.get("schema_version") != SHARD_SCHEMA_VERSION:
            raise WatchRuntimeError(f"runtime_shard_invalid:{filename}")
        if str(value.get("snapshot_id") or "") != str(manifest["snapshot_id"]):
            raise WatchRuntimeError(f"runtime_shard_snapshot_mismatch:{filename}")
        raw_packets = value.get("packets")
        if not isinstance(raw_packets, list):
            raise WatchRuntimeError(f"runtime_shard_packets_missing:{filename}")
        for raw in raw_packets:
            packet = validate_input_packet(raw)
            if str(packet["symbol"]) in wanted:
                packets.append(packet)

    covered = {str(packet["symbol"]) for packet in packets}
    missing = sorted(set(wanted) - covered)
    if missing:
        raise WatchRuntimeError("runtime_symbol_packets_missing:" + ",".join(missing))
    metadata = {
        "status": "runtime_compact",
        "snapshot_id": manifest["snapshot_id"],
        "requested_symbol_count": len(set(map(str, symbols))),
        "mapped_symbol_count": len(wanted),
        "loaded_shard_count": len(needed_files),
        "loaded_packet_count": len(packets),
        "private_position_data_included": False,
    }
    return packets, metadata
