from __future__ import annotations

"""Prospective, research-only capture for frozen Elliott vNext Module 6.

This sidecar does not add Elliott rules.  It binds one already-published scanner
snapshot to the existing causal 6A->6D replay and the existing 6H output
assembler, then stores every genuinely available degree/timeframe output for
later 6G/QM-G research.
"""

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from .output import build_module_output, validate_module_output
from .validation import ValidationConfig, validation_partition
from .validation_replay import replay_symbol_states


SCHEMA_VERSION = "elliott_vnext_prospective_capture_v1"
CAPTURE_ENGINE_VERSION = "prospective_capture_engine_v2_iso_date_replay"
MODULE = "6H_prospective_shadow_capture"


class ProspectiveCaptureError(ValueError):
    """Raised when a prospective capture cannot be created without inference."""


def _iso_timestamp(value: object, field: str) -> str:
    text = str(value or "").strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProspectiveCaptureError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProspectiveCaptureError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc).isoformat()


def _canonical_hash(value: object) -> str:
    frozen = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return sha256(frozen.encode("utf-8")).hexdigest()


def _daily_identity(daily: Mapping[str, object]) -> tuple[str, str, list[str]]:
    snapshot_id = str(daily.get("snapshot_id") or "").strip()
    as_of = str(daily.get("as_of") or "").strip()
    symbols = daily.get("symbols")
    if not snapshot_id:
        raise ProspectiveCaptureError("snapshot_id_required")
    if not as_of:
        raise ProspectiveCaptureError("as_of_required")
    try:
        pd.Timestamp(as_of).date().isoformat()
    except (TypeError, ValueError) as exc:
        raise ProspectiveCaptureError("invalid_as_of") from exc
    if not isinstance(symbols, Mapping) or not symbols:
        raise ProspectiveCaptureError("daily_symbols_required")
    names = sorted(str(symbol).strip() for symbol in symbols if str(symbol).strip())
    if len(names) != len(symbols) or len(set(names)) != len(names):
        raise ProspectiveCaptureError("daily_symbols_invalid")
    universe_size = daily.get("universe_size")
    if universe_size is not None and int(universe_size) != len(names):
        raise ProspectiveCaptureError("daily_universe_size_mismatch")
    return snapshot_id, as_of, names


def build_prospective_capture(
    price_rows: pd.DataFrame | Iterable[Mapping[str, object]],
    daily_snapshot: Mapping[str, object],
    *,
    source_publication_commit: str,
    scanner_published_at: str,
    run_id: str,
    price_source_sha256: str,
    daily_source_sha256: str,
    captured_at: str | None = None,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, object]:
    """Build one current-snapshot capture using only the frozen Module-6 chain."""
    snapshot_id, as_of, symbols = _daily_identity(daily_snapshot)
    commit = str(source_publication_commit or "").strip()
    if len(commit) < 12:
        raise ProspectiveCaptureError("source_publication_commit_required")
    run = str(run_id or "").strip()
    if not run:
        raise ProspectiveCaptureError("run_id_required")
    scanner_available = _iso_timestamp(scanner_published_at, "scanner_published_at")
    captured = _iso_timestamp(
        captured_at or datetime.now(timezone.utc).isoformat(),
        "captured_at",
    )
    if datetime.fromisoformat(captured) < datetime.fromisoformat(scanner_available):
        raise ProspectiveCaptureError("capture_before_scanner_publication")

    frame = price_rows.copy() if isinstance(price_rows, pd.DataFrame) else pd.DataFrame(list(price_rows))
    if "symbol" not in frame.columns or "date" not in frame.columns:
        raise ProspectiveCaptureError("price_history_requires_symbol_and_date")

    outputs: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    output_keys: set[tuple[str, str, str]] = set()

    for symbol in symbols:
        snapshots, coverage = replay_symbol_states(
            frame,
            symbol,
            config=config,
            as_of_dates=[as_of],
            keep_unchanged=True,
        )
        coverage_rows.append(dict(coverage))
        for routed in snapshots:
            if str(routed.get("as_of") or "") != as_of:
                raise ProspectiveCaptureError(f"replay_as_of_mismatch:{symbol}")
            output = validate_module_output(build_module_output(routed))
            if str(output.get("symbol") or "") != symbol:
                raise ProspectiveCaptureError(f"output_symbol_mismatch:{symbol}")
            if str(output.get("as_of") or "") != as_of:
                raise ProspectiveCaptureError(f"output_as_of_mismatch:{symbol}")
            key = (
                symbol,
                str(output.get("timeframe") or ""),
                str(output.get("degree") or ""),
            )
            if not key[1] or not key[2]:
                raise ProspectiveCaptureError(f"output_identity_incomplete:{symbol}")
            if key in output_keys:
                raise ProspectiveCaptureError(
                    "duplicate_symbol_timeframe_degree:" + ":".join(key)
                )
            output_keys.add(key)
            outputs.append(dict(output))

    outputs.sort(
        key=lambda row: (
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("degree") or ""),
        )
    )
    symbols_with_outputs = len({str(row["symbol"]) for row in outputs})
    coverage = {
        "symbols_requested": len(symbols),
        "symbols_with_outputs": symbols_with_outputs,
        "symbols_without_outputs": len(symbols) - symbols_with_outputs,
        "output_count": len(outputs),
        "details": coverage_rows,
        "missing_evidence_not_imputed": True,
    }
    identity = {
        "schema_version": SCHEMA_VERSION,
        "capture_engine_version": CAPTURE_ENGINE_VERSION,
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "run_id": run,
        "source_publication_commit": commit,
        "scanner_published_at": scanner_available,
        "rules_frozen_through": config.rules_frozen_through,
        "validation_partition": validation_partition(as_of, config),
        "replay_price_basis": config.replay_price_basis,
        "price_source_sha256": str(price_source_sha256 or ""),
        "daily_source_sha256": str(daily_source_sha256 or ""),
        "output_ids": [str(row.get("output_id") or "") for row in outputs],
        "output_keys": [list(key) for key in sorted(output_keys)],
        "coverage": coverage,
    }
    capture_id = _canonical_hash(identity)
    return {
        "schema_version": SCHEMA_VERSION,
        "capture_engine_version": CAPTURE_ENGINE_VERSION,
        "module": MODULE,
        "capture_id": capture_id,
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "run_id": run,
        "source_publication_commit": commit,
        "scanner_published_at": scanner_available,
        "captured_at": captured,
        "rules_frozen_through": config.rules_frozen_through,
        "validation_partition": validation_partition(as_of, config),
        "replay_price_basis": config.replay_price_basis,
        "universe_size": len(symbols),
        "symbols_with_outputs": symbols_with_outputs,
        "output_count": len(outputs),
        "outputs": outputs,
        "coverage": coverage,
        "source_hashes": {
            "market_ohlcv_sha256": str(price_source_sha256 or ""),
            "daily_research_sha256": str(daily_source_sha256 or ""),
        },
        "guards": {
            "research_only": True,
            "productive_integration_enabled": False,
            "w10_source_emitted": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "future_rows_used": False,
            "missing_evidence_not_imputed": True,
            "frozen_elliott_core_modified": False,
            "multi_degree_outputs_retained_without_reducer": True,
        },
    }


def _load_archive(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProspectiveCaptureError(f"invalid_archive_json_line:{line_number}") from exc
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
            raise ProspectiveCaptureError(f"invalid_archive_record:{line_number}")
        rows.append(value)
    return rows


def archive_capture(
    path: Path,
    capture: Mapping[str, object],
) -> tuple[bool, dict[str, object]]:
    """Append once; allow one explicit repair of a pre-v2 empty capture.

    The first live Stage-1 run exposed an operational date-transport bug that
    could archive a structurally valid but completely empty capture.  Those
    records remain in history for auditability, but a capture produced by the
    repaired engine may supersede such a legacy all-zero record for the same
    scanner snapshot/run.  Any other identity conflict still fails closed.
    """
    if capture.get("schema_version") != SCHEMA_VERSION:
        raise ProspectiveCaptureError("unsupported_capture_schema")
    capture_id = str(capture.get("capture_id") or "")
    snapshot_id = str(capture.get("snapshot_id") or "")
    run_id = str(capture.get("run_id") or "")
    engine = str(capture.get("capture_engine_version") or "")
    if not capture_id or not snapshot_id or not run_id:
        raise ProspectiveCaptureError("capture_identity_incomplete")
    if engine != CAPTURE_ENGINE_VERSION:
        raise ProspectiveCaptureError("unsupported_capture_engine_version")

    rows = _load_archive(path)
    superseded: list[str] = []
    for existing in rows:
        same_snapshot = str(existing.get("snapshot_id") or "") == snapshot_id
        same_run = str(existing.get("run_id") or "") == run_id
        if not (same_snapshot or same_run):
            continue
        if str(existing.get("capture_id") or "") == capture_id:
            return False, existing

        existing_engine = str(existing.get("capture_engine_version") or "")
        existing_output_count = int(existing.get("output_count") or 0)
        existing_symbols = int(existing.get("symbols_with_outputs") or 0)
        repairable_legacy_empty = (
            existing_engine != CAPTURE_ENGINE_VERSION
            and existing_output_count == 0
            and existing_symbols == 0
        )
        if repairable_legacy_empty:
            old_id = str(existing.get("capture_id") or "")
            if old_id:
                superseded.append(old_id)
            continue
        raise ProspectiveCaptureError("prospective_capture_identity_conflict")

    stored = dict(capture)
    if superseded:
        stored["repair"] = {
            "reason": "supersede_pre_v2_empty_capture_after_iso_date_replay_fix",
            "supersedes_capture_ids": sorted(set(superseded)),
            "legacy_records_preserved": True,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(stored, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            + "\n"
        )
    return True, stored

