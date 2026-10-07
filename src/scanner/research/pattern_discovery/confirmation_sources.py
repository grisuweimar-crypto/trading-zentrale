"""Auditable prospective source proofs for Pattern Discovery L9.

L9 needs more than copied labels. This module replays the exact L7 binding
semantics against caller-supplied prospective snapshot projections and market
session maps. A source bundle is accepted only when:

- every L7 capture report is hash valid;
- every projected scanner row reproduces the L7 current-row hashes and the
  capture report's projected_rows_hash;
- every supplied market-session record reproduces the capture report's
  session_map_hash;
- claim IDs/hashes are taken from those verified capture reports.

The resulting bundle lets L9 prove both baseline-universe membership and
capture-time regime/sector/segment context without retrofitting modern values.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from .feature_library import FeatureLibrary
from .prospective_capture import verify_capture_report


SOURCE_BUNDLE_SCHEMA_VERSION = "pattern_discovery_l9_prospective_source_bundle_v1"
DEFAULT_FEATURE_LIBRARY_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "feature_library_v1.json"
)


class ConfirmationSourceError(ValueError):
    """Raised when prospective L7 source evidence cannot be proven."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ConfirmationSourceError(f"value_required:{field}")
    return result


def _safe_token(value: Any, field: str) -> str:
    result = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", result):
        raise ConfirmationSourceError(f"safe_token_required:{field}")
    return result


def _sha256_text(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", result):
        raise ConfirmationSourceError(f"sha256_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfirmationSourceError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise ConfirmationSourceError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _feature_library(
    path: str | Path | None = None,
) -> FeatureLibrary:
    return FeatureLibrary(path or DEFAULT_FEATURE_LIBRARY_PATH)


def _projection_fields(feature_library: FeatureLibrary) -> tuple[str, ...]:
    fields = {
        feature["source_field"]
        for feature in feature_library.features.values()
    }
    fields.update(
        {
            "symbol",
            "as_of",
            "generated_at",
            "snapshot_id",
            "observation_type",
            "data_source",
        }
    )
    return tuple(sorted(fields))


def _normalize_projection_row(
    raw: Mapping[str, Any],
    *,
    snapshot_id: str,
    snapshot_generated_at: str,
    fields: Sequence[str],
    index: int,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ConfirmationSourceError(
            f"snapshot_projection_row_must_be_object:{index}"
        )
    symbol = _text(raw.get("symbol"), f"snapshot_rows[{index}].symbol")
    current_snapshot = _safe_token(
        raw.get("snapshot_id"),
        f"snapshot_rows[{index}].snapshot_id",
    )
    if current_snapshot != snapshot_id:
        raise ConfirmationSourceError(
            f"snapshot_projection_snapshot_id_mismatch:{symbol}"
        )
    generated_at = _timestamp(
        raw.get("generated_at"),
        f"snapshot_rows[{index}].generated_at",
    )
    if generated_at != snapshot_generated_at:
        raise ConfirmationSourceError(
            f"snapshot_projection_generated_at_mismatch:{symbol}"
        )
    as_of = _timestamp(
        raw.get("as_of"),
        f"snapshot_rows[{index}].as_of",
    )
    if datetime.fromisoformat(as_of.replace("Z", "+00:00")) > datetime.fromisoformat(
        snapshot_generated_at.replace("Z", "+00:00")
    ):
        raise ConfirmationSourceError(
            f"snapshot_projection_as_of_after_generation:{symbol}"
        )
    row = {
        field: raw.get(field)
        for field in fields
        if field in raw
    }
    row["symbol"] = symbol
    row["snapshot_id"] = current_snapshot
    row["generated_at"] = generated_at
    row["as_of"] = as_of
    return row


def _normalize_snapshot_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    capture_report: Mapping[str, Any],
    feature_library: FeatureLibrary,
) -> tuple[list[dict[str, Any]], str]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise ConfirmationSourceError("snapshot_rows_sequence_required")
    binding = capture_report["snapshot_binding"]
    snapshot_id = _safe_token(binding["snapshot_id"], "snapshot_id")
    generated_at = _timestamp(
        binding["snapshot_generated_at"],
        "snapshot_generated_at",
    )
    fields = _projection_fields(feature_library)
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        row = _normalize_projection_row(
            raw,
            snapshot_id=snapshot_id,
            snapshot_generated_at=generated_at,
            fields=fields,
            index=index,
        )
        symbol = str(row["symbol"])
        if symbol in seen:
            raise ConfirmationSourceError(
                f"duplicate_snapshot_projection_symbol:{symbol}"
            )
        seen.add(symbol)
        row_hash = _hash(row)
        normalized.append(
            {
                "symbol": symbol,
                "as_of": row["as_of"],
                "snapshot_id": row["snapshot_id"],
                "row_hash": row_hash,
                "row_projection": row,
            }
        )
    normalized.sort(key=lambda item: item["symbol"])
    projected_rows_hash = _hash(
        [
            {
                "symbol": item["symbol"],
                "as_of": item["as_of"],
                "snapshot_id": item["snapshot_id"],
                "row_hash": item["row_hash"],
            }
            for item in normalized
        ]
    )
    expected = _sha256_text(
        binding["projected_rows_hash"],
        "snapshot_binding.projected_rows_hash",
    )
    if projected_rows_hash != expected:
        raise ConfirmationSourceError(
            f"snapshot_projection_hash_mismatch:{snapshot_id}"
        )
    if len(normalized) != int(binding["row_count"]):
        raise ConfirmationSourceError(
            f"snapshot_projection_row_count_mismatch:{snapshot_id}"
        )
    if len(seen) != int(binding["symbol_count"]):
        raise ConfirmationSourceError(
            f"snapshot_projection_symbol_count_mismatch:{snapshot_id}"
        )
    return normalized, projected_rows_hash


def _normalize_sessions(
    rows: Sequence[Mapping[str, Any]],
    *,
    capture_report: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise ConfirmationSourceError("market_sessions_sequence_required")
    capture_at = datetime.fromisoformat(
        _timestamp(
            capture_report["captured_at"],
            "capture_report.captured_at",
        ).replace("Z", "+00:00")
    )
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise ConfirmationSourceError(
                f"market_session_must_be_object:{index}"
            )
        symbol = _text(raw.get("symbol"), f"market_sessions[{index}].symbol")
        if symbol in seen:
            raise ConfirmationSourceError(
                f"duplicate_market_session_symbol:{symbol}"
            )
        seen.add(symbol)
        start_at = _timestamp(
            raw.get("start_at"),
            f"market_sessions[{index}].start_at",
        )
        if datetime.fromisoformat(start_at.replace("Z", "+00:00")) <= capture_at:
            raise ConfirmationSourceError(
                f"market_session_not_after_capture:{symbol}"
            )
        normalized.append(
            {
                "symbol": symbol,
                "session_id": _safe_token(
                    raw.get("session_id"),
                    f"market_sessions[{index}].session_id",
                ),
                "calendar_id": _safe_token(
                    raw.get("calendar_id"),
                    f"market_sessions[{index}].calendar_id",
                ),
                "start_at": start_at,
                "source": _text(
                    raw.get("source"),
                    f"market_sessions[{index}].source",
                ),
            }
        )
    normalized.sort(key=lambda item: item["symbol"])
    session_map_hash = _hash(normalized)
    expected = _sha256_text(
        capture_report["session_map_hash"],
        "capture_report.session_map_hash",
    )
    if session_map_hash != expected:
        raise ConfirmationSourceError(
            f"market_session_map_hash_mismatch:{capture_report['capture_id']}"
        )
    return normalized, session_map_hash


def build_prospective_source_bundle(
    capture_sources: Sequence[Mapping[str, Any]],
    *,
    source_bundle_id: str,
    generated_at: str,
    feature_library_path: str | Path | None = None,
) -> dict[str, Any]:
    if isinstance(capture_sources, (str, bytes, bytearray)) or not isinstance(
        capture_sources, Sequence
    ):
        raise ConfirmationSourceError("capture_sources_sequence_required")
    if not capture_sources:
        raise ConfirmationSourceError("capture_sources_nonempty_required")
    library = _feature_library(feature_library_path)
    snapshots: list[dict[str, Any]] = []
    claims: list[dict[str, Any]] = []
    seen_snapshots: set[str] = set()
    seen_claims: set[str] = set()

    for index, source in enumerate(capture_sources):
        if not isinstance(source, Mapping):
            raise ConfirmationSourceError(
                f"capture_source_must_be_object:{index}"
            )
        report = source.get("capture_report")
        if not isinstance(report, Mapping):
            raise ConfirmationSourceError(
                f"capture_report_missing:{index}"
            )
        verify_capture_report(report)
        snapshot_id = _safe_token(
            report["snapshot_binding"]["snapshot_id"],
            f"capture_sources[{index}].snapshot_id",
        )
        if snapshot_id in seen_snapshots:
            raise ConfirmationSourceError(
                f"duplicate_capture_snapshot:{snapshot_id}"
            )
        seen_snapshots.add(snapshot_id)
        projected_rows, projected_rows_hash = _normalize_snapshot_rows(
            source.get("snapshot_rows") or [],
            capture_report=report,
            feature_library=library,
        )
        sessions, session_map_hash = _normalize_sessions(
            source.get("market_sessions") or [],
            capture_report=report,
        )
        snapshot = {
            "capture_id": report["capture_id"],
            "capture_hash": report["capture_hash"],
            "captured_at": report["captured_at"],
            "snapshot_id": snapshot_id,
            "snapshot_binding_hash": report["snapshot_binding"][
                "snapshot_binding_hash"
            ],
            "snapshot_file_sha256": report["snapshot_binding"][
                "snapshot_file_sha256"
            ],
            "snapshot_generated_at": report["snapshot_binding"][
                "snapshot_generated_at"
            ],
            "projected_rows_hash": projected_rows_hash,
            "session_map_hash": session_map_hash,
            "rows": projected_rows,
            "market_sessions": sessions,
        }
        snapshots.append(snapshot)

        row_by_symbol = {
            item["symbol"]: item
            for item in projected_rows
        }
        for raw_claim in report["claims"]:
            claim_id = _safe_token(raw_claim["claim_id"], "claim_id")
            if claim_id in seen_claims:
                raise ConfirmationSourceError(
                    f"duplicate_claim_across_capture_sources:{claim_id}"
                )
            seen_claims.add(claim_id)
            symbol = _text(raw_claim["match"]["symbol"], "claim.match.symbol")
            row = row_by_symbol.get(symbol)
            if row is None:
                raise ConfirmationSourceError(
                    f"claim_symbol_missing_from_snapshot_projection:{claim_id}"
                )
            current_row_hash = _sha256_text(
                raw_claim["match"]["current_row_hash"],
                f"claim.current_row_hash:{claim_id}",
            )
            if row["row_hash"] != current_row_hash:
                raise ConfirmationSourceError(
                    f"claim_current_row_hash_mismatch:{claim_id}"
                )
            claims.append(
                {
                    "claim_id": claim_id,
                    "claim_hash": _sha256_text(
                        raw_claim["claim_hash"],
                        f"claim.claim_hash:{claim_id}",
                    ),
                    "event_id": _safe_token(
                        raw_claim["event_id"],
                        f"claim.event_id:{claim_id}",
                    ),
                    "pattern_id": _safe_token(
                        raw_claim["pattern"]["pattern_id"],
                        f"claim.pattern_id:{claim_id}",
                    ),
                    "pattern_version": _safe_token(
                        raw_claim["pattern"]["pattern_version"],
                        f"claim.pattern_version:{claim_id}",
                    ),
                    "pattern_spec_hash": _sha256_text(
                        raw_claim["pattern"]["pattern_spec_hash"],
                        f"claim.pattern_spec_hash:{claim_id}",
                    ),
                    "symbol": symbol,
                    "snapshot_id": snapshot_id,
                    "snapshot_binding_hash": snapshot[
                        "snapshot_binding_hash"
                    ],
                    "current_row_hash": current_row_hash,
                    "observation_as_of": raw_claim["match"][
                        "observation_as_of"
                    ],
                }
            )

    snapshots.sort(key=lambda item: item["snapshot_id"])
    claims.sort(key=lambda item: item["claim_id"])
    body: dict[str, Any] = {
        "schema_version": SOURCE_BUNDLE_SCHEMA_VERSION,
        "research_only": True,
        "source_bundle_id": _safe_token(
            source_bundle_id,
            "source_bundle_id",
        ),
        "generated_at": _timestamp(generated_at, "generated_at"),
        "feature_library_version": library.version,
        "feature_library_hash": library.library_hash,
        "snapshots": snapshots,
        "claims": claims,
    }
    body["source_bundle_hash"] = _hash(body)
    verify_prospective_source_bundle(
        body,
        feature_library_path=feature_library_path,
    )
    return body


def verify_prospective_source_bundle(
    bundle: Mapping[str, Any],
    *,
    feature_library_path: str | Path | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise ConfirmationSourceError(
            "prospective_source_bundle_must_be_object"
        )
    if bundle.get("schema_version") != SOURCE_BUNDLE_SCHEMA_VERSION:
        raise ConfirmationSourceError(
            "prospective_source_bundle_schema_invalid"
        )
    if bundle.get("research_only") is not True:
        raise ConfirmationSourceError(
            "prospective_source_bundle_research_only_guard_missing"
        )
    library = _feature_library(feature_library_path)
    if bundle.get("feature_library_version") != library.version:
        raise ConfirmationSourceError(
            "prospective_source_feature_library_version_mismatch"
        )
    if bundle.get("feature_library_hash") != library.library_hash:
        raise ConfirmationSourceError(
            "prospective_source_feature_library_hash_mismatch"
        )
    stored = _sha256_text(
        bundle.get("source_bundle_hash"),
        "source_bundle_hash",
    )
    body = dict(bundle)
    body.pop("source_bundle_hash", None)
    if _hash(body) != stored:
        raise ConfirmationSourceError(
            "prospective_source_bundle_hash_mismatch"
        )
    snapshots = bundle.get("snapshots")
    claims = bundle.get("claims")
    if not isinstance(snapshots, list) or not snapshots:
        raise ConfirmationSourceError(
            "prospective_source_snapshots_nonempty_list_required"
        )
    if not isinstance(claims, list):
        raise ConfirmationSourceError(
            "prospective_source_claims_list_required"
        )

    snapshot_ids: set[str] = set()
    row_hashes_by_snapshot: dict[str, dict[str, str]] = {}
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, Mapping):
            raise ConfirmationSourceError(
                f"prospective_source_snapshot_must_be_object:{index}"
            )
        snapshot_id = _safe_token(
            snapshot.get("snapshot_id"),
            f"snapshots[{index}].snapshot_id",
        )
        if snapshot_id in snapshot_ids:
            raise ConfirmationSourceError(
                f"prospective_source_duplicate_snapshot:{snapshot_id}"
            )
        snapshot_ids.add(snapshot_id)
        _sha256_text(
            snapshot.get("capture_hash"),
            f"snapshots[{index}].capture_hash",
        )
        _sha256_text(
            snapshot.get("snapshot_binding_hash"),
            f"snapshots[{index}].snapshot_binding_hash",
        )
        _sha256_text(
            snapshot.get("snapshot_file_sha256"),
            f"snapshots[{index}].snapshot_file_sha256",
        )
        rows = snapshot.get("rows")
        if not isinstance(rows, list):
            raise ConfirmationSourceError(
                f"prospective_source_rows_list_required:{snapshot_id}"
            )
        row_hashes: dict[str, str] = {}
        projected_identity: list[dict[str, Any]] = []
        for row_index, item in enumerate(rows):
            if not isinstance(item, Mapping):
                raise ConfirmationSourceError(
                    f"prospective_source_row_invalid:{snapshot_id}:{row_index}"
                )
            symbol = _text(
                item.get("symbol"),
                f"snapshots[{index}].rows[{row_index}].symbol",
            )
            if symbol in row_hashes:
                raise ConfirmationSourceError(
                    f"prospective_source_duplicate_symbol:{snapshot_id}:{symbol}"
                )
            projection = item.get("row_projection")
            if not isinstance(projection, Mapping):
                raise ConfirmationSourceError(
                    f"prospective_source_row_projection_missing:{snapshot_id}:{symbol}"
                )
            row_hash = _sha256_text(
                item.get("row_hash"),
                f"snapshots[{index}].rows[{row_index}].row_hash",
            )
            if _hash(projection) != row_hash:
                raise ConfirmationSourceError(
                    f"prospective_source_row_hash_mismatch:{snapshot_id}:{symbol}"
                )
            if projection.get("symbol") != symbol:
                raise ConfirmationSourceError(
                    f"prospective_source_row_symbol_mismatch:{snapshot_id}:{symbol}"
                )
            if projection.get("snapshot_id") != snapshot_id:
                raise ConfirmationSourceError(
                    f"prospective_source_row_snapshot_mismatch:{snapshot_id}:{symbol}"
                )
            row_hashes[symbol] = row_hash
            projected_identity.append(
                {
                    "symbol": symbol,
                    "as_of": item.get("as_of"),
                    "snapshot_id": snapshot_id,
                    "row_hash": row_hash,
                }
            )
        if _hash(projected_identity) != snapshot.get("projected_rows_hash"):
            raise ConfirmationSourceError(
                f"prospective_source_projected_rows_hash_mismatch:{snapshot_id}"
            )
        sessions = snapshot.get("market_sessions")
        if not isinstance(sessions, list):
            raise ConfirmationSourceError(
                f"prospective_source_sessions_list_required:{snapshot_id}"
            )
        if _hash(sessions) != snapshot.get("session_map_hash"):
            raise ConfirmationSourceError(
                f"prospective_source_session_map_hash_mismatch:{snapshot_id}"
            )
        row_hashes_by_snapshot[snapshot_id] = row_hashes

    seen_claims: set[str] = set()
    for index, claim in enumerate(claims):
        if not isinstance(claim, Mapping):
            raise ConfirmationSourceError(
                f"prospective_source_claim_invalid:{index}"
            )
        claim_id = _safe_token(
            claim.get("claim_id"),
            f"claims[{index}].claim_id",
        )
        if claim_id in seen_claims:
            raise ConfirmationSourceError(
                f"prospective_source_duplicate_claim:{claim_id}"
            )
        seen_claims.add(claim_id)
        snapshot_id = _safe_token(
            claim.get("snapshot_id"),
            f"claims[{index}].snapshot_id",
        )
        if snapshot_id not in row_hashes_by_snapshot:
            raise ConfirmationSourceError(
                f"prospective_source_claim_snapshot_missing:{claim_id}"
            )
        symbol = _text(
            claim.get("symbol"),
            f"claims[{index}].symbol",
        )
        expected_row_hash = row_hashes_by_snapshot[snapshot_id].get(symbol)
        if expected_row_hash is None:
            raise ConfirmationSourceError(
                f"prospective_source_claim_symbol_missing:{claim_id}"
            )
        if claim.get("current_row_hash") != expected_row_hash:
            raise ConfirmationSourceError(
                f"prospective_source_claim_row_hash_mismatch:{claim_id}"
            )
        _sha256_text(
            claim.get("claim_hash"),
            f"claims[{index}].claim_hash",
        )
        _sha256_text(
            claim.get("pattern_spec_hash"),
            f"claims[{index}].pattern_spec_hash",
        )

    return {
        "valid": True,
        "source_bundle_id": bundle["source_bundle_id"],
        "source_bundle_hash": stored,
        "snapshot_count": len(snapshots),
        "claim_count": len(claims),
    }


def source_bundle_indexes(
    bundle: Mapping[str, Any],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    verify_prospective_source_bundle(bundle)
    snapshots = {
        str(item["snapshot_id"]): dict(item)
        for item in bundle["snapshots"]
    }
    claims = {
        str(item["claim_id"]): dict(item)
        for item in bundle["claims"]
    }
    return snapshots, claims
