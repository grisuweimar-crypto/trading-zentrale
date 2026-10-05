"""Privacy-safe prospective W8 shadow-trace capture.

The private Depot-Watch bundle set contains position context that must never be
persisted in the public repository.  This module extracts only the minimum W8
review-state information required by the frozen 7I validation surface.

It does not compute outcomes, declare W8 metrics ready, validate W8
empirically, release promotion, or generate execution instructions.
"""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .portfolio_action import PortfolioActionError, validate_portfolio_action


ROOT = Path(__file__).resolve().parents[4]
TRACE_ROW_SCHEMA = "w8_private_shadow_trace_row_v1"
SUMMARY_SCHEMA = "decision_shadow_trace_summary_v1"
W8_POLICY_SCHEMA = "decision_depot_action_policy_v1"
W8_POLICY_ID = "w8_complete_7f_action_matrix_v1"
W8_PROSPECTIVE_START = "2026-10-02"

PRIVATE_POSITION_FIELDS = frozenset({
    "quantity",
    "average_entry_price",
    "current_price",
    "market_value",
    "cost_basis",
    "unrealized_pnl",
    "realized_pnl",
    "target_weight",
    "position_size",
})


class W8ShadowTraceError(ValueError):
    pass


def _canonical_hash(value: object) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def _day(value: object) -> str:
    text = str(value or "").strip()
    if len(text) < 10:
        raise W8ShadowTraceError("w8_trace_as_of_required")
    return text[:10]


def _ensure_no_private_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in PRIVATE_POSITION_FIELDS:
                raise W8ShadowTraceError(f"private_position_field_forbidden:{key}")
            _ensure_no_private_fields(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _ensure_no_private_fields(item)


def build_w8_trace_row(action: Mapping[str, object]) -> dict[str, object]:
    try:
        validated = validate_portfolio_action(action)
    except PortfolioActionError as exc:
        raise W8ShadowTraceError(str(exc)) from exc

    policy = validated.get("depot_action_policy")
    if not isinstance(policy, Mapping):
        raise W8ShadowTraceError("w8_policy_missing")
    if policy.get("schema_version") != W8_POLICY_SCHEMA:
        raise W8ShadowTraceError("w8_policy_schema_invalid")
    if policy.get("policy_id") != W8_POLICY_ID:
        raise W8ShadowTraceError("w8_policy_id_invalid")
    if policy.get("review_only") is not True:
        raise W8ShadowTraceError("w8_policy_must_remain_review_only")
    if policy.get("execution_allowed") is not False:
        raise W8ShadowTraceError("w8_execution_forbidden")

    as_of = str(validated.get("as_of") or "")
    if _day(as_of) < W8_PROSPECTIVE_START:
        raise W8ShadowTraceError("pre_w8_prospective_trace_forbidden")

    stance = validated.get("universal_stance_context")
    transition = validated.get("transition_context")
    position = validated.get("position_context")
    action_row = validated.get("portfolio_action")
    if not all(isinstance(item, Mapping) for item in (stance, transition, position, action_row)):
        raise W8ShadowTraceError("w8_trace_context_missing")

    row: dict[str, object] = {
        "schema_version": TRACE_ROW_SCHEMA,
        "symbol": str(validated.get("symbol") or ""),
        "as_of": as_of,
        "source_snapshot_id": str(validated.get("source_snapshot_id") or ""),
        "policy_id": W8_POLICY_ID,
        "policy_case": str(policy.get("policy_case") or ""),
        "base_action_state": str(policy.get("base_action_state") or ""),
        "resolved_action_state": str(policy.get("resolved_action_state") or ""),
        "action_changed": policy.get("action_changed") is True,
        "warning_code": policy.get("warning_code"),
        "conflict": policy.get("conflict") is True,
        "reassessment_required": policy.get("reassessment_required") is True,
        "state_history_state": policy.get("state_history_state"),
        "state_history_used_for_review_routing": (
            policy.get("state_history_used_for_review_routing") is True
        ),
        "swing_contexts": sorted(str(value) for value in (policy.get("swing_contexts") or [])),
        "universal_stance_raw_state": stance.get("raw_state"),
        "transition_status": transition.get("status"),
        "position_state": position.get("position_state"),
        "portfolio_action_state": action_row.get("state"),
        "contains_raw_position_values": False,
        "outcome_attached": False,
        "metrics_ready": False,
        "execution_allowed": False,
    }
    if not row["symbol"] or not row["source_snapshot_id"]:
        raise W8ShadowTraceError("w8_trace_identity_missing")
    _ensure_no_private_fields(row)
    row["observation_key"] = _canonical_hash({
        "symbol": row["symbol"],
        "as_of": row["as_of"],
        "source_snapshot_id": row["source_snapshot_id"],
        "policy_id": row["policy_id"],
    })
    row["trace_id"] = _canonical_hash(row)
    return row


def extract_w8_trace_rows(bundle_set: Mapping[str, object]) -> list[dict[str, object]]:
    bundles = bundle_set.get("bundles")
    if not isinstance(bundles, list):
        raise W8ShadowTraceError("bundle_set_bundles_required")
    rows: list[dict[str, object]] = []
    for bundle in bundles:
        if not isinstance(bundle, Mapping):
            raise W8ShadowTraceError("bundle_must_be_object")
        action = bundle.get("action")
        if not isinstance(action, Mapping):
            raise W8ShadowTraceError("bundle_action_required")
        rows.append(build_w8_trace_row(action))
    return rows


def _assert_private_path(path: Path, repo_root: Path = ROOT) -> None:
    resolved = path.resolve()
    root = repo_root.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        return
    raise W8ShadowTraceError("w8_private_trace_must_not_be_persisted_in_repository")


def load_private_trace(path: str | Path) -> list[dict[str, object]]:
    path = Path(path)
    _assert_private_path(path)
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise W8ShadowTraceError(f"invalid_private_trace_json:{line_no}") from exc
        if not isinstance(row, dict) or row.get("schema_version") != TRACE_ROW_SCHEMA:
            raise W8ShadowTraceError(f"invalid_private_trace_row:{line_no}")
        _ensure_no_private_fields(row)
        if row.get("contains_raw_position_values") is not False:
            raise W8ShadowTraceError("private_trace_raw_position_flag_invalid")
        if row.get("metrics_ready") is not False:
            raise W8ShadowTraceError("collector_may_not_mark_w8_metrics_ready")
        rows.append(dict(row))
    return rows


def append_private_trace(
    path: str | Path,
    new_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    path = Path(path)
    _assert_private_path(path)
    existing = load_private_trace(path)
    by_key = {str(row["observation_key"]): row for row in existing}

    added = 0
    duplicates = 0
    for raw in new_rows:
        row = dict(raw)
        if row.get("schema_version") != TRACE_ROW_SCHEMA:
            raise W8ShadowTraceError("invalid_new_trace_row_schema")
        _ensure_no_private_fields(row)
        key = str(row.get("observation_key") or "")
        trace_id = str(row.get("trace_id") or "")
        if not key or not trace_id:
            raise W8ShadowTraceError("trace_identity_missing")
        previous = by_key.get(key)
        if previous is not None:
            if previous.get("trace_id") != trace_id:
                raise W8ShadowTraceError("w8_trace_observation_identity_collision")
            duplicates += 1
            continue
        by_key[key] = row
        existing.append(row)
        added += 1

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in existing),
        encoding="utf-8",
    )
    return {
        "trace_path": str(path),
        "trace_rows": len(existing),
        "rows_added": added,
        "rows_already_present": duplicates,
        "public_repository_persistence": False,
        "contains_raw_position_values": False,
    }


def summarize_w8_trace(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    clean = [dict(row) for row in rows]
    for row in clean:
        if row.get("schema_version") != TRACE_ROW_SCHEMA:
            raise W8ShadowTraceError("invalid_trace_row_schema")
        _ensure_no_private_fields(row)
        if row.get("contains_raw_position_values") is not False:
            raise W8ShadowTraceError("trace_contains_raw_position_values")
        if row.get("metrics_ready") is not False:
            raise W8ShadowTraceError("collector_summary_may_not_mark_metrics_ready")
    dates = sorted(str(row.get("as_of") or "") for row in clean)
    symbols = {str(row.get("symbol") or "") for row in clean if row.get("symbol")}
    return {
        "schema_version": SUMMARY_SCHEMA,
        "status": "available" if clean else "empty",
        "trace_rows": len(clean),
        "symbols": len(symbols),
        "captured_layers": ["W8"] if clean else [],
        "layer_metrics_ready": {"W8": False} if clean else {},
        "contains_raw_position_values": False,
        "public_repository_persistence": False,
        "as_of_min": dates[0] if dates else None,
        "as_of_max": dates[-1] if dates else None,
        "w8_trace_rows": len(clean),
        "w8_as_of_min": dates[0] if dates else None,
        "w8_as_of_max": dates[-1] if dates else None,
        "future_trace_rows_ignored": 0,
    }
