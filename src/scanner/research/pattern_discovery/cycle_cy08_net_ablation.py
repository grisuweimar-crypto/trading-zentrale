"""CY-08: cost-aware, paired L13 shadow ablation; research-only, never promotion.

Compare existing Timing with its conservative L13 fused shadow on the SAME
matured L8 observation. Each directional observation is an isolated
hypothetical full roundtrip; the costs are model assumptions, not fills.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import math
from pathlib import Path
import random
from statistics import mean
from typing import Any, Mapping, Sequence

from .challenger_integration import verify_challenger_trace
from .outcome_maturation import verify_matured_outcome

SCHEMA_VERSION = "cycle_direction_cy08_net_cost_v1"
RESULT_SCHEMA = "cycle_direction_cy08_net_shadow_ablation_v1"
DEFAULT_PATH = Path(__file__).resolve().parents[4] / "configs/cycle_direction/cy08_net_cost_v1.json"


class CycleCY08NetAblationError(ValueError):
    """Unsafe contract, missing provenance or unpairable evidence."""


def _hash(v: Any) -> str:
    return sha256(json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _finite(v: Any, field: str) -> float:
    if isinstance(v, bool):
        raise CycleCY08NetAblationError(f"invalid_number:{field}")
    try:
        value = float(v)
    except (TypeError, ValueError) as exc:
        raise CycleCY08NetAblationError(f"invalid_number:{field}") from exc
    if not math.isfinite(value):
        raise CycleCY08NetAblationError(f"invalid_number:{field}")
    return value


def load_cy08_net_contract(path: str | Path | None = None) -> dict[str, Any]:
    try:
        v = json.loads((Path(path) if path is not None else DEFAULT_PATH).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CycleCY08NetAblationError("cost_contract_unreadable") from exc
    if not isinstance(v, dict) or v.get("schema_version") != SCHEMA_VERSION:
        raise CycleCY08NetAblationError("cost_contract_schema_mismatch")
    for field, expected in {
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "matured_l8_required": True,
        "no_auto_promotion": True,
        "all_claims_for_trace_must_be_matured": True,
        "same_snapshot_symbol_horizon_target_and_baseline_required": True,
        "original_currency_and_adjusted_close_required": True,
        "costs_are_research_assumptions_not_observed_execution": True,
        "no_position_carry_between_overlapping_signals": True,
        "overlapping_forward_windows_not_independent": True,
        "positive_candidate_requires_primary_and_stress_lower_ci_gt_zero": True,
        "cost_model": "ISOLATED_SIGNAL_ROUNDTRIP_PER_NONZERO_SIDE_V1",
        "gross_target": "SIGNED_L8_CONTINUOUS_RETURN_OR_PEER_EXCESS",
        "paired_comparison": "EXISTING_TIMING_VS_EXISTING_TIMING_PLUS_PROMOTED_PATTERNS",
        "baseline_side_source": "L13_TRACE_TIMING_ABLATION_BASELINE_TIMING",
        "challenger_side_source": "L13_TRACE_TIMING_ABLATION_SHADOW_FUSED_TIMING",
        "costs_bps_roundtrip": [10, 20, 50],
        "primary_cost_bps_roundtrip": 20,
        "stress_cost_bps_roundtrip": 50,
        "minimum_paired_n": 30,
        "minimum_side_disagreement_n": 10,
        "minimum_temporal_support_regions": 2,
        "support_block_length_horizon_multiplier": 2,
        "bootstrap_interval": [0.025, 0.975],
        "evaluation_statuses": ["INSUFFICIENT_SUPPORT", "NO_NET_INCREMENTAL_VALUE", "NET_INCREMENTAL_VALUE_CANDIDATE"],
        "directional_mapping": {"positive": 1, "negative": -1, "conflicted": 0, "insufficient_evidence": 0},
    }.items():
        if v.get(field) != expected:
            raise CycleCY08NetAblationError(f"cost_contract_guard_mismatch:{field}")
    if type(v.get("bootstrap_repetitions")) is not int or v["bootstrap_repetitions"] < 1:
        raise CycleCY08NetAblationError("invalid_bootstrap_repetitions")
    if type(v.get("bootstrap_seed")) is not int:
        raise CycleCY08NetAblationError("invalid_bootstrap_seed")
    return v


def _side(v: Mapping[str, Any], where: str) -> int:
    if not isinstance(v, Mapping):
        raise CycleCY08NetAblationError(f"side_missing:{where}")
    state = v.get("state")
    mapping = {"positive": 1, "negative": -1, "conflicted": 0, "insufficient_evidence": 0}
    if state not in mapping:
        raise CycleCY08NetAblationError(f"side_state_invalid:{where}")
    if v.get("direction") != (state if abs(mapping[state]) else None):
        raise CycleCY08NetAblationError(f"side_direction_invalid:{where}")
    return mapping[state]


def _quantile(values: list[float], q: float) -> float:
    arr = sorted(values)
    p = q * (len(arr) - 1)
    lo, hi = math.floor(p), math.ceil(p)
    return float(arr[lo] if lo == hi else arr[lo] * (hi - p) + arr[hi] * (p - lo))


def _bootstrap(rows: list[dict[str, Any]], horizon: int, bps: int, spec: Mapping[str, Any]) -> list[float] | None:
    if not rows:
        return None
    dates = sorted({row["observation_date"] for row in rows})
    grouped = {d: [] for d in dates}
    for row in rows:
        grouped[row["observation_date"]].append(row["gross_difference"] - bps * row["exposure_difference"] / 10000)
    block = horizon * int(spec["support_block_length_horizon_multiplier"])
    rng = random.Random(int(spec["bootstrap_seed"]) + 100 * horizon + bps)
    values = []
    for _ in range(int(spec["bootstrap_repetitions"])):
        sample = []
        for _ in range(math.ceil(len(dates) / block)):
            start = rng.randrange(len(dates))
            for j in range(block):
                sample.extend(grouped[dates[(start + j) % len(dates)]])
        values.append(mean(sample))
    return [_quantile(values, float(q)) for q in spec["bootstrap_interval"]]


def evaluate_cy08_net_shadow_ablation(
    traces: Sequence[Mapping[str, Any]],
    matured_outcomes: Sequence[Mapping[str, Any]],
    *,
    horizon_sessions: int,
    evaluated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Hash-bound model-net evidence; cannot authorize L12 or live actions."""
    spec = dict(contract) if contract is not None else load_cy08_net_contract()
    if spec != load_cy08_net_contract():
        raise CycleCY08NetAblationError("cost_contract_unfrozen")
    if type(horizon_sessions) is not int or horizon_sessions not in {5, 20, 40, 60}:
        raise CycleCY08NetAblationError("unsupported_horizon")
    try:
        when = datetime.fromisoformat(evaluated_at.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise CycleCY08NetAblationError("invalid_evaluated_at") from exc
    if when.tzinfo is None:
        raise CycleCY08NetAblationError("evaluated_at_requires_timezone")

    outcomes: dict[str, Mapping[str, Any]] = {}
    for o in matured_outcomes:
        result = verify_matured_outcome(o)
        key = str(result["claim_id"])
        if key in outcomes:
            raise CycleCY08NetAblationError("duplicate_matured_claim")
        outcomes[key] = o

    rows: list[dict[str, Any]] = []
    incomplete: list[str] = []
    used_outcome_hashes: set[str] = set()
    seen: set[tuple[str, str, int, str, str]] = set()
    target_scopes: set[tuple[str, str]] = set()
    for trace in traces:
        verify_challenger_trace(trace)
        if trace.get("horizon_sessions") != horizon_sessions:
            raise CycleCY08NetAblationError("trace_horizon_mismatch")
        scope = trace.get("target_scope") or {}
        key = (
            str(trace.get("symbol")), str(trace.get("snapshot_id")),
            horizon_sessions, str(scope.get("target_id")), str(scope.get("baseline")),
        )
        if key in seen:
            raise CycleCY08NetAblationError("duplicate_symbol_snapshot_horizon_scope")
        seen.add(key)
        if not scope.get("target_id") or not scope.get("baseline"):
            continue
        target_scopes.add((key[-2], key[-1]))
        if len(target_scopes) > 1:
            raise CycleCY08NetAblationError("target_or_baseline_mixed")
        claims = list(trace.get("active_pattern_claims") or [])
        if not claims:
            continue
        ids = [str(c.get("claim_id")) for c in claims]
        if len(ids) != len(set(ids)):
            raise CycleCY08NetAblationError("duplicate_active_claim_id")
        selected = [outcomes.get(i) for i in ids]
        if any(o is None for o in selected):
            incomplete.append(str(trace["trace_hash"]))
            continue
        matured = [o for o in selected if o is not None]
        first = matured[0]
        claim = first.get("claim") or {}
        target = first.get("target") or {}
        value = _finite(first.get("outcome", {}).get("target_value"), "target_value")
        if (claim.get("symbol") != trace.get("symbol")
                or claim.get("capture_snapshot_id") != trace.get("snapshot_id")
                or target.get("target_id") != scope["target_id"]
                or target.get("baseline") != scope["baseline"]
                or target.get("horizon_sessions") != horizon_sessions):
            raise CycleCY08NetAblationError("l8_trace_identity_or_scope_mismatch")
        kind = {"DIRECTIONAL": ("RETURN", "return"), "RELATIVE_ALPHA": ("PEER_EXCESS", "peer_excess")}.get(target.get("pattern_type"))
        if kind is None:
            raise CycleCY08NetAblationError("l8_pattern_type_unsupported")
        if first.get("outcome", {}).get("target_value_kind") != kind[0] or not math.isclose(
            value, _finite(first["outcome"].get(kind[1]), kind[1]), abs_tol=1e-12, rel_tol=0
        ):
            raise CycleCY08NetAblationError("l8_continuous_target_kind_or_value_mismatch")
        price = first.get("price_provenance") or {}
        if price.get("price_kind") != "ADJUSTED_CLOSE" or price.get("currency_conversion_performed") is not False or not price.get("currency"):
            raise CycleCY08NetAblationError("l8_currency_or_price_invalid")
        if first.get("horizon_provenance", {}).get("target_session_date", "9999-12-31") > when.date().isoformat():
            raise CycleCY08NetAblationError("l8_not_mature_at_evaluation")
        for c, o in zip(claims, matured):
            if (c.get("claim_hash") != o.get("claim", {}).get("claim_hash")
                    or o.get("claim", {}).get("symbol") != claim.get("symbol")
                    or o.get("claim", {}).get("capture_snapshot_id") != claim.get("capture_snapshot_id")
                    or o.get("target") != target
                    or o.get("price_provenance", {}).get("currency") != price["currency"]
                    or o.get("horizon_provenance", {}).get("start_session_date") != first.get("horizon_provenance", {}).get("start_session_date")
                    or o.get("horizon_provenance", {}).get("target_session_date") != first.get("horizon_provenance", {}).get("target_session_date")
                    or not math.isclose(_finite(o.get("outcome", {}).get("target_value"), "target_value"), value, abs_tol=1e-12, rel_tol=0)):
                raise CycleCY08NetAblationError("l8_claims_disagree_within_trace")
        t = trace["timing_ablation"]
        baseline = _side(t["baseline_timing"], "baseline")
        pattern = _side(t["pattern_challenger"], "pattern")
        b, p = t["baseline_timing"]["state"], t["pattern_challenger"]["state"]
        expected = ("conflicted" if b == "conflicted" or p == "conflicted"
                    else b if p == "insufficient_evidence"
                    else p if b == "insufficient_evidence"
                    else b if b == p else "conflicted")
        fused = t["shadow_fused_timing"]
        if fused.get("state") != expected:
            raise CycleCY08NetAblationError("fused_shadow_state_mismatch")
        shadow = _side(fused, "shadow")
        if fused.get("action_change_allowed") is not False:
            raise CycleCY08NetAblationError("shadow_action_authority_forbidden")
        bgross, sgross = baseline * value, shadow * value
        rows.append({
            "trace_hash": str(trace["trace_hash"]),
            "observation_date": str(trace["observation_as_of"])[:10],
            "baseline_side": baseline, "shadow_side": shadow,
            "baseline_gross": bgross, "shadow_gross": sgross,
            "gross_difference": sgross - bgross,
            "exposure_difference": abs(shadow) - abs(baseline),
        })
        used_outcome_hashes.update(str(o["outcome_hash"]) for o in matured)

    days = sorted({r["observation_date"] for r in rows})
    block_length = horizon_sessions * spec["support_block_length_horizon_multiplier"]
    support_regions = math.ceil(len(days) / block_length)
    disagreement_n = sum(r["baseline_side"] != r["shadow_side"] for r in rows)
    sufficient = (len(rows) >= spec["minimum_paired_n"]
                  and disagreement_n >= spec["minimum_side_disagreement_n"]
                  and support_regions >= spec["minimum_temporal_support_regions"])
    scenarios = {}
    for bps in spec["costs_bps_roundtrip"]:
        bn = [x["baseline_gross"] - abs(x["baseline_side"]) * bps / 10000 for x in rows]
        sn = [x["shadow_gross"] - abs(x["shadow_side"]) * bps / 10000 for x in rows]
        scenarios[str(bps)] = {
            "cost_bps_roundtrip": bps,
            "mean_baseline_net": mean(bn) if bn else None,
            "mean_shadow_net": mean(sn) if sn else None,
            "mean_net_difference": mean(sn) - mean(bn) if rows else None,
            "paired_block_bootstrap_95": _bootstrap(rows, horizon_sessions, bps, spec),
        }
    primary = scenarios[str(spec["primary_cost_bps_roundtrip"])]
    stress = scenarios[str(spec["stress_cost_bps_roundtrip"])]
    positive = (sufficient and primary["mean_net_difference"] is not None
                and primary["mean_net_difference"] > 0 and stress["mean_net_difference"] > 0
                and primary["paired_block_bootstrap_95"][0] > 0
                and stress["paired_block_bootstrap_95"][0] > 0)
    status = ("INSUFFICIENT_SUPPORT" if not sufficient else
              "NET_INCREMENTAL_VALUE_CANDIDATE" if positive else "NO_NET_INCREMENTAL_VALUE")
    result = {
        "schema_version": RESULT_SCHEMA, "package": "CY-08",
        "research_only": True, "productive_integration_enabled": False,
        "execution_allowed": False, "promotion_performed": False,
        "regular_integration_approved": False, "no_auto_promotion": True,
        "costs_are_model_assumptions": True, "comparison": spec["paired_comparison"],
        "horizon_sessions": horizon_sessions, "evaluated_at": when.isoformat(),
        "contract_sha256": _hash(spec),
        "sample": {
            "paired_n": len(rows), "side_disagreement_n": disagreement_n,
            "temporal_support_regions": support_regions,
            "support_block_length_sessions_proxy": block_length,
            "immature_trace_count": len(incomplete),
            "immature_trace_hashes": sorted(incomplete),
            "target_scope": [{"target_id": a, "baseline": b} for a, b in sorted(target_scopes)],
        },
        "net_cost_scenarios": scenarios, "status": status,
        "positive_net_evidence_candidate": positive,
        "l13_trace_hashes": sorted(r["trace_hash"] for r in rows),
        "l8_outcome_hashes": sorted(used_outcome_hashes),
    }
    result["result_hash"] = _hash(result)
    return result


def verify_cy08_net_shadow_ablation(result: Mapping[str, Any], *, contract: Mapping[str, Any] | None = None) -> bool:
    spec = dict(contract) if contract is not None else load_cy08_net_contract()
    if spec != load_cy08_net_contract():
        raise CycleCY08NetAblationError("cost_contract_unfrozen")
    if result.get("schema_version") != RESULT_SCHEMA or result.get("contract_sha256") != _hash(spec):
        raise CycleCY08NetAblationError("result_contract_mismatch")
    for k in ("productive_integration_enabled", "execution_allowed", "promotion_performed", "regular_integration_approved"):
        if result.get(k) is not False:
            raise CycleCY08NetAblationError(f"result_boundary_invalid:{k}")
    if (result.get("research_only") is not True or result.get("no_auto_promotion") is not True
            or result.get("costs_are_model_assumptions") is not True):
        raise CycleCY08NetAblationError("result_research_only_guard_missing")
    if result.get("status") not in spec["evaluation_statuses"]:
        raise CycleCY08NetAblationError("result_status_invalid")
    body = dict(result)
    stored = body.pop("result_hash", None)
    if stored != _hash(body):
        raise CycleCY08NetAblationError("result_hash_mismatch")
    return True
