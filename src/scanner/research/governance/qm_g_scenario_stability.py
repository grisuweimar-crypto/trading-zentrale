"""QM-G executable Scenario Stability challenger feature.

Research-only sidecar feature. It consumes already validated Elliott-vNext 6H
outputs plus their real source availability timestamps. It never changes the
frozen Elliott core, selects a count, creates a review context, changes
Universal Stance, or emits a portfolio action/order.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.decision_layer.phase6_elliott import (
    Elliott6HAdapterError,
    validate_elliott_6h_output,
)
from scanner.research.governance.qm_i_lineage import LineageRegistry, content_hash as lineage_content_hash

SCHEMA_VERSION = "qm_g_scenario_stability_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_g_scenario_stability_v1.json"


class ScenarioStabilityError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ScenarioStabilityError(f"value_required:{field}")
    return result


def _git_sha(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 40 or any(ch not in "0123456789abcdef" for ch in result):
        raise ScenarioStabilityError(f"git_sha_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ScenarioStabilityError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ScenarioStabilityError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(_text(value, field))
    except ValueError as exc:
        raise ScenarioStabilityError(f"invalid_date:{field}") from exc


def load_scenario_stability_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScenarioStabilityError(f"scenario_stability_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ScenarioStabilityError("scenario_stability_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ScenarioStabilityError("scenario_stability_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise ScenarioStabilityError("scenario_stability_execution_scope_invalid")
    return payload


def _scenario_identity(output: Mapping[str, Any]) -> dict[str, Any] | None:
    primary = output.get("primary_scenario")
    if not isinstance(primary, Mapping):
        return None
    scenario_id = str(primary.get("scenario_id") or "").strip()
    if not scenario_id:
        return None
    return {
        "scenario_id": scenario_id,
        "pattern_class": str(primary.get("pattern_class") or "") or None,
        "family": str(primary.get("family") or "") or None,
        "direction": str(primary.get("direction") or "") or None,
        "stage": str(primary.get("stage") or "") or None,
    }


def _snapshot(raw: Mapping[str, Any], index: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ScenarioStabilityError(f"snapshot_must_be_object:{index}")
    source_commit = _git_sha(raw.get("source_commit"), f"snapshots[{index}].source_commit")
    available_from = _timestamp(raw.get("available_from"), f"snapshots[{index}].available_from")
    output_raw = raw.get("output")
    if not isinstance(output_raw, Mapping):
        raise ScenarioStabilityError(f"output_must_be_object:{index}")
    try:
        output = validate_elliott_6h_output(output_raw)
    except Elliott6HAdapterError as exc:
        raise ScenarioStabilityError(f"invalid_6h_output:{index}:{exc}") from exc
    if _date(output["as_of"], f"snapshots[{index}].output.as_of") > available_from.date():
        raise ScenarioStabilityError(f"future_output_relative_to_source_availability:{index}")
    output_id = _text(output.get("output_id"), f"snapshots[{index}].output.output_id")
    return {
        "source_commit": source_commit,
        "available_from": available_from,
        "output": output,
        "output_id": output_id,
        "scenario": _scenario_identity(output),
    }


def build_scenario_stability_feature(
    snapshots: Sequence[Mapping[str, Any]],
    *,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build strict consecutive-primary-scenario identity stability observations."""
    contract = load_scenario_stability_contract(contract_path)
    if len(snapshots) < 2:
        raise ScenarioStabilityError("at_least_two_snapshots_required")
    normalized = [_snapshot(raw, index) for index, raw in enumerate(snapshots)]
    normalized.sort(key=lambda item: item["available_from"])

    symbol = str(normalized[0]["output"]["symbol"])
    timeframe = str(normalized[0]["output"]["timeframe"])
    degree = str(normalized[0]["output"]["degree"])
    seen_output_ids: set[str] = set()
    previous_available: datetime | None = None
    for index, item in enumerate(normalized):
        output = item["output"]
        if (
            str(output["symbol"]) != symbol
            or str(output["timeframe"]) != timeframe
            or str(output["degree"]) != degree
        ):
            raise ScenarioStabilityError(f"snapshot_identity_mismatch:{index}")
        if item["output_id"] in seen_output_ids:
            raise ScenarioStabilityError(f"duplicate_output_id:{item['output_id']}")
        seen_output_ids.add(item["output_id"])
        if previous_available is not None and item["available_from"] <= previous_available:
            raise ScenarioStabilityError("source_availability_must_be_strictly_increasing")
        previous_available = item["available_from"]

    observations: list[dict[str, Any]] = []
    counts = {"STABLE": 0, "CHANGED": 0, "INSUFFICIENT_EVIDENCE": 0}
    for prior, current in zip(normalized, normalized[1:]):
        prior_scenario = prior["scenario"]
        current_scenario = current["scenario"]
        if prior_scenario is None or current_scenario is None:
            status = "INSUFFICIENT_EVIDENCE"
            stable = None
        else:
            stable = prior_scenario["scenario_id"] == current_scenario["scenario_id"]
            status = "STABLE" if stable else "CHANGED"
        counts[status] += 1
        observations.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "degree": degree,
                "prior_output_id": prior["output_id"],
                "current_output_id": current["output_id"],
                "prior_source_commit": prior["source_commit"],
                "current_source_commit": current["source_commit"],
                "prior_available_from": prior["available_from"].isoformat(),
                "available_from": current["available_from"].isoformat(),
                "prior_scenario": deepcopy(prior_scenario),
                "current_scenario": deepcopy(current_scenario),
                "feature_status": status,
                "primary_scenario_id_stable": stable,
                "missing_is_not_neutral": True,
                "research_only": True,
            }
        )

    feature_definition = {
        "feature_id": contract["feature"]["feature_id"],
        "definition": contract["feature"]["definition"],
        "identity_field": contract["feature"]["identity_field"],
        "comparison": contract["feature"]["comparison"],
        "threshold_used": False,
        "smoothing_used": False,
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "challenger_name": "Scenario Stability",
        "symbol": symbol,
        "timeframe": timeframe,
        "degree": degree,
        "feature_definition": feature_definition,
        "feature_definition_hash": _hash(feature_definition),
        "observation_count": len(observations),
        "counts": counts,
        "observations": observations,
        "pit_safe": True,
        "uses_only_consecutive_available_6h_outputs": True,
        "outcomes_used_to_build_feature": False,
        "retroactive_reclassification_performed": False,
        "creates_review_context": False,
        "changes_elliott_core": False,
        "changes_universal_stance": False,
        "changes_portfolio_action": False,
        "execution_allowed": False,
        "productive_integration_enabled": False,
        "research_only": True,
    }
    result["feature_output_hash"] = _hash(result)
    return result


def register_scenario_stability_lineage(
    feature: Mapping[str, Any],
    *,
    source_refs: Sequence[Mapping[str, str]],
    lineage_registry: LineageRegistry,
    node_id: str,
    version_id: str,
    actor_id: str,
    actor_role: str,
) -> dict[str, str]:
    """Register the immutable feature and all declared 6H source ancestry in QM-I."""
    if feature.get("schema_version") != SCHEMA_VERSION or feature.get("research_only") is not True:
        raise ScenarioStabilityError("valid_scenario_stability_feature_required")
    if feature.get("changes_elliott_core") is not False or feature.get("changes_portfolio_action") is not False:
        raise ScenarioStabilityError("scenario_stability_feature_boundary_violation")
    if len(source_refs) < 2:
        raise ScenarioStabilityError("scenario_stability_lineage_requires_two_sources")

    normalized_sources: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(source_refs):
        source_id = _text(raw.get("node_id"), f"source_refs[{index}].node_id")
        source_version = _text(raw.get("version_id"), f"source_refs[{index}].version_id")
        expected_hash = _text(raw.get("content_hash"), f"source_refs[{index}].content_hash").lower()
        key = (source_id, source_version)
        if key in seen:
            raise ScenarioStabilityError(f"duplicate_scenario_stability_source:{source_id}::{source_version}")
        seen.add(key)
        node = lineage_registry.get_node(source_id, source_version)
        if node.get("content_hash") != expected_hash:
            raise ScenarioStabilityError(f"scenario_stability_source_hash_mismatch:{source_id}::{source_version}")
        if node.get("lineage_complete") is not True:
            raise ScenarioStabilityError(f"scenario_stability_source_lineage_incomplete:{source_id}::{source_version}")
        normalized_sources.append(
            {"node_id": source_id, "version_id": source_version, "content_hash": expected_hash}
        )

    node_id = _text(node_id, "node_id")
    version_id = _text(version_id, "version_id")
    feature_hash = lineage_content_hash(dict(feature))
    observations = feature.get("observations")
    if not isinstance(observations, list) or not observations:
        raise ScenarioStabilityError("scenario_stability_observations_required_for_lineage")
    as_of = max(_text(row.get("available_from"), "observation.available_from") for row in observations if isinstance(row, Mapping))
    lineage_registry.register_node(
        record={
            "node_id": node_id,
            "version_id": version_id,
            "node_type": "FEATURE",
            "content_hash": feature_hash,
            "lineage_complete": True,
            "as_of": as_of,
            "metadata": {
                "module": "QM-G",
                "business_area": "BA-QM6",
                "challenger_name": "Scenario Stability",
                "feature_id": feature["feature_definition"]["feature_id"],
                "feature_output_hash": feature.get("feature_output_hash"),
                "source_count": len(normalized_sources),
                "sidecar_evidence_only": True,
            },
        },
        actor_id=actor_id,
        actor_role=actor_role,
    )
    for source in normalized_sources:
        edge_seed = [
            source["node_id"],
            source["version_id"],
            node_id,
            version_id,
            "DERIVED_FROM",
        ]
        lineage_registry.register_edge(
            record={
                "edge_id": "qm-g:" + _hash(edge_seed)[:24],
                "from_node_id": source["node_id"],
                "from_version_id": source["version_id"],
                "to_node_id": node_id,
                "to_version_id": version_id,
                "relation": "DERIVED_FROM",
                "material_for_ancestry": True,
            },
            actor_id=actor_id,
            actor_role=actor_role,
        )
    return {"node_id": node_id, "version_id": version_id, "content_hash": feature_hash}
