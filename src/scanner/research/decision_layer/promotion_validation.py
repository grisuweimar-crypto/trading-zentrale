"""Phase-7I validation and promotion-readiness review.

7I does not create or change a stance, transition, portfolio action, reliability
assessment or Depot-Watch decision. It separates technical readiness from
prospective empirical evidence and explicit future promotion review.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping

import pandas as pd

from scanner.reports.selection_timing import HORIZONS
from scanner.research.decision_layer.conflict_research import (
    _moving_date_blocks,
    _support_region_count,
    attach_timing_topology,
)
from scanner.research.decision_layer.dataset import (
    DecisionDatasetConfig,
    build_decision_research_dataset,
)
from scanner.research.decision_layer.evidence_archive import load_evidence_archive


SCHEMA_VERSION = "decision_validation_promotion_v1"
TRACE_SUMMARY_SCHEMA_VERSION = "decision_shadow_trace_summary_v1"
READINESS_STATES = frozenset({
    "pre_prospective_start",
    "shadow_capture_not_started",
    "collecting_prospective_evidence",
    "collecting_prospective_dataset",
    "awaiting_mature_outcomes",
    "collecting_prospective_comparison_support",
    "awaiting_downstream_shadow_trace",
    "metrics_ready_for_promotion_review",
})
REQUIRED_PHASES = ("7A", "7B", "7C", "7D", "7E", "7F", "7G", "7H")
DOWNSTREAM_TRACE_LAYERS = frozenset({"7D", "7E", "7F", "7G", "7H"})
FORBIDDEN_TRUE_FIELDS = frozenset({
    "productive_integration_enabled",
    "execution_allowed",
    "productive_promotion_approved",
    "automatic_promotion_performed",
    "spent_data_used_for_empirical_confirmation",
})


class PromotionValidationError(ValueError):
    """Raised when Phase-7I validation or promotion guards fail."""


def _day(value: object) -> date:
    text = str(value or "").strip()
    if not text:
        raise PromotionValidationError("date_required")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise PromotionValidationError(f"invalid_date:{text}") from exc


def _canonical_hash(value: object) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def _load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PromotionValidationError(f"required_file_missing:{path.as_posix()}") from exc
    except json.JSONDecodeError as exc:
        raise PromotionValidationError(f"invalid_json:{path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise PromotionValidationError(f"json_object_required:{path.as_posix()}")
    return value


def _source_contracts(
    root: Path,
    protocol: Mapping[str, object],
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    specs = protocol.get("required_contracts")
    if not isinstance(specs, Mapping):
        raise PromotionValidationError("required_contracts_missing")

    contracts: dict[str, dict[str, object]] = {}
    hashes: dict[str, str] = {}
    for phase in REQUIRED_PHASES:
        spec = specs.get(phase)
        if not isinstance(spec, Mapping):
            raise PromotionValidationError(f"required_contract_spec_missing:{phase}")
        path = root / str(spec.get("path") or "")
        contract = _load_json(path)
        expected_schema = str(spec.get("schema_version") or "")
        if contract.get("schema_version") != expected_schema:
            raise PromotionValidationError(f"source_contract_schema_mismatch:{phase}")
        if contract.get("phase") != phase:
            raise PromotionValidationError(f"source_contract_phase_mismatch:{phase}")
        if contract.get("research_only") is not True:
            raise PromotionValidationError(f"source_contract_not_research_only:{phase}")
        if contract.get("productive_integration_enabled") is not False:
            raise PromotionValidationError(f"source_contract_productive_at_freeze:{phase}")
        contracts[phase] = contract
        hashes[phase] = _canonical_hash(contract)

    start = str(protocol.get("prospective_unspent_from") or "")
    spent = str(protocol.get("legacy_replay_spent_through") or "")
    if _day(start) <= _day(spent):
        raise PromotionValidationError("prospective_start_must_follow_spent_cutoff")

    checks = (
        ("7B", contracts["7B"].get("prospective_unspent_start")),
        ("7C", contracts["7C"].get("prospective_confirmation_start")),
        ("7D", contracts["7D"].get("prospective_confirmation_start")),
        ("7D", (contracts["7D"].get("research_partition") or {}).get("prospective_unspent_from")
         if isinstance(contracts["7D"].get("research_partition"), Mapping) else None),
        ("7E", (contracts["7E"].get("research_partition") or {}).get("prospective_unspent_from")
         if isinstance(contracts["7E"].get("research_partition"), Mapping) else None),
        ("7F", (contracts["7F"].get("research_partition") or {}).get("prospective_unspent_from")
         if isinstance(contracts["7F"].get("research_partition"), Mapping) else None),
    )
    for phase, value in checks:
        if str(value or "") != start:
            raise PromotionValidationError(f"prospective_partition_mismatch:{phase}")

    candidate = contracts["7E"].get("candidate_rule")
    if not isinstance(candidate, Mapping):
        raise PromotionValidationError("7e_candidate_rule_missing")
    if candidate.get("empirically_validated") is not False:
        raise PromotionValidationError("7e_candidate_must_remain_unvalidated_at_freeze")
    if candidate.get("production_eligible") is not False:
        raise PromotionValidationError("7e_candidate_must_not_be_production_eligible_at_freeze")

    guards_7f = contracts["7F"].get("guards")
    if not isinstance(guards_7f, Mapping):
        raise PromotionValidationError("7f_guards_missing")
    if guards_7f.get("execution_allowed") is not False:
        raise PromotionValidationError("7f_execution_must_remain_disabled")
    if guards_7f.get("portfolio_action_rule_empirically_validated") is not False:
        raise PromotionValidationError("7f_action_rule_must_remain_unvalidated_at_freeze")

    validation_7g = contracts["7G"].get("validation")
    if not isinstance(validation_7g, Mapping):
        raise PromotionValidationError("7g_validation_missing")
    if validation_7g.get("reliability_model_empirically_validated") is not False:
        raise PromotionValidationError("7g_reliability_must_remain_unvalidated_at_freeze")
    if validation_7g.get("promotion_eligible") is not False:
        raise PromotionValidationError("7g_must_not_be_promotion_eligible_at_freeze")
    if validation_7g.get("execution_allowed") is not False:
        raise PromotionValidationError("7g_execution_must_remain_disabled")

    if contracts["7H"].get("execution_allowed") is not False:
        raise PromotionValidationError("7h_execution_must_remain_disabled")
    validation_7h = contracts["7H"].get("validation")
    if not isinstance(validation_7h, Mapping):
        raise PromotionValidationError("7h_validation_missing")
    if validation_7h.get("integration_empirically_validated") is not False:
        raise PromotionValidationError("7h_integration_must_remain_unvalidated_at_freeze")
    if validation_7h.get("phase_7i_promotion_review_required") is not True:
        raise PromotionValidationError("7h_must_require_7i_review")
    if validation_7h.get("promotion_eligible") is not False:
        raise PromotionValidationError("7h_must_not_be_promotion_eligible_at_freeze")

    inherited = protocol.get("inherited_statistical_rules")
    if not isinstance(inherited, Mapping):
        raise PromotionValidationError("inherited_statistical_rules_missing")
    if int(inherited.get("minimum_group_n", -1)) != int(contracts["7C"].get("min_group_n", -2)):
        raise PromotionValidationError("7i_may_not_change_7c_min_group_n")
    stat_rules = contracts["7C"].get("statistical_rules")
    if not isinstance(stat_rules, Mapping):
        raise PromotionValidationError("7c_statistical_rules_missing")
    if int(inherited.get("minimum_temporal_support_regions", -1)) != int(
        stat_rules.get("minimum_temporal_support_regions", -2)
    ):
        raise PromotionValidationError("7i_may_not_change_7c_temporal_support_rule")
    if list(inherited.get("horizons_sessions", [])) != list(contracts["7C"].get("horizons_sessions", [])):
        raise PromotionValidationError("7i_may_not_change_7c_horizons")
    if inherited.get("new_performance_thresholds_added_by_7i") is not False:
        raise PromotionValidationError("7i_new_performance_thresholds_forbidden")

    return contracts, hashes


def _publication_as_of(root: Path) -> str:
    metadata = _load_json(root / "artifacts" / "research" / "history_metadata.json")
    value = str(metadata.get("as_of") or "").strip()
    if not value:
        raise PromotionValidationError("publication_as_of_missing")
    _day(value)
    return value


def _comparison_readiness(
    mature: pd.DataFrame,
    horizon: int,
    contract_7c: Mapping[str, object],
) -> dict[str, object]:
    min_group_n = int(contract_7c.get("min_group_n", -1))
    statistical_rules = contract_7c.get("statistical_rules")
    if min_group_n < 1 or not isinstance(statistical_rules, Mapping):
        raise PromotionValidationError("invalid_7c_support_contract")
    min_regions = int(statistical_rules.get("minimum_temporal_support_regions", -1))
    if min_regions < 1:
        raise PromotionValidationError("invalid_7c_temporal_support_requirement")

    work = attach_timing_topology(mature, horizon)
    topology = f"timing_topology_{horizon}t"
    dates, _blocks, block_length = _moving_date_blocks(work, horizon)
    results: dict[str, object] = {}
    comparisons = contract_7c.get("pre_registered_comparisons")
    if not isinstance(comparisons, list):
        raise PromotionValidationError("7c_pre_registered_comparisons_missing")
    for item in comparisons:
        if not isinstance(item, Mapping):
            continue
        if int(item.get("horizon_sessions", -1)) != horizon:
            continue
        comparison_id = str(item.get("id") or "").strip()
        if not comparison_id:
            raise PromotionValidationError("7c_comparison_id_missing")
        left_name = str(item.get("left") or "")
        right_name = str(item.get("right") or "")
        left = work.loc[work[topology].eq(left_name)]
        right = work.loc[work[topology].eq(right_name)]
        left_regions = _support_region_count(left, dates, block_length)
        right_regions = _support_region_count(right, dates, block_length)
        data_ready = (
            len(left) >= min_group_n
            and len(right) >= min_group_n
            and left_regions >= min_regions
            and right_regions >= min_regions
        )
        results[comparison_id] = {
            "horizon_sessions": horizon,
            "left": left_name,
            "right": right_name,
            "left_N": int(len(left)),
            "right_N": int(len(right)),
            "minimum_group_n": min_group_n,
            "left_temporal_support_regions": int(left_regions),
            "right_temporal_support_regions": int(right_regions),
            "minimum_temporal_support_regions": min_regions,
            "block_length_sessions": int(block_length),
            "data_ready": bool(data_ready),
            "outcome_direction_evaluated": False,
        }
    return results


def _dataset_summary(
    root: Path,
    contract_7b: Mapping[str, object],
    contract_7c: Mapping[str, object],
    reviewed_as_of: date,
) -> dict[str, object]:
    research = root / "artifacts" / "research"
    history = pd.read_csv(research / "history_analysis.csv", low_memory=False)
    prices = pd.read_csv(research / "price_backfill.csv", low_memory=False)
    timing_catalog = _load_json(research / "timing_patterns_1b_frozen.json")
    config = DecisionDatasetConfig.from_contract(contract_7b)
    frame, metadata = build_decision_research_dataset(history, prices, timing_catalog, config)

    obs = pd.to_datetime(frame["obs_date"], errors="coerce")
    if obs.isna().any():
        raise PromotionValidationError("dataset_obs_date_invalid")
    visible = frame.loc[obs.dt.date <= reviewed_as_of].copy()
    prospective = visible.loc[visible["research_partition"].eq("prospective_unspent")].copy()

    horizon_summary: dict[str, object] = {}
    all_comparisons: list[bool] = []
    for horizon in HORIZONS:
        peer = f"peer_excess_{horizon}t"
        if peer not in prospective.columns:
            raise PromotionValidationError(f"prospective_peer_label_missing:{horizon}")
        mature = prospective.loc[pd.to_numeric(prospective[peer], errors="coerce").notna()].copy()
        comparisons = _comparison_readiness(mature, horizon, contract_7c)
        all_comparisons.extend(
            bool(value.get("data_ready"))
            for value in comparisons.values()
            if isinstance(value, Mapping)
        )
        horizon_summary[str(horizon)] = {
            "prospective_rows": int(len(prospective)),
            "mature_peer_excess_rows": int(len(mature)),
            "mature_symbols": int(mature["symbol"].nunique()) if len(mature) else 0,
            "mature_observation_days": int(
                pd.to_datetime(mature["obs_date"], errors="coerce").dt.normalize().nunique()
            ) if len(mature) else 0,
            "pre_registered_comparison_readiness": comparisons,
            "all_pre_registered_comparisons_data_ready": bool(comparisons) and all(
                bool(value.get("data_ready"))
                for value in comparisons.values()
                if isinstance(value, Mapping)
            ),
        }

    return {
        "schema_version": metadata.get("schema_version"),
        "rows_visible_as_of_review": int(len(visible)),
        "future_rows_ignored": int(len(frame) - len(visible)),
        "prospective_rows": int(len(prospective)),
        "prospective_symbols": int(prospective["symbol"].nunique()) if len(prospective) else 0,
        "prospective_date_min": (
            pd.to_datetime(prospective["obs_date"]).min().date().isoformat()
            if len(prospective) else None
        ),
        "prospective_date_max": (
            pd.to_datetime(prospective["obs_date"]).max().date().isoformat()
            if len(prospective) else None
        ),
        "horizons": horizon_summary,
        "all_pre_registered_comparisons_data_ready": bool(all_comparisons) and all(all_comparisons),
        "spent_rows_visible": int(
            visible["research_partition"].eq("legacy_replay_spent").sum()
        ),
        "spent_rows_count_toward_empirical_confirmation": False,
    }


def _archive_summary(
    root: Path,
    reviewed_as_of: date,
    prospective_start: str,
) -> dict[str, object]:
    path = root / "artifacts" / "research" / "decision_evidence_7a.jsonl"
    packets, metadata = load_evidence_archive(
        path,
        prospective_start=prospective_start,
        missing_ok=True,
    )
    visible = [
        packet
        for packet in packets
        if _day(packet.get("as_of")) <= reviewed_as_of
        and packet.get("archive_partition") == "prospective_unspent"
    ]
    future_ignored = sum(_day(packet.get("as_of")) > reviewed_as_of for packet in packets)
    return {
        "path": path.as_posix(),
        "archive_status": metadata.get("status"),
        "prospective_packet_count": len(visible),
        "prospective_symbol_count": len({str(packet.get("symbol")) for packet in visible}),
        "prospective_snapshot_count": len({
            str(packet.get("source_snapshot_id")) for packet in visible
        }),
        "future_packets_ignored": int(future_ignored),
        "legacy_replay_packets_count_toward_empirical_confirmation": False,
    }


def validate_shadow_trace_summary(
    summary: Mapping[str, object] | None,
    *,
    prospective_start: str,
    reviewed_as_of: date,
) -> dict[str, object]:
    if summary is None:
        return {
            "schema_version": TRACE_SUMMARY_SCHEMA_VERSION,
            "status": "not_supplied",
            "trace_rows": 0,
            "symbols": 0,
            "captured_layers": [],
            "layer_metrics_ready": {},
            "contains_raw_position_values": False,
            "public_repository_persistence": False,
            "future_trace_rows_ignored": 0,
        }
    if summary.get("schema_version") != TRACE_SUMMARY_SCHEMA_VERSION:
        raise PromotionValidationError("invalid_shadow_trace_summary_schema")
    if summary.get("contains_raw_position_values") is not False:
        raise PromotionValidationError("raw_position_values_forbidden_in_trace_summary")
    if summary.get("public_repository_persistence") is not False:
        raise PromotionValidationError("public_shadow_trace_persistence_forbidden")
    rows = int(summary.get("trace_rows", -1))
    symbols = int(summary.get("symbols", -1))
    if rows < 0 or symbols < 0:
        raise PromotionValidationError("invalid_shadow_trace_counts")
    layers = summary.get("captured_layers")
    if not isinstance(layers, list):
        raise PromotionValidationError("shadow_trace_captured_layers_missing")
    if any(str(layer) not in DOWNSTREAM_TRACE_LAYERS for layer in layers):
        raise PromotionValidationError("invalid_shadow_trace_layer")
    metrics = summary.get("layer_metrics_ready")
    if rows and not isinstance(metrics, Mapping):
        raise PromotionValidationError("shadow_trace_layer_metrics_ready_required")
    if metrics is None:
        metrics = {}
    if not isinstance(metrics, Mapping):
        raise PromotionValidationError("invalid_shadow_trace_layer_metrics_ready")
    if any(str(layer) not in DOWNSTREAM_TRACE_LAYERS for layer in metrics):
        raise PromotionValidationError("invalid_shadow_trace_metric_layer")
    if any(not isinstance(value, bool) for value in metrics.values()):
        raise PromotionValidationError("shadow_trace_metric_readiness_must_be_boolean")
    as_of_min = summary.get("as_of_min")
    as_of_max = summary.get("as_of_max")
    if rows:
        if not as_of_min or not as_of_max:
            raise PromotionValidationError("shadow_trace_dates_required_when_rows_exist")
        if _day(as_of_min) < _day(prospective_start):
            raise PromotionValidationError("pre_prospective_shadow_trace_forbidden")
        if _day(as_of_max) > reviewed_as_of:
            raise PromotionValidationError("future_shadow_trace_forbidden")
    return {
        "schema_version": TRACE_SUMMARY_SCHEMA_VERSION,
        "status": "available" if rows else "empty",
        "trace_rows": rows,
        "symbols": symbols,
        "captured_layers": sorted({str(layer) for layer in layers}),
        "layer_metrics_ready": {
            str(layer): bool(value) for layer, value in sorted(metrics.items(), key=lambda item: str(item[0]))
        },
        "contains_raw_position_values": False,
        "public_repository_persistence": False,
        "as_of_min": str(as_of_min) if as_of_min else None,
        "as_of_max": str(as_of_max) if as_of_max else None,
        "future_trace_rows_ignored": 0,
    }


def _mature_total(dataset_summary: Mapping[str, object]) -> int:
    horizons = dataset_summary.get("horizons")
    if not isinstance(horizons, Mapping):
        return 0
    return sum(
        int(value.get("mature_peer_excess_rows", 0))
        for value in horizons.values()
        if isinstance(value, Mapping)
    )


def _review_state(
    reviewed_as_of: date,
    prospective_start: str,
    archive: Mapping[str, object],
    dataset: Mapping[str, object],
    trace: Mapping[str, object],
) -> str:
    if reviewed_as_of < _day(prospective_start):
        return "pre_prospective_start"
    packets = int(archive.get("prospective_packet_count", 0))
    rows = int(dataset.get("prospective_rows", 0))
    traces = int(trace.get("trace_rows", 0))
    if packets == 0 and rows == 0 and traces == 0:
        return "shadow_capture_not_started"
    if packets == 0:
        return "collecting_prospective_evidence"
    if rows == 0:
        return "collecting_prospective_dataset"
    if _mature_total(dataset) == 0:
        return "awaiting_mature_outcomes"
    if dataset.get("all_pre_registered_comparisons_data_ready") is not True:
        return "collecting_prospective_comparison_support"
    captured = set(map(str, trace.get("captured_layers", [])))
    metrics = trace.get("layer_metrics_ready")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    layer_metrics_ready = all(metrics.get(layer) is True for layer in DOWNSTREAM_TRACE_LAYERS)
    if traces == 0 or not DOWNSTREAM_TRACE_LAYERS.issubset(captured) or not layer_metrics_ready:
        return "awaiting_downstream_shadow_trace"
    return "metrics_ready_for_promotion_review"


def build_promotion_report(
    root: str | Path,
    *,
    reviewed_as_of: str | None = None,
    trace_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    root = Path(root)
    protocol = _load_json(root / "configs" / "decision_validation_promotion_v1.json")
    if protocol.get("schema_version") != SCHEMA_VERSION:
        raise PromotionValidationError("unsupported_7i_protocol_schema")
    if protocol.get("phase") != "7I":
        raise PromotionValidationError("invalid_7i_protocol_phase")
    if protocol.get("research_only") is not True:
        raise PromotionValidationError("7i_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise PromotionValidationError("7i_productive_integration_forbidden")
    if protocol.get("execution_allowed") is not False:
        raise PromotionValidationError("7i_execution_forbidden")

    contracts, contract_hashes = _source_contracts(root, protocol)
    review_day = _day(reviewed_as_of or _publication_as_of(root))
    prospective_start = str(protocol.get("prospective_unspent_from") or "")
    dataset = _dataset_summary(root, contracts["7B"], contracts["7C"], review_day)
    archive = _archive_summary(root, review_day, prospective_start)
    trace = validate_shadow_trace_summary(
        trace_summary,
        prospective_start=prospective_start,
        reviewed_as_of=review_day,
    )
    state = _review_state(review_day, prospective_start, archive, dataset, trace)

    blockers: list[str] = []
    if state == "pre_prospective_start":
        blockers.append("prospective_partition_has_not_started")
    elif state == "shadow_capture_not_started":
        blockers.append("prospective_shadow_capture_not_started")
    elif state == "collecting_prospective_evidence":
        blockers.append("typed_7a_prospective_packets_missing")
    elif state == "collecting_prospective_dataset":
        blockers.append("prospective_7b_dataset_rows_missing")
    elif state == "awaiting_mature_outcomes":
        blockers.append("prospective_forward_outcomes_not_mature")
    elif state == "collecting_prospective_comparison_support":
        blockers.append("pre_registered_7c_comparisons_lack_frozen_minimum_support")
    elif state == "awaiting_downstream_shadow_trace":
        blockers.append("private_downstream_7d_7h_shadow_trace_missing_or_incomplete")

    metrics_ready = state == "metrics_ready_for_promotion_review"
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7I",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "reviewed_as_of": review_day.isoformat(),
        "prospective_unspent_from": prospective_start,
        "legacy_replay_spent_through": str(protocol.get("legacy_replay_spent_through")),
        "protocol_id": _canonical_hash(protocol),
        "source_contract_hashes": contract_hashes,
        "technical_readiness": {
            "contract_consistency_gate_passed": True,
            "shadow_collection_eligible": True,
            "passing_tests_is_empirical_validation": False,
        },
        "prospective_evidence": {
            "typed_evidence_archive": archive,
            "decision_dataset": dataset,
            "downstream_shadow_trace": trace,
        },
        "readiness": {
            "state": state,
            "promotion_review_metrics_ready": metrics_ready,
            "blockers": blockers,
        },
        "layer_status": {
            "7A": "contract_ready_for_shadow_collection",
            "7B": "dataset_ready_for_prospective_partition",
            "7C": (
                "prospective_metrics_can_be_reviewed"
                if metrics_ready else "discovery_only_until_prospective_confirmation"
            ),
            "7D": (
                "prospective_trace_available_for_review"
                if metrics_ready else "awaiting_prospective_trace_and_mature_outcomes"
            ),
            "7E": "candidate_rule_not_empirically_validated",
            "7F": "portfolio_action_rule_not_empirically_validated",
            "7G": "reliability_structure_not_empirically_validated",
            "7H": "integration_research_only_until_upstream_promotion_review",
        },
        "promotion": {
            "promotion_review_eligible": metrics_ready,
            "productive_promotion_approved": False,
            "automatic_promotion_performed": False,
            "source_contracts_modified": False,
            "separate_explicit_change_required_after_review": True,
        },
        "semantics": {
            "spent_data_used_for_empirical_confirmation": False,
            "zero_prospective_coverage_is_failure": False,
            "technical_readiness_equals_empirical_validation": False,
            "metrics_ready_equals_promotion": False,
            "shadow_collection_equals_productive_integration": False,
            "broker_execution_can_be_enabled_by_7i": False,
            "position_sizing_can_be_enabled_by_7i": False,
            "target_weight_can_be_enabled_by_7i": False,
        },
    }
    unsigned = deepcopy(report)
    report["report_id"] = _canonical_hash(unsigned)
    validate_promotion_report(report)
    return report


def validate_promotion_report(report: Mapping[str, object]) -> dict[str, object]:
    if report.get("schema_version") != SCHEMA_VERSION:
        raise PromotionValidationError("invalid_7i_report_schema")
    if report.get("research_only") is not True:
        raise PromotionValidationError("7i_report_must_be_research_only")
    if report.get("productive_integration_enabled") is not False:
        raise PromotionValidationError("7i_report_productive_integration_forbidden")
    if report.get("execution_allowed") is not False:
        raise PromotionValidationError("7i_report_execution_forbidden")

    readiness = report.get("readiness")
    if not isinstance(readiness, Mapping):
        raise PromotionValidationError("7i_readiness_missing")
    state = str(readiness.get("state") or "")
    if state not in READINESS_STATES:
        raise PromotionValidationError("invalid_7i_readiness_state")

    promotion = report.get("promotion")
    if not isinstance(promotion, Mapping):
        raise PromotionValidationError("7i_promotion_block_missing")
    if promotion.get("productive_promotion_approved") is not False:
        raise PromotionValidationError("7i_cannot_approve_productive_promotion")
    if promotion.get("automatic_promotion_performed") is not False:
        raise PromotionValidationError("7i_automatic_promotion_forbidden")
    if promotion.get("source_contracts_modified") is not False:
        raise PromotionValidationError("7i_source_contract_mutation_forbidden")
    expected_metrics_ready = state == "metrics_ready_for_promotion_review"
    if promotion.get("promotion_review_eligible") is not expected_metrics_ready:
        raise PromotionValidationError("promotion_review_eligibility_state_mismatch")

    semantics = report.get("semantics")
    if not isinstance(semantics, Mapping):
        raise PromotionValidationError("7i_semantics_missing")
    if semantics.get("spent_data_used_for_empirical_confirmation") is not False:
        raise PromotionValidationError("spent_data_confirmation_forbidden")
    if semantics.get("technical_readiness_equals_empirical_validation") is not False:
        raise PromotionValidationError("technical_empirical_conflation_forbidden")
    if semantics.get("metrics_ready_equals_promotion") is not False:
        raise PromotionValidationError("metrics_ready_cannot_equal_promotion")

    for field in FORBIDDEN_TRUE_FIELDS:
        if field in report and report.get(field) is True:
            raise PromotionValidationError(f"forbidden_true_field:{field}")

    report_id = str(report.get("report_id") or "")
    if not report_id:
        raise PromotionValidationError("report_id_required")
    unsigned = deepcopy(dict(report))
    unsigned.pop("report_id", None)
    if _canonical_hash(unsigned) != report_id:
        raise PromotionValidationError("report_id_integrity_failure")
    return deepcopy(dict(report))


def run_promotion_review(
    root: str | Path,
    output_path: str | Path,
    *,
    reviewed_as_of: str | None = None,
    trace_summary_path: str | Path | None = None,
) -> dict[str, object]:
    trace_summary = None
    if trace_summary_path is not None:
        trace_summary = _load_json(Path(trace_summary_path))
    report = build_promotion_report(
        root,
        reviewed_as_of=reviewed_as_of,
        trace_summary=trace_summary,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return report
