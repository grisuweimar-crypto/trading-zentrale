from __future__ import annotations

from copy import deepcopy
from datetime import date

import pytest

from scanner.research.decision_layer.promotion_validation import (
    PromotionValidationError,
    SCHEMA_VERSION,
    TRACE_SUMMARY_SCHEMA_VERSION,
    _canonical_hash,
    _review_state,
    validate_promotion_report,
    validate_shadow_trace_summary,
)


START = "2026-09-26"
LAYERS = ["7D", "7E", "7F", "7G", "7H"]


def _archive(packets=0):
    return {"prospective_packet_count": packets}


def _dataset(rows=0, mature=0, comparison_ready=False):
    return {
        "prospective_rows": rows,
        "all_pre_registered_comparisons_data_ready": comparison_ready,
        "horizons": {
            "5": {"mature_peer_excess_rows": mature},
            "20": {"mature_peer_excess_rows": 0},
            "40": {"mature_peer_excess_rows": 0},
            "60": {"mature_peer_excess_rows": 0},
        },
    }


def _trace(rows=0, layers=None, metrics_ready=False):
    captured = list(layers or [])
    return {
        "trace_rows": rows,
        "captured_layers": captured,
        "layer_metrics_ready": {layer: metrics_ready for layer in captured},
    }


def _valid_report(state="pre_prospective_start"):
    metrics_ready = state == "metrics_ready_for_promotion_review"
    value = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7I",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "readiness": {
            "state": state,
            "promotion_review_metrics_ready": metrics_ready,
            "blockers": [],
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
            "technical_readiness_equals_empirical_validation": False,
            "metrics_ready_equals_promotion": False,
        },
    }
    value["report_id"] = _canonical_hash(value)
    return value


def test_pre_prospective_start_blocks_even_if_future_counts_are_supplied():
    state = _review_state(
        date(2026, 9, 25),
        START,
        _archive(100),
        _dataset(100, 100, comparison_ready=True),
        _trace(100, LAYERS, metrics_ready=True),
    )
    assert state == "pre_prospective_start"


def test_shadow_capture_not_started_after_start():
    assert _review_state(
        date(2026, 9, 26), START, _archive(), _dataset(), _trace()
    ) == "shadow_capture_not_started"


def test_typed_evidence_is_required_even_if_dataset_rows_exist():
    assert _review_state(
        date(2026, 9, 28), START, _archive(), _dataset(10, 0), _trace()
    ) == "collecting_prospective_evidence"


def test_dataset_rows_are_required_after_packets_arrive():
    assert _review_state(
        date(2026, 9, 28), START, _archive(5), _dataset(), _trace()
    ) == "collecting_prospective_dataset"


def test_mature_outcomes_are_required():
    assert _review_state(
        date(2026, 10, 5), START, _archive(5), _dataset(10, 0), _trace()
    ) == "awaiting_mature_outcomes"


def test_one_mature_outcome_does_not_make_metrics_ready():
    assert _review_state(
        date(2026, 10, 5),
        START,
        _archive(5),
        _dataset(10, 1, comparison_ready=False),
        _trace(),
    ) == "collecting_prospective_comparison_support"


def test_downstream_trace_is_required_after_frozen_comparison_support():
    assert _review_state(
        date(2027, 3, 20),
        START,
        _archive(100),
        _dataset(500, 100, comparison_ready=True),
        _trace(),
    ) == "awaiting_downstream_shadow_trace"


def test_captured_layers_without_ready_layer_metrics_remain_blocked():
    assert _review_state(
        date(2027, 3, 20),
        START,
        _archive(100),
        _dataset(500, 100, comparison_ready=True),
        _trace(100, LAYERS, metrics_ready=False),
    ) == "awaiting_downstream_shadow_trace"


def test_metrics_ready_requires_frozen_support_and_all_downstream_layer_metrics():
    assert _review_state(
        date(2027, 3, 20),
        START,
        _archive(100),
        _dataset(500, 100, comparison_ready=True),
        _trace(100, LAYERS, metrics_ready=True),
    ) == "metrics_ready_for_promotion_review"
    assert _review_state(
        date(2027, 3, 20),
        START,
        _archive(100),
        _dataset(500, 100, comparison_ready=True),
        _trace(100, ["7D", "7E", "7F", "7G"], metrics_ready=True),
    ) == "awaiting_downstream_shadow_trace"


def test_shadow_trace_summary_rejects_raw_position_values():
    with pytest.raises(PromotionValidationError, match="raw_position_values_forbidden"):
        validate_shadow_trace_summary(
            {
                "schema_version": TRACE_SUMMARY_SCHEMA_VERSION,
                "trace_rows": 1,
                "symbols": 1,
                "captured_layers": LAYERS,
                "layer_metrics_ready": {layer: True for layer in LAYERS},
                "contains_raw_position_values": True,
                "public_repository_persistence": False,
                "as_of_min": "2026-09-26",
                "as_of_max": "2026-09-26",
            },
            prospective_start=START,
            reviewed_as_of=date(2026, 9, 26),
        )


def test_shadow_trace_summary_requires_layer_metric_readiness_when_rows_exist():
    with pytest.raises(PromotionValidationError, match="layer_metrics_ready_required"):
        validate_shadow_trace_summary(
            {
                "schema_version": TRACE_SUMMARY_SCHEMA_VERSION,
                "trace_rows": 1,
                "symbols": 1,
                "captured_layers": LAYERS,
                "contains_raw_position_values": False,
                "public_repository_persistence": False,
                "as_of_min": "2026-09-26",
                "as_of_max": "2026-09-26",
            },
            prospective_start=START,
            reviewed_as_of=date(2026, 9, 26),
        )


def test_shadow_trace_summary_rejects_preprospective_rows():
    with pytest.raises(PromotionValidationError, match="pre_prospective_shadow_trace"):
        validate_shadow_trace_summary(
            {
                "schema_version": TRACE_SUMMARY_SCHEMA_VERSION,
                "trace_rows": 1,
                "symbols": 1,
                "captured_layers": LAYERS,
                "layer_metrics_ready": {layer: True for layer in LAYERS},
                "contains_raw_position_values": False,
                "public_repository_persistence": False,
                "as_of_min": "2026-09-25",
                "as_of_max": "2026-09-26",
            },
            prospective_start=START,
            reviewed_as_of=date(2026, 9, 26),
        )


def test_validator_rejects_productive_approval():
    report = _valid_report()
    report["promotion"]["productive_promotion_approved"] = True
    report["report_id"] = _canonical_hash({k: v for k, v in report.items() if k != "report_id"})
    with pytest.raises(PromotionValidationError, match="cannot_approve_productive_promotion"):
        validate_promotion_report(report)


def test_validator_rejects_spent_data_as_confirmation():
    report = _valid_report()
    report["semantics"]["spent_data_used_for_empirical_confirmation"] = True
    report["report_id"] = _canonical_hash({k: v for k, v in report.items() if k != "report_id"})
    with pytest.raises(PromotionValidationError, match="spent_data_confirmation_forbidden"):
        validate_promotion_report(report)


def test_report_id_detects_tampering():
    report = _valid_report()
    tampered = deepcopy(report)
    tampered["readiness"]["blockers"] = ["invented"]
    with pytest.raises(PromotionValidationError, match="report_id_integrity_failure"):
        validate_promotion_report(tampered)


def test_metrics_ready_is_review_eligibility_not_promotion():
    report = _valid_report("metrics_ready_for_promotion_review")
    validated = validate_promotion_report(report)
    assert validated["promotion"]["promotion_review_eligible"] is True
    assert validated["promotion"]["productive_promotion_approved"] is False
    assert validated["execution_allowed"] is False
