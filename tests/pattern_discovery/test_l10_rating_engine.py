from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import pytest

import scanner.research.pattern_discovery.rating_engine as rating_engine
from scanner.research.pattern_discovery.rating_engine import (
    RatingEngineError,
    RatingHistoryRegistry,
    build_rating_history,
    load_rating_contract,
    verify_rating_history,
)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest(value):
    return sha256(canonical(value).encode("utf-8")).hexdigest()


def frozen_pattern():
    pattern = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": "PAT-L10-A",
        "pattern_version": "v1",
        "pattern_spec_hash": digest({"spec": "PAT-L10-A"}),
        "natural_language_description": "L10 fixture",
        "pattern_spec": {"fixture": True},
        "freeze_timestamp": "2026-10-01T12:00:00Z",
        "data_cutoff": "2026-10-01T11:00:00Z",
        "code_version": "l10-test",
        "universe_version": "u-test",
        "feature_library_version": "f-test",
        "candidate_id": "CAND-L10-A",
        "discovery_run_id": "DISC-L10",
        "l4_evidence_hash": digest({"l4": 1}),
        "discovery_evidence": {"hit_rate": 0.99},
        "qm_c_handoff": {"application_status": "READY_NOT_APPLIED_BY_L5"},
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    pattern["frozen_record_hash"] = digest(pattern)
    return pattern


def metrics(*, aligned=0.04, lift=0.12, direction=0.68):
    return {
        "raw_n": 24,
        "effective_n": 8,
        "symbol_count": 5,
        "support_region_count": 4,
        "direction_probability": direction,
        "baseline_probability": direction - lift,
        "probability_advantage_lift": lift,
        "mean_aligned_outcome": aligned,
        "effect_size_vs_baseline": 0.03,
        "robust_uncertainty": {
            "aligned_effect_interval_95": [0.01, 0.07],
            "probability_lift_interval_95": [0.02, 0.20],
        },
        "concentration": {"top_symbol_share": 0.3},
        "temporal_stability": {"status": "AVAILABLE", "sign_reversal": False},
        "regime_diagnostics": {"qualified_sign_reversal": False},
        "confirmation_period": {
            "start": "2026-10-02",
            "end": "2026-10-20",
        },
    }


def report(
    result_class,
    *,
    day,
    plan="MON-1",
    final=True,
    aligned=0.04,
    lift=0.12,
    direction=0.68,
):
    p = frozen_pattern()
    result = {
        "pattern_id": p["pattern_id"],
        "pattern_version": p["pattern_version"],
        "pattern_spec_hash": p["pattern_spec_hash"],
        "result_class": result_class,
        "result_reasons": (
            [] if result_class == "SUPPORTED" else ["FIXTURE_REASON"]
        ),
        "multiple_testing": {"passed": result_class == "SUPPORTED"},
        "prospective_evidence": metrics(
            aligned=aligned,
            lift=lift,
            direction=direction,
        ),
        "discovery_evidence_used_as_confirmation": False,
    }
    payload = {
        "schema_version": "pattern_discovery_l9_confirmation_look_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "confirmation_look_id": f"LOOK-{plan}-{day}",
        "evaluated_at": f"2026-10-{day:02d}T20:00:00Z",
        "qm_governance": {
            "monitoring_plan_id": plan,
            "monitoring_plan_version": "v1",
            "look_id": "FINAL" if final else "LOOK_1",
            "is_final_look": final,
        },
        "look_status": "EVALUATED",
        "pattern_results": [result],
        "confirmation_evidence_hash": digest(
            {"evidence": plan, "day": day}
        ),
    }
    payload["look_hash"] = digest(
        {"look": plan, "day": day, "class": result_class}
    )
    return payload


def unresolved_report(*, day=5):
    p = frozen_pattern()
    payload = {
        "schema_version": "pattern_discovery_l9_confirmation_look_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "confirmation_look_id": f"LOOK-U-{day}",
        "evaluated_at": f"2026-10-{day:02d}T20:00:00Z",
        "qm_governance": {
            "monitoring_plan_id": "MON-U",
            "monitoring_plan_version": "v1",
            "next_look": {"look_id": "LOOK_1"},
        },
        "look_status": "UNRESOLVED_NOT_DUE",
        "family_readiness": {
            "H-PAT-L10-A": {
                "pattern_id": p["pattern_id"],
                "pattern_version": p["pattern_version"],
                "ready": False,
            }
        },
        "pattern_results": [],
        "confirmation_evidence_hash": None,
    }
    payload["look_hash"] = digest({"unresolved": day})
    return payload


@pytest.fixture(autouse=True)
def bypass_l9_fixture_verifier(monkeypatch):
    # L9 has its own exhaustive suite. The L10 workflow runs that full suite;
    # these tests isolate lifecycle mapping and audit behavior.
    monkeypatch.setattr(
        rating_engine,
        "verify_confirmation_look",
        lambda value: {
            "valid": True,
            "look_hash": value.get("look_hash"),
        },
    )


def ratings(history):
    return [row["new_rating"] for row in history["transitions"]]


def test_initial_discovery_is_d_and_hit_rate_is_not_a_rating():
    history = build_rating_history(frozen_pattern(), [])
    assert history["current_rating"] == "D"
    assert ratings(history) == ["D"]
    assert history["transitions"][0]["evidence_snapshot"][
        "discovery_evidence_used_as_confirmation"
    ] is False


def test_not_due_is_u_not_f():
    history = build_rating_history(frozen_pattern(), [unresolved_report()])
    assert history["current_rating"] == "U"
    assert ratings(history) == ["D", "U"]


def test_positive_inconclusive_is_c_but_hit_rate_alone_is_insufficient():
    challenger = build_rating_history(
        frozen_pattern(),
        [
            report(
                "INCONCLUSIVE",
                day=5,
                aligned=0.02,
                lift=0.05,
                direction=0.85,
            )
        ],
    )
    assert challenger["current_rating"] == "C"

    hit_rate_only = build_rating_history(
        frozen_pattern(),
        [
            report(
                "INCONCLUSIVE",
                day=5,
                aligned=-0.01,
                lift=-0.02,
                direction=0.90,
            )
        ],
    )
    assert hit_rate_only["current_rating"] == "U"


def test_supported_is_b_and_repeated_distinct_final_epoch_is_a():
    one = report("SUPPORTED", day=5, plan="MON-A")
    two_same_epoch = report("SUPPORTED", day=6, plan="MON-A")
    two_distinct = report("SUPPORTED", day=7, plan="MON-B")

    history_one = build_rating_history(frozen_pattern(), [one])
    assert history_one["current_rating"] == "B"

    history_same = build_rating_history(
        frozen_pattern(),
        [one, two_same_epoch],
    )
    assert history_same["current_rating"] == "B"

    history_distinct = build_rating_history(
        frozen_pattern(),
        [one, two_distinct],
    )
    assert history_distinct["current_rating"] == "A"
    assert ratings(history_distinct) == ["D", "B", "A"]


def test_a_can_degrade_and_f_is_terminal_for_same_pattern_version():
    history = build_rating_history(
        frozen_pattern(),
        [
            report("SUPPORTED", day=5, plan="MON-A"),
            report("SUPPORTED", day=7, plan="MON-B"),
            report(
                "INCONCLUSIVE",
                day=9,
                plan="MON-C",
                aligned=0.02,
                lift=0.03,
            ),
            report(
                "FALSIFIED",
                day=11,
                plan="MON-D",
                aligned=-0.04,
                lift=-0.08,
            ),
            report("SUPPORTED", day=13, plan="MON-E"),
        ],
    )
    assert ratings(history) == ["D", "B", "A", "B", "F"]
    assert history["current_rating"] == "F"


def test_final_non_replication_is_f_while_inconclusive_remains_u_or_c():
    negative = build_rating_history(
        frozen_pattern(),
        [
            report(
                "NEGATIVE_NOT_CONFIRMED",
                day=5,
                aligned=0.001,
                lift=0.0,
            )
        ],
    )
    unresolved = build_rating_history(
        frozen_pattern(),
        [report("INCONCLUSIVE", day=5, aligned=0.0, lift=0.0)],
    )
    assert negative["current_rating"] == "F"
    assert unresolved["current_rating"] == "U"


def test_formal_retirement_is_audited_and_requires_evidence_hash():
    retirement = {
        "reason_code": "STRUCTURAL_INVALIDATION",
        "observed_at": "2026-10-08T12:00:00Z",
        "evidence_hash": digest({"retirement": "proof"}),
    }
    history = build_rating_history(
        frozen_pattern(),
        [report("SUPPORTED", day=5)],
        retirement=retirement,
    )
    assert ratings(history) == ["D", "B", "F"]
    assert history["transitions"][-1]["reason_codes"] == [
        "STRUCTURAL_INVALIDATION"
    ]


def test_history_hash_and_evidence_snapshot_are_tamper_evident():
    history = build_rating_history(
        frozen_pattern(),
        [report("SUPPORTED", day=5)],
    )
    assert verify_rating_history(history)["valid"] is True

    tampered = deepcopy(history)
    tampered["transitions"][-1]["evidence_snapshot"][
        "result_class"
    ] = "FALSIFIED"
    with pytest.raises(
        RatingEngineError,
        match="evidence_snapshot_hash_mismatch",
    ):
        verify_rating_history(tampered)


def test_registry_is_append_only_hash_chained_and_idempotent(tmp_path):
    history = build_rating_history(
        frozen_pattern(),
        [
            report("SUPPORTED", day=5, plan="MON-A"),
            report("SUPPORTED", day=7, plan="MON-B"),
        ],
    )
    registry = RatingHistoryRegistry(tmp_path / "ratings.jsonl")
    first = registry.register_history(
        history,
        actor_id="tester",
        actor_role="researcher",
    )
    second = registry.register_history(
        history,
        actor_id="tester",
        actor_role="researcher",
    )
    assert first["appended_transition_count"] == 3
    assert second["idempotent"] is True
    assert registry.verify_integrity()["registry_event_count"] == 3


def test_contract_keeps_rating_research_only_and_separate_from_promotion():
    contract = load_rating_contract()
    assert contract["principles"]["rating_is_not_performance_score"] is True
    assert contract["principles"]["rating_never_promotes_by_itself"] is True
    assert contract["boundaries"]["promotion_performed"] is False
    assert contract["boundaries"][
        "decision_layer_integration_performed"
    ] is False
