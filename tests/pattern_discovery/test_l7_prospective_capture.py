from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest

from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.pattern_discovery import (
    FeatureLibrary,
    build_run_manifest,
    run_discovery_search,
)
from scanner.research.pattern_discovery.candidate_registry import freeze_candidates
from scanner.research.pattern_discovery.prospective_capture import (
    ProspectiveCaptureError,
    ProspectiveClaimRegistry,
    build_prospective_capture,
    load_prospective_capture_contract,
    match_frozen_pattern,
    persist_prospective_capture,
    verify_capture_report,
    verify_prospective_claim,
)
from scanner.research.pattern_discovery.statistical_guard import (
    apply_statistical_guard,
)


LIBRARY_PATH = Path("configs/pattern_discovery/feature_library_v1.json")


def preregistration():
    return {
        "declared_start_at": "2026-10-06T15:00:00+00:00",
        "data_cutoff": "2026-10-06T14:59:00+00:00",
        "data_sources": [
            "artifacts/research/l7_fixture.json",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-l7-test-v1",
        "feature_library_version": "PDL-FEATURE-LIBRARY-v1",
        "allowed_transformations": [
            "raw",
            "delta_observations",
            "change_direction",
            "threshold_crossing",
            "state_transition",
            "regime_context",
        ],
        "pattern_complexity": {
            "min_atomic_conditions": 1,
            "max_atomic_conditions": 3,
        },
        "pattern_types": ["DIRECTIONAL"],
        "targets": ["return_5t_gt_0"],
        "horizons_sessions": [5],
        "baselines": {
            "return_5t_gt_0": "same_horizon_unconditional_return_baseline"
        },
        "minimum_criteria": {
            "minimum_raw_n": 20,
            "minimum_temporal_support_regions": 3,
            "minimum_effect_size": 0.01,
            "minimum_baseline_lift": 0.10,
        },
        "search_budget": {
            "max_tested_candidates_total": 60,
            "max_tested_candidates_per_family": 60,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": 8,
            "max_frozen_candidates_per_horizon": 8,
        },
        "multiple_testing": {
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {"fdr_q": 0.05},
        },
        "statistical_primary_method":
            "cluster_aware_effect_and_probability_estimation",
        "robustness_checks": [
            "moving_block_bootstrap",
            "symbol_concentration",
            "temporal_support",
        ],
        "exclusion_rules": [
            "missing_required_feature",
            "missing_forward_target",
        ],
        "dependency_rules": [
            "overlapping_events_not_independent",
            "duplicate_specs_rejected",
        ],
        "code_version": "commit-l7-test",
        "determinism": {
            "randomness_allowed": False,
            "seed": None,
        },
    }


def discovery_observations():
    rows = []
    symbols = [
        ("P1", True),
        ("P2", True),
        ("P3", True),
        ("P4", True),
        ("N1", False),
        ("N2", False),
        ("N3", False),
        ("N4", False),
    ]
    for i in range(35):
        if i < 31:
            day = f"2026-07-{i + 1:02d}T12:00:00+00:00"
        else:
            day = f"2026-08-{i - 30:02d}T12:00:00+00:00"
        end = f"2026-09-{(i % 20) + 1:02d}T12:00:00+00:00"
        for rank, (symbol, positive) in enumerate(symbols):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": day,
                    "score": (
                        20.0 + i + rank * 0.01
                        if positive
                        else 80.0 - i - rank * 0.01
                    ),
                    "market_regime_stock": "bull" if positive else "bear",
                    "sector": (
                        "positive_sector"
                        if positive
                        else "negative_sector"
                    ),
                    "outcomes": {
                        "return_5t_gt_0": {
                            "value": (
                                0.04 + rank * 0.0005
                                if positive
                                else -0.03 - rank * 0.0005
                            ),
                            "end_at": end,
                        }
                    },
                }
            )
    return rows


def qm_context():
    return {
        "universe_ledger_version": "qm-b-test-ledger-v1",
        "instrument_master_version": "instrument-master-test-v1",
        "environment_or_dependency_fingerprint": "python311-test-env",
    }


@pytest.fixture(scope="module")
def l7_fixture(tmp_path_factory):
    root = tmp_path_factory.mktemp("l7")
    fixture = root / "artifacts/research/l7_fixture.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text('{"fixture":true}\n', encoding="utf-8")
    target_library = root / "configs/pattern_discovery/feature_library_v1.json"
    target_library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIBRARY_PATH, target_library)

    manifest = build_run_manifest(preregistration(), repo_root=root)
    library = FeatureLibrary()
    rows = discovery_observations()
    l3 = run_discovery_search(
        manifest,
        rows,
        repo_root=root,
        feature_library=library,
    )
    l4 = apply_statistical_guard(
        manifest,
        l3,
        rows,
        repo_root=root,
        feature_library=library,
    )
    snapshot = freeze_candidates(
        manifest,
        l3,
        l4,
        repo_root=root,
        freeze_timestamp="2026-10-06T15:05:00Z",
        qm_external_context=qm_context(),
        actor_id="l7-fixture",
        actor_role="researcher",
    )
    assert snapshot["frozen_patterns"]

    hypotheses = HypothesisRegistry(root / "qm" / "hypotheses.jsonl")
    plans = AnalysisPlanRegistry(root / "qm" / "plans.jsonl")
    closure = json.loads(
        Path("configs/qm_b_closure_v1.json").read_text(encoding="utf-8")
    )
    for record in snapshot["frozen_patterns"]:
        handoff = record["qm_c_handoff"]
        hypotheses.register_hypothesis(
            record=handoff["hypothesis_record"],
            actor_id="l7-governance",
            actor_role="researcher",
        )
        plans.register_plan(
            record=handoff["analysis_plan_record"],
            hypothesis_registry=hypotheses,
            actor_id="l7-governance",
            actor_role="researcher",
        )
        plans.freeze_plan(
            analysis_plan_id=handoff["analysis_plan_record"][
                "analysis_plan_id"
            ],
            analysis_plan_version=handoff["analysis_plan_record"][
                "analysis_plan_version"
            ],
            freeze_context=handoff["freeze_context"],
            hypothesis_registry=hypotheses,
            qm_b_closure=closure,
            actor_id="l7-governance",
            actor_role="researcher",
            reason="Apply exact L5 handoff before L7 fixture capture.",
        )
        hypotheses.transition(
            hypothesis_id=handoff["hypothesis_record"]["hypothesis_id"],
            hypothesis_version=handoff["hypothesis_record"][
                "hypothesis_version"
            ],
            to_state=handoff["requested_hypothesis_target_state"],
            actor_id="l7-governance",
            actor_role="researcher",
            reason="Freeze hypothesis after exact L5 analysis plan.",
        )

    return {
        "root": root,
        "snapshot": snapshot,
        "hypotheses": hypotheses,
        "plans": plans,
        "library": library,
    }


def _typed_state(state):
    if state == "TRUE":
        return True
    if state == "FALSE":
        return False
    try:
        return float(state)
    except (TypeError, ValueError):
        return state


def matching_inputs(fixture, *, add_outcome_noise=False):
    library = fixture["library"]
    pattern = fixture["snapshot"]["frozen_patterns"][0]
    conditions = pattern["pattern_spec"]["semantics"]["conditions"]

    history = []
    for i in range(10):
        day = 27 + i
        if day <= 30:
            as_of = f"2026-09-{day:02d}T12:00:00Z"
            generated = f"2026-09-{day:02d}T13:00:00Z"
        else:
            october = day - 30
            as_of = f"2026-10-{october:02d}T12:00:00Z"
            generated = f"2026-10-{october:02d}T13:00:00Z"
        history.append(
            {
                "symbol": "TEST",
                "as_of": as_of,
                "generated_at": generated,
                "snapshot_id": f"hist-{i:02d}",
            }
        )

    current = {
        "symbol": "TEST",
        "as_of": "2026-10-07T12:00:00Z",
        "generated_at": "2026-10-07T18:00:00Z",
        "snapshot_id": "snap-current",
        "observation_type": "observed_scanner",
        "data_source": "scanner_run",
    }

    for condition in conditions:
        feature = library.get_feature(
            condition["feature_id"],
            condition["feature_version"],
        )
        field = feature["source_field"]
        tid = condition["transformation_id"]
        state = str(condition["state"])

        if tid in {"raw", "regime_context"}:
            current[field] = _typed_state(state)
            for row in history:
                row[field] = _typed_state(state)
        elif tid == "change_direction":
            if state == "UP":
                current[field] = 10.0
                for row in history:
                    row[field] = 0.0
            else:
                current[field] = 0.0
                for row in history:
                    row[field] = 10.0
        elif tid == "threshold_crossing":
            threshold = float(condition["parameters"]["threshold"])
            if state == "CROSS_UP":
                current[field] = threshold + 1.0
                for row in history:
                    row[field] = threshold - 1.0
            else:
                current[field] = threshold - 1.0
                for row in history:
                    row[field] = threshold + 1.0
        elif tid == "state_transition":
            before, after = state.split("->", 1)
            current[field] = _typed_state(after)
            for row in history:
                row[field] = _typed_state(before)
        else:
            raise AssertionError(f"unexpected fixture transform: {tid}")

    if add_outcome_noise:
        current["outcomes"] = {
            "return_5t_gt_0": {
                "value": 99.0,
                "end_at": "2099-01-01T00:00:00Z",
            }
        }
        current["future_return"] = 999.0
        current["close"] = 12345.0
        for row in history:
            row["outcomes"] = {"forbidden_future": 123.0}
            row["close"] = 777.0

    metadata = {
        "schema_version": "research_views_v1",
        "snapshot_id": "snap-current",
        "as_of": "2026-10-07T12:00:00Z",
        "generated_at": "2026-10-07T18:00:00Z",
        "latest_run_complete": True,
        "source": {
            "file": "artifacts/watchlist/watchlist_full.csv",
            "sha256": "a" * 64,
        },
    }
    sessions = [
        {
            "symbol": "TEST",
            "session_id": "TEST-2026-10-08",
            "calendar_id": "TEST-CALENDAR",
            "start_at": "2026-10-08T08:00:00Z",
            "source": "test-session-calendar-v1",
        }
    ]
    return pattern, metadata, [current], history, sessions


def build_capture(fixture, **overrides):
    _, metadata, current, history, sessions = matching_inputs(fixture)
    kwargs = {
        "capture_at": "2026-10-07T18:10:00Z",
        "snapshot_file_sha256": "b" * 64,
        "history_file_sha256": "c" * 64,
        "hypothesis_registry": fixture["hypotheses"],
        "analysis_plan_registry": fixture["plans"],
        "feature_library": fixture["library"],
    }
    kwargs.update(overrides)
    return build_prospective_capture(
        [fixture["snapshot"]],
        metadata,
        current,
        history,
        sessions,
        **kwargs,
    )


def test_l7_contract_is_prospective_research_only():
    contract = load_prospective_capture_contract()
    assert contract["phase"] == "L7"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["schema_version"] == "pattern_discovery_l7_prospective_capture_v2"
    assert contract["principles"]["snapshot_age_does_not_invalidate_recorded_pit_inputs"] is True
    assert contract["principles"]["prospective_claims_require_future_start_session_at_capture"] is True
    assert "max_capture_delay_minutes" not in contract["snapshot"]
    assert contract["principles"]["outcome_information_is_forbidden_at_claim_creation"] is True
    assert contract["boundaries"]["outcome_maturation_performed"] is False


def test_exact_frozen_pattern_matches_later_pit_snapshot(l7_fixture):
    pattern, metadata, current, history, _ = matching_inputs(l7_fixture)
    result = match_frozen_pattern(
        pattern,
        current[0],
        history,
        snapshot_generated_at=metadata["generated_at"],
        feature_library=l7_fixture["library"],
    )
    assert result["status"] == "MATCH"
    assert result["pattern_id"] == pattern["pattern_id"]
    assert result["pattern_version"] == pattern["pattern_version"]
    assert all(
        item["match_status"] == "MATCH"
        for item in result["condition_evidence"]
    )


def test_capture_creates_immutable_claim_with_snapshot_version_session_and_horizon(l7_fixture):
    report = build_capture(l7_fixture)
    assert verify_capture_report(report)["valid"] is True
    assert report["counts"]["prospective_claim_count"] >= 1
    claim = report["claims"][0]
    assert verify_prospective_claim(claim)["valid"] is True
    assert claim["pattern"]["pattern_version"] == "v1"
    assert claim["match"]["snapshot_id"] == "snap-current"
    assert len(claim["match"]["snapshot_binding_hash"]) == 64
    assert len(claim["match"]["history_binding_hash"]) == 64
    assert claim["start_market_session"]["session_id"] == "TEST-2026-10-08"
    assert claim["forecast"]["horizon_sessions"] == 5
    assert claim["capture_state"] == "CAPTURED_UNMATURED"
    assert claim["outcome_available_at_capture"] is False
    assert claim["outcome_maturation_performed"] is False


def test_claim_requires_real_qm_c1_c2_confirmation_freeze(l7_fixture, tmp_path):
    _, metadata, current, history, sessions = matching_inputs(l7_fixture)
    empty_h = HypothesisRegistry(tmp_path / "empty" / "hypotheses.jsonl")
    empty_p = AnalysisPlanRegistry(tmp_path / "empty" / "plans.jsonl")
    with pytest.raises(
        ProspectiveCaptureError,
        match="l7_qm_c_authorization_invalid",
    ):
        build_prospective_capture(
            [l7_fixture["snapshot"]],
            metadata,
            current,
            history,
            sessions,
            capture_at="2026-10-07T18:10:00Z",
            snapshot_file_sha256="b" * 64,
            history_file_sha256="c" * 64,
            hypothesis_registry=empty_h,
            analysis_plan_registry=empty_p,
            feature_library=l7_fixture["library"],
        )


def test_delayed_snapshot_capture_allowed_before_future_session(l7_fixture):
    # The exact frozen snapshot is still valid more than six hours later.
    report = build_capture(l7_fixture, capture_at="2026-10-08T01:00:01Z")
    assert report["counts"]["prospective_claim_count"] >= 1
    assert report["claims"][0]["captured_at"] == "2026-10-08T01:00:01Z"
    assert verify_capture_report(report)["valid"] is True


def test_historical_v1_contract_still_blocks_legacy_late_capture(l7_fixture):
    legacy = load_prospective_capture_contract(
        "configs/pattern_discovery/l7_prospective_capture_v1.json"
    )
    assert legacy["schema_version"] == "pattern_discovery_l7_prospective_capture_v1"
    with pytest.raises(
        ProspectiveCaptureError,
        match="stale_snapshot_backfill_forbidden",
    ):
        build_capture(
            l7_fixture,
            capture_at="2026-10-08T01:00:01Z",
            contract=legacy,
        )
    old_report = build_capture(l7_fixture, contract=legacy)
    assert verify_capture_report(old_report)["valid"] is True
    assert verify_capture_report(old_report, contract=legacy)["valid"] is True
    assert old_report["l7_contract_hash"] != build_capture(l7_fixture)["l7_contract_hash"]
    with pytest.raises(
        ProspectiveCaptureError,
        match="capture_report_contract_hash_mismatch",
    ):
        verify_capture_report(
            old_report, contract=load_prospective_capture_contract()
        )


def test_l7_rejects_delayed_capture_after_supplied_market_session(l7_fixture):
    with pytest.raises(
        ProspectiveCaptureError,
        match="market_session_not_strictly_after_capture",
    ):
        build_capture(l7_fixture, capture_at="2026-10-08T09:00:00Z")


def test_pre_freeze_snapshot_creates_no_prospective_claim(l7_fixture):
    _, metadata, current, history, sessions = matching_inputs(l7_fixture)
    metadata = deepcopy(metadata)
    metadata["as_of"] = "2026-10-06T14:00:00Z"
    metadata["generated_at"] = "2026-10-06T15:00:00Z"
    current = deepcopy(current)
    current[0]["as_of"] = "2026-10-06T14:00:00Z"
    current[0]["generated_at"] = "2026-10-06T15:00:00Z"
    history = [
        row
        for row in history
        if row["generated_at"] < "2026-10-06T15:00:00Z"
    ]
    sessions = deepcopy(sessions)
    sessions[0]["start_at"] = "2026-10-07T08:00:00Z"

    report = build_prospective_capture(
        [l7_fixture["snapshot"]],
        metadata,
        current,
        history,
        sessions,
        capture_at="2026-10-06T15:01:00Z",
        snapshot_file_sha256="b" * 64,
        history_file_sha256="c" * 64,
        hypothesis_registry=l7_fixture["hypotheses"],
        analysis_plan_registry=l7_fixture["plans"],
        feature_library=l7_fixture["library"],
    )
    assert report["counts"]["post_freeze_pattern_count"] == 0
    assert report["claims"] == []
    assert all(
        item["reason"] == "SNAPSHOT_NOT_STRICTLY_POST_FREEZE"
        for item in report["exclusions"]
    )


def test_future_history_row_is_rejected_as_pit_leakage(l7_fixture):
    _, metadata, current, history, sessions = matching_inputs(l7_fixture)
    history = deepcopy(history)
    history.append(
        {
            "symbol": "TEST",
            "as_of": "2026-10-07T12:01:00Z",
            "generated_at": "2026-10-07T18:01:00Z",
            "snapshot_id": "future-history",
            "score": 1.0,
        }
    )
    with pytest.raises(
        ProspectiveCaptureError,
        match="history_not_strictly_before_current_snapshot",
    ):
        build_prospective_capture(
            [l7_fixture["snapshot"]],
            metadata,
            current,
            history,
            sessions,
            capture_at="2026-10-07T18:10:00Z",
            snapshot_file_sha256="b" * 64,
            history_file_sha256="c" * 64,
            hypothesis_registry=l7_fixture["hypotheses"],
            analysis_plan_registry=l7_fixture["plans"],
            feature_library=l7_fixture["library"],
        )


def test_missing_feature_history_remains_unavailable_and_creates_no_fake_claim(l7_fixture):
    pattern, metadata, current, history, sessions = matching_inputs(l7_fixture)
    current = deepcopy(current)
    first_condition = pattern["pattern_spec"]["semantics"]["conditions"][0]
    feature = l7_fixture["library"].get_feature(
        first_condition["feature_id"],
        first_condition["feature_version"],
    )
    current[0].pop(feature["source_field"], None)

    report = build_prospective_capture(
        [l7_fixture["snapshot"]],
        metadata,
        current,
        history,
        sessions,
        capture_at="2026-10-07T18:10:00Z",
        snapshot_file_sha256="b" * 64,
        history_file_sha256="c" * 64,
        hypothesis_registry=l7_fixture["hypotheses"],
        analysis_plan_registry=l7_fixture["plans"],
        feature_library=l7_fixture["library"],
    )
    summary = next(
        item
        for item in report["pattern_summaries"]
        if item["pattern_id"] == pattern["pattern_id"]
    )
    assert summary["unavailable_symbol_count"] == 1
    assert summary["prospective_claim_count"] == 0


def test_missing_start_market_session_blocks_claim_without_inference(l7_fixture):
    _, metadata, current, history, _ = matching_inputs(l7_fixture)
    report = build_prospective_capture(
        [l7_fixture["snapshot"]],
        metadata,
        current,
        history,
        [],
        capture_at="2026-10-07T18:10:00Z",
        snapshot_file_sha256="b" * 64,
        history_file_sha256="c" * 64,
        hypothesis_registry=l7_fixture["hypotheses"],
        analysis_plan_registry=l7_fixture["plans"],
        feature_library=l7_fixture["library"],
    )
    assert report["claims"] == []
    assert any(
        item["reason"] == "START_MARKET_SESSION_UNAVAILABLE"
        for item in report["exclusions"]
    )


def test_market_session_must_start_after_capture(l7_fixture):
    _, metadata, current, history, sessions = matching_inputs(l7_fixture)
    sessions = deepcopy(sessions)
    sessions[0]["start_at"] = "2026-10-07T18:00:00Z"
    with pytest.raises(
        ProspectiveCaptureError,
        match="market_session_not_strictly_after_capture",
    ):
        build_prospective_capture(
            [l7_fixture["snapshot"]],
            metadata,
            current,
            history,
            sessions,
            capture_at="2026-10-07T18:10:00Z",
            snapshot_file_sha256="b" * 64,
            history_file_sha256="c" * 64,
            hypothesis_registry=l7_fixture["hypotheses"],
            analysis_plan_registry=l7_fixture["plans"],
            feature_library=l7_fixture["library"],
        )


def test_outcome_and_price_noise_do_not_enter_or_change_claim_semantics(l7_fixture):
    _, metadata, current, history, sessions = matching_inputs(l7_fixture)
    clean = build_prospective_capture(
        [l7_fixture["snapshot"]],
        metadata,
        current,
        history,
        sessions,
        capture_at="2026-10-07T18:10:00Z",
        snapshot_file_sha256="b" * 64,
        history_file_sha256="c" * 64,
        hypothesis_registry=l7_fixture["hypotheses"],
        analysis_plan_registry=l7_fixture["plans"],
        feature_library=l7_fixture["library"],
    )

    _, metadata2, noisy_current, noisy_history, sessions2 = matching_inputs(
        l7_fixture,
        add_outcome_noise=True,
    )
    noisy = build_prospective_capture(
        [l7_fixture["snapshot"]],
        metadata2,
        noisy_current,
        noisy_history,
        sessions2,
        capture_at="2026-10-07T18:10:00Z",
        snapshot_file_sha256="b" * 64,
        history_file_sha256="c" * 64,
        hypothesis_registry=l7_fixture["hypotheses"],
        analysis_plan_registry=l7_fixture["plans"],
        feature_library=l7_fixture["library"],
    )
    assert clean == noisy
    serialized = json.dumps(noisy["claims"], sort_keys=True)
    assert '"outcomes"' not in serialized
    assert '"future_return"' not in serialized
    assert '"close"' not in serialized


def test_same_inputs_produce_same_capture_identity_and_claims(l7_fixture):
    first = build_capture(l7_fixture)
    second = build_capture(l7_fixture)
    assert first == second
    assert first["capture_id"] == second["capture_id"]
    assert first["capture_hash"] == second["capture_hash"]


def test_claim_registry_is_append_only_idempotent_and_detects_identity_collision(l7_fixture, tmp_path):
    report = build_capture(l7_fixture)
    first = persist_prospective_capture(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    assert first["registry"]["appended_claim_count"] == len(report["claims"])

    second = persist_prospective_capture(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    assert second["registry"]["appended_claim_count"] == 0
    assert second["registry"]["idempotent_claim_count"] == len(report["claims"])

    later = build_capture(
        l7_fixture,
        capture_at="2026-10-07T18:11:00Z",
    )
    with pytest.raises(
        ProspectiveCaptureError,
        match="prospective_claim_identity_collision",
    ):
        persist_prospective_capture(
            tmp_path,
            later,
            actor_id="tester",
            actor_role="researcher",
        )


def test_registry_hash_chain_detects_tampering(l7_fixture, tmp_path):
    report = build_capture(l7_fixture)
    status = persist_prospective_capture(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    path = Path(status["registry_path"])
    lines = path.read_text(encoding="utf-8").splitlines()
    event = json.loads(lines[0])
    event["claim"]["match"]["symbol"] = "TAMPER"
    lines[0] = json.dumps(event, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    registry = ProspectiveClaimRegistry(path)
    with pytest.raises(
        ProspectiveCaptureError,
        match="prospective_registry_entry_hash_invalid",
    ):
        registry.verify_integrity()


def test_capture_report_is_hash_protected(l7_fixture):
    report = build_capture(l7_fixture)
    tampered = deepcopy(report)
    tampered["counts"]["prospective_claim_count"] += 1
    with pytest.raises(
        ProspectiveCaptureError,
        match="capture_report_hash_mismatch",
    ):
        verify_capture_report(tampered)


def test_claim_has_no_productive_or_outcome_authority(l7_fixture):
    report = build_capture(l7_fixture)
    claim = report["claims"][0]
    serialized = json.dumps(claim, sort_keys=True)
    for forbidden_key in (
        '"universal_stance"',
        '"portfolio_action"',
        '"trade_decision"',
        '"order_instruction"',
        '"buy_signal"',
        '"sell_signal"',
        '"position_size"',
        '"target_weight"',
        '"outcome"',
        '"return"',
        '"price"',
        '"close"',
    ):
        assert forbidden_key not in serialized
    assert claim["confirmation_evaluation_performed"] is False
    assert claim["rating_assigned"] is False
    assert claim["promotion_performed"] is False
