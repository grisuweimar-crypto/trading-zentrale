from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest

from scanner.research.governance.qm_c import hypothesis_version_hash
from scanner.research.governance.qm_c_analysis_plan import analysis_plan_hash
from scanner.research.pattern_discovery import (
    FeatureLibrary,
    build_run_manifest,
    run_discovery_search,
)
from scanner.research.pattern_discovery.candidate_registry import (
    CandidateFreezeError,
    PatternCandidateRegistry,
    build_frozen_pattern_record,
    freeze_candidates,
    load_candidate_registry_contract,
    pattern_id_for_candidate,
    verify_freeze_snapshot,
    write_freeze_snapshot,
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
            "artifacts/research/l5_fixture.json",
            "configs/pattern_discovery/feature_library_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-l5-test-v1",
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
        "code_version": "commit-l5-test",
        "determinism": {
            "randomness_allowed": False,
            "seed": None,
        },
    }


def repo_fixture(tmp_path: Path):
    fixture = tmp_path / "artifacts/research/l5_fixture.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text('{"fixture":true}\n', encoding="utf-8")

    target_library = tmp_path / "configs/pattern_discovery/feature_library_v1.json"
    target_library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LIBRARY_PATH, target_library)
    return tmp_path


def observations():
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
                    "sector": "positive_sector" if positive else "negative_sector",
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


def bundle(tmp_path: Path):
    root = repo_fixture(tmp_path)
    manifest = build_run_manifest(preregistration(), repo_root=root)
    library = FeatureLibrary()
    rows = observations()
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
    return root, manifest, l3, l4


def test_l5_contract_keeps_qm_registry_writes_outside_lab():
    contract = load_candidate_registry_contract()
    assert contract["phase"] == "L5"
    assert contract["research_only"] is True
    assert contract["bindings"]["direct_qm_registry_write_forbidden_by_l0"] is True
    assert contract["bindings"]["l1_candidate_budget_is_hard_freeze_limit"] is True
    assert contract["qm_handoff"]["handoff_application_status"] == "READY_NOT_APPLIED_BY_L5"
    assert contract["boundaries"]["prospective_capture_started"] is False
    assert contract["boundaries"]["promotion_performed"] is False


def test_freeze_creates_immutable_pat_objects_with_masterplan_identity(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    snapshot = freeze_candidates(
        manifest,
        l3,
        l4,
        repo_root=root,
        freeze_timestamp="2026-10-06T15:05:00+00:00",
        qm_external_context=qm_context(),
        actor_id="tester",
        actor_role="researcher",
    )

    assert verify_freeze_snapshot(snapshot)["valid"] is True
    assert snapshot["frozen_pattern_count"] > 0
    assert snapshot["frozen_pattern_count"] <= 8

    record = snapshot["frozen_patterns"][0]
    assert record["pattern_id"].startswith("PAT-")
    assert record["pattern_version"] == "v1"
    assert len(record["pattern_spec_hash"]) == 64
    assert record["state"] == "FROZEN_CANDIDATE"
    assert record["freeze_timestamp"] == "2026-10-06T15:05:00Z"
    assert record["data_cutoff"] == manifest["preregistration"]["data_cutoff"]
    assert record["code_version"] == "commit-l5-test"
    assert record["universe_version"] == "universe-l5-test-v1"
    assert record["discovery_evidence"]
    assert record["confirmation_data_used"] is False
    assert record["prospective_capture_started"] is False
    assert record["rating"] is None

    spec = record["pattern_spec"]
    assert set(spec) == {
        "identity",
        "semantics",
        "forecast",
        "data",
        "statistics",
        "freeze",
    }
    assert spec["semantics"]["natural_language_description"]
    assert spec["forecast"]["target_id"] == "return_5t_gt_0"
    assert spec["forecast"]["horizon_sessions"] == 5
    assert spec["data"]["discovery_period"]["start"] == l3["observations"]["as_of_min"]
    assert spec["data"]["discovery_period"]["end"] == l3["observations"]["as_of_max"]


def test_freeze_only_uses_l4_eligible_discovery_shortlist_candidates(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    snapshot = freeze_candidates(
        manifest,
        l3,
        l4,
        repo_root=root,
        freeze_timestamp="2026-10-06T15:05:00Z",
        qm_external_context=qm_context(),
        actor_id="tester",
        actor_role="researcher",
    )
    source = {row["candidate_id"]: row for row in l4["candidate_evidence"]}
    for record in snapshot["frozen_patterns"]:
        candidate = source[record["candidate_id"]]
        assert candidate["l4_gate_status"] == "ELIGIBLE_FOR_L5"
        assert candidate["l3_shortlist_status"] == "DISCOVERY_SHORTLIST"

    eligible_shortlist = [
        row
        for row in l4["candidate_evidence"]
        if row["l4_gate_status"] == "ELIGIBLE_FOR_L5"
        and row["l3_shortlist_status"] == "DISCOVERY_SHORTLIST"
    ]
    assert snapshot["frozen_pattern_count"] == len(eligible_shortlist)


def test_nonshortlisted_candidate_cannot_be_frozen_directly(tmp_path):
    _, manifest, l3, l4 = bundle(tmp_path)
    candidate = next(
        row
        for row in l4["candidate_evidence"]
        if row["l4_gate_status"] == "ELIGIBLE_FOR_L5"
    )
    candidate = deepcopy(candidate)
    candidate["l3_shortlist_status"] = "NOT_SHORTLISTED"

    with pytest.raises(
        CandidateFreezeError,
        match="candidate_not_in_discovery_shortlist",
    ):
        build_frozen_pattern_record(
            candidate=candidate,
            manifest=manifest,
            l3_result=l3,
            l4_evidence=l4,
            freeze_timestamp="2026-10-06T15:05:00Z",
            qm_external_context=qm_context(),
        )


def test_pattern_identity_is_deterministic_and_qm_handoff_hashes_are_exact(tmp_path):
    _, manifest, l3, l4 = bundle(tmp_path)
    candidate = next(
        row
        for row in l4["candidate_evidence"]
        if row["l4_gate_status"] == "ELIGIBLE_FOR_L5"
        and row["l3_shortlist_status"] == "DISCOVERY_SHORTLIST"
    )
    first = build_frozen_pattern_record(
        candidate=candidate,
        manifest=manifest,
        l3_result=l3,
        l4_evidence=l4,
        freeze_timestamp="2026-10-06T15:05:00Z",
        qm_external_context=qm_context(),
    )
    second = build_frozen_pattern_record(
        candidate=deepcopy(candidate),
        manifest=manifest,
        l3_result=l3,
        l4_evidence=l4,
        freeze_timestamp="2026-10-06T15:05:00+00:00",
        qm_external_context=qm_context(),
    )
    assert first == second
    assert first["pattern_id"] == pattern_id_for_candidate(candidate)

    handoff = first["qm_c_handoff"]
    assert handoff["application_status"] == "READY_NOT_APPLIED_BY_L5"
    assert handoff["direct_registry_write_performed"] is False
    assert handoff["l0_write_boundary_preserved"] is True
    assert handoff["hypothesis_version_hash"] == hypothesis_version_hash(
        handoff["hypothesis_record"]
    )
    assert handoff["analysis_plan_hash"] == analysis_plan_hash(
        handoff["analysis_plan_record"]
    )
    assert handoff["qm_a_identity"]["hypothesis_version_hash"] == handoff[
        "hypothesis_version_hash"
    ]
    assert handoff["qm_a_identity"]["analysis_plan_hash"] == handoff[
        "analysis_plan_hash"
    ]


def test_l5_does_not_write_central_qm_registries(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    snapshot = freeze_candidates(
        manifest,
        l3,
        l4,
        repo_root=root,
        freeze_timestamp="2026-10-06T15:05:00Z",
        qm_external_context=qm_context(),
        actor_id="tester",
        actor_role="researcher",
    )
    assert snapshot["qm_c_handoff"]["direct_qm_registry_write_performed"] is False
    assert not (root / "artifacts/research/qm").exists()
    assert (root / "artifacts/research/pattern_discovery/pattern_registry.jsonl").exists()


def test_missing_qm_identity_context_fails_closed(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    with pytest.raises(
        CandidateFreezeError,
        match="qm_external_context_fields_missing:instrument_master_version",
    ):
        freeze_candidates(
            manifest,
            l3,
            l4,
            repo_root=root,
            freeze_timestamp="2026-10-06T15:05:00Z",
            qm_external_context={
                "universe_ledger_version": "ledger-v1",
                "environment_or_dependency_fingerprint": "env-v1",
            },
            actor_id="tester",
            actor_role="researcher",
        )


def test_freeze_timestamp_cannot_precede_discovery_cutoff(tmp_path):
    _, manifest, l3, l4 = bundle(tmp_path)
    candidate = next(
        row
        for row in l4["candidate_evidence"]
        if row["l4_gate_status"] == "ELIGIBLE_FOR_L5"
        and row["l3_shortlist_status"] == "DISCOVERY_SHORTLIST"
    )
    with pytest.raises(
        CandidateFreezeError,
        match="freeze_timestamp_before_data_cutoff",
    ):
        build_frozen_pattern_record(
            candidate=candidate,
            manifest=manifest,
            l3_result=l3,
            l4_evidence=l4,
            freeze_timestamp="2026-10-06T14:00:00Z",
            qm_external_context=qm_context(),
        )


def test_pattern_registry_is_append_only_and_successors_are_explicit(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry = PatternCandidateRegistry(path)
    base = {
        "pattern_id": "PAT-EXAMPLE",
        "pattern_version": "v1",
        "freeze_timestamp": "2026-10-06T15:05:00Z",
        "pattern_spec_hash": "a" * 64,
        "value": "original",
    }
    base["frozen_record_hash"] = __import__("hashlib").sha256(
        json.dumps(
            base,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    first = registry.register_frozen_pattern(
        record=base,
        actor_id="tester",
        actor_role="researcher",
    )
    assert first["event_type"] == "PATTERN_FROZEN"

    idem = registry.register_frozen_pattern(
        record=base,
        actor_id="tester",
        actor_role="researcher",
    )
    assert idem["idempotent"] is True

    changed = dict(base)
    changed["value"] = "changed-in-place"
    changed_body = dict(changed)
    changed_body.pop("frozen_record_hash", None)
    changed["frozen_record_hash"] = __import__("hashlib").sha256(
        json.dumps(
            changed_body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    with pytest.raises(
        CandidateFreezeError,
        match="pattern_version_already_frozen",
    ):
        registry.register_frozen_pattern(
            record=changed,
            actor_id="tester",
            actor_role="researcher",
        )

    successor = dict(base)
    successor["pattern_version"] = "v2"
    successor["pattern_spec_hash"] = "b" * 64
    successor["value"] = "successor"
    successor_body = dict(successor)
    successor_body.pop("frozen_record_hash", None)
    successor["frozen_record_hash"] = __import__("hashlib").sha256(
        json.dumps(
            successor_body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    with pytest.raises(
        CandidateFreezeError,
        match="pattern_successor_requires_supersedes_reference",
    ):
        registry.register_frozen_pattern(
            record=successor,
            actor_id="tester",
            actor_role="researcher",
        )

    registry.register_frozen_pattern(
        record=successor,
        actor_id="tester",
        actor_role="researcher",
        supersedes_pattern_version="v1",
    )
    assert registry.verify_integrity()["pattern_version_count"] == 2


def test_tampered_l4_evidence_fails_before_freeze(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    tampered = deepcopy(l4)
    tampered["counts"]["eligible_for_l5"] += 1

    with pytest.raises(Exception, match="l4_evidence_hash_mismatch"):
        freeze_candidates(
            manifest,
            l3,
            tampered,
            repo_root=root,
            freeze_timestamp="2026-10-06T15:05:00Z",
            qm_external_context=qm_context(),
            actor_id="tester",
            actor_role="researcher",
        )


def test_freeze_snapshot_is_hash_protected_and_write_once(tmp_path):
    root, manifest, l3, l4 = bundle(tmp_path)
    snapshot = freeze_candidates(
        manifest,
        l3,
        l4,
        repo_root=root,
        freeze_timestamp="2026-10-06T15:05:00Z",
        qm_external_context=qm_context(),
        actor_id="tester",
        actor_role="researcher",
    )
    target = write_freeze_snapshot(root, snapshot)
    assert target.exists()
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert verify_freeze_snapshot(saved)["snapshot_hash"] == snapshot["snapshot_hash"]

    with pytest.raises(
        CandidateFreezeError,
        match="l5_snapshot_already_exists",
    ):
        write_freeze_snapshot(root, snapshot)
