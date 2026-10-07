from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from hashlib import sha256
import csv
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import (
    HypothesisRegistry,
    hypothesis_version_hash,
)
from scanner.research.governance.qm_c_analysis_plan import (
    AnalysisPlanRegistry,
    analysis_plan_hash,
)
from scanner.research.governance.qm_c_families_multiplicity import (
    FamilyMultiplicityRegistry,
)
from scanner.research.governance.qm_c_sequential_monitoring import (
    SequentialMonitoringRegistry,
)
from scanner.research.pattern_discovery.confirmation_engine import (
    ConfirmationEngineError,
    ConfirmationLookRegistry,
    build_baseline_bundle,
    build_confirmation_look,
    build_context_bundle,
    build_context_bundle_from_sources,
    canonical_baseline_price_state,
    load_confirmation_contract,
    persist_confirmation_look,
    verify_confirmation_look,
)
from scanner.research.pattern_discovery.confirmation_sources import (
    ConfirmationSourceError,
    build_prospective_source_bundle,
)
from scanner.research.pattern_discovery.outcome_maturation import (
    _record_id,
    load_outcome_maturation_contract,
    maturation_registry_repo_path,
)


ROOT = Path(__file__).resolve().parents[2]
QM_B_CLOSURE = ROOT / "configs/qm_b_closure_v1.json"


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


def qm_b_closure():
    return json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))


def registries(tmp_path):
    return {
        "repo_root": tmp_path,
        "hypotheses": HypothesisRegistry(tmp_path / "qm_c1.jsonl"),
        "plans": AnalysisPlanRegistry(tmp_path / "qm_c2.jsonl"),
        "qm_a": GovernanceLedger(tmp_path / "qm_a.jsonl"),
        "c3": FamilyMultiplicityRegistry(tmp_path / "qm_c3.jsonl"),
        "c4": SequentialMonitoringRegistry(tmp_path / "qm_c4.jsonl"),
    }


def make_pattern(
    *,
    pattern_id="PAT-L9-A",
    family_id="FAM-L9",
    freeze_timestamp="2026-10-06T15:05:00Z",
    minimum_raw_n=20,
    minimum_support=3,
    minimum_effect=0.01,
    minimum_lift=0.10,
):
    hypothesis_id = f"H-{pattern_id}"
    plan_id = f"AP-{pattern_id}"
    qm_a_id = f"A-{pattern_id}"
    hypothesis_record = {
        "hypothesis_id": hypothesis_id,
        "hypothesis_version": "v1",
        "research_question": f"Does {pattern_id} retain its prospective effect?",
        "hypothesis_statement": f"{pattern_id} has positive return_5t_gt_0 versus its frozen baseline.",
        "hypothesis_family_id": f"HF-{family_id}",
        "research_mode": "CONFIRMATION",
        "qm_a_analysis_id": qm_a_id,
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
    }
    hypothesis_hash = hypothesis_version_hash(hypothesis_record)
    plan_record = {
        "analysis_plan_id": plan_id,
        "analysis_plan_version": "v1",
        "hypothesis_id": hypothesis_id,
        "hypothesis_version": "v1",
        "hypothesis_version_hash": hypothesis_hash,
        "research_mode": "CONFIRMATION",
        "primary_estimand": f"Prospective probability advantage and aligned outcome of {pattern_id}.",
        "primary_metrics": [
            "direction_probability",
            "probability_advantage_lift",
            "mean_aligned_outcome",
        ],
        "population_definition": f"Strictly post-freeze PIT observations matching {pattern_id}.",
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
        "evaluation_windows": ["5_sessions"],
        "exclusion_rules": [
            "pre_freeze_observation",
            "missing_or_immature_outcome",
            "pattern_version_mismatch",
        ],
        "outcome_definitions": [
            "return_5t_gt_0 with expected_direction=POSITIVE and baseline=same_horizon_unconditional_return_baseline"
        ],
        "planned_sensitivity_analyses": [
            "moving_block_bootstrap",
            "symbol_concentration",
            "temporal_support",
        ],
    }
    plan_hash = analysis_plan_hash(plan_record)
    freeze_context = {
        "code_or_commit_hash": "l9-test-code",
        "config_hash": "l9-test-config",
        "dataset_snapshot_hash": "l9-test-dataset",
        "universe_ledger_version": "l9-test-universe-ledger",
        "instrument_master_version": "l9-test-instrument-master",
        "label_definition_hash": digest(
            {
                "target_id": "return_5t_gt_0",
                "expected_direction": "POSITIVE",
                "horizon_sessions": 5,
            }
        ),
        "benchmark_definition_hash": digest(
            {
                "pattern_type": "DIRECTIONAL",
                "baseline": "same_horizon_unconditional_return_baseline",
            }
        ),
        "environment_or_dependency_fingerprint": "python311-pytest",
        "evaluation_cohort_id": f"COHORT-{pattern_id}-v1-POSTFREEZE",
        "temporal_boundary": freeze_timestamp,
    }
    qm_a_identity = {
        "hypothesis_version_hash": hypothesis_hash,
        "analysis_plan_hash": plan_hash,
        **freeze_context,
    }
    pattern_spec = {
        "identity": {
            "pattern_id": pattern_id,
            "pattern_version": "v1",
            "discovery_run_id": "DISC-L9-TEST",
            "candidate_id": f"CAND-{pattern_id}",
            "candidate_spec_hash": digest({"candidate": pattern_id}),
        },
        "semantics": {
            "pattern_type": "DIRECTIONAL",
            "natural_language_description": f"Fixture {pattern_id}",
            "conditions": [],
            "feature_versions": [],
            "transformation_rules": [],
        },
        "forecast": {
            "target_id": "return_5t_gt_0",
            "expected_direction": "POSITIVE",
            "horizon_sessions": 5,
            "baseline": "same_horizon_unconditional_return_baseline",
            "reference_definition": "same_horizon_unconditional_return_baseline",
        },
        "data": {
            "universe_version": "universe-l9-test",
            "feature_library_version": "PDL-FEATURE-LIBRARY-v1",
            "discovery_period": {
                "start": "2026-06-01T00:00:00Z",
                "end": "2026-10-01T00:00:00Z",
                "data_cutoff": "2026-10-01T00:00:00Z",
            },
            "pit_rules": ["no_future_features"],
            "minimum_coverage_rule": "L4_MATURE_TARGET_COVERAGE_GATE",
        },
        "statistics": {
            "primary_method": "cluster_aware_effect_and_probability_estimation",
            "multiple_testing": {
                "primary_method": "BENJAMINI_HOCHBERG_FDR",
                "parameters": {"fdr_q": 0.05},
            },
            "minimum_criteria": {
                "minimum_raw_n": minimum_raw_n,
                "minimum_temporal_support_regions": minimum_support,
                "minimum_effect_size": minimum_effect,
                "minimum_baseline_lift": minimum_lift,
            },
            "robustness_checks": [
                "moving_block_bootstrap",
                "symbol_concentration",
                "temporal_support",
            ],
            "discovery_evidence_hash": digest({"l4": pattern_id}),
        },
        "freeze": {
            "freeze_timestamp": freeze_timestamp,
            "code_version": "l9-test-code",
            "l1_manifest_hash": digest({"l1": pattern_id}),
            "l3_result_hash": digest({"l3": pattern_id}),
            "l4_evidence_hash": digest({"l4e": pattern_id}),
        },
    }
    spec_hash = digest(pattern_spec)
    handoff = {
        "application_status": "READY_NOT_APPLIED_BY_L5",
        "direct_registry_write_performed": False,
        "l0_write_boundary_preserved": True,
        "application_order": [],
        "hypothesis_record": hypothesis_record,
        "hypothesis_version_hash": hypothesis_hash,
        "requested_hypothesis_target_state": "FROZEN_FOR_CONFIRMATION",
        "analysis_plan_record": plan_record,
        "analysis_plan_hash": plan_hash,
        "freeze_context": freeze_context,
        "freeze_context_hash": digest(freeze_context),
        "requested_analysis_plan_target_state": "FROZEN_FOR_CONFIRMATION",
        "qm_a_analysis_id": qm_a_id,
        "qm_a_identity": qm_a_identity,
        "qm_a_state_after_l5": "NOT_REGISTERED_BY_L5",
        "pattern_spec_hash": spec_hash,
        "pattern_description": f"Fixture {pattern_id}",
    }
    handoff["handoff_hash"] = digest(handoff)
    frozen = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": pattern_id,
        "pattern_version": "v1",
        "pattern_spec_hash": spec_hash,
        "natural_language_description": f"Fixture {pattern_id}",
        "pattern_spec": pattern_spec,
        "freeze_timestamp": freeze_timestamp,
        "data_cutoff": "2026-10-01T00:00:00Z",
        "code_version": "l9-test-code",
        "universe_version": "universe-l9-test",
        "feature_library_version": "PDL-FEATURE-LIBRARY-v1",
        "candidate_id": f"CAND-{pattern_id}",
        "discovery_run_id": "DISC-L9-TEST",
        "l4_evidence_hash": digest({"l4e": pattern_id}),
        "discovery_evidence": {
            "raw_n": 999,
            "direction_probability": 0.99,
            "probability_advantage_lift": 0.49,
        },
        "qm_c_handoff": handoff,
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    frozen["frozen_record_hash"] = digest(frozen)
    return frozen


def apply_l5_handoff(pattern, regs, *, qm_a_version="analysis-v1"):
    handoff = pattern["qm_c_handoff"]
    hypothesis_record = handoff["hypothesis_record"]
    plan_record = handoff["analysis_plan_record"]

    regs["hypotheses"].register_hypothesis(
        record=hypothesis_record,
        actor_id="tester",
        actor_role="researcher",
    )
    regs["plans"].register_plan(
        record=plan_record,
        hypothesis_registry=regs["hypotheses"],
        actor_id="tester",
        actor_role="researcher",
    )
    regs["plans"].freeze_plan(
        analysis_plan_id=plan_record["analysis_plan_id"],
        analysis_plan_version=plan_record["analysis_plan_version"],
        freeze_context=handoff["freeze_context"],
        hypothesis_registry=regs["hypotheses"],
        qm_b_closure=qm_b_closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze Pattern L9 fixture analysis plan.",
    )
    regs["qm_a"].register_analysis(
        analysis_id=handoff["qm_a_analysis_id"],
        version_id=qm_a_version,
        actor_id="tester",
        actor_role="researcher",
    )
    regs["qm_a"].transition(
        analysis_id=handoff["qm_a_analysis_id"],
        version_id=qm_a_version,
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze exact L9 fixture analysis identity.",
        analysis_identity=regs["plans"].build_qm_a_identity(
            plan_record["analysis_plan_id"],
            plan_record["analysis_plan_version"],
        ),
    )
    regs["hypotheses"].transition(
        hypothesis_id=hypothesis_record["hypothesis_id"],
        hypothesis_version=hypothesis_record["hypothesis_version"],
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze Pattern hypothesis.",
    )
    return {
        "hypothesis_id": hypothesis_record["hypothesis_id"],
        "hypothesis_version": hypothesis_record["hypothesis_version"],
        "hypothesis_version_hash": handoff["hypothesis_version_hash"],
        "analysis_plan_id": plan_record["analysis_plan_id"],
        "analysis_plan_version": plan_record["analysis_plan_version"],
        "analysis_plan_hash": handoff["analysis_plan_hash"],
        "qm_a_analysis_id": handoff["qm_a_analysis_id"],
        "qm_a_version_id": qm_a_version,
    }


def freeze_family(
    regs,
    members,
    *,
    family_id="HF-FAM-L9",
    strategy="PREDECLARED_SINGLE_PRIMARY",
    parameters=None,
    planned_looks=None,
    early_stop_allowed=False,
):
    control_record = {
        "control_plan_id": f"CP-{family_id}",
        "control_plan_version": "v1",
        "hypothesis_family_id": family_id,
        "research_mode": "CONFIRMATION",
        "family_members": members,
        "multiplicity_strategy": strategy,
        "multiplicity_parameters": parameters or {},
    }
    regs["c3"].register_control_plan(
        record=control_record,
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        actor_id="tester",
        actor_role="researcher",
    )
    regs["c3"].freeze_control_plan(
        control_plan_id=control_record["control_plan_id"],
        control_plan_version="v1",
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        qm_a_ledger=regs["qm_a"],
        qm_b_closure=qm_b_closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze L9 test family.",
    )
    control = regs["c3"].get_control_plan(
        control_record["control_plan_id"],
        "v1",
    )

    looks = planned_looks or [
        {"look_id": "FINAL", "information_fraction": 1.0}
    ]
    monitoring_record = {
        "monitoring_plan_id": f"MP-{family_id}",
        "monitoring_plan_version": "v1",
        "control_plan_id": control["control_plan_id"],
        "control_plan_version": control["control_plan_version"],
        "control_plan_hash": control["control_plan_hash"],
        "mode": (
            "FIXED_HORIZON_NO_INTERIM"
            if len(looks) == 1
            else "PREDECLARED_LOOKS"
        ),
        "planned_looks": looks,
        "early_stop_allowed": early_stop_allowed,
        "stopping_rule": (
            "FINAL_ONLY"
            if len(looks) == 1
            else "CONTINUE_UNLESS_PREDECLARED_BOUNDARY"
        ),
    }
    regs["c4"].register_monitoring_plan(
        record=monitoring_record,
        control_registry=regs["c3"],
        actor_id="tester",
        actor_role="researcher",
    )
    regs["c4"].freeze_monitoring_plan(
        monitoring_plan_id=monitoring_record["monitoring_plan_id"],
        monitoring_plan_version="v1",
        control_registry=regs["c3"],
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze L9 test sequential schedule.",
    )
    monitor = regs["c4"].get_monitoring_plan(
        monitoring_record["monitoring_plan_id"],
        "v1",
    )
    return control, monitor


def matured_outcome(
    pattern,
    *,
    claim_id,
    symbol,
    start_day,
    target_value,
):
    l8_contract = load_outcome_maturation_contract()
    horizon = 5
    start = date.fromisoformat(start_day)
    dates = [
        (start + timedelta(days=offset)).isoformat()
        for offset in range(horizon + 1)
    ]
    record = {
        "schema_version": "pattern_discovery_l8_matured_outcome_v1",
        "state": "MATURED_OUTCOME",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "record_id": _record_id(claim_id, contract=l8_contract),
        "claim": {
            "claim_id": claim_id,
            "event_id": f"PEV-{claim_id}",
            "claim_hash": digest({"claim": claim_id}),
            "pattern_id": pattern["pattern_id"],
            "pattern_version": pattern["pattern_version"],
            "pattern_spec_hash": pattern["pattern_spec_hash"],
            "symbol": symbol,
            "capture_snapshot_id": "snap-l9-test",
            "capture_snapshot_binding_hash": digest({"snapshot": "l9"}),
        },
        "target": {
            "pattern_type": "DIRECTIONAL",
            "target_id": "return_5t_gt_0",
            "expected_direction": "POSITIVE",
            "horizon_sessions": 5,
            "baseline": "same_horizon_unconditional_return_baseline",
            "reference_definition": "same_horizon_unconditional_return_baseline",
        },
        "horizon_provenance": {
            "start_market_session_id": f"{symbol}-{start_day}",
            "calendar_id": f"{symbol}-CAL",
            "session_source": "fixture-session-source-v1",
            "start_at": f"{start_day}T08:00:00Z",
            "start_session_date": dates[0],
            "target_session_date": dates[-1],
            "horizon_sessions": 5,
            "observed_session_count_including_start": 6,
            "session_dates": dates,
        },
        "price_provenance": {
            "price_kind": "ADJUSTED_CLOSE",
            "currency": "USD",
            "currency_conversion_performed": False,
            "subject_path_hash": digest(
                {"claim": claim_id, "path": dates, "value": target_value}
            ),
        },
        "reference": {
            "status": "AVAILABLE",
            "method_id": "LEAVE_ONE_SYMBOL_OUT_PEER_MEDIAN_V1",
            "scope": "SAME_CURRENCY",
            "peer_snapshot_id": "snap-l9-test",
            "peer_snapshot_binding_hash": digest({"peer": claim_id}),
            "peer_return": 0.0,
            "peer_count": 3,
            "same_currency_peer_count": 3,
            "global_peer_count": 3,
            "eligible_peer_count": 3,
            "excluded_peer_count": 0,
            "excluded_peer_status_counts": {},
            "peer_details_hash": digest({"peers": claim_id}),
            "chosen_peer_set_hash": digest({"chosen": claim_id}),
            "reason_codes": ["LEAVE_ONE_OUT_SAME_CURRENCY_MEDIAN"],
        },
        "outcome": {
            "return": target_value,
            "peer_excess": None,
            "adverse_excursion": min(0.0, target_value),
            "path_max_drawdown": min(0.0, target_value / 2),
            "target_value": target_value,
            "target_value_kind": "RETURN",
        },
        "boundaries": {
            "direction_hit_computed": False,
            "probability_computed": False,
            "confirmation_evaluation_performed": False,
            "sequential_monitoring_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    record["outcome_hash"] = digest(record)
    return record


def prospective_rows(pattern, *, count=24, target_value=0.08, prefix="A"):
    rows = []
    base = date(2026, 10, 8)
    symbols = ("AAA", "BBB", "CCC", "DDD")
    for index in range(count):
        day = base + timedelta(days=index * 2)
        rows.append(
            matured_outcome(
                pattern,
                claim_id=f"{prefix}-{index:03d}",
                symbol=symbols[index % len(symbols)],
                start_day=day.isoformat(),
                target_value=target_value,
            )
        )
    return rows


def maturation_registry_events(
    outcomes,
    *,
    recorded_at="2027-01-15T20:00:00Z",
    prior_events=None,
):
    events = [deepcopy(event) for event in (prior_events or [])]
    previous = events[-1]["entry_hash"] if events else None
    start_sequence = len(events) + 1
    for sequence, record in enumerate(outcomes, start=start_sequence):
        event = {
            "schema_version": "pattern_discovery_l8_maturation_event_v1",
            "sequence": sequence,
            "event_id": (
                "PME-"
                + digest(
                    {
                        "claim_id": record["claim"]["claim_id"],
                        "outcome_hash": record["outcome_hash"],
                    }
                )[:24].upper()
            ),
            "event_type": "OUTCOME_MATURED",
            "recorded_at": recorded_at,
            "actor_id": "tester",
            "actor_role": "researcher",
            "record": record,
            "previous_event_hash": previous,
        }
        event["entry_hash"] = digest(event)
        events.append(event)
        previous = event["entry_hash"]
    return events


def fixture_start_session_binding(
    snapshot_id,
    session_map_hash,
    sessions,
    *,
    session_date_overrides=None,
):
    overrides = session_date_overrides or {}
    normalized = []
    for session in sessions:
        symbol = session["symbol"]
        session_date = overrides.get(symbol, session["start_at"][:10])
        normalized.append(
            {
                "symbol": symbol,
                "session_id": session["session_id"],
                "calendar_id": session["calendar_id"],
                "session_date": session_date,
                "start_at": session["start_at"],
                "source": session["source"],
            }
        )
    normalized.sort(key=lambda item: item["symbol"])
    binding_body = {
        "start_session_binding_file_sha256": digest(
            {
                "fixture": "l9-start-session-file",
                "snapshot_id": snapshot_id,
                "sessions": normalized,
            }
        ),
        "l7_session_map_hash": session_map_hash,
        "row_count": len(normalized),
        "symbol_count": len({row["symbol"] for row in normalized}),
        "sessions_hash": digest(normalized),
    }
    return {
        "capture_snapshot_id": snapshot_id,
        "start_session_binding": {
            **binding_body,
            "start_session_binding_hash": digest(binding_body),
        },
        "sessions": normalized,
    }


def prospective_source_for(
    patterns,
    outcomes,
    *,
    include_regime=True,
    session_date_shift_days=0,
):
    """Create hash-consistent synthetic L7 capture proofs for L9 tests."""
    patterns_by_id = {
        str(pattern["pattern_id"]): pattern
        for pattern in patterns
    }
    values = deepcopy(outcomes)
    capture_sources = []
    symbols = ("AAA", "BBB", "CCC", "DDD")
    capture_times = []

    for index, outcome in enumerate(values):
        claim_stub = outcome["claim"]
        pattern = patterns_by_id[str(claim_stub["pattern_id"])]
        claim_id = str(claim_stub["claim_id"])
        event_id = str(claim_stub["event_id"])
        symbol = str(claim_stub["symbol"])
        start_at = outcome["horizon_provenance"]["start_at"]
        start_day = date.fromisoformat(start_at[:10])
        observation_day = start_day - timedelta(days=1)
        observation_as_of = f"{observation_day.isoformat()}T12:00:00Z"
        generated_at = f"{observation_day.isoformat()}T18:00:00Z"
        captured_at = f"{observation_day.isoformat()}T18:10:00Z"
        capture_times.append(captured_at)
        snapshot_id = f"SNAP-{claim_id}"

        rows = []
        for rank, row_symbol in enumerate(symbols):
            row = {
                "symbol": row_symbol,
                "as_of": observation_as_of,
                "generated_at": generated_at,
                "snapshot_id": snapshot_id,
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
                "score": 40.0 + rank,
                "sector": "TECH" if rank % 2 == 0 else "INDUSTRIAL",
                "pillar_primary": "QUALITY",
                "cluster_official": "C1" if rank % 2 == 0 else "C2",
            }
            if include_regime:
                row["market_regime_stock"] = (
                    "BULL" if index % 2 == 0 else "NEUTRAL"
                )
                row["market_regime_crypto"] = "N/A"
            rows.append(row)

        row_identities = []
        row_hash_by_symbol = {}
        for row in sorted(rows, key=lambda value: value["symbol"]):
            row_hash = digest(row)
            row_hash_by_symbol[row["symbol"]] = row_hash
            row_identities.append(
                {
                    "symbol": row["symbol"],
                    "as_of": row["as_of"],
                    "snapshot_id": row["snapshot_id"],
                    "row_hash": row_hash,
                }
            )
        projected_rows_hash = digest(row_identities)

        sessions = []
        for row_symbol in symbols:
            sessions.append(
                {
                    "symbol": row_symbol,
                    "session_id": f"{snapshot_id}-{row_symbol}",
                    "calendar_id": f"{row_symbol}-CAL",
                    "start_at": start_at,
                    "source": "l9-test-session-map-v1",
                }
            )
        sessions.sort(key=lambda value: value["symbol"])
        session_map_hash = digest(sessions)
        session_date_overrides = {
            item["symbol"]: (
                date.fromisoformat(item["start_at"][:10])
                + timedelta(days=session_date_shift_days)
            ).isoformat()
            for item in sessions
        }
        fixture_session_binding = fixture_start_session_binding(
            snapshot_id,
            session_map_hash,
            sessions,
            session_date_overrides=session_date_overrides,
        )

        binding_body = {
            "snapshot_id": snapshot_id,
            "snapshot_as_of": observation_as_of,
            "snapshot_generated_at": generated_at,
            "snapshot_metadata_schema_version": "research_views_v1",
            "snapshot_source_file": "fixture://l9",
            "snapshot_source_sha256": digest(
                {"snapshot_source": snapshot_id}
            ),
            "snapshot_file_sha256": digest(
                {"snapshot_file": snapshot_id}
            ),
            "row_count": len(rows),
            "symbol_count": len(rows),
            "projected_rows_hash": projected_rows_hash,
        }
        snapshot_binding_hash = digest(binding_body)
        snapshot_binding = {
            **binding_body,
            "snapshot_binding_hash": snapshot_binding_hash,
        }
        history_binding_hash = digest(
            {"history_before": snapshot_id}
        )

        forecast = pattern["pattern_spec"]["forecast"]
        l7_claim = {
            "schema_version": "pattern_discovery_l7_prospective_claim_v1",
            "research_only": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "capture_state": "CAPTURED_UNMATURED",
            "claim_id": claim_id,
            "event_id": event_id,
            "captured_at": captured_at,
            "pattern": {
                "pattern_id": pattern["pattern_id"],
                "pattern_version": pattern["pattern_version"],
                "pattern_spec_hash": pattern["pattern_spec_hash"],
                "frozen_record_hash": pattern["frozen_record_hash"],
                "freeze_timestamp": pattern["freeze_timestamp"],
                "discovery_run_id": pattern["discovery_run_id"],
            },
            "governance": {
                "fixture": "l9-source-proof",
            },
            "match": {
                "symbol": symbol,
                "observation_as_of": observation_as_of,
                "snapshot_id": snapshot_id,
                "snapshot_generated_at": generated_at,
                "snapshot_binding_hash": snapshot_binding_hash,
                "history_binding_hash": history_binding_hash,
                "session_map_hash": session_map_hash,
                "current_row_hash": row_hash_by_symbol[symbol],
                "condition_evidence": [],
                "condition_evidence_hash": digest([]),
            },
            "forecast": {
                "target_id": forecast["target_id"],
                "expected_direction": forecast["expected_direction"],
                "horizon_sessions": forecast["horizon_sessions"],
                "baseline": forecast["baseline"],
                "reference_definition": forecast["reference_definition"],
            },
            "start_market_session": {
                "session_id": f"{snapshot_id}-{symbol}",
                "calendar_id": f"{symbol}-CAL",
                "start_at": start_at,
                "source": "l9-test-session-map-v1",
            },
            "outcome_available_at_capture": False,
            "outcome_maturation_performed": False,
            "confirmation_evaluation_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
        }
        l7_claim["claim_hash"] = digest(l7_claim)

        report = {
            "schema_version": "pattern_discovery_l7_capture_report_v1",
            "module": "pattern_discovery_lab",
            "phase": "L7",
            "research_only": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "capture_id": f"PCAP-{digest({'claim_id': claim_id})[:24].upper()}",
            "captured_at": captured_at,
            "l7_contract_hash": digest({"l7_contract": "fixture"}),
            "l5_snapshot_hashes": [digest({"l5": pattern["pattern_id"]})],
            "snapshot_binding": snapshot_binding,
            "history_binding": {
                "history_file_sha256": digest(
                    {"history_file": snapshot_id}
                ),
                "row_count": 0,
                "symbol_count": 0,
                "projected_rows_hash": digest([]),
                "max_generated_at": None,
                "history_binding_hash": history_binding_hash,
            },
            "session_map_hash": session_map_hash,
            "counts": {
                "frozen_pattern_count": 1,
                "post_freeze_pattern_count": 1,
                "current_symbol_count": len(rows),
                "prospective_claim_count": 1,
                "excluded_item_count": 0,
            },
            "pattern_summaries": [],
            "exclusions": [],
            "claims": [l7_claim],
            "boundaries": {
                "outcome_information_used": False,
                "outcome_maturation_performed": False,
                "confirmation_evaluation_performed": False,
                "sequential_monitoring_performed": False,
                "rating_assigned": False,
                "promotion_performed": False,
                "decision_layer_integration_performed": False,
            },
        }
        report["capture_hash"] = digest(report)
        capture_sources.append(
            {
                "capture_report": report,
                "snapshot_rows": rows,
                "market_sessions": sessions,
            }
        )

        claim_stub.update(
            {
                "claim_hash": l7_claim["claim_hash"],
                "event_id": event_id,
                "capture_snapshot_id": snapshot_id,
                "capture_snapshot_binding_hash": snapshot_binding_hash,
            }
        )
        outcome["horizon_provenance"]["start_market_session_id"] = (
            f"{snapshot_id}-{symbol}"
        )
        outcome["horizon_provenance"]["start_session_binding_hash"] = (
            fixture_session_binding["start_session_binding"][
                "start_session_binding_hash"
            ]
        )
        if session_date_shift_days:
            bound_session = next(
                item
                for item in fixture_session_binding["sessions"]
                if item["symbol"] == symbol
            )
            explicit_start = date.fromisoformat(
                bound_session["session_date"]
            )
            shifted_dates = [
                (explicit_start + timedelta(days=offset)).isoformat()
                for offset in range(
                    int(outcome["target"]["horizon_sessions"]) + 1
                )
            ]
            outcome["horizon_provenance"]["start_session_date"] = shifted_dates[0]
            outcome["horizon_provenance"]["target_session_date"] = shifted_dates[-1]
            outcome["horizon_provenance"]["session_dates"] = shifted_dates
            outcome["horizon_provenance"][
                "observed_session_count_including_start"
            ] = len(shifted_dates)
        outcome["outcome_hash"] = digest(
            {k: v for k, v in outcome.items() if k != "outcome_hash"}
        )

    bundle = build_prospective_source_bundle(
        capture_sources,
        source_bundle_id=(
            "SRC-"
            + digest(
                {
                    "captures": [
                        source["capture_report"]["capture_id"]
                        for source in capture_sources
                    ]
                }
            )[:24].upper()
        ),
        generated_at=max(capture_times),
    )
    return bundle, values


def write_authoritative_maturation_registry(repo_root, events):
    target = Path(repo_root) / maturation_registry_repo_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "".join(canonical(event) + "\n" for event in events),
        encoding="utf-8",
    )
    return target


def write_canonical_price_fixture(
    repo_root,
    prospective_source_bundle,
    *,
    horizon=5,
):
    target = Path(repo_root) / "artifacts/research/price_backfill.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = {}
    for snapshot in prospective_source_bundle["snapshots"]:
        for session in snapshot["market_sessions"]:
            symbol = session["symbol"]
            start_day = date.fromisoformat(session["start_at"][:10])
            for offset in range(horizon + 1):
                day = (start_day + timedelta(days=offset)).isoformat()
                rows[(symbol, day)] = {
                    "date": day,
                    "symbol": symbol,
                    "currency": "USD",
                    "open": "100",
                    "high": "100",
                    "low": "100",
                    "close": "100",
                    "adj_close": "100",
                    "volume": "1000",
                    "source": "l9-test-canonical-price",
                    "retrieved_at": "2027-01-14T20:00:00Z",
                    "observation_type": "observed_price",
                }
    fields = [
        "date",
        "symbol",
        "currency",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source",
        "retrieved_at",
        "observation_type",
    ]
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key in sorted(rows):
            writer.writerow(rows[key])
    return target


def canonical_price_state_for_test(
    repo_root,
    prospective_source_bundle,
    *,
    evaluated_at="2027-01-15T20:00:00Z",
    horizon=5,
):
    write_canonical_price_fixture(
        repo_root,
        prospective_source_bundle,
        horizon=horizon,
    )
    return canonical_baseline_price_state(
        repo_root,
        evaluated_at,
    )


def baseline_record(
    pattern,
    prospective_source_bundle,
    repo_root,
    *,
    evaluated_at="2027-01-15T20:00:00Z",
):
    forecast = pattern["pattern_spec"]["forecast"]
    price_groups, _ = canonical_price_state_for_test(
        repo_root,
        prospective_source_bundle,
        evaluated_at=evaluated_at,
        horizon=int(forecast["horizon_sessions"]),
    )
    observations = []
    snapshot_session_bindings = []
    counter = 0
    eligible_snapshot_ids = {
        str(claim["snapshot_id"])
        for claim in prospective_source_bundle["claims"]
        if claim["pattern_id"] == pattern["pattern_id"]
        and claim["pattern_version"] == pattern["pattern_version"]
        and claim["pattern_spec_hash"] == pattern["pattern_spec_hash"]
    }
    for snapshot in prospective_source_bundle["snapshots"]:
        if snapshot["snapshot_id"] not in eligible_snapshot_ids:
            continue
        fixture_binding = fixture_start_session_binding(
            snapshot["snapshot_id"],
            snapshot["session_map_hash"],
            snapshot["market_sessions"],
        )
        snapshot_session_bindings.append(fixture_binding)
        sessions = {
            item["symbol"]: item
            for item in fixture_binding["sessions"]
        }
        for row in snapshot["rows"]:
            symbol = row["symbol"]
            session = sessions.get(symbol)
            event = {
                "baseline_event_id": (
                    f"B-{pattern['pattern_id']}-{counter:04d}"
                ),
                "capture_snapshot_id": snapshot["snapshot_id"],
                "symbol": symbol,
                "session_id": None,
                "calendar_id": None,
                "session_source": None,
                "start_at": None,
                "start_session_date": None,
                "session_date_binding_hash": None,
                "target_session_date": None,
                "session_dates": [],
                "currency": None,
                "price_path_hash": None,
                "start_adjusted_close": None,
                "target_adjusted_close": None,
                "availability_status": "START_SESSION_UNAVAILABLE",
                "target_value": None,
                "target_id": forecast["target_id"],
                "horizon_sessions": forecast["horizon_sessions"],
            }
            if session is not None:
                start_session_date = session["session_date"]
                session_binding_body = {
                    "capture_snapshot_id": snapshot["snapshot_id"],
                    "symbol": symbol,
                    "session_id": session["session_id"],
                    "calendar_id": session["calendar_id"],
                    "start_at": session["start_at"],
                    "session_source": session["source"],
                    "start_session_date": start_session_date,
                }
                event.update(
                    {
                        "session_id": session["session_id"],
                        "calendar_id": session["calendar_id"],
                        "session_source": session["source"],
                        "start_at": session["start_at"],
                        "start_session_date": start_session_date,
                        "session_date_binding_hash": digest(
                            session_binding_body
                        ),
                    }
                )
                series = list(price_groups.get(symbol, ()))
                positions = {
                    str(value["date"]): index
                    for index, value in enumerate(series)
                }
                start_index = positions.get(start_session_date)
                horizon = int(forecast["horizon_sessions"])
                if (
                    start_index is not None
                    and start_index + horizon < len(series)
                ):
                    path = series[start_index : start_index + horizon + 1]
                    values = [float(value["adj_close"]) for value in path]
                    target_value = values[-1] / values[0] - 1.0
                    path_hash = digest(
                        [
                            {
                                "symbol": value["symbol"],
                                "date": value["date"],
                                "currency": value["currency"],
                                "adj_close": value["adj_close"],
                            }
                            for value in path
                        ]
                    )
                    event.update(
                        {
                            "availability_status": "AVAILABLE",
                            "target_session_date": path[-1]["date"],
                            "session_dates": [
                                value["date"] for value in path
                            ],
                            "currency": path[0]["currency"],
                            "price_path_hash": path_hash,
                            "start_adjusted_close": values[0],
                            "target_adjusted_close": values[-1],
                            "target_value": target_value,
                        }
                    )
                else:
                    event["availability_status"] = "MISSING_OUTCOME"
            event["source_hash"] = digest(event)
            observations.append(event)
            counter += 1

    return {
        "pattern_id": pattern["pattern_id"],
        "pattern_version": pattern["pattern_version"],
        "pattern_spec_hash": pattern["pattern_spec_hash"],
        "baseline_definition": forecast["baseline"],
        "universe_version": pattern["pattern_spec"]["data"]["universe_version"],
        "population_source_id": prospective_source_bundle["source_bundle_id"],
        "population_source_hash": prospective_source_bundle[
            "source_bundle_hash"
        ],
        "selection_rule_id": "ALL_ELIGIBLE_POST_FREEZE_PIT_OBSERVATIONS_IN_VERIFIED_L7_SCANNER_POPULATION_SAME_TARGET_HORIZON_V1",
        "selection_rule_hash": digest(
            {
                "selection_rule_id": "ALL_ELIGIBLE_POST_FREEZE_PIT_OBSERVATIONS_IN_VERIFIED_L7_SCANNER_POPULATION_SAME_TARGET_HORIZON_V1",
                "baseline_definition": forecast["baseline"],
                "universe_version": pattern["pattern_spec"]["data"][
                    "universe_version"
                ],
                "target_id": forecast["target_id"],
                "horizon_sessions": forecast["horizon_sessions"],
            }
        ),
        "eligible_population_count": len(observations),
        "target_id": forecast["target_id"],
        "horizon_sessions": forecast["horizon_sessions"],
        "snapshot_session_bindings": snapshot_session_bindings,
        "observations": observations,
    }




def baseline_bundle_for_test(
    patterns,
    prospective_source_bundle,
    repo_root,
    *,
    baseline_bundle_id="BASE-L9-TEST",
    generated_at="2027-01-15T20:00:00Z",
    records=None,
):
    horizons = [
        int(pattern["pattern_spec"]["forecast"]["horizon_sessions"])
        for pattern in patterns
    ]
    _, price_provenance = canonical_price_state_for_test(
        repo_root,
        prospective_source_bundle,
        evaluated_at=generated_at,
        horizon=max(horizons),
    )
    values = (
        records
        if records is not None
        else [
            baseline_record(
                pattern,
                prospective_source_bundle,
                repo_root,
                evaluated_at=generated_at,
            )
            for pattern in patterns
        ]
    )
    return build_baseline_bundle(
        values,
        prospective_source_bundle,
        price_provenance,
        baseline_bundle_id=baseline_bundle_id,
        generated_at=generated_at,
    )


def context_bundle_for(patterns, outcomes, prospective_source_bundle=None):
    source = prospective_source_bundle
    values = outcomes
    if source is None:
        source, values = prospective_source_for(patterns, outcomes)
    return build_context_bundle_from_sources(
        [row["claim"]["claim_id"] for row in values],
        source,
        context_bundle_id="CTX-L9-TEST",
        generated_at=source["generated_at"],
    )


def setup_single_family(
    tmp_path,
    *,
    planned_looks=None,
    early_stop_allowed=False,
    minimum_raw_n=20,
):
    regs = registries(tmp_path)
    pattern = make_pattern(minimum_raw_n=minimum_raw_n)
    member = apply_l5_handoff(pattern, regs)
    control, monitor = freeze_family(
        regs,
        [member],
        family_id="HF-FAM-L9",
        planned_looks=planned_looks,
        early_stop_allowed=early_stop_allowed,
    )
    return regs, pattern, control, monitor


def build_report(
    regs,
    pattern,
    control,
    monitor,
    *,
    outcomes=None,
    baseline=None,
    context=None,
    prospective_source=None,
    evaluated_at="2027-01-15T20:00:00Z",
    confirmation_registry=None,
    maturation_events_override=None,
):
    raw_values = outcomes if outcomes is not None else prospective_rows(pattern)
    if prospective_source is None:
        source, values = prospective_source_for([pattern], raw_values)
    else:
        source = prospective_source
        values = deepcopy(raw_values)
        source_claims = {
            row["claim_id"]: row
            for row in source["claims"]
        }
        for outcome in values:
            claim_id = outcome["claim"]["claim_id"]
            src = source_claims[claim_id]
            outcome["claim"]["claim_hash"] = src["claim_hash"]
            outcome["claim"]["event_id"] = src["event_id"]
            outcome["claim"]["capture_snapshot_id"] = src["snapshot_id"]
            outcome["claim"]["capture_snapshot_binding_hash"] = src[
                "snapshot_binding_hash"
            ]
            outcome["outcome_hash"] = digest(
                {
                    k: v
                    for k, v in outcome.items()
                    if k != "outcome_hash"
                }
            )

    events = (
        list(maturation_events_override)
        if maturation_events_override is not None
        else maturation_registry_events(
            values,
            recorded_at=evaluated_at,
        )
    )
    write_authoritative_maturation_registry(
        regs["repo_root"],
        events,
    )
    price_groups, price_provenance = canonical_price_state_for_test(
        regs["repo_root"],
        source,
        evaluated_at=evaluated_at,
        horizon=int(pattern["pattern_spec"]["forecast"]["horizon_sessions"]),
    )
    baseline_value = (
        baseline
        if baseline is not None
        else baseline_record(
            pattern,
            source,
            regs["repo_root"],
            evaluated_at=evaluated_at,
        )
    )
    baseline_bundle = build_baseline_bundle(
        [baseline_value],
        source,
        price_provenance,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at=evaluated_at,
    )
    contexts = (
        context
        if context is not None
        else context_bundle_for([pattern], values, source)
    )
    return build_confirmation_look(
        [pattern],
        events,
        source,
        baseline_bundle,
        control_plan_id=control["control_plan_id"],
        control_plan_version=control["control_plan_version"],
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version=monitor["monitoring_plan_version"],
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at=evaluated_at,
        repo_root=regs["repo_root"],
        confirmation_registry=confirmation_registry,
        context_bundle=contexts,
    )


def test_l9_contract_reuses_qm_and_defers_rating():
    contract = load_confirmation_contract()
    assert contract["phase"] == "L9"
    assert contract["research_only"] is True
    assert contract["principles"]["qm_c1_c2_c3_c4_are_authoritative"] is True
    assert contract["principles"]["discovery_evidence_never_counts_as_confirmation"] is True
    assert contract["principles"]["missing_maturity_is_unresolved_not_falsified"] is True
    assert contract["boundaries"]["rating_assigned"] is False
    assert contract["boundaries"]["promotion_performed"] is False


def test_positive_final_look_uses_only_prospective_l8_evidence(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    report = build_report(regs, pattern, control, monitor)

    checked = verify_confirmation_look(report)
    assert checked["valid"] is True
    assert report["look_status"] == "EVALUATED"
    assert report["family_decision"] == "FINAL_COMPLETE"
    result = report["pattern_results"][0]
    assert result["result_class"] == "SUPPORTED"
    assert result["discovery_evidence_used_as_confirmation"] is False
    evidence = result["prospective_evidence"]
    assert evidence["raw_n"] == 24
    assert evidence["effective_n"] <= evidence["raw_n"]
    assert evidence["support_region_count"] >= 3
    assert evidence["direction_probability"] == pytest.approx(1.0)
    assert evidence["baseline_probability"] == pytest.approx(0.0)
    assert evidence["probability_advantage_lift"] == pytest.approx(1.0)
    assert evidence["effect_size_vs_baseline"] > 0.07
    assert evidence["robust_uncertainty"]["aligned_effect_interval_95"][0] > 0
    assert evidence["robust_uncertainty"][
        "effect_size_vs_baseline_interval_95"
    ][0] > 0
    assert evidence["robust_uncertainty"]["probability_lift_interval_95"][0] > 0
    assert result["multiple_testing"]["passed"] is True
    assert report["qm_c_handoff"]["qm_c5_results"][0][
        "outcome_classification"
    ] == "POSITIVE"


def test_missing_maturity_is_unresolved_not_due_and_does_not_consume_look(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern, count=12)
    report = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=outcomes,
    )
    assert report["look_status"] == "UNRESOLVED_NOT_DUE"
    assert report["look_consumes_qm_c4_schedule"] is False
    assert report["pattern_results"] == []
    assert report["qm_c_handoff"]["qm_c4_record_look"] is None
    assert report["qm_c_handoff"]["qm_c5_results"] == []


def test_strong_negative_final_result_is_falsified_and_retained(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern, target_value=-0.08, prefix="NEG")
    report = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=outcomes,
    )
    result = report["pattern_results"][0]
    assert result["result_class"] == "FALSIFIED"
    assert "ROBUST_SIGN_REVERSAL" in result["result_reasons"]
    assert report["qm_c_handoff"]["qm_c5_results"][0][
        "outcome_classification"
    ] == "NEGATIVE"

    source, normalized = prospective_source_for([pattern], outcomes)
    baseline_bundle = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    saved = persist_confirmation_look(
        tmp_path,
        report,
        source,
        baseline_bundle,
        context_bundle=contexts,
        actor_id="tester",
        actor_role="researcher",
    )
    assert saved["registry"]["look_count"] == 1
    registry = ConfirmationLookRegistry(saved["registry_path"])
    assert registry.verify_integrity()["look_count"] == 1


def test_discovery_evidence_cannot_rescue_failed_confirmation(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    assert pattern["discovery_evidence"]["direction_probability"] == 0.99
    outcomes = prospective_rows(pattern, target_value=-0.08, prefix="NORESCUE")
    report = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=outcomes,
    )
    assert report["pattern_results"][0]["result_class"] == "FALSIFIED"


def test_no_posthoc_regime_rescue_of_negative_full_sample(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern, target_value=-0.04, prefix="REGIME")
    source, normalized = prospective_source_for([pattern], outcomes)
    contexts = context_bundle_for([pattern], normalized, source)
    contexts["claim_contexts"][0]["market_regime_stock"] = "SPECIAL"
    contexts["context_bundle_hash"] = digest(
        {k: v for k, v in contexts.items() if k != "context_bundle_hash"}
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="context_value_not_from_capture_row",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            context=contexts,
        )


def test_context_bundle_cannot_omit_entire_eligible_claims(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    empty_context = build_context_bundle(
        [],
        source,
        context_bundle_id="CTX-EMPTY",
        generated_at=source["generated_at"],
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="context_bundle_must_exactly_cover_prospective_source_claims",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            context=empty_context,
        )


def test_capture_time_regime_cannot_be_selectively_omitted(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    contexts = context_bundle_for([pattern], normalized, source)
    contexts["claim_contexts"][0].pop("market_regime_stock")
    contexts["context_bundle_hash"] = digest(
        {k: v for k, v in contexts.items() if k != "context_bundle_hash"}
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="context_value_required_from_exact_l7_row",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=normalized,
            prospective_source=source,
            context=contexts,
        )


def test_context_rows_without_capture_time_regime_are_inconclusive_not_supported(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for(
        [pattern],
        outcomes,
        include_regime=False,
    )
    contexts = context_bundle_for([pattern], normalized, source)
    report = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=normalized,
        prospective_source=source,
        context=contexts,
    )
    result = report["pattern_results"][0]
    assert result["result_class"] == "INCONCLUSIVE"
    assert "REGIME_CONTEXT_COVERAGE_INSUFFICIENT" in result["result_reasons"]


def test_l8_maturation_events_after_l9_evaluation_are_excluded_by_prefix(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    events = maturation_registry_events(
        normalized,
        recorded_at="2027-01-16T20:00:00Z",
    )
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-FUTURE-MATURATION",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    write_authoritative_maturation_registry(tmp_path, events)
    report = build_confirmation_look(
        [pattern],
        events,
        source,
        baseline,
        control_plan_id=control["control_plan_id"],
        control_plan_version=control["control_plan_version"],
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version=monitor["monitoring_plan_version"],
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at="2027-01-15T20:00:00Z",
        repo_root=tmp_path,
        context_bundle=contexts,
    )
    assert report["look_status"] == "UNRESOLVED_NOT_DUE"
    binding = report["input_bindings"]["l8_maturation_registry"]
    assert binding["eligible_prefix_event_count"] == 0
    assert binding["eligible_event_hashes"] == []


def test_same_day_target_session_is_not_eligible_until_later_date(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcome = matured_outcome(
        pattern,
        claim_id="SAME-DAY-CLOSE",
        symbol="AAA",
        start_day="2027-01-10",
        target_value=0.08,
    )
    source, normalized = prospective_source_for([pattern], [outcome])
    events = maturation_registry_events(
        normalized,
        recorded_at="2027-01-15T20:00:00Z",
    )
    write_authoritative_maturation_registry(tmp_path, events)
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-SAME-DAY-CLOSE",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    report = build_confirmation_look(
        [pattern],
        events,
        source,
        baseline,
        control_plan_id=control["control_plan_id"],
        control_plan_version=control["control_plan_version"],
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version=monitor["monitoring_plan_version"],
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at="2027-01-15T20:00:00Z",
        repo_root=tmp_path,
        context_bundle=contexts,
    )
    assert report["look_status"] == "UNRESOLVED_NOT_DUE"
    assert report["input_bindings"]["l8_maturation_registry"][
        "eligible_prefix_event_count"
    ] == 0


def test_truncated_valid_l8_chain_is_rejected_against_authoritative_registry(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    full_events = maturation_registry_events(normalized)
    write_authoritative_maturation_registry(tmp_path, full_events)
    truncated = full_events[:-1]
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-TRUNCATED-L8",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    with pytest.raises(
        ConfirmationEngineError,
        match="supplied_l8_maturation_chain_not_equal_authoritative_registry",
    ):
        build_confirmation_look(
            [pattern],
            truncated,
            source,
            baseline,
            control_plan_id=control["control_plan_id"],
            control_plan_version=control["control_plan_version"],
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version=monitor["monitoring_plan_version"],
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_l8_maturation_registry_recorded_time_must_be_monotonic(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern, count=2)
    source, normalized = prospective_source_for([pattern], outcomes)
    events = maturation_registry_events(
        normalized,
        recorded_at="2027-01-15T20:00:00Z",
    )
    events[1]["recorded_at"] = "2027-01-14T20:00:00Z"
    events[1]["entry_hash"] = digest(
        {k: v for k, v in events[1].items() if k != "entry_hash"}
    )
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-NONMONOTONIC",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    write_authoritative_maturation_registry(tmp_path, events)
    with pytest.raises(
        ConfirmationEngineError,
        match="l8_maturation_registry_time_not_monotonic",
    ):
        build_confirmation_look(
            [pattern],
            events,
            source,
            baseline,
            control_plan_id=control["control_plan_id"],
            control_plan_version=control["control_plan_version"],
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version=monitor["monitoring_plan_version"],
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_future_l8_registry_suffix_does_not_change_historical_l9_look(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    prefix = maturation_registry_events(
        normalized,
        recorded_at="2027-01-15T20:00:00Z",
    )
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-SUFFIX-INDEPENDENCE",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    write_authoritative_maturation_registry(tmp_path, prefix)
    first = build_confirmation_look(
        [pattern],
        prefix,
        source,
        baseline,
        control_plan_id=control["control_plan_id"],
        control_plan_version=control["control_plan_version"],
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version=monitor["monitoring_plan_version"],
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at="2027-01-15T20:00:00Z",
        repo_root=tmp_path,
        context_bundle=contexts,
    )

    future_record = matured_outcome(
        pattern,
        claim_id="FUTURE-SUFFIX",
        symbol="AAA",
        start_day="2026-12-01",
        target_value=0.50,
    )
    future_event = {
        "schema_version": "pattern_discovery_l8_maturation_event_v1",
        "sequence": len(prefix) + 1,
        "event_id": "PME-FUTURE-SUFFIX",
        "event_type": "OUTCOME_MATURED",
        "recorded_at": "2027-01-16T20:00:00Z",
        "actor_id": "tester",
        "actor_role": "researcher",
        "record": future_record,
        "previous_event_hash": prefix[-1]["entry_hash"],
    }
    future_event["entry_hash"] = digest(future_event)
    full_chain = [*prefix, future_event]
    write_authoritative_maturation_registry(tmp_path, full_chain)
    second = build_confirmation_look(
        [pattern],
        full_chain,
        source,
        baseline,
        control_plan_id=control["control_plan_id"],
        control_plan_version=control["control_plan_version"],
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version=monitor["monitoring_plan_version"],
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at="2027-01-15T20:00:00Z",
        repo_root=tmp_path,
        context_bundle=contexts,
    )
    assert second == first
    assert second["look_hash"] == first["look_hash"]


def test_tampered_l8_maturation_chain_fails_closed(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    events = maturation_registry_events(normalized)
    events[1]["previous_event_hash"] = "0" * 64
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-BAD-CHAIN",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    write_authoritative_maturation_registry(tmp_path, events)
    with pytest.raises(
        ConfirmationEngineError,
        match="l8_maturation_registry_previous_hash_invalid",
    ):
        build_confirmation_look(
            [pattern],
            events,
            source,
            baseline,
            control_plan_id=control["control_plan_id"],
            control_plan_version=control["control_plan_version"],
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version=monitor["monitoring_plan_version"],
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_tampered_l8_outcome_fails_closed(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    source, normalized = prospective_source_for(
        [pattern],
        prospective_rows(pattern),
    )
    normalized[0]["outcome"]["target_value"] = 99.0
    events = maturation_registry_events(normalized)
    baseline = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    write_authoritative_maturation_registry(tmp_path, events)
    with pytest.raises(Exception, match="matured_outcome_hash_mismatch"):
        build_confirmation_look(
            [pattern],
            events,
            source,
            baseline,
            control_plan_id=control["control_plan_id"],
            control_plan_version=control["control_plan_version"],
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version=monitor["monitoring_plan_version"],
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_pre_freeze_outcome_cannot_enter_confirmation(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    bad = deepcopy(normalized[0])
    bad["horizon_provenance"]["start_at"] = "2026-10-01T08:00:00Z"
    bad["outcome_hash"] = digest(
        {k: v for k, v in bad.items() if k != "outcome_hash"}
    )
    normalized[0] = bad
    with pytest.raises(
        ConfirmationEngineError,
        match="nonprospective_l8_outcome_after_freeze_required",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=normalized,
            prospective_source=source,
        )


def test_baseline_outcome_after_l9_evaluation_fails_closed(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)
    baseline["observations"][0]["end_at"] = "2027-01-16T08:00:00Z"
    body = dict(baseline["observations"][0])
    body.pop("source_hash", None)
    baseline["observations"][0]["source_hash"] = digest(body)
    report_bundle = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-FUTURE-OUTCOME",
        generated_at="2027-01-15T20:00:00Z",
        records=[baseline],
    )
    contexts = context_bundle_for([pattern], normalized, source)
    events = maturation_registry_events(normalized)
    write_authoritative_maturation_registry(tmp_path, events)
    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_outcome_after_l9_evaluation",
    ):
        build_confirmation_look(
            [pattern],
            events,
            source,
            report_bundle,
            control_plan_id=control["control_plan_id"],
            control_plan_version=control["control_plan_version"],
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version=monitor["monitoring_plan_version"],
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_baseline_definition_must_match_frozen_pattern(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)
    baseline["baseline_definition"] = "posthoc_new_baseline"
    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_frozen_pattern_mismatch",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            baseline=baseline,
        )


def test_baseline_start_date_cannot_depart_from_l7_session_map(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)
    baseline["observations"][0]["start_at"] = "2026-10-09T08:00:00Z"
    baseline["observations"][0]["end_at"] = "2026-10-14T08:00:00Z"
    body = dict(baseline["observations"][0])
    body.pop("source_hash", None)
    baseline["observations"][0]["source_hash"] = digest(body)
    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_start_at_not_from_l7_session_map",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            baseline=baseline,
        )


def test_baseline_selection_rule_cannot_be_changed_after_freeze(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)
    baseline["selection_rule_id"] = "POSTHOC_FAVORABLE_SUBSET"
    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_frozen_pattern_mismatch",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            baseline=baseline,
        )


def test_declared_baseline_population_must_be_fully_present(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)
    baseline["eligible_population_count"] += 1
    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_eligible_population_not_fully_present",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            baseline=baseline,
        )


def test_cherry_picked_baseline_subset_fails_against_l7_snapshot_population(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, _ = prospective_source_for([pattern], outcomes)
    baseline = baseline_record(pattern, source, tmp_path)

    removed = baseline["observations"].pop(0)
    baseline["eligible_population_count"] = len(baseline["observations"])
    assert removed["symbol"] not in {
        row["symbol"]
        for row in baseline["observations"]
        if row["capture_snapshot_id"] == removed["capture_snapshot_id"]
    }

    with pytest.raises(
        ConfirmationEngineError,
        match="baseline_snapshot_population_incomplete",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            baseline=baseline,
        )


def test_context_must_match_exact_l8_capture_snapshot(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    contexts = context_bundle_for([pattern], normalized, source)
    contexts["claim_contexts"][0]["capture_snapshot_id"] = "OTHER-SNAPSHOT"
    contexts["context_bundle_hash"] = digest(
        {k: v for k, v in contexts.items() if k != "context_bundle_hash"}
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="context_source_row_snapshot_mismatch|context_capture_snapshot_id_mismatch",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=outcomes,
            prospective_source=source,
            context=contexts,
        )


def test_qm_c3_family_membership_must_exactly_match_patterns(tmp_path):
    regs = registries(tmp_path)
    pattern_a = make_pattern(pattern_id="PAT-L9-A", family_id="FAM-L9")
    pattern_b = make_pattern(pattern_id="PAT-L9-B", family_id="FAM-L9")
    member_a = apply_l5_handoff(pattern_a, regs, qm_a_version="analysis-a")
    member_b = apply_l5_handoff(pattern_b, regs, qm_a_version="analysis-b")
    control, monitor = freeze_family(
        regs,
        [member_a, member_b],
        family_id="HF-FAM-L9",
        strategy="BONFERRONI_FWER",
        parameters={"family_alpha": 0.05},
    )
    outcomes = prospective_rows(pattern_a, prefix="A") + prospective_rows(
        pattern_b, prefix="B"
    )
    source, normalized = prospective_source_for(
        [pattern_a, pattern_b],
        outcomes,
    )
    baseline = baseline_bundle_for_test(
        [pattern_a, pattern_b],
        source,
        tmp_path,
        baseline_bundle_id="BASE-FAMILY",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for(
        [pattern_a, pattern_b],
        normalized,
        source,
    )
    events = maturation_registry_events(normalized)
    write_authoritative_maturation_registry(tmp_path, events)
    with pytest.raises(
        ConfirmationEngineError,
        match="l9_patterns_must_exactly_cover_qm_c3_family",
    ):
        build_confirmation_look(
            [pattern_a],
            events,
            source,
            baseline,
            control_plan_id=control["control_plan_id"],
            control_plan_version="v1",
            monitoring_plan_id=monitor["monitoring_plan_id"],
            monitoring_plan_version="v1",
            hypothesis_registry=regs["hypotheses"],
            analysis_plan_registry=regs["plans"],
            control_registry=regs["c3"],
            monitoring_registry=regs["c4"],
            qm_a_ledger=regs["qm_a"],
            evaluated_at="2027-01-15T20:00:00Z",
            repo_root=tmp_path,
            context_bundle=contexts,
        )


def test_bonferroni_family_and_sequential_threshold_are_both_applied(tmp_path):
    regs = registries(tmp_path)
    pattern_a = make_pattern(pattern_id="PAT-L9-A", family_id="FAM-L9")
    pattern_b = make_pattern(pattern_id="PAT-L9-B", family_id="FAM-L9")
    member_a = apply_l5_handoff(pattern_a, regs, qm_a_version="analysis-a")
    member_b = apply_l5_handoff(pattern_b, regs, qm_a_version="analysis-b")
    control, monitor = freeze_family(
        regs,
        [member_a, member_b],
        family_id="HF-FAM-L9",
        strategy="BONFERRONI_FWER",
        parameters={"family_alpha": 0.05},
        planned_looks=[
            {"look_id": "LOOK_1", "information_fraction": 0.5},
            {"look_id": "FINAL", "information_fraction": 1.0},
        ],
    )
    outcomes = prospective_rows(pattern_a, count=12, prefix="A") + prospective_rows(
        pattern_b, count=12, prefix="B"
    )
    source, normalized = prospective_source_for(
        [pattern_a, pattern_b],
        outcomes,
    )
    baseline = baseline_bundle_for_test(
        [pattern_a, pattern_b],
        source,
        tmp_path,
        baseline_bundle_id="BASE-MULTI",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for(
        [pattern_a, pattern_b],
        normalized,
        source,
    )
    events = maturation_registry_events(normalized)
    write_authoritative_maturation_registry(tmp_path, events)
    report = build_confirmation_look(
        [pattern_a, pattern_b],
        events,
        source,
        baseline,
        control_plan_id=control["control_plan_id"],
        control_plan_version="v1",
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version="v1",
        hypothesis_registry=regs["hypotheses"],
        analysis_plan_registry=regs["plans"],
        control_registry=regs["c3"],
        monitoring_registry=regs["c4"],
        qm_a_ledger=regs["qm_a"],
        evaluated_at="2027-01-15T20:00:00Z",
        repo_root=tmp_path,
        context_bundle=contexts,
    )
    assert report["look_status"] == "EVALUATED"
    for result in report["pattern_results"]:
        mt = result["multiple_testing"]
        assert mt["strategy"] == "BONFERRONI_FWER"
        assert mt["base_threshold"] == pytest.approx(0.05)
        assert mt["sequential_threshold"] == pytest.approx(0.025)
        assert mt["adjusted_p_value"] >= mt["raw_p_value"]


def test_predeclared_two_look_schedule_enforces_order_and_spent_qm_a(tmp_path):
    regs, pattern, control, monitor = setup_single_family(
        tmp_path,
        planned_looks=[
            {"look_id": "LOOK_1", "information_fraction": 0.5},
            {"look_id": "FINAL", "information_fraction": 1.0},
        ],
        minimum_raw_n=20,
    )
    first_outcomes = prospective_rows(pattern, count=12, prefix="SEQ")
    source1, normalized1 = prospective_source_for([pattern], first_outcomes)
    first_events = maturation_registry_events(
        normalized1,
        recorded_at="2027-01-15T20:00:00Z",
    )
    first = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=normalized1,
        prospective_source=source1,
        maturation_events_override=first_events,
    )
    assert first["qm_governance"]["look_id"] == "LOOK_1"
    assert first["family_decision"] == "CONTINUE"
    assert first["qm_c_handoff"]["qm_c5_results"] == []

    first_baseline = baseline_bundle_for_test(
        [pattern],
        source1,
        tmp_path,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at="2027-01-15T20:00:00Z",
    )
    first_context = context_bundle_for(
        [pattern],
        normalized1,
        source1,
    )
    persisted = persist_confirmation_look(
        tmp_path,
        first,
        source1,
        first_baseline,
        context_bundle=first_context,
        actor_id="tester",
        actor_role="researcher",
    )
    local_registry = ConfirmationLookRegistry(
        persisted["registry_path"]
    )

    regs["c4"].record_monitoring_look(
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version="v1",
        look_id="LOOK_1",
        artifact_hash=first["confirmation_evidence_hash"],
        decision="CONTINUE",
        control_registry=regs["c3"],
        qm_a_ledger=regs["qm_a"],
        actor_id="tester",
        actor_role="researcher",
        observed_at=first["evaluated_at"],
    )
    member = control["family_members"][0]
    regs["qm_a"].transition(
        analysis_id=member["qm_a_analysis_id"],
        version_id=member["qm_a_version_id"],
        to_state="CONFIRMATORY_EVALUATED",
        actor_id="tester",
        actor_role="researcher",
        reason="Consume first predeclared confirmation look.",
    )
    monitor2 = regs["c4"].get_monitoring_plan(
        monitor["monitoring_plan_id"],
        "v1",
    )
    assert monitor2["state"] == "MONITORING"

    second_outcomes = prospective_rows(pattern, count=24, prefix="SEQ")
    source2, normalized2 = prospective_source_for([pattern], second_outcomes)
    second_events = maturation_registry_events(
        normalized2[len(normalized1):],
        recorded_at="2027-02-15T20:00:00Z",
        prior_events=first_events,
    )
    second = build_report(
        regs,
        pattern,
        control,
        monitor2,
        outcomes=normalized2,
        prospective_source=source2,
        evaluated_at="2027-02-15T20:00:00Z",
        confirmation_registry=local_registry,
        maturation_events_override=second_events,
    )
    assert second["qm_governance"]["look_id"] == "FINAL"
    assert second["family_decision"] == "FINAL_COMPLETE"
    assert second["qm_c_handoff"]["qm_c5_results"]


def test_second_look_cannot_drop_evidence_consumed_by_first_look(tmp_path):
    regs, pattern, control, monitor = setup_single_family(
        tmp_path,
        planned_looks=[
            {"look_id": "LOOK_1", "information_fraction": 0.5},
            {"look_id": "FINAL", "information_fraction": 1.0},
        ],
        minimum_raw_n=20,
    )
    first_outcomes = prospective_rows(pattern, count=12, prefix="KEEP")
    source1, normalized1 = prospective_source_for([pattern], first_outcomes)
    first_events = maturation_registry_events(
        normalized1,
        recorded_at="2027-01-15T20:00:00Z",
    )
    first = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=normalized1,
        prospective_source=source1,
        maturation_events_override=first_events,
    )
    first_baseline = baseline_bundle_for_test(
        [pattern],
        source1,
        tmp_path,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at="2027-01-15T20:00:00Z",
    )
    first_context = context_bundle_for(
        [pattern],
        normalized1,
        source1,
    )
    persisted = persist_confirmation_look(
        tmp_path,
        first,
        source1,
        first_baseline,
        context_bundle=first_context,
        actor_id="tester",
        actor_role="researcher",
    )
    local_registry = ConfirmationLookRegistry(
        persisted["registry_path"]
    )
    regs["c4"].record_monitoring_look(
        monitoring_plan_id=monitor["monitoring_plan_id"],
        monitoring_plan_version="v1",
        look_id="LOOK_1",
        artifact_hash=first["confirmation_evidence_hash"],
        decision="CONTINUE",
        control_registry=regs["c3"],
        qm_a_ledger=regs["qm_a"],
        actor_id="tester",
        actor_role="researcher",
        observed_at=first["evaluated_at"],
    )
    member = control["family_members"][0]
    regs["qm_a"].transition(
        analysis_id=member["qm_a_analysis_id"],
        version_id=member["qm_a_version_id"],
        to_state="CONFIRMATORY_EVALUATED",
        actor_id="tester",
        actor_role="researcher",
        reason="Consume first L9 look before cumulative-evidence test.",
    )
    monitor2 = regs["c4"].get_monitoring_plan(
        monitor["monitoring_plan_id"],
        "v1",
    )

    disjoint = prospective_rows(pattern, count=24, prefix="DROP")
    source2, normalized2 = prospective_source_for([pattern], disjoint)
    disjoint_events = maturation_registry_events(
        normalized2,
        recorded_at="2027-02-15T20:00:00Z",
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="l8_maturation_evidence_not_cumulative_before_later_look",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor2,
            outcomes=normalized2,
            prospective_source=source2,
            evaluated_at="2027-02-15T20:00:00Z",
            confirmation_registry=local_registry,
            maturation_events_override=disjoint_events,
        )


def test_automatic_early_stop_fails_closed_without_machine_readable_boundary(tmp_path):
    regs, pattern, control, monitor = setup_single_family(
        tmp_path,
        planned_looks=[
            {"look_id": "LOOK_1", "information_fraction": 0.5},
            {"look_id": "FINAL", "information_fraction": 1.0},
        ],
        early_stop_allowed=True,
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="l9_v1_automatic_early_stop_not_supported",
    ):
        build_report(
            regs,
            pattern,
            control,
            monitor,
            outcomes=prospective_rows(pattern, count=12),
        )


def test_qm_c4_handoff_fields_are_bound_to_the_evaluated_report(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    report = build_report(regs, pattern, control, monitor)
    tampered = deepcopy(report)
    tampered["qm_c_handoff"]["qm_c4_record_look"]["look_id"] = "OTHER_LOOK"
    tampered["look_hash"] = digest(
        {k: v for k, v in tampered.items() if k != "look_hash"}
    )
    with pytest.raises(
        ConfirmationEngineError,
        match="qm_c4_handoff_report_binding_mismatch:look_id",
    ):
        verify_confirmation_look(tampered)


def test_local_registry_is_append_only_idempotent_and_hash_protected(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    outcomes = prospective_rows(pattern)
    source, normalized = prospective_source_for([pattern], outcomes)
    report = build_report(
        regs,
        pattern,
        control,
        monitor,
        outcomes=normalized,
        prospective_source=source,
    )
    baseline_bundle = baseline_bundle_for_test(
        [pattern],
        source,
        tmp_path,
        baseline_bundle_id="BASE-L9-TEST",
        generated_at="2027-01-15T20:00:00Z",
    )
    contexts = context_bundle_for([pattern], normalized, source)
    first = persist_confirmation_look(
        tmp_path,
        report,
        source,
        baseline_bundle,
        context_bundle=contexts,
        actor_id="tester",
        actor_role="researcher",
    )
    assert first["registry"]["idempotent"] is False
    second = persist_confirmation_look(
        tmp_path,
        report,
        source,
        baseline_bundle,
        context_bundle=contexts,
        actor_id="tester",
        actor_role="researcher",
    )
    assert second["registry"]["idempotent"] is True

    tampered = deepcopy(report)
    tampered["pattern_results"][0]["prospective_evidence"][
        "direction_probability"
    ] = 0.123
    with pytest.raises(
        ConfirmationEngineError,
        match="confirmation_look_hash_mismatch",
    ):
        verify_confirmation_look(tampered)


def test_l9_creates_no_rating_promotion_decision_or_execution_authority(tmp_path):
    regs, pattern, control, monitor = setup_single_family(tmp_path)
    report = build_report(regs, pattern, control, monitor)
    serialized = json.dumps(report, sort_keys=True)
    for forbidden in (
        '"universal_stance"',
        '"portfolio_action"',
        '"trade_decision"',
        '"order_instruction"',
        '"buy_signal"',
        '"sell_signal"',
        '"position_size"',
        '"target_weight"',
    ):
        assert forbidden not in serialized
    assert report["boundaries"]["rating_assigned"] is False
    assert report["boundaries"]["promotion_performed"] is False
    assert report["boundaries"]["decision_layer_integration_performed"] is False
    assert report["qm_c_handoff"]["direct_qm_registry_write_performed"] is False
