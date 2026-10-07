from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import (
    FamilyMultiplicityRegistry,
)
from scanner.research.governance.qm_c_sequential_monitoring import (
    SequentialMonitoringRegistry,
)
from scanner.research.pattern_discovery import (
    build_confirmation_look,
    persist_confirmation_look,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def _patterns(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("frozen_patterns"), list):
        return payload["frozen_patterns"]
    raise ValueError("frozen_patterns_json_must_be_list_or_l5_snapshot")


def _maturation_events(path: Path):
    if path.suffix.lower() == ".json":
        payload = _json(path)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return payload["events"]
        raise ValueError(
            "maturation_events_json_must_be_list_or_events_object"
        )

    events = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        event = json.loads(line)
        if not isinstance(event, dict):
            raise ValueError(
                f"maturation_registry_event_not_object:{line_number}"
            )
        events.append(event)
    return events


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the next permitted Pattern Discovery L9 confirmation look."
    )
    parser.add_argument("--frozen-patterns", required=True)
    parser.add_argument("--matured-outcomes", required=True)
    parser.add_argument("--baseline-bundle", required=True)
    parser.add_argument("--context-bundle")
    parser.add_argument("--control-plan-id", required=True)
    parser.add_argument("--control-plan-version", required=True)
    parser.add_argument("--monitoring-plan-id", required=True)
    parser.add_argument("--monitoring-plan-version", required=True)
    parser.add_argument("--evaluated-at", required=True)
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--actor-role", default="researcher")
    parser.add_argument(
        "--qm-c1-registry",
        default="artifacts/research/qm/qm_c_hypothesis_registry.jsonl",
    )
    parser.add_argument(
        "--qm-c2-registry",
        default="artifacts/research/qm/qm_c_analysis_plan_registry.jsonl",
    )
    parser.add_argument(
        "--qm-c3-registry",
        default="artifacts/research/qm/qm_c3_family_multiplicity.jsonl",
    )
    parser.add_argument(
        "--qm-c4-registry",
        default="artifacts/research/qm/qm_c4_sequential_monitoring.jsonl",
    )
    parser.add_argument(
        "--qm-a-registry",
        default="artifacts/research/qm/qm_a_governance_events.jsonl",
    )
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    patterns = _patterns(_json(_path(root, args.frozen_patterns)))
    maturation_events = _maturation_events(
        _path(root, args.matured_outcomes)
    )
    baseline = _json(_path(root, args.baseline_bundle))
    context = (
        _json(_path(root, args.context_bundle))
        if args.context_bundle
        else None
    )

    report = build_confirmation_look(
        patterns,
        maturation_events,
        baseline,
        control_plan_id=args.control_plan_id,
        control_plan_version=args.control_plan_version,
        monitoring_plan_id=args.monitoring_plan_id,
        monitoring_plan_version=args.monitoring_plan_version,
        hypothesis_registry=HypothesisRegistry(_path(root, args.qm_c1_registry)),
        analysis_plan_registry=AnalysisPlanRegistry(_path(root, args.qm_c2_registry)),
        control_registry=FamilyMultiplicityRegistry(_path(root, args.qm_c3_registry)),
        monitoring_registry=SequentialMonitoringRegistry(_path(root, args.qm_c4_registry)),
        qm_a_ledger=GovernanceLedger(_path(root, args.qm_a_registry)),
        evaluated_at=args.evaluated_at,
        context_bundle=context,
    )

    persisted = None
    if report["look_status"] == "EVALUATED":
        persisted = persist_confirmation_look(
            root,
            report,
            baseline,
            context_bundle=context,
            actor_id=args.actor_id,
            actor_role=args.actor_role,
        )

    print(
        json.dumps(
            {
                "confirmation_look_id": report["confirmation_look_id"],
                "look_hash": report["look_hash"],
                "look_status": report["look_status"],
                "qm_c4_look_id": report["qm_governance"]["next_look"]["look_id"]
                if report["look_status"] == "UNRESOLVED_NOT_DUE"
                else report["qm_governance"]["look_id"],
                "family_decision": report["family_decision"],
                "pattern_results": [
                    {
                        "pattern_id": row["pattern_id"],
                        "pattern_version": row["pattern_version"],
                        "result_class": row["result_class"],
                        "result_reasons": row["result_reasons"],
                    }
                    for row in report["pattern_results"]
                ],
                "qm_c_handoff": report["qm_c_handoff"],
                "persisted": persisted,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
