from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    PromotionRegistry,
    build_challenger_trace,
    evaluate_incremental_value,
    load_promotion_contract,
    persist_challenger_trace,
    persist_incremental_evaluation,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def _registry(root: Path, explicit: str | None) -> PromotionRegistry:
    if explicit:
        path = _path(root, explicit)
    else:
        contract = load_promotion_contract()
        path = root / contract["storage"]["registry_path"]
    return PromotionRegistry(path)


def _trace(args) -> int:
    root = Path(args.repo_root).resolve()
    trace = build_challenger_trace(
        _json(_path(root, args.decision_packet)),
        [_json(_path(root, value)) for value in args.l7_claim],
        [_json(_path(root, value)) for value in args.admission_context],
        promotion_registry=_registry(root, args.promotion_registry),
        horizon_sessions=args.horizon_sessions,
        generated_at=args.generated_at,
    )
    persisted = persist_challenger_trace(root, trace)
    print(
        json.dumps(
            {
                "phase": "L13",
                "mode": "trace",
                "trace_hash": trace["trace_hash"],
                "symbol": trace["symbol"],
                "horizon_sessions": trace["horizon_sessions"],
                "baseline_timing_state": trace["timing_ablation"][
                    "baseline_timing"
                ]["state"],
                "pattern_challenger_state": trace["timing_ablation"][
                    "pattern_challenger"
                ]["state"],
                "shadow_fused_state": trace["timing_ablation"][
                    "shadow_fused_timing"
                ]["state"],
                "persisted": persisted,
            },
            ensure_ascii=False,
        )
    )
    return 0


def _evaluate(args) -> int:
    root = Path(args.repo_root).resolve()
    evaluation = evaluate_incremental_value(
        [_json(_path(root, value)) for value in args.trace],
        [_json(_path(root, value)) for value in args.matured_outcome],
        horizon_sessions=args.horizon_sessions,
        evaluated_at=args.evaluated_at,
    )
    persisted = persist_incremental_evaluation(root, evaluation)
    print(
        json.dumps(
            {
                "phase": "L13",
                "mode": "evaluate",
                "evaluation_hash": evaluation["evaluation_hash"],
                "horizon_sessions": evaluation["horizon_sessions"],
                "incremental_value_status": evaluation[
                    "incremental_value_status"
                ],
                "positive_incremental_evidence": evaluation[
                    "positive_incremental_evidence"
                ],
                "persisted": persisted,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Pattern Discovery L13 shadow challenger integration."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    trace = sub.add_parser("trace")
    trace.add_argument("--decision-packet", required=True)
    trace.add_argument("--l7-claim", action="append", default=[])
    trace.add_argument("--admission-context", action="append", default=[])
    trace.add_argument("--promotion-registry")
    trace.add_argument("--horizon-sessions", type=int, required=True)
    trace.add_argument("--generated-at", required=True)
    trace.add_argument("--repo-root", default=".")
    trace.set_defaults(handler=_trace)

    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("--trace", action="append", required=True)
    evaluate.add_argument("--matured-outcome", action="append", required=True)
    evaluate.add_argument("--horizon-sessions", type=int, required=True)
    evaluate.add_argument("--evaluated-at", required=True)
    evaluate.add_argument("--repo-root", default=".")
    evaluate.set_defaults(handler=_evaluate)

    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
