"""Strict readiness report for real snapshot-dependent Watch acceptance.

Never manufacture, substitute, or backdate a sealed W10 snapshot.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping


def status(daily: Mapping[str, object], sealed: Mapping[str, object]) -> dict[str, object]:
    current = str(daily.get("snapshot_id") or "")
    w10 = str(sealed.get("snapshot_id") or "")
    if not current or not w10:
        return {"ready": False, "reason": "missing_snapshot_identity", "daily_snapshot_id": current, "w10_snapshot_id": w10}
    if daily.get("as_of") is None or sealed.get("status") != "sealed":
        return {"ready": False, "reason": "missing_daily_date_or_unsealed_w10", "daily_snapshot_id": current, "w10_snapshot_id": w10}
    stages = sealed.get("stages")
    final = stages.get("final_7a") if isinstance(stages, dict) else None
    archive = stages.get("phase7a_archive") if isinstance(stages, dict) else None
    if not isinstance(final, dict) or final.get("status") != "available" or not isinstance(archive, dict) or archive.get("status") != "available":
        return {"ready": False, "reason": "w10_final_or_archive_not_available", "daily_snapshot_id": current, "w10_snapshot_id": w10}
    return {
        "ready": current == w10,
        "reason": "same_snapshot_ready" if current == w10 else "awaiting_w10_for_latest_scanner",
        "daily_snapshot_id": current,
        "w10_snapshot_id": w10,
        "scanner_as_of": daily["as_of"],
        "w10_sealed_at": sealed.get("sealed_at"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", type=Path, default=Path("artifacts/research/daily_research.json"))
    parser.add_argument("--w10", type=Path, default=Path("artifacts/research/decision_snapshot_w10.json"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    result = status(json.loads(args.daily.read_text(encoding="utf-8")), json.loads(args.w10.read_text(encoding="utf-8")))
    print(json.dumps(result, sort_keys=True))
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as stream:
            stream.write(f"ready={'true' if result['ready'] else 'false'}\n")
            stream.write(f"reason={result['reason']}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
