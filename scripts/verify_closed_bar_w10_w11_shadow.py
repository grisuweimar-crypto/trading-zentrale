#!/usr/bin/env python3
"""Run the REAL same-snapshot W10 -> W11 chain on isolated scanner artifacts.

Research-only runner. Requires previous same-run closed-bar scan and daily
research; never commits outputs, leaks a private position book, or backdates.
The single W11 position is explicitly synthetic and NEVER a user's position.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
TEMP = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "closed_bar_w10_w11"
TEMP.mkdir(parents=True, exist_ok=True)
PUBLIC_RECEIPT = ROOT / "artifacts/research/closed_bar_shadow/w10_w11_status.json"


def run(*argv: str, capture: bool = False) -> str:
    print("EXEC", " ".join(argv), flush=True)
    result = subprocess.run(argv, cwd=ROOT, text=True, check=True,
                            capture_output=capture)
    if capture:
        print(result.stdout[-1500:], flush=True)
    return result.stdout if capture else ""


def python(script: str, *args: str) -> None:
    run(sys.executable, script, *args)


def save_status(state: dict) -> None:
    PUBLIC_RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_RECEIPT.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    print("STATUS", json.dumps(state, sort_keys=True), flush=True)


def load(path: str | Path) -> dict:
    return json.loads(Path(path).read_text())


def main() -> int:
    daily = load(ROOT / "artifacts/research/daily_research.json")
    metadata = load(ROOT / "artifacts/research/history_metadata.json")
    sid = str(daily.get("snapshot_id") or "")
    if not sid or sid != str(metadata.get("snapshot_id")):
        raise ValueError("new_scanner_daily_snapshot_identity_mismatch")
    if metadata.get("latest_run_complete") is not True:
        raise ValueError("scanner_not_complete")
    result = {"status": "running", "scanner_snapshot_id": sid,
              "scanner_as_of": daily.get("as_of"),
              "basis": "closed_archived_ohlcv",
              "production_changed": False, "private_test_position": "synthetic_only",
              "w10": "not_run", "w11": "not_run"}
    try:
        # Exact native Phase 2 calibration; MUST be calculated for today's
        # snapshot, not reused from the checked-in previous day's result.
        python("scripts/run_probability_calibration_2.py")
        phase2 = load(ROOT / "artifacts/research/probability_calibration_2.json")
        if str(phase2.get("source", {}).get("snapshot_id")) != sid:
            raise ValueError("phase2_not_from_new_scanner_snapshot")
        python("scripts/run_w10_snapshot_orchestration.py", "init")
        python("scripts/run_w10_snapshot_orchestration.py",
               "record-runtime", "--stage", "phase2_probability",
               "--artifact", "artifacts/research/probability_calibration_2.json")

        python("scripts/run_risk_vnext_3.py", "--output", str(TEMP / "risk_vnext_3.json"))
        python("scripts/run_w10_snapshot_orchestration.py",
               "record-runtime", "--stage", "phase3_risk",
               "--artifact", str(TEMP / "risk_vnext_3.json"))
        python("scripts/audit_phase4_risk_scale.py",
               "--output", str(TEMP / "risk_scale_4.json"))
        python("scripts/run_confidence_vnext_research_4.py",
               "--phase2", "artifacts/research/probability_calibration_2.json",
               "--risk", str(TEMP / "risk_vnext_3.json"),
               "--risk-scale", str(TEMP / "risk_scale_4.json"),
               "--output", str(TEMP / "confidence_vnext_research_4.json"))
        python("scripts/run_w10_snapshot_orchestration.py",
               "record-runtime", "--stage", "phase4_confidence",
               "--artifact", str(TEMP / "confidence_vnext_research_4.json"))

        # Exact preexisting Phase-5 report with real Git source provenance;
        # never manufacture a source commit or available_from timestamp.
        phase5_path = "artifacts/research/confidence_vnext_promotion_5e.json"
        run("git", "fetch", "origin",
            "phase4e-shadow-data:refs/remotes/origin/phase4e-shadow-data", "--force")
        sha = run("git", "log", "-1", "--format=%H", "origin/phase4e-shadow-data",
                  "--", phase5_path, capture=True).strip()
        if len(sha) < 12:
            raise ValueError("missing_phase5_source_commit")
        available = run("git", "show", "-s", "--format=%cI", sha, capture=True).strip()
        report_path = TEMP / "confidence_vnext_promotion_5e.json"
        with report_path.open("w") as f:
            subprocess.run(["git", "show", f"{sha}:{phase5_path}"],
                           cwd=ROOT, stdout=f, check=True)
        python("scripts/run_w10_snapshot_orchestration.py",
               "record-git", "--stage", "phase5_governance",
               "--artifact", str(report_path), "--source-commit", sha)
        # Phase 6 is NOT supplied when no verified current-snapshot capture
        # exists. This is a recorded lack of evidence, not a neutral vote.
        python("scripts/run_w10_snapshot_orchestration.py", "resolve-phase6")
        python("scripts/run_w10_snapshot_orchestration.py", "freeze")

        python("scripts/build_integrated_decision_evidence_7a.py",
               "--phase4-report", str(TEMP / "confidence_vnext_research_4.json"),
               "--phase5-report", str(report_path),
               "--phase5-source-commit", sha,
               "--phase5-available-from", available,
               "--archive", "--force-revision")
        python("scripts/run_w10_snapshot_orchestration.py", "seal")
        python("scripts/run_w10_snapshot_orchestration.py", "validate")

        manifest = load(ROOT / "artifacts/research/decision_snapshot_w10.json")
        packets = load(ROOT / "artifacts/research/current_decision_packets_7a.json")
        if not (manifest.get("status") == "sealed"
                and manifest.get("snapshot_id") == sid
                and packets.get("snapshot_id") == sid
                and packets.get("packet_count") == daily.get("universe_size")):
            raise ValueError("W10_seal_identity_or_packet_count_mismatch")
        result["w10"] = "sealed_same_snapshot"
        result["w10_phase6"] = manifest["stages"]["phase6_elliott"]["status"]
        result["w10_packet_count"] = packets["packet_count"]
        result["phase5_source_commit"] = sha
        save_status(result)

        python("scripts/build_watch_runtime_w10.py")
        # Real W11 runner with synthetic, clearly separate position only.
        current = daily.get("symbols") or {}
        symbol = "UEC" if "UEC" in current else sorted(current)[0]
        item = current[symbol].get("current") or {}
        price = float(item.get("close"))
        if price <= 0:
            raise ValueError("synthetic_test_price_unavailable")
        # The position's timestamp must be no later than the evidence
        # decision timestamp; use actual generated_at from the same daily.
        observed = str(daily["generated_at"])
        currency = str(item.get("currency") or "USD")
        book = {
            "schema_version": "decision_depot_position_book_v1",
            "source_snapshot_id": "SYNTHETIC-CLOSED-BARS-W11",
            "as_of": observed,
            "positions": [{
                "schema_version": "decision_position_snapshot_v1",
                "symbol": symbol,
                "source_snapshot_id": "SYNTHETIC-W11-UNIT-TEST",
                "as_of": observed,
                "position_state": "long",
                "quantity": 1,
                "currency": currency,
                "average_entry_price": round(price * 0.95, 6),
                "current_price": price,
            }],
        }
        book_path = TEMP / "synthetic_position_book.json"
        book_path.write_text(json.dumps(book))
        output = TEMP / "private_watch_output"
        python("scripts/run_depot_watch_e2e_w11.py",
               "--positions", str(book_path),
               "--private-output-dir", str(output))
        receipt = load(output / "w11_acceptance_receipt.json")
        if receipt.get("snapshot_id") != sid or receipt.get("status") not in ("passed",):
            raise ValueError("W11_receipt_not_valid:" + repr({k:receipt.get(k) for k in ("status","snapshot_id")}))
        result["w11"] = str(receipt.get("status"))
        result["w11_evidence_transport"] = "compact_watch_runtime"
        result["w11_fixture"] = "synthetic_one_share_not_real_depot"
        result["status"] = "passed"
        save_status(result)
        return 0
    except Exception as ex:
        result["status"] = "blocked"
        result["blocking_error"] = f"{type(ex).__name__}:{ex}"
        save_status(result)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
