from __future__ import annotations

"""Build everything (daily loop) with clear quality gates.

This is the "one command" entrypoint:
  1) scanner.app.run_daily
  2) scripts/test_pipeline.py
  3) scripts/test_golden.py
  4) scripts/validate_contract.py
  5) scanner.ui.generator

Run:
  python scripts/build_all.py

Exit codes:
  0  success
  1  a step failed
  2  interrupted (Ctrl+C)
"""

import argparse
import subprocess
import sys


def _run_step(args: list[str], *, title: str) -> int:
    """Run a step and return its exit code."""
    print(f"\n▶ {title}")
    print("  " + " ".join(args))
    try:
        p = subprocess.run(args, check=False)
        if p.returncode == 0:
            print(f"✅ OK: {title}")
        else:
            print(f"❌ FAIL ({p.returncode}): {title}")
        return int(p.returncode)
    except KeyboardInterrupt:
        return 2


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Scanner_vNext outputs + gates + dashboard")
    ap.add_argument("--skip-golden", action="store_true", help="skip scripts/test_golden.py")
    ap.add_argument("--skip-ui", action="store_true", help="skip UI generator")
    ap.add_argument("--skip-doctor", action="store_true", help="skip watchlist doctor report")
    ap.add_argument("--skip-run", action="store_true", help="skip run_daily (use existing artifacts)")
    ns = ap.parse_args()

    py = sys.executable

    steps: list[tuple[list[str], str]] = []
    if not ns.skip_run:
        steps.append(([py, "-m", "scanner.app.run_daily"], "run_daily (generate CSVs)") )

    steps.append(([py, "scripts/test_pipeline.py"], "pipeline gate") )

    if not ns.skip_golden:
        steps.append(([py, "scripts/test_golden.py"], "golden rows regression") )

    steps.append(([py, "scripts/validate_contract.py"], "UI contract validation") )

    if not ns.skip_ui:
        steps.append(([py, "-m", "scanner.ui.generator"], "build dashboard (artifacts/ui/index.html)") )

    # Non-gate: hygiene report (always returns 0)
    if not ns.skip_doctor:
        steps.append(([py, "scripts/watchlist_doctor.py"], "watchlist doctor (hygiene report)") )

    for cmd, title in steps:
        rc = _run_step(cmd, title=title)
        if rc == 2:
            print("\n⛔ Interrupted")
            return 2
        if rc != 0:
            print("\n⛔ Build stopped on first failing step.")
            return 1

    print("\n✅ ALL GREEN — outputs + gates + dashboard built")
    print("   Open: artifacts/ui/index.html")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
