"""Generate combined Briefing & Realities panel.

Inputs:
  - artifacts/reports/briefing.json
  - artifacts/reports/history_delta.json
  - artifacts/reports/reality_check.json
  - artifacts/reports/segment_monitor.json

Outputs:
  - artifacts/reports/briefing_realities.txt
  - artifacts/reports/briefing_realities.json
"""

from __future__ import annotations

from scanner.reports.briefing_realities import (
    build_briefing_realities_text,
    write_briefing_realities_outputs,
)


def main() -> int:
    text, payload = build_briefing_realities_text()
    out = write_briefing_realities_outputs(text, payload)
    print("✅ Briefing & Realities outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
