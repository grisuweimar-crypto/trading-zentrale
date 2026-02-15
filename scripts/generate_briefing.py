"""Generate a watchlist briefing (Stage A deterministic + optional Stage B AI).

Outputs (always under artifacts/):
  - artifacts/reports/briefing.json
  - artifacts/reports/briefing.txt
  - artifacts/reports/briefing_ai.txt (optional)

Important constraints
---------------------
This script is **explainability-only**. It reads an existing watchlist CSV and
derives a small, human-friendly summary from existing fields. It must never
recalculate scores or alter rankings.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from scanner.data.io.paths import project_root
from scanner.reports.briefing import (
    generate_ai_briefing_text,
    load_briefing_config,
    resolve_source_csv,
    validate_briefing_json,
    build_briefing_from_csv,
    write_briefing_outputs,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/briefing.yaml", help="briefing config (yaml)")
    ap.add_argument(
        "--source",
        default=None,
        help="Override source_csv (ALL|CORE|FULL or filename). If set, wins over config.",
    )
    ap.add_argument("--top", type=int, default=None, help="Override top_n. If set, wins over config.")
    ap.add_argument(
        "--enable-ai",
        action="store_true",
        help="Enable AI enhancement regardless of config (still requires OPENAI_API_KEY).",
    )
    ap.add_argument(
        "--disable-ai",
        action="store_true",
        help="Disable AI enhancement regardless of config.",
    )
    args = ap.parse_args()

    root = project_root()
    cfg = load_briefing_config(args.config)

    source_sel = args.source if args.source is not None else cfg.source_csv
    top_n = args.top if args.top is not None else cfg.top_n
    csv_path = resolve_source_csv(source_sel)

    out_dir = Path(cfg.output_dir)
    if not out_dir.is_absolute():
        out_dir = root / out_dir

    briefing = build_briefing_from_csv(csv_path, top_n=top_n, language=cfg.language)
    ok, errs = validate_briefing_json(briefing)
    if not ok:
        print("❌ briefing.json validation failed:")
        for e in errs:
            print("  -", e)
        return 2

    # Stage B (optional)
    enable_ai = cfg.enable_ai
    if args.enable_ai:
        enable_ai = True
    if args.disable_ai:
        enable_ai = False

    ai_text = None
    if enable_ai and cfg.ai_provider.lower() == "openai":
        try:
            ai_text = generate_ai_briefing_text(briefing, model=cfg.ai_model)
            print(f"🤖 AI briefing generated (model={cfg.ai_model}).")
        except Exception as e:
            # Never break the pipeline: Stage A is the truth.
            print(f"⚠️ AI enhancement skipped: {e}")
            ai_text = None

    out = write_briefing_outputs(briefing=briefing, output_dir=out_dir, write_ai=bool(ai_text), ai_text=ai_text)
    print("✅ Briefing outputs:")
    for k, p in out.items():
        print(f"  - {k}: {p.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
