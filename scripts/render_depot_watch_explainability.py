#!/usr/bin/env python3
"""Render human-readable A-Watch evidence report from private local Watch JSON.

No portfolio positions are transmitted or committed by this tool.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from scanner.research.decision_layer.watch_explainability import explain_watch, render_markdown


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", type=Path, required=True)
    p.add_argument("--w10-manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    watch = json.loads(args.watch.read_text(encoding="utf-8"))
    w10 = json.loads(args.w10_manifest.read_text(encoding="utf-8"))
    report = explain_watch(watch, w10)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
