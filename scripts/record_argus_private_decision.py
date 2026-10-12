#!/usr/bin/env python3
"""Append or verify a private ARGUS journal stored outside the Git repository."""
import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.private_decision_journal import append, verify


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--journal", required=True, type=Path)
    action = p.add_mutually_exclusive_group(required=True)
    action.add_argument("--entry", type=Path, help="JSON record: user decision or frozen ARGUS assessment")
    action.add_argument("--verify", action="store_true")
    args = p.parse_args()
    if args.verify:
        print(f"Verified events: {verify(args.journal)}")
    else:
        entry = json.loads(args.entry.read_text(encoding="utf-8"))
        print("Journal event ID:", append(
            args.journal, entry, repo_root=Path(__file__).resolve().parents[1]
        ))


if __name__ == "__main__":
    main()
