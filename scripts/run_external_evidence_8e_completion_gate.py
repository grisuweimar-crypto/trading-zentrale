#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.structured_events_8e_completion import (
    load_and_validate_8e_completion,
    write_completion_result,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STRUCTURED = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"
DEFAULT_COMPLETION = ROOT / "configs" / "external_evidence_8e_completion_v1.json"
DEFAULT_FDA = ROOT / "configs" / "external_evidence_8e_fda_approval_v1.json"
DEFAULT_DOJ = ROOT / "configs" / "external_evidence_8e_doj_antitrust_rss_v1.json"
DEFAULT_NEWS = ROOT / "configs" / "external_evidence_8e_news_discovery_v1.json"
DEFAULT_PRIMARY = ROOT / "configs" / "external_evidence_8e_primary_release_v1.json"
DEFAULT_OUTPUT = ROOT / "artifacts" / "research" / "external_evidence_8e_completion.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate and freeze Phase 8E source-layer completion without reading market outcomes."
    )
    parser.add_argument("--structured-config", default=str(DEFAULT_STRUCTURED))
    parser.add_argument("--completion-config", default=str(DEFAULT_COMPLETION))
    parser.add_argument("--fda-config", default=str(DEFAULT_FDA))
    parser.add_argument("--doj-config", default=str(DEFAULT_DOJ))
    parser.add_argument("--news-config", default=str(DEFAULT_NEWS))
    parser.add_argument("--primary-release-config", default=str(DEFAULT_PRIMARY))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    result = load_and_validate_8e_completion(
        structured_config_path=Path(args.structured_config),
        completion_config_path=Path(args.completion_config),
        fda_config_path=Path(args.fda_config),
        doj_config_path=Path(args.doj_config),
        news_config_path=Path(args.news_config),
        primary_release_config_path=Path(args.primary_release_config),
    )
    write_completion_result(result, Path(args.output))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
