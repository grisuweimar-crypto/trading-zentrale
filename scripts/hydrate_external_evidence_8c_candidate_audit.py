#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from scanner.research.external_evidence.semantic_validation import anchor_id

ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _build_relevant_anchor_index(
    anchor_rows: Iterable[Any], needed_ids: set[str]
) -> tuple[dict[str, dict[str, Any]], int]:
    """Index only anchors required by the frozen candidate sample.

    Unrelated 8C-G families may legitimately contain duplicate anchor IDs and must not
    block DIVIDEND/BUYBACK candidate hydration. Identical duplicates of a required
    anchor are harmless and are deduplicated. Conflicting duplicates remain fail-closed.
    """
    anchor_index: dict[str, dict[str, Any]] = {}
    identical_relevant_duplicates = 0
    for raw in anchor_rows:
        if not isinstance(raw, dict):
            continue
        cid = anchor_id(raw)
        if cid not in needed_ids:
            continue
        previous = anchor_index.get(cid)
        if previous is None:
            anchor_index[cid] = raw
            continue

        if str(previous.get("excerpt") or "") != str(raw.get("excerpt") or ""):
            raise ValueError(f"Conflicting duplicate relevant 8C-G anchor_id: {cid}")
        identical_relevant_duplicates += 1

    return anchor_index, identical_relevant_duplicates


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Hydrate the frozen 8C-I candidate audit sample with the original outcome-blind "
            "8C-G SEC excerpts, without changing sampling or challenger outputs."
        )
    )
    parser.add_argument(
        "--candidate-packet",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_i_real_audit" / "candidate_audit_packet.json"),
    )
    parser.add_argument(
        "--anchors",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_content_anchors.json"),
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_i_real_audit" / "candidate_audit_packet_with_evidence.json"),
    )
    args = parser.parse_args()

    packet = _load_json(Path(args.candidate_packet))
    anchors = _load_json(Path(args.anchors))

    if packet.get("schema_version") != "external_evidence_8c_i_candidate_audit_packet_v1":
        raise ValueError("Unsupported candidate audit packet schema")
    if packet.get("market_outcomes_visible") is not False:
        raise ValueError("Candidate audit packet is contaminated by market outcomes")
    if anchors.get("schema_version") != "external_evidence_8c_content_anchors_v1":
        raise ValueError("Unsupported 8C-G anchor artifact schema")
    if anchors.get("market_outcomes_read") is not False:
        raise ValueError("8C-G anchor artifact is contaminated by market outcomes")
    if anchors.get("market_direction_assigned") is not False:
        raise ValueError("8C-G anchor artifact has market direction assigned")

    anchor_rows = anchors.get("anchors")
    packet_rows = packet.get("rows")
    if not isinstance(anchor_rows, list) or not isinstance(packet_rows, list):
        raise ValueError("Expected anchors and candidate packet rows arrays")

    needed_ids: set[str] = set()
    for raw in packet_rows:
        if not isinstance(raw, dict):
            raise ValueError("Candidate audit row must be an object")
        cid = str(raw.get("anchor_id") or "")
        if not cid:
            raise ValueError("Candidate audit row is missing anchor_id")
        if cid in needed_ids:
            raise ValueError(f"Duplicate candidate audit anchor_id: {cid}")
        needed_ids.add(cid)

    anchor_index, identical_relevant_duplicates = _build_relevant_anchor_index(
        anchor_rows, needed_ids
    )

    hydrated: list[dict[str, Any]] = []
    missing: list[str] = []
    empty_excerpt: list[str] = []
    for raw in packet_rows:
        row = dict(raw)
        cid = str(row.get("anchor_id") or "")
        source = anchor_index.get(cid)
        if source is None:
            missing.append(cid)
            continue
        excerpt = source.get("excerpt")
        if not isinstance(excerpt, str) or not excerpt.strip():
            empty_excerpt.append(cid)
            continue
        row["evidence_excerpt"] = excerpt
        row["source_anchor_filename"] = source.get("filename")
        row["source_anchor_document_type"] = source.get("document_type")
        row["source_anchor_matched_phrase"] = source.get("matched_phrase")
        hydrated.append(row)

    if missing:
        raise ValueError(
            f"{len(missing)} sampled candidates have no matching 8C-G anchor; first={missing[0]}"
        )
    if empty_excerpt:
        raise ValueError(
            f"{len(empty_excerpt)} matching 8C-G anchors have empty excerpts; first={empty_excerpt[0]}"
        )
    if len(hydrated) != len(packet_rows):
        raise ValueError("Hydration changed candidate audit sample cardinality")

    output_payload = dict(packet)
    output_payload["schema_version"] = "external_evidence_8c_i_candidate_audit_packet_with_evidence_v1"
    output_payload["source_candidate_packet_schema"] = packet.get("schema_version")
    output_payload["source_anchor_schema"] = anchors.get("schema_version")
    output_payload["market_outcomes_visible"] = False
    output_payload["sampling_unchanged"] = True
    output_payload["identical_relevant_anchor_duplicates_ignored"] = identical_relevant_duplicates
    output_payload["rows"] = hydrated

    output = Path(args.output)
    _write_json(output, output_payload)
    print(
        json.dumps(
            {
                "candidate_count": len(hydrated),
                "nonempty_evidence_excerpt_count": sum(
                    1 for row in hydrated if row.get("evidence_excerpt")
                ),
                "identical_relevant_anchor_duplicates_ignored": identical_relevant_duplicates,
                "sampling_unchanged": True,
                "market_outcomes_visible": False,
                "output": str(output),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
