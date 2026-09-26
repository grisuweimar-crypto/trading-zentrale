import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.content_parser import (
    ContentParserError,
    anchor_coverage,
    extract_evidence_anchors,
    split_sec_documents,
    validate_anchor_contract,
    visible_text,
)

ROOT = Path(__file__).resolve().parents[1]


def contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_content_parser_v1.json").read_text(
            encoding="utf-8"
        )
    )


def sample_submission():
    return b"""<SEC-DOCUMENT>
<DOCUMENT>
<TYPE>8-K
<SEQUENCE>1
<FILENAME>form8-k.htm
<DESCRIPTION>CURRENT REPORT
<TEXT><html><head><style>.x{display:none}</style><script>bad()</script></head><body>Company outlook remains unchanged.</body></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<SEQUENCE>2
<FILENAME>press.htm
<DESCRIPTION>PRESS RELEASE
<TEXT><html><body>The board declared a quarterly dividend of $0.30 per share. The company also approved a share repurchase program.</body></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>GRAPHIC
<SEQUENCE>3
<FILENAME>logo.jpg
<DESCRIPTION>GRAPHIC
<TEXT>share repurchase program</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>"""


def anchors():
    c = contract()
    docs = split_sec_documents(sample_submission())
    p = c["document_policy"]
    return extract_evidence_anchors(
        docs,
        anchor_families=c["evidence_anchor_families"],
        parser_version=c["parser_version"],
        excluded_type_prefixes=p["exclude_document_type_prefixes"],
        minimum_visible_characters=p["minimum_visible_characters"],
    )


def test_split_preserves_document_metadata_and_visible_text():
    docs = split_sec_documents(sample_submission())
    assert len(docs) == 3
    assert docs[0]["document_type"] == "8-K"
    assert docs[1]["filename"] == "press.htm"
    assert docs[1]["sequence"] == "2"
    assert "quarterly dividend" in docs[1]["visible_text"]


def test_visible_text_removes_script_style_and_decodes_entities():
    text = visible_text("<style>hidden</style><script>bad()</script><p>A &amp; B</p>")
    assert text == "A & B"


def test_no_document_blocks_fails_closed():
    with pytest.raises(ContentParserError, match="no <DOCUMENT> blocks"):
        split_sec_documents(b"plain text only")


def test_anchor_extraction_finds_families_but_assigns_no_semantics_or_direction():
    rows = anchors()
    families = {row["family"] for row in rows}
    assert {"GUIDANCE", "DIVIDEND", "BUYBACK"}.issubset(families)
    assert all(row["semantic_status"] == "ANCHOR_ONLY" for row in rows)
    assert all(row["market_direction"] == "UNKNOWN" for row in rows)


def test_graphic_document_is_excluded_even_if_it_contains_keyword():
    rows = anchors()
    assert all(row["document_type"] != "GRAPHIC" for row in rows)


def test_anchor_excerpt_hash_is_deterministic_and_offsets_point_to_phrase():
    rows = anchors()
    dividend = next(row for row in rows if row["family"] == "DIVIDEND")
    repeat = anchors()
    dividend_repeat = next(row for row in repeat if row["family"] == "DIVIDEND")
    assert dividend["excerpt_sha256"] == dividend_repeat["excerpt_sha256"]
    phrase = dividend["matched_phrase"]
    local = dividend["excerpt"].casefold()
    assert phrase.casefold() in local
    assert dividend["character_end"] > dividend["character_start"]


def test_anchor_contract_rejects_semantic_promotion():
    c = contract()
    rows = anchors()
    rows[0]["semantic_status"] = "GUIDANCE_RAISE"
    with pytest.raises(ContentParserError, match="ANCHOR_ONLY"):
        validate_anchor_contract(rows, required_fields=c["anchor_output_fields"])


def test_anchor_contract_rejects_market_direction():
    c = contract()
    rows = anchors()
    rows[0]["market_direction"] = "POSITIVE"
    with pytest.raises(ContentParserError, match="may not assign market direction"):
        validate_anchor_contract(rows, required_fields=c["anchor_output_fields"])


def test_coverage_remains_outcome_blind():
    coverage = anchor_coverage(anchors())
    assert coverage["anchor_count"] > 0
    assert coverage["all_semantic_status_anchor_only"] is True
    assert coverage["all_market_directions_unknown"] is True
    assert coverage["outcome_research"] == "NOT_RUN"
