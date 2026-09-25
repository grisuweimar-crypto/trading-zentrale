from scanner.research.external_evidence.universe_coverage import (
    aggregate_company_coverage,
    build_sec_ticker_map,
    exact_sec_match,
)


def test_exact_sec_mapping_does_not_fuzzy_match_or_strip_suffixes():
    mapping = build_sec_ticker_map(
        {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 999, "ticker": "SNN", "title": "Smith & Nephew plc"},
        }
    )
    aapl = exact_sec_match("aapl", mapping)
    assert aapl["sec_match_status"] == "KNOWN"
    assert aapl["cik"] == "0000320193"

    native_foreign = exact_sec_match("SN.L", mapping)
    assert native_foreign["sec_match_status"] == "UNKNOWN"
    assert native_foreign["reason_codes"] == ["NO_EXACT_SEC_TICKER_MATCH"]


def test_conflicting_current_sec_ticker_mapping_fails_closed():
    payload = {
        "0": {"cik_str": 1, "ticker": "ABC", "title": "One"},
        "1": {"cik_str": 2, "ticker": "ABC", "title": "Two"},
    }
    try:
        build_sec_ticker_map(payload)
    except ValueError as exc:
        assert "Conflicting SEC ticker mapping" in str(exc)
    else:
        raise AssertionError("conflicting SEC ticker mapping must fail closed")


def test_aggregate_company_coverage_keeps_denominators_and_feature_counts():
    rows = [
        {
            "symbol": "AAA",
            "sec_match_status": "KNOWN",
            "processing_status": "SUCCESS",
            "research_ready": True,
            "concept_coverage": {
                "pit_usable_rows": 10,
                "mapped_rows": 6,
                "us_gaap_usable_rows": 8,
                "unmapped_us_gaap_rows": 2,
                "custom_taxonomy_unmapped_rows": 2,
                "metric_row_counts": {"revenue": 3, "equity": 1},
                "unmapped_us_gaap_concepts": {"RAndD": 2},
                "custom_taxonomies": {"aaa": 2},
            },
            "feature_counts": {"revenue_yoy_change": 2, "operating_margin": 1},
        },
        {
            "symbol": "BBB.L",
            "sec_match_status": "UNKNOWN",
            "processing_status": "NOT_RUN_NO_EXACT_SEC_MATCH",
            "research_ready": False,
        },
    ]
    result = aggregate_company_coverage(rows)
    assert result["company_count"] == 2
    assert result["sec_match_status_counts"] == {"KNOWN": 1, "UNKNOWN": 1}
    assert result["research_ready_company_count"] == 1
    assert result["mapped_share_of_usable"] == 0.6
    assert result["mapped_share_of_us_gaap"] == 0.75
    assert result["metric_entity_counts"]["revenue"] == 1
    assert result["feature_entity_counts"]["revenue_yoy_change"] == 1
    assert result["top_unmapped_us_gaap_concepts"]["RAndD"] == 2
    assert result["outcome_research"] == "NOT_RUN"
    assert result["direction"] == "UNASSIGNED"
