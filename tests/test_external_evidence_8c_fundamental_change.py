import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.fundamental_change import (
    asof_latest_rows,
    build_free_cash_flow,
    build_operating_margin,
    build_yoy_changes,
    first_release_rows,
    metric_for_row,
)

ROOT = Path(__file__).resolve().parents[1]


def contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_fundamental_change_v1.json").read_text(encoding="utf-8")
    )


def metric_families():
    return contract()["metric_families"]


def fact(
    *,
    concept,
    value,
    accession,
    valid_from,
    start="2026-01-01",
    end="2026-06-30",
    unit="USD",
    fiscal_period="Q2",
    taxonomy="us-gaap",
    cik="0000000001",
    pit_status="SAFE",
):
    return {
        "cik": cik,
        "taxonomy": taxonomy,
        "concept": concept,
        "unit": unit,
        "value": value,
        "start": start,
        "end": end,
        "fiscal_period": fiscal_period,
        "accession_number": accession,
        "valid_from": valid_from,
        "pit_status": pit_status,
        "reason_codes": [],
    }


def test_contract_keeps_feature_construction_non_directional_and_outcomes_off():
    c = contract()
    assert c["outcome_research_enabled"] is False
    assert c["decision_integration_enabled"] is False
    assert c["default_research_basis"] == "FIRST_RELEASE"
    assert c["direction_policy"]["feature_value_is_not_signal"] is True
    assert c["direction_policy"]["no_thresholds_before_outcome_research"] is True
    assert "inspect_market_outcomes_during_8c_c_feature_construction" in c["forbidden_shortcuts"]


def test_metric_mapping_accepts_only_explicit_us_gaap_concepts():
    row = fact(
        concept="RevenueFromContractWithCustomerExcludingAssessedTax",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    assert metric_for_row(row, metric_families()) == "revenue"
    custom = dict(row, taxonomy="example-custom", concept="OurRevenueMetric")
    assert metric_for_row(custom, metric_families()) is None


def test_first_release_preserves_earliest_version_of_same_fact_context():
    original = fact(
        concept="Revenues",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    amended = fact(
        concept="Revenues",
        value=105,
        accession="a2",
        valid_from="2026-08-20T20:00:00+00:00",
    )
    selected = first_release_rows([amended, original])
    assert len(selected) == 1
    assert selected[0]["accession_number"] == "a1"
    assert selected[0]["value"] == 100
    assert selected[0]["comparison_basis"] == "FIRST_RELEASE"


def test_asof_latest_changes_only_after_amendment_becomes_valid():
    original = fact(
        concept="Revenues",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    amended = fact(
        concept="Revenues",
        value=105,
        accession="a2",
        valid_from="2026-08-20T20:00:00+00:00",
    )
    before = asof_latest_rows([original, amended], as_of="2026-08-10T00:00:00+00:00")
    after = asof_latest_rows([original, amended], as_of="2026-08-21T00:00:00+00:00")
    assert before[0]["accession_number"] == "a1"
    assert before[0]["value"] == 100
    assert after[0]["accession_number"] == "a2"
    assert after[0]["value"] == 105


def test_yoy_revenue_change_requires_same_concept_unit_and_comparable_duration():
    previous = fact(
        concept="Revenues",
        value=100,
        accession="p1",
        valid_from="2025-07-30T20:00:00+00:00",
        start="2025-01-01",
        end="2025-06-30",
    )
    current = fact(
        concept="Revenues",
        value=120,
        accession="c1",
        valid_from="2026-07-30T20:00:00+00:00",
        start="2026-01-01",
        end="2026-06-30",
    )
    result = build_yoy_changes([previous, current], metric="revenue", metric_families=metric_families())
    assert len(result) == 1
    assert result[0]["pct_change"] == pytest.approx(0.20)
    assert result[0]["absolute_change"] == 20
    assert result[0]["direction"] == "UNASSIGNED"

    wrong_unit = dict(previous, unit="EUR")
    assert build_yoy_changes([wrong_unit, current], metric="revenue", metric_families=metric_families()) == []


def test_positive_revenue_growth_is_not_automatically_positive_evidence():
    previous = fact(
        concept="Revenues",
        value=100,
        accession="p1",
        valid_from="2025-07-30T20:00:00+00:00",
        start="2025-01-01",
        end="2025-06-30",
    )
    current = fact(
        concept="Revenues",
        value=150,
        accession="c1",
        valid_from="2026-07-30T20:00:00+00:00",
        start="2026-01-01",
        end="2026-06-30",
    )
    row = build_yoy_changes([previous, current], metric="revenue", metric_families=metric_families())[0]
    assert row["pct_change"] > 0
    assert row["direction"] == "UNASSIGNED"


def test_operating_margin_requires_same_accession_period_and_unit():
    revenue = fact(
        concept="Revenues",
        value=200,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    op_income = fact(
        concept="OperatingIncomeLoss",
        value=40,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    result = build_operating_margin([revenue, op_income], metric_families=metric_families())
    assert len(result) == 1
    assert result[0]["value"] == pytest.approx(0.20)
    assert result[0]["direction"] == "UNASSIGNED"

    different_accession = dict(op_income, accession_number="a2")
    assert build_operating_margin([revenue, different_accession], metric_families=metric_families()) == []


def test_free_cash_flow_requires_same_accession_context_and_stays_non_directional():
    ocf = fact(
        concept="NetCashProvidedByUsedInOperatingActivities",
        value=150,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    capex = fact(
        concept="PaymentsToAcquirePropertyPlantAndEquipment",
        value=50,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    result = build_free_cash_flow([ocf, capex], metric_families=metric_families())
    assert len(result) == 1
    assert result[0]["value"] == 100
    assert result[0]["direction"] == "UNASSIGNED"


def test_zero_previous_value_never_invents_infinite_growth():
    previous = fact(
        concept="Revenues",
        value=0,
        accession="p1",
        valid_from="2025-07-30T20:00:00+00:00",
        start="2025-01-01",
        end="2025-06-30",
    )
    current = fact(
        concept="Revenues",
        value=100,
        accession="c1",
        valid_from="2026-07-30T20:00:00+00:00",
        start="2026-01-01",
        end="2026-06-30",
    )
    row = build_yoy_changes([previous, current], metric="revenue", metric_families=metric_families())[0]
    assert row["pct_change"] is None
    assert row["status"] == "PARTIAL"
    assert "ZERO_PREVIOUS_VALUE" in row["reason_codes"]
