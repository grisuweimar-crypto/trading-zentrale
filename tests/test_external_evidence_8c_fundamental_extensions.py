import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.fundamental_change import (
    build_debt_to_equity,
    build_derived_yoy_changes,
    build_free_cash_flow,
    build_growth_acceleration,
    build_operating_margin,
    build_yoy_changes,
    metric_for_row,
    resolve_debt_values,
)
from scanner.research.external_evidence.fundamental_coverage import measure_concept_coverage

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
        "comparison_basis": "FIRST_RELEASE",
        "reason_codes": [],
    }


def test_contract_keeps_coverage_and_extensions_outcome_blind():
    c = contract()
    assert c["outcome_research_enabled"] is False
    assert c["decision_integration_enabled"] is False
    assert c["coverage_policy"]["coverage_may_not_use_market_outcomes"] is True
    assert c["debt_resolution_policy"]["guess_authoritative_representation"] is False
    assert "sum_debt_total_and_components" in c["forbidden_shortcuts"]


def test_metric_mapping_respects_declared_allowed_units():
    usd = fact(
        concept="Revenues",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        unit="USD",
    )
    unsupported = dict(usd, unit="BTC")
    assert metric_for_row(usd, metric_families()) == "revenue"
    assert metric_for_row(unsupported, metric_families()) is None


def test_concept_coverage_keeps_unmapped_and_custom_rows_visible():
    mapped = fact(
        concept="Revenues",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    unmapped = fact(
        concept="ResearchAndDevelopmentExpense",
        value=20,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
    )
    custom = fact(
        concept="OurSpecialMetric",
        value=30,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        taxonomy="company-custom",
    )
    report = measure_concept_coverage([mapped, unmapped, custom], metric_families=metric_families())
    assert report["pit_usable_rows"] == 3
    assert report["mapped_rows"] == 1
    assert report["unmapped_us_gaap_rows"] == 1
    assert report["custom_taxonomy_unmapped_rows"] == 1
    assert report["metric_row_counts"]["revenue"] == 1
    assert report["unmapped_us_gaap_concepts"]["ResearchAndDevelopmentExpense"] == 1
    assert report["custom_taxonomies"]["company-custom"] == 1
    assert report["outcome_research"] == "NOT_RUN"
    assert report["direction"] == "UNASSIGNED"


def test_growth_acceleration_requires_consecutive_yoy_chain():
    y2024 = fact(
        concept="Revenues",
        value=100,
        accession="a24",
        valid_from="2024-07-30T20:00:00+00:00",
        start="2024-01-01",
        end="2024-06-30",
    )
    y2025 = fact(
        concept="Revenues",
        value=120,
        accession="a25",
        valid_from="2025-07-30T20:00:00+00:00",
        start="2025-01-01",
        end="2025-06-30",
    )
    y2026 = fact(
        concept="Revenues",
        value=150,
        accession="a26",
        valid_from="2026-07-30T20:00:00+00:00",
        start="2026-01-01",
        end="2026-06-30",
    )
    yoy = build_yoy_changes([y2024, y2025, y2026], metric="revenue", metric_families=metric_families())
    acceleration = build_growth_acceleration(yoy)
    assert len(acceleration) == 1
    assert acceleration[0]["current_growth"] == pytest.approx(0.25)
    assert acceleration[0]["prior_growth"] == pytest.approx(0.20)
    assert acceleration[0]["value"] == pytest.approx(0.05)
    assert acceleration[0]["direction"] == "UNASSIGNED"

    broken = [dict(row) for row in yoy]
    for row in broken:
        if row["current_end"] == "2026-06-30":
            row["previous_end"] = "2025-06-29"
    assert build_growth_acceleration(broken) == []


def test_operating_margin_and_fcf_yoy_changes_are_absolute_and_non_directional():
    rows = [
        fact(concept="Revenues", value=100, accession="a25", valid_from="2025-07-30T20:00:00+00:00", start="2025-01-01", end="2025-06-30"),
        fact(concept="OperatingIncomeLoss", value=20, accession="a25", valid_from="2025-07-30T20:00:00+00:00", start="2025-01-01", end="2025-06-30"),
        fact(concept="NetCashProvidedByUsedInOperatingActivities", value=100, accession="a25", valid_from="2025-07-30T20:00:00+00:00", start="2025-01-01", end="2025-06-30"),
        fact(concept="PaymentsToAcquirePropertyPlantAndEquipment", value=40, accession="a25", valid_from="2025-07-30T20:00:00+00:00", start="2025-01-01", end="2025-06-30"),
        fact(concept="Revenues", value=120, accession="a26", valid_from="2026-07-30T20:00:00+00:00", start="2026-01-01", end="2026-06-30"),
        fact(concept="OperatingIncomeLoss", value=30, accession="a26", valid_from="2026-07-30T20:00:00+00:00", start="2026-01-01", end="2026-06-30"),
        fact(concept="NetCashProvidedByUsedInOperatingActivities", value=150, accession="a26", valid_from="2026-07-30T20:00:00+00:00", start="2026-01-01", end="2026-06-30"),
        fact(concept="PaymentsToAcquirePropertyPlantAndEquipment", value=50, accession="a26", valid_from="2026-07-30T20:00:00+00:00", start="2026-01-01", end="2026-06-30"),
    ]
    margins = build_operating_margin(rows, metric_families=metric_families())
    margin_changes = build_derived_yoy_changes(
        margins,
        source_feature="operating_margin",
        output_feature="operating_margin_change",
        kind="duration",
    )
    assert len(margin_changes) == 1
    assert margin_changes[0]["absolute_change"] == pytest.approx(0.05)
    assert margin_changes[0]["direction"] == "UNASSIGNED"

    fcf = build_free_cash_flow(rows, metric_families=metric_families())
    fcf_changes = build_derived_yoy_changes(
        fcf,
        source_feature="free_cash_flow",
        output_feature="free_cash_flow_yoy_change",
        kind="duration",
    )
    assert len(fcf_changes) == 1
    assert fcf_changes[0]["absolute_change"] == pytest.approx(40.0)
    assert fcf_changes[0]["direction"] == "UNASSIGNED"


def test_debt_resolution_accepts_total_only_or_complete_component_pair():
    total_only = fact(
        concept="LongTermDebt",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    resolved = resolve_debt_values([total_only], metric_families=metric_families())
    assert len(resolved) == 1
    assert resolved[0]["status"] == "KNOWN"
    assert resolved[0]["resolution_mode"] == "TOTAL_ONLY"
    assert resolved[0]["value"] == 100

    current = dict(total_only, concept="LongTermDebtCurrent", value=20)
    noncurrent = dict(total_only, concept="LongTermDebtNoncurrent", value=80)
    pair = resolve_debt_values([current, noncurrent], metric_families=metric_families())
    assert pair[0]["status"] == "KNOWN"
    assert pair[0]["resolution_mode"] == "CURRENT_PLUS_NONCURRENT"
    assert pair[0]["value"] == 100


def test_debt_resolution_refuses_total_plus_components_and_incomplete_pairs():
    total = fact(
        concept="LongTermDebt",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    current = dict(total, concept="LongTermDebtCurrent", value=20)
    noncurrent = dict(total, concept="LongTermDebtNoncurrent", value=80)
    conflict = resolve_debt_values([total, current, noncurrent], metric_families=metric_families())[0]
    assert conflict["status"] == "CONFLICTING_SOURCES"
    assert conflict["value"] is None
    assert "TOTAL_AND_COMPONENT_DEBT_COEXIST" in conflict["reason_codes"]

    incomplete = resolve_debt_values([current], metric_families=metric_families())[0]
    assert incomplete["status"] == "UNKNOWN"
    assert "INCOMPLETE_DEBT_COMPONENT_SET" in incomplete["reason_codes"]


def test_debt_to_equity_propagates_conflicts_and_builds_ratio_only_when_resolved():
    debt_fact = fact(
        concept="LongTermDebt",
        value=100,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    equity = fact(
        concept="StockholdersEquity",
        value=50,
        accession="a1",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    resolved = resolve_debt_values([debt_fact], metric_families=metric_families())
    ratio = build_debt_to_equity(resolved, [equity], metric_families=metric_families())
    assert ratio[0]["status"] == "KNOWN"
    assert ratio[0]["value"] == pytest.approx(2.0)
    assert ratio[0]["direction"] == "UNASSIGNED"

    current = dict(debt_fact, concept="LongTermDebtCurrent", value=20)
    conflict = resolve_debt_values([debt_fact, current], metric_families=metric_families())
    propagated = build_debt_to_equity(conflict, [equity], metric_families=metric_families())
    assert propagated[0]["status"] == "CONFLICTING_SOURCES"
    assert propagated[0]["value"] is None


def test_debt_to_equity_yoy_change_uses_instant_context():
    prior_debt_fact = fact(
        concept="LongTermDebt",
        value=100,
        accession="a25",
        valid_from="2025-07-30T20:00:00+00:00",
        start=None,
        end="2025-06-30",
    )
    prior_equity = fact(
        concept="StockholdersEquity",
        value=50,
        accession="a25",
        valid_from="2025-07-30T20:00:00+00:00",
        start=None,
        end="2025-06-30",
    )
    current_debt_fact = fact(
        concept="LongTermDebt",
        value=120,
        accession="a26",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    current_equity = fact(
        concept="StockholdersEquity",
        value=60,
        accession="a26",
        valid_from="2026-07-30T20:00:00+00:00",
        start=None,
        end="2026-06-30",
    )
    facts = [prior_debt_fact, prior_equity, current_debt_fact, current_equity]
    debt = resolve_debt_values(facts, metric_families=metric_families())
    ratios = build_debt_to_equity(debt, facts, metric_families=metric_families())
    changes = build_derived_yoy_changes(
        ratios,
        source_feature="debt_to_equity",
        output_feature="debt_to_equity_change",
        kind="instant",
    )
    assert len(changes) == 1
    assert changes[0]["absolute_change"] == pytest.approx(0.0)
    assert changes[0]["direction"] == "UNASSIGNED"
