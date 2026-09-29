from __future__ import annotations

import json
from pathlib import Path

from scanner.research.governance.qm_b import load_qm_b_contract


ROOT = Path(__file__).resolve().parents[1]


def test_qm_b_preserves_phase8_membership_and_coverage_vocabularies():
    qmb = load_qm_b_contract()
    phase8 = json.loads((ROOT / "configs" / "external_universe_coverage_contract_v1.json").read_text(encoding="utf-8"))
    assert qmb["universe_membership"]["membership_status_values"] == phase8["membership_status_values"]
    assert qmb["provider_coverage"]["coverage_status_values"] == phase8["coverage_status_values"]


def test_qm_b_preserves_phase8_anti_survivorship_rules():
    qmb = load_qm_b_contract()
    phase8 = json.loads((ROOT / "configs" / "external_universe_coverage_contract_v1.json").read_text(encoding="utf-8"))
    rules = phase8["rules"]
    assert rules["current_universe_may_define_historical_universe"] is False
    assert rules["current_symbol_may_replace_historical_identifier_without_mapping"] is False
    assert rules["delisted_symbols_may_be_dropped_from_historical_sample"] is False
    assert rules["unknown_membership_may_be_silently_excluded"] is False
    assert rules["missing_external_data_may_be_imputed_neutral"] is False
    assert qmb["sample_audit"]["historical_sample_may_be_filtered_to_current_symbols"] is False
    assert qmb["sample_audit"]["missing_outcomes_may_be_imputed_neutral"] is False
