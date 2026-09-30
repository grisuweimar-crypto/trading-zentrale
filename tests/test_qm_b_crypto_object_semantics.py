from __future__ import annotations

import json

import pytest

from scanner.research.governance.qm_b_crypto_object_semantics import (
    CryptoObjectSemanticsError,
    classify_identifier,
    load_contract,
)


def test_internal_base_reference_is_not_pair():
    row = classify_identifier("CRYPTO:BTC")
    assert row["object_class"] == "INTERNAL_BASE_REFERENCE"
    assert row["base"] == "BTC"
    assert row["quote"] is None
    assert row["stable_asset_identity_verified"] is False
    assert row["provider_instrument_identity_verified"] is False


def test_quote_pair_retains_quote_currency():
    usd = classify_identifier("BTC-USD")
    eur = classify_identifier("BTC-EUR")
    assert usd["object_class"] == "QUOTE_PAIR_REFERENCE"
    assert eur["object_class"] == "QUOTE_PAIR_REFERENCE"
    assert usd["base_component_id"] == eur["base_component_id"]
    assert usd["semantic_object_id"] != eur["semantic_object_id"]
    assert usd["quote"] == "USD"
    assert eur["quote"] == "EUR"


def test_internal_base_and_pair_share_only_component_reference():
    base = classify_identifier("CRYPTO:ETH")
    pair = classify_identifier("ETH-USD")
    assert base["base_component_id"] == pair["base_component_id"]
    assert base["semantic_object_id"] != pair["semantic_object_id"]
    assert base["object_class"] != pair["object_class"]


def test_unsupported_identifier_stays_unknown():
    row = classify_identifier("BTC")
    assert row["parse_status"] == "UNSUPPORTED"
    assert row["object_class"] == "UNKNOWN"
    assert row["semantic_object_id"] is None


def test_provider_is_not_inferred_from_pair():
    row = classify_identifier("SOL-USDT")
    assert row["provider_instrument_identity_verified"] is False
    assert "provider-unknown" in row["semantic_object_id"]


def test_contract_rejects_pair_asset_equivalence(tmp_path):
    path = tmp_path / "contract.json"
    path.write_text(json.dumps({
        "schema_version": "qm_b_crypto_object_semantics_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "rules": {
            "shared_base_token_proves_identity": False,
            "internal_base_equals_quote_pair": True,
            "different_quote_pairs_are_equivalent": False,
            "quote_currency_may_be_dropped": False,
            "provider_namespace_may_be_inferred": False,
            "venue_may_be_inferred": False,
            "current_crypto_symbol_may_rewrite_historical_identifier": False,
            "historical_pair_may_promote_current_membership": False,
            "pair_presence_proves_asset_listing": False,
            "pair_presence_proves_asset_tradability": False,
            "pair_presence_proves_project_investability": False
        },
        "promotion": {}
    }), encoding="utf-8")
    with pytest.raises(CryptoObjectSemanticsError):
        load_contract(path)


def test_contract_rejects_any_promotion(tmp_path):
    path = tmp_path / "contract.json"
    rules = {key: False for key in (
        "shared_base_token_proves_identity",
        "internal_base_equals_quote_pair",
        "different_quote_pairs_are_equivalent",
        "quote_currency_may_be_dropped",
        "provider_namespace_may_be_inferred",
        "venue_may_be_inferred",
        "current_crypto_symbol_may_rewrite_historical_identifier",
        "historical_pair_may_promote_current_membership",
        "pair_presence_proves_asset_listing",
        "pair_presence_proves_asset_tradability",
        "pair_presence_proves_project_investability",
    )}
    path.write_text(json.dumps({
        "schema_version": "qm_b_crypto_object_semantics_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "rules": rules,
        "promotion": {"stable_asset_identity": True}
    }), encoding="utf-8")
    with pytest.raises(CryptoObjectSemanticsError):
        load_contract(path)
