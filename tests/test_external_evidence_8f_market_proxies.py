from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.market_proxies_8f import (
    MarketProxy8FError,
    validate_market_proxies,
)


ROOT = Path(__file__).resolve().parents[1]


def _load():
    config = json.loads(
        (ROOT / "configs/external_evidence_8f_market_proxies_v1.json").read_text(encoding="utf-8")
    )
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )
    factor_ids = {item["factor_id"] for item in macro["factor_catalog"]}
    return config, factor_ids


def test_market_proxy_contract_passes_fail_closed():
    config, factor_ids = _load()
    result = validate_market_proxies(config, allowed_factor_ids=factor_ids)
    assert result["status"] == "PASS_MARKET_PROXY_CONTRACT"
    assert result["proxy_count"] == 4
    assert result["uranium_proxy_count"] == 2
    assert result["lithium_proxy_count"] == 2
    assert result["spot_substitution_enabled"] is False
    assert result["proxy_splicing_enabled"] is False
    assert result["direction_assigned"] is False
    assert result["outcomes_read"] is False


def test_proxy_cannot_be_relabelled_as_spot():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["rules"]["proxy_may_be_labeled_as_underlying_spot"] = True
    with pytest.raises(MarketProxy8FError, match="market-proxy rules"):
        validate_market_proxies(broken, allowed_factor_ids=factor_ids)


def test_proxy_histories_cannot_be_spliced():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["rules"]["different_proxy_series_may_be_spliced_into_one_history"] = True
    with pytest.raises(MarketProxy8FError, match="market-proxy rules"):
        validate_market_proxies(broken, allowed_factor_ids=factor_ids)


def test_proxy_requires_explicit_limitations_and_rights_status():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    proxy = next(item for item in broken["proxies"] if item["proxy_id"] == "lithium_equity_lit")
    proxy["limitations"] = []
    with pytest.raises(MarketProxy8FError, match="explicit limitations"):
        validate_market_proxies(broken, allowed_factor_ids=factor_ids)


def test_preferred_physical_and_futures_challengers_cannot_silently_disappear():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["preferred_routing"]["uranium"] = ["uranium_equity_ura"]
    with pytest.raises(MarketProxy8FError, match="SPUT physical NAV challenger"):
        validate_market_proxies(broken, allowed_factor_ids=factor_ids)

    broken = copy.deepcopy(config)
    broken["preferred_routing"]["lithium"] = ["lithium_equity_lit"]
    with pytest.raises(MarketProxy8FError, match="CME lithium futures challenger"):
        validate_market_proxies(broken, allowed_factor_ids=factor_ids)
