"""CY02-B01 partial remediation: externally checked Yahoo quote units only."""
from pathlib import Path
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1] / "data/inputs/universe_master.csv"
CURRENCIES = {"SYM":"USD","9888.HK":"HKD","9988.HK":"HKD","AMKR":"USD","ASX":"USD","KLAC":"USD","LRCX":"USD","ASTS":"USD","PL":"USD","RKLB":"USD","ACB.TO":"CAD","QBTS":"USD","RGTI":"USD"}


@pytest.fixture(scope="module")
def master():
    return pd.read_csv(ROOT, dtype=str, keep_default_na=False)


@pytest.mark.parametrize("symbol,currency", sorted(CURRENCIES.items()))
def test_currency_per_original_yahoo_listing(master, symbol, currency):
    selected = master[(master["active"] == "1") & (master["symbol"] == symbol)]
    assert len(selected) == 1, f"{symbol}: ambiguous listing"
    assert selected.iloc[0]["currency"] == currency
    assert selected.iloc[0]["isin"]
    if symbol == "ASX":
        assert selected.iloc[0]["country"] == "Taiwan"  # NYSE ADR in USD
    if symbol in ("9888.HK", "9988.HK"):
        assert selected.iloc[0]["currency"] == "HKD"


def test_vale_original_and_adr_are_separate_and_not_currency_reclassified(master):
    listed = master[(master["active"] == "1") & (master["symbol"].isin(["VALE", "VALE3.SA"]))]
    assert len(listed) == 2
    a = listed.set_index("symbol")
    assert a.loc["VALE", "currency"] == "USD"
    assert a.loc["VALE", "isin"] == "US91912E1055"
    assert a.loc["VALE3.SA", "currency"] == "BRL"
    assert a.loc["VALE3.SA", "isin"] == "BRVALEACNOR0"


def test_no_unexpected_missing_currency_for_13_confirmed_symbols(master):
    actual = master[master["symbol"].isin(CURRENCIES)]
    assert len(actual) == len(CURRENCIES)
    assert not any(actual["currency"].eq(""))
