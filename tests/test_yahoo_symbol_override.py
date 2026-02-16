from __future__ import annotations

import pandas as pd

from scanner.data.enrich.yahoo_prices import _apply_symbol_override


def test_nestle_isin_override() -> None:
    row = pd.Series(
        {
            "Name": "Nestle SA",
            "ISIN": "CH0038863350",
            "YahooSymbol": "NESN.SE",
        }
    )
    sym, src = _apply_symbol_override(row, "NESN.SE", "YahooSymbol")
    assert sym == "NESN.SW"
    assert src == "override:isin"

