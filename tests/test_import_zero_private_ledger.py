from decimal import Decimal
import pytest

from scripts.import_zero_private_ledger import parse, reconcile


def row(day, amount, memo):
    return {"Datum": day, "Valuta": day, "Betrag": amount,
            "Betrag storniert": "", "Status": "gebucht", "Verwendungszweck": memo}


def test_reconstruct_buy_sell_and_dividend():
    events = parse([
        row("01.09.2026", "-25,00", "Order Nr 1 ISIN US0000000001 - Kauf (TEST STK 1,25)"),
        row("02.09.2026", "-10,00", "Bruchstücke-Order zu ISIN US0000000001 - Kauf (TEST STK 0,5)"),
        row("03.09.2026", "4,00", "Order Nr 2 ISIN US0000000001 - Verkauf (TEST STK 0,25)"),
        row("04.09.2026", "0,43", "Coupons/Dividende: TEST ISIN US0000000001 STK 1,5"),
        row("05.09.2026", "50", "Gutschrift: Sparen"),
    ])
    report = reconcile(events, [{"ISIN": "US0000000001", "Anzahl": "1,5"}], [])
    assert report["matching_position_count"] == 1
    assert report["mismatches"] == []
    assert report["first_trade_dates"]["US0000000001"] == "2026-09-01"
    assert report["trade_counts"]["US0000000001"] == {"buys": 2, "sells": 1}
    assert Decimal(report["dividend_cash_eur"]) == Decimal("0.43")
    assert report["realized_profit_calculated"] is False


def test_missing_fill_is_reported_not_silently_matched():
    events = parse([row("01.09.2026", "-50,00",
                        "Order Nr 1 ISIN US0000000001 - Kauf (TEST STK 2)")])
    report = reconcile(events, [{"ISIN": "US0000000001", "Anzahl": "3"}], [])
    assert report["matching_position_count"] == 0
    assert len(report["mismatches"]) == 1


def test_unparseable_trade_is_rejected():
    with pytest.raises(ValueError, match="unparseable_trade_row"):
        parse([row("01.09.2026", "-50", "Order Nr 1 ISIN US0000000001 - Kauf (no quantity)")])
