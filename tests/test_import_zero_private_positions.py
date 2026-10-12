from pathlib import Path
import pytest

from scripts.import_zero_private_positions import convert, number


def test_german_decimal_and_fraction():
    assert number("1.367,02702703") == pytest.approx(1367.02702703)
    assert number("0,00083641") == pytest.approx(0.00083641)


def test_realistic_zero_rows_are_exact_and_private(tmp_path):
    header = "Name;ISIN;Art;Anzahl;Kaufkurs;Kurs;Kursdatum;Wert\n"
    stock = tmp_path / "stock.csv"
    crypto = tmp_path / "crypto.csv"
    stock.write_text(header + "DRONESHIELD;AU000000DRO2;AKTIE;100;1,0845;0,9772;12.10.2026;97,72\n", encoding="utf-8")
    crypto.write_text(header + "BTC;XC000A2YY6Q6;KRYPTO;0,00083641;59.811,30;74.607,4770225;12.10.2026;62,40\n", encoding="utf-8")
    book = convert(stock, crypto, {"AU000000DRO2": "DRO.AX", "XC000A2YY6Q6": "BTC"}, "2026-10-12T11:15:00+02:00")
    assert len(book["positions"]) == 2
    assert book["positions"][1]["quantity"] == pytest.approx(0.00083641)
    assert book["positions"][1]["average_entry_price"] == pytest.approx(59811.30)
    assert book["positions"][0]["currency"] == "EUR"
    assert book["positions"][0]["symbol"] == "DRO.AX"


def test_unresolved_mapping_fails_closed(tmp_path):
    header = "Name;ISIN;Art;Anzahl;Kaufkurs;Kurs;Kursdatum;Wert\n"
    for file in ("stock.csv", "crypto.csv"):
        (tmp_path / file).write_text(header + f"ABC;{file};AKTIE;1;10;11;12.10.2026;11\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unresolved_ISIN"):
        convert(tmp_path / "stock.csv", tmp_path / "crypto.csv", {}, "2026-10-12T11:15:00+02:00")
