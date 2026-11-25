import pytest
from scripts.audit_cycle_external_provider import parse_prices


def test_nasdaq_daily_schema_and_bounds():
    raw=b'{"status":{"rCode":200},"data":{"tradesTable":{"rows":[{"date":"11/24/2026","close":"$10.15"},{"date":"11/25/2026","close":"$11"}]}}}'
    assert parse_prices(raw,"NASDAQ")=={"2026-11-24":10.15}


def test_stooq_invalid_or_duplicate_rejected():
    assert parse_prices(b"Date,Close\n2026-11-24,10.15\n","STOOQ")=={"2026-11-24":10.15}
    with pytest.raises(ValueError):
        parse_prices(b"Date,Close\n2026-11-24,10\n2026-11-24,11\n","STOOQ")
    with pytest.raises(ValueError):
        parse_prices(b"<html>Access denied</html>","STOOQ")
