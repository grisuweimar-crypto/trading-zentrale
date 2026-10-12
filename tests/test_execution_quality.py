from decimal import Decimal
import pytest
from scanner.research.decision_layer.execution_quality import build_evidence

HASH = "a" * 64

def test_zero_ttd_ex_ante_keeps_standard_volume_separate():
    evidence = build_evidence(isin="US88339J1051", document_kind="ex_ante",
        document_time="2026-10-12T13:32:00+02:00",
        bid="10.882", ask="10.934", quoted_spread_pct="0.48",
        standard_order_eur="300", estimated_entry_cost="2.43",
        estimated_roundtrip_cost="3.43", exchange="gettex", source_sha256=HASH)
    assert abs(Decimal(evidence["quoted_spread_pct_from_mid"]) - Decimal("0.477")) < Decimal("0.002")
    assert evidence["execution_spread_verified"] is False
    assert evidence["standard_order_eur"] == "300"
    assert evidence["is_trading_signal"] is False

def test_execution_statement_has_no_invented_spread():
    evidence = build_evidence(isin="US88339J1051", document_kind="execution_statement",
        document_time="2026-08-11T16:07:52.556+02:00",
        executed_price="11.748", units="9", cash_total="106.73",
        exchange="gettex", source_sha256=HASH)
    assert evidence["quoted_spread_pct_from_mid"] is None
    assert evidence["executed_price_eur"] == "11.748"
    assert evidence["execution_spread_verified"] is False

def test_ex_ante_cannot_claim_fill():
    with pytest.raises(ValueError, match="ex_ante_cannot_claim_execution"):
        build_evidence(isin="US88339J1051", document_kind="ex_ante",
            document_time="2026-10-12T13:32:00+02:00",
            executed_price=11, source_sha256=HASH)
