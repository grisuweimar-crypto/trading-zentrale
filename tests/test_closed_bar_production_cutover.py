"""Production cutover contract: no scoring before canonical closed OHLCV."""
from pathlib import Path

from scanner.data.enrich.yahoo_prices import enrich_watchlist_with_yahoo
import pandas as pd
from datetime import datetime, timezone


def test_production_workflow_order_and_archive_flags():
    root=Path(__file__).resolve().parents[1]
    text=(root/".github/workflows/run_scanner.yml").read_text(encoding="utf-8")
    assert text.index("python scripts/prefetch_closed_bar_inputs.py") < text.index("python -m scanner.app.run_daily")
    assert text.index("python -m scanner.app.run_daily") < text.index("python scripts/generate_research_views.py --refresh-prices")
    assert "SCANNER_CLOSED_DAILY_ONLY: '1'" in text
    assert "SCANNER_PRICE_SOURCE: archive" in text
    assert "python scripts/prefetch_market_history.py --minimum-sessions 300" not in text
    assert "python scripts/record_cycle_provider_receipt.py" not in text
    assert "test ! -e artifacts/reports/cycle_provider_fetch_receipt_v1.json" in text


def test_injected_canonical_frame_has_no_forged_provider_receipt(monkeypatch):
    monkeypatch.setenv("SCANNER_CLOSED_DAILY_ONLY", "1")
    idx=pd.bdate_range(end="2026-10-16",periods=210)
    columns={("Close",t):pd.Series(range(100,310),index=idx) for t in ("TEST","SPY","BTC-USD")}
    columns.update({("Volume",t):pd.Series([1000000]*210,index=idx) for t in ("TEST","SPY","BTC-USD")})
    provider=pd.DataFrame(columns)
    provider.columns=pd.MultiIndex.from_tuples(provider.columns)
    watch=pd.DataFrame([{"YahooSymbol":"TEST","Currency":"USD"}])
    _,report=enrich_watchlist_with_yahoo(
        watch,enabled=True,downloaded_frame=provider,
        as_of=datetime(2026,10,17,18,tzinfo=timezone.utc))
    assert report.provider_fetch_started_at_utc is None
    assert report.provider_fetch_completed_at_utc is None
    assert report.provider_frame_sha256 is not None
    assert "cached projection" in report.to_text()
