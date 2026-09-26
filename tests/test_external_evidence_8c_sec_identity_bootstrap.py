import requests

from scanner.research.external_evidence.sec_identity_bootstrap import (
    FALLBACK_TICKER_URL,
    OFFICIAL_TICKER_URL,
    load_ticker_bootstrap,
    validate_submissions_identity,
)


class _Response:
    def __init__(self, payload=None, *, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self):
        return self._payload


def test_official_ticker_bootstrap_is_preferred():
    payload = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}

    def request_get(url, **kwargs):
        assert url == OFFICIAL_TICKER_URL
        return _Response(payload)

    result, metadata = load_ticker_bootstrap(
        user_agent="test contact test@example.invalid", request_get=request_get
    )
    assert result == payload
    assert metadata["mode"] == "SEC_OFFICIAL_LIVE"
    assert metadata["authoritative"] is True
    assert metadata["requires_current_submissions_validation"] is True


def test_403_official_uses_non_authoritative_candidate_fallback():
    payload = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    requested = []

    def request_get(url, **kwargs):
        requested.append(url)
        if url == OFFICIAL_TICKER_URL:
            return _Response(status_code=403)
        assert url == FALLBACK_TICKER_URL
        return _Response(payload)

    result, metadata = load_ticker_bootstrap(
        user_agent="test contact test@example.invalid", request_get=request_get
    )
    assert result == payload
    assert requested == [OFFICIAL_TICKER_URL, FALLBACK_TICKER_URL]
    assert metadata["mode"] == "NON_AUTHORITATIVE_MIRROR_BOOTSTRAP"
    assert metadata["authoritative"] is False
    assert metadata["official_http_status"] == 403
    assert metadata["identity_authority"] == "CURRENT_SEC_SUBMISSIONS_ONLY"


def test_submissions_identity_requires_exact_cik_and_ticker():
    valid, reasons = validate_submissions_identity(
        symbol="AAPL",
        candidate_cik="0000320193",
        submissions_payload={"cik": "320193", "tickers": ["AAPL"]},
    )
    assert valid is True
    assert reasons == []

    valid, reasons = validate_submissions_identity(
        symbol="AAPL",
        candidate_cik="0000320193",
        submissions_payload={"cik": "320193", "tickers": ["MSFT"]},
    )
    assert valid is False
    assert "TICKER_NOT_CONFIRMED_BY_CURRENT_SEC_SUBMISSIONS" in reasons

    valid, reasons = validate_submissions_identity(
        symbol="AAPL",
        candidate_cik="0000320193",
        submissions_payload={"cik": "789019", "tickers": ["AAPL"]},
    )
    assert valid is False
    assert "SUBMISSIONS_CIK_MISMATCH" in reasons
