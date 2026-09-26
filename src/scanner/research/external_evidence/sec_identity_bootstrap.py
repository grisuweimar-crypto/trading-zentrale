from __future__ import annotations

from typing import Any, Callable, Mapping

import requests

from .sec_edgar import normalize_cik

OFFICIAL_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
FALLBACK_TICKER_URL = (
    "https://raw.githubusercontent.com/lwowlwowl/"
    "company_name_to_ticker/main/company_tickers.json"
)


class SecIdentityBootstrapError(RuntimeError):
    """Raised when no safe ticker/CIK bootstrap can be obtained."""


def _json_object(response: Any, *, source_url: str) -> dict[str, Any]:
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SecIdentityBootstrapError(
            f"Ticker bootstrap JSON must be an object: {source_url}"
        )
    return payload


def load_ticker_bootstrap(
    *,
    user_agent: str,
    timeout: float = 60.0,
    request_get: Callable[..., Any] = requests.get,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load ticker/CIK candidates, preferring the official SEC file.

    A fallback mirror is candidate discovery only. It never becomes identity
    authority: every candidate must later be confirmed against the current official
    SEC submissions payload for the same CIK and exact ticker.
    """
    if not str(user_agent or "").strip():
        raise SecIdentityBootstrapError("A descriptive User-Agent is required")

    headers = {
        "User-Agent": str(user_agent).strip(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }
    official_error: Exception | None = None
    official_status: int | None = None
    try:
        response = request_get(OFFICIAL_TICKER_URL, headers=headers, timeout=timeout)
        payload = _json_object(response, source_url=OFFICIAL_TICKER_URL)
        return payload, {
            "mode": "SEC_OFFICIAL_LIVE",
            "source_url": OFFICIAL_TICKER_URL,
            "authoritative": True,
            "requires_current_submissions_validation": True,
            "official_source_url": OFFICIAL_TICKER_URL,
        }
    except Exception as exc:
        official_error = exc
        response = getattr(exc, "response", None)
        official_status = getattr(response, "status_code", None)

    try:
        response = request_get(FALLBACK_TICKER_URL, headers=headers, timeout=timeout)
        payload = _json_object(response, source_url=FALLBACK_TICKER_URL)
    except Exception as mirror_error:
        raise SecIdentityBootstrapError(
            "Official SEC ticker bootstrap failed and the non-authoritative fallback "
            f"also failed ({type(mirror_error).__name__})."
        ) from mirror_error

    return payload, {
        "mode": "NON_AUTHORITATIVE_MIRROR_BOOTSTRAP",
        "source_url": FALLBACK_TICKER_URL,
        "authoritative": False,
        "requires_current_submissions_validation": True,
        "official_source_url": OFFICIAL_TICKER_URL,
        "official_error_type": type(official_error).__name__ if official_error else None,
        "official_http_status": official_status,
        "identity_authority": "CURRENT_SEC_SUBMISSIONS_ONLY",
    }


def validate_submissions_identity(
    *,
    symbol: str,
    candidate_cik: str | int,
    submissions_payload: Mapping[str, Any],
) -> tuple[bool, list[str]]:
    """Confirm one bootstrap candidate against current official SEC submissions."""
    reasons: list[str] = []
    try:
        candidate = normalize_cik(candidate_cik)
    except Exception:
        return False, ["INVALID_BOOTSTRAP_CIK"]
    try:
        payload_cik = normalize_cik(submissions_payload.get("cik", ""))
    except Exception:
        return False, ["SUBMISSIONS_MISSING_VALID_CIK"]

    if payload_cik != candidate:
        reasons.append("SUBMISSIONS_CIK_MISMATCH")

    normalized_symbol = str(symbol or "").strip().upper()
    current_tickers = {
        str(value).strip().upper()
        for value in (submissions_payload.get("tickers") or [])
        if str(value).strip()
    }
    if normalized_symbol not in current_tickers:
        reasons.append("TICKER_NOT_CONFIRMED_BY_CURRENT_SEC_SUBMISSIONS")

    return not reasons, reasons
