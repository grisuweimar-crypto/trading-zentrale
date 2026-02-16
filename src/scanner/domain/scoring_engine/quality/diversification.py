from __future__ import annotations

from typing import Any, Dict
import pandas as pd


def _norm(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return ""
    return s


def _first_present(frame: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in frame.columns:
            return n
    return None


def _best_filled(frame: pd.DataFrame, names: list[str]) -> str | None:
    best = None
    best_count = -1
    for n in names:
        if n not in frame.columns:
            continue
        s = frame[n].fillna("").astype(str).str.strip()
        cnt = int(s.ne("").sum())
        if cnt > best_count:
            best = n
            best_count = cnt
    return best


def _is_crypto_like_symbol(v: Any) -> bool:
    t = _norm(v).upper()
    if not t:
        return False
    if "CRYPTO" in t:
        return True
    if "-" in t:
        base, quote = t.rsplit("-", 1)
        if quote in {"USD", "EUR", "USDT", "USDC", "GBP", "CHF", "BTC", "ETH"} and len(base) >= 2:
            return True
    return t.endswith("-USD")


def _lin_penalty(share: float, start: float, cap: float, max_penalty: float) -> float:
    if share <= start:
        return 0.0
    if share >= cap:
        return max_penalty
    span = max(cap - start, 1e-9)
    return ((share - start) / span) * max_penalty


def compute_diversification_penalty(
    row: Any,
    universe_df: pd.DataFrame,
    asset_class: str,
    config: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = config or {}
    cat_start = float(cfg.get("DIVERS_CAT_START_SHARE", 0.18))
    cat_cap = float(cfg.get("DIVERS_CAT_CAP_SHARE", 0.45))
    cat_max = float(cfg.get("DIVERS_CAT_MAX_PENALTY", 8.0))
    crypto_start = float(cfg.get("DIVERS_CRYPTO_START_SHARE", 0.25))
    crypto_cap = float(cfg.get("DIVERS_CRYPTO_CAP_SHARE", 0.45))
    crypto_max = float(cfg.get("DIVERS_CRYPTO_MAX_PENALTY", 2.0))
    max_total = float(cfg.get("DIVERS_MAX_TOTAL_PENALTY", 10.0))

    category_col = _best_filled(universe_df, ["cluster_official", "Sector", "Sektor", "category", "sector"])
    category_value = ""
    if category_col:
        category_value = _norm(row.get(category_col, None))
        if not category_value:
            for n in ("cluster_official", "Sector", "Sektor", "category", "sector"):
                category_value = _norm(row.get(n, None))
                if category_value:
                    break

    category_share = 0.0
    category_penalty = 0.0
    if category_col and category_value:
        s = universe_df[category_col].fillna("").astype(str).str.strip()
        s = s[s.ne("")]
        if len(s) > 0:
            category_share = float(s.str.lower().eq(category_value.lower()).sum()) / float(len(s))
            category_penalty = _lin_penalty(category_share, cat_start, cat_cap, cat_max)

    crypto_share = 0.0
    crypto_penalty = 0.0
    if str(asset_class).lower() == "crypto":
        ucol = _first_present(universe_df, ["YahooSymbol", "yahoo_symbol", "Ticker", "ticker", "Symbol", "symbol"])
        if ucol:
            s = universe_df[ucol].fillna("").astype(str)
            mask = s.map(_is_crypto_like_symbol)
            if len(mask) > 0:
                crypto_share = float(mask.sum()) / float(len(mask))
                crypto_penalty = _lin_penalty(crypto_share, crypto_start, crypto_cap, crypto_max)

    total = min(max_total, category_penalty + crypto_penalty)
    if total >= 6.0:
        label = "high"
    elif total >= 3.0:
        label = "medium"
    else:
        label = "low"

    return {
        "penalty_points": round(float(total), 2),
        "label": label,
        "category": category_value,
        "category_share": round(float(category_share), 4),
        "category_penalty": round(float(category_penalty), 2),
        "crypto_share": round(float(crypto_share), 4),
        "crypto_penalty": round(float(crypto_penalty), 2),
    }
