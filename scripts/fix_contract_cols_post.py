from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

CONTRACT_PATH = Path("configs/watchlist_contract.json")

TRUE_SET = {"true", "t", "yes", "y", "1"}
FALSE_SET = {"false", "f", "no", "n", "0"}
CRYPTO_QUOTES = {"USD", "EUR", "USDT", "USDC", "GBP", "CHF", "BTC", "ETH"}


def _coerce_bool_series(s: pd.Series) -> pd.Series:
    """Coerce common truthy/falsey values to pandas boolean dtype."""
    if pd.api.types.is_bool_dtype(s):
        return s.astype("boolean")
    if pd.api.types.is_numeric_dtype(s):
        out = pd.Series(pd.NA, index=s.index, dtype="boolean")
        out[s == 1] = True
        out[s == 0] = False
        return out
    ss = s.astype("string").str.strip().str.lower()
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out[ss.isin(TRUE_SET)] = True
    out[ss.isin(FALSE_SET)] = False
    return out


def _to_num(df: pd.DataFrame, cols: list[str], default: float) -> pd.Series:
    for c in cols:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default)
    return pd.Series([default] * len(df), index=df.index)


def _is_crypto_symbol(text: str) -> bool:
    t = str(text or "").strip().upper()
    if not t:
        return False
    if "CRYPTO" in t:
        return True
    if "-" in t:
        base, quote = t.rsplit("-", 1)
        if quote in CRYPTO_QUOTES and len(base) >= 2:
            return True
    return t.endswith("-USD") or t in {"BTC-USD", "ETH-USD"}


def _derive_is_crypto(df: pd.DataFrame) -> pd.Series:
    idx = df.index
    out = pd.Series(False, index=idx)

    for c in ("YahooSymbol", "yahoo_symbol", "Ticker", "ticker", "Symbol", "symbol"):
        if c in df.columns:
            m = df[c].astype("string").fillna("").map(_is_crypto_symbol).fillna(False).astype(bool)
            out = out | m

    for c in ("AssetClass", "asset_class", "ScoreAssetClass", "category", "Sektor", "Sector"):
        if c in df.columns:
            s = df[c].astype("string").fillna("").str.lower()
            out = out | s.str.contains(r"\b(?:crypto|krypto|kryptow)\w*\b", regex=True, na=False)

    return out.astype(bool)


def _ensure_required_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    added: list[str] = []

    cyc_fallback = _to_num(df, ["cycle", "Zyklus %", "Zyklus"], 50.0).clip(0, 100)
    if "cycle" in df.columns:
        cyc = pd.to_numeric(df["cycle"], errors="coerce")
        df["cycle"] = cyc.fillna(cyc_fallback).clip(0, 100)
    else:
        df["cycle"] = cyc_fallback
        added.append("cycle")

    if ("trend200" in df.columns) or ("Trend200" in df.columns):
        trend200 = _to_num(df, ["trend200", "Trend200"], 0.0)
        derived_trend_ok = trend200.gt(0.0)
    else:
        derived_trend_ok = pd.Series([True] * len(df), index=df.index)

    if "trend_ok" in df.columns:
        b = _coerce_bool_series(df["trend_ok"])
        df["trend_ok"] = b.fillna(derived_trend_ok).astype(bool)
    else:
        df["trend_ok"] = derived_trend_ok.astype(bool)
        added.append("trend_ok")

    has_dv = ("dollar_volume" in df.columns) or ("DollarVolume" in df.columns)
    has_av = ("avg_volume" in df.columns) or ("AvgVolume" in df.columns)
    dv = _to_num(df, ["dollar_volume", "DollarVolume"], 0.0) if has_dv else pd.Series([0.0] * len(df), index=df.index)
    av = _to_num(df, ["avg_volume", "AvgVolume"], 0.0) if has_av else pd.Series([0.0] * len(df), index=df.index)
    derived_liq = (dv.ge(5_000_000) | av.ge(200_000)) if (has_dv or has_av) else pd.Series([True] * len(df), index=df.index)

    if "liquidity_ok" in df.columns:
        b = _coerce_bool_series(df["liquidity_ok"])
        df["liquidity_ok"] = b.fillna(derived_liq).astype(bool)
    else:
        df["liquidity_ok"] = derived_liq.astype(bool)
        added.append("liquidity_ok")

    derived_crypto = _derive_is_crypto(df)
    if "is_crypto" in df.columns:
        b = _coerce_bool_series(df["is_crypto"])
        df["is_crypto"] = b.fillna(derived_crypto).astype(bool)
    else:
        df["is_crypto"] = derived_crypto.astype(bool)
        added.append("is_crypto")

    score = pd.to_numeric(df["score"], errors="coerce") if "score" in df.columns else pd.Series([pd.NA] * len(df), index=df.index)
    status_default = pd.Series("OK", index=df.index, dtype="string")
    status_default[score.isna()] = "NA"
    zero_mask = score.fillna(0).eq(0)
    status_default[zero_mask & df["is_crypto"].astype(bool)] = "AVOID_CRYPTO_BEAR"
    status_default[zero_mask & ~df["is_crypto"].astype(bool)] = "AVOID"

    if "score_status" in df.columns:
        s = df["score_status"].astype("string").fillna("").str.strip()
        df["score_status"] = s.where(s.ne(""), status_default).astype("string")
    else:
        df["score_status"] = status_default.astype("string")
        added.append("score_status")

    return df, added


def _target_paths() -> list[Path]:
    if CONTRACT_PATH.exists():
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        applies = contract.get("applies_to") or []
        out: list[Path] = []
        seen: set[str] = set()
        for p in applies:
            pp = Path(str(p))
            k = pp.as_posix().lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(pp)
        return out

    return [
        Path("artifacts/watchlist/watchlist_ALL.csv"),
        Path("artifacts/watchlist/watchlist_CORE.csv"),
        Path("artifacts/watchlist/watchlist_full.csv"),
        Path("artifacts/watchlist/watchlist_SCORED.csv"),
        Path("artifacts/watchlist/watchlist_TOP.csv"),
        Path("artifacts/watchlist/watchlist_TOP_RELAXED.csv"),
        Path("artifacts/watchlist/watchlist_AVOID.csv"),
        Path("artifacts/watchlist/watchlist_BROKEN.csv"),
    ]


def main() -> int:
    print("[INFO] fix_contract_cols_post.py - contract fallbacks (all applies_to)")
    paths = _target_paths()

    patched = 0
    for p in paths:
        if not p.exists():
            continue

        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        df, added = _ensure_required_cols(df)
        df.to_csv(p, index=False, encoding="utf-8")

        patched += 1
        added_txt = ", ".join(added) if added else "-"
        print(f"[OK] patched {p.as_posix()} (added: {added_txt})")

    if patched == 0:
        print("[WARN] No target CSVs found; nothing patched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
