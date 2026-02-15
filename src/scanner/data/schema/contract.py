from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


@dataclass
class ContractResult:
    ok: bool
    errors: list[str]
    warnings: list[str]

    def summary(self) -> str:
        if self.ok:
            w = f" (warnings={len(self.warnings)})" if self.warnings else ""
            return f"OK{w}"
        return f"FAIL (errors={len(self.errors)}, warnings={len(self.warnings)})"


def load_contract(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    return json.loads(p.read_text(encoding="utf-8"))


def _sample_ids(df: pd.DataFrame, mask: pd.Series, limit: int = 5) -> list[str]:
    if mask is None or not mask.any():
        return []
    id_col = "ticker" if "ticker" in df.columns else ("Ticker" if "Ticker" in df.columns else None)
    if not id_col:
        return [str(i) for i in df.index[mask].tolist()[:limit]]
    return df.loc[mask, id_col].astype(str).head(limit).tolist()


def _coerce_bool(series: pd.Series) -> pd.Series:
    """Coerce common truthy/falsey inputs to pandas boolean dtype.

    Accepts:
      - bool
      - 0/1 numbers
      - strings: true/false, yes/no, y/n, 0/1
    Unknown values become <NA>.
    """
    s = series.copy()
    if s.dtype == bool:
        return s.astype("boolean")

    # numeric 0/1
    if pd.api.types.is_numeric_dtype(s):
        out = pd.Series(pd.NA, index=s.index, dtype="boolean")
        out[s == 1] = True
        out[s == 0] = False
        return out

    # strings
    ss = s.astype("string").str.strip().str.lower()
    true_set = {"true", "t", "yes", "y", "1"}
    false_set = {"false", "f", "no", "n", "0"}
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out[ss.isin(true_set)] = True
    out[ss.isin(false_set)] = False
    return out


def _validate_string(df: pd.DataFrame, col: str, spec: dict[str, Any], *, required: bool) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    allow_null = bool(spec.get("allow_null", False))
    min_len = int(spec.get("min_len", 0) or 0)
    allowed = spec.get("allowed")

    s = df[col].astype("string")
    if not allow_null:
        is_bad = s.isna() | s.str.strip().eq("")
        if is_bad.any():
            kind = "error" if required else "warning"
            msg = f"{col}: {is_bad.sum()} empty/null values (sample: {_sample_ids(df, is_bad)})"
            (errors if kind == "error" else warnings).append(msg)

    if min_len > 0:
        lens = s.fillna("").str.strip().str.len()
        is_bad = lens < min_len
        if is_bad.any():
            kind = "error" if required else "warning"
            msg = f"{col}: {is_bad.sum()} values shorter than {min_len} (sample: {_sample_ids(df, is_bad)})"
            (errors if kind == "error" else warnings).append(msg)

    if allowed is not None:
        allowed_set = set(map(str, allowed))
        vals = s.fillna("").astype(str)
        is_bad = ~vals.isin(allowed_set)
        # empty is already handled above; ignore empties here to reduce noise
        is_bad &= vals.str.strip().ne("")
        if is_bad.any():
            msg = f"{col}: {is_bad.sum()} values not in allowed set {sorted(allowed_set)} (sample: {df.loc[is_bad, col].astype(str).head(5).tolist()})"
            (errors if required else warnings).append(msg)

    return errors, warnings


def _validate_number(df: pd.DataFrame, col: str, spec: dict[str, Any], *, required: bool) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    allow_null = bool(spec.get("allow_null", False))
    min_v = spec.get("min", None)
    max_v = spec.get("max", None)

    n = pd.to_numeric(df[col], errors="coerce")
    is_na = n.isna()
    if (not allow_null) and is_na.any():
        msg = f"{col}: {is_na.sum()} non-numeric/NA values (sample: {_sample_ids(df, is_na)})"
        (errors if required else warnings).append(msg)

    if min_v is not None:
        is_bad = (~is_na) & (n < float(min_v))
        if is_bad.any():
            msg = f"{col}: {is_bad.sum()} values < {min_v} (sample: {_sample_ids(df, is_bad)})"
            (errors if required else warnings).append(msg)

    if max_v is not None:
        is_bad = (~is_na) & (n > float(max_v))
        if is_bad.any():
            msg = f"{col}: {is_bad.sum()} values > {max_v} (sample: {_sample_ids(df, is_bad)})"
            (errors if required else warnings).append(msg)

    return errors, warnings


def _validate_bool(df: pd.DataFrame, col: str, spec: dict[str, Any], *, required: bool) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    b = _coerce_bool(df[col])
    is_bad = b.isna()
    if is_bad.any():
        msg = f"{col}: {is_bad.sum()} values not coercible to bool (sample: {df.loc[is_bad, col].astype(str).head(5).tolist()})"
        (errors if required else warnings).append(msg)

    return errors, warnings


def validate_df_against_contract(
    df: pd.DataFrame,
    contract: dict[str, Any],
    *,
    strict_optional: bool = False,
) -> ContractResult:
    errors: list[str] = []
    warnings: list[str] = []

    req: dict[str, Any] = contract.get("required_columns", {}) or {}
    opt: dict[str, Any] = contract.get("optional_columns", {}) or {}

    # column presence
    for col, spec in req.items():
        if col not in df.columns:
            errors.append(f"missing required column: {col}")

    for col, spec in opt.items():
        if col not in df.columns:
            (errors if strict_optional else warnings).append(f"missing optional column: {col}")

    # type validation
    def validate_cols(columns: dict[str, Any], required_flag: bool):
        nonlocal errors, warnings
        for col, spec in columns.items():
            if col not in df.columns:
                continue
            t = str(spec.get("type", "")).lower().strip()
            if t == "string":
                e, w = _validate_string(df, col, spec, required=required_flag)
            elif t == "number":
                e, w = _validate_number(df, col, spec, required=required_flag)
            elif t == "bool":
                e, w = _validate_bool(df, col, spec, required=required_flag)
            else:
                e, w = ([f"{col}: unknown type '{t}' in contract"], [])
            errors.extend(e)
            warnings.extend(w)

    validate_cols(req, True)
    validate_cols(opt, strict_optional)

    # row rules (minimal, hard-coded)
    # Rule: score_status matches score and asset class
    if all(c in df.columns for c in ("score", "score_status", "is_crypto")):
        score = pd.to_numeric(df["score"], errors="coerce")
        status = df["score_status"].astype("string").fillna("").str.strip()
        crypto = _coerce_bool(df["is_crypto"]).fillna(False).astype(bool)

        # ignore NA/ERROR rows for the OK/AVOID consistency rule
        ignore = status.isin(["NA", "ERROR"])

        zero = score.fillna(0.0).eq(0.0)
        bad_zero_crypto = zero & crypto & ~status.eq("AVOID_CRYPTO_BEAR") & ~ignore
        bad_zero_stock = zero & (~crypto) & ~status.eq("AVOID") & ~ignore
        bad_pos = (~zero) & status.isin(["AVOID", "AVOID_CRYPTO_BEAR"]) & ~ignore

        if bad_zero_crypto.any():
            errors.append(
                f"row_rule score_status_matches_score_and_asset_class: {bad_zero_crypto.sum()} crypto rows have score==0 but score_status!=AVOID_CRYPTO_BEAR (sample: {_sample_ids(df, bad_zero_crypto)})"
            )
        if bad_zero_stock.any():
            errors.append(
                f"row_rule score_status_matches_score_and_asset_class: {bad_zero_stock.sum()} non-crypto rows have score==0 but score_status!=AVOID (sample: {_sample_ids(df, bad_zero_stock)})"
            )
        if bad_pos.any():
            errors.append(
                f"row_rule score_status_matches_score_and_asset_class: {bad_pos.sum()} rows have score>0 but score_status indicates avoid (sample: {_sample_ids(df, bad_pos)})"
            )

    ok = len(errors) == 0
    return ContractResult(ok=ok, errors=errors, warnings=warnings)


def validate_csv(
    csv_path: str | Path,
    contract_path: str | Path,
    *,
    strict_optional: bool = False,
    read_kwargs: dict[str, Any] | None = None,
) -> ContractResult:
    p_csv = Path(csv_path)
    p_contract = Path(contract_path)
    if not p_csv.exists():
        return ContractResult(False, [f"missing CSV: {p_csv}"], [])
    if not p_contract.exists():
        return ContractResult(False, [f"missing contract: {p_contract}"], [])

    contract = load_contract(p_contract)
    kwargs = read_kwargs or {}
    df = pd.read_csv(p_csv, **kwargs)
    return validate_df_against_contract(df, contract, strict_optional=strict_optional)
