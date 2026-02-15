from __future__ import annotations
import pandas as pd

ALIASES = {
    # UI-Namen -> CSV-Spalten
    "Cycle": "Zyklus %",
    "Confidence": "ConfidenceScore",  # falls bei dir so heißt
    "Momentum": "RS3M",
    "Trend200": "Trend200",
}

def _col(df: pd.DataFrame, key: str) -> str | None:
    # direkte Spalte?
    if key in df.columns:
        return key
    # alias?
    ali = ALIASES.get(key)
    if ali and ali in df.columns:
        return ali
    return None

def _ensure_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # TrendOK (wenn Trend200 existiert)
    if "TrendOK" not in out.columns:
        tcol = _col(out, "Trend200")
        if tcol:
            out["TrendOK"] = pd.to_numeric(out[tcol], errors="coerce") > 0

    # LiquidityOK (sehr simpel, kannst du später verfeinern)
    # Wenn DollarVolume existiert -> OK ab 5 Mio; sonst wenn AvgVolume -> OK ab 200k
    if "LiquidityOK" not in out.columns:
        if "DollarVolume" in out.columns:
            dv = pd.to_numeric(out["DollarVolume"], errors="coerce")
            out["LiquidityOK"] = dv >= 5_000_000
        elif "AvgVolume" in out.columns:
            av = pd.to_numeric(out["AvgVolume"], errors="coerce")
            out["LiquidityOK"] = av >= 200_000

    return out

def _iter_filters(preset: dict):
    """
    Unterstützt beide Formate:

    1) dict:
       "filters": {
         "Cycle": {"min": 10, "max": 40},
         "TrendOK": {"eq": true}
       }

    2) list:
       "filters": [
         {"field": "Cycle", "min": 10, "max": 40},
         {"field": "TrendOK", "eq": true}
       ]
    """
    filters = preset.get("filters", [])

    if isinstance(filters, dict):
        for key, rule in filters.items():
            yield key, rule

    elif isinstance(filters, list):
        for i, item in enumerate(filters):
            if not isinstance(item, dict):
                raise TypeError(
                    f"Filter #{i+1} must be an object/dict, got {type(item).__name__}"
                )
            key = item.get("field") or item.get("key") or item.get("name")
            if not key:
                raise KeyError(
                    f"Filter #{i+1} is missing 'field' (or 'key'/'name'): {item}"
                )
            # rest ist die eigentliche Rule
            rule = {k: v for k, v in item.items() if k not in ("field", "key", "name")}
            yield key, rule

    else:
        raise TypeError(
            f"preset['filters'] must be dict or list, got {type(filters).__name__}"
        )

def apply_preset(df: pd.DataFrame, preset: dict) -> pd.DataFrame:
    df = _ensure_derived_columns(df)

    mask = pd.Series(True, index=df.index)

    for key, rule in _iter_filters(preset):
        if not isinstance(rule, dict):
            raise TypeError(
                f"Filter '{key}' must be an object/dict, got {type(rule).__name__}"
            )

        col = key if key in ("TrendOK", "LiquidityOK") else _col(df, key)
        if col is None or col not in df.columns:
            print(f"⚠️ Preset-Filter ignoriert (Spalte fehlt): {key}")
            continue

        s = df[col]

        # numeric comparisons via min/max
        if "min" in rule or "max" in rule:
            x = pd.to_numeric(s, errors="coerce")
            if "min" in rule:
                mask &= x >= float(rule["min"])
            if "max" in rule:
                mask &= x <= float(rule["max"])

        # eq comparison (bool/string/number)
        if "eq" in rule:
            mask &= (s == rule["eq"])

        # in list
        if "in" in rule:
            mask &= s.isin(rule["in"])

        # neq comparison (not equal)
        if "neq" in rule:
            mask &= (s != rule["neq"])

        # not_in list
        if "not_in" in rule:
            mask &= ~s.isin(rule["not_in"])

        # notnull
        if rule.get("notnull") is True:
            mask &= s.notna()

    out = df.loc[mask].copy()

    # sorting
    sort_specs = preset.get("sort", [])
    if sort_specs:
        cols = []
        ascending = []
        for spec in sort_specs:
            if ":" in spec:
                name, direction = spec.split(":", 1)
            else:
                name, direction = spec, "desc"

            scol = _col(out, name) or name
            if scol not in out.columns:
                continue
            cols.append(scol)
            ascending.append(direction.strip().lower() != "desc")

        if cols:
            out = out.sort_values(cols, ascending=ascending, kind="mergesort")

    # limit
    limit = preset.get("limit")
    if isinstance(limit, int) and limit > 0:
        out = out.head(limit)
        
    
    return out