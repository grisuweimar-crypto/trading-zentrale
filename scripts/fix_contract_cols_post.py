from pathlib import Path
import pandas as pd

# Mögliche watchlist-Dateien, die run_daily erzeugen könnte
possible_files = [
    Path("artifacts/watchlist/watchlist_ALL.csv"),
    Path("artifacts/watchlist/watchlist_CORE.csv"),
    Path("artifacts/watchlist/watchlist_full.csv"),
    Path("artifacts/watchlist/watchlist.csv")
]

# Finde die erste existierende Datei
P = None
for file_path in possible_files:
    if file_path.exists():
        P = file_path
        print(f"📁 Found watchlist: {P}")
        break

if P is None:
    raise SystemExit("❌ No watchlist file found (tried ALL, CORE, full, watchlist.csv)")

df = pd.read_csv(P, dtype=str, keep_default_na=False)

def to_num(col, default):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(default)
    return pd.Series([default] * len(df))

# cycle
if "cycle" not in df.columns:
    if "Zyklus %" in df.columns:
        df["cycle"] = to_num("Zyklus %", 50.0)
    elif "Zyklus" in df.columns:
        df["cycle"] = to_num("Zyklus", 50.0)
    else:
        df["cycle"] = 50.0

# trend_ok
if "trend_ok" not in df.columns:
    if "Trend200" in df.columns:
        df["trend_ok"] = to_num("Trend200", 1.0).ge(1.0)
    else:
        df["trend_ok"] = True

# liquidity_ok
if "liquidity_ok" not in df.columns:
    if "DollarVolume" in df.columns:
        df["liquidity_ok"] = to_num("DollarVolume", 0.0).gt(0.0)
    elif "AvgVolume" in df.columns:
        df["liquidity_ok"] = to_num("AvgVolume", 0.0).gt(0.0)
    else:
        df["liquidity_ok"] = True

df.to_csv(P, index=False, encoding="utf-8")
print("✅ patched:", P)
print("✅ now has:", [c for c in ["cycle","trend_ok","liquidity_ok"] if c in df.columns])
