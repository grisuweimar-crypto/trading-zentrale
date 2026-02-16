from pathlib import Path
import pandas as pd
import os

print("🔧 Starting fix_contract_cols_post.py...")
print(f"📁 Working directory: {os.getcwd()}")

# Liste alle Dateien im artifacts/watchlist Verzeichnis auf
watchlist_dir = Path("artifacts/watchlist")
if watchlist_dir.exists():
    print(f"📂 Files in {watchlist_dir}:")
    for file in watchlist_dir.glob("*.csv"):
        print(f"   - {file.name}")
else:
    print(f"❌ Directory {watchlist_dir} does not exist")

# Mögliche watchlist-Dateien, die run_daily erzeugen könnte
possible_files = [
    Path("artifacts/watchlist/watchlist_ALL.csv"),
    Path("artifacts/watchlist/watchlist_CORE.csv"),
    Path("artifacts/watchlist/watchlist_full.csv"),
    Path("artifacts/watchlist/watchlist.csv")
]

print("🔍 Checking for watchlist files...")
# Finde die erste existierende Datei
P = None
for file_path in possible_files:
    print(f"   Checking: {file_path} -> {file_path.exists()}")
    if file_path.exists():
        P = file_path
        print(f"📁 Found watchlist: {P}")
        break

if P is None:
    raise SystemExit("❌ No watchlist file found (tried ALL, CORE, full, watchlist.csv)")

print(f"📖 Reading {P}...")
df = pd.read_csv(P, dtype=str, keep_default_na=False)
print(f"📊 DataFrame shape: {df.shape}")
print(f"📋 Columns: {list(df.columns)}")

def to_num(col, default):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(default)
    return pd.Series([default] * len(df))

# cycle
print("🔧 Checking cycle column...")
if "cycle" not in df.columns:
    print("   ❌ cycle missing, adding...")
    if "Zyklus %" in df.columns:
        df["cycle"] = to_num("Zyklus %", 50.0)
        print("   ✅ Added cycle from Zyklus %")
    elif "Zyklus" in df.columns:
        df["cycle"] = to_num("Zyklus", 50.0)
        print("   ✅ Added cycle from Zyklus")
    else:
        df["cycle"] = 50.0
        print("   ✅ Added default cycle = 50.0")
else:
    print("   ✅ cycle already exists")

# trend_ok
print("🔧 Checking trend_ok column...")
if "trend_ok" not in df.columns:
    print("   ❌ trend_ok missing, adding...")
    if "Trend200" in df.columns:
        df["trend_ok"] = to_num("Trend200", 0.0).gt(0.0)
    else:
        df["trend_ok"] = True
        print("   ✅ Added default trend_ok = True")
else:
    print("   ✅ trend_ok already exists")

# liquidity_ok
print("🔧 Checking liquidity_ok column...")
if "liquidity_ok" not in df.columns:
    print("   ❌ liquidity_ok missing, adding...")
    if "DollarVolume" in df.columns:
        df["liquidity_ok"] = to_num("DollarVolume", 0.0).ge(5_000_000)
        print("   ✅ Added liquidity_ok from DollarVolume (≥5M)")
    elif "AvgVolume" in df.columns:
        df["liquidity_ok"] = to_num("AvgVolume", 0.0).ge(200_000)
        print("   ✅ Added liquidity_ok from AvgVolume (≥200K)")
    else:
        df["liquidity_ok"] = True
        print("   ✅ Added default liquidity_ok = True")
else:
    print("   ✅ liquidity_ok already exists")

print(f"💾 Saving to {P}...")
df.to_csv(P, index=False, encoding="utf-8")
print("✅ File saved successfully!")

# Final check
required_cols = ["cycle","trend_ok","liquidity_ok"]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    print(f"❌ STILL MISSING: {missing_cols}")
    raise SystemExit(f"❌ Failed to add columns: {missing_cols}")
else:
    print(f"✅ All required columns present: {required_cols}")
    print(f"📋 Final columns: {list(df.columns)}")
