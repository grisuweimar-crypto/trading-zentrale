import re
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

SRC = Path("artifacts/watchlist/watchlist.csv")
REPORT = Path("artifacts/reports/watchlist_clean_for_excel.csv")
REPORT.parent.mkdir(parents=True, exist_ok=True)

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

def norm(x):
    x = "" if x is None else str(x).strip()
    return "" if x.lower() in ("", "nan", "none", "0") else x

def is_isin_like(x: str) -> bool:
    x = norm(x).upper()
    return bool(x) and bool(ISIN_RE.match(x))

def good_symbol(x: str) -> bool:
    x = norm(x)
    return bool(x) and not is_isin_like(x)

def pick(row, *cols):
    for c in cols:
        if c in row and norm(row[c]):
            return norm(row[c])
    return ""

def main():
    if not SRC.exists():
        raise SystemExit(f"❌ Not found: {SRC}")

    df = pd.read_csv(SRC, dtype=str, keep_default_na=False)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = SRC.with_name(f"watchlist.BACKUP_{ts}.csv")
    df.to_csv(backup, index=False, encoding="utf-8")

    rows = []
    for _, r in df.iterrows():
        name = pick(r, "Name", "name")
        isin = pick(r, "ISIN", "isin")

        candidates = [
            pick(r, "YahooSymbol"), pick(r, "yahoo_symbol"),
            pick(r, "Symbol"), pick(r, "symbol"),
            pick(r, "Ticker"), pick(r, "ticker"),
            pick(r, "Yahoo"), pick(r, "yahoo"),
        ]
        yahoo_symbol = next((c for c in candidates if good_symbol(c)), "")

        sektor = pick(r, "Sektor", "Kategorie", "Category")
        country = pick(r, "Country", "country")
        sector_off = pick(r, "Sector", "sector")
        industry = pick(r, "Industry", "industry")
        currency = pick(r, "Currency", "Währung", "currency")

        rows.append({
            "Ticker": yahoo_symbol,
            "Name": name,
            "Yahoo": yahoo_symbol,
            "ISIN": isin if is_isin_like(isin) else "",
            "Symbol": yahoo_symbol,
            "YahooSymbol": yahoo_symbol,
            "Sektor": sektor,
            "Country": country,
            "Sector": sector_off,
            "Industry": industry,
            "Currency": currency,
        })

    out = pd.DataFrame(rows)

    # Dedup: bevorzugt nach ISIN, sonst Name
    if out["ISIN"].astype(str).str.strip().ne("").any():
        out = out.sort_values(by=["ISIN", "YahooSymbol", "Name"]).drop_duplicates(subset=["ISIN"], keep="first")
    else:
        out = out.sort_values(by=["Name", "YahooSymbol"]).drop_duplicates(subset=["Name"], keep="first")

    out.to_csv(SRC, index=False, encoding="utf-8")
    out.to_csv(REPORT, index=False, sep=";", encoding="utf-8-sig")

    missing = int((out["YahooSymbol"].astype(str).str.strip() == "").sum())
    print(f"✅ Clean watchlist written: {SRC}")
    print(f"🧾 Backup created: {backup}")
    print(f"✅ Excel-friendly copy: {REPORT}")
    print(f"Rows: {len(out)} | Missing YahooSymbol: {missing}")

if __name__ == "__main__":
    main()
