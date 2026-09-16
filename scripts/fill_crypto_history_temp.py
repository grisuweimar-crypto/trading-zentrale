from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import yfinance as yf

CSV = Path('artifacts/reconstruction_temp/score_history_reconstructed.csv')
REPORT = Path('artifacts/reconstruction_temp/reconstruction_report.json')
LOG = Path('artifacts/reconstruction_temp/reconstruction_log.csv')


def extract(dl: pd.DataFrame, ticker: str) -> pd.Series | None:
    if dl is None or dl.empty:
        return None
    s = None
    if isinstance(dl.columns, pd.MultiIndex):
        for key in [(ticker, 'Close'), ('Close', ticker)]:
            try:
                x = dl[key]
                if isinstance(x, pd.Series):
                    s = x
                    break
            except Exception:
                pass
    else:
        x = dl.get('Close')
        if isinstance(x, pd.Series):
            s = x
    if s is None:
        return None
    s = pd.to_numeric(s, errors='coerce').dropna()
    if s.empty:
        return None
    idx = pd.to_datetime(s.index, utc=True, errors='coerce')
    s.index = idx.tz_convert(None).normalize()
    return s[~s.index.duplicated(keep='last')].sort_index()


def hist(s: pd.Series, d: pd.Timestamp) -> pd.Series:
    return s[s.index <= d]


def vol_1y(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist(s, d)
    h = h[h.index >= d - pd.Timedelta(days=365)]
    r = h.pct_change().dropna()
    return float(r.std(ddof=1)) if len(r) >= 2 else None


def dd_1y(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist(s, d)
    h = h[h.index >= d - pd.Timedelta(days=365)]
    if len(h) < 2:
        return None
    dd = h / h.cummax() - 1.0
    return float(abs(dd.min()))


def trend200(s: pd.Series, d: pd.Timestamp) -> float | None:
    h = hist(s, d)
    if len(h) < 200:
        return None
    sma = float(h.tail(200).mean())
    return float(h.iloc[-1]) / sma - 1.0 if sma else None


def rs3m(s: pd.Series, b: pd.Series, d: pd.Timestamp) -> float | None:
    z = pd.concat([hist(s,d).rename('a'), hist(b,d).rename('b')], axis=1, join='inner').dropna()
    if len(z) <= 63:
        return None
    a0,a1 = float(z.a.iloc[-63]), float(z.a.iloc[-1])
    b0,b1 = float(z.b.iloc[-63]), float(z.b.iloc[-1])
    if not a0 or not b0:
        return None
    return (a1/a0-1.0) - (b1/b0-1.0)


def main() -> None:
    df = pd.read_csv(CSV, low_memory=False)
    original = df.copy(deep=True)
    mask = df['symbol'].fillna('').astype(str).str.startswith('CRYPTO:')
    pairs = {}
    for idx, row in df.loc[mask].iterrows():
        base = str(row['symbol']).split(':',1)[1].upper()
        cur = str(row['currency']).upper()
        pairs[idx] = f'{base}-{cur}'
    tickers = sorted(set(pairs.values()) | {'BTC-USD'})
    dmin = pd.to_datetime(df.loc[mask,'date']).min() - pd.Timedelta(days=400)
    dmax = pd.to_datetime(df.loc[mask,'date']).max() + pd.Timedelta(days=3)
    dl = yf.download(tickers=tickers, start=dmin.strftime('%Y-%m-%d'), end=dmax.strftime('%Y-%m-%d'), interval='1d', auto_adjust=True, group_by='ticker', threads=True, progress=False)
    prices = {t:s for t in tickers if (s:=extract(dl,t)) is not None}
    bench = prices.get('BTC-USD')
    appended=[]
    fill_counts={k:0 for k in ['volatility','drawdown','rs3m','trend200']}
    for idx,row in df.loc[mask].iterrows():
        t=pairs[idx]
        s=prices.get(t)
        if s is None:
            continue
        d=pd.Timestamp(row['date']).normalize()
        vals={
            'volatility': vol_1y(s,d),
            'drawdown': dd_1y(s,d),
            'trend200': trend200(s,d),
            'rs3m': rs3m(s,bench,d) if bench is not None else None,
        }
        for col,v in vals.items():
            if pd.isna(row[col]) and v is not None and math.isfinite(v):
                df.at[idx,col]=v
                fill_counts[col]+=1
                appended.append({'row_index':idx,'date':row['date'],'symbol':row['symbol'],'mapped_yahoo':t,'metric':col,'value':v,'status':'filled','method':'legacy_crypto_alias'})
    # no pre-existing value may change
    for c in df.columns:
        if c in ['volatility','drawdown','rs3m','trend200']:
            m=original[c].notna()
            if not original.loc[m,c].fillna('<NA>').astype(str).equals(df.loc[m,c].fillna('<NA>').astype(str)):
                raise RuntimeError(f'existing value changed: {c}')
        elif not original[c].fillna('<NA>').astype(str).equals(df[c].fillna('<NA>').astype(str)):
            raise RuntimeError(f'non-target column changed: {c}')
    df.to_csv(CSV,index=False)
    if LOG.exists():
        old=pd.read_csv(LOG,low_memory=False)
        pd.concat([old,pd.DataFrame(appended)],ignore_index=True).to_csv(LOG,index=False)
    rep=json.loads(REPORT.read_text(encoding='utf-8'))
    rep['legacy_crypto_alias_fill']=fill_counts
    rep['unresolved_after']={m:int(df[m].isna().sum()) for m in ['close','volatility','drawdown','rs3m','trend200']}
    rep['notes'].append('Legacy CRYPTO:* symbols were remapped to BASE-original_currency pairs and filled only where Yahoo returned verifiable history.')
    REPORT.write_text(json.dumps(rep,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'pairs':sorted(set(pairs.values())),'fetched':sorted(prices),'filled':fill_counts,'unresolved':rep['unresolved_after']},indent=2))

if __name__=='__main__':
    main()
