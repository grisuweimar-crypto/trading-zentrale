import pandas as pd

def compute_cycle_oscillator(df: pd.DataFrame, period: int = 20) -> float:
    """
    Einfacher Preis-Zyklus-Oszillator (0-100).
    Idee: Wie weit ist der aktuelle Close innerhalb seiner
    detrendeten Schwankung der letzten 2*period Tage?
    
    Rückgabe:
        float: 0-100
        0   = Zyklustief
        50  = Mitte
        100 = Zyklushoch
    """
    if df is None or len(df) < period * 3:
        # Zu wenig Daten, neutrale Aussage
        return 50.0

    close = df["Close"].astype(float)

    # 1. Trend glätten (einfaches SMA)
    sma = close.rolling(window=period, min_periods=period).mean()
    detrended = close - sma

    # 2. Schwankungsbereich der letzten 2*period
    lookback = period * 2
    recent = detrended.tail(lookback)

    # Wenn noch nicht genug valide Werte vorhanden sind
    if recent.isna().sum() > lookback // 2:
        return 50.0

    min_val = recent.min()
    max_val = recent.max()

    # Schutz: Wenn kaum Schwankung, ist Zyklus sinnlos → neutral
    if max_val == min_val:
        return 50.0

    current = detrended.iloc[-1]

    # 3. Normierung auf 0-100
    cycle_idx = (current - min_val) / (max_val - min_val) * 100.0

    # Begrenzen auf [0, 100]
    cycle_idx = max(0.0, min(100.0, cycle_idx))

    return float(cycle_idx)

def classify_cycle(cycle_idx: float) -> str:
    if cycle_idx <= 20:
        return "Zyklus-Tief"
    elif cycle_idx >= 80:
        return "Zyklus-Hoch"
    else:
        return "Zyklus-neutral"

