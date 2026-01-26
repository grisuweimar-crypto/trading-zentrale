import numpy as np
import pandas as pd

def calculate_probability(history: pd.DataFrame, days=30, simulations=500):
    """Deine Monte-Carlo Logik."""
    try:
        returns = history['Close'].pct_change().dropna()
        sims = np.random.normal(returns.mean(), returns.std(), (days, simulations))
        final_prices = (1 + sims).cumprod(axis=0)[-1] * history['Close'].iloc[-1]
        return float(np.mean(final_prices > history['Close'].iloc[-1]) * 100)
    except:
        return 0.0