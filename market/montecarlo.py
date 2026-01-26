import numpy as np

def run_monte_carlo(hist, days=30, simulations=100):
    """
    Berechnet die Wahrscheinlichkeit, dass der Kurs in 30 Tagen höher steht.
    """
    if hist is None or len(hist) < 20:
        return {"probability": 0}

    try:
        # Log-Returns berechnen
        returns = np.log(1 + hist['Close'].pct_change())
        mu = returns.mean()
        sigma = returns.std()
        
        last_price = hist['Close'].iloc[-1]
        results = []
        
        for _ in range(simulations):
            # Simulation der Preisbewegung
            prices = [last_price]
            for _ in range(days):
                prices.append(prices[-1] * np.exp(mu + sigma * np.random.standard_normal()))
            results.append(prices[-1])
        
        # Wie viele Pfade enden über dem aktuellen Preis?
        prob = (np.sum(np.array(results) > last_price) / simulations) * 100
        return {"probability": round(prob, 1)}
        
    except Exception as e:
        print(f"Monte Carlo Fehler: {e}")
        return {"probability": 0}