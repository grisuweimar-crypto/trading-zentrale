def calculate_crv(entry, target, stop_loss=None):
    """
    Berechnet das Chance-Risiko-Verhältnis.
    Ist kein Stop-Loss gegeben, wird ein Standard-Stop von 10% unter Einstieg angenommen.
    """
    try:
        entry = float(entry)
        target = float(target)
        
        if entry <= 0 or target <= entry:
            return 0.0
            
        # Falls kein Stop-Loss geliefert wird (z.B. von Elliott), 
        # nutzen wir einen defensiven Standard-Stop von 10%
        if stop_loss is None or stop_loss <= 0:
            stop_loss = entry * 0.90
            
        risk = entry - stop_loss
        reward = target - entry
        
        if risk <= 0: return 0.0
        
        return round(reward / risk, 2)
    except:
        return 0.0