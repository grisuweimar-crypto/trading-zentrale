import pandas as pd

# KONFIGURATION: Alle Gewichtungen zentral und anpassbar
CONFIG = {
    'mc_weight': 20.0,          # Monte Carlo Chance (0-100% -> 0 bis 20)
    'upside_weight': 15.0,      # Upside % / 30 * 15, max 15
    'kgv_max_tech': 80.0,       # Strafe Tech bei >80
    'kgv_max_mining': 35.0,     # Strafe Mining bei >35
    'growth_weight': 10.0,      # Wachstum % / 20 * 10, max 10
    'margin_high': 0.20,        # Marge >20% -> +10
    'margin_mid': 0.05,         # Marge >5% -> +5
    'elliott_weight': 20.0,     # Valid BUY-Signal
    'elliott_min_distance': 0.02,  # Min 2% bis Target
    'crv_high': 3.0,            # CRV >=3 -> +15
    'crv_mid': 2.0,             # CRV >=2 -> +10
    'crv_penalty': 1.0,         # CRV <1 -> -20
    'bonus_mc_elliott': 15.0,   # MC>70 + valid Elliott
    'bonus_crypto': 15.0,       # Krypto-Bonus
    'analyst_strong_buy': 10.0,
    'analyst_buy': 7.0,
    'analyst_hold': 2.0,
    'upside_penalty': -10.0,    # Upside <-10
    'pe_high_penalty': -20.0,   # Allgemein PE>80
    'pe_mining_penalty': -15.0  # Mining PE>35
}

SEKTOREN = {
    'TECH': ['TECH', 'GEHIRN', 'SOFTWARE', 'DIGITAL', 'TECHNOLOGY'],
    'MINING': ['MINING', 'EDELMETALLE', 'ROHSTOFFE', 'INDUSTRIE', 'MINERALS'],
    'KRYPTO': ['KRYPTO', 'CRYPTO']
}

def normalize_margin(marge):
    """Normalisiert Margin: Prozent (z.B. 20) oder Dezimal (0.20) -> Dezimal"""
    if marge is None or pd.isna(marge):
        return 0.0
    marge = float(marge)
    if marge > 1.0:
        return marge / 100.0
    return marge

def get_sector_category(sektor):
    """Robustes Sektor-Matching: exakte Liste statt 'in'"""
    sektor = str(sektor).upper().strip()
    for cat, keywords in SEKTOREN.items():
        if any(keyword in sektor for keyword in keywords):
            return cat
    return 'OTHER'

def calculate_final_score(ticker, elliott_data, fundamental_data, monte_carlo_data, current_price=0.0, crv_value=0.0):
    """
    MASTER-SCORING v5.0 - Optimiert für Watchlist-Scan:
    - Normale Margin
    - Robuste Sektoren
    - Elliott + CRV gekoppelt (keine Strafe bei keinem Setup)
    - KGV-Strafen symmetrisch
    - Zentrale Config
    """
    score = 0.0
    ticker = str(ticker).upper().strip()
    
    # 1. MONTE CARLO (unverändert, aber konfiguriert)
    mc_chance = float(monte_carlo_data.get('probability', 0) or 0)
    score += (mc_chance / 100) * CONFIG['mc_weight']
    
    # 2. FUNDAMENTAL UPSIDE
    upside = float(fundamental_data.get('upside', 0) or 0)
    if upside > 0:
        score += min((upside / 30) * CONFIG['upside_weight'], CONFIG['upside_weight'])
    elif upside < -10:
        score += CONFIG['upside_penalty']
    
    # 3. KGV / SEKTOR (robuster, symmetrische Strafen)
    sektor = get_sector_category(fundamental_data.get('sector', 'OTHER'))
    pe = float(fundamental_data.get('pe', 0) or 0)
    
    kgv_bonus = 0
    if pe > 0:
        if sektor == 'TECH':
            if pe <= 25: kgv_bonus = 15
            elif pe <= 45: kgv_bonus = 10
            if pe > CONFIG['kgv_max_tech']: score += CONFIG['pe_high_penalty']
        elif sektor == 'MINING':
            if pe <= 12: kgv_bonus = 15
            elif pe <= 20: kgv_bonus = 10
            if pe > CONFIG['kgv_max_mining']: score += CONFIG['pe_mining_penalty']
        else:  # OTHER
            if pe <= 18: kgv_bonus = 15
            elif pe <= 28: kgv_bonus = 10
            if pe > CONFIG['kgv_max_tech']: score += CONFIG['pe_high_penalty']
    score += kgv_bonus
    
    # 4. WACHSTUM & MARGE (normalisiert)
    growth = float(fundamental_data.get('growth', 0) or 0)
    score += min((growth / 20) * CONFIG['growth_weight'], CONFIG['growth_weight'])
    
    marge_norm = normalize_margin(fundamental_data.get('margin'))
    if marge_norm > CONFIG['margin_high']:
        score += 10
    elif marge_norm > CONFIG['margin_mid']:
        score += 5
    
    # 5. ANALYSTEN (konfiguriert)
    rec = str(fundamental_data.get('recommendation', 'none')).lower()
    if 'strong_buy' in rec: score += CONFIG['analyst_strong_buy']
    elif 'buy' in rec: score += CONFIG['analyst_buy']
    elif 'hold' in rec: score += CONFIG['analyst_hold']
    
    # 6. ELLIOTT & CRV (gekoppelte Logik, nur bei BUY)
    e_sig = str(elliott_data.get('signal', 'Warten')).upper()
    e_target = float(elliott_data.get('target', 0) or 0)
    is_valid_elliott_buy = False
    
    if e_sig == 'BUY' and e_target > 0 and current_price < (e_target * (1 - CONFIG['elliott_min_distance'])):
        score += CONFIG['elliott_weight']
        is_valid_elliott_buy = True
        
        # CRV nur bei validem Elliott-BUY aktivieren
        if crv_value >= CONFIG['crv_high']:
            score += 15
        elif crv_value >= CONFIG['crv_mid']:
            score += 10
        elif crv_value < CONFIG['crv_penalty']:
            score -= 20
    
    # 7. BONI
    if mc_chance > 70 and is_valid_elliott_buy:
        score += CONFIG['bonus_mc_elliott']
    
    is_krypto = get_sector_category(sektor) == 'KRYPTO' or any(x in ticker for x in ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE'])
    if is_krypto:
        score += CONFIG['bonus_crypto']
    
    return round(score, 2)