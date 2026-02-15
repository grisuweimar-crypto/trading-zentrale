import pandas as pd
import numpy as np
import yfinance as yf
import traceback
import time
import json
import logging
from datetime import datetime, timedelta

# Importiere die neuen Module
from scoring_engine.engine import calculate_final_score_v6_from_csv
from scoring_engine.factors.risk.price_risk import price_risk_features_from_hist
from market.cycle import compute_cycle_oscillator, classify_cycle
from scoring_engine.factors.opportunity.relative_strength import rs_3m
from market.crv import calculate_crv
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from alerts.telegram import send_signal
from cloud.repository import TradingRepository
from dashboard_gen import generate_dashboard
from utils.logging_setup import setup_logging

logger = setup_logging()

def get_price_data(symbol):
    """Holt Preisdaten mit yfinance"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1y", interval="1d")
        if hist.empty:
            return None
        
        # Währung und andere Metadaten speichern
        info = ticker.info
        hist.attrs['currency'] = info.get('currency', 'USD')
        hist.attrs['symbol'] = symbol
        
        return hist
    except Exception as e:
        logger.error(f"Fehler beim Laden von {symbol}: {e}")
        return None

def main():
    logger.info("=== START MAIN SCAN ===")
    
    # --- WATCHLIST MIGRATION (Einmalig) ---
    from utils.watchlist_migrate import migrate_watchlist_inplace, print_migration_report, validate_migration_result, print_validation_report
    logger.info("🔄 Running watchlist migration (if needed)...")
    migration_report = migrate_watchlist_inplace("watchlist.csv")
    if 'error' not in migration_report:
        logger.info(f"✅ Watchlist migration completed: {migration_report}")
        print_migration_report(migration_report)
        
        # Validierung nach Migration
        validation = validate_migration_result("watchlist.csv")
        logger.info(f"🔍 Migration validation: {validation}")
        print_validation_report(validation)
    else:
        logger.error(f"❌ Migration failed: {migration_report}")
        return

    # --- WATCHLIST NORMALIZATION ---
    from utils.watchlist_normalizer import normalize_watchlist_inplace, print_normalization_report
    logger.info("🔧 Normalizing watchlist identifiers...")
    normalization_report = normalize_watchlist_inplace("watchlist.csv")
    if 'error' not in normalization_report:
        logger.info(f"✅ Watchlist normalization completed: {normalization_report}")
        print_normalization_report(normalization_report)
    else:
        logger.error(f"❌ Normalization failed: {normalization_report}")
        return

    repo = TradingRepository()
    df = repo.load_watchlist()
    
    if df.empty:
        logger.error("❌ FEHLER: watchlist.csv leer.")
        return
    
    # --- MARKET REGIME SPALTEN GLOBAL SETZEN ---
    from scoring_engine.regime.market_regime import get_market_regime
    
    # Benchmark-Daten laden
    bench_stock_hist = get_price_data("SPY")
    bench_crypto_hist = get_price_data("BTC-USD")
    
    # Regime berechnen
    stock_regime = get_market_regime("stock")
    crypto_regime = get_market_regime("crypto")
    
    stock_trend = stock_regime.trend200 if stock_regime.trend200 else 0
    crypto_trend = crypto_regime.trend200 if crypto_regime.trend200 else 0
    
    # Global setzen für Dashboard-Zugriff
    df['MarketRegimeStock'] = stock_regime.regime
    df['MarketTrend200Stock'] = stock_trend
    df['MarketRegimeCrypto'] = crypto_regime.regime
    df['MarketTrend200Crypto'] = crypto_trend
    df['MarketDate'] = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"🌍 Market Regime - Stocks: {stock_regime.regime} ({stock_trend:.3f}), Crypto: {crypto_regime.regime} ({crypto_trend:.3f})")
    logger.info("🚀 TRADING-ZENTRALE: AKTIVIERE SCAN...")
    
    processed_symbols = set()
    
    for index, row in df.iterrows():
        ticker = str(row.get('Ticker', '')).strip()
        stock_name = str(row.get('Name', '')).strip()
        
        # ISIN-Handling für Yahoo-Symbol
        row_dict = row.to_dict()
        if len(ticker) == 12 and ticker.startswith(('DE', 'CH', 'AT', 'FI', 'FR', 'GB', 'IT', 'NL', 'NO', 'SE')):
            # ISIN → Yahoo-Symbol Mapping - skip if not available
            try:
                from utils.isin_to_yahoo import isin_to_yahoo
                mapped_symbol = isin_to_yahoo(ticker)
                if mapped_symbol:
                    row_dict['Yahoo'] = mapped_symbol
                    logger.info(f"🔄 ISIN {ticker} → {mapped_symbol}")
                else:
                    logger.warning(f"⚠️ Kein Yahoo-Symbol für ISIN {ticker} gefunden")
                    continue
            except ImportError:
                logger.warning(f"⚠️ ISIN mapping nicht verfügbar für {ticker}")
                continue
        elif '.' in ticker and ticker.endswith(('.DE', '.CH', '.AT', '.PA', '.L', '.MI', '.AS', '.OL', '.ST', '.HE')):
            # Fallback: Ticker direkt verwenden
            row_dict['ISIN'] = ticker
        symbol_for_yahoo = (str(row.get('Yahoo', '') or '').strip() or ticker)

        # Dubletten-Schutz: Überspringe, wenn bereits gescannt
        symbol_key = symbol_for_yahoo.upper()
        if symbol_key in processed_symbols:
            logger.debug(f"⏭️  [{(index+1)}/{(len(df))}] {ticker} bereits gescannt, überspringe...")
            continue
        processed_symbols.add(symbol_key)

        logger.info(f"🔍 [{(index+1)}/{(len(df))}] Scanne {ticker}...")

        try:
            hist = get_price_data(symbol_for_yahoo)
            if hist is None or hist.empty:
                logger.warning(f"Kein Preishistorie für {symbol_for_yahoo} (Ticker {ticker}), überspringe.")
                continue

            # --- PREISRISIKO BERECHNEN ---
            pr = price_risk_features_from_hist(hist)
            
            # --- ZYKLUS BERECHNEN ---
            cycle_value = compute_cycle_oscillator(hist, period=20)
            cycle_status = classify_cycle(cycle_value)
            
            # 1. Preis fixieren & WÄHRUNG HOLEN
            current_price = float(hist['Close'].iloc[-1]) 
            # Holt die Währung aus den Attributen, die wir in yahoo.py gesetzt haben
            currency_code = hist.attrs.get('currency', 'USD') 
            
            # 2. Daten sammeln
            elliott = calculate_elliott(hist)
            fundamentals = get_fundamental_data(symbol_for_yahoo)
            monte_carlo = run_monte_carlo(hist)
            
            # --- NEU: CRV BERECHNEN ---
            e_target = elliott.get('target', 0)
            crv_value = calculate_crv(current_price, e_target) 
            
            # 3. Score berechnen (mit Preis & CRV Übergabe)
            result = calculate_final_score_v6_from_csv(symbol_for_yahoo)
            final_calculated_score = result.get('score', 0)
            
            # 4. Performance
            perf_pct = 0.0
            if len(hist) > 1:
                perf_pct = ((current_price - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100

            # 5. Daten in Zeile schreiben (Yahoo-Symbol speichern → Link im Dashboard geht direkt auf die richtige Aktie)
            # Nutze .loc mit expliziter Typ-Konvertierung, um dtype-Konflikte zu vermeiden
            try:
                df.loc[index, 'Yahoo'] = str(symbol_for_yahoo)
                df.loc[index, 'Akt. Kurs'] = float(round(current_price, 2))
                df.loc[index, 'Währung'] = str(currency_code)
                df.loc[index, 'Perf %'] = float(round(perf_pct, 2))
                df.loc[index, 'Score'] = float(final_calculated_score)
                df.loc[index, 'CRV'] = float(crv_value)
                # Risk-Metriken: NICHT auf 0 defaulten (0 würde das Universe zerstören)
                vol = pr.get('volatility', None)
                down = pr.get('downside_dev', None)
                mdd = pr.get('max_drawdown', None)

                df.loc[index, 'Volatility'] = None if vol is None else float(round(vol, 6))
                df.loc[index, 'DownsideDev'] = None if down is None else float(round(down, 6))
                df.loc[index, 'MaxDrawdown'] = None if mdd is None else float(round(mdd, 6))
                
                # Liquidity metrics
                avg_vol = None
                dollar_vol = None

                try:
                    if hist is not None and (not hist.empty) and ('Volume' in hist.columns):
                        avg_vol = float(hist['Volume'].dropna().tail(60).mean())
                except Exception:
                    avg_vol = None

                try:
                    if avg_vol is not None and current_price is not None:
                        dollar_vol = float(avg_vol) * float(current_price)
                except Exception:
                    dollar_vol = None

                df.loc[index, 'AvgVolume'] = None if avg_vol is None else float(round(avg_vol, 2))
                df.loc[index, 'DollarVolume'] = None if dollar_vol is None else float(round(dollar_vol, 2))
                
                # --- Trend200 (200-Tage-Linie) ---
                sma200 = None
                trend200 = None
                try:
                    if hist is not None and not hist.empty and 'Close' in hist.columns:
                        close_series = hist['Close'].dropna()
                        if len(close_series) >= 200:
                            sma200 = float(close_series.tail(200).mean())
                            last = float(close_series.iloc[-1])
                            if sma200 and sma200 > 0:
                                trend200 = (last / sma200) - 1.0
                except Exception:
                    sma200 = None
                    trend200 = None

                df.loc[index, 'SMA200'] = None if sma200 is None else round(sma200, 4)
                df.loc[index, 'Trend200'] = None if trend200 is None else round(trend200, 6)
                
                # --- Relative Strength vs Benchmark ---
                from scoring_engine.factors.opportunity.relative_strength import rs_3m
                
                ticker_u = str(ticker).upper()
                is_crypto = ("-USD" in ticker_u)
                
                bench_hist = bench_crypto_hist if is_crypto else bench_stock_hist
                rs3m = rs_3m(hist, bench_hist)
                
                df.loc[index, 'RS3M'] = None if rs3m is None else float(round(rs3m, 6))
            except Exception as e:
                logger.warning(f"⚠️ Warnung bei Zuweisung für {ticker}: {e}")
                logger.debug(traceback.format_exc())
            # Fundamentale Kennzahlen (für CSV & Dashboard)
            try:
                roe_pct = round(float(fundamentals.get('roe', 0) or 0) * 100, 2)
            except Exception:
                roe_pct = 0.0
            try:
                debt_eq = fundamentals.get('debt_to_equity', 100) or 100
            except Exception:
                debt_eq = 100
            try:
                div_pct = round(float(fundamentals.get('div_rendite', 0) or 0) * 100, 2)
            except Exception:
                div_pct = 0.0
            fcf = fundamentals.get('fcf', 0)
            enterprise_value = fundamentals.get('enterprise_value', 1) or 1
            revenue = fundamentals.get('revenue', 1) or 1
            try:
                fcf_yield = round((float(fcf) / float(enterprise_value)) * 100, 2) if enterprise_value else 0.0
            except Exception:
                fcf_yield = 0.0
            try:
                growth_pct = round(float(fundamentals.get('growth', 0) or 0) * 100, 2)
            except Exception:
                growth_pct = 0.0
            try:
                margin_pct = round(float(fundamentals.get('margin', 0) or 0) * 100, 2)
            except Exception:
                margin_pct = 0.0
            rule40 = round(growth_pct + margin_pct, 2)
            current_ratio = fundamentals.get('current_ratio', '')
            inst_own = round(float(fundamentals.get('institutional_ownership', 0) or 0) * 100, 2)

            # --- Radar-Vektor (normalisiert 0-100 für 5 Achsen) ---
            # Achsen: Wachstum, Rentabilität (ROE), Sicherheit (1/Debt), Technik (Elliott/Zyklus), Bewertung (Upside/PE)
            try:
                # Wachstum (growth_pct ist bereits in Prozent, clamp 0..50 -> 0..100)
                growth_norm = max(0.0, min(growth_pct, 50.0)) / 50.0 * 100.0
            except Exception:
                growth_norm = 0.0
            try:
                roe_norm = max(0.0, min(roe_pct, 50.0)) / 50.0 * 100.0
            except Exception:
                roe_norm = 0.0
            try:
                # Sicherheit: geringere Verschuldung -> höherer Score. debt_eq is Debt/Equity.
                de = float(debt_eq or 100)
                safety_norm = 0.0
                if de <= 0:
                    safety_norm = 100.0
                else:
                    # Map de: 0 ->100, 0.5->75, 1->50, 2->0, >2->0
                    safety_norm = max(0.0, min((2.0 - de) / 2.0, 1.0)) * 100.0
            except Exception:
                safety_norm = 0.0
            try:
                # Technik: niedriger Zyklus% ist besser. Zyklus % liegt in df['Zyklus %'] (0-100). Elliott BUY adds bonus.
                cycle_pct = float(cycle_value if 'cycle_value' in locals() else df.loc[index, 'Zyklus %'] or 50.0)
                tech_base = max(0.0, min(100.0, 100.0 - cycle_pct))
                e_sig = str(elliott.get('signal', '')).upper()
                tech_norm = min(100.0, tech_base + (20.0 if e_sig == 'BUY' else 0.0))
            except Exception:
                tech_norm = 50.0
            try:
                # Bewertung: Upside normalisiert (-50..150 -> 0..100) minus PE-Penalty
                upside_val = float(fundamentals.get('upside', 0) or 0)
                pe_val = float(fundamentals.get('pe', 0) or 0)
                upside_clamped = max(-50.0, min(upside_val, 150.0))
                upside_norm = (upside_clamped + 50.0) / 200.0 * 100.0
                pe_pen = min(pe_val / 2.0, 50.0)
                valuation_norm = max(0.0, min(100.0, upside_norm - pe_pen))
            except Exception:
                valuation_norm = 0.0

            radar_vector = [
                round(growth_norm, 2),
                round(roe_norm, 2),
                round(safety_norm, 2),
                round(tech_norm, 2),
                round(valuation_norm, 2)
            ]

            # Store as JSON string in CSV-friendly column
            try:
                df.loc[index, 'Radar Vector'] = str(json.dumps(radar_vector))
                df.loc[index, 'ROE %'] = float(roe_pct)
                df.loc[index, 'Debt/Equity'] = float(debt_eq) if debt_eq else 0.0
                df.loc[index, 'Div. Rendite %'] = float(div_pct)
                df.loc[index, 'FCF'] = float(fcf) if fcf else 0.0
                df.loc[index, 'Enterprise Value'] = float(enterprise_value) if enterprise_value else 0.0
                df.loc[index, 'Revenue'] = float(revenue) if revenue else 0.0
                df.loc[index, 'FCF Yield %'] = float(fcf_yield)
                df.loc[index, 'Growth %'] = float(growth_pct)
                df.loc[index, 'Margin %'] = float(margin_pct)
                df.loc[index, 'Rule of 40'] = float(rule40)
                df.loc[index, 'Current Ratio'] = float(current_ratio) if isinstance(current_ratio, (int, float)) else current_ratio
                df.loc[index, 'Institutional Ownership %'] = float(inst_own)
                df.loc[index, 'Elliott-Signal'] = str(elliott.get('signal', 'Warten'))
                df.loc[index, 'Elliott-Einstieg'] = float(elliott.get('entry', 0))
                df.loc[index, 'Elliott-Ausstieg'] = float(elliott.get('target', 0))
                df.loc[index, 'MC-Chance'] = float(monte_carlo.get('probability', 0))
                df.loc[index, 'PE'] = float(pe_val) if pe_val else 0.0
                # --- ZYKLUS-SPALTE ---
                df.loc[index, 'Zyklus %'] = float(round(cycle_value, 1))
                df.loc[index, 'Zyklus-Status'] = str(cycle_status)
                
                # --- CONFIDENCE SCORE PERSISTIEREN ---
                result = calculate_final_score_v6_from_csv(symbol_for_yahoo)
                if 'confidence_score' in result and result['confidence_score'] is not None:
                    df.loc[index, 'ConfidenceScore'] = float(result['confidence_score'])
                else:
                    df.loc[index, 'ConfidenceScore'] = 50.0  # Default fallback
                    
                if 'confidence_label' in result and result['confidence_label']:
                    df.loc[index, 'ConfidenceLabel'] = str(result['confidence_label'])
                else:
                    df.loc[index, 'ConfidenceLabel'] = 'MED'
                    
                if 'confidence_breakdown' in result and result['confidence_breakdown']:
                    df.loc[index, 'ConfidenceBreakdown'] = str(json.dumps(result['confidence_breakdown']))
                else:
                    df.loc[index, 'ConfidenceBreakdown'] = '{}'
            except Exception as e:
                logger.warning(f"⚠️ Warnung bei Fundamental-Zuweisung für {ticker}: {e}")
                logger.debug(traceback.format_exc())
            
            # 6. TELEGRAM (Nutzt jetzt die Variable von oben)
            # Wir prüfen das Signal direkt aus den Elliott-Daten
            if elliott.get('signal') == "BUY" and final_calculated_score > 75:
                try:
                    send_signal(ticker, elliott, final_calculated_score, name=stock_name, currency=currency_code)
                    logger.info(f"📲 Telegram-Alarm für {stock_name} raus (Score: {final_calculated_score})!")
                except Exception as e:
                    logger.warning(f"Fehler beim Senden von Telegram für {ticker}: {e}")
                    logger.debug(traceback.format_exc())

            time.sleep(0.5)

        except Exception as e:
            logger.exception(f"❌ Fehler bei {ticker}: {e}")
            

    # SPEICHERN
    final_df = df
    repo.save_watchlist(final_df)
    
    # QUALITY & CONTROL: Daily Snapshot speichern
    try:
        from config import CALIBRATION_ENABLED, CALIBRATION_SNAPSHOT_PATH
        from scoring_engine.quality.snapshots import save_daily_snapshot
        
        if CALIBRATION_ENABLED:
            logger.info("💾 Saving daily snapshot for calibration...")
            save_daily_snapshot(final_df, CALIBRATION_SNAPSHOT_PATH)
        else:
            logger.info("Daily snapshot disabled")
            
    except ImportError:
        logger.info("Calibration module not available - skipping snapshot")
    except Exception as e:
        logger.warning(f"Failed to save daily snapshot: {e}")
    
    # DASHBOARD GENERIEREN (AUCH WENN SCAN FEHLGESCHLAGEN)
    try:
        logger.info("🏗️ Erstelle Dashboard...")
        generate_dashboard()
    except Exception as e:
        logger.warning(f"⚠️ Dashboard-Fehler: {e}")
        logger.debug(traceback.format_exc())
        # Fallback: Versuche trotzdem zu Generieren
        try:
            generate_dashboard()
        except Exception:
            logger.debug("Dashboard-Fallback fehlgeschlagen.")

    logger.info("🏁 SCAN BEENDET. Scan abgeschlossen.")

if __name__ == "__main__":
    main()
