# Trading-Zentrale Operating Playbook

*Version 1.0 - Quality & Control Integration*

## 🎯 System Overview

Die Trading-Zentrale ist ein quantitativer Screening-Engine für Aktien und Krypto, der auf einer 15-Faktor-Matrix basiert. Das System kombiniert:

- **Technische Analyse**: Elliott-Wellen, Relative Strength, Trend-Indikatoren
- **Fundamentale Daten**: ROE, Marge, Wachstum, Verschuldung
- **Risiko-Management**: Volatilität, Drawdown, Liquidität
- **Monte-Carlo-Simulation**: Wahrscheinlichkeitsanalyse für Preisziele

---

## 📊 Score-Komponenten

### **Score (0-200)**
Gesamtbewertung der Attraktivität. Höher ist besser.
- **≥100**: Sehr attraktiv
- **80-99**: Attraktiv
- **50-79**: Neutral
- **<50**: Nicht attraktiv

### **Opportunity Score (0-100)**
Chancen-basierte Komponente (Wachstum, Momentum, Quality).
- **≥75**: Starke Opportunity-Treiber
- **50-74**: Moderate Opportunity
- **<50**: Schwache Opportunity

### **Risk Score (0-100)**
Risiko-basierte Komponente (Volatilität, Drawdown, Liquidität).
- **≤25**: Niedriges Risiko
- **26-50**: Moderates Risiko
- **>50**: Hohes Risiko

### **Confidence Score (0-100)**
**NEU**: Datenqualität und Verlässlichkeit der Bewertung.
- **HIGH (≥75)**: Vollständige Daten, starke Signale, gutes Regime-Alignment
- **MED (50-74)**: Akzeptable Daten, moderate Signale
- **LOW (<50)**: Unvollständige Daten, schwache Signale

---

## 🎛️ Filter-Strategien

### **Bull Market (Bull-Mode)**
**Ziel**: Wachstums- und Momentum-Assets priorisieren
- **Score**: ≥80 (Top-Qualität)
- **Trend200**: >0 (Aufwärtstrend)
- **RS3M**: >0 (Relative Strength vs Markt)
- **Confidence**: ≥75 (verlässliche Daten)
- **Sektoren**: KI, Chips, Energie, Automation

### **Bear Market (Bear-Mode)**
**Ziel**: Defensive und Quality-Assets priorisieren
- **Score**: ≥60 (niedrigere Hürde)
- **RS3M**: >0 (Relative Strength wichtig)
- **Trend200**: >0 (trotz Bear-Markt)
- **Volatilität**: Niedrig (<0.3)
- **ROE/Marge**: Hoch (>15%)
- **Confidence**: HIGH (Datenqualität kritisch)

### **All-Weather Core**
**Ziel**: Stabile Portfolio-Basis
- **Score**: ≥70
- **Confidence**: HIGH
- **Liquidität**: HIGH
- **Drawdown**: <30%

---

## ⚖️ Rebalancing-Regeln

### **Turnover-Limit**
- **Maximal**: 35% des Portfolio-Wertes
- **Optimal**: 20-30% für stabile Performance

### **Rebalancing-Trigger**
1. **Wöchentlich**: Automatisch via `run_daily.py`
2. **Score-Drift**: >15 Punkte Abweichung
3. **Regime-Wechsel**: Bull→Bear oder umgekehrt
4. **Konfigurations-Update**: Neue Gewichte/Faktoren

### **Position-Sizing**
- **Top-10**: Gleichgewichtet (10% pro Position)
- **Confidence-Adjustment**: HIGH Confidence +20%, LOW Confidence -20%
- **Liquidity-Filter**: Mindestens $1M Daily Volume

---

## 📋 Daten-Quellen & Quality

### **Source of Truth**
1. **ISIN**: Primärer Identifier (unique)
2. **YahooSymbol**: Preis-Daten & Technicals
3. **Ticker**: Display & Links

### **Quality-Monitoring**
**NEU**: Automatischer Health Check via `scripts/health_report.py`
- **Missing Rates**: <10% pro Key-Spalte
- **Zero Volatility**: <5% (Datenfehler)
- **Yahoo Coverage**: >98%
- **Outlier Detection**: Winsorizing auf 1%/99%

### **Winsorizing**
**NEU**: Ausreißer-Kontrolle für stabile Scores
- **Quantile**: 1% / 99%
- **Spalten**: Growth, ROE, Margin, Volatility, RS3M, Trend200
- **Report**: Outlier-Counts im Log

---

## 🔄 Kalibration & Improvement

### **Calibration Light**
**NEU**: Lernen aus historischer Performance
- **Snapshot**: Täglich Speichern aller Scores
- **Forward Returns**: 20T Performance analysieren
- **Korrelationen**: Score vs Return, Opportunity vs Return, Risk vs Drawdown
- **Gewichts-Anpassung**: Basierend auf Korrelations-Ergebnissen

**Usage**: `python scripts/calibrate_light.py --days 60`

### **Continuous Improvement**
1. **Monatlich**: Health Check Report
2. **Quartalsweise**: Kalibration-Analyse
3. **Halbjährlich**: Gewichts-Review
4. **Jährlich**: System-Review & Refaktoring

---

## 🚨 Operating Procedures

### **Daily Routine**
1. **17:00 CEST**: Automatischer Scan via GitHub Actions (der GitHub-Cron ist waehrend der Sommerzeit auf 15:00 UTC gesetzt)
2. **Check**: Log-File auf Errors/Warnings
3. **Health**: `python scripts/health_report.py` bei Problemen
4. **Review**: Top-10 Liste + Confidence Scores
5. **Decisions**: Rebalancing basierend auf Regeln

### **Weekly Routine**
1. **Sunday**: Kalibration-Check der letzten Woche
2. **Review**: Performance vs Erwartungen
3. **Adjustments**: Parameter-Tuning bei Bedarf
4. **Planning**: Nächste Woche Sektoren/Fokus

### **Issue Response**
1. **Data Quality**: Health Report + Telegram Alert
2. **System Errors**: Log-Analysis + Fix
3. **Performance**: Kalibration + Gewichts-Anpassung
4. **Market Changes**: Regime-Filter anpassen

---

## 📁 Key Files & Structure

```
Scanner/
├── main.py                 # Haupt-Scan-Engine
├── rebalance_run.py        # Rebalancing-Logic
├── scripts/
│   ├── run_daily.py        # Automatischer Daily Runner
│   ├── health_report.py    # Data Quality Monitoring
│   ├── calibrate_light.py  # Performance-Analyse
│   └── telegram_test.py    # Alert-Testing
├── scoring_engine/
│   ├── quality/            # NEU: Quality Control Module
│   │   ├── winsorize.py    # Ausreißer-Kontrolle
│   │   ├── confidence.py   # Datenqualitäts-Score
│   │   └── snapshots.py    # Historische Snapshots
│   └── engine.py           # Scoring-Engine (mit Confidence)
├── config.py               # Gewichte & Thresholds
├── logs/scanner.log        # Zentrales Log-File
└── data/
    ├── watchlist.csv       # Source of Truth
    └── snapshots/
        └── score_history.csv # Kalibrations-Daten
```

---

## 🎯 Success Metrics

### **System Health**
- **Uptime**: >95% Daily Scans
- **Data Quality**: <5% Missing Rate
- **Alert Response**: <24h bei Issues

### **Performance Targets**
- **Hit Rate**: >60% (positiver 20D Return)
- **Score Correlation**: >0.3 mit Forward Returns
- **Turnover**: 20-35% quartalsweise

### **Quality Metrics**
- **Confidence HIGH**: >30% der Top-20
- **Outlier Rate**: <2% nach Winsorizing
- **Calibration**: Verbessernde Korrelationen über Zeit

---

## 📞 Support & Troubleshooting

### **Common Issues**
1. **Unicode Errors**: Emojis in Logs entfernt
2. **Missing Data**: Health Report prüfen
3. **Telegram Alerts**: ENV-Variablen checken
4. **Performance**: Kalibration laufen lassen

### **Debug Commands**
```bash
# Health Check
python scripts/health_report.py --alert

# Manual Scan
python scripts/run_daily.py --skip_rebalance

# Calibration Analysis
python scripts/calibrate_light.py --days 30

# Telegram Test
python scripts/telegram_test.py
```

---

## 🔄 Version History

- **v1.0**: Basis-System mit 15-Faktor-Matrix
- **v1.1**: Dashboard + Top-10 Zone
- **v1.2**: Automatisierung + Logging
- **v1.3**: **NEU** - Quality & Control Integration
  - Winsorizing für stabile Scores
  - Confidence Score für Datenqualität
  - Health Monitoring
  - Calibration Light

---

*Dieses Playbook ist lebendig und wird mit dem System weiterentwickelt.*
