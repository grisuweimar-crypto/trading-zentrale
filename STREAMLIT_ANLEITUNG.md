# 🚀 Streamlit Interactive Radar Dashboard — Anleitung

## Was ist neu?

Das alte **Hover-basierte JavaScript Radar** wird durch ein **Streamlit-Dashboard mit Plotly-Radar** ersetzt. Warum?
- ✅ Blockiert nicht mehr durch asynchrones JavaScript-Laden
- ✅ Interaktive Komponenten (Sidebar, Filter, Button-Auswahl)
- ✅ Live-Update des Radar-Charts bei Auswahl einer Aktie
- ✅ Bessere Mobile-Unterstützung
- ✅ Keine Hover-Bugs mehr

---

## 🏃 Schnellstart

### 1. **watchlist.csv aktualisieren**
Laufe `main.py` wie gewohnt aus, um die CSV mit aktuellen Daten zu füllen:
```bash
python main.py
```

Das erzeugt/aktualisiert auch noch das alte `index.html` (optional, kann deaktiviert werden).

### 2. **Streamlit Dashboard starten**
```bash
streamlit run streamlit_dashboard.py
```

Streamlit öffnet automatisch `http://localhost:8501` in deinem Browser.

---

## 🎯 Funktionen

### **Header & Sidebar**
- 🎯 **Sektor-Filter** (Multiselect): Wähle einen oder mehrere Sektoren
- 📊 **Asset Count**: Zeigt an, wie viele Assets geladen sind
- 📖 **Info-Boxen**: Erklärungen zu Radar und Scoring

### **Radar-Chart (oben)**
- 📊 **5-Faktor Radar**: Wachstum, Rentabilität, Sicherheit, Technik, Bewertung
- 🔵 **Blaue Linie** = Das ausgewählte Asset
- ⚫ **Graue Linie** = Sektor-Benchmark (oder global, wenn keine Sektor-Daten)
- **Benchmark wird automatisch berechnet**, wenn du eine neue Aktie auswählst

### **Tabelle (darunter)**
- 📋 **10 Spalten**: Asset, Ticker, Sektor, Kurs, ROE, Debt/Equity, Score, Signal, CRV, Zyklus
- 🖱️ **Ticker-Buttons**: Klick auf einen Ticker, um das Radar sofort zu aktualisieren
- 🔍 **Sortierbar**: Klick auf Spalten-Header zum Sortieren

---

## 🔧 Konfiguration

### **Radar-Daten**
- Daten kommen aus `watchlist.csv` Spalte `"Radar Vector"` (JSON)
- Format: `[wachstum, rentabilität, sicherheit, technik, bewertung]` (je 0–100)

### **Sektoren-Farben**
Alle 14 Sektoren sind in `streamlit_dashboard.py` definiert:
```python
SECTOR_COLORS = {
    'ki_chips': '#3b82f6',      # Blau
    'gold_silber': '#f59e0b',   # Gelb
    'energie': '#f97316',       # Orange
    ...
}
```

### **Dark Theme**
Der CSS-Block sorgt für:
- Dunkler Hintergrund (#020617)
- Grüne Accents (#10b981)
- Responsive Design

---

## 📱 Mobile Support

- **Desktop**: Vollständige Tabelle, großes Radar (380px hoch)
- **Mobile**: Compact-View, Radar responsive
- Streamlit auto-responsive (keine manuel Anpassung nötig)

---

## 🐛 Fehlerbehebung

### **"CSV nicht gefunden"**
- Stelle sicher, dass `watchlist.csv` im gleichen Verzeichnis ist
- Führe `python main.py` aus, um die CSV zu erzeugen

### **Radar-Chart zeigt "Radar-Daten ungültig"**
- CSV Spalte `"Radar Vector"` ist leer oder kein gültiges JSON
- Stelle sicher, dass `main.py` in `market/scoring.py` die Radar-Vektoren berechnet

### **Wenn es sehr langsam ist**
- `@st.cache_data` auf `load_data()` ist aktiv → Daten werden gecacht
- Für Echtzeit-Updates: `st.write(st.session_state)` zum Debuggen

---

## 📊 Vergleich: Alt vs. Neu

| Feature | Alt (HTML/Chart.js) | Neu (Streamlit/Plotly) |
|---------|---------------------|------------------------|
| **Hover-Radar** | ✅ Ja (buggy mit Streamlit) | ❌ Nein (aber Click-basiert) |
| **Rahmen** | HTML (`index.html`) | Streamlit Web-App |
| **Interaktivität** | Begrenzt (nur Hover/Filter) | ✅ Sidebar, Multiselect, Buttons |
| **Radar-Update** | Manuell Hover | ✅ Auto bei Klick |
| **Sektoren-Filter** | Buttons | ✅ Multiselect-Box |
| **Mobile** | Semi-responsive | ✅ Fully responsive |
| **Performance** | ~50KB HTML | Dynamisch ~2–3 MB |

---

## 🚀 Nächste Schritte

1. **Wenn du main.py noch brauchst**:
   - Lasse es laufen, um `watchlist.csv` zu aktualisieren
   - Das alte `index.html` wird auch generiert (optional)

2. **Wenn du nur Streamlit nutzen willst**:
   - Kommentiere `generate_dashboard()` in `main.py` aus (optional)
   - Starte nur `streamlit run streamlit_dashboard.py`

3. **Beide Dashboards parallel**:
   - `python main.py` → generiert CSV + `index.html`
   - `streamlit run streamlit_dashboard.py` → Live-Dashboard
   - Beide zeigen die gleichen Daten (gleiche CSV)

---

## 📞 Support

Falls du Fragen hast:
- Checke die **Info-Boxen** in der Sidebar
- Lese `main.py` und `streamlit_dashboard.py` für Konfiguration
- Teste mit `python -m streamlit run streamlit_dashboard.py --logger.level=debug`

---

**Viel Erfolg beim Traden! 🚀**
