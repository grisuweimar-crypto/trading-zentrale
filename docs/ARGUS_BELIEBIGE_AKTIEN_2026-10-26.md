# ARGUS: beliebige Aktie prüfen – Forschungsbetrieb, unabhängig von fünf Pilotwerten

**Geltungsbereich:** Eine einzelne an einem Börsenplatz handelbare, eindeutig identifizierte Aktie kann über `scripts/argus_zones.py` unabhängig davon untersucht werden, ob sie auf der Watchlist, im Depot oder im M5-Pilotuniversum steht. Der Aufruf erzeugt echte M1–M3-Kursbereiche und bedingte Szenarien. Er ändert weder Scanner-/Portfolio-/Decision-Logik noch eine Order.

## Aufruf

```bash
PYTHONPATH=src:. python scripts/argus_zones.py \
  --symbol SAP.DE --asof 2026-10-23 --output-dir /tmp/argus-sap
PYTHONPATH=src:. python scripts/argus_zones.py \
  --symbol 7203.T --asof 2026-10-23 --output-dir /tmp/argus-toyota
PYTHONPATH=src:. python scripts/argus_zones.py \
  --symbol FISV --asof 2026-10-23 --output-dir /tmp/argus-fiserv
```

Alternativ in GitHub unter **Actions → ARGUS arbitrary-equity single-stock zone research (read-only) → Run workflow** Börsenticker und vollständig abgeschlossenen Sitzungstag wählen. Die Aktion speichert die Ergebnisse als Artefakt, die nearest Support-/Resistance-Bänder und bedingte Einstieg-/Invalidierungs-/Zielbereiche erscheinen in den CI-Logs. Das ist kein simuliertes Ergebnis aus Screenshots, sondern verwendet den vorhandenen Yahoo-OHLCV-Adapter und die Forschungsregeln des ARGUS-Zonenmodells.

**Symbole sind Anbieter-Ticker, keine Firmennamen:** `AAPL`, `FISV`, `SAP.DE`, `7203.T`, `RR.L` usw. müssen der tatsächlichen Notierung entsprechen. Bei Börsenwechseln, Namenswechseln oder mehreren Listings wird nicht geraten. Anbieterdaten müssen `instrumentType=EQUITY`, exakten Symbolnamen, Handelsplatz, Handelsplatz-Zeitzone und originale Quote-Währung ausweisen. Fonds, Indizes, Kryptowährungen, verwechselte Ticker und fehlende Kurse werden abgelehnt.

**Währung:** Es erfolgt **keine** Umrechnung von Handelswährungen; auch `GBp` (Pence) und `ZAc` (Cents) bleiben als **separate Kurseinheiten** erhalten und dürfen nicht zu GBP/ZAR aufgerundet werden. Die eindeutige Asset-ID enthält Börse, Tickernamen und Quote-Währung. Alte dokumentierte Pilot-IDs bleiben bei identischer Börse/Währung stabil. Die MIC-Zuordnung beruht zunächst auf Anbieter-Metadaten und ist ausdrücklich *nicht* mit einer unabhängig verifizierten ISIN/MIC-Identitätsprüfung gleichzusetzen.

**Börsentage:** Für bekannte Börsen (z.B. NYSE/NASDAQ, Xetra, London, Tokio) werden Markt-Kalender und eine 90-Minuten-Sicherheitsfrist nach dem Tagesende geprüft. Für derzeit unbekannte Anbieter-Börsen gilt ein konservatives 48-Stunden-Lag nach dem historischen Sitzungstag. In allen Fällen muss ein gültiger OHLCV-Tagesbalken genau zum gewünschten Datum vorliegen. Bei Börsenkalender-/Metadatenfehlern oder unter 80 abgeschlossenen Handelstagen: transparenter Fehler statt erfundener Zone.

## Stichproben nicht mit Systemgrenzen verwechseln

- **Universelle M1–M3-Einzelwertfunktion:** Kein fünfteiliger Whitelist-Filter. Jede vom Anbieter unterstützte und eindeutig aufgelöste Börsenaktie mit ausreichender Historie kann untersucht werden.
- **M5-Pilot:** AMD, INTC, ARM, SMH, NOW sind vorregistrierte **prospektive Forschungsteilnehmer**. Eine Erweiterung darf diese eingefrorene Stichprobe nicht stillschweigend ändern. Für weitere Aktien braucht es einen **separat versionierten** neuen Prospektiv-Snapshot und eigene Ressourcen-/Kapitalmaßnahmenprüfungen.
- **M6-/Depot-Watch:** Eine spontane Untersuchung ist zunächst ein **read-only Forschungsbericht**. Eine Aktie wird nicht durch das Ansehen automatisch Depotposition oder Orderkandidat.
- **M7:** Prospektive Regeln können später auf zusätzliche, unveränderlich registrierte Aktien angewendet werden. Die erste M7-Kohorte ist für den wissenschaftlichen Vergleich eingefroren; sie ist kein Produkt-Universumslimit.
- **Automatische Skalierung:** Die bestehende Scanner-Liste liefert einen wachsenden Rahmen für die tägliche Zonenverarbeitung. Für die geplante automatische Mehrwert-Integration benötigt ARGUS noch Batch-/Rate-Limit-Logik, verifizierte Metadaten und ein versioniertes erweitertes First-seen-Archiv. **Das ist nicht durch die Einzelwertfunktion bereits erledigt.**

## Evidenz und Risiken

Jedes Ergebnis enthält `research_cli_receipt` mit Zeilen-Hash, Börsen-/Währungs-Herkunft, abgeschlossenem asof, Anzahl Quellkerzen, `first_seen_prospective=false`, `can_trade=false` und `empirical_probability=null`. `degraded` kennzeichnet die Nutzung rückwirkend adjustierter Yahoo-Kurse als historischen Proxy; ein heute nachgerechneter historischer Kursbereich ist **keine** damals eingefrorene Zukunftsprognose.

### Reales Demonstrationsbeispiel

Fiserv `NASDAQ:FISV`, 23.10.2026: 639 Tageskerzen, 75 Rohzonen, 49 M3-Cluster, sieben bedingte Szenarien. Original-Test: [GitHub Actions #40394218029](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/40394218029). Einstieg, Invalidierung und Ziele sind nicht empirisch validiert und keine Handelsfreigabe.
