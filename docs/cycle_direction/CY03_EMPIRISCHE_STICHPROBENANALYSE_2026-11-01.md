# CYCLE-DIR: Erste empirische Datenqualitäts- und Stichprobenanalyse (2026-11-01)

**Status:** tatsächlich aus dem produktiven `artifacts/cycle_history/coverage.csv` berechnete *deskriptive* Auswertung. **Keine** Auswertung von Forward-Returns und **keine** wissenschaftliche oder Handelsfreigabe. Quelle: CY-03 v1, Manifest `as_of=2026-10-31`.

## Methodik
Die 220 instrumentbezogenen Monatszeilen der `coverage.csv` wurden vollständig aggregiert. `observations` umfasst nur die kanonische Tagesauswahl, nicht sämtliche 6.580 Archivzeilen aus 30 Snapshots. Die Kettenzahlen stammen aus den reproduzierbaren CY-03-Masken und sind vorläufig. Kein Nachladen von Kursen, keine Outcome-Inspektion, keine Veränderung des Archivs.

## Ergebnisse
- 220 Instrumente, 4.830 kanonische Beobachtungen, 4.760 intern gültige aktuelle Beobachtungen.
- Vorläufige Lags: 1obs=4.540, 5obs=3.660, 10obs=2.570; wissenschaftlich freigegeben: **0**.
- 210/220 Instrumente haben genau 22 gültige Beobachtungen und 12 vorläufige 10obs-Ketten.
- Fünf Instrumente haben **0** vorläufige 10obs-Ketten: `8035.T`, `CRYPTO:DOGE`, `CRYPTO:SOL`, `CRYPTO:XRP`, `NPN.JO`. Sie haben jeweils nur 8 gültige aktuelle Beobachtungen. Ihre Ausschlussangaben lauten `INVALID_CURRENT:42;NO_PRIOR:14`.
- 4.830 kanonische Beobachtungen und 6.580 Archivbeobachtungen sind verschiedene Nenner; Quoten dürfen nicht vermischt werden.
- 10obs-Ketten überlappen zeitlich und sind nicht unabhängig. Ein Pooling über Instrumente ohne Cluster-/Zeitkorrektur ist ungeeignet.

## Forschungsfolgerungen (noch keine Prognoseergebnisse)
1. **Vorab definierte Qualitätskohorte:** 210 Instrumente mit 22/22 gültigen Tagen und 12 vorläufigen 10obs-Ketten; diese Auswahl erfolgt ausschließlich anhand von Datenqualität, nicht anhand späterer Renditen. Keine automatische Research-Freigabe.
2. **Separate Ausschlusskohorte:** 5 benannte Instrumente ohne 10obs-Ketten. Fehlende Beobachtungen nicht imputieren; Datenfehler/Provider-Mapping und Stale/Insufficient-History anhand der Einzel-Quality-Reasons untersuchen.
3. **Kein Scheineffekt durch Mehrfachzählung:** eine kanonische Tagesbeobachtung pro Instrument; Abhängigkeit durch überlappende Fenster, Instrument-Cluster und Kalenderzeit berücksichtigen.
4. **Vordefinierter erster echter Prognosetest nach unabhängigem PIT-Release:** CY-05 A/B/C/D, primär `C-B`, Cycle-Band <25 und steigender 5obs-Cycle, Ziel `peer_excess_20t_gt_0`, Kosten und FDR wie `configs/cycle_direction/cy05_preregistered_design_v1.json`. Ohne unabhängige Quellen-PIT, festes Cutoff/Holdout und gereifte Forward-Outcomes dürfen keine Trefferquoten als valide Forschungsresultate ausgewiesen werden.

## Nächster überprüfbarer Arbeitsschritt
Unabhängige PIT-/Listing-/Währungsbelege pro Instrument und Snapshot (#269), CY-03-v2-Release (#290), dann L1-Freeze **vor** Outcome-Inspektion. Danach erst den vorregistrierten Vergleich rechnen. Die vorliegende Auswertung ist bereits ein nutzbares Ergebnis zur Stichprobenauswahl und Fehlerpriorisierung, aber keine Aussage über Prognosegüte.

## Reproduktion
Aus `coverage.csv` pro Zeile `observations`, `valid_current`, `lag_1obs`, `lag_5obs`, `lag_10obs` als Integer summieren; Instrumente nach `lag_10obs==0` bzw. `observations==22 and valid_current==22 and lag_10obs==12` filtern. Mit Manifest `provisional_lags` und `research_eligible` gegenprüfen.
