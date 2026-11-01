# ARGUS – tatsächliche Elliott/Fibonacci-Empirie: 10-Sitzungen-Kontrast (01.11.2026)

**Status:** Neue sekundäre Auswertung eines bereits existierenden historischen Forschungsartefakts; **nicht** unabhängige prospektive Validierung. Keine Handelsfreigabe. Basis: `artifacts/research/elliott_vnext_stage4_historical_validation.json` (Partition `legacy_development_descriptive_only`, `horizon_sessions=10`). Ergänzend: `docs/argus_zones_m4_research.md` und `docs/argus_zones_m41_research.md`.

## Tatsächliche Vergleichsergebnisse (jeweils Grad primary, 10 Sessions)
| Fibonacci-/Elliott-Projektion | Zonen-Trefferquote | Durchschnittliche signierte Rendite | dokumentiertes 95%-Intervall Rendite |
|---|---:|---:|---:|
| Wave 3: W1-Extension ab W2 | 6,7% | +0,52% | −1,03% bis +1,87% |
| Wave 4: W3-Retracement | 69,6% | −0,50% | −1,29% bis +0,44% |
| Wave 5: W1-Länge ab W4 | 28,7% | +1,52% | ca. −0,00% bis +3,27% |
| Wave 5: W1–W3-Struktur ab W4 | 30,9% | +1,55% | +0,15% bis +3,33% |

**Interpretation:** Die Trefferquote der Wave-4-Retracement-Zonen ist hoch, aber die mittlere signierte Rendite negativ. **Ein Zonentreffer ist kein profitables Handelssignal.** Bei den zwei W5-Projektionsarten sind die mittleren signierten Renditen im primary-Grad höher, aber dies ist eine nachträglich betrachtete Teilgruppe im historischen Entwicklungssatz; insbesondere der scheinbar positive untere Intervallrand bei W1–W3/W4 ist **kein** konfirmatorischer Signifikanzbeweis (Mehrfachtests, Abhängigkeiten, Auswahl, fehlende unabhängige PIT-Validierung).

## Abgleich mit unabhängigem Zonen-Replay M4/M4.1
- M4: 574 historische Szenarien, 282 unerreicht, 177 ohne unmittelbare Bestätigung, 103 Risikogate-Ablehnungen, nur 2 Stop-Exits und 0 Target-Exits. Die extrem strenge Folgetagsbestätigung ist in diesem Test kein nützlicher allgemeiner Auslöser.
- M4.1: 6.781 Prognose-/Outcome-Paare, 1.735 reale/Kontroll-Paarungen, 973 beiderseits berührt; 596 reale und 599 Kontrollzonen mit erwartetem erstem Close-Ausgang. **Kein klarer allgemeiner Zonenvorteil**.
- Das sind **verschiedene Populationen und Definitionen**; die Werte dürfen nicht zu einer einzigen Erfolgsquote zusammengezählt werden.

## Konkrete neue Hypothesen, nicht bestätigte Muster
1. **H1:** Wave-4-Retracement-Zonen haben eine hohe geometrische Berührungswahrscheinlichkeit, aber keinen automatisch positiven gerichteten Renditevorteil. Prüfen: Berührung gegen geometrisch gleich breite Kontrollzonen, plus Netto-Rendite.
2. **H2:** Primary-Wave-5-Projektionen (W1–W3 ab W4) könnten gegenüber W1-Extension-W3 eine andere bedingte Renditeverteilung haben. Prüfen: gleicher Asset-/Zeit-/Horizon-Sample, paarweise Differenz, Zeitblock- und Symbolcluster-Bootstrap, Holdout.
3. **H3:** Erst **Konfluenz** aus bestätigter Elliott-Struktur, Fibonacci-Zone und Cycle-Richtung könnte einen Mehrwert liefern. Aktuell **nicht getestet**, weil kein belegter, as-of-korrekter Join von CY-03 v1 mit Elliott-Zonen/Forward-Outcomes vorliegt. Primärvergleich: Zone-only gegen Zone+Cycle, jeweils gleiche Fälle und Kosten.

## Reproduzierbarkeit
Die vier Zahlenreihen wurden direkt aus `projection_summary` gefiltert mit `partition == 'legacy_development_descriptive_only'`, `horizon_sessions == 10`, `degree == 'primary'`. `zone_hit.mean`, `signed_return.mean` und `signed_return.mean_95` mit Faktor 100 in Prozent umgerechnet. Kein neuer Marktdatenabruf, kein erneuter Backtest und keine unabhängige Hashprüfung des Gesamtartefakts.

**Nächster Ausführungsschritt:** die bereits archivierten as-of-Forecasts mit ihren nachfolgenden Kursen auf identischen Symbol-/Zeit-/Währungs-/Kursbasis-Schlüsseln verbinden; auf Basis vorab definierter Regeln 10-/20-/60-Session Zone-only vs Zone+Elliott vs Zone+Cycle testen. Bei fehlender PIT-Identität gesondert `NOT_COMPARABLE` statt einen scheinbaren Effekt auszugeben.
