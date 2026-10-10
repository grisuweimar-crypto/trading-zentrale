# CYCLE-DIR CY-05 — Startprotokoll, L2/L3-Versionierung und Forschungsdesign

**Plan:** CYCLE-DIR-2026-10-09-v1 · **Stand:** 10.10.2026, Europe/Berlin
**Branch:** `feat/cycle-dir-cy05-level-direction-20261010`
**Basis-main (Start):** `2ae9d503a1fc9c36ec8ba0dba59fd95ea262c2d1`
**Status:** **IN_ARBEIT** (technische Implementierung zur PR-Prüfung; **nicht** empirisch/fachlich freigegeben).

## 1. Abhängigkeiten und reale Datenlage

Laut Masterplan benötigt CY-05 **CY-03**, aber **nicht CY-04**. CY-04 bleibt ein eigenständiger UI-Zweig (vorbereiteter Draft-PR #279); keine UI-Arbeiten in diesem Paket.

CY-03 veröffentlichte am 10.10.2026 genau **einen** prospektiven Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`: **215** Beobachtungen, **209** intern replaybar, **6** excluded, aber **0/0/0** echte Vergleichsketten über 1/5/10 Beobachtungen und **0** research-eligible Assets. Das externe Quell-/Listing-/Kalender-/Bar-PIT-Gate [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) bleibt offen. Technischer Code/Unit-Tests dürfen weiterentwickelt werden; reale L3-Outcome-Suchen, Evidenz, Backtest-Freigaben, Signale und Trading-Empfehlungen daraus **nicht**.

Bestandsaufnahme L2/L3 auf `main`:
- `configs/pattern_discovery/feature_library_v1.json`: `scanner.cycle` hat `change_direction` (1/5/10), `threshold_crossing` (25/50/75) und `delta_observations` bereits registriert. `delta_observations` ist **nicht** frei atomisierbar.
- `configs/pattern_discovery/l3_search_contract_v1.json` bildet Richtung `UP/DOWN` und `CROSS_UP/CROSS_DOWN` ab, **keinen laufenden Niveauzustand**. Schwellenübergang ist nicht dasselbe wie aktuelles Niveau.
- `run_contract.py` bindet L1-Manifeste an Fingerprints, Zeitpunkt, L0/L1-Hashes und L2-Bibliotheksbytes. L3-v1 unterstützt die Target-Familien Directional und Relative Alpha mit Horizonten **5/20/40/60 Handelssessions**.

## 2. Versionierte technische CY-05-Änderungen (opt-in)

| Datei | Änderung / Schutz |
|---|---|
| `configs/pattern_discovery/feature_library_cycle_v2.json` | Separate L2-Version `PDL-FEATURE-LIBRARY-CYCLE-v2`; neuer registrierter Transform `level_band` für `scanner.cycle`, mit **unveränderlichen Grenzen 25/50/75**. |
| `configs/pattern_discovery/l3_search_contract_cycle_v2.json` | Separater, explizit zu übergebender L3-Vertrag `CYCLE-DIR-CY05-L3-v2`; die vier Niveauzustände werden atomisierbar. Gleiche strukturelle Schema-Familie `v1`, aber **eigene Vertragsbytes / Hash**. |
| `src/scanner/research/pattern_discovery/feature_library.py` | Nur bei explizit geladener CY-05-L2-Version gelten zusätzliche Research-Gates für Cycle. Ohne **freigegebenen** CY-03-Ledger-Status, **VALID**-Qualität und Metadaten keine Verfügbarkeit; Lags benötigen gesonderte Eligibility und kohärente Listing-/Währungs-/Formel-Identität. |
| `src/scanner/research/pattern_discovery/search_engine.py` | Atomisierung `level_band` entlang 0–<25 / 25–<50 / 50–<75 / 75–100; echte numerische 0 und 100 bleiben gültig. Kein Veränderung am aktiven L3-v1-Contract. |
| `tests/pattern_discovery/test_cy05_cycle_level_direction.py` | Synthetische Unit-Regressionen: Grenzen, Ungültigkeit, Quellen-/Research-Gate, 1/5/10-Eligibility, Fremdwährung und Kombination Niveau + Richtung. |

Die alte L2-v1-Datei und der alte L3-v1-Vertrag bleiben **unangetastet**. Kein Default-Wechsel: CY-05 wird nur mit expliziter Bibliotheks- und L3-Vertragswahl aktiviert. Der Code selbst **zertifiziert keine** externe Quell-Herkunft; dafür ist ein eigener, künftig nachweisbar hash-/PIT-verifizierender, read-only CY-03→L3-Adapter nötig. Die Metadatenfelder `cycle_research_status=ELIGIBLE`, `cycle_history_source=CY03_VERIFIED_LEDGER` und `cycle_lag_{1,5,10}obs=RESEARCH_ELIGIBLE` sind eine **künftige Schnittstelle**, keine heute behaupteten produktiven Ledger-Werte; aktuell liefert der echte Ledger `BLOCKED_EXTERNAL_VERIFICATION_269`. Niemals diese Felder aus bloßer Anwesenheit von Zahlen synthetisieren.

## 3. Vorab festgelegtes Vergleichsdesign — keine Outcome-Auswertung

**Fragestellung:** Bringt `niedriger Cycle + steigende Richtung` gegenüber derselben Auswahl ohne die Richtung reproduzierbare, **inkrementelle** Richtungs-/Alpha-Information? Bei welchem Horizont und nach plausiblen Handelskosten?

Vier Arme, **gleiches Universe, selbe PIT-Beobachtungen, gleiche Auswertungs- und Mehrfachtestpolitik**:

- **A – Kontrollmodell:** freigegebene Nicht-Cycle-Features/Regime mit gleichem festgelegten Suchbudget, **keine** Cycle-Kondition.
- **B – Niveau:** A + **eine** CY-05-`level_band`-Bedingung (alternativ gesondert registrierte Threshold-Crossings; niemals als `level_band` umdeuten).
- **C – Niveau + Richtung:** B + `scanner.cycle/change_direction` mit ausschließlich `lag_observations ∈ {1,5,10}`; primärer Kontrast **C gegen B** für ein vorab definiertes niedriges Niveau (`LEVEL_LT_25`) und `UP`.
- **D – Kombinationsarm:** C plus höchstens **eine** weitere bereits zugelassene Bedingung, z. B. RS3M oder Trend200. Das L3-Maximum bleibt **drei Atome**. D ist explorativ und unterliegt zusätzlicher Multiplikitätskontrolle.

**Primäre Messfamilien:** `DIRECTIONAL` (Return >0 bzw. <0) und `RELATIVE_ALPHA` (`peer_excess_{5,20,40,60}t_{gt,lt}_0`), jeweils über 5/20/40/60 tatsächliche Handelssessions des L3-Vertrags. **SPY-Alpha ist nicht automatisch ein bestehendes L3-Target**. Keine Verwechslung der Cycle-Scannerlags 1/5/10 mit den Forward-Outcome-Sessions 5/20/40/60.

**Multiplikität / robuste Auswertung:** BH-FDR `q=0.05` als vorregistrierte Familie für Suchkandidaten; nachgelagerte L4-Abhängigkeit, überlappende Forward-Fenster, Symbol- und Zeitkonzentration, Cluster-/Block-Sensitivität und negative Kontrollen. Zielgrößen und primäre C-vs-B-Hypothese **vor** Ergebnisansicht einfrieren. L3-Minimum-N und Effektschwelle nur aus künftig versiegeltem L1-Manifest, nicht nachträglich anpassen.

**Kostenannahme für künftige Nettosensitivität (Planwert, keine beobachtete Brokergebühr):** 20 Basispunkte Roundtrip, Sensitivität 10 und 50 Basispunkte; Rückgabewerte, Spreads, FX-Einheit, Markt-/Listingidentität und tatsächlich verfügbare Sessions getrennt prüfen. Falls eine Kostenkorrektur den bestehenden Target-Vertrag überschreitet, erst einen neuen Vertrag erstellen — keine stillen Änderungen von L3-Outcome-Labels.

**PIT / Ausschlüsse:** Altes, eventuell imputiertes `cycle` aus Legacy-History zählt nicht als automatisch valide Reihe. Numerische 0/50/100 können echte Cyclewerte sein, fehlen aber bei schlechter Qualität. Ausgeschlossen: fehlende oder veraltete Werte, fehlender Vorläufer, geänderte Formel/Listing/Originalwährung, Gaps, ungeprüfter Snapshot, nicht gematchte Assets, falscher Preiszeitpunkt, nicht gereifte Outcomes, Daten jenseits des Cutoff. Pro As-of-Tag nur eine kanonische Beobachtung pro Asset. Niemals Daten/Outcomes vom Bestätigungszeitraum zum Optimieren der Hypothesen nutzen.

**Noch offene formale L1-Freeze-Felder vor produktiver Discovery:** bindbarer Datensatz mit Research-Eignung und aktiver Quelle, exakte Source-/Universe- und Repo-SHAs, `declared_start_at`, `data_cutoff`, explizite Ziel-ID-Liste, konkrete fixe L1-Minimum-N-/Effekt-/Budgetwerte und Ein- vs. Ausschluss nach Stichproben-Coverage; anschließend `build_run_manifest(...)` mit kryptografisch gebundenen Eingabedateien und deterministischer Run-ID **vor** dem ersten Outcome-Lauf. Die vorliegende Konzeption ist **kein** `FROZEN_PRE_RUN`-Manifest und darf auch nicht als solches etikettiert werden.

## 4. Abnahmematrix / verbleibende Arbeiten

| Gate | Aktueller Stand |
|---|---|
| L2/L3 opt-in Niveau 25/50/75 versioniert | Im Branch implementiert, PR-CI noch zu prüfen |
| Existing v1 unverändert | Änderungen nur an Python-Implementierungen und **neuen** Configs; v1-Configbytes unverändert; Regression ausstehend |
| Numerische 0, Grenzen 25/50/75, echte UP/DOWN, Missingness | synthetische Unit-Tests angelegt, unabhängige Ausführung ausstehend |
| 1/5/10 ohne Vorläufer/fremde Währung/Block #269 | Fail-closed Code und Unit-Tests angelegt; externe Verifikation weiterhin offen |
| Verschlüsselungs-/Hashbindung L1 und Replay-Determinismus v2 | Weitere Integrationstests nötig |
| Vollständiges L1-Präregistrierungsmanifest mit realen Input-Fingerprints | **NICHT ERSTELLT**; keine freigegebene Datenbasis |
| Empirisches Niveau+Richtung-Signal / Vorteil gegen Baseline | **NICHT GETESTET**, keine Outcome-Suche ausgeführt |
| Scoring / Decision / Handelsentscheidung | **NICHT GEÄNDERT** |

**Exakter Wiedereinstieg:** eigene CY-05-L1-Freeze- und Run-Binding-Tests ergänzen; im PR CI/Regression prüfen; danach lesenden CY-03→L3-Rechercheadapter nur bei bestätigter #269- und CY-03-Freigabe verdrahten, Input-Fingerprints und L1-Manifeste *vor* der ersten echten Auswertung versiegeln. CY-06 beginnt erst nach CY-05-Abnahme, nicht bereits nach diesem technischen Start.

## 5. CYCLE-DIR-Übergabe

- **Technisch:** In Arbeit; neue opt-in Implementierung und Unit-Tests auf Branch, noch keine Merge-/CI-Freigabe.
- **Fachlich:** Research-Design spezifiziert, Abschlussgate (realer L1-Freeze und verifizierter CY-03-Adapter) offen.
- **Empirisch:** Nicht validiert; aktuelle Datenbasis 0 research-eligible, 0/0/0 Lags.
- **Unverändert:** alte L1/L2/L3-Library- und Ergebnisversionen, CY-04-UI, Scannerformel und -Ausgabe, Risk/Confidence/Elliott/Decision/Portfolio/Execution.
- **Offene Gate-ID:** #269; CY-03 Mehrtageshistorie; zukünftiger CY-05-L1-Freeze und vollwertige Regressionsabnahme.
