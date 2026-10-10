# CY-08 — getrennte Reviewer- und Shadow-Abnahmevorlage

**Geltungsbereich:** CYCLE-DIR-2026-10-09-v1, CY-08, L12–L14. Diese Vorlage ist ein Audit-Check, **keine** Reviewerfreigabe und kein Forschungsresultat.

**Regel:** Ohne echten identifizierbaren L5-PAT plus L6-Graf, prospektives L9-`SUPPORTED`, aktuelles L10 A/B und aktuelle Evidenzbindung kann **kein** `ANNOTATION_ONLY` oder `SHADOW_CHALLENGER` genehmigt werden. Die vorgesehenen Review-Ausgänge sind `BLOCKED`, `DEFERRED`, `REJECTED`, `ANNOTATION_ONLY` oder `SHADOW_CHALLENGER`, niemals eine automatische produktive Zulassung.

## A. Reviewer-Identität und unveränderliche Belege (vor Freigabe ausfüllen)

| Feld | Nachweis / Eintrag |
| --- | --- |
| Pattern ID / Version / Spec SHA-256 | **FEHLT / NICHT AUSFÜLLBAR**, solange CY-06 gesperrt |
| L1 Run Manifest SHA / Daten-Cutoff / Holdout | **FEHLT** |
| L5 PAT Freeze Hash / Freeze-Zeit / Originaldaten-Coverage | **FEHLT** |
| L6 Graph Hash / Duplicate-, Nested-, High-/Critical-Cluster | **FEHLT** |
| L7 Capture Hash / späterer unabhängiger Publisher-Zeitbeweis | **FEHLT** |
| L8 Outcome Hash / gereifte 5/20/40/60 Marktsessions | **FEHLT** |
| L9 QM-C Plan-/Familien-/Look-Hash, aktuelles terminales Ergebnis | **FEHLT** |
| L10 Rating History Hash und Rating A/B | **FEHLT** |
| Reviewer-ID, Rolle, UTC-Timestamp, Reason Codes | **OFFEN**, kein hypothetischer Reviewer |
| L12 Ergebnis/Registry-Event/Integration Mode/Authority | **BLOCKED** bis Belege vorliegen |

## B. Reproduzierbarer Review-Entscheid

1. **Herkunft und PIT**: externe Quelle, Quellwährung (Originaleinheit), Original-Listing, Handelskalender, Bar-/Capture-Zeit belegt? Bei nein **BLOCKED**.
2. **Prospektiv**: L5/QM-C-Zeit vor erstem zugelassenen L7-Capture; gereifte L8-Outcomes, vorbestimmte L9-Looks, Multiplicity, QM-C5-Negativerhalt? Bei nein **BLOCKED**.
3. **Statistik**: L9 aktuell terminal `SUPPORTED`, L10 exakt A/B, L6 High/CRITICAL Abhängigkeiten korrekt behandelt? Bei nein **REJECTED** oder **DEFERRED** nach expliziter Prüfung; niemals Ersatz durch Discovery-Trefferquote.
4. **Admission**: Reviewer entscheidet explizit `ANNOTATION_ONLY` (Authority `NONE`) oder `SHADOW_CHALLENGER` (Authority `SHADOW_ONLY`). Nur L12 erhält die exakte Pattern-ID/Version/Hash-Registrierung. Änderungen oder Rollback machen alte Admission stale.
5. **Shadow-Paarung**: gleicher Zeitpunkt/Asset/Horizont/Target/Baseline; gleiches gereiftes L8 Outcome für `EXISTING_TIMING` und **konservativ fusionierten** L13-Shadow. Dependency-Cluster nicht mehrfach zählen; Widerspruch bedeutet Konflikt/Abstention.
6. **Netto-Kosten**: `configs/cycle_direction/cy08_net_cost_v1.json`; Modell-Roundtrip 10/20/50 bps (20 primär, 50 Stress), nicht reale Gebührendaten. Nur isolierter Research-PnL-Proxy, keine nutzerseitige Order; `NET_INCREMENTAL_VALUE_CANDIDATE` benötigt vorfixierte paarweise Mindestunterstützung und positive untere 95%-Bootstrap-Grenze in Primär- und Stress-Szenario. Gültige L12 Admission bei jeder echten Nutzung **zusätzlich aktuell erneut prüfen**.
7. **L14 Operations**: explizite L1-Prereg für jede Discovery; überprüfbare Negative/Decay, Source-Coverage, Quelle-/Frische- und QM-Fehler; automatisches Refit oder Promotion verboten.
8. **Schlussentscheidung**: Reviewer dokumentiert `reason_codes`, zulässigen Status, Evidenzhashes und eventuelle `DEMOTE/ROLLBACK`-Ereignisse. Weitergehende produktive Eingriffe sind **nicht CY-08**, sondern ein eigenes zukünftiges, gesondert abgenommenes Vorhaben.

**Abnahmekriterium:** technische Tests und Governance sind vom empirischen Wirksamkeitsnachweis getrennt. Ein positiver hypothetischer Netto-Proxy oder grüner CI-Lauf ist weder eine bestätigte Handelsstrategie noch eine produktive `Decision`- oder `Portfolio Action`-Freigabe.

## Aktueller Status — 10.10.2026

**BLOCKED / NICHT_FREIGEGEBEN**: #269 ist offen, keine gültige CY-03-Research-Lag-Kette, kein CY-06-L5 PAT und kein CY-07-Prospektivbeleg. **Keine Review-ID/Reviewerzustimmung erfinden.** Der CY-08-Brancharbeitspaket enthält nur sichere technische Vorbereitungen und den vorfixierten Kostenvergleich; die tatsächlichen L12–L14-Ergebnisse sind nicht ausgeführt.
