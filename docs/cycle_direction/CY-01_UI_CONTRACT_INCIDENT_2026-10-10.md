# CY-01 Nachtrag – Autopilot-Abbruch am UI-Vertrag (10.10.2026)

**Herkunft:** [Autopilot #38030550084](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38030550084), manueller `workflow_dispatch`, `main` `e77eb4082d04e9269a0fb5120259ecc9bb76109d`, Start 2026-10-10 06:19:50 UTC.

## Gesicherter Befund

- `autorun` bestand. `build` bestand alle bisherigen Schritte bis einschließlich Research, Preis-Backfill, Decision-Evidence, History Delta, Segment Monitor, Reality Check, Macro Chain, Briefing und Briefing/Realities.
- Abbruch bei `python -m scanner.ui.generator` in `build_ui` → `validate_csv`.
- Fehlermeldung: `cycle: 80 non-numeric/NA values` (Beispiele `V`, `ILMN`, `DIS`, `CME`, `BAS.DE`).
- `configs/watchlist_contract.json` verlangte bisher eine numerische `cycle`-Spalte **ohne** `allow_null`. CY-01 schützt fehlende Ursprungswerte dagegen absichtlich als nullable. Dieser Vertragswiderspruch blieb in den CY-01-Regressionen bislang ungetestet.
- Da der Job **vor** `git add artifacts/; git commit; git push` abbrach, wurde dieser Scanner-Output nicht als erfolgreicher Lauf veröffentlicht. Historische Archive auf `main` bleiben unverändert.

## Kleinstmögliche Korrektur

1. Nur `cycle` im bestehenden **Pflichtspalten-Vertrag** auf `allow_null: true` setzen: Spalte muss weiterhin existieren; valide echte Zahlen 0–100 bleiben gültig.
2. Der Kontrakt-Validator darf bei `allow_null` ausschließlich leere/fehlende Werte tolerieren, nicht willkürlich nichtnumerische Tokens. Ausreißer außerhalb 0–100 werden weiterhin abgelehnt.
3. Wenn `cycle_quality` verfügbar ist, `VALID` ausschließlich mit numerischer `cycle` akzeptieren; `MISSING_SOURCE`, `STALE`, `INVALID_VALUE` oder `INSUFFICIENT_HISTORY` dürfen keinen numerischen, als gültig zu deutenden Zyklus mitführen. Alte Snapshots ohne Qualitätsfeld behalten die bestehende Spaltenvertragsprüfung.
4. Tests reproduzieren exakt den Fehlerpfad mit `watchlist_ALL.csv` und erfassen Null, hundert, fehlend, fehlende Pflichtspalte, ungültigen Text, negative Werte und Statuswiderspruch.

**Keine Änderung:** CY-02-Formel oder Draft PR #266, Scoring, Decision, History-Daten, Pattern Discovery, UI-Semantik `—` bei fehlendem Wert.

## Abnahme (ausstehend, bis echte GitHub-Ergebnisse vorliegen)

- [ ] GitHub CY-01-CI grün, inkl. neuer Vertragsregression.
- [ ] Autopilot-Retry auf korrigiertem `main`: UI, `scripts/validate_contract.py`, Publication-Integrity-Gates und Commit/Push erfolgreich.
- [ ] Publizierter Run/Metadata/Snapshot frisch und vollständig, keine Imputation.
- [ ] CY-02 erst gesondert gemäß PR #266 fachlich freigeben.

**Status:** Korrektur-PR vorbereitet. Kein Erfolg oder abgeschlossener produktiver Retry vor tatsächlich vorhandenem Testbeleg behauptet.
