# BA-QM10: Sicherheitsreserve der W10-/Watch-Runtime – Korrektur in PR #326

Stand 17.10.2026. **Entwurfs-/Branchmaßnahme, kein Produktivrelease.**

## Ausgangsbefund
- [BA-QM10-Lauf 39062439259](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39062439259): 91/93 Tests bestanden, zwei Fehler.
- Ursache: `audit_current_runtime_capacity().health_status == REVIEW_REQUIRED` wegen weniger als 20 Prozent freier Nutzlastkapazität im größten der 64 bestehenden Transportshards.
- **Unverändert:** 2.000.000 Byte harte Publikationsgrenze pro Shard und 20-%-Review-Grenze; keine Downgrade der Gesundheitsskala, keine Entscheidung-/Scoring-Änderung.
- Der bisherige historische `BA-QM10-F01/F05`-Evidence-Contract bleibt unverändert. Dieser enthält historische 16/32/64-Schritte, aber die Live-Kapazitätsprüfung bindet den jeweils tatsächlich konfigurierten `SHARD_IDS`-Satz.

## Reine Transportmaßnahme
- Neue W10-Snapshots: `SHARD_IDS` mit **128** statt 64 deterministischen Hexadezimal-IDs von `00` bis `7f`.
- Bestehende `size_balanced_v1`-Verteilung, gesamte Symbolhistorie bleibt jeweils zusammen und vollständig. Keine Paketverkürzung, keine Komprimierung mit Informationsverlust und keine geänderte Universal-Stance-Bewertung.
- Greedy-Verteilung ohne teuren lokalen Optimierungsschritt, **sofern** bereits unter 1.400.000 Byte Nutzlast auf jedem Shard liegt. Das ist konservativer als die Betriebsreview-Schwelle bei 1.600.000 Byte. Bei höheren Lasten bleibt die bisherige lokale Suchverbesserung aktiv.
- Alte veröffentlichte 64-Shard-Runtimes bleiben anhand ihrer jeweiligen Manifest-`symbol_shards`-Zuordnung lesbar. Regressionstest hinzugefügt.
- Loader nutzt `symbol_shards` aus dem jeweiligen Manifest, nicht einen implizit erzwungenen neuen Dateinamen. Beim Schreiben einer *neuen* Runtime werden alte veraltete `shard_*.json`-Dateien im Zielverzeichnis bereinigt.

## Erforderliche empirische Abnahme
1. `BA-QM10 Production Operations QM` muss auf **aktuellem PR-HEAD** erfolgreich enden, mit `headroom_ratio >= 0.20`, `health_status=PASS`, `configured_shard_count=128`, `max_shard_bytes < 2_000_000`.
2. `tests/test_watch_runtime_transport.py` inklusive historischer Loader-Rückwärtskompatibilität muss erfolgreich sein. BA-QM8 / BA-QM9 und W11 lesen weiterhin identische Pakete.
3. `Return Integrity Recheck`, `Closed-bar production sequence prepublication` und die übrigen verpflichtenden Checks müssen auf demselben HEAD bestanden sein. Ungeklärte Fehler bleiben Merge-Blocker.
4. Kein Merge, solange auf diesem HEAD die aktuelle Betriebsprüfung nicht mit einem echten Kapazitätsbeleg bestanden ist.

**Wissenschaftlicher Hinweis:** Transportvergrößerung betrifft ausschließlich die Veröffentlichung der Entscheidungsbelege. Sie beweist nicht erneut die empirische Qualität oder Profitabilität der zugrunde liegenden Prognosen.
