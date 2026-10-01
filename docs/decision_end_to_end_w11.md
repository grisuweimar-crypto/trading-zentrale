# W11 – echter End-to-End-Test

W11 fügt **keine neue Fachlogik** hinzu. Der Schritt prüft, ob ein realer, bereits durch W10 versiegelter Snapshot die bestehende Wertpapierdepot-Watch-Kette vollständig durchläuft:

`Scanner → 1 → 2 → 3 → 4/5 → 6 → 7A → 7D → 7E → 7F → 7G → 7H`

## Acceptance-Kriterien

Ein W11-Lauf gilt nur als bestanden, wenn alle folgenden Bedingungen gleichzeitig erfüllt sind:

- gleiche Snapshot-Identität bis 7H,
- PIT der aktuellen 7A-Evidence ist akzeptabel,
- keine Future-Evidence,
- fehlende Evidence wird nicht als neutral behandelt,
- kein Scanner-Scalar-Fallback,
- keine privaten Depotdaten werden im Repository persistiert,
- keine doppelten aktuellen Evidence-Claims oder Current-Packets,
- kein Super-Score / keine neue aggregierte Ersatzbewertung,
- keine unerlaubte Phase-8-Wirkung,
- der finale 7H-Watch ist `complete`, nicht nur `partial` oder `unavailable`.

## Realer Lauf

Voraussetzungen:

1. `artifacts/research/decision_snapshot_w10.json` existiert und ist `sealed`.
2. Der versiegelte W10-Snapshot stimmt mit `daily_research.json` überein.
3. Das reale `decision_depot_position_book_v1` liegt **außerhalb** des Repository-Verzeichnisses.
4. Auch das private W11-Ausgabeverzeichnis liegt **außerhalb** des Repository-Verzeichnisses.

Beispiel:

```bash
python scripts/run_depot_watch_e2e_w11.py \
  --positions /private/path/positions.json \
  --private-output-dir /private/path/w11-output
```

Bei Erfolg entstehen ausschließlich im privaten Ausgabeverzeichnis:

- `depot_watch_7h.json`
- `w11_diagnostics.json`
- `w11_acceptance_receipt.json`

Die Acceptance-Quittung enthält keine Positionszeilen. Sie bestätigt nur Snapshot-ID, Kettenstatus und die W11-Prüfmerkmale.

## Fail-Closed

W11 darf nicht durch künstliche Testartefakte als real bestanden markiert werden. Fehlt der produktiv erzeugte W10-Snapshot oder das reale private Positionsbuch, kann nur die W11-Implementierung regressionsgetestet werden; der reale W11-Durchstich bleibt offen.

W11 verändert weder Universal Stance noch Hysterese noch Portfolio Action. Phase-8-Evidence bleibt im aktuellen kanonischen Watch-Pfad ohne Wirkung, solange sie nicht über eine separat genehmigte produktive Integration an die Decision Layer gebunden wurde.
