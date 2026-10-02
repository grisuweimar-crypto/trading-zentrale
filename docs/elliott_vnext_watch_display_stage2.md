# Elliott vNext – Stage 2: read-only Anzeige in der Depot-Watch

## Ziel

Stage 2 macht die prospektiv erzeugten internen Elliott-vNext-6H-Ausgaben in der
bestehenden Decision-Watch-Symbolansicht sichtbar. Die Anzeige ist ein
Präsentations-Sidecar und keine neue Decision-Layer-Evidence.

Die kanonische Entscheidungsfolge 7A -> 7D -> 7E -> 7F -> 7G -> 7H bleibt
unverändert.

## Datenquelle

Die Anzeige liest ausschließlich den aktuellen prospektiven Capture aus dem
isolierten Branch `elliott-vnext-shadow-data`:

`artifacts/research/elliott_vnext_prospective_current_6h.json`

Der Capture wird zur Laufzeit in den Symbol-View-Workflow kopiert. Die rohe
Shadow-Datei wird nicht nach `main` übernommen.

## Snapshot-Bindung

Ein Elliott-Capture wird nur als aktuelle Anzeige verwendet, wenn

- `capture.snapshot_id == watch_runtime.snapshot_id`
- `capture.as_of == daily_research.as_of`

gilt.

Ein älterer oder anderweitig abweichender Capture wird nicht an den aktuellen
Watch-Zustand angeheftet. In diesem Fall enthält die Symbolansicht den Status
`prospective_capture_not_current_snapshot` und keine 6H-Ausgaben.

Fehlt der Capture vollständig, lautet der Status
`prospective_capture_not_supplied`.

Fehlt für ein konkretes Symbol trotz aktuellem Capture ein 6H-Output, lautet
der Status `no_elliott_output_for_symbol`.

Damit bleibt fehlende Evidenz fehlend und wird nicht als neutral interpretiert.

## Angezeigte Informationen

Jede Symbol-Zusammenfassung erhält den zusätzlichen Block

`elliott_vnext_research`

mit dem Schema `elliott_vnext_watch_display_v1`.

Bei aktuellem Capture werden alle für das Symbol vorhandenen
`(timeframe, degree)`-Ausgaben transportiert. Es gibt keinen Reducer und keine
Auswahl eines vermeintlich "besten" Wellengrads.

Je 6H-Ausgabe werden unter anderem sichtbar:

- Timeframe und Degree
- aktueller Wellenstatus
- Primary Scenario und kompakte Alternative-Szenarien
- Fibonacci-Status und -Geometrie
- Projektionszonen
- harte Invalidierungen
- Routing-Trigger
- Research-only Swing-Review-Kontexte
- vorhandene historische Expectancy
- 6H-Validierungsstatus
- Warnungen

Die Anzeige übernimmt nur bereits vorhandene 6H-Felder. Sie erzeugt keine neue
Richtung, keinen Score, keine Count-Auswahl und keine Handelsaktion.

## Entscheidungsgrenze

Jeder Anzeige-Block trägt explizit:

- `research_only = true`
- `read_only_presentation = true`
- `w10_source_emitted = false`
- `changes_universal_stance = false`
- `changes_portfolio_action = false`
- `direct_ordering_allowed = false`
- `review_contexts_are_actions = false`
- `decision_effect = "none"`

Stage 2 erzeugt insbesondere **kein**
`decision_elliott_6h_source_v1`-Objekt.

W10 Phase 6 bleibt deshalb weiterhin `not_supplied`, solange nicht in einer
separaten späteren Stage 3 ein ausdrücklich geprüfter Integrationsentscheid
getroffen wird.

## Automatisierung

`Decision Watch Symbol Views` wird künftig sowohl nach

- `Decision Watch Runtime Publish`
- als auch nach `Module 6 Elliott Prospective Shadow`

ausgelöst.

Damit kann ein zunächst ohne Elliott-Capture erzeugter Watch-Symbolstand erneut
gebaut werden, sobald der zum selben Scanner-Snapshot gehörende prospektive
Elliott-Capture tatsächlich verfügbar ist.

## Bedeutung für die tägliche Watch

Die interne Anzeige ist künftig getrennt von einer eventuellen externen
technischen/Elliott-Prüfung zu behandeln:

- **Elliott vNext intern · Research-only** = eigener empirischer Scanner-vNext-
  Forschungsstand aus dem prospektiven Capture.
- **Externe technische/Elliott-Bestätigung** = unabhängige externe Evidenz,
  sofern sie separat erhoben wird.

Beide Quellen dürfen nicht miteinander gleichgesetzt werden.
