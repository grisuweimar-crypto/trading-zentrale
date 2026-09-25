# Modul 6G – Historische / prospektive Validierung

Status: Research-only Validierungs-Layer.

6G prüft die eingefrorenen Module 6A–6F. Es entdeckt keinen neuen Elliott-Count, optimiert keine bestehenden Scannerphasen neu und darf keine Handelsentscheidung erzeugen.

## 1. Evidenzgrenze

Die Module-6-Regeln gelten mit Stand **25.09.2026** als für die Validierungsarchitektur eingefroren.

Daraus folgt:

- Claims mit `available_from <= 2026-09-25` sind `legacy_development_descriptive_only`.
- Claims mit `available_from > 2026-09-25` sind `prospective_unspent`.
- Legacy-Daten dürfen für Debugging, deskriptive Backtests, Größenordnungen und Fehleranalyse verwendet werden.
- Legacy-Daten dürfen **nicht** als unverbrauchte Bestätigung für eine spätere Promotion ausgegeben werden.
- Eine technische Fertigstellung von 6G ist ausdrücklich **nicht** gleichbedeutend mit empirisch reifer Promotion.

Diese Trennung verhindert, dass Daten, auf denen die Architektur entwickelt, geprüft oder angesehen wurde, anschließend als unabhängiger Beweis derselben Regeln ausgegeben werden.

## 2. Prefix-Walk-forward-Replay

`validation_replay.py` kann die komplette eingefrorene Kette historisch erneut ausführen:

`6A Pivots → 6B Szenarien → 6C Fibonacci → 6D Review-Routing`

Für jeden Replay-Tag wird nur der Kurspräfix `date <= as_of` übergeben.

Dadurch gelten unverändert:

- Pivot erst ab `confirmed_time`,
- Weekly erst nach kausaler Wochenbestätigung,
- Projektion erst ab `available_from`,
- Fibonacci wählt niemals den Count,
- spätere Kurszeilen dürfen einen früheren Zustand nicht verändern.

Der Replay speichert standardmäßig nur Zustandsänderungen. Das ist eine Speicheroptimierung; jede Session wird trotzdem geprüft. Eine Änderung des Szenarios, der Projektionszonen, des Zonenstatus oder des Routings erzeugt einen neuen Snapshot.

Fehlerhafte oder unvollständige Kursserien werden nicht repariert. Sie erscheinen als fehlende Evidenz.

## 3. Strukturvalidierung

Struktur und Performance bleiben getrennt.

6G extrahiert Motive-Szenarien und bildet eine Lineage aus:

- Pattern-Klasse,
- Richtung,
- Timeframe,
- Wellengrad,
- kausalen Origin/W1/W2-Pivots.

Geprüft wird anschließend, ob eine Struktur später:

- in eine höhere bestätigte Wellenphase fortschreitet,
- hart invalidiert wird,
- oder bis zum Ende der verfügbaren Historie unresolved bleibt.

`unresolved` zählt **nicht** automatisch als Fehlschlag.

Die Strukturauflösung verwendet keine Forward-Returns.

## 4. Forward-Outcomes

Standardhorizonte:

- 5T
- 10T
- 20T
- 40T
- 60T

Raw Close definiert ausschließlich den beobachteten Session-Kalender. Performance benötigt Adjusted Close.

Wenn Adjusted Close fehlt:

- kein Ersatz durch Raw Close,
- kein erfundener Return,
- Outcome bleibt unavailable.

Für Pfadmetriken werden Adjusted High/Low über den täglichen `adj_close / close`-Faktor abgeleitet. Fehlt die notwendige Adjusted-Information, bleiben MFE/MAE bzw. Target-Hit unbekannt.

## 5. Projektionszonen

Für jede erstmals kausal sichtbare 6C-Zone werden unter anderem geprüft:

- Treffer innerhalb 5/10/20/40/60 Sessions,
- erste Trefferdauer in Sessions,
- directional Forward Return,
- MFE,
- MAE.

Wichtig: Das Trefferfenster beginnt **erst in der nächsten beobachteten Session**.

Ein High/Low derselben Session darf nicht als zukünftiger Treffer gezählt werden, weil die Projektion ggf. erst am Session-Ende bekannt wurde.

Zonen, die bei ihrer ersten Beobachtung bereits `inside` oder `reached` sind, sind keine sauberen prospektiven Future-Hit-Claims.

### W5

Die beiden 6C-W5-Basen bleiben Research-Kandidaten:

1. W5 ≈ 1,0 × W1 ab W4,
2. W5 ≈ 0,618 × Origin→W3 ab W4.

6G darf beide historisch/prospektiv vergleichen. Es darf aber **keinen** numerischen W5-Level automatisch einfrieren oder promoten.

## 6. Review-Routing

6D liefert Review-Kontexte, keine Orderlogik.

6G darf deshalb messen, ob ein Review-Kontext nachfolgend directional sinnvoll war:

- supportive review: Bewegung in Elliott-Richtung,
- defensive review: Bewegung gegen Elliott-Richtung,
- neutral review: keine binäre Erfolgsdefinition.

6G darf außerdem MFE/MAE und Forward-Returns dokumentieren.

Es darf jedoch **keinen Round-trip-P&L erfinden**, solange keine explizite Ausführungs-Policy eingefroren ist, z. B.:

- welcher Positionsanteil reduziert wird,
- zu welchem Reentry-Trigger zurückgekauft wird,
- welche Reentry-Größe gilt.

Die in 6D vorhandenen Kostenannahmen reichen dafür allein nicht aus.

## 7. Cross-System-Validierung

6G konsumiert ausschließlich kausale 6E-Relations.

Für `confirmation`, `redundancy`, `elliott_rescue`, `scanner_rescue` und `conflict` kann der spätere directional Outcome gegen eine **gleiche Event-Klasse / gleiche Orientierung** verglichen werden.

Es wird nicht einfach ein anderer Marktzeitraum als Baseline verwendet.

Aktuelle Phase-2/3/4/5-Erkenntnisse dürfen nicht rückwirkend als historische Model-Claims erzeugt werden. Wenn 6E keinen damaligen Claim hat, bleibt dieser fehlend.

## 8. Externer Markt-/Sektorkontext

6G kann Alpha nur dann berechnen, wenn 6F für den Claim-Zeitpunkt eine kausale und nutzbare externe Zuordnung auflöst.

Zulässig:

- `sufficient`,
- `limited`.

Nicht zulässig als reale externe Evidenz:

- `unreliable`,
- `unavailable`,
- Scanner-Peers als Ersatz für externen Kontext.

Fehlt verifizierter Kontext, bleibt Benchmark-Alpha fehlend.

## 9. Unsicherheit

6G verwendet dieselbe robuste Grundlogik wie die korrigierte Phase 2:

- Circular moving observation-date blocks,
- effektive Blocklänge = `2 × Horizont`,
- komplette Datumskluster bleiben zusammen,
- mindestens zwei zeitlich getrennte Support-Regionen,
- iid-Intervalle dürfen robuste Evidenz nicht begründen.

Bei unzureichender zeitlicher Abdeckung wird das robuste 95%-Intervall nicht geschätzt.

## 10. Promotion

6G promotet nichts automatisch.

Mögliche Statuswerte des Reports:

- `awaiting_unspent_prospective_evidence`
- `prospective_evidence_accumulating_no_automatic_promotion`

Selbst wenn später robuste prospektive Effekte entstehen, ist eine getrennte Promotion-/Integrationsentscheidung erforderlich.

## 11. Explizit nicht Teil von 6G

- kein autonomes BUY/HOLD/SELL,
- keine Orderanweisung,
- keine nachträgliche Elliott-Neuzählung mit Zukunftswissen,
- kein Raw-Fallback für Performance,
- keine erfundene Branchenklassifikation,
- keine automatische W5-Level-Auswahl,
- keine erfundene Teilverkaufsquote,
- keine erneute Optimierung alter Scannerphasen,
- kein Wiederverwenden verbrauchter Holdouts als unabhängige Bestätigung.

## 12. Abnahme

6G ist technisch abgeschlossen, wenn:

1. Prefix-Replay kausal und prefix-invariant ist.
2. Strukturvalidierung von Performance getrennt bleibt.
3. 5/10/20/40/60T-Outcomes ohne Raw-Fallback berechnet werden.
4. Target-Hits niemals dieselbe Signal-Session rückwirkend verwenden.
5. MFE/MAE nur bei belastbarer Adjusted-Pfadabdeckung berechnet werden.
6. Routing ohne eingefrorene Ausführungsregel keinen Round-trip-P&L behauptet.
7. Cross-System-Relationen nur kausale 6E-Claims verwenden.
8. Markt-/Sektor-Alpha nur qualitätsgeprüften 6F-Kontext verwendet.
9. robuste Unsicherheit die 2×H-Dateblockregel verwendet und unter zwei Support-Regionen fail-closed bleibt.
10. Legacy- und unverbrauchte prospective Evidence maschinenlesbar getrennt sind.
11. keine automatische Promotion, Handelsentscheidung oder Order erzeugt wird.

Empirische Promotion-Reife kann erst nach ausreichender unverbrauchter prospektiver Evidenz entstehen.
