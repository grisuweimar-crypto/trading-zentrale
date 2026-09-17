# Research Query Layer

## Warum die History nicht geshardet wird

`history_recent.csv` bleibt die canonische, vollständige Source of Truth. Sie enthält die echte Scanner-History und wird nicht in symbolbezogene Dateien aufgeteilt. Das schützt die Integrität der Daten, vermeidet Duplikate und verhindert, dass Auswertungen auf partiellen oder rekonstruierten Zeitreihen beruhen.

Der Query-Layer und `daily_research.json` sind nur Zugriffsschichten. Sie reduzieren die Last für Watch- und Chat-Workflows, ohne die fachlich vollständige Quelle zu ersetzen.

## Rolle von `daily_research.json`

Die tägliche Datei ist eine kompakte, lesefreundliche Zusammenfassung für Depot-, Scanner- und Chat-Workflows. Sie enthält die aktuellen Kennzahlen eines vollständigen Scanner-Scans sowie eine kompakte Historie der wichtigsten Dynamiken, Bewertungen und Klassifikationen.

Sie wird nach jedem validierten vollständigen Lauf neu erzeugt. Die Datei ist bewusst leicht und dient der täglichen Watch-Ansicht, nicht der archivischen Quelle.

## Rolle des Query-Endpunkts

Der Query-Layer ermöglicht gezielte historische Abfragen ohne den kompletten 7+ MB-Downloads zu laden. Es werden nur die angeforderten Felder und Zeilen zurückgegeben, in der Regel als JSON.

Wenn ein Cloudflare Worker oder ein vergleichbarer serverloser Endpoint genutzt wird, bleibt die Quelle weiterhin `history_recent.csv` und nicht eine zweite fachliche Datenbank.

## Beispielabfragen

- `GET /history?symbol=RACE&days=120`
- `GET /history?symbols=RACE,MGMA.V,UEC,RGTI&days=120`
- `GET /history?symbol=RACE&from=2026-06-01&to=2026-09-17`
- `GET /history?symbol=RACE&fields=date,score,rank,rank_percentile,r_code,rs3m,trend200`

## Response-Schema

```json
{
  "schema_version": "history_query_v1",
  "source": "history_recent.csv",
  "as_of": "2026-09-17",
  "count": 12,
  "data": []
}
```

Zusätzliche Metadaten wie `etag` und `source_hash` können für HTTP-Caching verwendet werden.

## Cache-Verhalten

Da der Historienstand sich nur nach Scannerläufen ändert, ist HTTP-Caching sinnvoll. Der Query-Layer kann einen Hash der Quelldatei als ETag zurückgeben und einen `Cache-Control`-Header setzen. Dadurch müssen GitHub bzw. die Quelle bei wiederholten Anfragen nicht erneut komplett belastet werden.

## Deployment des Workers

Ein Cloudflare Worker ist für den hier beschriebenen, öffentlichen Read-Only-Zugriff sinnvoll, sofern das Projekt ein statisches Hosting mit einer dateibasierten Quelle nutzt. Der Worker sollte aber keine eigene fachliche Datenbank erstellen, sondern nur `history_recent.csv` oder `history_analysis.csv` auslesen und filtern.

Empfohlener Ablauf:

1. Repository mit `history_recent.csv` und `history_metadata.json` veröffentlichen.
2. Worker mit einem Bucket- oder GitHub-Download-URL-Endpoint konfigurieren.
3. `CACHE_CONTROL`, `ETag` und `max`-Limits definieren.
4. Keine Zugangsdaten, keine Secrets im Code hinterlegen.

## Grenzen der API

- maximal 25 Symbole pro Anfrage
- maximal 365 Tage oder ein kompatibler Datumsbereich
- nur erlaubte Spaltennamen
- keine Authentifikation für öffentliche Research-Daten
- kein Import von Daten außerhalb der Quelldatei

## Unterschied Query-Layer vs. Source of Truth

- Source of Truth: `history_recent.csv` und bei Langzeitforschung `history_analysis.csv`
- Query-Layer: Filter, Zugriffslogik, Cache, Zugriffsbeschränkungen, JSON-Transform

Die Quelle bleibt die signifikante fachliche Datei; der Query-Layer ist lediglich eine Zugriffsschicht.

## Definition von 5T/10T/20T/40T

`5T`, `10T`, `20T` und `40T` beziehen sich auf echte Handelstage und nicht auf Kalender- oder Scanner-Zeilen. Wochenenden und andere Nicht-Handelstage verlängern oder verkürzen keinen Forward-Horizont. Wenn keine gültige spätere Kurs- oder Score-Beobachtung existiert, darf der Horizon nicht als Treffer gezählt werden.

## Cooldown-Logik

Bei direkt aufeinanderfolgenden Tagen desselben Symbols wird derselbe historische Musterfall nur einmal als unabhängiger Fall gezählt. Ein konfigurierbarer Cooldown wie 5 Handelstage verhindert eine künstliche Vervielfachung desselben Ereignisses.

## Historische Vergleichslogik

Die Vergleichslogik versucht möglichst relevante historische Fälle zu finden, allerdings ohne zu eng zu filtern. Zunächst werden Rechteckbereiche aus Rank-Perzentil, R-Code, RS3M- und Trend200-Bereichen verwendet. Wenn zu wenige Treffer existieren, werden die Filter schrittweise erweitert. Die genaue Filterstufe und die zugehörige Beschreibung werden für jeden Fall mit ausgegeben.

## Worker-Stub

Ein minimaler Cloudflare Worker kann als schlanker Proxy auf die CSV-Datei dienen und keine Daten rekonstruieren. Die tatsächliche Produktion muss jedoch im Betrieb mit der vorhandenen Datenquelle und den Maximallimits konfiguriert werden.

```js
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const params = new URLSearchParams(url.search);
    const symbol = params.get('symbol');
    const symbols = params.get('symbols');
    if (!symbol && !symbols) {
      return Response.json({ error: 'empty_query' }, { status: 400 });
    }
    return Response.json({ schema_version: 'history_query_v1', count: 0, data: [] }, {
      headers: { 'Cache-Control': 'public, max-age=300', 'Content-Type': 'application/json' }
    });
  }
};
```

Die vollständige Deploy-Umgebung hängt vom vorhandenen Cloudflare-Setup ab; für dieses Repository gilt: keine Zugangsdaten im Code, keine neue fachliche Datenquelle und keine Authentifikation für öffentliche Research-Daten.
