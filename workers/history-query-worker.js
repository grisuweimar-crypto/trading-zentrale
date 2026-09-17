export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const params = new URLSearchParams(url.search);
    const symbol = params.get('symbol');
    const symbols = params.get('symbols');
    const from = params.get('from');
    const to = params.get('to');
    const days = params.get('days');
    const fields = params.get('fields');

    const requested = [symbol, ...(symbols ? symbols.split(',') : [])].filter(Boolean);
    if (!requested.length && !from && !to && !days && !fields) {
      return Response.json({ error: 'empty_query', schema_version: 'history_query_v1' }, { status: 400, headers: { 'Cache-Control': 'no-store' } });
    }
    if (requested.length > 25) {
      return Response.json({ error: 'request_too_large', schema_version: 'history_query_v1' }, { status: 400, headers: { 'Cache-Control': 'no-store' } });
    }
    if (days && (Number(days) <= 0 || Number(days) > 365)) {
      return Response.json({ error: 'request_too_large', schema_version: 'history_query_v1' }, { status: 400, headers: { 'Cache-Control': 'no-store' } });
    }

    const source = env.HISTORY_SOURCE_URL || 'https://example.invalid/history_recent.csv';
    const response = await fetch(source, { cf: { cacheEverything: true, cacheTtl: 300 } });
    if (!response.ok) {
      return Response.json({ error: 'source_unavailable', schema_version: 'history_query_v1' }, { status: 502, headers: { 'Cache-Control': 'no-store' } });
    }

    const text = await response.text();
    const rows = text.split(/\r?\n/).filter(Boolean);
    const header = rows.shift()?.split(',') || [];
    const data = [];

    for (const row of rows) {
      const values = row.split(',');
      const entry = Object.fromEntries(header.map((key, index) => [key.trim(), values[index] || '']));
      if (!symbol && !symbols && !from && !to && !days) {
        data.push(entry);
        continue;
      }
      const symbolMatch = !symbol && !symbols ? true : (symbol ? entry.symbol === symbol : false) || (symbols ? symbols.split(',').includes(entry.symbol) : false);
      const dateMatch = (!from && !to) || (!from || entry.date >= from) && (!to || entry.date <= to);
      if (symbolMatch && dateMatch) {
        data.push(entry);
      }
    }

    const output = {
      schema_version: 'history_query_v1',
      source: 'history_recent.csv',
      as_of: new Date().toISOString().slice(0, 10),
      count: data.length,
      data: fields ? data.map((entry) => Object.fromEntries((fields.split(',').filter(Boolean).map((field) => [field.trim(), entry[field.trim()] || ''])))) : data,
    };

    return Response.json(output, {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=300, s-maxage=300',
        ETag: crypto.subtle ? 'pending' : 'not-supported'
      }
    });
  }
};
