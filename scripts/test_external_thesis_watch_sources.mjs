// Regression for the actual production source-note validator and current registry.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';

const root = new URL('../argus-preview/observations/', import.meta.url);
const js = readFileSync(new URL('external-thesis-watch.js', root), 'utf8');
const registry = JSON.parse(readFileSync(new URL('external_thesis_source_notes.json', root), 'utf8'));
const refs = JSON.parse(readFileSync(new URL('external_thesis_watch_reference.json', root), 'utf8'));
const candidates = JSON.parse(readFileSync(new URL('external_thesis_watch_candidates.json', root), 'utf8'));
const all = new Set([...refs.items, ...candidates.items].map(x => x.symbol));
const begin = js.indexOf('const SOURCE_NOTE_KINDS=');
const end = js.indexOf('async function refresh()', begin);
assert.ok(begin >= 0 && end > begin, 'production validator must be directly testable');
const validate = runInNewContext(js.slice(begin, end) + '\nvalidateSourceNotesDocument;', {});
const result = validate(registry, all);
assert.equal(result.valid.length, 27);
assert.equal(result.quarantined.length, 0);
assert.equal(result.valid.filter(x => x.symbol === 'IFX.DE').length, 2);
const hood = result.valid.find(x => x.symbol === 'HOOD');
assert.equal(hood.source_kind, 'user_documented_external_video');
assert.equal(hood.source_target.lower, 168);
assert.equal(hood.source_target.upper, 178);
assert.ok(candidates.items.some(x => x.symbol === 'LULU'), 'separate LULU Elliott candidate kept');
for (const symbol of ['MSFT', '1211.HK', 'TSLA', 'JPM', 'GS']) {
  assert.ok(candidates.items.some(x => x.symbol === symbol), 'new candidate retained: ' + symbol);
  assert.ok(result.valid.some(x => x.symbol === symbol), 'new source note retained: ' + symbol);
}
assert.equal(candidates.items.length, 15);
const invalid = structuredClone(registry);
invalid.items.push({...invalid.items[0],source_target:{lower:200,upper:100,currency:'USD'}});
const quarantined = validate(invalid, all);
assert.equal(quarantined.valid.length, 27, 'one broken note may not discard valid notes');
assert.deepEqual(Array.from(quarantined.quarantined), [28]);
assert.ok(js.includes("n.source_kind==='observed_newsletter'"), 'video sources must not join automatic newsletter follow-up');
console.log('PASS: 27 notes, IFX/HOOD/LULU, target range and per-item quarantine');
