import { Database } from "bun:sqlite";
import { fileURLToPath } from "node:url";
import { concat, equals } from "@std/bytes";
import { assert } from "@std/assert/assert";

const db = new Database(fileURLToPath(new URL("plc_data.db", import.meta.url)));
db.exec(
  `
    PRAGMA cache_size = 10000;
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = normal;
    CREATE TABLE IF NOT EXISTS records (
        cid BLOB,
        did BLOB,
        operation BLOB,
        nullified INTEGER,
        created_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS index_did ON records (did);
    CREATE INDEX IF NOT EXISTS index_created_at ON records (created_at);
    CREATE INDEX IF NOT EXISTS index_cid ON records (cid);
  `
);
const stmt = db.query(
  "INSERT INTO records (cid, did, operation, nullified, created_at) VALUES (?1, ?2, ?3, ?4, ?5)"
);
const commitTx = db.transaction((rows) => {
  if (!rows.length) return;
  console.log("inserting", new Date(rows[0][4]).toJSON());
  for (const row of rows) {
    stmt.run(...row);
  }
});
let { created_at: after, cid: after_cid } =
  db
    .prepare<{ created_at: number; cid: Uint8Array }, []>(
      `SELECT created_at, cid FROM records ORDER BY created_at DESC LIMIT 1;`
    )
    .get() ?? {};
function expectl(data: Uint8Array, needle: Uint8Array): Uint8Array {
  assert(equals(data.subarray(0, needle.length), needle));
  return data.subarray(needle.length);
}
function splitl(data: Uint8Array, needle: number): [Uint8Array, Uint8Array] {
  const needleIndex = data.indexOf(needle);
  assert(needleIndex !== -1);
  return [data.subarray(0, needleIndex), data.subarray(needleIndex + 1)];
}
function splitr(data: Uint8Array, needle: number): [Uint8Array, Uint8Array] {
  const needleIndex = data.lastIndexOf(needle);
  assert(needleIndex !== -1);
  return [data.subarray(needleIndex + 1), data.subarray(0, needleIndex + 1)];
}
function expectr(data: Uint8Array, needle: Uint8Array): Uint8Array {
  assert(equals(data.subarray(-1 * needle.length), needle));
  return data.subarray(0, -1 * needle.length);
}
function boolr(
  data: Uint8Array,
  needle_true: Uint8Array,
  needle_false: Uint8Array
): [1 | 0, Uint8Array] {
  if (equals(data.subarray(-1 * needle_true.length), needle_true)) {
    return [1, data.subarray(0, -1 * needle_false.length)];
  }
  assert(equals(data.subarray(-1 * needle_false.length), needle_false));
  return [0, data.subarray(0, -1 * needle_false.length)];
}
const encoder = new TextEncoder();
const t_didplc = encoder.encode(`{"did":"did:plc:`);
const t_op = encoder.encode(`,"operation":`);
const t_end = encoder.encode(`"}`);
const t_createdAt = encoder.encode(`,"createdAt":"`);
const t_notnull = encoder.encode(`","nullified":false`);
const t_null = encoder.encode(`","nullified":true`);
const t_cid = encoder.encode(`,"cid":"`);
const d = new TextDecoder();
while (1) {
  const aj = after ? new Date(after).toJSON() : undefined;
  console.log("downloading", aj);
  const r = await fetch(
    "https://plc.directory/export?count=1000" + (after ? `&after=` + aj! : "")
  );
  if (!r.ok) {
    throw new Error(`http error ${r.status} - ${Bun.inspect(r.headers)}`);
  }
  let lines = 0;
  let rows: any[][] = [];
  for await (let line of readStream(r.body!)) {
    lines++;
    let did, createdAt, nullified, cid;
    line = expectl(line, t_didplc);
    [did, line] = splitl(line, 0x22);
    line = expectl(line, t_op);
    line = expectr(line, t_end);
    [createdAt, line] = splitr(line, 0x22);
    const created_at = Date.parse(d.decode(createdAt));
    line = expectr(line, t_createdAt);
    [nullified, line] = boolr(line, t_null, t_notnull);
    [cid, line] = splitr(line, 0x22);
    if (after_cid && equals(cid, after_cid)) continue;
    const operation = expectr(line, t_cid);
    rows.push([
      (after_cid = cid.slice()),
      did.slice(),
      operation.slice(),
      nullified,
      created_at,
    ]);
    after = created_at;
    if (rows.length > 500) {
      commitTx(rows);
      rows = [];
    }
  }
  commitTx(rows);
  if (!rows.length) await Bun.sleep(750);
}
async function* readStream(stream: ReadableStream) {
  let prevBuf = new ArrayBuffer(100);
  let prev: Uint8Array | undefined;

  const reader = stream.getReader();
  let result;
  while ((result = await reader.read())) {
    const { done, value } = result;

    if (done) {
      if (prev) {
        yield prev;
      }
      return;
    }

    let start = 0;
    for (let i = 0; i < value.length; i++) {
      if (value[i] === 0x0a) {
        if (prev) {
          yield concat([prev, value.subarray(start, i)]);
          prev = undefined;
        } else {
          yield value.subarray(start, i);
        }
        start = i + 1;
      }
    }
    const keep = value.byteLength - start;
    if (keep) {
      const lastPrevLength = prev?.byteLength || 0;
      const lastPrev = prev;
      if (lastPrevLength + keep > prevBuf.byteLength) {
        prevBuf = new ArrayBuffer((lastPrevLength + keep) * 2);
        if (lastPrev) {
          new Uint8Array(prevBuf, 0, lastPrev.byteLength).set(lastPrev);
        }
      }
      prev = new Uint8Array(prevBuf, 0, lastPrevLength + keep);
      prev.set(value.subarray(start), lastPrevLength);
    }
  }
}
