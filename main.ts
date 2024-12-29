import prettyMilliseconds from "pretty-ms";
import { collectionId, didId, insertLink, kv, pathId, recordId } from "./db.js";
import { Firehose, type CommitEvt } from "./firehose.js";

const atURI = /^at:\/\/([^\/#?]+)\/([^\/#?]+)\/([^\/#?]+)$/;
function* walkObject(
  obj: unknown,
  path: (string | number)[] = [],
  indices: number[] = []
): Generator<[string, string | null, string, string, string]> {
  let m;
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      yield* walkObject(obj[i], [...path, 0], [...indices, i]);
    }
  } else if (obj !== null && typeof obj === "object") {
    for (const key in obj) {
      if (Object.hasOwn(obj, key)) {
        yield* walkObject(
          (obj as Record<string, unknown>)[key],
          [...path, key],
          indices
        );
      }
    }
  } else if (typeof obj === "string" && (m = obj.match(atURI))) {
    yield [
      JSON.stringify(path),
      indices.length ? JSON.stringify(indices) : null,
      decodeURIComponent(m[1]),
      decodeURIComponent(m[2]),
      decodeURIComponent(m[3]),
    ];
  }
}

let lastUpdate = 0;
let processed = 0;
let traceMs: Record<string, number> = {};
let traceCount: Record<string, number> = {};
export const trace = <T>(name: string, fn: () => T): T => {
  const start = performance.now();
  try {
    return fn();
  } finally {
    traceMs[name] = (traceMs[name] || 0) + (performance.now() - start);
    traceCount[name] = (traceCount[name] || 0) + 1;
  }
};
export const traceAsync = <T>(
  name: string,
  fn: () => Promise<T>
): Promise<T> => {
  const start = performance.now();
  return fn().finally(() => {
    traceMs[name] = (traceMs[name] || 0) + (performance.now() - start);
    traceCount[name] = (traceCount[name] || 0) + 1;
  });
};

function process(event: CommitEvt) {
  const now = performance.now();
  processed++;
  const since = now - lastUpdate;
  if (since > 1000) {
    kv.set("cursor", JSON.stringify(firehose.cursor));
    console.log(
      "behind by",
      prettyMilliseconds((Date.now() - new Date(event.time).valueOf()) / 1000),
      "-",
      Math.floor(processed / (since / 1000)),
      "commits per second\ntraces:",
      Object.fromEntries(
        Object.entries(traceMs).map(([k, v]) => [
          k,
          prettyMilliseconds(v / (traceCount[k] || 1), {
            formatSubMilliseconds: true,
          }) +
            " × " +
            traceCount[k],
        ])
      )
    );
    processed = 0;
    for (const k in traceMs) traceMs[k] = 0;
    for (const k in traceCount) traceCount[k] = 0;
    lastUpdate = now;
  }
  if ("record" in event) {
    trace("overall", () => {
      const colid = trace("collection_id", () =>
        collectionId(event.collection)
      );
      const didid = trace("did_id", () => didId(event.did));
      const rid = trace("record_id", () => recordId(didid, colid, event.rkey));
      trace("walk", () => {
        for (const [path, indices, d2, c2, r2] of walkObject(event.record)) {
          const pid = trace("path_id", () => pathId(path));
          const d2id = trace("did_id", () => didId(d2));
          const c2id = trace("collection_id", () => collectionId(c2));
          const r2id = trace("record_id", () => recordId(d2id, c2id, r2));
          trace("link", () => insertLink(rid, r2id, pid, indices));
        }
      });
    });
  }
}

const firehose = new Firehose({
  cursor: JSON.parse(kv.get("cursor")! || "false") || undefined,
});

firehose.on("commit", process);

firehose.start();
