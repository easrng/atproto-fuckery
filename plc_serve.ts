import { Database } from "@db/sqlite";
import { fileURLToPath } from "node:url";
import { concat } from "@std/bytes";
import { md } from "./md.ts";
import { validateOperationLog } from "./plc_validate/data.ts";
import { PlcError } from "./plc_validate/error.ts";

export const baseHeaders = {
  "access-control-allow-origin": "*",
  "access-control-allow-headers": "*",
};

const db = new Database(
  fileURLToPath(new URL("plc_data.db", import.meta.url)),
  {
    readonly: true,
    int64: true,
  }
);
db.exec(
  `
    PRAGMA cache_size = 10000;         
    -- CREATE TABLE IF NOT EXISTS records (
    --     cid BLOB,
    --     did BLOB,
    --     operation BLOB,
    --     nullified INTEGER,
    --     created_at INTEGER
    -- );
    -- CREATE INDEX IF NOT EXISTS index_did ON records (did);
    -- CREATE INDEX IF NOT EXISTS index_created_at ON records (created_at);
  `
);

const latestOperation = db.prepare(`
  select *
  from records
  order by created_at desc 
  limit 1;
`);

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const [
  tmpl_did_first,
  tmpl_did,
  tmpl_op,
  tmpl_cid,
  tmpl_nullified,
  tmpl_createdAt,
  tmpl_end,
  tmpl_true,
  tmpl_false,
] = [
  `{"did":"did:plc:`,
  `\n{"did":"did:plc:`,
  `","operation":`,
  `,"cid":"`,
  `","nullified":`,
  `,"createdAt":"`,
  `"}`,
  "true",
  "false",
].map((e) => encoder.encode(e));

const encode = (value: any, first: boolean) => [
  first ? tmpl_did_first : tmpl_did,
  value.did,
  tmpl_op,
  value.operation,
  tmpl_cid,
  value.cid,
  tmpl_nullified,
  value.nullified ? tmpl_true : tmpl_false,
  tmpl_createdAt,
  encoder.encode(new Date(value.created_at).toJSON()),
  tmpl_end,
];

export default {
  async fetch(request: Request): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: baseHeaders });
    }
    if (request.method !== "GET") {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: baseHeaders,
      });
    }

    const url = new URL(request.url);

    if (url.pathname === "/") {
      const opJson = JSON.stringify(
        JSON.parse(decoder.decode(concat(encode(latestOperation.get(), true)))),
        null,
        2
      );

      return md`
        # plc.easrng.net

        a did:plc mirror

        ## routes

        - \`/did:plc:*\`
          fetch a did document
        - \`/export\`
          dump plc operations as jsonl
          query params:
          - \`count\` (default: 1000)
            number of items per page
            you can also pass 'all' to just get everything in one request (it's like 20GB lol)
          - \`after\` (optional)
            get operations created after a date
          - \`did\` (optional)
            only get operations for a specific did (can be specified multiple times!)

        ## latest op

        \`\`\`json
        ${opJson}
        \`\`\`
      `;
    }
    const maybeDid = decodeURIComponent(url.pathname.split("/")[1]);
    if (maybeDid?.startsWith("did:plc:")) {
      const did = maybeDid;
      const result = db
        .prepare(
          `
        SELECT *
        FROM records
        WHERE did = ?
        order by created_at asc
        `
        )
        .all(encoder.encode(did.slice("did:plc:".length)));
      try {
        const doc = await validateOperationLog(
          did,
          result.map((e) => JSON.parse(decoder.decode(concat(encode(e, true)))))
        );
        return Response.json(doc, {
          headers: {
            ...baseHeaders,
            "content-type": "application/did+ld+json; charset=utf-8",
          },
        });
      } catch (e) {
        return Response.json(
          {
            message: PlcError.is(e)
              ? e.message
              : (console.error(e), "Internal Server Error"),
          },
          { status: 500, headers: baseHeaders }
        );
      }
    }
    if (url.pathname !== "/export") {
      return new Response("Not Found", { status: 404, headers: baseHeaders });
    }

    const all = url.searchParams.get("count") === "all";
    const count = parseInt(url.searchParams.get("count") ?? "1000", 10);
    if (!all && (!count || count < 1)) {
      return new Response("Invalid 'count' parameter", {
        status: 400,
        headers: baseHeaders,
      });
    }

    const after = url.searchParams.get("after");
    const afterTimestamp = after ? new Date(after).getTime() : 0;
    if (Number.isNaN(afterTimestamp)) {
      return new Response("Invalid 'after' parameter", {
        status: 400,
        headers: baseHeaders,
      });
    }

    const dids = (url.searchParams.getAll("did").join(",") || null)
      ?.split(/,+/g)
      .map((e) => encoder.encode(e.match(/^did:plc:(.+)$/)![1]));

    const result = db
      .prepare(
        `
      SELECT *
      FROM records
      WHERE (created_at > ?)
      ${
        dids?.length
          ? `and did in (${"?".repeat(dids.length).split("").join(",")})`
          : ""
      }
      order by created_at asc
      ${all ? "" : "limit ?"}
    `
      )
      .iter(afterTimestamp, ...(dids || []), ...(all ? [] : [count]));

    let first = true;
    return new Response(
      new ReadableStream({
        cancel() {
          result.return?.();
        },
        pull(controller) {
          try {
            let written = 0;
            let close = false;
            const chunks = [];
            while (written < 1_000 * 500) {
              const { done, value } = result.next();
              if (done) {
                close = true;
                break;
              }
              for (const buf of encode(value, first)) {
                written += buf.byteLength;
                chunks.push(buf);
              }
              first = false;
            }
            if (written) {
              const view =
                (controller?.byobRequest?.view?.byteLength || 0) >= written &&
                controller.byobRequest?.view;
              const output = view
                ? new Uint8Array(view.buffer, view.byteOffset, view.byteLength)
                : new Uint8Array(written);
              let index = 0;
              for (const buffer of chunks) {
                output.set(buffer, index);
                index += buffer.length;
              }
              if (view) {
                controller.byobRequest.respond(written);
              } else {
                console.warn("couldn't use byob zerocopy");
                controller.enqueue(output);
              }
            }
            if (close) {
              controller.close();
            }
          } catch (e) {
            console.error(e);
            controller.error(e);
          }
        },
        type: "bytes",
        autoAllocateChunkSize: 1_000 * 1_000,
      }),
      {
        status: 200,
        headers: { ...baseHeaders, "content-type": "application/jsonlines" },
      }
    );
  },
};
