import { decode, decodeFirst, fromBytes, toCIDLink } from "@atcute/cbor";
import type { At, ComAtprotoSyncSubscribeRepos } from "@atcute/client/lexicons";
import { EventEmitter } from "node:events";
import { traceAsync } from "./main.ts";

/**
 * Options for the Firehose class.
 */
export interface FirehoseOptions {
  /**
   * The Relay to connect to.
   */
  relay?: string;
  /**
   * The cursor to listen from. If not provided, the firehose will start from the latest event.
   */
  cursor?: string;
  /**
   * Whether to automatically reconnect when no new messages are received for a period of time.
   * This will not reconnect if the connection was closed intentionally.
   * To do that, listen for the `"close"` event and call `start()` again.
   * @default true
   */
  autoReconnect?: boolean;
}

export type CommitEvt = Create | Update | Delete;

export type CommitMeta = {
  seq: number;
  time: string;
  commit: At.CID;
  blocks: unknown;
  rev: string;
  uri: At.Uri;
  did: string;
  collection: string;
  rkey: string;
};

export type Create = CommitMeta & {
  event: "create";
  record: any;
  cid: At.CID;
};

export type Update = CommitMeta & {
  event: "update";
  record: any;
  cid: At.CID;
};

export type Delete = CommitMeta & {
  event: "delete";
};

export class Firehose extends EventEmitter<{
  commit: [CommitEvt];
  open: [];
  close: [string];
  websocketError: [{ cursor: string; error: any }];
  reconnect: [];
}> {
  /** The relay to connect to. */
  public relay: string;

  /** WebSocket connection to the relay. */
  public ws?: InstanceType<typeof WebSocket>;

  /** The current cursor. */
  public cursor = "";

  private autoReconnect: boolean;

  private reconnectTimeout: Timer | undefined;

  /**
   * Creates a new Firehose instance.
   * @param options Optional configuration.
   */
  constructor(options: FirehoseOptions = {}) {
    super();
    this.relay = options.relay ?? "wss://bsky.network";
    this.cursor = options.cursor ?? "";
    this.autoReconnect = options.autoReconnect ?? true;
  }

  /**
   * Opens a WebSocket connection to the relay.
   */
  start() {
    const cursorQueryParameter = this.cursor ? `?cursor=${this.cursor}` : "";
    this.ws = new WebSocket(
      `${this.relay}/xrpc/com.atproto.sync.subscribeRepos${cursorQueryParameter}`
    );
    this.ws.binaryType = "arraybuffer";

    this.ws.addEventListener("open", () => {
      this.emit("open");
    });

    this.ws.addEventListener("message", async ({ data }) => {
      try {
        const events = await this.parseMessage(data);
        if (events) {
          for (const event of events) {
            this.emit("commit", event);
            await Promise.resolve();
            this.cursor = `${event.seq}`;
          }
        }
      } catch (error) {
        console.error(error);
        //this.emit("error", { cursor: this.cursor, error });
      } finally {
        if (this.autoReconnect) this.preventReconnect();
      }
    });

    this.ws.addEventListener("close", () => {
      this.emit("close", this.cursor);
    });

    this.ws.addEventListener("error", (error) => {
      this.emit("websocketError", {
        cursor: this.cursor,
        error: error instanceof ErrorEvent ? error.error : error,
      });
    });
  }

  /**
   * Closes the WebSocket connection.
   */
  close() {
    this.ws?.close();
  }

  private async parseMessage(
    data: ArrayBuffer
  ): Promise<CommitEvt[] | undefined> {
    const [header, remainder] = decodeFirst(new Uint8Array(data));

    const { t, op } = parseHeader(header);

    if (op === -1) {
      const [body, remainder2] = decodeFirst(remainder);
      if (remainder2.length > 0) {
        throw new Error("Excess bytes in message");
      }
      throw new Error(`Error: ${body.message}\nError code: ${body.error}`);
    }

    if (t === "#commit") {
      const result = decode(
        remainder
      );
      if (!result) return;
      return result;
    }
    return undefined;
  }

  private preventReconnect() {
    if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    this.reconnectTimeout = setTimeout(() => {
      this.reconnect();
    }, 5_000);
  }

  private reconnect() {
    this.ws?.close();
    this.start();
    this.emit("reconnect");
  }
}
function parseHeader(header: any): { t: string; op: 1 | -1 } {
  if (
    !header ||
    typeof header !== "object" ||
    !header.t ||
    typeof header.t !== "string" ||
    !header.op ||
    typeof header.op !== "number"
  )
    throw new Error("Invalid header received");
  return { t: header.t, op: header.op };
}
