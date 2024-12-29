import { Database } from "bun:sqlite";
import flru_ from "flru";
const flru = flru_ as unknown as typeof flru_.default;
class KV {
  readonly #database: Database;
  readonly #table: string;
  #select;
  #set;
  #delete;

  constructor(database: Database, table?: string) {
    this.#database = database;
    this.#table = table || "kv";
    this.#database
      .prepare(
        `
			CREATE TABLE IF NOT EXISTS ${this.#table} (
				key TEXT NOT NULL PRIMARY KEY,
				value TEXT NOT NULL
			) WITHOUT ROWID;
		`
      )
      .run();
    this.#select = this.#database.prepare(
      `SELECT value FROM ${this.#table} WHERE key = $key`
    );
    this.#set = this.#database.prepare(
      `INSERT OR REPLACE INTO ${this.#table} (key, value) VALUES ($key, $value)`
    );
    this.#delete = this.#database.prepare(
      `DELETE FROM ${this.#table} WHERE key = $key`
    );
  }

  get(key: string) {
    const row = this.#select.get(key) as { value: string } | null;
    return row?.value;
  }

  set(key: string, value: string) {
    return this.#set.run(key, value);
  }

  remove(key: string) {
    return this.#delete.run(key);
  }
}

const db = new Database("refview.db");
export const kv = new KV(db);
db.exec(`
  PRAGMA cache_size = 10000;  -- Set cache size to 10,000 pages
  pragma journal_mode = WAL;
  pragma synchronous = normal;
  CREATE TABLE IF NOT EXISTS records (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      did INTEGER NOT NULL,
      collection INTEGER NOT NULL,
      rkey TEXT NOT NULL,
      FOREIGN KEY (did) REFERENCES dids(id),
      FOREIGN KEY (collection) REFERENCES collections(id)
  );
  CREATE TABLE IF NOT EXISTS paths (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      path TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS dids (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      did TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS collections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      collection TEXT NOT NULL UNIQUE
  );
  CREATE TABLE IF NOT EXISTS links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source INTEGER NOT NULL,
      target INTEGER NOT NULL,
      path INTEGER NOT NULL,
      indices TEXT,
      FOREIGN KEY (source) REFERENCES records(id),
      FOREIGN KEY (target) REFERENCES records(id),
      FOREIGN KEY (path) REFERENCES paths(id)
  );
  CREATE INDEX IF NOT EXISTS idx_records_did_collection_rkey ON records (rkey, did, collection);
  CREATE INDEX IF NOT EXISTS idx_dids_did ON dids (did);
  CREATE INDEX IF NOT EXISTS idx_collections_collection ON collections (collection);
  CREATE INDEX IF NOT EXISTS idx_paths_path ON paths (path);
`);

const qInsertLink = db.prepare(
  "INSERT INTO links (source, target, path, indices) VALUES (?, ?, ?, ?);"
);
export const insertLink = qInsertLink.run.bind(qInsertLink);

const qLookupRecord = db.prepare(
  "SELECT id FROM records WHERE did = ? AND collection = ? AND rkey = ?;"
);
const qInsertRecord = db.prepare(
  "INSERT INTO records (did, collection, rkey) VALUES (?, ?, ?);"
);
export function recordId(
  didId: number,
  collectionId: number,
  rkey: string
): number {
  const query = qLookupRecord.get(didId, collectionId, rkey);
  if (query != null) {
    return (query as any).id;
  }
  const { lastInsertRowid } = qInsertRecord.run(didId, collectionId, rkey);
  return lastInsertRowid as number;
}

const qLookupDid = db.prepare("SELECT id FROM dids WHERE did = ?;");
const qInsertDid = db.prepare("INSERT INTO dids (did) VALUES (?);");
export function didId(did: string): number {
  const query = qLookupDid.get(did);
  if (query != null) {
    return (query as any).id;
  }
  const { lastInsertRowid } = qInsertDid.run(did);
  return lastInsertRowid as number;
}

const collectionCache = flru<number>(1024);
const qLookupCollection = db.prepare(
  "SELECT id FROM collections WHERE collection = ?;"
);
const qInsertCollection = db.prepare(
  "INSERT INTO collections (collection) VALUES (?);"
);
export function collectionId(collection: string): number {
  const cached = collectionCache.get(collection);
  if (typeof cached === "number") {
    return cached;
  }
  const query = qLookupCollection.get(collection);
  if (query != null) {
    collectionCache.set(collection, (query as any).id);
    return (query as any).id;
  }
  const { lastInsertRowid } = qInsertCollection.run(collection);
  collectionCache.set(collection, lastInsertRowid as number);
  return lastInsertRowid as number;
}

const pathCache = flru<number>(1024);
const qLookupPath = db.prepare("SELECT id FROM paths WHERE path = ?;");
const qInsertPath = db.prepare("INSERT INTO paths (path) VALUES (?);");
export function pathId(path: string): number {
  const cached = pathCache.get(path);
  if (typeof cached === "number") {
    return cached;
  }
  const query = qLookupPath.get(path);
  if (query != null) {
    pathCache.set(path, (query as any).id);
    return (query as any).id;
  }
  const { lastInsertRowid } = qInsertPath.run(path);
  pathCache.set(path, lastInsertRowid as number);
  return lastInsertRowid as number;
}
