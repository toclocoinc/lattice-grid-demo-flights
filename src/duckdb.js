/**
 * DuckDB-Wasm, started in the page.
 *
 * The grid ships no engine. `duckdbAdapter` takes a connection the host made
 * and imports nothing, so everything about DuckDB — which version, where it
 * comes from, what extensions are loaded — is decided here and nowhere else.
 *
 * Two details matter more than the rest:
 *
 *   `LOAD httpfs`. Without it DuckDB's HTTP filesystem is not in play and the
 *   engine reads the whole Parquet file in one GET before it can answer
 *   anything. With it, the file is opened by its footer and read in byte
 *   ranges: the metadata, then only the row groups and column chunks a query
 *   needs. That is the difference between a 15 MB download and a few hundred
 *   kilobytes, and it is the measurement the README quotes.
 *
 *   The absolute URL. DuckDB resolves the path itself, inside a worker, so a
 *   relative path has nothing to resolve against. `new URL(..., location.href)`
 *   is what makes the same code work at the site root and under a project
 *   path on GitHub Pages.
 */

/** Pinned. A CDN import with no version is a demo that breaks on someone else's release day. */
export const DUCKDB_VERSION = '1.32.0';
export const DUCKDB_MODULE = `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_VERSION}/+esm`;

/**
 * Start the engine and open a connection with httpfs loaded.
 *
 * @param {(message: string) => void} [onStatus] progress, for the loading panel
 * @returns {Promise<{duckdb: object, db: object, connection: object, worker: Worker, version: string, close: () => void}>}
 */
export async function startDuckDB(onStatus = () => {}) {
  onStatus(`Loading DuckDB-Wasm ${DUCKDB_VERSION} from jsDelivr...`);
  const duckdb = await import(/* @vite-ignore */ DUCKDB_MODULE);
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

  onStatus('Starting the engine...');
  const worker = await duckdb.createWorker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  const connection = await db.connect();
  await connection.query('LOAD httpfs;');

  const version = (await readOne(connection, 'SELECT version() AS v'))?.v ?? 'unknown';

  return {
    duckdb,
    db,
    connection,
    worker,
    version: String(version),
    close() {
      try { connection.close?.(); } catch { /* already gone */ }
      try { worker.terminate?.(); } catch { /* already gone */ }
    },
  };
}

/** One row of a one-row query, as a plain object. */
export async function readOne(connection, sql) {
  const result = await connection.query(sql);
  const rows = result.toArray().map((r) => r.toJSON());
  return rows[0] ?? null;
}

/**
 * Wrap a connection so every statement that passes through it is recorded.
 *
 * The adapter is handed the wrapper, never the connection, so there is no way
 * for a statement to reach DuckDB without being seen. This is what fills the
 * push-plan panel: not a reconstruction of what the grid probably asked for,
 * but the text the engine actually ran, with the values that were bound to it
 * and how long it took.
 *
 * It has exactly the two methods `duckdbAdapter` uses. `prepare` matters:
 * filter values are bound, never interpolated, and the recorded `params` are
 * what proves it.
 *
 * @param {object} connection a DuckDB-Wasm connection
 * @param {(entry: {sql: string, params: unknown[], ms: number, rows: number}) => void} record
 */
export function recordingConnection(connection, record) {
  const note = (sql, params, started, result) => {
    const rows = typeof result?.numRows === 'number' ? result.numRows : 0;
    record({ sql, params, ms: Math.round(performance.now() - started), rows });
  };
  return {
    query: async (sql) => {
      const started = performance.now();
      const result = await connection.query(sql);
      note(sql, [], started, result);
      return result;
    },
    prepare: async (sql) => {
      const statement = await connection.prepare(sql);
      return {
        query: async (...params) => {
          const started = performance.now();
          const result = await statement.query(...params);
          note(sql, params, started, result);
          return result;
        },
        close: () => statement.close?.(),
      };
    },
  };
}

/**
 * Whether the byte-accounting endpoint can exist at all.
 *
 * `/__ranges` is served by `tools/serve.mjs` and by nothing else. Asking a
 * static host for it is not a harmless miss: GitHub Pages answers the POST
 * with 405 and the GET with 404, and a browser writes both into the console as
 * errors. A demo that claims to run without errors cannot be the thing putting
 * two of them there, and a check that insists on zero console errors is only
 * worth having if the page does not manufacture them.
 *
 * So the endpoint is asked for only where it can answer. Anywhere else the page
 * falls back to `data/range-measurement.json` and says that it is a recorded
 * measurement rather than a live one — which it already did, correctly; it just
 * made two doomed requests first.
 */
const hasRangeAccounting = () => (
  location.protocol !== 'file:'
  && ['localhost', '127.0.0.1', '[::1]', '::1'].includes(location.hostname)
);

/**
 * The byte accounting, read back from the server that served the file.
 *
 * DuckDB reads inside a worker, through its own HTTP filesystem, so nothing in
 * the page can observe those requests: `performance.getEntriesByType('resource')`
 * never sees them, and a `fetch` patch in the page is not in the path. The only
 * place that knows what went over the wire is the server, so that is where the
 * count is kept. `tools/serve.mjs` records every request for a file under
 * `data/` and serves the tally at `/__ranges`.
 *
 * @returns {Promise<object|null>} null wherever no such server is listening
 */
export async function readRangeAccounting() {
  if (!hasRangeAccounting()) return null;
  try {
    const response = await fetch('./__ranges', { cache: 'no-store' });
    if (!response.ok) return null;
    const body = await response.json();
    return body && Array.isArray(body.files) ? body : null;
  } catch {
    return null;
  }
}

/** Clear the server's range tally, so the next measurement starts from zero. */
export async function resetRangeAccounting() {
  if (!hasRangeAccounting()) return false;
  try {
    await fetch('./__ranges', { method: 'POST', cache: 'no-store' });
    return true;
  } catch {
    return false;
  }
}
