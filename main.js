/**
 * The entry point.
 *
 * Starts DuckDB, hands the connection to the dashboard, and gets out of the
 * way. Nothing here is awaited at the top level: a module with a pending
 * top-level await leaves the document in its loading state, which blocks a
 * headless browser's evaluations and makes the page look hung to anything
 * driving it. `start()` is called and the module finishes.
 */

import * as LG from './node_modules/@toclocoinc/lattice-grid/lattice-grid.esm.min.js';
import { createChart } from './node_modules/@toclocoinc/lattice-grid/modules/charts.esm.min.js';
import { createKPI } from './node_modules/@toclocoinc/lattice-grid/modules/kpi.esm.min.js';
import { DEMO_LICENCE } from './src/licence.js';
import { startDuckDB, recordingConnection, resetRangeAccounting } from './src/duckdb.js';
import { buildDashboard } from './src/dashboard.js';
import { PARQUET } from './src/flights.js';

/* Applied before anything is drawn: a grid keeps whatever licence was in force
   when it was built. */
LG.setLicence(DEMO_LICENCE);

const root = document.querySelector('#app');

/** The statements DuckDB ran, newest last. The push-plan panel reads this. */
const sqlLog = [];
const recordSql = (connection) => recordingConnection(connection, (entry) => {
  sqlLog.push(entry);
  if (sqlLog.length > 200) sqlLog.splice(0, sqlLog.length - 200);
});

function showProgress(message) {
  root.textContent = '';
  const panel = document.createElement('div');
  panel.className = 'loading';
  const title = document.createElement('h1');
  title.textContent = 'US flight delays, June 2026';
  const line = document.createElement('p');
  line.className = 'loading-message';
  line.textContent = message;
  panel.append(title, line);
  root.append(panel);
  return (next) => { line.textContent = next; };
}

function showError(error) {
  root.textContent = '';
  const panel = document.createElement('div');
  panel.className = 'loading';
  const title = document.createElement('h1');
  title.textContent = 'The flights could not be loaded';
  const line = document.createElement('p');
  line.className = 'loading-message';
  line.textContent = String(error?.message ?? error);
  const hint = document.createElement('p');
  hint.className = 'loading-message';
  hint.textContent =
    'This page runs DuckDB compiled to WebAssembly, loaded from jsDelivr, and reads a Parquet file '
    + 'from this same site with HTTP range requests. It needs a browser with WebAssembly, and a host '
    + 'that answers 206 Partial Content.';
  panel.append(title, line, hint);
  root.append(panel);
  console.error('[flights demo]', error);
}

/** The size of the file, and the range measurement recorded when it was built. */
async function readFacts() {
  const out = { facts: null, recorded: null };
  try {
    const response = await fetch('./data/facts.json', { cache: 'no-store' });
    if (response.ok) out.facts = await response.json();
  } catch { /* the page works without it */ }
  try {
    const response = await fetch('./data/range-measurement.json', { cache: 'no-store' });
    if (response.ok) out.recorded = await response.json();
  } catch { /* the page works without it */ }
  return out;
}

async function start() {
  const started = performance.now();
  const update = showProgress('Starting DuckDB in your browser...');
  try {
    /* Clear the local server's byte tally, when there is one, so the panel's
       measurement covers this page load and not the last one too. */
    await resetRangeAccounting();

    const { connection, version, close } = await startDuckDB(update);
    update('Reading the Parquet file...');

    const { facts, recorded } = await readFacts();

    const built = buildDashboard({
      root,
      LG,
      createChart,
      createKPI,
      connection,
      recordSql,
      sqlLog,
      duckdbVersion: version,
    });
    if (facts?.bytes) built.setParquetSize(facts.bytes);
    built.facts = facts;

    window.addEventListener('pagehide', () => { built.destroy(); close(); }, { once: true });

    /*
     * Ready means the whole page is true, not that something appeared: the
     * first window of rows, the match count, the whole-set statistics behind
     * the tiles, and the charts those statistics feed. Anything driving this
     * page then has one thing to wait for.
     */
    const deadline = Date.now() + 90000;
    const settled = () => built.grid?.rows.count() > 0
      && built.matchCount != null
      && built.headline?.flights != null
      && Object.keys(built.chartGrids).length === 5;
    while (Date.now() < deadline && !settled()) {
      await new Promise((r) => setTimeout(r, 150));
    }
    await built.renderRanges(recorded);

    built.ready = true;
    built.bootMs = Math.round(performance.now() - started);
    built.parquet = new URL(PARQUET, location.href).href;
    window.__flightsDemo = built;
    console.log('[flights demo] ready', {
      ms: built.bootMs, rows: built.grid?.rows.totalCount(), duckdb: version,
    });
  } catch (error) {
    window.__flightsDemo = { ready: false, error: String(error?.message ?? error) };
    showError(error);
  }
}

start();
