/**
 * Load the demo in a real browser and check that it is telling the truth.
 *
 * "It rendered" is not the claim this demo makes, so it is not what is
 * checked. What is checked:
 *
 *   - the rows on screen came from DuckDB reading the Parquet file, and the
 *     row count is the file's;
 *   - the push-plan panel shows the statements that ran, with bound values,
 *     and reports nothing left unpushed;
 *   - every headline figure equals the same statistic computed here in Node,
 *     by a second DuckDB over the same file, written out independently;
 *   - the charts drew marks, and their data matches Node's aggregates;
 *   - the file was read in byte ranges, not downloaded: the server that served
 *     it reports how many requests and how many bytes, for the first paint and
 *     for one filtered query, and both are printed;
 *   - the grouping guard refuses while the match is too large and lets go once
 *     a filter has narrowed it;
 *   - no console errors, and no watermark.
 *
 * `--record` writes the range measurement to data/range-measurement.json,
 * which is what the page falls back to when it is published on GitHub Pages
 * and has no local server to ask.
 *
 * `--live <url>` runs the same page checks against the published site instead
 * of a local server. The byte accounting cannot come from GitHub Pages, so
 * that mode asks the host directly for a byte range and insists on a 206 with
 * the right Content-Range, which is the fact the page's claim rests on.
 *
 * Usage:
 *   node tools/verify.mjs [--record] [--shots <dir>]
 *   node tools/verify.mjs --live [https://toclocoinc.github.io/lattice-grid-demo-flights/]
 */

import { spawn } from 'node:child_process';
import { access, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { startServer } from './serve.mjs';
/* The page's own name for the data file, not a second copy of it: renaming the
   file is one line in src/flights.js and this follows. */
import { PARQUET as PAGE_PARQUET } from '../src/flights.js';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');

const args = process.argv.slice(2);
const record = args.includes('--record');
const shotIndex = args.indexOf('--shots');
const shotDir = shotIndex >= 0 ? resolve(args[shotIndex + 1]) : null;
const liveIndex = args.indexOf('--live');
const live = liveIndex >= 0;
const liveUrl = live
  ? (args[liveIndex + 1] && !args[liveIndex + 1].startsWith('--')
    ? args[liveIndex + 1]
    : 'https://toclocoinc.github.io/lattice-grid-demo-flights/')
  : null;

const PARQUET = PAGE_PARQUET.replace(/^\.?\//, '');
/* Match the served file by its name. Matching by extension is what broke when
   the file was renamed to dodge the host's compression. */
const DATA_FILE = PARQUET.split('/').pop();

/* The published check is the one people look at the screenshots of, so it runs
   at a size worth looking at. */
const VIEWPORT = live ? { width: 1920, height: 1200 } : { width: 1500, height: 1000 };

/*
 * How long to wait, relative to the local run.
 *
 * Against localhost the engine's range reads cost a memory copy. Against
 * GitHub Pages every one of them is an HTTPS round trip, and a whole-set
 * statistic is a few hundred of them. The work is identical; only the wire is
 * different, so the right response is patience, not a smaller claim.
 */
const PATIENCE = live ? 6 : 1;

const CHROME_CANDIDATES = [
  process.env.CHROME_PATH,
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/snap/bin/chromium',
].filter(Boolean);

async function findChrome() {
  for (const path of CHROME_CANDIDATES) {
    try { await access(path); return path; } catch { /* next */ }
  }
  throw new Error(`No browser found. Tried:\n  ${CHROME_CANDIDATES.join('\n  ')}\nSet CHROME_PATH.`);
}

function requireModernNode() {
  if (typeof WebSocket === 'undefined') {
    throw new Error(`This check needs Node 22 or newer for its WebSocket. You are on ${process.version}.`);
  }
}

const failures = [];
const notes = [];

function check(ok, description, detail) {
  const line = `${description}${detail ? ` (${detail})` : ''}`;
  if (ok) notes.push(`  ok   ${line}`);
  else { failures.push(line); notes.push(`  FAIL ${line}`); }
}

/** Two numbers agree to `places` decimals. Nulls only agree with nulls. */
function same(a, b, places = 6) {
  if (a == null || b == null) return a == null && b == null;
  return Math.abs(Number(a) - Number(b)) < 10 ** -places;
}

const int = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'));
const mb = (n) => `${(Number(n) / 1048576).toFixed(2)} MB`;

/* ------------------------------------------------------------------ */
/* The independent answer: a second DuckDB, in Node, over the same file */
/* ------------------------------------------------------------------ */

/**
 * Everything the page claims, recomputed here.
 *
 * The SQL is written out rather than borrowed from the grid, so an error in
 * the adapter's SQL generation cannot hide by being made twice. `filter` is
 * the WHERE the page's chip stands for, written again in the same terms.
 */
async function nodeAnswers(filterSql) {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const db = await DuckDBInstance.create(':memory:');
  const conn = await db.connect();
  const file = join(root, PARQUET).replaceAll("'", "''");
  /* A parenthesised subquery, not a trailing WHERE: one of the queries below
     adds a WHERE of its own, and two of them in one statement is a parse
     error. This way every query can filter further without knowing whether a
     filter is already in force. */
  const base = `read_parquet('${file}')`;
  const from = filterSql ? `(SELECT * FROM ${base} WHERE ${filterSql})` : base;
  const one = async (sql) => {
    const result = await conn.runAndReadAll(sql);
    return result.getRowObjects()[0];
  };
  const many = async (sql) => (await conn.runAndReadAll(sql)).getRowObjects();

  const headline = await one(`
    SELECT count(*) AS flights,
           avg(arr_delay) AS meanArrival,
           quantile_cont(arr_delay, 0.95) AS p95Arrival,
           avg(on_time_n) AS onTime,
           avg(cancelled_n) AS cancelled
    FROM ${from}
  `);
  const carriers = await many(`
    SELECT carrier, count(*) AS flights, median(arr_delay) AS med, quantile_cont(arr_delay, 0.95) AS p95
    FROM ${from} GROUP BY carrier ORDER BY carrier
  `);
  const hours = await many(`
    SELECT dep_hour AS hour, count(*) AS flights, avg(arr_delay) AS mean
    FROM ${from} GROUP BY dep_hour ORDER BY dep_hour
  `);
  const routes = await many(`
    SELECT route, count(*) AS flights FROM ${from} GROUP BY route ORDER BY flights DESC, route LIMIT 40
  `);
  /* Formatted in SQL rather than in JavaScript, so the key compared against the
     page is a plain string on both sides and no date marshalling is in the way. */
  const days = await many(`
    SELECT strftime(flight_date, '%Y-%m-%d') AS day, avg(arr_delay) AS mean
    FROM ${from} GROUP BY flight_date ORDER BY flight_date
  `);
  const delays = await many(`
    SELECT arr_delay AS delay, count(*) AS flights FROM ${from} WHERE arr_delay IS NOT NULL GROUP BY arr_delay
  `);
  conn.closeSync();

  const n = (v) => (typeof v === 'bigint' ? Number(v) : (v == null ? null : Number(v)));
  return {
    headline: {
      flights: n(headline.flights),
      meanArrival: n(headline.meanArrival),
      p95Arrival: n(headline.p95Arrival),
      onTime: n(headline.onTime),
      cancelled: n(headline.cancelled),
    },
    carriers: carriers.map((r) => ({ carrier: r.carrier, flights: n(r.flights), median: n(r.med), p95: n(r.p95) })),
    hours: hours.map((r) => ({ hour: n(r.hour), flights: n(r.flights), mean: n(r.mean) })),
    routes: routes.map((r) => ({ route: r.route, flights: n(r.flights) })),
    days: days.map((r) => ({ day: String(r.day), mean: n(r.mean) })),
    /* The same five-minute bucketing the page does, written again. */
    buckets: (() => {
      const map = new Map();
      for (const row of delays) {
        const delay = n(row.delay);
        const clamped = Math.min(180, Math.max(-60, delay));
        const floor = Math.floor(clamped / 5) * 5;
        map.set(floor, (map.get(floor) ?? 0) + n(row.flights));
      }
      return [...map.entries()].sort((a, b) => a[0] - b[0]).map(([floor, flights]) => ({ floor, flights }));
    })(),
  };
}

/**
 * Write one host's measurement into data/range-measurement.json, keeping the
 * other host's.
 *
 * Both are worth publishing, and the contrast is the interesting part: the same
 * work against a local server and against the published host reads different
 * amounts, because an edge cache and a `cache-control` that permits reuse mean
 * the browser answers DuckDB's repeat reads of the same byte ranges without
 * going back to the wire. Replacing one with the other would throw away the
 * comparison, so this merges.
 */
async function recordMeasurement({ host, url, phases }, size) {
  const file = join(root, 'data', 'range-measurement.json');
  let existing = {};
  try { existing = JSON.parse(await readFile(file, 'utf8')); } catch { /* first time */ }
  const hosts = (existing.hosts && typeof existing.hosts === 'object') ? existing.hosts : {};
  hosts[host] = {
    name: host === 'pages' ? 'GitHub Pages' : 'a local server (tools/serve.mjs)',
    ...(url ? { url } : {}),
    measuredOn: new Date().toISOString().slice(0, 10),
    countedBy: host === 'pages'
      ? "the browser's own network log, the DuckDB worker's requests included — there is no server of ours in the path"
      : 'the server that served the file, which is the only thing that can see a worker\'s requests',
    ...phases,
  };
  const out = {
    file: PARQUET,
    size,
    hosts,
    note: 'The two hosts differ because GitHub Pages permits caching (cache-control: max-age=600) '
      + 'while the local server sends no-store, so on Pages the browser answers DuckDB\'s repeat reads '
      + 'of the same byte ranges without going back to the wire. Neither figure is a whole-file download.',
  };
  await writeFile(file, `${JSON.stringify(out, null, 2)}\n`);
  console.log(`  written to data/range-measurement.json (hosts: ${Object.keys(hosts).join(', ')})`);
}

/* ------------------------------------------------------------------ */
/* The browser                                                         */
/* ------------------------------------------------------------------ */

let browser = null;
let browserPid = null;
let profile = null;
let served = null;

async function shutdown() {
  /* Only ever the browser this run started, by its own process group. */
  if (browserPid) {
    try { process.kill(-browserPid, 'SIGKILL'); } catch { /* already gone */ }
    try { browser?.kill('SIGKILL'); } catch { /* already gone */ }
  }
  served?.server?.close();
  if (profile) await rm(profile, { recursive: true, force: true }).catch(() => {});
}

function freePort() {
  return import('node:net').then(({ createServer }) => new Promise((ok, fail) => {
    const probe = createServer();
    probe.on('error', fail);
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address();
      probe.close(() => ok(port));
    });
  }));
}

async function run() {
  requireModernNode();
  const chromePath = await findChrome();

  let origin;
  if (live) {
    origin = liveUrl.replace(/\/$/, '');
    console.log(`Browser: ${chromePath}`);
    console.log(`Checking the published site: ${origin}/`);
  } else {
    served = await startServer(0);
    origin = `http://127.0.0.1:${served.port}`;
    console.log(`Browser: ${chromePath}`);
    console.log(`Serving: ${origin}`);
  }

  profile = await mkdtemp(join(tmpdir(), 'flights-verify-'));
  const port = await freePort();
  browser = spawn(chromePath, [
    '--headless=new',
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${profile}`,
    '--no-first-run',
    '--no-default-browser-check',
    '--no-sandbox',
    '--disable-gpu',
    '--disable-dev-shm-usage',
    '--hide-scrollbars',
    `--window-size=${VIEWPORT.width},${VIEWPORT.height}`,
    'about:blank',
  ], { stdio: ['ignore', 'pipe', 'pipe'], detached: true });
  browserPid = browser.pid;
  browser.stderr.on('data', () => {});

  let wsUrl;
  for (let i = 0; i < 150 && !wsUrl; i += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/json/version`);
      if (response.ok) wsUrl = (await response.json()).webSocketDebuggerUrl;
    } catch { /* not up yet */ }
    if (!wsUrl) await sleep(200);
  }
  if (!wsUrl) throw new Error('the browser never opened its debugging port');

  const socket = new WebSocket(wsUrl);
  await new Promise((ok, fail) => {
    socket.onopen = ok;
    socket.onerror = () => fail(new Error('could not attach to the browser'));
  });

  let nextId = 0;
  const pending = new Map();
  let consoleErrors = [];
  let pageErrors = [];
  /* Requests for the Parquet file, as the browser saw them. Declared before the
     message handler because the handler fills them. */
  let wireEntries = [];
  const wirePending = new Map();
  const networkFor = () => wirePending;
  const wireLog = () => wireEntries;

  socket.onclose = () => {
    for (const { reject } of pending.values()) reject(new Error('the browser went away before it answered'));
    pending.clear();
  };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.id != null && pending.has(message.id)) {
      const { resolve: ok, reject } = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error)));
      else ok(message.result);
      return;
    }
    if (message.method === 'Runtime.consoleAPICalled' && message.params.type === 'error') {
      consoleErrors.push(message.params.args.map((a) => a.value ?? a.description ?? a.type).join(' '));
    }
    if (message.method === 'Runtime.exceptionThrown') {
      const details = message.params.exceptionDetails;
      pageErrors.push(details.exception?.description || details.text);
    }
    if (message.method === 'Log.entryAdded' && message.params.entry.level === 'error') {
      consoleErrors.push(message.params.entry.text);
    }
    /* A newly attached target (the DuckDB worker) reports nothing until its own
       Network domain is on. */
    if (message.method === 'Target.attachedToTarget') {
      const child = message.params.sessionId;
      socket.send(JSON.stringify({ id: ++nextId, method: 'Network.enable', params: {}, sessionId: child }));
      socket.send(JSON.stringify({ id: ++nextId, method: 'Runtime.runIfWaitingForDebugger', params: {}, sessionId: child }));
    }
    if (message.method === 'Network.requestWillBeSent') {
      const { requestId, request } = message.params;
      if (request.url.endsWith(DATA_FILE)) {
        const headers = request.headers || {};
        const range = headers.Range ?? headers.range ?? null;
        networkFor().set(requestId, { url: request.url, range, method: request.method, status: null, bytes: 0 });
      }
    }
    if (message.method === 'Network.responseReceived') {
      const entry = networkFor().get(message.params.requestId);
      if (entry) entry.status = message.params.response.status;
    }
    if (message.method === 'Network.loadingFailed') {
      const entry = networkFor().get(message.params.requestId);
      if (entry) {
        entry.status = 'FAILED';
        entry.errorText = message.params.errorText;
        entry.blockedReason = message.params.blockedReason ?? null;
        networkFor().delete(message.params.requestId);
        wireLog().push(entry);
      }
    }
    if (message.method === 'Network.loadingFinished') {
      const entry = networkFor().get(message.params.requestId);
      if (entry) {
        entry.bytes = message.params.encodedDataLength ?? 0;
        networkFor().delete(message.params.requestId);
        wireLog().push(entry);
      }
    }
  };

  const send = (method, params = {}, sessionId) => new Promise((ok, reject) => {
    const id = ++nextId;
    pending.set(id, { resolve: ok, reject });
    socket.send(JSON.stringify({ id, method, params, sessionId }));
  });

  const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true });
  const call = (method, params) => send(method, params, sessionId);

  await call('Page.enable');
  await call('Runtime.enable');
  await call('Log.enable');
  await call('Emulation.setDeviceMetricsOverride', { ...VIEWPORT, deviceScaleFactor: 1, mobile: false });

  /*
   * Watch the wire, including the worker's.
   *
   * On a local server the byte accounting comes from the server, which is the
   * honest place to count. Against GitHub Pages there is no such server, and
   * DuckDB does its reading inside a worker, so nothing on the page can see
   * those requests either. The browser can: auto-attaching to the worker target
   * and enabling Network on it reports every request DuckDB makes, with its
   * Range header, its status, and the bytes that came back.
   */
  await call('Network.enable');
  await call('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  const evaluate = async (expression, ms = 30000) => {
    const result = await Promise.race([
      call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true }),
      sleep(ms).then(() => { throw new Error('the page did not answer in time'); }),
    ]);
    if (result.exceptionDetails) {
      throw new Error(`${result.exceptionDetails.text} ${result.exceptionDetails.exception?.description || ''}`);
    }
    return result.result.value;
  };

  const waitFor = async (expression, timeout, what) => {
    const until = Date.now() + timeout;
    while (Date.now() < until) {
      try { if (await evaluate(expression, 15000)) return true; } catch { /* still busy */ }
      await sleep(500);
    }
    throw new Error(`timed out waiting for ${what}`);
  };

  const shoot = async (name) => {
    if (!shotDir) return;
    await mkdir(shotDir, { recursive: true });
    const { data } = await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true });
    await writeFile(join(shotDir, `${name}.png`), Buffer.from(data, 'base64'));
    console.log(`  shot ${join(shotDir, `${name}.png`)}`);
  };

  /* ---------------------------------------------------------------- */
  /* Open it                                                           */
  /* ---------------------------------------------------------------- */

  served?.reset();
  consoleErrors = [];
  pageErrors = [];
  wireEntries = [];
  await call('Page.navigate', { url: `${origin}/` });
  await waitFor('!!window.__flightsDemo', 180000 * PATIENCE, 'the page to report in');
  const state = await evaluate('({ ready: window.__flightsDemo.ready, error: window.__flightsDemo.error || null })');
  if (!state.ready) throw new Error(`the page reported a failure: ${state.error}`);
  await waitFor('window.__flightsDemo.grid && window.__flightsDemo.grid.rows.count() > 0', 90000 * PATIENCE, 'rows');
  /* The aggregates land a moment after the first page of rows. */
  try {
    await waitFor('window.__flightsDemo.headline && window.__flightsDemo.headline.flights != null', 60000 * PATIENCE, 'the whole-set figures');
  } catch (error) {
    /* Say what the page was actually doing when the patience ran out, and keep
       a picture of it. A timeout with no diagnosis is not a finding. */
    const stuck = await evaluate(`(() => {
      const d = window.__flightsDemo || {};
      return {
        ready: d.ready, error: d.error || null,
        rows: d.grid ? d.grid.rows.count() : null,
        match: d.matchCount, headline: d.headline || null,
        charts: d.chartGrids ? Object.keys(d.chartGrids).length : 0,
        bootMs: d.bootMs, rangeSource: d.rangeSource,
        sqlSeen: (document.querySelector('[data-plan-sql]') || {}).textContent || '',
      };
    })()`).catch((e) => ({ unreadable: String(e.message) }));
    console.log(`\nThe page did not settle. What it was doing:\n${JSON.stringify(stuck, null, 2)}`);
    console.log(`Console errors so far: ${JSON.stringify(consoleErrors.slice(0, 8))}`);
    console.log(`Page errors so far: ${JSON.stringify(pageErrors.slice(0, 8))}`);
    console.log(`\nParquet requests that COMPLETED (${wireEntries.length}, `
      + `${mb(wireEntries.reduce((n, e) => n + (e.bytes || 0), 0))}):`);
    for (const e of wireEntries.slice(0, 14)) {
      console.log(`  ${String(e.status).padStart(6)}  range=${e.range ?? '(none)'}  ${e.bytes} bytes`
        + (e.errorText ? `  errorText=${e.errorText}` : '') + (e.blockedReason ? `  blocked=${e.blockedReason}` : ''));
    }
    const stillOpen = [...wirePending.values()];
    console.log(`Parquet requests still UNANSWERED (${stillOpen.length}):`);
    for (const e of stillOpen.slice(0, 14)) {
      console.log(`  pending  range=${e.range ?? '(none)'}  status=${e.status ?? 'none yet'}`);
    }
    await shoot('timed-out');
    throw error;
  }

  /* Everything the browser fetched of the Parquet file up to the moment the
     page declared itself ready. */
  const firstPaintWire = wireEntries.slice();
  const firstPaintRanges = served ? served.ranges() : null;
  const firstPaintFile = firstPaintRanges?.files?.find((f) => f.path.endsWith(DATA_FILE)) ?? null;

  const shown = await evaluate(`(() => {
    const d = window.__flightsDemo;
    const chart = (id) => {
      const c = d.charts[id];
      if (!c) return { drawn: false };
      let points = null;
      try {
        const data = c.data();
        const series = data.series || [];
        points = series[0] && series[0].points ? series[0].points.length : (data.points ? data.points.length : null);
      } catch { /* some types report differently */ }
      const marks = document.querySelectorAll('[data-chart=' + id + '] svg rect, [data-chart=' + id + '] svg path, [data-chart=' + id + '] svg circle, [data-chart=' + id + '] svg line').length;
      return { drawn: true, points, marks };
    };
    const rowsOf = (id) => (d.chartGrids[id] ? d.chartGrids[id].rows.count() : 0);
    const readRows = (id, fields) => {
      const out = [];
      d.chartGrids[id].rows.forEach((r) => { const data = r && r.data ? r.data : r; if (data) out.push(fields.map((f) => data[f])); });
      return out;
    };
    return {
      rows: d.grid.rows.count(),
      total: d.grid.rows.totalCount(),
      match: d.matchCount,
      headline: d.headline,
      provenance: d.aggregateProvenance,
      plan: d.plan ? { unpushed: d.plan.unpushed, needsAll: d.plan.needsAll, full: d.plan.full } : null,
      tiles: Object.fromEntries(d.kpi.tiles().map((t) => [t.id, t.value])),
      tileText: Object.fromEntries(d.kpi.tiles().map((t) => [t.id, t.formatted])),
      watermark: d.grid.licence.watermark(),
      planSql: (document.querySelector('[data-plan-sql]') || {}).textContent || '',
      planSummary: (document.querySelector('[data-plan-summary]') || {}).textContent || '',
      pushed: [...document.querySelectorAll('[data-pushed] li')].map((n) => n.textContent),
      client: [...document.querySelectorAll('[data-client] li')].map((n) => n.textContent),
      groupNote: (document.querySelector('[data-group-note]') || {}).textContent || '',
      groupDisabled: !!(document.querySelector('[data-group]') || {}).disabled,
      rangeText: (document.querySelector('[data-range]') || {}).textContent || '',
      rangeSource: (document.querySelector('[data-range]') || {}).dataset ? document.querySelector('[data-range]').dataset.rangeSource : null,
      rangeHost: (document.querySelector('[data-range]') || {}).dataset ? document.querySelector('[data-range]').dataset.rangeHost : null,
      charts: { dist: chart('dist'), carrier: chart('carrier'), hour: chart('hour'), routes: chart('routes'), daily: chart('daily') },
      chartRows: { dist: rowsOf('dist'), carrier: rowsOf('carrier'), hour: rowsOf('hour'), routes: rowsOf('routes'), daily: rowsOf('daily') },
      carrierRows: readRows('carrier', ['carrier', 'flights', 'median', 'p95']),
      hourRows: readRows('hour', ['id', 'flights', 'mean']),
      routeRows: readRows('routes', ['route', 'flights']),
      bucketRows: readRows('dist', ['floor', 'flights']),
      dayRows: readRows('daily', ['day', 'mean']),
      bootMs: d.bootMs,
      duckdb: d.duckdbVersion,
    };
  })()`);

  console.log(`\nDuckDB in the browser: ${shown.duckdb}; the page was ready in ${shown.bootMs} ms.`);
  await shoot('unfiltered');

  const expected = await nodeAnswers(null);

  /* --------------------------- the rows --------------------------- */

  check(shown.rows > 0, 'rows are painted from DuckDB', `${int(shown.rows)} loaded`);
  check(shown.total === expected.headline.flights,
    'the grid\'s row count is the file\'s row count',
    `${int(shown.total)} against ${int(expected.headline.flights)}`);
  check(shown.match === expected.headline.flights,
    'the match count is the whole file with no filter', `${int(shown.match)}`);
  check(shown.watermark === false, 'no watermark', `licence state reported by the grid`);

  /* ------------------------- the push plan ------------------------ */

  check(/read_parquet\(/.test(shown.planSql), 'the plan panel shows the read_parquet statement');
  check(/LIMIT/.test(shown.planSql), 'the plan panel shows the window as LIMIT');
  check(/count\(\*\)/.test(shown.planSql), 'the plan panel shows the match count as count(*)');
  check(/GROUP BY ROLLUP/.test(shown.planSql), 'the plan panel shows a whole-set aggregate as GROUP BY ROLLUP');
  check(shown.plan != null && Array.isArray(shown.plan.unpushed) && shown.plan.unpushed.length === 0,
    'nothing was left unpushed', `unpushed: ${JSON.stringify(shown.plan?.unpushed)}`);
  check(shown.plan?.needsAll === false, 'a window was fetched, not the whole result');
  check(shown.pushed.length >= 4, 'the "pushed to the engine" list is populated', `${shown.pushed.length} lines`);
  check(shown.client.length >= 1, 'the "stayed in the browser" list is populated', `${shown.client.length} lines`);
  check((shown.provenance?.engine ?? []).length === 5 && (shown.provenance?.client ?? []).length === 0,
    'all five headline statistics were computed by the engine',
    `engine ${JSON.stringify(shown.provenance?.engine)}, client ${JSON.stringify(shown.provenance?.client)}`);

  /* --------------------------- the tiles -------------------------- */

  const tileChecks = [
    ['flights', 0],
    ['meanArrival', 6],
    ['p95Arrival', 6],
    ['onTime', 9],
    ['cancelled', 9],
  ];
  for (const [id, places] of tileChecks) {
    check(same(shown.tiles[id], expected.headline[id], places),
      `the ${id} tile equals Node's own DuckDB answer`,
      `${shown.tiles[id]} against ${expected.headline[id]}`);
  }

  /* -------------------------- the charts -------------------------- */

  for (const id of ['dist', 'carrier', 'hour', 'routes', 'daily']) {
    check(shown.charts[id].drawn, `the ${id} chart was created`);
    check(shown.charts[id].marks > 0, `the ${id} chart drew marks, not an empty frame`, `${shown.charts[id].marks} SVG marks`);
  }

  const carrierShown = new Map(shown.carrierRows.map(([carrier, flights, median, p95]) => [carrier, { flights, median, p95 }]));
  const carrierExpected = expected.carriers.filter((c) => c.median != null);
  check(carrierShown.size === carrierExpected.length,
    'the carrier chart has one bar per carrier', `${carrierShown.size} against ${carrierExpected.length}`);
  let carrierMismatch = null;
  for (const want of carrierExpected) {
    const got = carrierShown.get(want.carrier);
    if (!got || got.flights !== want.flights || !same(got.median, want.median) || !same(got.p95, want.p95)) {
      carrierMismatch = `${want.carrier}: ${JSON.stringify(got)} against ${JSON.stringify(want)}`;
      break;
    }
  }
  check(carrierMismatch === null, 'every carrier figure equals Node\'s', carrierMismatch ?? `${carrierExpected.length} carriers`);

  const hourShown = new Map(shown.hourRows.map(([hour, flights, mean]) => [hour, { flights, mean }]));
  let hourMismatch = null;
  for (const want of expected.hours) {
    const got = hourShown.get(want.hour);
    if (!got || got.flights !== want.flights || !same(got.mean, want.mean)) {
      hourMismatch = `hour ${want.hour}: ${JSON.stringify(got)} against ${JSON.stringify(want)}`;
      break;
    }
  }
  check(hourMismatch === null, 'every departure-hour figure equals Node\'s', hourMismatch ?? `${expected.hours.length} hours`);

  const routesShown = shown.routeRows.map(([route, flights]) => `${route}:${flights}`);
  const routesExpected = expected.routes.map((r) => `${r.route}:${r.flights}`);
  /* Ties at the cut-off can order differently; compare as sets of the counts. */
  check(routesShown.length === routesExpected.length,
    'the treemap shows the 40 busiest routes', `${routesShown.length}`);
  check(routesShown[0] === routesExpected[0],
    'the busiest route and its count match Node\'s', `${routesShown[0]} against ${routesExpected[0]}`);
  const shownTotal = shown.routeRows.reduce((s, [, n]) => s + n, 0);
  const wantTotal = expected.routes.reduce((s, r) => s + r.flights, 0);
  check(shownTotal === wantTotal, 'the 40 routes cover the same flights Node counts', `${int(shownTotal)} against ${int(wantTotal)}`);

  const bucketsShown = shown.bucketRows.map(([floor, flights]) => `${floor}:${flights}`).join(',');
  const bucketsExpected = expected.buckets.map((b) => `${b.floor}:${b.flights}`).join(',');
  check(bucketsShown === bucketsExpected, 'the delay distribution equals Node\'s, bucket for bucket',
    bucketsShown === bucketsExpected ? `${expected.buckets.length} buckets` : `${bucketsShown.slice(0, 120)} against ${bucketsExpected.slice(0, 120)}`);

  const daysShown = new Map(shown.dayRows.map(([day, mean]) => [day, mean]));
  let dayMismatch = null;
  for (const want of expected.days) {
    if (!same(daysShown.get(want.day), want.mean)) {
      dayMismatch = `${want.day}: ${daysShown.get(want.day)} against ${want.mean}`;
      break;
    }
  }
  check(dayMismatch === null, 'every daily mean equals Node\'s', dayMismatch ?? `${expected.days.length} days`);

  /* ---------------------- the grouping guard ---------------------- */

  check(shown.groupDisabled === true, 'grouping is refused while the whole month matches');
  check(/would describe the window/.test(shown.groupNote), 'the guard says why', shown.groupNote.slice(0, 90));

  /* ------------------------- console errors ----------------------- */

  check(consoleErrors.length === 0, 'no console errors on the first paint', consoleErrors.slice(0, 3).join(' | '));
  check(pageErrors.length === 0, 'no uncaught page errors on the first paint', pageErrors.slice(0, 3).join(' | '));

  /* ---------------------------------------------------------------- */
  /* One more page of rows on its own, to separate the two costs       */
  /* ---------------------------------------------------------------- */

  /*
   * Two more phases, measured apart, because they cost wildly different
   * amounts and quoting only the cheap one would be the flattering third of
   * the truth.
   *
   * A sort or a page re-queries the rows and the count and nothing else — the
   * whole-set aggregates only re-run when the filter changes. But an ORDER BY
   * over 607,577 rows makes DuckDB read that whole column across every row
   * group, while an unsorted page reads one row group's chunks. So: sort
   * first, measure; then clear the sort, measure that on its own.
   */
  served?.reset();
  wireEntries = [];
  await evaluate("window.__flightsDemo.grid.sort.set([{ col: 'arr_delay', dir: 'desc' }])");
  await sleep(4000);
  const sortRanges = served ? served.ranges() : null;
  const sortFile = sortRanges?.files?.find((f) => f.path.endsWith(DATA_FILE)) ?? null;
  const sortWire = wireEntries.slice();
  const sorted = await evaluate(`(() => {
    const d = window.__flightsDemo;
    const first = [];
    d.grid.rows.forEach((r) => { if (first.length < 1 && r && r.data) first.push(r.data.arr_delay); });
    return { first: first[0], unpushed: d.plan ? d.plan.unpushed : null, sql: (document.querySelector('[data-plan-sql]') || {}).textContent || '' };
  })()`);
  check(sorted.unpushed?.length === 0, 'the sort was pushed too', JSON.stringify(sorted.unpushed));
  check(/ORDER BY/.test(sorted.sql), 'and appears as ORDER BY in the plan panel');

  served?.reset();
  wireEntries = [];
  await evaluate("window.__flightsDemo.grid.sort.set([])");
  await sleep(4000);
  const pageRanges = served ? served.ranges() : null;
  const pageFile = pageRanges?.files?.find((f) => f.path.endsWith(DATA_FILE)) ?? null;
  const pageWire = wireEntries.slice();
  if (pageFile && sortFile) {
    check(pageFile.bytes < sortFile.bytes,
      'an unsorted page costs far less than a re-sort of the whole file',
      `${mb(pageFile.bytes)} against ${mb(sortFile.bytes)}`);
  }

  /* ---------------------------------------------------------------- */
  /* A filtered query: the figures follow, and so do the byte reads    */
  /* ---------------------------------------------------------------- */

  served?.reset();
  wireEntries = [];
  consoleErrors = [];
  pageErrors = [];
  await evaluate("window.__flightsDemo.togglePreset('late60')");
  await waitFor('window.__flightsDemo.matchCount !== null && window.__flightsDemo.matchCount < window.__flightsDemo.totalRows', 60000, 'the filter to land');
  await sleep(2500);

  const filteredRanges = served ? served.ranges() : null;
  const filteredFile = filteredRanges?.files?.find((f) => f.path.endsWith(DATA_FILE)) ?? null;
  const filteredWire = wireEntries.slice();

  const filtered = await evaluate(`(() => {
    const d = window.__flightsDemo;
    return {
      match: d.matchCount,
      headline: d.headline,
      tiles: Object.fromEntries(d.kpi.tiles().map((t) => [t.id, t.value])),
      plan: d.plan ? { unpushed: d.plan.unpushed, needsAll: d.plan.needsAll } : null,
      planSql: (document.querySelector('[data-plan-sql]') || {}).textContent || '',
      groupDisabled: !!(document.querySelector('[data-group]') || {}).disabled,
      groupNote: (document.querySelector('[data-group-note]') || {}).textContent || '',
      chartRows: { carrier: d.chartGrids.carrier.rows.count(), dist: d.chartGrids.dist.rows.count() },
    };
  })()`);
  await shoot('filtered');

  const expectedFiltered = await nodeAnswers('arr_delay >= 60');
  check(filtered.match === expectedFiltered.headline.flights,
    'the filtered match count equals Node\'s', `${int(filtered.match)} against ${int(expectedFiltered.headline.flights)}`);
  for (const [id, places] of tileChecks) {
    check(same(filtered.tiles[id], expectedFiltered.headline[id], places),
      `the filtered ${id} tile equals Node's`, `${filtered.tiles[id]} against ${expectedFiltered.headline[id]}`);
  }
  check(filtered.plan?.unpushed?.length === 0, 'the filter was pushed whole', JSON.stringify(filtered.plan?.unpushed));
  check(/bound: \[/.test(filtered.planSql), 'the filter value was bound, not interpolated into the SQL');
  check(consoleErrors.length === 0, 'no console errors after filtering', consoleErrors.slice(0, 3).join(' | '));
  check(pageErrors.length === 0, 'no uncaught page errors after filtering', pageErrors.slice(0, 3).join(' | '));

  /* ---------------- the guard lets go when it should --------------- */

  /*
   * Narrow to something small AND non-empty. Stacking `cancelled` on top of
   * `arr_delay >= 60` would match nothing — a cancelled flight has no arrival
   * delay — and a grouping test over an empty set passes without testing
   * anything. So swap the chips rather than adding one.
   */
  await evaluate("window.__flightsDemo.togglePreset('late60'); window.__flightsDemo.togglePreset('cancelled')");
  await waitFor('window.__flightsDemo.matchCount !== null && window.__flightsDemo.matchCount > 0 && window.__flightsDemo.matchCount < 40000', 60000, 'a small, non-empty match');
  await sleep(1500);
  const narrowed = await evaluate(`({
    match: window.__flightsDemo.matchCount,
    groupDisabled: !!(document.querySelector('[data-group]') || {}).disabled,
    note: (document.querySelector('[data-group-note]') || {}).textContent || '',
  })`);
  check(narrowed.match > 0 && narrowed.match < 40000,
    'the filter narrowed to a small, non-empty set', `${int(narrowed.match)} rows`);
  check(narrowed.groupDisabled === false,
    'grouping is offered once the filter has narrowed the match below the limit',
    `${int(narrowed.match)} rows match`);

  await evaluate("window.__flightsDemo.setGrouping('carrier')");
  /* Wait for the REBUILT grid's own fetch to land, not merely for the flag: the
     whole-set pull is what makes the subtotals real, and `plan.full` is only
     true once the new source has answered. Screenshotting before that caught a
     grid with headers and no cells. */
  await waitFor('window.__flightsDemo.grouped === true && window.__flightsDemo.plan && window.__flightsDemo.plan.full === true && window.__flightsDemo.grid.rows.count() > 0', 90000, 'the grouped grid to finish its whole-set pull');
  await sleep(3000);
  const groupedState = await evaluate(`({
    grouped: window.__flightsDemo.grouped,
    full: window.__flightsDemo.plan ? window.__flightsDemo.plan.full : null,
    rows: window.__flightsDemo.grid.rows.count(),
    /* What is actually on screen, not what the model says is loaded. */
    painted: (document.querySelector('.grid-host') || { textContent: '' }).textContent.replace(/\s+/g, ' ').trim().length,
    text: (document.querySelector('.grid-host') || { textContent: '' }).textContent.replace(/\s+/g, ' ').trim(),
    client: [...document.querySelectorAll('[data-client] li')].map((n) => n.textContent),
  })`);
  check(groupedState.painted > 200,
    'the grouped grid actually painted cells, not an empty frame',
    `${groupedState.painted} characters of text in the grid`);
  /* Cancelled flights are on screen here, so this is the one moment the
     cancellation-reason column has something to show. BTS stores a letter; the
     column is declared as a lookup so the reader sees the word BTS defines. */
  /* "Carrier" is also a column heading, so matching it would pass on the
     header row alone. "Weather" and "National Air System" appear nowhere but
     in a cancellation-reason cell. */
  const reason = /Weather|National Air System/.exec(groupedState.text);
  check(reason !== null,
    'the cancellation reason renders as BTS\'s word, not its raw letter',
    reason ? `found "${reason[0]}"` : groupedState.text.slice(0, 100));

  /*
   * A filter that leaves nothing measurable must empty every chart, not leave
   * the last picture on screen. Cancelled flights have no arrival delay, so at
   * this point four of the five charts have nothing to draw.
   */
  const stale = await evaluate(`(() => {
    const d = window.__flightsDemo;
    const marks = (id) => document.querySelectorAll('[data-chart=' + id + '] svg rect, [data-chart=' + id + '] svg path, [data-chart=' + id + '] svg circle').length;
    return { rows: Object.fromEntries(['dist','carrier','hour','daily','routes'].map((id) => [id, d.chartGrids[id].rows.count()])),
             marks: Object.fromEntries(['dist','carrier','hour','daily'].map((id) => [id, marks(id)])) };
  })()`);
  const emptied = ['dist', 'carrier', 'hour', 'daily'].filter((id) => stale.rows[id] === 0);
  check(emptied.length === 4,
    'every delay chart is empty when the filter leaves no measurable delay',
    `rows ${JSON.stringify(stale.rows)}`);
  check(stale.rows.routes > 0, 'while the route treemap, which counts flights, still has data', `${stale.rows.routes} routes`);
  check(groupedState.rows > 0, 'the grouped grid holds rows', `${int(groupedState.rows)}`);
  check(groupedState.grouped === true && groupedState.full === true,
    'the grouped grid holds the whole matching set rather than a window',
    `plan.full ${groupedState.full}`);
  check(groupedState.client.some((line) => /grouping by carrier/.test(line)),
    'the plan panel names grouping as client-side work');
  await shoot('grouped');

  /* ---------------------------------------------------------------- */
  /* The byte accounting                                               */
  /* ---------------------------------------------------------------- */

  const size = (await stat(join(root, PARQUET))).size;
  let measurement = null;

  if (live) {
    /*
     * The byte accounting, as the BROWSER saw it against the published host.
     *
     * There is no server of ours in the path, so the count comes from the
     * browser's own network log — including the DuckDB worker's requests, which
     * is why the worker target is auto-attached. Same phases as the local run,
     * measured the same way, so the two are comparable.
     */
    const fromWire = (entries, label) => {
      const real = entries.filter((e) => e.method !== 'HEAD');
      const partial = real.filter((e) => e.status === 206).length;
      const whole = real.filter((e) => e.status === 200).length;
      const bytes = real.reduce((sum, e) => sum + (e.bytes || 0), 0);
      return {
        label,
        requests: real.length,
        partial,
        whole,
        head: entries.length - real.length,
        bytes,
        percentOfFile: Number(((bytes / size) * 100).toFixed(1)),
      };
    };
    const livePhases = {
      firstPaint: fromWire(firstPaintWire, 'the first paint: a page of rows, the count, and the six whole-set queries behind the tiles and the charts'),
      pageOnly: fromWire(pageWire, 'returning to the unsorted order after a sort — what a page costs when the engine already holds the row groups'),
      sorted: fromWire(sortWire, 're-sorting the whole file by arrival delay and fetching the first page of that order'),
      filtered: { ...fromWire(filteredWire, 'one filtered query: arr_delay >= 60, its count, and the whole-set queries again'), query: 'arr_delay >= 60' },
    };
    const first = livePhases.firstPaint;
    const withRange = firstPaintWire.filter((e) => e.range).length;
    const line = (name, p) => `  ${name.padEnd(22)} ${String(p.requests).padStart(4)} requests, ${String(p.partial).padStart(4)} answered 206, `
      + `${String(p.whole).padStart(2)} answered 200, ${mb(p.bytes).padStart(9)} of ${mb(size)} = ${String(p.percentOfFile).padStart(5)}% of the file`;
    console.log('\nRange reads against the published host, as the browser reported them:');
    console.log(line('first paint', first) + ` (${withRange} carried a Range header)`);
    console.log(line('page, already held', livePhases.pageOnly));
    console.log(line('re-sort whole file', livePhases.sorted));
    console.log(line('arr_delay >= 60', livePhases.filtered));
    check(first.requests > 0, 'the live page fetched the Parquet file', `${first.requests} requests`);
    check(first.partial > 0, 'and Pages answered them with 206 Partial Content', `${first.partial} of ${first.requests}`);
    check(first.whole === 0, 'with no whole-file GET', `${first.whole} responses were 200`);
    check(first.bytes < size, 'so the live first paint did NOT download the file', `${mb(first.bytes)} of ${mb(size)} — ${first.percentOfFile}%`);
    measurement = { host: 'pages', url: origin, phases: livePhases };

    /*
     * And ask the host directly, the way a browser does.
     *
     * This is the check that was missing, and its absence let a broken deploy
     * look healthy. `curl` sends no `Accept-Encoding`; every browser sends one.
     * A host that compresses the response then measures `Range` against the
     * COMPRESSED length, so:
     *
     *   - the tail of the real file is past that length, and the footer read —
     *     which is how DuckDB opens a Parquet at all — comes back 416;
     *   - a range that IS satisfiable returns a slice of the compressed bytes,
     *     which is not the slice of the file that was asked for.
     *
     * `bytes=0-1023` is the one request shape that survives both, which is
     * exactly why it certified a deploy that does not work. So the assertions
     * below are made with a browser's headers, against the real length, at the
     * end of the file.
     */
    const url = `${origin}/${PARQUET}`;
    const browserish = { 'Accept-Encoding': 'gzip, deflate, br, zstd' };

    const browserHead = await fetch(url, { method: 'HEAD', headers: browserish });
    const encoding = browserHead.headers.get('content-encoding');
    check(!encoding,
      'the host serves the Parquet uncompressed to a browser',
      encoding ? `content-encoding: ${encoding} — Range will be measured against the compressed length` : 'no content-encoding');
    check(Number(browserHead.headers.get('content-length')) === size,
      'and reports the real length to a browser',
      `${browserHead.headers.get('content-length')} against ${size}`);

    /* The footer read, as DuckDB issues it: the last bytes of the real file. */
    const tailFrom = size - 16384;
    const tail = await fetch(url, { headers: { ...browserish, Range: `bytes=${tailFrom}-${size - 1}` } });
    const tailRange = tail.headers.get('content-range');
    check(tail.status === 206,
      'a footer Range — how DuckDB opens a Parquet — is satisfiable',
      `status ${tail.status}${tailRange ? `, content-range ${tailRange}` : ''}`);
    check(tailRange === `bytes ${tailFrom}-${size - 1}/${size}`,
      'and is measured against the real length, not a compressed one',
      String(tailRange));

    /* The suffix form, which is the one DuckDB actually opens with. */
    const suffix = await fetch(url, { headers: { ...browserish, Range: 'bytes=-16384' } });
    check(suffix.status === 206, 'the suffix Range form is satisfiable too', `status ${suffix.status}`);
    check(/\/(\d+)$/.test(suffix.headers.get('content-range') || '')
      && Number((suffix.headers.get('content-range') || '').split('/')[1]) === size,
      'and reports the real length as the total',
      String(suffix.headers.get('content-range')));

    const response = await fetch(url, { headers: { Range: 'bytes=0-1023' } });
    const contentRange = response.headers.get('content-range');
    check(response.status === 206, 'the published host answers 206 Partial Content', `status ${response.status}`);
    check(/^bytes 0-1023\//.test(contentRange || ''), 'and the range it returns is the range asked for', String(contentRange));
    const body = await response.arrayBuffer();
    check(body.byteLength === 1024, 'and it sends exactly those bytes', `${body.byteLength} bytes`);
    const head = await fetch(url, { method: 'HEAD' });
    check(head.headers.get('accept-ranges') === 'bytes', 'and advertises accept-ranges: bytes');
    check(Number(head.headers.get('content-length')) === size,
      'the published Parquet file is the committed one',
      `${head.headers.get('content-length')} against ${size}`);
    /* Pages has no /__ranges endpoint, so the page must fall back to the
       recorded measurement rather than showing nothing. */
    if (record) await recordMeasurement(measurement, size);
    check(shown.rangeSource === 'recorded',
      'the live page falls back to the recorded byte measurement', String(shown.rangeSource));
    check(/of the file/.test(shown.rangeText), 'and shows it', shown.rangeText.slice(0, 90));
  } else {
    check(firstPaintFile != null, 'the server saw the Parquet file being read');
    if (firstPaintFile && filteredFile) {
      check(firstPaintFile.partial > 0, 'the first paint used range requests', `${firstPaintFile.partial} of ${firstPaintFile.requests} were 206`);
      check((firstPaintFile.whole ?? 0) === 0, 'and never fell back to a whole-file GET', `${firstPaintFile.whole} whole-body responses`);
      check(firstPaintFile.bytes < size,
        'the first paint did NOT download the file',
        `${mb(firstPaintFile.bytes)} of ${mb(size)} — ${firstPaintFile.percentOfFile}%`);
      check(filteredFile.bytes < size,
        'nor did the filtered query',
        `${mb(filteredFile.bytes)} of ${mb(size)} — ${filteredFile.percentOfFile}%`);
      const phase = (file, label) => ({
        label,
        requests: file.requests,
        partial: file.partial,
        head: file.head ?? 0,
        bytes: file.bytes,
        percentOfFile: file.percentOfFile,
      });
      measurement = {
        host: 'local',
        phases: {
          firstPaint: phase(firstPaintFile, 'the first paint: a page of rows, the count, and the six whole-set queries behind the tiles and the charts'),
          pageOnly: pageFile ? phase(pageFile, 'returning to the unsorted order after a sort — what a page costs when the engine already holds the row groups') : null,
          sorted: sortFile ? phase(sortFile, 're-sorting the whole file by arrival delay and fetching the first page of that order') : null,
          filtered: {
            ...phase(filteredFile, 'one filtered query: arr_delay >= 60, its count, and the whole-set queries again'),
            query: 'arr_delay >= 60',
          },
        },
      };
      const line = (name, file) => `  ${name.padEnd(22)} ${String(file.requests).padStart(4)} requests, ${String(file.partial).padStart(4)} answered 206, `
        + `${String(file.head ?? 0).padStart(2)} HEAD, ${mb(file.bytes).padStart(9)} of ${mb(size)} = ${String(file.percentOfFile).padStart(5)}% of the file`;
      console.log('\nRange reads, measured by the server that served the file:');
      console.log(line('first paint', firstPaintFile));
      if (pageFile) console.log(line('page, already held', pageFile));
      if (sortFile) console.log(line('re-sort whole file', sortFile));
      console.log(line('arr_delay >= 60', filteredFile));
      if (record) await recordMeasurement(measurement, size);
    }
    /* The page's own readout should be the live one when a server is answering. */
    check(shown.rangeSource === 'live', 'the page read the byte accounting from the server', String(shown.rangeSource));
    check(/of the file/.test(shown.rangeText), 'and shows it', shown.rangeText.slice(0, 90));
    check(/^On this host/.test(shown.rangeText.trim().replace(/^What was actually read off the wire/, '')),
      'and says which host it is quoting', shown.rangeText.slice(0, 60));
  }

  return measurement;
}

/* ------------------------------------------------------------------ */

try {
  await run();
} catch (error) {
  failures.push(String(error?.message ?? error));
  notes.push(`  FAIL ${error?.message ?? error}`);
} finally {
  await shutdown();
}

console.log(`\n${live ? 'Live' : 'Local'} checks:`);
for (const note of notes) console.log(note);

if (failures.length) {
  console.error(`\n${failures.length} check${failures.length === 1 ? '' : 's'} failed:`);
  for (const failure of failures) console.error(`  - ${failure}`);
  process.exit(1);
}
console.log(`\nAll ${notes.length} checks passed.`);
process.exit(0);
