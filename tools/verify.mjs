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

const PARQUET = 'data/flights-2026-06.parquet';

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
  const from = `read_parquet('${file}')${filterSql ? ` WHERE ${filterSql}` : ''}`;
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
    '--window-size=1500,1000',
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
  await call('Emulation.setDeviceMetricsOverride', { width: 1500, height: 1000, deviceScaleFactor: 1, mobile: false });

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
  await call('Page.navigate', { url: `${origin}/` });
  await waitFor('!!window.__flightsDemo', 180000, 'the page to report in');
  const state = await evaluate('({ ready: window.__flightsDemo.ready, error: window.__flightsDemo.error || null })');
  if (!state.ready) throw new Error(`the page reported a failure: ${state.error}`);
  await waitFor('window.__flightsDemo.grid && window.__flightsDemo.grid.rows.count() > 0', 90000, 'rows');
  /* The aggregates land a moment after the first page of rows. */
  await waitFor('window.__flightsDemo.headline && window.__flightsDemo.headline.flights != null', 60000, 'the whole-set figures');

  const firstPaintRanges = served ? served.ranges() : null;
  const firstPaintFile = firstPaintRanges?.files?.find((f) => f.path.endsWith('.parquet')) ?? null;

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
   * A sort re-queries the rows and the count and nothing else — the whole-set
   * aggregates only re-run when the filter changes. So this isolates what a
   * page of the table costs, apart from the five whole-set queries behind the
   * tiles and the charts. They are very different numbers and quoting only
   * one of them would be the flattering half of the truth.
   */
  served?.reset();
  await evaluate("window.__flightsDemo.grid.sort.set([{ col: 'arr_delay', dir: 'desc' }])");
  await sleep(4000);
  const pageRanges = served ? served.ranges() : null;
  const pageFile = pageRanges?.files?.find((f) => f.path.endsWith('.parquet')) ?? null;
  const sorted = await evaluate(`(() => {
    const d = window.__flightsDemo;
    const first = [];
    d.grid.rows.forEach((r) => { if (first.length < 1 && r && r.data) first.push(r.data.arr_delay); });
    return { first: first[0], unpushed: d.plan ? d.plan.unpushed : null, sql: (document.querySelector('[data-plan-sql]') || {}).textContent || '' };
  })()`);
  check(sorted.unpushed?.length === 0, 'the sort was pushed too', JSON.stringify(sorted.unpushed));
  check(/ORDER BY/.test(sorted.sql), 'and appears as ORDER BY in the plan panel');

  /* ---------------------------------------------------------------- */
  /* A filtered query: the figures follow, and so do the byte reads    */
  /* ---------------------------------------------------------------- */

  await evaluate("window.__flightsDemo.grid.sort.set([])");
  await sleep(1500);
  served?.reset();
  consoleErrors = [];
  pageErrors = [];
  await evaluate("window.__flightsDemo.togglePreset('late60')");
  await waitFor('window.__flightsDemo.matchCount !== null && window.__flightsDemo.matchCount < window.__flightsDemo.totalRows', 60000, 'the filter to land');
  await sleep(2500);

  const filteredRanges = served ? served.ranges() : null;
  const filteredFile = filteredRanges?.files?.find((f) => f.path.endsWith('.parquet')) ?? null;

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

  await evaluate("window.__flightsDemo.togglePreset('cancelled')");
  await waitFor(`window.__flightsDemo.matchCount !== null && window.__flightsDemo.matchCount < 40000`, 60000, 'a small enough match');
  await sleep(1500);
  const narrowed = await evaluate(`({
    match: window.__flightsDemo.matchCount,
    groupDisabled: !!(document.querySelector('[data-group]') || {}).disabled,
    note: (document.querySelector('[data-group-note]') || {}).textContent || '',
  })`);
  check(narrowed.groupDisabled === false,
    'grouping is offered once the filter has narrowed the match below the limit',
    `${int(narrowed.match)} rows match`);

  await evaluate("window.__flightsDemo.setGrouping('carrier')");
  await waitFor('window.__flightsDemo.grouped === true && window.__flightsDemo.grid.rows.count() > 0', 90000, 'the grouped grid');
  await sleep(1500);
  const groupedState = await evaluate(`({
    grouped: window.__flightsDemo.grouped,
    full: window.__flightsDemo.plan ? window.__flightsDemo.plan.full : null,
    rows: window.__flightsDemo.grid.rows.count(),
    client: [...document.querySelectorAll('[data-client] li')].map((n) => n.textContent),
  })`);
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
    /* GitHub Pages keeps no log this can read, so ask it directly whether it
       serves ranges, which is the fact the page's claim depends on. */
    const url = `${origin}/${PARQUET}`;
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
  } else {
    check(firstPaintFile != null, 'the server saw the Parquet file being read');
    if (firstPaintFile && filteredFile) {
      check(firstPaintFile.partial > 0, 'the first paint used range requests', `${firstPaintFile.partial} of ${firstPaintFile.requests} were 206`);
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
        bytes: file.bytes,
        percentOfFile: file.percentOfFile,
      });
      measurement = {
        file: PARQUET,
        size,
        firstPaint: phase(firstPaintFile, 'the first paint: a page of rows, the count, and the six whole-set queries behind the tiles and the charts'),
        pageOnly: pageFile ? phase(pageFile, 'one more page of rows and its count, after a sort — no whole-set queries') : null,
        filtered: {
          ...phase(filteredFile, 'one filtered query: arr_delay >= 60, its count, and the whole-set queries again'),
          query: 'arr_delay >= 60',
        },
        measuredOn: new Date().toISOString().slice(0, 10),
        note: 'Measured by tools/serve.mjs, which records every request for a file under data/. GitHub Pages answers 206 the same way.',
      };
      const line = (name, file) => `  ${name.padEnd(22)} ${String(file.requests).padStart(4)} requests, ${String(file.partial).padStart(4)} of them 206, `
        + `${mb(file.bytes).padStart(9)} of ${mb(size)} = ${String(file.percentOfFile).padStart(5)}% of the file`;
      console.log('\nRange reads, measured by the server that served the file:');
      console.log(line('first paint', firstPaintFile));
      if (pageFile) console.log(line('one page of rows', pageFile));
      console.log(line('arr_delay >= 60', filteredFile));
      if (record) {
        await writeFile(join(root, 'data', 'range-measurement.json'), `${JSON.stringify(measurement, null, 2)}\n`);
        console.log('  written to data/range-measurement.json');
      }
    }
    /* The page's own readout should be the live one when a server is answering. */
    check(shown.rangeSource === 'live', 'the page read the byte accounting from the server', String(shown.rangeSource));
    check(/of the file/.test(shown.rangeText), 'and shows it', shown.rangeText.slice(0, 90));
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
