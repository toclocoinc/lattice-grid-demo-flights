/**
 * The dashboard.
 *
 * One idea runs through the whole file: the browser holds a page of rows, and
 * every figure on screen is computed by DuckDB over the whole matching set.
 *
 *   the table        a window. 200 rows, fetched per interaction, with the
 *                    match count riding along in the same statement.
 *   the tiles        `source.aggregate()` with no groupBy — five statistics
 *                    over everything the filter matches, computed in SQL.
 *   the charts       `source.aggregate()` with a groupBy — one `GROUP BY
 *                    ROLLUP` each, so a bar, a box, a tile of the treemap and
 *                    a point of the control chart are all whole-set figures.
 *   the push plan    what DuckDB actually ran, with its bound values, beside
 *                    `source.lastPlan()`'s account of what stayed here.
 *
 * Nothing on screen is reduced from the loaded page. That is the difference
 * between this demo and a grid with a pre-built aggregation cube behind it:
 * there is no cube, there is a query.
 *
 * The one thing that genuinely cannot work that way is grouping, and it is not
 * pretended otherwise. See `mountGrid` and the guard around the group control.
 */

import {
  COLUMNS, GRID_FIELDS, HEADLINE_AGGREGATES, MONTH_LABEL, PARQUET, PRESETS,
  fmt, num,
} from './flights.js';
import { readRangeAccounting } from './duckdb.js';

/**
 * Grouping needs the whole matching set in the browser, so it is gated on the
 * filter having narrowed the data to something a browser should hold. Past
 * this, the control is disabled and says why rather than grouping a page and
 * calling the subtotals whole.
 */
export const GROUP_LIMIT = 40000;

/** The distribution chart's bucket width, in minutes. */
const BUCKET = 5;
/** Delays outside this are gathered into the end buckets, which are labelled as such. */
const BUCKET_MIN = -60;
const BUCKET_MAX = 180;
/** How many routes the treemap shows. */
const TOP_ROUTES = 40;

const el = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
};

/**
 * Build the whole page.
 *
 * Every factory is handed in rather than imported here, so this file names no
 * module path and the entry point decides what is loaded.
 */
export function buildDashboard({
  root, LG, createChart, createKPI, connection, recordSql, sqlLog, duckdbVersion,
}) {
  const FILE = new URL(PARQUET, location.href).href;
  /* The data file's own name, not its extension: the extension is a hosting
     decision that changes, and matching on it is how a rename quietly stops the
     byte readout finding its own file. */
  const DATA_FILE = PARQUET.split('/').pop();

  const built = {
    ready: false,
    error: null,
    grid: null,
    source: null,
    adapter: null,
    kpi: null,
    charts: {},
    chartGrids: {},
    headline: {},
    grouped: false,
    groupBy: null,
    matchCount: null,
    totalRows: null,
    plan: null,
    lastAggregateMs: null,
    ranges: null,
    rangeSource: null,
    presets: new Set(),
    duckdbVersion,
  };

  /* ------------------------------------------------------------------ */
  /* The frame                                                           */
  /* ------------------------------------------------------------------ */

  root.textContent = '';
  root.className = 'app';

  const header = el('header', 'masthead');
  header.append(el('h1', null, `Every US flight in ${MONTH_LABEL}, queried in your browser`));
  const lede = el('p', 'lede');
  lede.textContent =
    'One Parquet file on a static host. DuckDB compiled to WebAssembly reads it in byte ranges, '
    + 'Lattice Grid turns every filter, sort and page into SQL, and every figure below is computed '
    + 'over everything that matches — not over the rows on screen.';
  header.append(lede);
  const factLine = el('p', 'facts');
  header.append(factLine);
  root.append(header);

  const kpiHost = el('section', 'kpi-strip');
  kpiHost.setAttribute('aria-label', 'Headline figures over the whole matching set');
  root.append(kpiHost);

  const controls = el('section', 'controls');
  controls.setAttribute('aria-label', 'Filters');
  root.append(controls);

  const gridHost = el('section', 'grid-host');
  gridHost.setAttribute('aria-label', 'Flights');
  root.append(gridHost);

  const planSection = el('section', 'plan');
  planSection.setAttribute('aria-label', 'The push plan');
  root.append(planSection);

  const chartSection = el('section', 'charts');
  chartSection.setAttribute('aria-label', 'Charts');
  root.append(chartSection);

  const footer = el('footer', 'colophon');
  root.append(footer);

  /* ------------------------------------------------------------------ */
  /* The filter chips                                                    */
  /* ------------------------------------------------------------------ */

  const chipRow = el('div', 'chips');
  chipRow.append(el('span', 'chips-label', 'Push down:'));
  const chipButtons = new Map();
  for (const preset of PRESETS) {
    const button = el('button', 'chip', preset.label);
    button.type = 'button';
    button.dataset.preset = preset.id;
    button.setAttribute('aria-pressed', 'false');
    button.addEventListener('click', () => {
      if (built.presets.has(preset.id)) built.presets.delete(preset.id);
      else built.presets.add(preset.id);
      applyPresets();
    });
    chipButtons.set(preset.id, button);
    chipRow.append(button);
  }
  const clearButton = el('button', 'chip-clear', 'Clear');
  clearButton.type = 'button';
  clearButton.dataset.clear = '';
  clearButton.addEventListener('click', () => { built.presets.clear(); applyPresets(); });
  chipRow.append(clearButton);
  controls.append(chipRow);

  const groupRow = el('div', 'group-row');
  const groupLabel = el('span', 'chips-label', 'Group, client-side:');
  const groupSelect = el('select', 'group-select');
  groupSelect.dataset.group = '';
  for (const option of [
    { value: '', label: 'No grouping' },
    { value: 'carrier', label: 'Carrier' },
    { value: 'origin_state', label: 'Departure state' },
    { value: 'route', label: 'Route' },
  ]) {
    const node = el('option', null, option.label);
    node.value = option.value;
    groupSelect.append(node);
  }
  const groupNote = el('span', 'group-note');
  groupNote.dataset.groupNote = '';
  groupSelect.addEventListener('change', () => { void setGrouping(groupSelect.value || null); });
  groupRow.append(groupLabel, groupSelect, groupNote);
  controls.append(groupRow);

  /**
   * Turn the chips into one filter tree and hand it to the grid.
   *
   * A grouped grid is holding the whole matching set under a ceiling, so a
   * filter that widens the match would take it past that ceiling and the source
   * would refuse — correctly, and with an error where a table should be. Rather
   * than let that happen, changing the filter drops back to the windowed grid
   * first; the group control then re-offers itself if the new match is small
   * enough. Ungrouping is a rebuild, so the filter is applied by the rebuild.
   */
  function applyPresets() {
    const conditions = PRESETS.filter((p) => built.presets.has(p.id)).map((p) => p.tree);
    const tree = conditions.length === 0 ? null : conditions.length === 1 ? conditions[0] : { op: 'and', conditions };
    for (const [id, button] of chipButtons) {
      const on = built.presets.has(id);
      button.classList.toggle('on', on);
      button.setAttribute('aria-pressed', String(on));
    }
    if (built.grouped) {
      mountGrid({ full: false, groupBy: null, filters: tree, sort: built.grid?.sort.get() ?? [] });
      groupSelect.value = '';
      renderGroupNote();
      return;
    }
    built.grid?.filters.set(tree);
  }

  /* ------------------------------------------------------------------ */
  /* The tiles                                                           */
  /* ------------------------------------------------------------------ */

  /*
   * The panel is fed one row: the answer DuckDB gave for the current filter.
   *
   * It is deliberately NOT bound to the grid. A grid-bound panel reduces over
   * the rows the grid holds, and this grid holds 200 of 607,577 — so an
   * average would be the average of a page, which is wrong and looks right.
   * The grid drives it all the same: every filter change re-runs the query and
   * calls setRows with the new answer.
   */
  const kpi = createKPI(kpiHost, {
    rows: [],
    rowKey: 'id',
    columns: 5,
    ariaLabel: 'Headline figures over the whole matching set',
    nullText: '—',
    tiles: [
      { id: 'flights', label: 'Flights matching', aggregation: 'max', field: 'flights', format: { type: 'number', decimals: 0 } },
      {
        id: 'onTime', label: 'Arrived on time', aggregation: 'max', field: 'onTime',
        format: { type: 'percent', decimals: 1 },
        thresholds: { warn: 0.8, critical: 0.7, direction: 'higherIsBetter' },
      },
      { id: 'meanArrival', label: 'Mean arrival delay, minutes', aggregation: 'max', field: 'meanArrival', format: { type: 'number', decimals: 1 } },
      { id: 'p95Arrival', label: '95th percentile arrival delay, minutes', aggregation: 'max', field: 'p95Arrival', format: { type: 'number', decimals: 0 } },
      {
        id: 'cancelled', label: 'Cancelled', aggregation: 'max', field: 'cancelled',
        format: { type: 'percent', decimals: 2 },
        thresholds: { warn: 0.02, critical: 0.04, direction: 'lowerIsBetter' },
      },
    ],
  });
  built.kpi = kpi;

  const tileNote = el('p', 'tile-note');
  tileNote.dataset.tileNote = '';
  tileNote.textContent =
    'Computed by DuckDB over every matching row, in one statement — not reduced from the page on screen. '
    + 'The two delay figures are in minutes, signed, so an early arrival pulls the mean down.';
  kpiHost.append(tileNote);

  /* ------------------------------------------------------------------ */
  /* The push-plan panel                                                 */
  /* ------------------------------------------------------------------ */

  const planHead = el('div', 'plan-head');
  planHead.append(el('h2', null, 'What went to DuckDB'));
  const planSummary = el('p', 'plan-summary');
  planSummary.dataset.planSummary = '';
  planHead.append(planSummary);
  const planPre = el('pre', 'plan-sql');
  planPre.dataset.planSql = '';
  planPre.textContent = '-- the statements DuckDB ran appear here';
  const planSplit = el('div', 'plan-split');
  const pushedList = el('div', 'plan-col');
  pushedList.append(el('h3', null, 'Pushed to the engine'));
  const pushedItems = el('ul', 'plan-list');
  pushedItems.dataset.pushed = '';
  pushedList.append(pushedItems);
  const clientList = el('div', 'plan-col');
  clientList.append(el('h3', null, 'Stayed in the browser'));
  const clientItems = el('ul', 'plan-list');
  clientItems.dataset.client = '';
  clientList.append(clientItems);
  planSplit.append(pushedList, clientList);
  planSection.append(planHead, planSplit, planPre);

  const rangePanel = el('div', 'range-panel');
  rangePanel.dataset.range = '';
  planSection.append(rangePanel);

  /* ------------------------------------------------------------------ */
  /* The charts                                                          */
  /* ------------------------------------------------------------------ */

  const CHART_SPECS = [
    { id: 'dist', title: `Arrival delay, every matching flight`, wide: true },
    { id: 'carrier', title: 'Median arrival delay by carrier' },
    { id: 'hour', title: 'Mean arrival delay by scheduled departure hour' },
    { id: 'routes', title: `The ${TOP_ROUTES} busiest routes`, wide: true },
    { id: 'daily', title: 'Daily mean arrival delay', wide: true },
  ];
  const chartBoxes = {};
  for (const spec of CHART_SPECS) {
    const box = el('div', `chart-box${spec.wide ? ' wide' : ''}`);
    box.dataset.chart = spec.id;
    chartSection.append(box);
    chartBoxes[spec.id] = box;
  }

  /* ------------------------------------------------------------------ */
  /* The source, and the grid over it                                    */
  /* ------------------------------------------------------------------ */

  /**
   * A source over the Parquet file.
   *
   * `full` swaps the windowed source for one that holds the whole matching set
   * client-side. That is the only honest way to group here, and it is why the
   * group control is gated: `fullDataset` is a design-time decision, fixed for
   * the life of a grid, so switching it means building a new grid — which is
   * exactly what `setGrouping` does.
   */
  function makeSource(full) {
    const adapter = LG.duckdbAdapter({
      connection: recordSql(connection),
      from: `read_parquet('${FILE}')`,
      fields: GRID_FIELDS,
    });
    const source = LG.createPushdownSource({
      adapter,
      compute: LG,
      pageSize: 200,
      /* Push every statistic whose engine result is verified identical to the
         grid's own kernel. All five headline figures qualify. */
      aggregates: { default: 'engine-if-identical' },
      ...(full ? { fullDataset: { enabled: true, maxRows: GROUP_LIMIT } } : {}),
    });
    return { adapter, source };
  }

  /** Build (or rebuild) the grid, carrying the filter and sort across. */
  function mountGrid({ full, groupBy, filters, sort }) {
    built.grid?.destroy?.();
    gridHost.textContent = '';
    const { adapter, source } = makeSource(full);
    built.adapter = adapter;
    built.source = source;
    built.grouped = Boolean(groupBy);
    built.groupBy = groupBy ?? null;

    const columns = COLUMNS.map((column) => (
      groupBy && column.field === groupBy
        ? { ...column, allowGroup: true, group: { enabled: true, index: 0 } }
        : column
    ));

    const grid = LG.createGrid(gridHost, {
      rowKey: 'flight_id',
      theme: 'light',
      columns,
      source,
      toolPanel: { side: 'right', panels: ['filters', 'columns'] },
      showTotalInHeader: true,
      ...(groupBy ? { groupFooter: true, grandTotalRow: 'bottom' } : {}),
    });
    built.grid = grid;

    if (filters) grid.filters.set(filters);
    if (sort?.length) grid.sort.set(sort);

    /*
     * The rows query is where the match count comes from: the adapter asks for
     * it in the same dispatch as the page. Watching the adapter's own execute
     * is how the page learns a filter changed without guessing at an event.
     */
    const inner = adapter.execute.bind(adapter);
    let lastSignature = '';
    adapter.execute = async (query, request) => {
      const result = await inner(query, request);
      built.matchCount = typeof result.total === 'number' ? result.total : null;
      if (built.totalRows === null && !query.filters) built.totalRows = built.matchCount;
      built.plan = source.lastPlan?.() ?? null;
      const signature = JSON.stringify(query.filters ?? null);
      if (signature !== lastSignature) {
        lastSignature = signature;
        void refresh(query.filters ?? null);
      }
      renderPlan();
      renderFacts();
      return result;
    };

    return grid;
  }

  /**
   * Switch grouping on or off, under the guard.
   *
   * A grouped grid here is a different grid: it holds the whole matching set,
   * because a subtotal over a fetched window describes the window. The control
   * refuses rather than misleads, and says what would unlock it.
   */
  async function setGrouping(groupBy) {
    const matching = built.matchCount;
    if (groupBy && (matching == null || matching > GROUP_LIMIT)) {
      groupSelect.value = built.groupBy ?? '';
      renderGroupNote();
      return;
    }
    const filters = built.grid?.filters.get() ?? null;
    const sort = built.grid?.sort.get() ?? [];
    mountGrid({ full: Boolean(groupBy), groupBy, filters, sort });
    groupSelect.value = groupBy ?? '';
    renderGroupNote();
  }

  function renderGroupNote() {
    const matching = built.matchCount;
    const overLimit = matching == null || matching > GROUP_LIMIT;
    groupSelect.disabled = overLimit && !built.grouped;
    groupNote.textContent = overLimit
      ? `Filter below ${fmt.int(GROUP_LIMIT)} rows to group. ${fmt.int(matching)} match; grouping a fetched window would describe the window, not the match.`
      : `${fmt.int(matching)} rows match, so the whole matching set can be held here and the subtotals are its own.`
        + (built.grouped ? ' Changing the filter returns to the windowed grid.' : '');
    groupNote.classList.toggle('blocked', overLimit);
  }

  /* ------------------------------------------------------------------ */
  /* The whole-set queries behind the tiles and the charts               */
  /* ------------------------------------------------------------------ */

  let refreshSequence = 0;

  /** The request shape `source.aggregate` takes. */
  const request = (filters, groupBy) => ({ filters, sort: [], range: null, groupBy });

  /** Only real groups; the ROLLUP grand total carries grouping[0] === 1. */
  const realGroups = (groups) => (groups ?? []).filter((g) => !g.grouping?.[0]);

  /**
   * Re-run everything the filter changes: five tiles and five charts, each one
   * `GROUP BY ROLLUP` in DuckDB over the whole matching set.
   */
  async function refresh(filters) {
    const sequence = ++refreshSequence;
    const source = built.source;
    if (!source) return;
    const started = performance.now();
    try {
      const [headline, byDelay, byCarrier, byHour, byRoute, byDay] = await Promise.all([
        source.aggregate(request(filters, []), HEADLINE_AGGREGATES),
        source.aggregate(request(filters, ['arr_delay']), [{ id: 'flights', col: 'flight_id', fn: 'count' }]),
        source.aggregate(request(filters, ['carrier']), [
          { id: 'flights', col: 'flight_id', fn: 'count' },
          { id: 'median', col: 'arr_delay', fn: 'median' },
          { id: 'p95', col: 'arr_delay', fn: 'p95' },
        ]),
        source.aggregate(request(filters, ['dep_hour']), [
          { id: 'flights', col: 'flight_id', fn: 'count' },
          { id: 'mean', col: 'arr_delay', fn: 'avg' },
        ]),
        source.aggregate(request(filters, ['route']), [{ id: 'flights', col: 'flight_id', fn: 'count' }]),
        source.aggregate(request(filters, ['flight_date']), [{ id: 'mean', col: 'arr_delay', fn: 'avg' }]),
      ]);
      if (sequence !== refreshSequence) return;
      built.lastAggregateMs = Math.round(performance.now() - started);

      const values = headline.values ?? {};
      built.headline = {
        flights: num(values.flights),
        onTime: num(values.onTime),
        meanArrival: num(values.meanArrival),
        p95Arrival: num(values.p95Arrival),
        cancelled: num(values.cancelled),
      };
      built.aggregateProvenance = {
        engine: (headline.engine ?? []).map((a) => `${a.fn}(${a.col})`),
        client: (headline.client ?? []).map((a) => `${a.fn}(${a.col})`),
      };
      kpi.setRows([{ id: 'now', ...built.headline }]);

      feedDistribution(byDelay.groups);
      feedCarriers(byCarrier.groups);
      feedHours(byHour.groups);
      feedRoutes(byRoute.groups);
      feedDaily(byDay.groups);
      renderPlan();
      renderGroupNote();
    } catch (error) {
      console.error('[flights demo] aggregate', error);
      built.error = String(error?.message ?? error);
    }
  }

  /* ---------------- chart data, all of it engine-computed ------------- */

  /** A headless grid per chart: the chart is an ordinary viewer of ordinary rows. */
  function feed(id, columns, rows, spec) {
    if (built.chartGrids[id]) {
      /* `load` replaces the whole set, which is what a new answer to a new
         filter is. The chart is a viewer of the grid and follows on its own. */
      built.chartGrids[id].rows.load(rows);
      built.charts[id]?.refresh?.();
      return;
    }
    const grid = LG.createHeadlessGrid({ rowKey: 'id', columns, rows });
    built.chartGrids[id] = grid;
    try {
      built.charts[id] = createChart({ grid, container: chartBoxes[id], legend: false, ...spec });
    } catch (error) {
      chartBoxes[id].append(el('p', 'chart-error', `This chart could not be drawn: ${error.message}`));
      console.error('[flights demo] chart', id, error);
    }
  }

  /**
   * The delay distribution.
   *
   * DuckDB groups by `arr_delay` itself — the column is a whole number of
   * minutes, so one GROUP BY gives the exact distribution of the whole
   * matching set in about 1,200 rows. Those exact counts are gathered into
   * five-minute buckets here.
   *
   * It is drawn as a bar chart of counts rather than with the charts module's
   * `histogram` type, and that is a deliberate choice, not a shortcut: a
   * histogram bins raw readings, so it would need every matching arrival delay
   * in the browser — the download this whole demo exists to avoid — and at this
   * size the type currently throws (see the README, F-FLT-1). What is drawn is
   * the exact distribution of every matching flight, which is the stronger
   * claim anyway.
   */
  function feedDistribution(groups) {
    const buckets = new Map();
    for (const group of realGroups(groups)) {
      const delay = num(group.keys[0]);
      if (delay == null) continue;
      const clamped = Math.min(BUCKET_MAX, Math.max(BUCKET_MIN, delay));
      const floor = Math.floor(clamped / BUCKET) * BUCKET;
      buckets.set(floor, (buckets.get(floor) ?? 0) + num(group.values.flights));
    }
    const rows = [...buckets.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([floor, flights]) => ({
        id: floor,
        bucket: floor <= BUCKET_MIN ? `≤ ${BUCKET_MIN}` : floor >= BUCKET_MAX ? `${BUCKET_MAX}+` : String(floor),
        floor,
        flights,
      }));
    feed('dist',
      [{ field: 'id' }, { field: 'bucket' }, { field: 'floor', type: 'number' }, { field: 'flights', type: 'number' }],
      rows,
      {
        type: 'bar', x: 'bucket', y: 'flights',
        title: `Arrival delay, minutes, in ${BUCKET}-minute buckets`,
        subtitle: 'Exact counts over every matching flight, computed by DuckDB',
        axis: { x: { labels: true, every: 4 }, y: 'Flights' },
        annotations: [{ kind: 'line', orient: 'vertical', x: '15', label: 'BTS "late": 15 min', colour: '#c22b2b' }],
      });
  }

  /**
   * Arrival delay by carrier.
   *
   * The five-number summary is DuckDB's, per carrier, over the whole matching
   * set. It is drawn as a bar of the median rather than as a box plot: the
   * charts module's `boxplot` scales its measure axis to the per-category
   * *sum* in 1.62.1, which puts every box off the top of the plot (see the README,
   * F-FLT-2, the same defect as the grid's own F-1329-1). A chart that cannot
   * be drawn correctly is not drawn incorrectly here.
   */
  function feedCarriers(groups) {
    const rows = realGroups(groups)
      .map((group) => ({
        id: String(group.keys[0]),
        carrier: String(group.keys[0]),
        flights: num(group.values.flights),
        median: num(group.values.median),
        p95: num(group.values.p95),
      }))
      .filter((row) => row.median != null)
      .sort((a, b) => b.median - a.median);
    feed('carrier',
      [{ field: 'id' }, { field: 'carrier' }, { field: 'flights', type: 'number' }, { field: 'median', type: 'number' }, { field: 'p95', type: 'number' }],
      rows,
      {
        type: 'horizontalBar', x: 'carrier', y: 'median',
        title: 'Median arrival delay by carrier, minutes',
        subtitle: 'The median of every matching flight, computed by DuckDB',
        axis: { x: 'Minutes' },
        margin: { left: 52 },
      });
  }

  /** Mean arrival delay by the hour the flight was scheduled to leave. */
  function feedHours(groups) {
    const rows = realGroups(groups)
      .map((group) => ({
        id: num(group.keys[0]),
        hour: `${String(num(group.keys[0])).padStart(2, '0')}:00`,
        flights: num(group.values.flights),
        mean: num(group.values.mean),
      }))
      /* A group whose measure is null has nothing to plot. Dropping it here is
         what lets a filter with no measurable delays — cancelled flights, say —
         leave a chart genuinely empty rather than holding its last picture. */
      .filter((row) => row.id != null && row.mean != null)
      .sort((a, b) => a.id - b.id);
    feed('hour',
      [{ field: 'id', type: 'number' }, { field: 'hour' }, { field: 'flights', type: 'number' }, { field: 'mean', type: 'number' }],
      rows,
      {
        type: 'bar', x: 'hour', y: 'mean',
        title: 'Mean arrival delay by scheduled departure hour',
        subtitle: 'Local clock time at the departure airport, as BTS records it',
        axis: { x: { labels: true, every: 2 }, y: 'Minutes' },
      });
  }

  /** The busiest routes, as a treemap of flight counts. */
  function feedRoutes(groups) {
    const rows = realGroups(groups)
      .map((group) => ({ id: String(group.keys[0]), route: String(group.keys[0]), flights: num(group.values.flights) }))
      .sort((a, b) => b.flights - a.flights)
      .slice(0, TOP_ROUTES);
    feed('routes',
      [{ field: 'id' }, { field: 'route' }, { field: 'flights', type: 'number' }],
      rows,
      {
        type: 'treemap', x: 'route', y: 'flights',
        title: `The ${TOP_ROUTES} busiest routes among the matching flights`,
        subtitle: 'Area is the flight count, counted by DuckDB over the whole match',
      });
  }

  /** Daily mean arrival delay, as a control chart. */
  function feedDaily(groups) {
    const rows = realGroups(groups)
      .map((group) => {
        const stamp = num(group.keys[0]);
        const date = stamp == null ? null : new Date(stamp);
        return {
          id: stamp,
          day: date ? date.toISOString().slice(0, 10) : '',
          label: date ? String(date.getUTCDate()) : '',
          mean: num(group.values.mean),
        };
      })
      .filter((row) => row.id != null && row.mean != null)
      .sort((a, b) => a.id - b.id);
    feed('daily',
      [{ field: 'id', type: 'number' }, { field: 'day' }, { field: 'label' }, { field: 'mean', type: 'number' }],
      rows,
      {
        type: 'control', x: 'label', y: 'mean', rules: 'nelson', baseline: 10,
        title: `Daily mean arrival delay, ${MONTH_LABEL}`,
        subtitle: 'One point per day, each the mean of every matching flight that day. '
          + 'Control limits fixed on the first ten days, then held, so a later day is judged against the month\'s own early behaviour.',
        axis: { x: { labels: true, every: 2 }, y: 'Minutes' },
      });
  }

  /* ------------------------------------------------------------------ */
  /* Rendering the plan, the facts and the range accounting              */
  /* ------------------------------------------------------------------ */

  /** One statement per kind, so the panel shows the latest of each rather than a scroll. */
  function latestStatements() {
    const kinds = new Map();
    for (const entry of sqlLog) {
      const kind = entry.sql.startsWith('DESCRIBE') ? 'describe'
        : entry.sql.includes('__lattice_total') ? 'count'
          : entry.sql.includes('GROUP BY ROLLUP') ? 'aggregate'
            : /\bLIMIT\b|\bOFFSET\b/.test(entry.sql) ? 'rows'
              : 'summary';
      kinds.set(kind, entry);
    }
    return kinds;
  }

  function renderPlan() {
    const plan = built.plan;
    const statements = latestStatements();

    const block = (title, entry) => {
      if (!entry) return '';
      const sql = entry.sql.replace(/ (FROM|WHERE|GROUP BY|ORDER BY|LIMIT|OFFSET) /g, '\n$1 ');
      const bound = entry.params?.length ? `\n-- bound: ${JSON.stringify(entry.params)}` : '';
      return `-- ${title} · ${entry.ms} ms · ${fmt.int(entry.rows)} rows back\n${sql}${bound}\n\n`;
    };
    planPre.textContent =
      block('the page on screen', statements.get('rows'))
      + block('the match count, dispatched with it', statements.get('count'))
      + block('the five tiles, over everything that matches', statements.get('summary'))
      + block('one of the charts, grouped over everything that matches', statements.get('aggregate'))
      + block('the column types, read once at startup', statements.get('describe'));

    pushedItems.textContent = '';
    clientItems.textContent = '';
    const pushed = [];
    const client = [];

    if (plan) {
      const unpushed = plan.unpushed ?? [];
      pushed.push(unpushed.includes('filter') ? 'nothing of the filter' : 'the whole filter tree, as a bound WHERE');
      pushed.push(unpushed.includes('sort') ? 'nothing of the sort' : 'the sort, as ORDER BY');
      pushed.push(plan.needsAll ? 'no window: the whole matching set was fetched' : 'the window, as LIMIT and OFFSET');
      pushed.push('the match count, as count(*) in the same dispatch');
      for (const part of unpushed) client.push(`the ${part}, applied here after the fetch`);
      if (plan.full) client.push('the whole matching set is held here, so grouping and its subtotals are the browser’s');
    }
    const provenance = built.aggregateProvenance;
    if (provenance?.engine?.length) pushed.push(`the statistics: ${provenance.engine.join(', ')}`);
    if (provenance?.client?.length) client.push(`the statistics: ${provenance.client.join(', ')}`);
    /* The honest remainder. Every one of these is arithmetic on a few thousand
       numbers the engine already reduced, not work over the 607,577 rows. */
    client.push(`gathering DuckDB's exact per-minute counts into ${BUCKET}-minute buckets`);
    client.push(`picking the busiest ${TOP_ROUTES} from the route counts the engine returned`);
    if (built.grouped) client.push(`grouping by ${built.groupBy}, over the matching set held here`);
    client.push('rendering, scrolling and the filter UI');

    for (const item of pushed) pushedItems.append(el('li', null, item));
    for (const item of client) clientItems.append(el('li', null, item));

    const rowsStatement = statements.get('rows');
    planSummary.textContent =
      `${fmt.int(built.matchCount)} rows match. The page came back in ${rowsStatement ? `${rowsStatement.ms} ms` : '—'}`
      + `, the whole-set statistics in ${built.lastAggregateMs == null ? '—' : `${built.lastAggregateMs} ms`}.`;
  }

  function renderFacts() {
    factLine.textContent =
      `${fmt.int(built.totalRows)} flights · one ${fmt.bytes(built.parquetBytes ?? 0)} Parquet file · `
      + `DuckDB ${built.duckdbVersion} in WebAssembly · Lattice Grid over the DuckDB adapter`;
  }

  /**
   * The range accounting.
   *
   * Live from the local server when there is one; otherwise the measurement
   * taken here and committed with the repository, clearly labelled as recorded
   * rather than live. GitHub Pages answers 206 — the reads happen there too —
   * it just keeps no log this page could read.
   */
  async function renderRanges(recorded) {
    const live = await readRangeAccounting();
    built.ranges = live;
    built.rangeSource = live ? 'live' : 'recorded';
    const file = live?.files?.find((f) => f.path.endsWith(DATA_FILE));
    rangePanel.textContent = '';
    rangePanel.append(el('h3', null, 'What was actually read off the wire'));
    const body = el('p', 'range-body');
    if (file) {
      body.innerHTML =
        `<b>${fmt.int(file.requests)} requests</b>, ${fmt.int(file.partial)} of them answered 206 Partial Content, `
        + `<b>${fmt.bytes(file.bytes)} of ${fmt.bytes(file.size)}</b> — ${file.percentOfFile}% of the file. `
        + 'Counted by the server that served it, because DuckDB reads inside a worker and nothing in this page can see those requests.';
      rangePanel.dataset.rangeSource = 'live';
    } else if (recorded) {
      const phase = (p) => `<b>${fmt.int(p.requests)} requests, ${fmt.bytes(p.bytes)}</b> — ${p.percentOfFile}% of the file`;
      body.innerHTML =
        `Recorded on a local server running this same code, against this same file (${fmt.bytes(recorded.size)}). `
        + `The first paint — a page of rows, the count, and the six whole-set queries behind the tiles and charts — took `
        + `${phase(recorded.firstPaint)}. `
        + (recorded.pageOnly ? `One more page of rows, unsorted, took ${phase(recorded.pageOnly)}. ` : '')
        + (recorded.sorted ? `Re-sorting the whole file by arrival delay took ${phase(recorded.sorted)} — an ORDER BY over 607,577 rows has to read that column out of every row group. ` : '')
        + `One filtered query (${recorded.filtered.query}) took ${phase(recorded.filtered)}. `
        + 'GitHub Pages answers 206 too, so the same reads happen here; it just keeps no log this page can read.';
      rangePanel.dataset.rangeSource = 'recorded';
    } else {
      body.textContent = 'No measurement available.';
      rangePanel.dataset.rangeSource = 'none';
    }
    rangePanel.append(body);
  }

  /* ------------------------------------------------------------------ */
  /* Start                                                               */
  /* ------------------------------------------------------------------ */

  mountGrid({ full: false, groupBy: null, filters: null, sort: [] });
  applyPresets();
  renderGroupNote();

  footer.innerHTML =
    'Source: <a href="https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ">US Bureau of Transportation Statistics</a>, '
    + 'Reporting Carrier On-Time Performance, June 2026 — a work of the US government, in the public domain. '
    + 'Built with <a href="https://latticegrid.dev">Lattice Grid</a> and '
    + '<a href="https://duckdb.org/docs/api/wasm/overview.html">DuckDB-Wasm</a>.'
    + '<br><span class="why-zip">Why the data file is called <code>.zip</code>: GitHub Pages gzips binary files on the fly '
    + 'and evaluates byte ranges against the compressed length, which breaks DuckDB\u2019s footer read. It leaves archive '
    + 'types alone, so the file is published under a <code>.zip</code> name — it is a plain zstd Parquet file, not an '
    + 'archive. The same bytes are downloadable as <code>flights-2026-06.parquet</code> from this repository\u2019s '
    + '<code>data-2026-06</code> release.</span>';

  built.setParquetSize = (bytes) => { built.parquetBytes = bytes; renderFacts(); };
  built.renderRanges = renderRanges;
  built.setGrouping = setGrouping;
  built.applyPresets = applyPresets;
  built.togglePreset = (id) => {
    if (built.presets.has(id)) built.presets.delete(id); else built.presets.add(id);
    applyPresets();
  };
  built.destroy = () => {
    for (const chart of Object.values(built.charts)) chart?.destroy?.();
    kpi.destroy();
    built.grid?.destroy?.();
  };

  return built;
}
