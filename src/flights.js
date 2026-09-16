/**
 * What the data is, and how the grid should read it.
 *
 * One month of the US Bureau of Transportation Statistics "Reporting Carrier
 * On-Time Performance" release, converted to a single Parquet file by
 * tools/build-parquet.mjs. Every column here exists in that file; nothing is
 * computed in the page that the engine could compute instead.
 */

/** The file the engine reads. Absolute, because DuckDB resolves it itself, in a worker. */
export const PARQUET = './data/flights-2026-06.parquet';

/** The month on show, for headings. */
export const MONTH_LABEL = 'June 2026';

/**
 * Only these columns are selected. The adapter's default is `SELECT *`, which
 * on a columnar file means reading every column chunk of every row group the
 * query touches — 1.2 MB for two rows, measured. Naming the fields is what
 * keeps a page fetch to the few hundred kilobytes the README quotes.
 *
 * The two aggregation twins (`on_time_n`, `cancelled_n`) are deliberately not
 * here: nothing displays them, and a column nobody reads should not be read.
 */
export const GRID_FIELDS = [
  'flight_id', 'flight_date', 'day_of_week', 'carrier', 'flight_number', 'tail_number',
  'origin', 'origin_city', 'origin_state', 'dest', 'dest_city', 'dest_state', 'route',
  'dep_hour', 'scheduled_dep', 'dep_delay', 'arr_delay', 'taxi_out', 'taxi_in',
  'air_time', 'distance', 'cancelled', 'cancellation_code', 'diverted', 'on_time',
  'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay',
];

/**
 * BTS publishes the cancellation reason as a single letter and defines the
 * four letters in its own documentation for the table. The expansion is theirs,
 * not ours; an unrecognised letter is shown as itself rather than guessed at.
 */
export const CANCELLATION_REASONS = {
  A: 'Carrier',
  B: 'Weather',
  C: 'National Air System',
  D: 'Security',
};

/** BTS numbers the days of the week from Monday. */
export const DAY_NAMES = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

const minutes = { decimals: 0 };

/**
 * The grid's columns.
 *
 * Every filter here is one the DuckDB adapter declares it can apply, so the
 * whole filter tree goes to the engine and `lastPlan().unpushed` stays empty.
 * That is checkable on screen, which is the point of the panel below the grid.
 *
 * `allowGroup: false` throughout is not a limitation of the data, it is the
 * truth about this source: the grid is windowed, so a grouping applied to it
 * would group the 200 rows that happen to be loaded and present the subtotals
 * as if they described the 607,577. The demo offers grouping through its own
 * control instead, over a filtered subset it has actually fetched.
 */
export const COLUMNS = [
  {
    field: 'flight_date', title: 'Date', type: 'date', filter: { type: 'date' },
    layout: { width: 112, pin: 'start' }, allowGroup: false,
  },
  {
    field: 'carrier', title: 'Carrier', filter: { type: 'set' },
    layout: { width: 86 }, allowGroup: false,
  },
  { field: 'flight_number', title: 'Flight', filter: { type: 'text' }, layout: { width: 82 }, allowGroup: false },
  { field: 'tail_number', title: 'Tail', filter: { type: 'text' }, layout: { width: 88, hidden: true }, allowGroup: false },
  { field: 'origin', title: 'From', filter: { type: 'text' }, layout: { width: 74 }, allowGroup: false },
  { field: 'origin_city', title: 'From city', filter: { type: 'text' }, layout: { width: 150, hidden: true }, allowGroup: false },
  { field: 'origin_state', title: 'From state', filter: { type: 'set' }, layout: { width: 96, hidden: true }, allowGroup: false },
  { field: 'dest', title: 'To', filter: { type: 'text' }, layout: { width: 74 }, allowGroup: false },
  { field: 'dest_city', title: 'To city', filter: { type: 'text' }, layout: { width: 150, hidden: true }, allowGroup: false },
  { field: 'dest_state', title: 'To state', filter: { type: 'set' }, layout: { width: 96, hidden: true }, allowGroup: false },
  { field: 'route', title: 'Route', filter: { type: 'text' }, layout: { width: 104 }, allowGroup: false },
  {
    field: 'scheduled_dep', title: 'Scheduled', type: 'datetime', filter: { type: 'date' },
    layout: { width: 150 }, allowGroup: false,
  },
  {
    field: 'dep_delay', title: 'Dep delay', type: 'number', filter: { type: 'number' },
    format: minutes, layout: { width: 104 }, allowGroup: false,
  },
  {
    field: 'arr_delay', title: 'Arr delay', type: 'number', filter: { type: 'number' },
    format: minutes, layout: { width: 104 }, allowGroup: false,
    /* Late is the thing a reader is looking for, so late is the thing that is
       marked. The threshold is BTS's own fifteen minutes. */
    cell: {
      decoration: 'pill',
      variant: { when: [{ op: 'gte', value: 60, use: 'danger' }, { op: 'gte', value: 15, use: 'warning' }], default: 'none' },
    },
  },
  { field: 'taxi_out', title: 'Taxi out', type: 'number', format: minutes, layout: { width: 92, hidden: true }, allowGroup: false },
  { field: 'taxi_in', title: 'Taxi in', type: 'number', format: minutes, layout: { width: 88, hidden: true }, allowGroup: false },
  { field: 'air_time', title: 'Air time', type: 'number', format: minutes, layout: { width: 92, hidden: true }, allowGroup: false },
  {
    field: 'distance', title: 'Miles', type: 'number', filter: { type: 'number' },
    layout: { width: 88 }, allowGroup: false,
  },
  { field: 'cancelled', title: 'Cancelled', type: 'boolean', layout: { width: 96 }, allowGroup: false },
  {
    /* BTS writes the reason as a single letter and defines the four letters in
       its own documentation. A `lookup` column shows the word and keeps the
       letter as the value, so the set filter still pushes down as the letter. */
    field: 'cancellation_code', title: 'Why cancelled', type: 'lookup',
    lookup: { options: Object.entries(CANCELLATION_REASONS).map(([id, label]) => ({ id, label })), unknownLabel: (v) => String(v) },
    filter: { type: 'set' }, layout: { width: 150 }, allowGroup: false,
  },
  { field: 'diverted', title: 'Diverted', type: 'boolean', layout: { width: 90, hidden: true }, allowGroup: false },
  { field: 'carrier_delay', title: 'Carrier mins', type: 'number', format: minutes, layout: { width: 110, hidden: true }, allowGroup: false },
  { field: 'weather_delay', title: 'Weather mins', type: 'number', format: minutes, layout: { width: 118, hidden: true }, allowGroup: false },
  { field: 'nas_delay', title: 'Air-system mins', type: 'number', format: minutes, layout: { width: 132, hidden: true }, allowGroup: false },
  { field: 'security_delay', title: 'Security mins', type: 'number', format: minutes, layout: { width: 120, hidden: true }, allowGroup: false },
  { field: 'late_aircraft_delay', title: 'Late-aircraft mins', type: 'number', format: minutes, layout: { width: 142, hidden: true }, allowGroup: false },
];

/**
 * The aggregates the tiles and the charts are computed from, requested through
 * `source.aggregate()` so the pushdown policy decides where each runs. Every
 * one of these is classified IDENTICAL by the grid's own pushdown map, so with
 * `aggregates: { default: 'engine-if-identical' }` they all run in DuckDB over
 * the whole matching set — never over the page on screen.
 */
export const HEADLINE_AGGREGATES = [
  { id: 'flights', col: 'flight_id', fn: 'count' },
  { id: 'meanArrival', col: 'arr_delay', fn: 'avg' },
  { id: 'p95Arrival', col: 'arr_delay', fn: 'p95' },
  { id: 'onTime', col: 'on_time_n', fn: 'avg' },
  { id: 'cancelled', col: 'cancelled_n', fn: 'avg' },
];

/** Preset filters. Each is an ordinary filter tree, so it takes the same route a header filter does. */
export const PRESETS = [
  {
    id: 'late60',
    label: 'An hour or more late',
    tree: { col: 'arr_delay', op: 'gte', value: 60 },
  },
  {
    id: 'cancelled',
    label: 'Cancelled',
    tree: { col: 'cancelled', op: 'eq', value: true },
  },
  {
    id: 'ord',
    label: 'Through Chicago O’Hare',
    tree: { op: 'or', conditions: [{ col: 'origin', op: 'eq', value: 'ORD' }, { col: 'dest', op: 'eq', value: 'ORD' }] },
  },
  {
    id: 'redeye',
    label: 'Scheduled before 06:00',
    tree: { col: 'dep_hour', op: 'lt', value: 6 },
  },
  {
    id: 'longhaul',
    label: '1,500 miles or more',
    tree: { col: 'distance', op: 'gte', value: 1500 },
  },
  {
    id: 'lastweek',
    /* A window on the DATE column. The adapter binds it through a typed
       placeholder — "flight_date" >= CAST(? AS DATE) — because a prepared
       statement binds an ISO string as VARCHAR, which DuckDB will not compare
       against a DATE. It is also the filter that shows the row groups being
       skipped: the file is sorted by date. */
    label: 'The last week of the month',
    tree: { col: 'flight_date', op: 'gte', value: '2026-06-24T00:00:00.000Z' },
  },
];

/** Formatting used in the tiles, the panels and the chart labels. */
export const fmt = {
  int: (n) => (n == null ? '—' : Number(n).toLocaleString('en-US')),
  minutes: (n) => (n == null ? '—' : `${Number(n) >= 0 ? '' : '−'}${Math.abs(Number(n)).toFixed(1)} min`),
  percent: (n) => (n == null ? '—' : `${(Number(n) * 100).toFixed(1)}%`),
  bytes: (n) => {
    const v = Number(n);
    if (!Number.isFinite(v)) return '—';
    if (v < 1024) return `${v} B`;
    if (v < 1048576) return `${(v / 1024).toFixed(1)} KB`;
    return `${(v / 1048576).toFixed(2)} MB`;
  },
};

/**
 * A DuckDB `count(*)` arrives as a BigInt, because a count can exceed what a
 * JavaScript number represents exactly. Everything downstream — tiles, charts,
 * arithmetic — wants a number, so it is converted in one place.
 */
export const num = (value) => (typeof value === 'bigint' ? Number(value) : (value == null ? null : Number(value)));
