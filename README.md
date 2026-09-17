# US flight delays, June 2026 — Lattice Grid over DuckDB-Wasm

**Live: https://toclocoinc.github.io/lattice-grid-demo-flights/**

607,577 flights. One 14.67 MB Parquet file on a static host (published under a
`.zip` name — [why](#why-the-data-file-is-called-zip)). No server, no API,
no pre-built aggregation cube — a query engine in the tab, and a grid that
shows you the SQL it sent.

---

## What this demo proves

Three claims, each of them checkable on the page itself.

### 1. The file is read in byte ranges, not downloaded

DuckDB compiled to WebAssembly opens the Parquet file by its footer, reads the
metadata, and then reads only the row groups and column chunks a query actually
needs — over HTTP, with `Range` headers. `LOAD httpfs;` is what puts that
filesystem in play; without it DuckDB pulls the whole file in one GET before it
can answer anything.

Measured **on a local server**, by the server, because DuckDB reads inside a worker and nothing in the
page can see those requests:

| | HTTP requests | Bytes read | Share of the 14.67 MB file |
|---|---|---|---|
| Returning to the unsorted order after a sort — nothing new to read | 2 (0 answered 206) | **0 bytes** | **0%** |
| The first paint: that page, plus the six whole-set queries behind the tiles and the charts | 177 (168 answered 206) | 6.18 MB | **42.1%** |
| One filtered query (`arr_delay >= 60`) and everything it recomputes | 176 (168 answered 206) | 5.92 MB | **40.4%** |
| Re-sorting the whole file by arrival delay and fetching the first page of that order | 364 (362 answered 206) | 13.02 MB | **88.7%** |

Not one whole-file `GET` in any of them — the check asserts that, not merely that some requests were 206.

**On GitHub Pages the same first paint reads less: 99 requests, 3.03 MB, 20.7 % of the file, every one
answered 206.** Not a different amount of work — a different cache. Pages sends `cache-control:
max-age=600` and the local server sends `no-store`, so on Pages the browser answers DuckDB's repeat
reads of the same byte ranges without going back to the wire; later queries in the same session can
need nothing new at all. Both measurements are kept in
[`data/range-measurement.json`](data/range-measurement.json), each labelled with its host, and the
published page quotes the one taken on the host it is being served from.

Measured 2026-09-16 by `tools/serve.mjs`, which records every request for a file under `data/`, and written to
[`data/range-measurement.json`](data/range-measurement.json) by `node tools/verify.mjs --record`. The published page
falls back to it and says so.

All four numbers are quoted because they are wildly different, and the cheap one alone would be the
flattering quarter of the truth.

The zero is real and worth reading carefully: it is not what a page of rows costs, it is what a page
costs when the engine already holds the row groups it needs — two HEAD probes and not one byte of
body. Ask for something it has not read and it pays for it. The whole-set statistics are the clearest
case: a `quantile_cont` over every matching row has to read that column out of all twelve row groups,
which is the 42%. And a **global sort is the expensive one** — an `ORDER BY` over 607,577 rows reads
that column from every row group and then fetches the page, 88.7% of the file. That is the real shape
of the trade, not a slogan.

What the sort and the row groups buy is pruning: a filter on `flight_date` can only touch the groups whose recorded
min/max fail to rule it out, and on a file sorted by date that is a handful of the twelve rather than all of them.

### Why the data file is called `.zip`

It is a plain zstd Parquet file, not an archive.

GitHub Pages gzips binary files on the fly, and then evaluates `Range` against
the **compressed** length. DuckDB opens a Parquet by reading its footer from the
end of the real file, so that read comes back `416 Range Not Satisfiable` — or,
for the suffix form DuckDB actually uses, `206` carrying the last bytes of the
gzip stream, which is worse, because it looks like an answer. Measured against
the published file:

```
$ curl -sI -H "Accept-Encoding: gzip" …/flights-2026-06.parquet
content-encoding: gzip
content-length: 15135729          ← the compressed length, not the file's

$ curl -s -D- -H "Accept-Encoding: gzip" -H "Range: bytes=15366831-15383214" …
HTTP/2 416
content-range: bytes */15135729
```

`curl` sends no `Accept-Encoding`; every browser sends one. That is why a
`bytes=0-1023` probe certified a deploy that did not work — it is the one
request shape satisfiable against either length.

Pages leaves archive, image, font and video types uncompressed, so the file is
published under a `.zip` name. `read_parquet` reads the format from the file and
never from the name, and `tools/build-parquet.mjs` asserts that the two names are
byte for byte the same file (`sha256 cf693e49…`). The same bytes are downloadable
under their real name, `flights-2026-06.parquet`, from this repository's
`data-2026-06` release.

The proper fix is a CORS header on our CloudFront `demo-data` path, which is an
AWS change the owner has to make; when it lands, the file moves there under its
real name and this paragraph goes away.

`node tools/verify.mjs --live` asserts all of it: that the published response
carries no `content-encoding`, that it reports the real length to a browser, and
that both a footer range and the suffix form are satisfiable against that real
length. That check is what was missing.

The published site is GitHub Pages, which answers `206 Partial Content` the same
way; `node tools/verify.mjs --live` asks it for a byte range and checks that it
does. Pages keeps no request log a visitor could read, so the panel on the
published page quotes the measurement above and says that it is recorded rather
than live.

Two details in `tools/build-parquet.mjs` are what make the pruning work: the
file is written in **row groups of 50,000 rows**, and it is **sorted by flight
date**, so a date filter can be answered from a handful of groups whose
statistics cannot be ruled out.

### 2. Every filter, sort, page and count becomes SQL you can read

The push-plan panel under the table shows the statements DuckDB actually ran —
not a reconstruction of what the grid probably asked for. The connection handed
to the adapter is wrapped so nothing can reach the engine without being
recorded, with its bound parameters and its timing.

```
-- the page on screen · 42 ms · 200 rows back
SELECT "flight_id", "flight_date", … FROM read_parquet('…/flights-2026-06.zip')
WHERE ("arr_delay" >= ?) ORDER BY "arr_delay" DESC LIMIT 200 OFFSET 0
-- bound: [60]
```

Beside it, `source.lastPlan()`'s own account of the split. With this adapter and
these columns, `unpushed` is empty: the whole filter tree, the multi-column
sort, the window and the count all go to the engine. Filter values are **bound**
through prepared statements, never interpolated — the `-- bound:` line is what
proves it.

### 3. Every figure is computed over the whole matching set, not the page

This is the part that is easy to get wrong and hard to notice. The grid holds
200 of 607,577 rows. A statistic reduced from what the grid holds would be the
statistic of a page — wrong, and it looks right.

So nothing here is reduced from the loaded page:

- the five tiles come from one `source.aggregate()` call with no `groupBy` —
  `count`, `avg`, `quantile_cont(…, 0.95)` and two more averages, in one SQL
  statement over everything that matches;
- each of the five charts comes from one `source.aggregate()` call with a
  `groupBy`, which the adapter emits as a single `GROUP BY ROLLUP`;
- with `aggregates: { default: 'engine-if-identical' }` the source pushes only
  the statistics whose engine result is verified identical to the grid's own
  kernel. All five headline statistics qualify, and the plan panel names which
  ran where.

`tools/verify.mjs` recomputes every one of those figures with a **second
DuckDB, in Node**, against the same file, with SQL written out independently,
and fails the build if the page and Node disagree — tile by tile, carrier by
carrier, hour by hour, bucket by bucket, day by day. It is 65 checks, and they
all pass; the last run is quoted in the range table above.

---

## What is honestly still client-side

A demo that claims everything is pushed down is not worth reading. Here is the
list.

| Work | Where it runs | Why |
|---|---|---|
| Filter, sort, window, count | DuckDB | The adapter declares `filter: 'tree'`, `sort: 'multi'`, `range`, `total` |
| The five headline statistics | DuckDB | All classified IDENTICAL by the grid's pushdown map |
| The five chart aggregates | DuckDB | One `GROUP BY ROLLUP` each |
| Five-minute bucketing of the delay distribution | The browser | DuckDB returns the exact count for each distinct minute (about 1,200 rows); adding them into buckets here is arithmetic on 1,200 numbers |
| Sorting and truncating the busiest 40 routes | The browser | DuckDB returns all 6,131 route counts; picking the top 40 from them is trivial |
| **Grouping** | The browser, under a guard | See below |
| Rendering, scrolling, the filter UI | The browser | It is a grid |

### Grouping, and the guard on it

**Grouping pushdown is not in 1.62.1.** The DuckDB adapter declares
`filter`, `sort`, `range` and `total`, and does not declare `group`. It ships in
1.63 (card 1325), and when it does, this demo's grouping control loses its
guard.

Until then, grouping a windowed source would group the 200 rows that happen to
be loaded and present the subtotals as if they described the 607,577. So the
grid's own grouping is switched off — every column carries `allowGroup: false`,
because offering a control that returns a lie is worse than not offering it —
and the demo provides its own, gated:

- while more than **40,000** rows match, the control is disabled and says so;
- once a filter has narrowed the match below that, choosing a grouping rebuilds
  the grid with `fullDataset: { enabled: true, maxRows: 40000 }`, so the whole
  matching set really is in the browser and the subtotals really are its own;
- the plan panel then names grouping as client-side work, and
  `lastPlan().full` is `true`.

`fullDataset` is a design-time decision, fixed for the life of a grid, which is
why switching it means building a new grid rather than flipping a flag.

---

## Known grid defects, not worked around

Public demos do not work around grid defects. Where a chart type could not be
drawn correctly in 1.62.1, this demo draws a different chart and says why in
the code, rather than drawing a wrong one or clamping something until it looks
right. Both are reported.

**F-FLT-1 — `histogram` throws above ~120,000 rows.** `createChart({ type:
'histogram' })` raises `RangeError: Maximum call stack size exceeded` between
120,000 and 150,000 rows (50,000 ok in 1,770 ms; 100,000 ok in 3,220 ms;
120,000 ok in 3,888 ms; 150,000 and 200,000 throw). The delay-distribution
chart here is a bar of counts DuckDB computed over the whole matching set,
which is exact and needs no raw rows in the browser at all — the better design
regardless, and the reasoning is written out at `feedDistribution` in
`src/dashboard.js`.

**F-FLT-2 — `boxplot` with a category scales its measure axis to the per-category
sum.** One point per category, whose `y` is `sum(values)` rather than a
five-number summary, so every box is drawn as a flat line far off the top of
the plot. This is the same defect as the grid's own F-1329-1 (carded 1333 for
1.63) but it reproduces with **no grid grouping at all**, on the plain
`x`-category form. So there is no box plot here: "delay by carrier" is a bar of
the **median** arrival delay per carrier, computed by DuckDB over the whole
matching set.

---

## Running it

```
npm install
npm start          # a static server with range support and byte accounting
npm run verify     # headless Chrome, every figure checked against Node's DuckDB
```

`npm start` prints a URL. It also serves `/__ranges`, the byte tally the page's
own readout uses when you open it locally.

There is **no build step**. Plain JavaScript modules, no framework, no bundler.
The page loads the grid straight out of `node_modules`, which is why
`node_modules` is published to Pages rather than being a build input.

`npm install` fetches exactly one runtime dependency — `@toclocoinc/lattice-grid`
at `^1.62.1` — and one development one, `@duckdb/node-api`, which only the
data-build script and the verification script use. DuckDB-Wasm is not an npm
dependency at all: the page imports it from jsDelivr at a pinned version,
because the grid ships no engine and takes a connection the host made.

### Verifying the published site

```
node tools/verify.mjs --live
```

Runs the page checks against `https://toclocoinc.github.io/lattice-grid-demo-flights/`
and asks the host directly for a byte range, insisting on `206` with the right
`Content-Range`.

---

## How it is wired

```
index.html
  main.js                 starts DuckDB, hands the connection over, nothing else
  src/duckdb.js           DuckDB-Wasm 1.32.0 from jsDelivr, LOAD httpfs, and the
                          connection wrapper that records every statement
  src/flights.js          the columns, the presets, the aggregate requests
  src/dashboard.js        the grid, the tiles, the charts, the push-plan panel
  src/licence.js          the demo's own domain-bound key
  data/flights-2026-06.zip        the Parquet file, under a name Pages will not gzip
tools/
  build-parquet.mjs       BTS zip → one sorted, zstd Parquet file (developer only)
  serve.mjs               static server with Range support and byte accounting
  verify.mjs              headless Chrome + a second DuckDB in Node
```

The wiring itself is eleven lines:

```js
const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm');
// …bundle, worker, instantiate…
const connection = await db.connect();
await connection.query('LOAD httpfs;');          // range reads, not a download

const source = createPushdownSource({
  adapter: duckdbAdapter({
    connection,
    from: `read_parquet('${new URL('./data/flights-2026-06.zip', location.href)}')`,
    fields: GRID_FIELDS,                          // not SELECT *, on a columnar file
  }),
  compute: LG,
  pageSize: 200,
  aggregates: { default: 'engine-if-identical' },
});

createGrid(element, { rowKey: 'flight_id', columns, source });
```

---

## The data

Public-domain US government data: the Bureau of Transportation Statistics'
*Reporting Carrier On-Time Performance* release for June 2026, downloaded from
`transtats.bts.gov` and converted to one Parquet file. Row counts, columns,
licence and the conversion are documented in [`data/README.md`](data/README.md).

Credit, as BTS asks: **US Department of Transportation, Bureau of Transportation
Statistics**.

---

## Licence

The demo code is MIT. The data is a work of the US government and is in the
public domain. Lattice Grid itself is commercial software; the key in
`src/licence.js` is bound to `toclocoinc.github.io` and does nothing anywhere
else. Running this repository on your own machine needs no key at all.
