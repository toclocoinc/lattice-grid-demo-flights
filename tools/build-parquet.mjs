/**
 * Build data/flights-2026-06.parquet from the BTS monthly release.
 *
 * This is a one-off developer tool. The published page never runs it: it reads
 * the committed Parquet file directly with DuckDB-Wasm. Run it only to refresh
 * the data, or to point the demo at a different month.
 *
 *   node tools/build-parquet.mjs                 # the committed month
 *   node tools/build-parquet.mjs --month 2026-05 # a different one
 *   node tools/build-parquet.mjs --keep          # keep the 275 MB CSV around
 *
 * What it does:
 *
 *   1. Downloads the month's zip from transtats.bts.gov, unless a copy is
 *      already cached in .cache/ (31 MB).
 *   2. Unzips it (needs the `unzip` command) into .cache/. The CSV inside is
 *      about 275 MB and has 110 columns, most of them empty diversion fields.
 *   3. Reads it with DuckDB and writes ONE Parquet file with the 27 columns
 *      this demo uses, zstd-compressed, in row groups of 50,000 rows, sorted
 *      by flight date.
 *
 * The sort and the row-group size are the whole point. DuckDB-Wasm reads the
 * file over HTTP with range requests, and it can only skip a row group when
 * that group's min/max statistics rule it out. Sorted by date, a filter on a
 * date window touches a handful of groups instead of all of them; 50,000 rows
 * a group keeps each range read a sensible size rather than one enormous one.
 *
 * Nothing is invented here. Every column is either copied from the BTS file or
 * derived from BTS columns by arithmetic that is written out below.
 */

import { spawn } from 'node:child_process';
import { access, mkdir, rm, stat, writeFile } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
const cache = join(root, '.cache');

const args = process.argv.slice(2);
const monthArg = args.includes('--month') ? args[args.indexOf('--month') + 1] : '2026-06';
const keepCsv = args.includes('--keep');

const [year, month] = monthArg.split('-').map(Number);
if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
  throw new Error(`--month wants YYYY-MM, got "${monthArg}"`);
}

const stem = `On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}`;
const ZIP_URL = `https://transtats.bts.gov/PREZIP/${stem}.zip`;
const zipFile = join(cache, `${stem}.zip`);
const csvFile = join(cache, `On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_${year}_${month}.csv`);
const outFile = join(root, 'data', `flights-${String(year)}-${String(month).padStart(2, '0')}.parquet`);

const exists = async (path) => {
  try { await access(path); return true; } catch { return false; }
};

const mb = (bytes) => `${(bytes / 1048576).toFixed(1)} MB`;

/** Run a command, inheriting stdio, and reject on a non-zero exit. */
const run = (command, argv) => new Promise((ok, fail) => {
  const child = spawn(command, argv, { stdio: ['ignore', 'inherit', 'inherit'] });
  child.on('error', fail);
  child.on('exit', (code) => (code === 0 ? ok() : fail(new Error(`${command} exited ${code}`))));
});

await mkdir(cache, { recursive: true });
await mkdir(join(root, 'data'), { recursive: true });

/* ---------------------------------------------------------------- download */

if (await exists(zipFile)) {
  console.log(`Using the cached download: ${zipFile}`);
} else {
  console.log(`Downloading ${ZIP_URL}`);
  const response = await fetch(ZIP_URL);
  if (!response.ok) throw new Error(`BTS answered ${response.status} for ${ZIP_URL}`);
  await pipeline(Readable.fromWeb(response.body), createWriteStream(zipFile));
  console.log(`  ${mb((await stat(zipFile)).size)}`);
}

if (await exists(csvFile)) {
  console.log(`Using the extracted CSV: ${csvFile}`);
} else {
  console.log('Unzipping...');
  await run('unzip', ['-o', '-q', zipFile, '-d', cache]);
}
console.log(`  CSV is ${mb((await stat(csvFile)).size)}`);

/* ----------------------------------------------------------------- convert */

const { DuckDBInstance } = await import('@duckdb/node-api');
const db = await DuckDBInstance.create(':memory:');
const conn = await db.connect();

/*
 * The projection.
 *
 * CRSDepTime and CRSArrTime are local clock times written as hhmm strings, and
 * BTS writes midnight as 2400. `% 24` folds that back to hour 0 of the same
 * date, which is where the schedule's own day puts it.
 *
 * Every delay column is BTS's, in minutes, signed: a negative arrival delay is
 * an early arrival. ArrDelayMinutes (which floors early arrivals at 0) is left
 * out deliberately, because the distribution chart wants the signed value.
 *
 * on_time is BTS's own ArrDel15 indicator inverted, not a threshold of our
 * choosing: ArrDel15 is 1 when the flight arrived 15 or more minutes late.
 * Cancelled and diverted flights have no arrival delay, so on_time is NULL for
 * them rather than false, and the on-time rate below counts only flights that
 * actually arrived.
 */
const SELECT = `
  SELECT
    row_number() OVER (ORDER BY "FlightDate", "CRSDepTime", "Reporting_Airline", "Flight_Number_Reporting_Airline")::INTEGER AS flight_id,
    "FlightDate"::DATE                                   AS flight_date,
    "DayOfWeek"::TINYINT                                 AS day_of_week,
    "Reporting_Airline"::VARCHAR                         AS carrier,
    "Flight_Number_Reporting_Airline"::VARCHAR           AS flight_number,
    nullif("Tail_Number", '')::VARCHAR                   AS tail_number,
    "Origin"::VARCHAR                                    AS origin,
    "OriginCityName"::VARCHAR                            AS origin_city,
    "OriginState"::VARCHAR                               AS origin_state,
    "Dest"::VARCHAR                                      AS dest,
    "DestCityName"::VARCHAR                              AS dest_city,
    "DestState"::VARCHAR                                 AS dest_state,
    ("Origin" || '-' || "Dest")::VARCHAR                 AS route,
    (CAST("CRSDepTime" AS INTEGER) // 100) % 24          AS dep_hour,
    ("FlightDate"::DATE::TIMESTAMP
      + to_hours(((CAST("CRSDepTime" AS INTEGER) // 100) % 24)::BIGINT)
      + to_minutes((CAST("CRSDepTime" AS INTEGER) % 100)::BIGINT)) AS scheduled_dep,
    ("FlightDate"::DATE::TIMESTAMP
      + to_hours(((CAST("CRSArrTime" AS INTEGER) // 100) % 24)::BIGINT)
      + to_minutes((CAST("CRSArrTime" AS INTEGER) % 100)::BIGINT)) AS scheduled_arr,
    "DepDelay"::SMALLINT                                 AS dep_delay,
    "ArrDelay"::SMALLINT                                 AS arr_delay,
    "TaxiOut"::SMALLINT                                  AS taxi_out,
    "TaxiIn"::SMALLINT                                   AS taxi_in,
    "AirTime"::SMALLINT                                  AS air_time,
    "Distance"::SMALLINT                                 AS distance,
    ("Cancelled" = 1)                                    AS cancelled,
    nullif("CancellationCode", '')::VARCHAR              AS cancellation_code,
    ("Diverted" = 1)                                     AS diverted,
    CASE WHEN "ArrDel15" IS NULL THEN NULL ELSE "ArrDel15" = 0 END AS on_time,
    -- The 1/0 twins of the two indicator columns. DuckDB has no avg(BOOLEAN),
    -- so a percentage tile computed in the engine needs a number to average.
    -- on_time_n is NULL when the flight never arrived, which is what keeps the
    -- on-time rate a rate over arrivals rather than over schedules.
    CASE WHEN "ArrDel15" IS NULL THEN NULL WHEN "ArrDel15" = 0 THEN 1 ELSE 0 END::TINYINT AS on_time_n,
    CASE WHEN "Cancelled" = 1 THEN 1 ELSE 0 END::TINYINT       AS cancelled_n,
    "CarrierDelay"::SMALLINT                             AS carrier_delay,
    "WeatherDelay"::SMALLINT                             AS weather_delay,
    "NASDelay"::SMALLINT                                 AS nas_delay,
    "SecurityDelay"::SMALLINT                            AS security_delay,
    "LateAircraftDelay"::SMALLINT                        AS late_aircraft_delay
  FROM read_csv(?, header = true, all_varchar = false, sample_size = 200000)
`;

console.log('Reading the CSV with DuckDB and writing the Parquet file...');
await conn.run(`
  COPY (${SELECT} ORDER BY flight_date, scheduled_dep, flight_id)
  TO '${outFile.replaceAll("'", "''")}'
  (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000)
`, [csvFile]);

/* -------------------------------------------------------------------- facts */

const quoted = outFile.replaceAll("'", "''");
const facts = (await conn.runAndReadAll(`
  SELECT
    count(*)                                   AS rows,
    min(flight_date)                           AS first_date,
    max(flight_date)                           AS last_date,
    count(DISTINCT carrier)                    AS carriers,
    count(DISTINCT origin)                     AS airports,
    count(DISTINCT route)                      AS routes,
    sum(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancellations
  FROM read_parquet('${quoted}')
`)).getRowObjects()[0];

const groups = (await conn.runAndReadAll(`
  SELECT count(*) AS row_groups FROM parquet_metadata('${quoted}')
  WHERE column_id = 0
`)).getRowObjects()[0];

const size = (await stat(outFile)).size;
const summary = {
  file: outFile.slice(root.length + 1),
  bytes: size,
  megabytes: Number((size / 1048576).toFixed(2)),
  rows: Number(facts.rows),
  row_groups: Number(groups.row_groups),
  first_date: String(facts.first_date),
  last_date: String(facts.last_date),
  carriers: Number(facts.carriers),
  airports: Number(facts.airports),
  routes: Number(facts.routes),
  cancellations: Number(facts.cancellations),
  source: ZIP_URL,
  built: new Date().toISOString().slice(0, 10),
};

await writeFile(join(root, 'data', 'facts.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));

if (size > 60 * 1048576) {
  console.error(`\nThat file is ${mb(size)}, over the 60 MB the repository allows. Take a smaller slice.`);
  process.exitCode = 1;
}

conn.closeSync();
if (!keepCsv) {
  await rm(csvFile, { force: true });
  console.log('\nRemoved the extracted CSV; the zip stays cached in .cache/.');
}
