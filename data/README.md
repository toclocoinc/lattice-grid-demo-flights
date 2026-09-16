# The data

`flights-2026-06.zip` — one month of US domestic flights, as the airlines
reported them.

**It is a plain zstd Parquet file, not an archive.** GitHub Pages gzips binary
files on the fly and evaluates byte ranges against the compressed length, which
breaks DuckDB's footer read; it leaves archive types alone, so the file is
published under a `.zip` name. `read_parquet` reads the format from the file,
never from the name. The same bytes are downloadable under their real name,
`flights-2026-06.parquet`, from this repository's `data-2026-06` release, and
`tools/build-parquet.mjs` asserts the two are byte for byte identical. The
proper fix is a CORS header on our CloudFront `demo-data` path — an AWS change
the owner has to make; when it lands the file moves there under its real name.

| | |
|---|---|
| Source | [US Bureau of Transportation Statistics](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ), *Reporting Carrier On-Time Performance (1987–present)* |
| Downloaded from | `https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2026_6.zip` (31.6 MB) |
| Month | June 2026 |
| Rows | 607,577 |
| Carriers | 12 |
| Airports | 351 |
| Routes | 6,131 |
| Cancellations | 10,019 |
| File | 14.67 MB, Parquet, zstd, 12 row groups of 50,000 rows, sorted by flight date |
| sha256 | `cf693e498274d554e9f6ee0e0a366abfcfee532d527c7c3cf75c51adb19b3801` |
| Licence | A work of the United States government. Not subject to copyright in the US; in the public domain. BTS asks that it be credited, and the page does. |

`facts.json` holds the same figures, written by the build script, and the page
reads the file size from it.

`range-measurement.json` is the byte accounting recorded by
`node tools/verify.mjs --record`: how many HTTP range requests DuckDB made and
how much of the file it actually read, for the first paint, for one more page
of rows, and for one filtered query. The published page falls back to it,
labelled as recorded rather than live, because GitHub Pages keeps no request
log a visitor could read.

## Rebuilding it

```
npm install                            # @duckdb/node-api comes in as a devDependency
node tools/build-parquet.mjs --ext zip # the name the page reads
```

`--ext` decides only the file name; the bytes are the same whatever it is. When
a sibling of the same month exists under another extension the script compares
their hashes and fails the build if they differ, because the copy published for
download under its real name must be the file the page reads.

The script downloads the month's zip (caching it in `.cache/`), unzips it with
the `unzip` command, and writes the Parquet file with DuckDB. Nothing the
published page does needs any of that: the page reads the committed Parquet
file and nothing else.

`node tools/build-parquet.mjs --month 2026-05` builds a different month. BTS
publishes each month a couple of months in arrears.

## What is in it

Thirty columns, all either copied from the BTS file or derived from BTS columns
by arithmetic written out in `tools/build-parquet.mjs`. The source file has 110
columns; the ones left out are the five diversion blocks, which are empty for
all but a few hundred rows.

| Column | From BTS | Note |
|---|---|---|
| `flight_id` | — | A row number over the sorted file. The grid's `rowKey`. |
| `flight_date` | `FlightDate` | |
| `day_of_week` | `DayOfWeek` | 1 is Monday, as BTS numbers it |
| `carrier` | `Reporting_Airline` | The IATA code. BTS's monthly file carries no carrier name, and its lookup table is only downloadable through a form, so the code is what the demo shows rather than a name we made up. |
| `flight_number` | `Flight_Number_Reporting_Airline` | |
| `tail_number` | `Tail_Number` | |
| `origin`, `origin_city`, `origin_state` | `Origin`, `OriginCityName`, `OriginState` | |
| `dest`, `dest_city`, `dest_state` | `Dest`, `DestCityName`, `DestState` | |
| `route` | `Origin ‖ '-' ‖ Dest` | |
| `dep_hour` | `CRSDepTime` | The scheduled departure hour, 0–23. BTS writes midnight as 2400, which folds to hour 0. |
| `scheduled_dep`, `scheduled_arr` | `FlightDate` + `CRSDepTime` / `CRSArrTime` | Local clock time at the airport, as BTS records it — there is no timezone in the source. |
| `dep_delay`, `arr_delay` | `DepDelay`, `ArrDelay` | Minutes, **signed**: negative is early. `ArrDelayMinutes`, which floors early arrivals at zero, is deliberately not used — the distribution chart wants the real number. |
| `taxi_out`, `taxi_in`, `air_time` | `TaxiOut`, `TaxiIn`, `AirTime` | Minutes |
| `distance` | `Distance` | Miles |
| `cancelled`, `diverted` | `Cancelled`, `Diverted` | |
| `cancellation_code` | `CancellationCode` | A, B, C or D. BTS defines them as Carrier, Weather, National Air System and Security; the page shows those words and an unrecognised letter as itself. |
| `on_time` | `ArrDel15` | BTS's own indicator, inverted: `ArrDel15 = 1` means 15 minutes late or more. NULL when the flight never arrived, so an on-time rate is a rate over arrivals. |
| `on_time_n`, `cancelled_n` | as above | The 1/0 twins of those two indicators. DuckDB has no `avg(BOOLEAN)`, so a percentage computed in the engine needs a number to average. Nothing displays them. |
| `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay` | same names | Minutes, BTS's attribution of a delay of 15 minutes or more. Blank for on-time flights. |
