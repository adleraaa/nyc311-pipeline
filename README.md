# nyc311-pipeline

Incremental ELT of [NYC 311 service requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)
into DuckDB, modeled with dbt, checked for data quality, refreshed daily by GitHub Actions and
published as a static dashboard: **https://adleraaa.github.io/nyc311-pipeline/**

The 311 dataset is large (tens of millions of rows) and changes in place: requests are
created, reassigned and closed days later. The pipeline keeps a rolling 30-day window
of requests in sync without re-downloading it every day: a Python extractor pages
through the Socrata API from a stored watermark, lands each page as Parquet and merges
it into DuckDB on `unique_key`; dbt builds staging, intermediate and mart models with
tests; a Python step runs quality checks (freshness, volume anomaly, duplicate rate,
late-arriving data) and the marts are exported to JSON for a Vega-Lite dashboard.

## Results

Measured on 2026-09-29 (UTC) on a Windows 11 laptop (i9-14900HX, home internet), from the
committed files in [`results/`](results/).

| What | Value | Source |
|---|---|---|
| Cold backfill of the 30-day window (created since 2026-08-30) | 300,328 rows, 16 pages of 20,000, 162.7 s | `results/extract_run_1_backfill.json` |
| Warehouse row count vs the API's own `count(*)` for the same window | 300,328 vs 300,328 (difference 0, 0 duplicate keys) | `results/reconciliation.json` |
| Incremental run right after the backfill | 0 rows fetched, 1.5 s | `results/extract_run_2_incremental.json` |
| `dbt build` (5 models, 24 data tests) | 29 of 29 passed, 2.89 s | `results/dbt_build_log.txt` |
| Custom quality checks | overall pass (details below) | `results/quality_report.md` |
| Rows sharing a single `:updated_at` value in the source (last 10 days) | 538,015 of 662,561 updated rows in one batch | `results/source_update_batches.json` |
| Same pipeline on GitHub Actions: cold start (no cached state) / next run (state restored from cache) | 300,328 rows in 341.9 s / 0 rows in 0.6 s | `results/github_actions_runs.md` |
| pytest | 32 passed | `results/pytest_summary.txt` |

The incremental run fetched nothing because the source had not published since the
backfill (it publishes once a day around 01:30 UTC). The scheduled workflow does the
day-to-day incremental loads.

What the data showed for this window (`results/pipeline_summary.json`):

- 241,592 closed requests had a valid resolution time; citywide median 3.45 h, p90 132.49 h.
- Fastest median by agency: NYPD 1.38 h (143,284 closed). Slowest with at least 30 closures:
  OOS 135.24 h, HPD 122.64 h, DOHMH 80.32 h.
- Top complaint types: Illegal Parking (49,574), Noise - Residential (30,014),
  Noise - Street/Sidewalk (19,382).
- Quality report: freshness lag 22.9 h; the last complete day (2026-09-26) had 8,831 requests,
  0.82x its trailing 28-day mean, and 0 of 21 evaluated days fell outside +/-50%;
  median publication lag (request created to row first published) 35.2 h;
  206 requests had a closed date earlier than their created date.

## Architecture

```
Socrata API (erm2-nwe9)
   |  SoQL: $where created_date >= window_start AND (:updated_at, unique_key) > watermark
   |        $order :updated_at, unique_key   $limit 20000
   v
nyc311 extract (Python)
   |-- data/landing/<run_id>/page_NNNNN.parquet     raw pages, kept 7 days
   '-- data/warehouse.duckdb
         raw.service_requests   INSERT ... ON CONFLICT (unique_key) DO UPDATE
         raw.extract_state      watermark, committed in the same transaction as each page
         raw.extract_runs       one audit row per run
   v
dbt build (dbt-duckdb)
   staging.stg_service_requests      types, trimming, borough normalization, bad close dates
   intermediate.int_requests_enriched   day, open/closed, resolution hours, open age
   marts.fct_daily_volume            day x borough x complaint type
   marts.agg_resolution_time         median / p90 hours by agency x type (+ agency rollup)
   marts.agg_open_backlog_aging      open requests by agency x type x age bucket
   v
nyc311 quality   ->  quality_report.json / .md
nyc311 build-site -> _site/ (static HTML + data/*.json)  ->  GitHub Pages
```

Two workflows:

- `.github/workflows/ci.yml` (push / PR): ruff, pytest with mocked HTTP, then the whole
  pipeline on the committed fixture (`load-fixture`, `dbt build`, `quality`, `build-site`).
  No network or secrets.
- `.github/workflows/pipeline.yml` (daily 07:30 UTC + manual): restore state from the
  Actions cache, extract, save state, `dbt build`, quality, build the site, deploy Pages.

## Quickstart / Reproduce

Requires Python 3.12+.

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

# Live data (about 3 minutes and 300k rows for a cold 30-day backfill)
python -m nyc311 -v extract --window-days 30 --stats-out results/extract_run.json
dbt build --project-dir dbt --profiles-dir dbt
python -m nyc311 quality --out results
python -m nyc311 summary
python -m nyc311 build-site --out _site
python -m http.server -d _site 8000                    # open http://localhost:8000

# Checks that produced the other files in results/
python scripts/reconcile.py
python scripts/profile_source.py

# Offline, from the committed fixture (what CI runs)
python -m nyc311 load-fixture tests/fixtures/sample_311.json.gz
dbt build --project-dir dbt --profiles-dir dbt

# Tests and lint
pytest -q
ruff check . && ruff format --check .
```

Running `extract` again continues from the stored watermark. Delete `data/` to start over.
Set `SOCRATA_APP_TOKEN` to send an app token (optional; raises the API rate limit).

## Design decisions

- **Keyset paging on `(:updated_at, unique_key)` instead of `$offset`.** The source writes its
  daily update as one batch with a single `:updated_at` (538,015 rows shared one value in the
  last 10 days). A watermark on the timestamp alone cannot say where inside that batch a page
  ended, and `$offset` shifts when rows change during a run. The key tiebreaker gives every row a
  unique position; `results/reconciliation.json` shows the paged total matching the source count.
- **Watermark committed in the same transaction as each page.** A crash loses at most the page
  in flight, and the next run resumes after the last committed row. Reprocessing is safe because
  the load is an upsert (tested in `tests/test_extract.py`).
- **`ON CONFLICT DO UPDATE` rather than `INSERT OR REPLACE`.** Replace would delete and
  re-insert the row, losing `_first_loaded_at` and `_load_count`, which the late-update check
  reads.
- **Raw layer is all VARCHAR; typing happens in dbt staging.** A malformed value (for example
  a ZIP of "N/A") becomes NULL in staging instead of failing the load. Closed dates earlier than
  created dates (206 in this window) are nulled and flagged rather than producing negative
  resolution times.
- **State lives in the Actions cache, and a cache miss is a full rebuild.** The cache keeps the
  DuckDB file between daily runs; if it is evicted, the
  extractor sees no watermark and backfills the 30-day window, which takes minutes. This avoids
  committing data to git or needing a cloud bucket and credentials.
- **Checks use the data's clock, not the wall clock.** Freshness compares against the run's
  recorded `source_as_of`, and backlog age is measured at the latest `created_at`. The same
  fixture therefore gives the same answers in CI on any day.

## Data

Source: NYC Open Data, "311 Service Requests from 2010 to Present", dataset `erm2-nwe9`,
https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9,
published by the City of New York under the
[NYC Open Data Terms of Use](https://opendata.cityofnewyork.us/overview/#termsofuse)
(free to use and redistribute; no warranty). `tests/fixtures/sample_311.json.gz` is a
3,010-row sample of that dataset (requests whose `unique_key` ends in `00`, created in the 30
days before 2026-09-29), generated by `scripts/make_fixture.py`. No data is LLM-generated.

## Limitations

- Resolution times only include requests that were created in the window and are already
  closed. Long-running requests are still open, so medians and p90s are biased low, most for
  agencies with slow cases (HPD, DOHMH). Backlog ages are likewise capped at the window length.
- The window boundary uses the UTC calendar date while `created_date` is New York local time,
  so the boundary can be off by a few hours. Publication lag compares NY-local creation time with
  a UTC publish time, which overstates the lag by 4-5 hours.
- The volume check compares each day with a trailing 28-day mean and does not model
  day-of-week seasonality; it uses a loose +/-50% band to avoid weekend false alarms, so it only
  catches large breaks (for example a partial load).
- Borough normalization maps the county and borough names found in the feed; it does not
  geocode requests whose borough is unspecified; they are kept as `UNSPECIFIED` and left
  off the borough chart.
- GitHub disables scheduled workflows after 60 days without repository activity, and cache
  entries unused for 7 days are evicted (which triggers a full backfill).
- The duplicate check can only see keys delivered more than once in a run, plus duplicate keys
  in the warehouse (always 0 because of the primary key); it does not detect two different
  requests describing the same incident.

## License

MIT, copyright 2026 Yunlong Lu. See [LICENSE](LICENSE).
