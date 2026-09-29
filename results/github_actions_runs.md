# Scheduled-pipeline runs on GitHub Actions (ubuntu-latest)

Extract stats lines copied verbatim from the job logs of `.github/workflows/pipeline.yml`
(manual `workflow_dispatch` triggers on 2026-09-29).

| Run | State cache | Extract output |
|---|---|---|
| [36504170610](https://github.com/adleraaa/nyc311-pipeline/actions/runs/36504170610) | not found (cold start) | `{"run_id": "20260929T004005Z-api", "pages": 16, "rows_fetched": 300328, "rows_inserted": 300328, "rows_updated": 0, "rows_pruned": 0, "rows_duplicate_in_run": 0, "seconds": 341.9}` |
| [36504720501](https://github.com/adleraaa/nyc311-pipeline/actions/runs/36504720501) | restored from `warehouse-v1-36504170610-1` | `{"run_id": "20260929T004653Z-api", "pages": 0, "rows_fetched": 0, "rows_inserted": 0, "rows_updated": 0, "rows_pruned": 0, "rows_duplicate_in_run": 0, "seconds": 0.6}` |
| [36520857273](https://github.com/adleraaa/nyc311-pipeline/actions/runs/36520857273) | restored from `warehouse-v1-36504720501-1` | `{"run_id": "20260929T041626Z-api-244afa", "mode": "incremental", "status": "success", "pages": 6, "rows_fetched": 107494, "rows_inserted": 9150, "rows_updated": 98344, "rows_pruned": 0, "rows_duplicate_in_run": 0, "seconds": 168.6}` |

Runs 1 and 2: `dbt build` PASS=29, quality overall pass, Pages deploy succeeded (code as of
commit a171942, before the review fixes).

Cold-start timing, from the job log of run 1: the extract started at 00:40:05.6; the first page
request hit the 180 s read timeout at 00:43:05.9 and was retried by urllib3; page 1 arrived at
00:44:13.2, about 248 s into the run, and page 16 at 00:45:47.4, so the other 15 pages took
about 94 s of the 341.9 s.

Run 3 (manual trigger at 04:15 UTC on 2026-09-29, after the source's daily update became
queryable around 04:05 UTC; code as of commit "Refuse answers from stale Socrata replicas")
is the first incremental run on Actions with real changes. It restored the warehouse saved by
run 2, which predates the `status` column; the `ADD COLUMN IF NOT EXISTS` migration upgraded it
in place. Its extract log has 14 "stale replica" warnings, each followed by a 10 s wait before
the retry, so roughly 140 s of the 168.6 s was waiting for an up-to-date replica. `dbt build`
PASS=31, quality overall pass (`source_count`: warehouse 309478 rows vs source 309478),
Pages deploy succeeded.
