# Scheduled-pipeline runs on GitHub Actions (ubuntu-latest)

Extract stats lines copied verbatim from the job logs of `.github/workflows/pipeline.yml`
(manual `workflow_dispatch` triggers on 2026-09-29).

| Run | State cache | Extract output |
|---|---|---|
| [36504170610](https://github.com/adleraaa/nyc311-pipeline/actions/runs/36504170610) | not found (cold start) | `{"run_id": "20260929T004005Z-api", "pages": 16, "rows_fetched": 300328, "rows_inserted": 300328, "rows_updated": 0, "rows_pruned": 0, "rows_duplicate_in_run": 0, "seconds": 341.9}` |
| [36504720501](https://github.com/adleraaa/nyc311-pipeline/actions/runs/36504720501) | restored from `warehouse-v1-36504170610-1` | `{"run_id": "20260929T004653Z-api", "pages": 0, "rows_fetched": 0, "rows_inserted": 0, "rows_updated": 0, "rows_pruned": 0, "rows_duplicate_in_run": 0, "seconds": 0.6}` |

Both runs: `dbt build` PASS=29, quality overall pass, Pages deploy succeeded.
