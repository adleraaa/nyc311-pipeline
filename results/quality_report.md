# Data quality report

Run `20260929T040819Z-api-036de8` (api, incremental, success), source observed at 2026-09-29T04:08:19.987474 UTC. Overall: **pass**.

| Check | Status | Detail |
|---|---|---|
| freshness | pass | latest source update 2.4 h before the run (warn > 36 h, fail > 72 h) |
| volume_anomaly | pass | 2026-09-26: 8840 requests = 0.84x the mean of the same weekday in the 3 preceding weeks; 0 of 14 evaluated days outside +/-25% (complete = ended >= 40.2 h, the p90 publication lag, before the latest source update) |
| duplicate_rate | pass | 0.00% of 107494 rows fetched in the latest run were repeats (warn > 1%) |
| source_count | pass | warehouse 309478 rows vs source 309478 (difference +0, warn above 0.1%) |
| late_arriving | info | publication lag p50 31.2 h, p90 40.2 h; 0.8% published > 72 h after creation; latest run updated 98344 existing requests, 44994 of them created > 7 days earlier |
| closed_before_created | info | 212 of 309478 requests have closed_date < created_date (resolution time set to null) |
