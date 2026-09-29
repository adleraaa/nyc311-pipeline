# Data quality report

Run `20260929T003228Z-api` (api), source observed at 2026-09-29T00:32:28.900201 UTC. Overall: **pass**.

| Check | Status | Detail |
|---|---|---|
| freshness | pass | latest source update 22.9 h before the run (warn > 36 h, fail > 72 h) |
| volume_anomaly | pass | 2026-09-26: 8831 requests = 0.82x the trailing 28-day mean; 0 of 21 evaluated days outside +/-50% |
| duplicate_rate | pass | 0 duplicate keys in the warehouse; 0.00% of rows fetched in the latest run were repeats |
| late_arriving | info | publication lag p50 35.2 h, p90 44.2 h; 1.0% published > 72 h after creation; latest run touched 0 requests created > 7 days earlier |
| closed_before_created | info | 206 of 300328 requests have closed_date < created_date (resolution time set to null) |
