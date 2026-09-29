-- Open requests rank as +infinity and quantile_disc picks the value at
-- position ceil(n * q) (1-based), so the median must be reported exactly when
-- at least ceil(n / 2) requests of the cohort are closed.

select *
from {{ ref('agg_resolution_time') }}
where (median_hours is not null) <> (n_closed >= ceil(n_requests * 0.5))
