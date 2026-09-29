-- Resolution time (hours) by agency and complaint type, plus an agency rollup
-- row (complaint_type = '(all)'). Medians cannot be averaged, so the rollup is
-- computed from the request rows with GROUPING SETS, not from the detail rows.
--
-- Censoring: taking only requests that are already closed would bias toward
-- fast cases, because the slow ones are still open. Instead the cohort is every
-- request created at least `resolution_min_age_days` before snapshot_at, and a
-- request that is still open counts as "not resolved yet" (+infinity). A
-- quantile is then exact whenever it lands on a closed request, and NULL
-- otherwise: the median needs more than half of the cohort closed, the p90
-- more than 90%. quantile_disc returns an observed value, which avoids
-- interpolating toward infinity.

with cohort as (
    select
        agency,
        complaint_type,
        is_open,
        case when is_open then 'infinity'::double else resolution_hours end as hours
    from {{ ref('int_requests_enriched') }}
    where created_at_utc <= snapshot_at_utc - to_days({{ var('resolution_min_age_days') }})
        -- Closed with an unusable closed date, or status Unspecified: the
        -- outcome is unknown, so the request cannot be placed in the ranking.
        and (is_open or resolution_hours is not null)
),

grouped as (
    select
        agency,
        case when grouping(complaint_type) = 1 then '(all)' else complaint_type end
            as complaint_type,
        count(*) as n_requests,
        count(*) filter (where not is_open) as n_closed,
        quantile_disc(hours, 0.5) as median_any,
        quantile_disc(hours, 0.9) as p90_any
    from cohort
    group by grouping sets ((agency, complaint_type), (agency))
)

select
    agency,
    complaint_type,
    n_requests,
    n_closed,
    (n_requests - n_closed) / n_requests as share_open,
    case when isfinite(median_any) then median_any end as median_hours,
    case when isfinite(p90_any) then p90_any end as p90_hours
from grouped
