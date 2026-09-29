-- Resolution time (hours) by agency and complaint type, plus an agency-level
-- rollup row (complaint_type = '(all)'). Medians cannot be averaged, so the
-- rollup is computed from the rows with GROUPING SETS, not from the detail rows.
-- Only requests created in the window and already closed are included, which
-- biases toward fast resolutions (see README, Limitations).

select
    agency,
    coalesce(complaint_type, '(all)') as complaint_type,
    count(*) as n_closed,
    quantile_cont(resolution_hours, 0.5) as median_hours,
    quantile_cont(resolution_hours, 0.9) as p90_hours
from {{ ref('int_requests_enriched') }}
where resolution_hours is not null
group by grouping sets ((agency, complaint_type), (agency))
