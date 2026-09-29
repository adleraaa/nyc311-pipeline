-- Open requests by agency, complaint type and age bucket at snapshot_at.

with open_requests as (
    select
        agency,
        complaint_type,
        open_age_days,
        case
            when open_age_days < 1 then 1
            when open_age_days < 3 then 2
            when open_age_days < 7 then 3
            when open_age_days < 14 then 4
            when open_age_days < 30 then 5
            else 6
        end as bucket_order
    from {{ ref('int_requests_enriched') }}
    where is_open
)

select
    agency,
    complaint_type,
    bucket_order,
    case bucket_order
        when 1 then '<1d'
        when 2 then '1-3d'
        when 3 then '3-7d'
        when 4 then '7-14d'
        when 5 then '14-30d'
        else '30d+'
    end as age_bucket,
    count(*) as n_open,
    max(open_age_days) as max_age_days
from open_requests
group by all
