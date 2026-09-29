-- Requests created per day x borough x complaint type.

select
    created_day,
    borough,
    complaint_type,
    count(*) as n_requests
from {{ ref('int_requests_enriched') }}
group by all
