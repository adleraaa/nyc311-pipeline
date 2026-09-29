-- Resolution times are non-negative and p90 >= median for every group.

select *
from {{ ref('agg_resolution_time') }}
where median_hours < 0 or p90_hours < median_hours
