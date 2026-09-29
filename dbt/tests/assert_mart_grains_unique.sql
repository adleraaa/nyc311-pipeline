-- Each mart has one row per declared grain.

select 'fct_daily_volume' as model, created_day::varchar || '|' || borough || '|' || complaint_type as grain
from {{ ref('fct_daily_volume') }}
group by all having count(*) > 1

union all

select 'agg_resolution_time', agency || '|' || complaint_type
from {{ ref('agg_resolution_time') }}
group by all having count(*) > 1

union all

select 'agg_open_backlog_aging', agency || '|' || complaint_type || '|' || age_bucket
from {{ ref('agg_open_backlog_aging') }}
group by all having count(*) > 1
