-- Every open request lands in exactly one age bucket.

select enriched.n as open_rows, mart.n as mart_rows
from (select count(*) as n from {{ ref('int_requests_enriched') }} where is_open) as enriched
cross join (select sum(n_open) as n from {{ ref('agg_open_backlog_aging') }}) as mart
where enriched.n <> coalesce(mart.n, 0)
