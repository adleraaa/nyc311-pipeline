-- The daily-volume mart must account for every staged request exactly once.
-- Returns a row (= failure) if the totals differ.

select staged.n as staged_rows, mart.n as mart_rows
from (select count(*) as n from {{ ref('stg_service_requests') }}) as staged
cross join (select sum(n_requests) as n from {{ ref('fct_daily_volume') }}) as mart
where staged.n <> coalesce(mart.n, 0)
