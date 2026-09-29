-- Adds the derived fields every mart needs.
-- snapshot_at: the latest created_at in the data. Using the data's own clock
-- (not now()) keeps backlog ages reproducible when a fixture is rebuilt later.

with requests as (
    select * from {{ ref('stg_service_requests') }}
),

snapshot as (
    select max(created_at) as snapshot_at from requests
)

select
    requests.*,
    snapshot.snapshot_at,
    cast(created_at as date) as created_day,
    status = 'Closed' as is_closed,
    status not in ('Closed', 'Unspecified') as is_open,
    case
        when status = 'Closed' and closed_at is not null
            then date_diff('second', created_at, closed_at) / 3600.0
    end as resolution_hours,
    case
        when status not in ('Closed', 'Unspecified')
            then date_diff('second', created_at, snapshot.snapshot_at) / 86400.0
    end as open_age_days
from requests
cross join snapshot
