-- Adds the derived fields every mart needs.
-- snapshot_at_utc: the latest created_at in the data. Using the data's own
-- clock (not now()) keeps backlog ages reproducible when a fixture is rebuilt
-- later. All durations use the UTC columns so DST changes do not skew them.

with requests as (
    select * from {{ ref('stg_service_requests') }}
),

snapshot as (
    select max(created_at_utc) as snapshot_at_utc from requests
)

select
    requests.*,
    snapshot.snapshot_at_utc,
    -- Calendar day in New York, where the request was made.
    cast(created_at as date) as created_day,
    status = 'Closed' as is_closed,
    status not in ('Closed', 'Unspecified') as is_open,
    case
        when status = 'Closed' and closed_at_utc is not null
            then date_diff('second', created_at_utc, closed_at_utc) / 3600.0
    end as resolution_hours,
    case
        when status not in ('Closed', 'Unspecified')
            then date_diff('second', created_at_utc, snapshot.snapshot_at_utc) / 86400.0
    end as open_age_days
from requests
cross join snapshot
