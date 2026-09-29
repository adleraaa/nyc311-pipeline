-- One row per 311 request: typed, trimmed, borough-normalized.
-- Source timestamps: created/closed are New York local time without an offset;
-- the Socrata system fields (_sys_*) are UTC with a trailing "Z". Local times
-- are kept for calendar-day grouping, and *_utc versions are added for every
-- duration and for comparisons with the UTC system fields.

with source as (
    select * from {{ source('raw', 'service_requests') }}
),

typed as (
    select
        unique_key,
        try_cast(created_date as timestamp) as created_at,
        try_cast(closed_date as timestamp) as closed_at_raw,
        try_cast(resolution_action_updated_date as timestamp) as resolution_updated_at,
        coalesce(nullif(upper(trim(agency)), ''), 'UNKNOWN') as agency,
        trim(agency_name) as agency_name,
        coalesce(nullif(trim(complaint_type), ''), 'Unspecified') as complaint_type,
        nullif(trim(descriptor), '') as descriptor,
        coalesce(nullif(trim(status), ''), 'Unspecified') as status,
        {{ normalize_borough('borough') }} as borough,
        -- ZIPs arrive as text and sometimes as "N/A" or 9-digit ZIP+4.
        case when regexp_matches(trim(incident_zip), '^[0-9]{5}')
             then left(trim(incident_zip), 5) end as incident_zip,
        upper(trim(open_data_channel_type)) as channel,
        try_cast(latitude as double) as latitude,
        try_cast(longitude as double) as longitude,
        try_cast(replace(_sys_created_at, 'Z', '') as timestamp) as published_at_utc,
        try_cast(replace(_sys_updated_at, 'Z', '') as timestamp) as source_updated_at_utc,
        _first_loaded_at,
        _last_loaded_at,
        _last_run_id,
        _load_count
    from source
),

with_utc as (
    select
        *,
        {{ ny_to_utc('created_at') }} as created_at_utc,
        {{ ny_to_utc('closed_at_raw') }} as closed_at_raw_utc
    from typed
)

select
    * exclude (closed_at_raw, closed_at_raw_utc),
    -- A closed date before the created date is a known defect in this feed
    -- (placeholder dates, data entry errors). Treat it as unknown instead of
    -- producing a negative resolution time, and keep a flag for QA.
    case when closed_at_raw_utc >= created_at_utc then closed_at_raw end as closed_at,
    case when closed_at_raw_utc >= created_at_utc then closed_at_raw_utc end as closed_at_utc,
    coalesce(closed_at_raw_utc < created_at_utc, false) as has_invalid_closed_date
from with_utc
where created_at is not null
