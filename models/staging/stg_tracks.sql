{{ config(materialized='table') }}

with createsco_tracks as (
    select
        id as event_id,
        'track' as event_type,
        event as event_name,
        timestamp,
        anonymous_id,
        id as user_id, -- Mapping id as user_id for this source
        event_date,
        null as context_library_name,
        null as context_library_version
    from {{ source('createsco_dev', 'tracks') }}
),

-- rudderstack_tracks as (
--     select
--         id as event_id,
--         'track' as event_type,
--         event as event_name,
--         timestamp,
--         anonymous_id,
--         user_id,
--         null as event_date,
--         null as context_library_name,
--         null as context_library_version
--     from {{ source('rudderstack', 'tracks') }}
-- ),

-- unioned as (
--     select * from createsco_tracks
--     union all
--     select * from rudderstack_tracks
-- )

select
    event_id,
    event_type,
    event_name,
    timestamp,
    trim(anonymous_id) as anonymous_id,
    trim(user_id) as user_id,
    event_date,
    context_library_name,
    context_library_version
from createsco_tracks
