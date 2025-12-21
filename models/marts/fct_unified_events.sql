with events as (
    select * from {{ ref('stg_tracks') }}
),

mapping as (
    select * from {{ ref('int_id_mapping') }}
)

select
    e.event_id,
    e.event_name,
    e.timestamp,
    e.anonymous_id,
    e.user_id,
    m.stitched_id as blended_user_id,
    e.context_library_name,
    e.context_library_version
from events e
left join mapping m 
    on coalesce(e.user_id, e.anonymous_id) = m.original_id