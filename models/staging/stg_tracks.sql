with createsco_tracks as (
    select
        id as event_id,
        'track' as event_type,
        event as event_name,
        timestamp,
        anonymous_id,
        event_date,
        user_id,
        null as context_library_name,
        null as context_library_version,
        context_traits_interested_services_weighted as interested_services_weighted
    from {{ source('createsco_dev', 'tracks') }}
)


select
    event_id,
    event_type,
    event_name,
    interested_services_weighted,
    timestamp,
    trim(anonymous_id) as anonymous_id,
    event_date,
    trim(user_id) as user_id,
    context_library_name,
    context_library_version
from createsco_tracks
