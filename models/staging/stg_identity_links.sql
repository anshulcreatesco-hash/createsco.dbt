with tracks_links as (
    select
        anonymous_id,
        user_id,
        timestamp as linkage_timestamp
    from {{ ref('stg_tracks') }}
    where anonymous_id is not null 
      and user_id is not null
),

pages_links as (
    select
        anonymous_id,
        user_id,
        timestamp as linkage_timestamp
    from {{ ref('stg_pages') }}
    where anonymous_id is not null 
      and user_id is not null
),

unioned as (
    select * from tracks_links
    union all
    select * from pages_links
)

select distinct
    anonymous_id,
    user_id,
    linkage_timestamp
from unioned