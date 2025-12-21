with all_identifiers as (
    select distinct anonymous_id as id from {{ ref('stg_tracks') }}
    union
    select distinct user_id as id from {{ ref('stg_tracks') }} where user_id is not null
    union
    select distinct anonymous_id as id from {{ ref('stg_pages') }}
    union
    select distinct user_id as id from {{ ref('stg_pages') }} where user_id is not null
),

links as (
    select * from {{ ref('stg_identity_links') }}
),

-- Map each anonymous_id to its most recent user_id
anon_to_user as (
    select
        anonymous_id,
        max_by(user_id, linkage_timestamp) as master_user_id
    from links
    group by 1
)

select
    ids.id as original_id,
    coalesce(mapping.master_user_id, ids.id) as stitched_id
from all_identifiers ids
left join anon_to_user mapping
    on ids.id = mapping.anonymous_id