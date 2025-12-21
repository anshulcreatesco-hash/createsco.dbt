select
    blended_user_id,
    count(*) as total_interactions,
    array_agg(distinct event_name) as distinct_actions,
    -- Example: Get the last 3 categories they viewed for the recommender
    collect_list(properties:category) as viewed_categories
from {{ ref('fct_unified_events') }}
group by 1