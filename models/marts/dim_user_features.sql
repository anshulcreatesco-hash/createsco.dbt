select
    blended_user_id,
    count(*) as total_interactions,
    array_agg(distinct event_name) as distinct_actions,
    max_by(interested_services_weighted, timestamp) 
        as interested_services_weighted
    -- Example: Get the last 3 categories they viewed for the recommender
    -- properties column is not currently preserved in fct_unified_events
    -- collect_list(properties:category) as viewed_categories
from {{ ref('fct_unified_events') }}
group by blended_user_id