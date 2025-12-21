select
    id as event_id,
    'page' as event_type,
    name as event_name,
    timestamp,
    trim(anonymous_id) as anonymous_id,
    trim(id) as user_id, -- Mapping id as user_id for this source
    event_date,
    url,
    path,
    referrer
from {{ source('createsco_dev', 'pages') }}
