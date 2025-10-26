-- all the events
WITH base_events AS (
    SELECT
        event_id,
        user_id,
        session_id,
        event_name,
        event_type,
        event_timestamp,
        page_location,
        traffic_source_source,
        traffic_source_medium,
        campaign_id,
        ga_client_id
    FROM {{ ref('stg_ga4_events') }}
    -- WHERE event_name IN (
    --     'session_start',
    --     'page_view',
    --     'form_submit',
    --     'generate_lead',
    --     'purchase',
    --     'request_quote'
    -- )
),

-- apply a touchpoint and source of traffic classification 
classified_touches AS (
    SELECT
        event_id,
        user_id,
        session_id,
        event_name,
        event_type,
        event_timestamp,
        page_location,
        traffic_source_source,
        traffic_source_medium,
        campaign_id,
        ga_client_id,

        -- source type
        CASE
            WHEN traffic_source_medium IN ('cpc', 'paid_social') THEN 'paid'
            WHEN traffic_source_medium IN ('organic', 'referral', 'email') THEN 'organic'
            ELSE 'other'
        END AS source_type,

        -- categorize the events -> to be extended, just for showcase purposes
        CASE
            WHEN event_name = 'session_start' THEN 'landing'
            WHEN event_name IN ('form_submit','generate_lead','request_quote') THEN 'conversion'
            WHEN event_name = 'purchase' THEN 'purchase'
            WHEN event_name = 'page_view' THEN 'engagement'
            ELSE 'other'
        END AS touch_category
    FROM base_events
),

--order events within sessions
ordered_touches AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) AS touch_order
    FROM classified_touches
)

SELECT
    event_id,
    user_id,
    session_id,
    ga_client_id,
    event_name,
    event_type,
    event_timestamp,
    page_location,
    traffic_source_source,
    traffic_source_medium,
    campaign_id,
    source_type,
    touch_category,
    touch_order
FROM ordered_touches
ORDER BY user_id, session_id, event_timestamp
