-- Reorganize the GA4 raw table

WITH base events AS(
    SELECT *
    FROM {{ ref('stg_ga4_events') }}
),


session_landing AS (
    SELECT
        CONCAT(session_id, '_landing') AS event_id,
        user_id,
        session_id,
        'landing_on_website' AS event_name,
        'session_start' AS event_type,
        MIN(event_timestamp) AS event_timestamp,
        NULL AS page_location,
        traffic_source_source,
        traffic_source_medium,
        campaign_id,
        ga_client_id,
        CASE
            WHEN traffic_source_medium IN ('cpc', 'paid_social') THEN 'paid'
            ELSE 'organic'
        END AS touch_type,
        'landing' AS touch_category
    FROM base_events
    GROUP BY
        session_id, user_id, traffic_source_source, traffic_source_medium, campaign_id, ga_client_id
),

real_events AS (
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
        -- categorize touches
        CASE
            WHEN event_type IN ('page_view','form_submit','quote_request') THEN 'website'
            WHEN traffic_source_medium IN ('cpc', 'paid_social') THEN 'paid'
            ELSE 'other'
        END AS touch_type,
        CASE
            WHEN event_type IN ('page_view','form_submit','quote_request') THEN 'website'
            ELSE 'other'
        END AS touch_category
    FROM base_events
    WHERE event_type IN ('page_view','form_submit','quote_request','session_start')
),


all_touches AS (
    SELECT * FROM session_landing
    UNION ALL
    SELECT * FROM real_events
)

-- Order touches chronologically
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) AS touch_order
FROM all_touches
ORDER BY user_id, session_id, event_timestamp