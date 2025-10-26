--Join touchpoints from GA4 with hubspot_contacts

WITH touchpoints AS (
    SELECT
        user_id AS ga_client_id,    
        session_id,
        event_timestamp,
        event_name,
        traffic_source_source,
        traffic_source_medium,
        touch_type,
        event_date
    FROM {{ ref('int_marketing_touchpoints') }}
),

contacts AS (
    SELECT
        contact_id,
        ga_client_id,
        email,
        date_created AS contact_created_at,
        first_name,
        last_name
    FROM {{ ref('stg_hubspot_contacts') }}
    WHERE ga_client_id IS NOT NULL
),

-- for each contact get all the associated events from ga4
joined AS (
    SELECT
        c.contact_id,
        c.email,
        c.contact_created_at,
        t.ga_client_id,
        t.session_id,
        t.event_timestamp,
        t.event_name,
        t.traffic_source_source,
        t.traffic_source_medium,
        t.touch_type,
        t.event_date
    FROM contacts c
    INNER JOIN touchpoints t --to remove the customers with no associated ga4 sessions as for those it is infeasible to track their customer journey
    ON c.ga_client_id = t.ga_client_id
    --AND TIMESTAMP_MILLIS(t.event_timestamp) <= c.contact_created_at
)

SELECT
    contact_id,
    email,
    contact_created_at,
    ga_client_id,
    session_id,
    event_date,
    event_name,
    traffic_source_source,
    traffic_source_medium,
    touch_type,
    ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY event_timestamp) AS touchpoint_order
FROM joined
WHERE session_id IS NOT NULL
ORDER BY contact_id, touchpoint_order