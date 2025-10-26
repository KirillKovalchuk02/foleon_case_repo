WITH ordered AS (
    SELECT
        deal_id,
        contact_id,
        touchpoint_id,
        touch_type,
        ad_platform,
        campaign_id,
        campaign_name,
        traffic_source_medium,
        event_date,
        revenue_amount,
        total_cost,
        cost_per_click,
        ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY event_date ASC) AS touch_order,
        COUNT(*) OVER (PARTITION BY deal_id) AS total_touches
    FROM FROM {{ ref('int_full_joined') }}
),