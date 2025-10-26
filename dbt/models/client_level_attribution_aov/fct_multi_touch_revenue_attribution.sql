WITH ordered AS (
    SELECT
        contact_id,
        deal_id,
        deal_rank,
        revenue_amount,
        currency,

        ga_client_id,
        session_id,
        event_date,
        event_name,
        traffic_source_source,
        traffic_source_medium,
        source_type,
        touch_category,
        touchpoint_order,
        ad_platform,

        campaign_id,
        campaign_name,
        total_cost_campaign,
        cost_per_click
    FROM {{ ref('int_full_joined') }}
),


touch_counts AS (
    SELECT
        deal_id,
        COUNT(*) AS num_touches
    FROM touches
    GROUP BY deal_id
    HAVING deal_rank = 1
),

filtered_touchpoints AS (
    SELECT *
    FROM ordered
    WHERE touch_category IN ('landing', 'conversion', 'engagement') -- The filter to choose the events that are considered touches -> to be discussed with the team
    --or WHERE event_name IN ('', '', '')
),

deals_aggregated_revenue (
    SELECT
        contact_id,
        -- deal_id,
        -- deal_rank,
        SUM(revenue_amount) AS lifetime_revenue,
        currency,

        ga_client_id,
        session_id,
        event_date,
        event_name,
        traffic_source_source,
        traffic_source_medium,
        source_type,
        touch_category,
        touchpoint_order,
        ad_platform,

        campaign_id,
        campaign_name,
        total_cost_campaign,
        cost_per_click
    FROM filtered_touchpoints
    GROUP BY
)

revenue_attribution AS (
    SELECT
        t.*,
        tc.num_touches,
        SAFE_DIVIDE(t.revenue_amount, tc.num_touches) AS revenue_attributed
    FROM filtered_touchpoints t
    LEFT JOIN touch_counts tc
        ON t.deal_id = tc.deal_id
),

touch_costs AS (
    SELECT
        *,
        CASE 
            WHEN source_type = 'paid' AND clicks IS NOT NULL AND clicks > 0 
            THEN cost_per_click 
            ELSE 0 
        END AS touch_cost,
        CASE 
            WHEN source_type = 'paid' AND clicks IS NOT NULL AND clicks > 0 
            THEN SAFE_DIVIDE(revenue_attributed, cost_per_click)
            ELSE NULL
        END AS roas
    FROM revenue_allocation
)

SELECT
    contact_id,
    deal_id,
    ga_client_id,
    session_id,
    event_date,
    event_name,
    traffic_source_source,
    traffic_source_medium,
    source_type,
    touchpoint_order,
    ad_platform,
    campaign_id,
    campaign_name,
    revenue_amount,
    currency,
    num_touches,
    revenue_attributed,
    total_cost_campaign,
    touch_cost,
    roas
FROM touch_costs
ORDER BY deal_id, touchpoint_order