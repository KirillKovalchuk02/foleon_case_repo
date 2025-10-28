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
    FROM {{ ref('int_deals_customer_journey') }}
),

filtered_touchpoints AS (
    SELECT *
    FROM ordered
    WHERE touch_category IN ('landing', 'conversion', 'engagement') -- The filter to choose the events that are considered touches -> to be discussed with the team
    --or WHERE event_name IN ('', '', '')
)


revenue_attribution AS (
    SELECT
        t.*,
        COUNT(*) OVER (PARTITION BY t.deal_id) AS num_touches, 
        SAFE_DIVIDE(t.revenue_amount, COUNT(*) OVER (PARTITION BY t.deal_id)) AS revenue_attributed  --assuming linear attribution for simplicity
    FROM filtered_touchpoints t
)

SELECT *
FROM revenue_attribution



