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
    FROM {{ ref('int_full_joined') }}
),

revenue_attribution AS (
    SELECT
        t.*,
        COUNT(*) OVER (PARTITION BY t.deal_id) AS num_touches, 
        SAFE_DIVIDE(t.revenue_amount, COUNT(*) OVER (PARTITION BY t.deal_id)) AS revenue_attributed  --assuming linear attribution for simplicity
    FROM ordered t
),

total_aov_calculation AS(
    SELECT contact_id,
            COUNT(deal_id) as number_of_deals,
            SUM(revenue_amount) as total_revenue,
            SUM(revenue_amount) / COUNT(deal_id) as total_aov
    FROM revenue_attribution
    GROUP BY 1
)


SELECT ra.*,
        aov.total_aov,
CASE WHEN ad_platform IS NULL 
    THEN 1 
    ELSE 0 
END AS is_offline               -- offline-based deals flag
FROM revenue_attribution ra
LEFT JOIN total_aov_calculation aov
ON ra.contact_id = aov.contact_id



