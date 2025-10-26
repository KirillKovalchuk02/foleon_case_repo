--Join contact+touchpoints+deals with cost data

WITH deals_journey AS (
    SELECT
        contact_id,
        deal_id,
        deal_name,
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
        deal_rank
    FROM {{ ref('int_deals_customer_journey') }}
    WHERE event_date BETWEEN DATE_SUB(deal_closed, INTERVAL 90 DAY) AND deal_closed --lookback window
),

google_ads AS (
    SELECT
        campaign_id,
        campaign_name,
        cost AS total_cost,
        clicks,
        --impressions,
        SAFE_DIVIDE(cost, clicks) AS cost_per_click,
        'google_ads' AS ad_platform
    FROM {{ ref('stg_google_ads') }}
),

linkedin_ads AS (
    SELECT
        campaign_id,
        campaign_name,
        cost AS total_cost,
        clicks,
        --impressions,
        SAFE_DIVIDE(cost, clicks) AS cost_per_click,
        'linkedin_ads' AS ad_platform
    FROM {{ ref('stg_linkedin_ads') }}
),

ads_union AS (
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM linkedin_ads
),


SELECT
    dj.contact_id,
    dj.deal_id,
    dj.revenue_amount,
    dj.currency,
    dj.ga_client_id,
    dj.session_id,
    dj.event_date,
    dj.event_name,
    dj.traffic_source_source,
    dj.traffic_source_medium,
    dj.source_type,
    dj.touch_category,
    dj.touchpoint_order,
    dj.deal_rank,
    au.ad_platform,
    au.campaign_id,
    au.campaign_name,
    au.total_cost as total_cost_campaign,
    au.cost_per_click
FROM deals_journey AS dj
LEFT JOIN ads_union AS au
    ON LOWER(dj.traffic_source_source) LIKE CONCAT('%', LOWER(au.campaign_name), '%') --this is an example of a join logic, to be adjusted for the actual data
