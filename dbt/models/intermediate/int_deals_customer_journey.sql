-- 1. Identify all winning deals and determine the start date for the attribution window of the current deal.
WITH deals_with_window_start AS (
    SELECT
        contact_id,
        deal_id,
        deal_name,
        -- deal_stage,
        -- deal_stage_entered,
        deal_closed,
        revenue_amount,
        currency,
        is_won_deal,
        ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY deal_closed) AS deal_rank,

        LAG(deal_closed, 1) OVER (
            PARTITION BY contact_id
            ORDER BY deal_closed
        ) AS previous_deal_close_date
    FROM {{ ref('stg_hubspot_deals') }}
    WHERE is_won_deal = 1 and revenue_amount IS NOT NULL
),

-- 2. Join touchpoints to deals, applying the strict, sequenced attribution window logic.
joined AS (
    SELECT
        cj.contact_id,
        cj.email,
        cj.ga_client_id,
        cj.session_id,
        cj.event_date,
        cj.event_name,
        cj.traffic_source_source,
        cj.traffic_source_medium,
        cj.touch_type,
        cj.touchpoint_order,
        d.deal_id,
        d.deal_name,
        -- d.deal_stage, 
        -- d.deal_stage_entered,
        d.deal_closed,
        d.revenue_amount,
        d.currency,
        d.is_won_deal
    FROM {{ ref('int_touchpoint_contact_stitch') }} cj
    INNER JOIN deals_with_window_start d
        ON cj.contact_id = d.contact_id

    -- A touchpoint is relevant to deal 'd' only if:
    -- 1. It occurred AFTER the previous deal closed (d.previous_deal_close_date).
    -- 2. It occurred BEFORE or AT the current deal closed (d.deal_closed).
    -- plus the 90 days lookback window 
    WHERE 
        cj.event_date > d.previous_deal_close_date
        AND cj.event_date <= d.deal_closed
        AND cj.event_date >= DATE_SUB(d.deal_closed, INTERVAL 90 DAY)
)

SELECT
    contact_id,
    email,
    ga_client_id,
    session_id,
    event_date,
    event_name,
    traffic_source_source,
    traffic_source_medium,
    touch_type,
    touchpoint_order,
    deal_id,
    deal_name,
    deal_closed,
    revenue_amount,
    currency
FROM joined
ORDER BY contact_id, deal_closed, touchpoint_order


