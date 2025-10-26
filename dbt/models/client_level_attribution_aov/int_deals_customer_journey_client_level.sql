--Join contacts+touchpoints with deals

WITH deals AS (
    SELECT
        contact_id,
        deal_id,
        deal_name,
        --deal_stage,
        --deal_stage_entered,
        deal_closed,
        revenue_amount,
        currency,
        is_won_deal
        ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY deal_closed) AS deal_rank
    FROM {{ ref('stg_hubspot_deals') }}
    WHERE is_won_deal = 1
),

-- in this version, we get all the deals associated with the client, not just the first one
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
        d.deal_stage,
        d.deal_stage_entered,
        d.deal_closed,
        d.deal_rank,
        d.revenue_amount,
        d.currency
    FROM contact_journeys cj {{ ref('int_touchpoint_contact_stitch') }}
    LEFT JOIN deals d
        ON cj.contact_id = d.contact_id
        AND cj.event_date <= d.deal_closed
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
    deal_stage,
    deal_stage_entered,
    deal_closed,
    revenue_amount,
    currency,
    is_won_deal
FROM joined
WHERE deal_id IS NOT NULL
ORDER BY contact_id, touchpoint_order