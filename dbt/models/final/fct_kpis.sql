WITH campaign_kpis AS (
    SELECT
        ad_platform,
        campaign_name,
        
        SUM(revenue_attributed) AS total_attributed_revenue,
        COUNT(DISTINCT deal_id) AS deals_influenced, -- number of unique deals a campaign contributed to
        SUM(revenue_amount) AS total_influenced_revenue, -- sum of the full deal values influenced by campaign

        MAX(total_cost_campaign) AS total_campaign_cost,
        COUNT(DISTINCT contact_id) AS customers_acquired  
    FROM {{ ref('fct_revenue_attribution') }}
    WHERE ad_platform IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    *,
    SAFE_DIVIDE(total_attributed_revenue, total_campaign_cost) AS return_on_ad_spend,  --roas
    SAFE_DIVIDE(total_campaign_cost, customers_acquired) AS cost_per_acquisition, 
    SAFE_DIVIDE(total_influenced_revenue, deals_influenced) AS average_order_value_per_campaign
    --can add more metrics here if needed

FROM campaign_kpis


