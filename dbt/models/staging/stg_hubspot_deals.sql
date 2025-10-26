{{ config(
    materialized='incremental',
    unique_key='deal_id'
) }}


SELECT
    deal_id,
    deal_name,
    contact_id,         
    deal_stage,
    deal_stage_entered,
    deal_closed AS deal_closed_timestamp,
    amount AS revenue_amount,
    CASE 
        WHEN deal_stage = 'closed_won' THEN 1
        ELSE 0
    END AS is_won_deal,
    currency
FROM {{ source('raw', 'hubspot_deals') }}

