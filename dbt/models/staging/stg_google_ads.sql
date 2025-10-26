{{ config(materialized='table') }}

SELECT
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    cost / clicks AS cpc,
    'USD' AS currency 
FROM {{ source('raw', 'google_ads') }}