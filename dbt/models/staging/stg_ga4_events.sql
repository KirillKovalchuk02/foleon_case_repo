{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

SELECT
    event_id, --
    user_id, --
    session_id, --
    event_name, --
    event_type,
    event_timestamp, --
    page_location,
    traffic_source_source, --
    traffic_source_medium, --
    campaign_id, 
FROM {{ source('raw', 'ga4_events') }}

