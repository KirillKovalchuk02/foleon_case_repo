{{ config(
    materialized='incremental',
    unique_key='contact_id'
) }}

WITH raw_contacts AS (
    SELECT *
    FROM {{ source('raw', 'hubspot_contacts') }}
)

SELECT
    contact_id,
    email,
    first_name,
    last_name,
    date_created,
    lifecycle_stage,
    ga_user_id,  -- used for joining with GA4 events
    created_at AS contact_created_timestamp
FROM raw_contacts

