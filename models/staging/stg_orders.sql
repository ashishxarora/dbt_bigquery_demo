{{ config(
    materialized='view',
    schema='demo_staging'
) }}

SELECT 
    order_id,
    customer_id,
    customer_email,
    ordered_at,
    status,
    subtotal_usd,
    shipping_usd,
    tax_usd,
    total_usd,
    attribution_channel
FROM {{ source('seed', 'orders') }}