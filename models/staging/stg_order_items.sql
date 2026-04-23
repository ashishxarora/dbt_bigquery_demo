{{ config(
    materialized='view',
    schema='demo_staging'
) }}

SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_usd,
    line_total_usd
FROM {{ source('seed', 'order_items') }}