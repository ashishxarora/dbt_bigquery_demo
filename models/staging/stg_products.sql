{{ config(
    materialized='view',
    schema='demo_staging'
) }}

SELECT 
    product_id,
    product_name,
    category,
    list_price as price_usd,
    unit_cost,
    status
FROM {{ source('seed', 'products') }}