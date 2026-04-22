{{ config(
    materialized='view',
    schema='demo_staging'
) }}

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    signup_date,
    cohort_month,
    acquisition_channel
FROM {{ source('seed', 'customers') }}