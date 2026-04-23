{{ config(
    materialized='table',
    schema='demo_mart'
) }}

-- Customer Cohort Revenue Analysis
-- This model calculates revenue by customer cohort

WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.state,
        c.signup_date,
        c.cohort_month,
        c.acquisition_channel,
        o.order_id,
        o.ordered_at,
        o.status,
        o.total_usd AS revenue
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o 
        ON c.customer_id = o.customer_id
    WHERE o.status = 'completed'
),

cohort_revenue AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) AS customer_count,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(revenue) AS total_revenue,
        AVG(revenue) AS avg_order_value
    FROM customer_orders
    GROUP BY cohort_month
)

SELECT 
    cr.cohort_month,
    cr.customer_count,
    cr.order_count,
    cr.total_revenue,
    cr.avg_order_value,
    -- Additional cohort details
    MIN(cr.cohort_month) AS min_cohort,
    MAX(cr.cohort_month) AS max_cohort
FROM cohort_revenue cr
GROUP BY 
    cr.cohort_month,
    cr.customer_count,
    cr.order_count,
    cr.total_revenue,
    cr.avg_order_value
ORDER BY cr.cohort_month DESC