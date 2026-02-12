{{ config(materialized='table') }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    t.transaction_id,
    t.customer_id,
    t.product_id,

    t.transaction_date,
    CAST(t.transaction_date AS DATE) AS transaction_date,
    DATE_TRUNC('week', t.transaction_date) AS transaction_week,
    DATE_TRUNC('month', t.transaction_date) AS transaction_month,
    YEAR(t.transaction_date) AS transaction_year,
    MONTH(t.transaction_date) AS transaction_month_num,
    DAYOFWEEK(t.transaction_date) AS transaction_day_of_week,

    t.quantity,
    t.unit_price,
    t.total_amount,

    c.full_name AS customer_name,
    c.customer_segment,
    c.city AS customer_city,
    c.state AS customer_state,

    p.product_name,
    p.product_category,

    t.total_amount - (t.quantity * t.unit_price * 0.7) AS estimated_profit,
    ROUND(t.total_amount / NULLIF(t.quantity, 0), 2) AS revenue_per_unit
FROM transactions t 
LEFT JOIN customers c 
    ON t.customer_id = c.customer_id
LEFT JOIN products p
    ON t.product_id = p.product_id
