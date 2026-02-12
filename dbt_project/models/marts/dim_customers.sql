{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_transactions AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        MIN(transaction_date) AS first_order_date,
        MAX(transaction_date) AS last_order_date,
        AVG(total_amount) AS avg_order_value
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.city,
    c.state,
    c.country,
    c.customer_segment,
    c.registration_date,
    -- Transaction Metrics
    COALESCE(t.total_orders, 0) AS total_orders,
    COALESCE(t.lifetime_value, 0) AS lifetime_value,
    COALESCE(t.avg_order_value, 0) AS avg_order_value,
    t.first_order_date,
    t.last_order_date,

    CASE 
        WHEN t.total_orders IS NULL THEN 'Never Purchased'
        WHEN t.total_orders = 1 THEN 'One Time'
        WHEN t.total_orders <=5 THEN 'Occasional'
        ELSE 'Frequent'
    END AS purchase_frequency,

    CASE
        WHEN t.last_order_date >= current_date - interval '30 days' THEN 'Active'
        WHEN t.last_order_date >= current_date - interval '90 days' THEN 'At Risk'
        WHEN t.last_order_date IS NOT NULL THEN 'Churned'
        ELSE 'Never Purchased'
    END AS customer_status

FROM customers c
LEFT JOIN customer_transactions t
    ON c.customer_id = t.customer_id
