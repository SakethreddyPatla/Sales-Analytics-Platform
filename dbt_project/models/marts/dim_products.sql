{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_transactions AS (
    SELECT
        product_id,
        COUNT(*) AS total_orders,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM {{ ref('stg_transactions') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_image,
    p.product_category,
    p.product_description,
    p.unit_price,
    COALESCE(t.total_orders, 0) AS total_orders,
    COALESCE(t.total_units_sold, 0) AS total_units_sold,
    COALESCE(t.total_revenue, 0) AS total_revenue,
    COALESCE(t.avg_order_value, 0) AS avg_order_value,

    CASE
        WHEN COALESCE(t.total_revenue, 0) = 0 THEN 'No Sales'
        WHEN t.total_revenue >= 5000 THEN 'Top Performer'
        WHEN t.total_revenue >= 1000 THEN 'Mid Performer'
        ELSE 'Low Performer'
    END AS performance_tier,

    CASE
        WHEN unit_price < 20 THEN 'Budget'
        WHEN unit_price < 100 THEN 'Mid Range'
        WHEN unit_price < 500 THEN 'Premium'
        ELSE 'Luxury'
    END AS price_tier
FROM products p
LEFT JOIN product_transactions t 
    ON p.product_id = t.product_id
