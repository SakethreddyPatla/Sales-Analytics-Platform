WITH source AS (
    SELECT * FROM raw.transactions
),

cleaned AS (
    SELECT
        transaction_id,
        CAST(transaction_date AS date) AS transaction_date,
        CAST(transaction_date AS timestamp) AS transaction_timestamp,
        product_id,
        customer_id,
        quantity,
        CAST(unit_price as decimal(10,2)) AS unit_price, 
        CAST(total_amount as decimal(10,2)) AS total_amount,
        product_title as product_name,
        category AS product_category,
        extracted_at AS loaded_at
    FROM source
)
SELECT * FROM cleaned