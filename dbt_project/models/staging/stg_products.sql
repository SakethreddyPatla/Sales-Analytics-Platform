WITH source AS (
    SELECT * FROM raw.products
),

cleaned AS (
    SELECT
        id AS product_id,
        title AS product_name,
        category AS product_category,
        description AS product_description,
        CAST(price as decimal(10,2)) AS unit_price,
        CAST(json_extract(rating, '$.rate') AS decimal(3,2)) AS rating_score,
        CAST(json_extract(rating, '$.count') AS integer) AS rating_count,
        image AS product_image,
        extracted_at AS loaded_at
    FROM source
)

SELECT * FROM cleaned