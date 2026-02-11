WITH source AS (
    SELECT * FROM raw.customers
),

cleaned AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name AS full_name,
        email,
        phone,
        address AS street_address,
        city,
        state,
        zip_code,
        country,
        CAST(registration_date AS date) AS registration_date,
        customer_segment,
        created_at AS loaded_at
    FROM source
)
SELECT * FROM cleaned