WITH source AS (
    SELECT * FROM "merchant_data"."public"."transaction_statuses"
)
SELECT
    id::INTEGER AS id,
    name::VARCHAR(255) AS name,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at
    
FROM source