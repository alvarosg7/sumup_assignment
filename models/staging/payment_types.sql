WITH source AS (
    SELECT * FROM {{ source('merchant_data', 'payment_types') }}
)
SELECT
    id::INTEGER AS id,
    code::INTEGER AS code,
    description::VARCHAR(255) AS description,
    is_active::BOOLEAN AS is_active,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at
   
FROM source