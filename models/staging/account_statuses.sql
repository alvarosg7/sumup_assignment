
WITH source AS (
    SELECT * from {{ source('merchant_data', 'account_statuses') }}
)
    SELECT
         id::INTEGER AS id,
         status::VARCHAR(50) AS status,
         description::TEXT AS description,
         src_created_at::TIMESTAMPTZ AS src_created_at,
         src_updated_at::TIMESTAMPTZ AS src_updated_at,
         created_at::TIMESTAMPTZ AS created_at,
         updated_at::TIMESTAMPTZ AS updated_at

    FROM source