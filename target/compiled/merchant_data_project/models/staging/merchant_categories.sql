WITH source AS (
    SELECT * FROM "merchant_data"."public"."merchant_categories"
)
SELECT
    id::INTEGER AS id,
    code::INTEGER AS code,
    description::VARCHAR(255) AS description,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    section_id::INTEGER AS section_id,
    vat_inclusive::BOOLEAN AS vat_inclusive,
    enabled::BOOLEAN AS enabled,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at
   
FROM source