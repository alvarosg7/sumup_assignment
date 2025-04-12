WITH source AS (
    SELECT * FROM "merchant_data"."public"."legal_types"
)
SELECT
    id::INTEGER AS id,
    description::VARCHAR(255) AS description,
    short_description::VARCHAR(255) AS short_description,
    country_id::INTEGER AS country_id,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    active::BOOLEAN AS active,
    signup_screen::INTEGER AS signup_screen,
    translation_key::VARCHAR(255) AS translation_key,
    parent_id::INTEGER AS parent_id,
    sort_order::INTEGER AS sort_order,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at

FROM source