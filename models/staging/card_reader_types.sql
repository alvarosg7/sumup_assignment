WITH source AS (
    SELECT * FROM {{ source('merchant_data', 'card_reader_types') }}
)
    SELECT
        id::INTEGER AS id,
        name::VARCHAR(255) AS name,
        model::VARCHAR(255) AS model,
        pin_entry_capability::INTEGER AS pin_entry_capability,
        pin_block_format_code::INTEGER AS pin_block_format_code,
        encryption::BOOLEAN AS encryption,
        color::VARCHAR(50) AS color,
        description::TEXT AS description,
        is_emv_capable::BOOLEAN AS is_emv_capable,
        src_created_at::TIMESTAMPTZ AS src_created_at,
        src_updated_at::TIMESTAMPTZ AS src_updated_at,
        model_type::VARCHAR(50) AS model_type,
        created_at::TIMESTAMPTZ AS created_at,
        updated_at::TIMESTAMPTZ AS updated_at

    FROM source