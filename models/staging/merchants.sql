WITH source AS (
    SELECT * FROM {{ source('merchant_data', 'merchants') }}
)
SELECT
    merchant_id::BIGINT AS merchant_id,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    primary_user_id::BIGINT AS primary_user_id,
    country_id::INTEGER AS country_id,
    merchant_category_id::INTEGER AS merchant_category_id,
    sign_up_time::TIMESTAMPTZ AS sign_up_time,
    is_test_account::BOOLEAN AS is_test_account,
    address_detail_id::BIGINT AS address_detail_id,
    business_detail_id::BIGINT AS business_detail_id,
    payout_zone_id::INTEGER AS payout_zone_id,
    payout_detail_id::INTEGER AS payout_detail_id,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at
   
FROM source