
  create view "merchant_data"."public"."transactions__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "merchant_data"."public"."transactions"
)
SELECT
    transaction_id::BIGINT AS transaction_id,
    merchant_id::BIGINT AS merchant_id,
    user_id::BIGINT AS user_id,
    server_time_created_at::TIMESTAMPTZ AS server_time_created_at,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at,
    amount::DECIMAL(10, 2) AS amount,
    vat_amount::DECIMAL(10, 2) AS vat_amount,
    tip_amount::DECIMAL(10, 2) AS tip_amount,
    payment_type_code::INTEGER AS payment_type_code,
    currency::CHAR(3) AS currency,
    entry_mode_id::INTEGER AS entry_mode_id,
    tx_result::INTEGER AS tx_result,
    acquirer_code::INTEGER AS acquirer_code,
    current_status_id::INTEGER AS current_status_id,
    device_id::BIGINT AS device_id,
    cardholder_verification_method_id::INTEGER AS cardholder_verification_method_id,
    card_id::BIGINT AS card_id,
    card_reader_id::BIGINT AS card_reader_id,
    tax_enabled::BOOLEAN AS tax_enabled
   
FROM source
  );