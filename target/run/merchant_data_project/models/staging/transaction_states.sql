
  create view "merchant_data"."public"."transaction_states__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "merchant_data"."public"."transaction_states"
)
SELECT
    id::BIGINT AS id,
    transaction_id::BIGINT AS transaction_id,
    insert_time::TIMESTAMPTZ AS insert_time,
    settlement_run_id::BIGINT AS settlement_run_id,
    src_created_at::TIMESTAMPTZ AS src_created_at,
    src_updated_at::TIMESTAMPTZ AS src_updated_at,
    comment::TEXT AS comment,
    payout_event_id::BIGINT AS payout_event_id,
    transaction_status::INTEGER AS transaction_status,
    transaction_status_id::INTEGER AS transaction_status_id,
    created_at::TIMESTAMPTZ AS created_at,
    updated_at::TIMESTAMPTZ AS updated_at
   
FROM source
  );