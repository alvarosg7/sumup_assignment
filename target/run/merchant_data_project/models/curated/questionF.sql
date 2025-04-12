
  create view "merchant_data"."public"."questionF__dbt_tmp"
    
    
  as (
    SELECT
    t.merchant_id,
    count(*) AS all_time_successful_transactions
FROM "merchant_data"."public"."transactions" t
WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
GROUP BY t.merchant_id
  );