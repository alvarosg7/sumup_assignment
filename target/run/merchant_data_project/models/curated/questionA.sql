
  create view "merchant_data"."public"."questionA__dbt_tmp"
    
    
  as (
    SELECT
    m.merchant_id AS merchant_id,
    m.sign_up_time
FROM "merchant_data"."public"."merchants" m
  );