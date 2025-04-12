WITH successful_card_present_transactions AS (
    SELECT
        t.merchant_id,
        t.server_time_created_at,
        row_number() OVER (PARTITION BY t.merchant_id ORDER BY t.server_time_created_at) AS transaction_rank
    FROM "merchant_data"."public"."transactions" t
    JOIN "merchant_data"."public"."payment_types" pt
        ON t.payment_type_code = pt.code  -- Joining on the payment type code
    WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
      AND NOT pt.code IN (8,9,12,14)  -- Card-not-present transaction codes
)
SELECT
    st.merchant_id,
    min(st.server_time_created_at) AS first_card_present_transaction_date
FROM successful_card_present_transactions st
WHERE st.transaction_rank = 1  -- Get the first successful card-present transaction
GROUP BY st.merchant_id