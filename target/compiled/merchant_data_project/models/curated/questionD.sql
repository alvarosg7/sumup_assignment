WITH successful_transactions AS (
    SELECT
        t.merchant_id,
        t.server_time_created_at,
        row_number() OVER (PARTITION BY t.merchant_id ORDER BY t.server_time_created_at) AS transaction_rank
    FROM "merchant_data"."public"."transactions" t
    WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
)
SELECT
    st.merchant_id,
    min(st.server_time_created_at) AS fifth_successful_transaction_date
FROM successful_transactions st
WHERE st.transaction_rank = 5  -- Get the fifth successful transaction for each merchant
GROUP BY st.merchant_id