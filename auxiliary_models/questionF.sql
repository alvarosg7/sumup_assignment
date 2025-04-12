SELECT
    t.merchant_id,
    count(*) AS all_time_successful_transactions
FROM {{ ref('transactions') }} t
WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
GROUP BY t.merchant_id


