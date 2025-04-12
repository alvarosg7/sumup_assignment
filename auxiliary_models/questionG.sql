SELECT
    t.merchant_id,
    count(*) AS all_time_failed_transactions
FROM {{ ref('transactions') }} t
WHERE t.tx_result != 11  -- failed transaction (tx_result != 11)
GROUP BY t.merchant_id