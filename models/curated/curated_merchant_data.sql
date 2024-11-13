WITH merchant_signup AS (
    -- Define the 7, 30, 90, 180, 365 day periods after signup for each merchant
    SELECT
        merchant_id,
        sign_up_time,
        sign_up_time + INTERVAL '7 days' AS signup_plus_7_days,
        sign_up_time + INTERVAL '30 days' AS signup_plus_30_days,
        sign_up_time + INTERVAL '90 days' AS signup_plus_90_days,
        sign_up_time + INTERVAL '180 days' AS signup_plus_180_days,
        sign_up_time + INTERVAL '365 days' AS signup_plus_365_days
    FROM {{ ref('merchants') }}
),

-- Checking the output of merchant signup information
-- select * from merchant_signup
successful_transactions AS (
    -- Get successful transactions per merchant
    SELECT
        t.merchant_id,
        t.server_time_created_at,
        row_number() OVER (PARTITION BY t.merchant_id ORDER BY t.server_time_created_at) AS transaction_rank
    FROM {{ ref('transactions') }} t
    WHERE t.tx_result = 11  -- Only successful transactions
),

first_successful_transaction AS (
    -- Get first successful transaction date for each merchant
    SELECT
        st.merchant_id,
        min(st.server_time_created_at) AS first_successful_transaction_date
    FROM successful_transactions st
    WHERE st.transaction_rank = 1
    GROUP BY st.merchant_id
),

-- Checking the output of the first successful transaction
-- select * from first_successful_transaction

successful_card_present_transactions AS (
    SELECT
        t.merchant_id,
        t.server_time_created_at,
        row_number() OVER (PARTITION BY t.merchant_id ORDER BY t.server_time_created_at) AS transaction_rank
    FROM {{ ref('transactions') }} t
    JOIN {{ ref('payment_types') }} pt
        ON t.payment_type_code = pt.code  -- Joining ON the payment type code
    WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
      AND NOT pt.code IN (1,8,9,12,14)  -- Card-not-present transaction codes and 1 for cash payment
),

first_card_present_transaction AS (
    -- Get first card-present transaction date for each merchant
    SELECT
        st.merchant_id,
        min(st.server_time_created_at) AS first_card_present_transaction_date
    FROM successful_card_present_transactions st
    WHERE st.transaction_rank = 1  -- Get the first card-present transaction
    GROUP BY st.merchant_id
),

-- Checking the output of the first card-present transaction
-- select * from first_card_present_transaction

fifth_successful_transaction AS (
    -- Get fifth successful transaction date for each merchant
    SELECT
        st.merchant_id,
        min(st.server_time_created_at) AS fifth_successful_transaction_date
    FROM successful_transactions st
    WHERE st.transaction_rank = 5
    GROUP BY st.merchant_id
),

-- Checking the output of the fifth successful transaction
-- select * from fifth_successful_transaction


last_successful_transaction AS (
    -- Get last successful transaction date for each merchant
    SELECT
        st.merchant_id,
        max(st.server_time_created_at) AS last_successful_transaction_date
    FROM successful_transactions st
    WHERE st.transaction_rank = 1
    GROUP BY st.merchant_id
),

-- Checking the output of the fifth successful transaction
-- select * from last_successful_transaction

all_time_successful_transactions AS (
    -- Get all time successful transaction for each merchant
   SELECT
        t.merchant_id,
        count(*) AS all_time_successful_transactions
    FROM {{ ref('transactions') }} t
    WHERE t.tx_result = 11  -- Successful transaction (tx_result = 11)
    GROUP BY t.merchant_id
),

-- Checking the output of the all time successful transactions
-- select * from all_time_successful_transactions

all_time_failed_transactions AS (
    -- Get all time failed transaction for each merchant
   SELECT
        t.merchant_id,
        count(*) AS all_time_failed_transactions
    FROM {{ ref('transactions') }} t
    WHERE t.tx_result != 11  -- failed transaction (tx_result != 11)
    GROUP BY t.merchant_id
),

-- Checking the output of the all time failed transactions
-- select * from all_time_failed_transactions

transactions_in_period AS (
    -- Join transactions WITH calculated periods, filtering by successful transactions within 365 days of signup
    SELECT
        m.merchant_id,
        t.amount,
        t.vat_amount,
        t.tip_amount,
        t.currency,
        CASE
            WHEN t.server_time_created_at >= m.sign_up_time 
                 AND t.server_time_created_at < m.signup_plus_7_days THEN '7_day'
            WHEN t.server_time_created_at >= m.sign_up_time
                 AND t.server_time_created_at < m.signup_plus_30_days THEN '30_day'
            WHEN t.server_time_created_at >= m.sign_up_time
                 AND t.server_time_created_at < m.signup_plus_90_days THEN '90_day'
            WHEN t.server_time_created_at >= m.sign_up_time
                 AND t.server_time_created_at < m.signup_plus_180_days THEN '180_day'
            WHEN t.server_time_created_at >= m.sign_up_time
                 AND t.server_time_created_at < m.signup_plus_365_days THEN '365_day'
        END AS PERIOD
    FROM merchant_signup m
    JOIN {{ ref('transactions') }} t
        ON m.merchant_id = t.merchant_id
    WHERE t.tx_result = 11  -- Only successful transactions
      AND t.server_time_created_at < m.signup_plus_365_days
),

tpv_summary AS (
    SELECT
        merchant_id,
        sum(CASE WHEN PERIOD = '7_day' THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_7_day,
        sum(CASE WHEN PERIOD IN ('7_day', '30_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_30_day,
        sum(CASE WHEN PERIOD IN ('7_day', '30_day', '90_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_90_day,
        sum(CASE WHEN PERIOD IN ('7_day', '30_day','90_day','180_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_180_day,
        sum(CASE WHEN PERIOD IN ('7_day', '30_day','90_day','180_day','365_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_365_day
    FROM transactions_in_period
    WHERE PERIOD IS NOT NULL
    GROUP BY merchant_id
)

-- Final selection combining all computed metrics

SELECT
    ms.merchant_id,
    ms.sign_up_time,
    fst.first_successful_transaction_date,
    fcpt.first_card_present_transaction_date,
    fifth.fifth_successful_transaction_date,
    lst.last_successful_transaction_date,
    atst.all_time_successful_transactions,
    atft.all_time_failed_transactions,
    tpv.tpv_7_day,
    tpv.tpv_30_day,
    tpv.tpv_90_day,
    tpv.tpv_180_day,
    tpv.tpv_365_day

FROM merchant_signup ms
LEFT JOIN first_successful_transaction fst ON ms.merchant_id = fst.merchant_id
LEFT JOIN first_card_present_transaction fcpt ON ms.merchant_id = fcpt.merchant_id
LEFT JOIN fifth_successful_transaction fifth ON ms.merchant_id = fifth.merchant_id
LEFT JOIN last_successful_transaction lst ON ms.merchant_id = lst.merchant_id
LEFT JOIN all_time_successful_transactions atst ON ms.merchant_id = atst.merchant_id
LEFT JOIN all_time_failed_transactions atft ON ms.merchant_id = atft.merchant_id
LEFT JOIN tpv_summary tpv ON ms.merchant_id = tpv.merchant_id

ORDER BY sign_up_time
