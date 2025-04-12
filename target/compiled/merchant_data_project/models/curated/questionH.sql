WITH merchant_signup AS (
    -- Define the 7,30,90,180,365 day periods after signup for each merchant
    SELECT
        merchant_id,
        sign_up_time,
        sign_up_time + INTERVAL '7 days' AS signup_plus_7_days,
        sign_up_time + INTERVAL '30 days' AS signup_plus_30_days,
        sign_up_time + INTERVAL '90 days' AS signup_plus_90_days,
        sign_up_time + INTERVAL '180 days' AS signup_plus_180_days,
        sign_up_time + INTERVAL '365 days' AS signup_plus_365_days
    FROM "merchant_data"."public"."merchants"
),

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
    JOIN "merchant_data"."public"."transactions" t
        ON m.merchant_id = t.merchant_id
    WHERE t.tx_result = 11  -- Only successful transactions
      AND t.server_time_created_at < m.signup_plus_365_days
)

-- Final aggregation, calculating TPV for both 7-day AND 30-day periods
SELECT
    merchant_id,
    currency,
    sum(CASE WHEN PERIOD = '7_day' THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_7_day,
    sum(CASE WHEN PERIOD IN ('7_day', '30_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_30_day,
    sum(CASE WHEN PERIOD IN ('7_day', '30_day', '90_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_90_day,
    sum(CASE WHEN PERIOD IN ('7_day', '30_day','90_day','180_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_180_day,
    sum(CASE WHEN PERIOD IN ('7_day', '30_day','90_day','180_day','365_day') THEN amount + vat_amount + tip_amount ELSE 0 END) AS tpv_365_day
FROM transactions_in_period
WHERE PERIOD IS NOT NULL  -- Only transactions within 7 or 30 days of signup
GROUP BY merchant_id, currency
ORDER BY merchant_id;