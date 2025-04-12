SELECT
    m.merchant_id AS merchant_id,
    m.sign_up_time
FROM {{ ref('merchants') }} m