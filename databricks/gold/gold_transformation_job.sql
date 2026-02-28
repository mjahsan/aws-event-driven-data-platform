-- DIM_USERS
-- Extracting only the latest snapshot of the user
-- Per row per user
-- Grain can be altered if needed 
INSERT OVERWRITE TABLE demo_catalog.gold.dim_users
SELECT
    user_id,
    email,
    country
FROM (
    SELECT
        user_id,
        email,
        country,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM demo_catalog.silver.user_events
)
WHERE rn = 1;

-- FACT_PAYMENTS
-- Per row per payment attempt
INSERT OVERWRITE TABLE demo_catalog.gold.fact_payments
SELECT
    payment_id,
    order_id,
    CAST(event_ts AS DATE) AS payment_date,
    amount AS payment_amount,
    currency,
    status AS payment_status
FROM demo_catalog.silver.payment_events;

-- FACT_ORDERS
-- Per row per order
WITH latest_order AS (
    SELECT
        order_id,
        user_id,
        amount,
        currency,
        status,
        CAST(event_ts AS DATE) AS order_date,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM demo_catalog.silver.order_events
),

resolved_order AS (
    SELECT *
    FROM latest_order
    WHERE rn = 1
),

payment_summary AS (
    SELECT
        order_id,
        MAX(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS is_paid_flag
    FROM demo_catalog.gold.fact_payments
    GROUP BY order_id
)

INSERT OVERWRITE TABLE demo_catalog.gold.fact_orders
SELECT
    o.order_id,
    o.user_id,
    o.order_date,
    o.amount AS order_amount,
    o.currency,
    o.status AS order_status,
    CASE WHEN p.is_paid_flag = 1 THEN TRUE ELSE FALSE END AS is_paid_flag
FROM resolved_order o
LEFT JOIN payment_summary p
    ON o.order_id = p.order_id;