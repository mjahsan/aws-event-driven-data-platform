-- Optimize and Z-Order
OPTIMIZE demo_catalog.silver.events_user ZORDER BY (event_id);
OPTIMIZE demo_catalog.silver.events_payment ZORDER BY (event_id, order_id);
OPTIMIZE demo_catalog.silver.events_order ZORDER BY (event_id, order_id);

-- Clean up old file versions (Keep last 7 days)
VACUUM demo_catalog.silver.events_user RETAIN 168 HOURS;
VACUUM demo_catalog.silver.events_payment RETAIN 168 HOURS;
VACUUM demo_catalog.silver.events_order RETAIN 168 HOURS;
