OPTIMIZE gold.fact_orders ZORDER BY (order_id, user_id);
OPTIMIZE gold.fact_payments ZORDER BY (payment_id, order_id);