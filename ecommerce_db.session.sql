
SELECT d1.date_key, d2.date_key, INTERVAL(d1.date_key, d2.date_key) AS date_diff
FROM(
SELECT DISTINCT fact_order_item.order_id, dim_order.order_date order_date, dim_order.order_approved_date  order_approved_date
FROM fact_order_item
INNER JOIN dim_order
ON fact_order_item.order_id = dim_order.id
)
JOIN dim_date d1 ON d1.id = order_date
JOIN dim_date d2 ON d2.id = order_approved_date
