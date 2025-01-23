SELECT customer_state, COUNT(DISTINCT fact_order_item.order_id)
FROM fact_order_item
JOIN dim_order
ON fact_order_item.order_id = dim_order.id
JOIN dim_customer
ON dim_order.customer_id = dim_customer.id
GROUP BY customer_state
ORDER BY customer_state DESC
LIMIT 1

-- ANSWER: SUMATERA UTARA