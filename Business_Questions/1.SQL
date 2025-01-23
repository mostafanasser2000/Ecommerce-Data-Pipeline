SELECT date_season, COUNT(DISTINCT fact_order_item.order_id) number_of_orders
FROM fact_order_item
JOIN dim_order
ON fact_order_item.order_id = dim_order.id
JOIN dim_date
ON dim_date.id = dim_order.order_date
GROUP BY date_season
ORDER BY number_of_orders DESC 

-- Answer: 'Fall'