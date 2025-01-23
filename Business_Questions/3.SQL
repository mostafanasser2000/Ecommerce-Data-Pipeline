SELECT payment_type, COUNT(*) number_used
FROM fact_payment
WHERE payment_type != 'not_defined'
GROUP BY payment_type
ORDER BY number_used DESC
LIMIT 1
-- Answer: 'credit_card'
