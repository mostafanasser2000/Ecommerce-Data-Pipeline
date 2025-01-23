dim_customer_table = """
CREATE TABLE IF NOT EXISTS dim_customer(
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR UNIQUE NOT NULL,
    customer_zip_code VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
    )
"""
dim_seller_table = """
CREATE TABLE IF NOT EXISTS dim_seller(
    id SERIAL PRIMARY KEY,
    seller_id VARCHAR UNIQUE NOT NULL,
    seller_zip_code VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR)
"""
dim_product_table = """
CREATE TABLE IF NOT EXISTS dim_product(
    id SERIAL PRIMARY KEY,
    product_id VARCHAR UNIQUE NOT NULL,
    product_category_name VARCHAR,
    product_name_length DECIMAL(18,6),
    product_description_length DECIMAL(18,6),
    product_photos_qty DECIMAL(18,6),
    product_weight_g DECIMAL(18,6),
    product_length_cm DECIMAL(18,6),
    product_height_cm DECIMAL(18,6),
    product_width_cm DECIMAL(18,6)
)
"""


dim_date_table = """
CREATE TABLE IF NOT EXISTS dim_date(
    id SERIAL PRIMARY KEY,
    date_key TIMESTAMP UNIQUE NOT NULL,
    date_year INTEGER,
    date_quarter INTEGER ,
    date_season VARCHAR NOT NULL,
    date_month INTEGER,
    date_month_name VARCHAR NOT NULL,
    date_day INTEGER,
    date_day_name  VARCHAR NOT NULL,
    date_hour INTEGER,
    date_am_or_pm VARCHAR
)
"""
dim_payment = """
CREATE TABLE IF NOT EXISTS dim_payment(
    id SERIAL PRIMARY KEY,
    payment_id VARCHAR,
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value DECIMAL(18,6)
)
"""
bridge_order_payment = """
CREATE TABLE IF NOT EXISTS bridge_order_payment(
    id SERIAL PRIMARY KEY,
    order_id INT,
    payment_id INT,
    FOREIGN KEY (order_id) REFERENCES dim_order(id),
    FOREIGN KEY (payment_id) REFERENCES dim_payment(id)
)
"""

dim_order_table = """
CREATE TABLE IF NOT EXISTS dim_order(
    id SERIAL PRIMARY KEY,
    order_id VARCHAR,
    customer_id INT NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date INT NOT NULL,
    order_approved_date INT NOT NULL,
    pickup_date INT NOT NULL,
    delivered_date INT NOT NULL,
    estimated_time_delivery INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(id),
    FOREIGN KEY (order_date) REFERENCES dim_date(id),
    FOREIGN KEY (order_approved_date) REFERENCES dim_date(id),
    FOREIGN KEY (pickup_date) REFERENCES dim_date(id),
    FOREIGN KEY (delivered_date) REFERENCES dim_date(id),
    FOREIGN KEY (estimated_time_delivery) REFERENCES dim_date(id)
)
"""
fact_payment_table = """
CREATE TABLE IF NOT EXISTS fact_payment(
    id SERIAL PRIMARY KEY,
    order_id INT,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value DECIMAL(18,6),
    FOREIGN KEY (order_id) REFERENCES dim_order(id) 
)
"""

fact_feedback_table = """
CREATE TABLE IF NOT EXISTS fact_feedback(
    id SERIAL PRIMARY KEY,
    feedback_id VARCHAR NOT NULL,
    order_id INT,
    feedback_score INT,
    feedback_form_sent_date INT NOT NULL,
    feedback_answer_date INT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES dim_order(id),
    FOREIGN KEY (feedback_form_sent_date) REFERENCES dim_date(id),
    FOREIGN KEY (feedback_answer_date) REFERENCES dim_date(id)
)
"""

fact_order_item_table = """
CREATE TABLE IF NOT EXISTS fact_order_item(
    id SERIAL PRIMARY KEY,
    order_item_id VARCHAR NOT NULL,
    order_id INT,
    product_id INT,
    seller_id INT,
    pickup_limit_date INT,
    price DECIMAL(18,6),
    shipping_cost DECIMAL(18,6),
    FOREIGN KEY (order_id) REFERENCES dim_order(id),
    FOREIGN KEY (product_id) REFERENCES dim_product(id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(id),
    FOREIGN KEY (pickup_limit_date) REFERENCES dim_date(id)
)
"""

dim_customer_table_insert = """
INSERT INTO dim_customer(
    customer_id,
    customer_zip_code,
    customer_city,
    customer_state
) VALUES (%s, %s, %s, %s)
ON CONFLICT (customer_id) DO UPDATE SET 
customer_zip_code = EXCLUDED.customer_zip_code, 
customer_city = EXCLUDED.customer_city, 
customer_state = EXCLUDED.customer_state
"""

dim_seller_table_insert = """
INSERT INTO dim_seller(
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state
) VALUES (%s, %s, %s, %s)
ON CONFLICT (seller_id) DO UPDATE SET 
seller_zip_code = EXCLUDED.seller_zip_code, 
seller_city = EXCLUDED.seller_city, 
seller_state = EXCLUDED.seller_state
"""
dim_product_table_insert = """
INSERT INTO dim_product(
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
dim_date_table_insert = """
INSERT INTO dim_date(
    date_key,
    date_year,
    date_quarter,
    date_season,
    date_month,
    date_month_name,
    date_day,
    date_day_name,
    date_hour,
    date_am_or_pm
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (date_key) DO NOTHING
"""

dim_order_table_insert = """
INSERT INTO dim_order(
    order_id,
    customer_id,
    order_status,
    order_date,
    order_approved_date,
    pickup_date,
    delivered_date,
    estimated_time_delivery
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

fact_order_item_table_insert = """
INSERT INTO fact_order_item(
    order_item_id,
    order_id,
    product_id,
    seller_id,
    pickup_limit_date,
    price,
    shipping_cost
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
fact_payment_table_insert = """
INSERT INTO fact_payment(
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
) VALUES (%s, %s, %s, %s, %s)
"""

fact_feedback_table_insert = """
INSERT INTO fact_feedback(
    feedback_id,
    order_id,
    feedback_score,
    feedback_form_sent_date,
    feedback_answer_date
) VALUES (%s, %s, %s, %s, %s)
"""
drop_table_queries = [
    "DROP TABLE IF EXISTS fact_payment",
    "DROP TABLE IF EXISTS fact_order_item",
    "DROP TABLE IF EXISTS fact_feedback",
    "DROP TABLE IF EXISTS dim_order",
    "DROP TABLE IF EXISTS dim_date",
    "DROP TABLE IF EXISTS dim_product",
    "DROP TABLE IF EXISTS dim_customer",
    "DROP TABLE IF EXISTS dim_seller",
]
create_table_queries = [
    dim_date_table,
    dim_customer_table,
    dim_seller_table,
    dim_product_table,
    dim_order_table,
    fact_payment_table,
    fact_order_item_table,
    fact_feedback_table,
]

select_customer_by_id = "SELECT id FROM dim_customer WHERE customer_id = %s"
select_order_by_id = "SELECT id FROM dim_order WHERE order_id = %s"
select_product_by_id = "SELECT id FROM dim_product WHERE product_id = %s"
select_seller_by_id = "SELECT id FROM dim_seller WHERE seller_id = %s"
select_date_by_key = "SELECT id FROM dim_date WHERE date_key = %s"
