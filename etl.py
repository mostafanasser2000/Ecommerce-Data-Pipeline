import logging

import pandas as pd
import psycopg2

from quires import (
    dim_customer_table_insert,
    dim_date_table_insert,
    dim_order_table_insert,
    dim_product_table_insert,
    dim_seller_table_insert,
    fact_feedback_table_insert,
    fact_order_item_table_insert,
    fact_payment_table_insert,
    select_customer_by_id,
    select_date_by_key,
    select_order_by_id,
    select_product_by_id,
    select_seller_by_id,
)
from settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER

logger = logging.getLogger(__name__)


def extract_info_from_date(date):
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    season = ""
    month = date.month
    month_name = date.month_name()
    day = date.day
    day_name = date.day_name()
    hour = date.hour
    am_or_pm = "AM" if hour < 12 else "PM"
    if (month == 12 and day >= 21) or (month <= 3 and day < 21):
        season = "Winter"
    elif (month == 3 and day >= 21) or (month <= 6 and day < 21):
        season = "Spring"
    elif (month == 6 and day >= 21) or (month <= 9 and day < 21):
        season = "Summer"
    elif (month == 9 and day >= 21) or (month < 12 or (month == 12 and day < 21)):
        season = "Fall"
    return (
        date,
        year,
        quarter,
        season,
        month,
        month_name,
        day,
        day_name,
        hour,
        am_or_pm,
    )


def insert_data(cur, query, data):
    cur.executemany(query, data)


def main():
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    logger.info("Start ETL process")

    # ETL customer
    logger.info("Start ETL user")
    user_df = pd.read_csv("ecommerce_dataset/user_dataset.csv")
    user_df.dropna(subset=["user_name"], inplace=True)

    data_to_insert = [
        (
            row["user_name"],
            row["customer_zip_code"],
            row["customer_city"],
            row["customer_state"],
        )
        for index, row in user_df.iterrows()
    ]

    insert_data(cur, dim_customer_table_insert, data_to_insert)
    cur.execute("SELECT COUNT(*) FROM dim_customer")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("Finish ETL user")

    # ETL seller
    logger.info("Start ETL seller")
    seller_df = pd.read_csv("ecommerce_dataset/seller_dataset.csv")
    seller_df.dropna(subset=["seller_id"], inplace=True)
    seller_df["seller_zip_code"] = seller_df["seller_zip_code"].astype(str)

    data_to_insert = [
        (
            row["seller_id"],
            row["seller_zip_code"],
            row["seller_city"],
            row["seller_state"],
        )
        for index, row in seller_df.iterrows()
    ]

    insert_data(cur, dim_seller_table_insert, data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM dim_seller")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("Finish ETL seller")

    # ETL product
    logger.info("Start ETL product")
    products_df = pd.read_csv("ecommerce_dataset/products_dataset.csv")
    products_df.dropna(subset=["product_id"], inplace=True)
    products_df.fillna(
        {
            "product_category": "Unknown",
            "product_name_lenght": 0,
            "product_description_lenght": 0,
            "product_photos_qty": 0,
            "product_weight_g": 0.0,
            "product_length_cm": 0.0,
            "product_height_cm": 0.0,
            "product_width_cm": 0.0,
        },
        inplace=True,
    )
    data_to_insert = [
        (
            row["product_id"],
            row["product_category"],
            row["product_name_lenght"],
            row["product_description_lenght"],
            row["product_photos_qty"],
            row["product_weight_g"],
            row["product_length_cm"],
            row["product_height_cm"],
            row["product_width_cm"],
        )
        for index, row in products_df.iterrows()
    ]

    insert_data(cur, dim_product_table_insert, data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM dim_product")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("Finish ETL product")

    # ETL date
    logger.info("Start ETL date")
    order_df = pd.read_csv("ecommerce_dataset/order_dataset.csv")
    order_df.dropna(subset=["order_id"], inplace=True)
    order_df["order_date"] = pd.to_datetime(order_df["order_date"], errors="coerce")
    order_df["order_approved_date"] = pd.to_datetime(
        order_df["order_approved_date"], errors="coerce"
    )
    order_df["pickup_date"] = pd.to_datetime(order_df["pickup_date"], errors="coerce")
    order_df["delivered_date"] = pd.to_datetime(
        order_df["delivered_date"], errors="coerce"
    )
    order_df["estimated_time_delivery"] = pd.to_datetime(
        order_df["estimated_time_delivery"], errors="coerce"
    )

    order_df.fillna(pd.Timestamp("1900-12-31"), inplace=True)
    dates_df = order_df[
        [
            "order_date",
            "order_approved_date",
            "pickup_date",
            "delivered_date",
            "estimated_time_delivery",
        ]
    ]

    date_data_to_insert = []
    for index, row in dates_df.iterrows():
        for col in dates_df.columns:
            date_data_to_insert.append(extract_info_from_date(row[col]))

    insert_data(cur, dim_date_table_insert, date_data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM dim_date")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("End ETL date")

    # ETL order
    logger.info("Start ETL order")
    order_df["user_name"] = order_df["user_name"].astype(str).str.strip()
    order_df["order_status"] = order_df["order_status"].astype(str)
    order_data = []
    for index, row in order_df.iterrows():
        cur.execute(select_customer_by_id, (row["user_name"],))
        customer_id = cur.fetchone()
        cur.execute(select_date_by_key, (row["order_date"],))
        order_date_id = cur.fetchone()
        cur.execute(select_date_by_key, (row["order_approved_date"],))
        order_approved_date_id = cur.fetchone()
        cur.execute(select_date_by_key, (row["pickup_date"],))
        pickup_date_id = cur.fetchone()
        cur.execute(select_date_by_key, (row["delivered_date"],))
        delivered_date_id = cur.fetchone()
        cur.execute(select_date_by_key, (row["estimated_time_delivery"],))
        estimated_time_delivery_id = cur.fetchone()

        if (
            customer_id
            and order_date_id
            and order_approved_date_id
            and pickup_date_id
            and delivered_date_id
            and estimated_time_delivery_id
        ):
            order_data.append(
                (
                    row["order_id"],
                    customer_id[0],
                    row["order_status"],
                    order_date_id[0],
                    order_approved_date_id[0],
                    pickup_date_id[0],
                    delivered_date_id[0],
                    estimated_time_delivery_id[0],
                )
            )

    insert_data(cur, dim_order_table_insert, order_data)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM dim_order")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("End ETL order")

    # ETL order_item
    logger.info("Start ETL order_item")
    order_item_df = pd.read_csv("ecommerce_dataset/order_item_dataset.csv")
    order_item_df.dropna(
        subset=[
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "pickup_limit_date",
        ]
    )
    order_item_df["pickup_limit_date"] = pd.to_datetime(
        order_item_df["pickup_limit_date"], errors="coerce"
    )
    order_item_df["seller_id"] = order_item_df["seller_id"].str.strip()
    order_item_df["order_id"] = order_item_df["order_id"].str.strip()
    order_item_df["product_id"] = order_item_df["product_id"].str.strip()

    order_item_data_to_insert = []
    for index, row in order_item_df.iterrows():
        # Ensure date values are inserted into dim_date first
        cur.execute(select_date_by_key, (row["pickup_limit_date"],))
        pickup_limit_date_id = cur.fetchone()
        if not pickup_limit_date_id:
            date_info = extract_info_from_date(row["pickup_limit_date"])
            insert_data(cur, dim_date_table_insert, [date_info])
            cur.execute(select_date_by_key, (row["pickup_limit_date"],))
            pickup_limit_date_id = cur.fetchone()

        cur.execute(select_order_by_id, (row["order_id"],))
        order_id = cur.fetchone()
        cur.execute(select_product_by_id, (row["product_id"],))
        product_id = cur.fetchone()
        cur.execute(select_seller_by_id, (row["seller_id"],))
        seller_id = cur.fetchone()

        if order_id and product_id and seller_id and pickup_limit_date_id:
            order_item_data_to_insert.append(
                (
                    row["order_item_id"],
                    order_id[0],
                    product_id[0],
                    seller_id[0],
                    pickup_limit_date_id[0],
                    row["price"],
                    row["shipping_cost"],
                )
            )

    insert_data(cur, fact_order_item_table_insert, order_item_data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM fact_order_item")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("End ETL order_item")

    # ETL payment
    logger.info("Start ETL payment")
    payment_df = pd.read_csv("ecommerce_dataset/payment_dataset.csv")
    payment_df.dropna(subset=["order_id"], inplace=True)

    payment_data_to_insert = []
    for index, row in payment_df.iterrows():
        cur.execute(select_order_by_id, (row["order_id"],))
        order_id = cur.fetchone()
        if order_id:
            payment_data_to_insert.append(
                (
                    order_id[0],
                    row["payment_sequential"],
                    row["payment_type"],
                    row["payment_installments"],
                    row["payment_value"],
                )
            )

    insert_data(cur, fact_payment_table_insert, payment_data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM fact_payment")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("End ETL payment")

    # ETL feedback
    logger.info("Start ETL feedback")
    feedback_df = pd.read_csv("ecommerce_dataset/feedback_dataset.csv")
    feedback_df["feedback_form_sent_date"] = pd.to_datetime(
        feedback_df["feedback_form_sent_date"]
    )
    feedback_df["feedback_answer_date"] = pd.to_datetime(
        feedback_df["feedback_answer_date"], errors="coerce"
    )
    feedback_df["order_id"] = feedback_df["order_id"].str.strip()
    feedback_data_to_insert = []
    for index, row in feedback_df.iterrows():
        # Ensure date values are inserted into dim_date first
        cur.execute(select_date_by_key, (row["feedback_form_sent_date"],))
        feedback_form_sent_date_id = cur.fetchone()
        if not feedback_form_sent_date_id:
            date_info = extract_info_from_date(row["feedback_form_sent_date"])
            insert_data(cur, dim_date_table_insert, [date_info])
            cur.execute(select_date_by_key, (row["feedback_form_sent_date"],))
            feedback_form_sent_date_id = cur.fetchone()

        cur.execute(select_date_by_key, (row["feedback_answer_date"],))
        feedback_answer_date_id = cur.fetchone()
        if not feedback_answer_date_id:
            date_info = extract_info_from_date(row["feedback_answer_date"])
            insert_data(cur, dim_date_table_insert, [date_info])
            cur.execute(select_date_by_key, (row["feedback_answer_date"],))
            feedback_answer_date_id = cur.fetchone()

        cur.execute(select_order_by_id, (row["order_id"],))
        order_id = cur.fetchone()

        if order_id and feedback_form_sent_date_id and feedback_answer_date_id:
            feedback_data_to_insert.append(
                (
                    row["feedback_id"],
                    order_id[0],
                    row["feedback_score"],
                    feedback_form_sent_date_id[0],
                    feedback_answer_date_id[0],
                )
            )

    insert_data(cur, fact_feedback_table_insert, feedback_data_to_insert)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM fact_feedback")
    number_of_inserted_rows = cur.fetchone()[0]
    logger.info(f"Number of inserted rows: {number_of_inserted_rows}")
    logger.info("End ETL feedback")

    conn.commit()
    conn.close()
    logger.info("Finished ETL process successfully")


if __name__ == "__main__":
    main()
