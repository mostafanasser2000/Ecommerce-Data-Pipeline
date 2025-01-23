import psycopg2

from quires import *
from settings import *


def main():
    logger.info("Start dropping and creating tables")
    try:
        conn = psycopg2.connect(
            f"host={DB_HOST} dbname={SYSTEM_DB} user={DB_USER} password={DB_PASSWORD}"
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
        cur.execute(
            f"CREATE DATABASE {DB_NAME} WITH ENCODING 'utf8' TEMPLATE template0;"
        )
        conn.close()

    except Exception as e:
        logger.error(e)

    try:
        conn = psycopg2.connect(
            f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
        )
        cur = conn.cursor()

        for table in drop_table_queries:
            cur.execute(table)

        conn.commit()
        logger.info("Tables dropped successfully")
        for table in create_table_queries:
            cur.execute(table)

        conn.commit()
        conn.close()
        logger.info("Tables created successfully")

    except Exception as e:
        logger.error(e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
