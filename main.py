from psycopg2 import connect
from sqlframe.postgres import functions as F
from sqlframe.postgres import PostgresSession

from sqlframe.duckdb import DuckDBSession

def sample(session):
    df_employee = session.createDataFrame(
        [
            {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1},
            {"id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 2},
            {"id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 3},
            {"id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 1},
            {"id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 3},
        ]
    )

    df_store = session.createDataFrame(
        [
            {"store_id": 1, "store_name": "The Hatch"},
            {"store_id": 2, "store_name": "The Pearl"},
            {"store_id": 3, "store_name": "The Swan"},
        ]
    )

    (
        df_employee
        .join(df_store, on="store_id")
        .groupBy("store_name")
        .agg(F.count("*").alias("total_employees"))
        .show()
    )


if __name__ == '__main__':
    conn = connect(
        dbname="demodb",
        user="demouser",
        password="password",
        host="localhost",
        port="5432",
    )

    pg_session = PostgresSession(conn)
    # sample(pg_session)

    duck_session = DuckDBSession()
    sample(duck_session)