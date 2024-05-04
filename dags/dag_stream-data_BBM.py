import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from sqlalchemy import create_engine


from read_data import read_data

default_args = {"owner": "mad", "retry": 5, "retry_delay": timedelta(minutes=5)}


def get_data():
    data = read_data()
    return data.to_json()


def insert_data():
    import psycopg2
    import time

    res = read_data()
    # # Example: 'postgresql://username:password@localhost:5432/your_database'
    # code from https://medium.com/@askintamanli/fastest-methods-to-bulk-insert-a-pandas-dataframe-into-postgresql-2aa2ab6d2b24
    engine = create_engine("postgresql://airflow:airflow@host.docker.internal/data-bbm")

    start_time = time.time()  # get start time before insert
    # Create a connection to your PostgreSQL database
    conn = psycopg2.connect(
        database="data-bbm",
        user="airflow",
        password="airflow",
        host="host.docker.internal",
        port="5432",
    )
    try:
        res.to_sql(name="bbm", con=engine, if_exists="replace")
        conn.commit()
        end_time = time.time()  # get end time after insert
        total_time = end_time - start_time  # calculate the time
        logging.info(f"Insert time: {total_time} seconds")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")


with DAG(
    default_args=default_args,
    dag_id="dag_data_bbm_toSQL_table_v01",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
) as dag:

    task1 = PythonOperator(task_id="read_data", python_callable=get_data)

    create_table_bmm = PostgresOperator(
        task_id="create-table-bbm",
        postgres_conn_id="postgresSQL-data-bbm",
        sql="""
            CREATE TABLE IF NOT EXISTS bbm (
            bulan VARCHAR PRIMARY KEY,
            stasiun_pengisian_bahanbakar VARCHAR NOT NULL,
            jenis_bahanbakar VARCHAR NOT NULL,
            jumlah_penjualan INT NOT NULL,
            penerimaan_pajak INT NOT NULL,
            periode_data VARCHAR NOT NULL);
          """,
    )

    task3 = PythonOperator(task_id="insert-to-SQL", python_callable=insert_data)

    task1 >> create_table_bmm >> task3
