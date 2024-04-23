import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

default_args = {"owner": "mad", "retry": 5, "retry_delay": timedelta(minutes=5)}


def read_data():
    try:
        data = pd.read_csv("dags/data/Filedata_Data_Penjualan_dan_Pajak_BBM_di_DKI_Jakarta.csv")
        return data.to_json(orient="records")
    except Exception as e:
        logging.error(f"could not read data {e}")



with DAG(
    default_args=default_args,
    dag_id="dag_data_bbm_v01",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
) as dag:

    task1 = PythonOperator(task_id="read_data", python_callable=read_data)

    task1
