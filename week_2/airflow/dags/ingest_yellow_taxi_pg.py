import os
from datetime import datetime
from sqlite3 import SQLITE_INSERT
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DATE = "{{logical_date.strftime('%Y-%m')}}"

HOME = os.environ.get("HOME", "/opt/airflow/")
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"
URL_TEMPLATE = URL_PREFIX + F"{DATE}.csv"
FILE_TEMPLATE = os.path.join(HOME, F"yt_{DATE}.csv")
TABLE = "yt_{{logical_date.strftime('%Y_%m')}}"

PROJECT = os.path.join(HOME, "dags", "projects", "yt_local_insert")
CREATE_SQL = os.path.join(PROJECT, "create_yt_table.sql")
INSERT_SQL = os.path.join(PROJECT, "insert_yt_table.sql")

default_args = {
    "owner": "AT",
    "start_date": datetime(2021, 1, 1)
}

dag = DAG(
    dag_id="ingest_yellow_taxi",
    default_args=default_args,
    schedule_interval="0 10 2 * *",
)

download = BashOperator(
    task_id="download_csv",
    bash_command=F"curl -sSLf {URL_TEMPLATE} > {FILE_TEMPLATE}",
    dag=dag
)

create = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_local",
    sql=CREATE_SQL,
    params={"TABLE": TABLE},
    dag=dag
)

# upload = PostgresHook(
#     postgres_conn_id="postgres_local"
# ).copy_expert(sql="COPY yt_2021_01 FROM STDIN", filename=FILE_TEMPLATE)

download >> create
