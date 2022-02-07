import os
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator)
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

DATE = "{{logical_date.strftime('%Y-%m')}}"
UDATE = "{{logical_date.strftime('%Y_%m')}}"
URL_PREFIX = "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_"
URL = URL_PREFIX + F"{DATE}.csv"
FILE = F"fhv_{UDATE}.csv"
TABLE = F"fhv_{UDATE}"
PFILE = FILE.replace(".csv", ".parquet")


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format")
        return 1
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="ingest_fhv_gcs",
    schedule_interval="0 10 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=["zoomcamp"]
)

download_csv = BashOperator(
    task_id="download_csv",
    bash_command=f"curl -sSLf {URL} > {HOME}/{FILE}",
    dag=dag)

convert_to_parquet = PythonOperator(
    task_id="convert_to_parquet",
    python_callable=format_to_parquet,
    op_kwargs={
        "src_file": f"{HOME}/{FILE}",
    },
    dag=dag)

local_to_gcs = PythonOperator(
    task_id="local_to_gcs",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": f"raw/{PFILE}",
        "local_file": f"{HOME}/{PFILE}",
    },
    dag=dag)

gcs_to_bigquery_table = BigQueryCreateExternalTableOperator(
    task_id="gcs_to_bigquery_table",
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": TABLE,
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/raw/{PFILE}"],
        },
    },
    dag=dag)

remove_flat_files = BashOperator(
    task_id="remove_flat_files",
    bash_command=f"rm {HOME}/{FILE} {HOME}/{PFILE}",
    dag=dag)

download_csv >> convert_to_parquet >> local_to_gcs
local_to_gcs >> gcs_to_bigquery_table >> remove_flat_files
