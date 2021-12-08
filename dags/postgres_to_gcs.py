#
# DAG to copy from a table in postgres into a csv file in a GCS bucket.

import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

temporary_file = 'archivo_temporal.csv'

GCS_BUCKET = "wzlnde-staging-0"
FILENAME = "csv_delete.csv"

def copy_hook():
    pg_hook = PostgresHook()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.copy_expert("COPY bootcampdb.user_purchase TO STDOUT WITH CSV HEADER", FILENAME)

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=FILENAME,
        filename=temporary_file,
        timeout=120
    )

# Default arguments 
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='postgres_to_gcs',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=['capstone']
    ) as dag:

    postgres_to_bucket = PythonOperator(
        task_id='postgres_to_bucket',
        provide_context=True,
        python_callable=copy_hook,
    )