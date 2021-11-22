from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

# DAG to copy from a table in postgres into a parquet file in a GCS bucket.

# Default arguments
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

GCS_BUCKET = "wzlnde-staging-0"
FILENAME = "user_purchase.parquet"
SQL_QUERY = "SELECT * FROM bootcampdb.user_purchase"

with DAG(
    dag_id='postgres_to_gcs',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
    ) as dag:

    # Write parquet to GCS staging bucket
    dump_table_to_bucket = PostgresToGCSOperator(
        task_id="postgres_table_to_parquet",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        gzip=False,
        export_format='parquet',
        use_server_side_cursor=True
    )