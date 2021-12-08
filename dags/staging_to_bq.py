# Airflow DAG to create a Dataproc cluster, submit a Pyspark Job from GCS and
# if succeeded triger another DAG to upload the file just created from staging layer
# to BigQuery, after trigger it destroys the cluster.

from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DATASET_NAME = 'bootcamp'
TABLE_NAME = 'user_behavior_metric'
BUCKET = 'wzlnde-staging-0'
SOURCE = ['user_behavior/*.parquet']

# Default arguments 
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='staging_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['capstone']
    ) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_airflow_dataset', dataset_id=DATASET_NAME
    )

    load_parquet = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET,
        source_objects=SOURCE,
        source_format='parquet',
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',   
    )

    create_dataset >> load_parquet