import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_csv():

    # Download the file to a temporary directory and return a file handle
    gcs_hook = GCSHook()
    tmp_file = gcs_hook.provide_file(object_url='gs://testing-bucket-wzln-0/user_purchase.csv')
    
    # Open Postgres Connection
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()

    # Load csv into postgres
    with tmp_file as tmp:
        tmp.flush()
        with open(tmp.name) as fh:
            curr.copy_expert("COPY bootcampdb.user_purchase FROM STDIN WITH CSV HEADER", fh)
            get_postgres_conn.commit()

# Default arguments 
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='csv_on_gcs_to_postgres',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
    ) as dag:
    
    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_default",
        sql='sql/create_schema.sql'
    )

    load_csv_to_table = PythonOperator(
        task_id='load_csv_to_table',
        provide_context=True,
        python_callable=load_csv,
    )

    create_schema >> load_csv_to_table