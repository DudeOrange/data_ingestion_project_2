
import os
from datetime import datetime
from de_project.tests.data_quality_test_script import data_quality_test
from de_project.src.ingest_script import data_ingestion

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

URL = 'https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD'
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
OUTPUT_FILE = AIRFLOW_HOME + '/police_data.csv'


with DAG(
    'data_ingestion',
    schedule_interval='@monthly',
    start_date=datetime(2022, 8, 1)
) as dag:
    download_data = BashOperator(
        task_id = 'download',
        bash_command = f'curl {URL} > {OUTPUT_FILE}'
    )
    quality_test = PythonOperator(
        task_id = 'data_quality_test',
        python_callable = data_quality_test,
        op_kwargs = dict(
            path = OUTPUT_FILE
        )
    )
    load_to_db = PythonOperator(
        task_id = 'load_to_database',
        python_callable = data_ingestion,
        op_kwargs = dict(
            host = PG_HOST,
            user = PG_USER,
            password = PG_PASSWORD,
            database = PG_DATABASE,
            port = PG_PORT,
            path = OUTPUT_FILE
        )
    )
    
    download_data >> quality_test >> load_to_db