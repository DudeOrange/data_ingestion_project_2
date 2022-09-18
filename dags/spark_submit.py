import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
SPARK_APPLICATION = os.getenv('SPARK_APPLICATION')
HDFS_PATH = os.getenv('HDFS_PATH')

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
OUTPUT_FILE = HDFS_PATH + '/tmp/police_data.parquet'



with DAG(
    'spark_submit',
    schedule_interval='@monthly',
    start_date=datetime(2022, 8, 1)
) as dag:
    relocate_data = SparkSubmitOperator(
        task_id = 'from_postgres_to_parquet_task', 
        application = f'{SPARK_APPLICATION}/relocate_data.py',
        conn_id = 'spark_local',
        application_args = (PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE, PG_PORT, OUTPUT_FILE),
        jars = f'{AIRFLOW_HOME}/jar/postgresql-42.3.4.jar',
        driver_class_path = f'{AIRFLOW_HOME}/jar/postgresql-42.3.4.jar'
    )
    to_hive_table = SparkSubmitOperator(
        task_id = 'from_parquet_to_hive_table_task', 
        application = f'{SPARK_APPLICATION}/write_to_hive.py',
        conn_id = 'spark_local',
        application_args = [OUTPUT_FILE]
    )
    
    
    relocate_data >> to_hive_table
