from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date

default_args = {
    'start_date': datetime(2023, 7, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('elt_data_pipeline', default_args=default_args, schedule_interval='0 0 * * *')  # Runs daily at midnight

def extract_data():
    # Path to the extracting script
    extracting_script_path = '/home/harsh/Dags_script/json_scraper.py'
    
    # Execute the extracting script
    exec(open(extracting_script_path).read())

def transform_data():
    # Path to the transforming script
    transforming_script_path = '/home/harsh/Dags_script/script.py'
    
    # Execute the transforming script
    exec(open(transforming_script_path).read())


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)



# Define task dependencies
extract_task >> transform_task