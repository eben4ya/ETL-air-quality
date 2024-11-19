from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 16),
    'email': ['dzakiwismadi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# initialize DAG
with DAG('weather_dag',
         default_args=default_args,
         description='A simple weather DAG',
         schedule_interval='@daily',
         catchup=False) as dag:
  
    # check if weather API is ready
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='openweather_api',
        endpoint='/data/2.5/weather?q=Jakarta&appid=a394145349a7323a58c762a69910fdfd',
     )

    # extract open weather data
    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'openweather_api',
        endpoint='/data/2.5/weather?q=Jakarta&appid=a394145349a7323a58c762a69910fdfd',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response= True
    )
    
    # transform weather data
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
    )
    
    
    is_weather_api_ready >> extract_weather_data
