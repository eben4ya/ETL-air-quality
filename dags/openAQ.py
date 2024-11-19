from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

import pandas as pd
import json

# Check if API response status is "success"
def is_ready_openaq_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_openaq_data")
    if not data or "results" not in data:
        raise AirflowFailException("OpenAQ API response is empty or missing 'results' field.")
    print("API response is valid and ready for processing.")

# Transform and save OpenAQ data
def transform_load_openaq_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_openaq_data")
    results = data['results'][0]  # Extract the first (and only) result

    sensors = results['sensors']
    transformed_data = []

    for sensor in sensors:
        transformed_data.append({
            "Sensor ID": sensor['id'],
            "Sensor Name": sensor['name'],
            "Parameter": sensor['parameter']['name'],
            "Units": sensor['parameter']['units'],
            "Display Name": sensor['parameter']['displayName'],
            "Location Name": results['name'],
            "Latitude": results['coordinates']['latitude'],
            "Longitude": results['coordinates']['longitude'],
            "Date First Recorded (UTC)": results['datetimeFirst']['utc'],
            "Date Last Recorded (UTC)": results['datetimeLast']['utc']
        })

    # Save transformed data as a CSV file
    df = pd.DataFrame(transformed_data)
    now = datetime.now()
    filename = f"openaq_data_wedomartani_{now.strftime('%Y%m%d%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 19),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'openaq_wedomartani_dag',
    default_args=default_args,
    description='A DAG to extract and transform OpenAQ data for Wedomartani',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task to extract data from OpenAQ API
    extract_openaq_data = SimpleHttpOperator(
        task_id='extract_openaq_data',
        http_conn_id='openAQ',
        endpoint='/v3/locations/3037147',
        method='GET',
        headers={
            'X-API-Key': '3abcfd7f19217d02d084277877b2bf2a14dd85d359a7a9960505013c703a62d3'
        },
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # Task to check if data is ready
    is_ready_openaq_data = PythonOperator(
        task_id='is_ready_openaq_data',
        python_callable=is_ready_openaq_data
    )

    # Task to transform and load the data
    transform_load_openaq_data = PythonOperator(
        task_id='transform_load_openaq_data',
        python_callable=transform_load_openaq_data
    )

    # Task dependencies
    extract_openaq_data >> is_ready_openaq_data >> transform_load_openaq_data
