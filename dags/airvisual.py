from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json

# Transform and load AirVisual weather and pollution data
def transform_load_airvisual_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_airvisual_data")

    if data.get("status") != "success":
        raise ValueError("API response indicates failure")

    city = data["data"]["city"]
    state = data["data"]["state"]
    country = data["data"]["country"]
    coordinates = data["data"]["location"]["coordinates"]
    longitude = coordinates[0]
    latitude = coordinates[1]

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    transformed_data = {
        "City": city,
        "State": state,
        "Country": country,
        "Longitude": longitude,
        "Latitude": latitude,
        "Pollution Timestamp (UTC)": pollution["ts"],
        "AQI (US)": pollution["aqius"],
        "Main Pollutant (US)": pollution["mainus"],
        "AQI (CN)": pollution["aqicn"],
        "Main Pollutant (CN)": pollution["maincn"],
        "Weather Timestamp (UTC)": weather["ts"],
        "Temperature (°C)": weather["tp"],
        "Pressure (hPa)": weather["pr"],
        "Humidity (%)": weather["hu"],
        "Wind Speed (m/s)": weather["ws"],
        "Wind Direction (°)": weather["wd"],
        "Weather Icon": weather["ic"],
    }

    df = pd.DataFrame([transformed_data])

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    filename = f"airvisual_yogyakarta_data_{dt_string}.csv"
    df.to_csv(filename, index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 19),
    'email': ['dzakiwismadi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Initialize AirVisual API DAG
with DAG('airvisual_yogyakarta_dag',
         default_args=default_args,
         description='A DAG for AirVisual data for Yogyakarta',
         schedule_interval='@daily',
         catchup=False) as dag:

    # Check if AirVisual API is ready
    is_airvisual_api_ready = HttpSensor(
        task_id='is_airvisual_api_ready',
        http_conn_id='airvisual',
        endpoint='/v2/city?city=Yogyakarta&state=Yogyakarta&country=Indonesia&key=75eb34bb-91da-4ff0-aeec-711438badbeb',
    )

    # Extract AirVisual data
    extract_airvisual_data = SimpleHttpOperator(
        task_id='extract_airvisual_data',
        http_conn_id='airvisual',
        endpoint='/v2/city?city=Yogyakarta&state=Yogyakarta&country=Indonesia&key=75eb34bb-91da-4ff0-aeec-711438badbeb',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )

    # Transform and load AirVisual data
    transform_load_airvisual_data = PythonOperator(
        task_id='transform_load_airvisual_data',
        python_callable=transform_load_airvisual_data,
    )

    is_airvisual_api_ready >> extract_airvisual_data >> transform_load_airvisual_data
