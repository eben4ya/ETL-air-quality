from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json

# Transform and load BMKG weather data
def transform_load_bmkg_weather_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_bmkg_weather_data")
    if not data or 'data' not in data:
        # Push failure status
        task_instance.xcom_push(key='bmkg_weather_data_status', value='failure')
        raise ValueError("No valid weather data received from the extract task")
    
    # real api_data is in index 0 which is a dictionary
    api_data = data['data'][0]
    
    # location is api_data['lokasi']
    location_data = api_data.get('lokasi', {})
    
    # weather is api_data['cuaca']
    weather_data = api_data.get('cuaca', [])
    
    transformed_data_list = []
    
    city = location_data.get("desa", "Unknown")
    kecamatan = location_data.get("kecamatan", "Unknown")
    province = location_data.get("provinsi", "Unknown")
    
    for forecast_group in weather_data:
        for forecast in forecast_group:
            # Fetching data from the dictionaries
            utc_datetime = forecast.get("utc_datetime")
            local_datetime = forecast.get("local_datetime")
            temperature = forecast.get("t", None)
            humidity = forecast.get("hu", None)
            weather_desc_en = forecast.get("weather_desc_en", "Unknown")
            weather_desc = forecast.get("weather_desc", "Unknown")
            weather = forecast.get("weather", None)
            wind_speed = forecast.get("ws", None)
            wind_from = forecast.get('wd', 'Unknown')
            wind_to = forecast.get('wd_to', 'Unknown')
            cloud_cover = forecast.get("tcc", None)
            visibility_text = forecast.get("vs_text", None)
            visibility = forecast.get("vs", None)
            analysis_date = forecast.get("analysis_date", None)
            image = forecast.get("image", None)
            wind_dir_deg = forecast.get("wd_deg", None)
            dew_point = forecast.get("tp", None)

        transformed_data_list.append({
            "BMKG_Province": province,
            "BMKG_City": city,
            "BMKG_Subdistrict": kecamatan,
            "BMKG_UTC Datetime": utc_datetime,
            "BMKG_Local Datetime": local_datetime,
            "BMKG_Temperature (°C)": temperature,
            "BMKG_Humidity (%)": humidity,
            "BMKG_Weather Description": weather_desc,
            "BMKG_Wind Speed (km/h)": wind_speed,
            "BMKG_Wind From": wind_from,
            "BMKG_Wind To": wind_to,
            "BMKG_Cloud Cover (%)": cloud_cover,
            "BMKG_Visibility (km)": visibility,
            "BMKG_Analysis Date": analysis_date,
        })
        
    # Raise error if transformed data is empty
    if not transformed_data_list:
        raise ValueError("No valid weather data to transform")
    
    task_instance.xcom_push(key='bmkg_weather_data', value=transformed_data_list)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 16),  # Define start_date here
    'email': ['dzakiwismadi@gmail.com', 'yitzhaketmanalu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Initialize BMKG Weather DAG
with DAG(
    dag_id='bmkg_weather_dag',
    default_args=default_args,
    description='A DAG to extract, transform, and load BMKG weather data',
    # run every hour
    schedule_interval='@hourly',
    catchup=False,  # Do not run for past dates if schedule was missed
    max_active_runs=1,
) as dag:
  
    # Check if BMKG API is ready
    is_bmkg_api_ready = HttpSensor(
        task_id='is_bmkg_api_ready',
        http_conn_id='weather_bmkg',
        endpoint='/publik/prakiraan-cuaca?adm4=31.71.01.1001',
    )

    # Extract BMKG weather data
    extract_bmkg_weather_data = SimpleHttpOperator(
        task_id='extract_bmkg_weather_data',
        http_conn_id='weather_bmkg',
        endpoint='/publik/prakiraan-cuaca?adm4=31.71.01.1001',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )

    # Transform BMKG weather data
    transform_load_bmkg_weather_data = PythonOperator(
        task_id='transform_load_bmkg_weather_data',
        python_callable=transform_load_bmkg_weather_data,
    )

    is_bmkg_api_ready >> extract_bmkg_weather_data >> transform_load_bmkg_weather_data
