from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import json

# Convert kelvin to celcius
def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius

# transform and load data
def transform_load_weather_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    
    # Adjusting the time to WIB (UTC+7)
    timezone_offset = timedelta(seconds=data['timezone'] + 25200)  # WIB offset is UTC+7 (25200 seconds)
    time_of_record = datetime.utcfromtimestamp(data['dt']) + timezone_offset
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise']) + timezone_offset
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset']) + timezone_offset

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (C)": temp_celsius,
        "Feels Like (C)": feels_like_celsius,
        "Minimum Temp (C)": min_temp_celsius,
        "Maximum Temp (C)": max_temp_celsius,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record (WIB)": time_of_record,
        "Sunrise (Local Time WIB)": sunrise_time,
        "Sunset (Local Time WIB)": sunset_time                        
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"{dt_string}.csv", index=False)

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
        python_callable=transform_load_weather_data
    )
    
    
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
