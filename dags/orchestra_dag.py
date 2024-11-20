# Airflow Libraries
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowFailException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import xcom

# Python Standard Libraries
from datetime import datetime, timedelta
import json

# Third-party Libraries
import pandas as pd

def transform_load_airvisual_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_airvisual_data")

    if data.get("status") != "success":
        task_instance.xcom_push(key='airvisual_data_status', value='failure')
        raise ValueError("API response indicates failure")

    # Extract relevant information from the data
    city = data["data"]["city"]
    state = data["data"]["state"]
    country = data["data"]["country"]
    coordinates = data["data"]["location"]["coordinates"]
    longitude = coordinates[0]
    latitude = coordinates[1]

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    # Transform the data into a more structured format
    transformed_data = {
        "AV_City": city,
        "AV_State": state,
        "AV_Country": country,
        "AV_Longitude": longitude,
        "AV_Latitude": latitude,
        "AV_Pollution Timestamp (UTC)": pollution["ts"],
        "AV_AQI (US)": pollution["aqius"],
        "AV_Main Pollutant (US)": pollution["mainus"],
        "AV_AQI (CN)": pollution["aqicn"],
        "AV_Main Pollutant (CN)": pollution["maincn"],
        "AV_Weather Timestamp (UTC)": weather["ts"],
        "AV_Temperature (°C)": weather["tp"],
        "AV_Pressure (hPa)": weather["pr"],
        "AV_Humidity (%)": weather["hu"],
        "AV_Wind Speed (m/s)": weather["ws"],
        "AV_Wind Direction (°)": weather["wd"],
        "AV_Weather Icon": weather["ic"],
    }
    
    # Save transformed data to XCom
    task_instance.xcom_push(key='airvisual_data', value=transformed_data)

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

    # Push transformed data to XCom for later tasks
    task_instance.xcom_push(key='bmkg_weather_data', value=transformed_data_list)

# Transform and save OpenAQ data
def transform_load_openaq_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_openaq_data")
    results = data['results'][0]  # Extract the first (and only) result

    sensors = results['sensors']
    transformed_data = []

    for sensor in sensors:
        transformed_data.append({
            "OAQ_Sensor ID": sensor['id'],
            "OAQ_Sensor Name": sensor['name'],
            "OAQ_Parameter": sensor['parameter']['name'],
            "OAQ_Units": sensor['parameter']['units'],
            "OAQ_Display Name": sensor['parameter']['displayName'],
            "OAQ_Location Name": results['name'],
            "OAQ_Latitude": results['coordinates']['latitude'],
            "OAQ_Longitude": results['coordinates']['longitude'],
            "OAQ_Date First Recorded (UTC)": results['datetimeFirst']['utc'],
            "OAQ_Date Last Recorded (UTC)": results['datetimeLast']['utc']
        })
        
    # Push transformed data to XCom for later tasks
    task_instance.xcom_push(key='openaq_data', value=transformed_data)

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
        "OW_City": city,
        "OW_Description": weather_description,
        "OW_Temperature (C)": temp_celsius,
        "OW_Feels Like (C)": feels_like_celsius,
        "OW_Minimum Temp (C)": min_temp_celsius,
        "OW_Maximum Temp (C)": max_temp_celsius,
        "OW_Pressure": pressure,
        "OW_Humidity": humidity,
        "OW_Wind Speed": wind_speed,
        "OW_Time of Record (WIB)": time_of_record,
        "OW_Sunrise (Local Time WIB)": sunrise_time,
        "OW_Sunset (Local Time WIB)": sunset_time                        
    }
    
    # Save transformed data to XCom
    task_instance.xcom_push(key='weather_data', value=transformed_data)

# Function to combine data and save to CSV
def combine_and_save_csv(task_instance: TaskInstance):
    # Pull data from XComs by key
    bmkg_weather_data = task_instance.xcom_pull(task_ids='transform_load_bmkg_weather_data', key='bmkg_weather_data')
    airvisual_data = task_instance.xcom_pull(task_ids='transform_load_airvisual_data', key='airvisual_data')
    openaq_data = task_instance.xcom_pull(task_ids='transform_load_openaq_data', key='openaq_data')
    openweather_data = task_instance.xcom_pull(task_ids='transform_load_weather_data', key='weather_data')

    # Initialize a list to hold combined data
    combined_data = []

    # Combine BMKG data (multiple data per fetch)
    if bmkg_weather_data:
        for data in bmkg_weather_data:
            combined_data.append({
                'Province': data.get('BMKG_Province'),
                'City': data.get('BMKG_City'),
                'Subdistrict': data.get('BMKG_Subdistrict'),
                'UTC Datetime': data.get('BMKG_UTC Datetime'),
                'Local Datetime': data.get('BMKG_Local Datetime'),
                'Temperature (°C)': data.get('BMKG_Temperature (°C)'),
                'Humidity (%)': data.get('BMKG_Humidity (%)'),
                'Weather Description': data.get('BMKG_Weather Description'),
                'Wind Speed (km/h)': data.get('BMKG_Wind Speed (km/h)'),
                'Wind From': data.get('BMKG_Wind From'),
                'Wind To': data.get('BMKG_Wind To'),
                'Cloud Cover (%)': data.get('BMKG_Cloud Cover (%)'),
                'Visibility (km)': data.get('BMKG_Visibility (km)'),
                'Analysis Date': data.get('BMKG_Analysis Date'),
            })
    else:
        raise ValueError("BMKG data is empty or missing 'results' field.")

    # Combine Airvisual data (single data per fetch)
    if airvisual_data:
        combined_data.append({
            'City': airvisual_data.get('AV_City'),
            'State': airvisual_data.get('AV_State'),
            'Country': airvisual_data.get('AV_Country'),
            'Longitude': airvisual_data.get('AV_Longitude'),
            'Latitude': airvisual_data.get('AV_Latitude'),
            'Pollution Timestamp (UTC)': airvisual_data.get('AV_Pollution Timestamp (UTC)'),
            'AQI (US)': airvisual_data.get('AV_AQI (US)'),
            'Main Pollutant (US)': airvisual_data.get('AV_Main Pollutant (US)'),
            'AQI (CN)': airvisual_data.get('AV_AQI (CN)'),
            'Main Pollutant (CN)': airvisual_data.get('AV_Main Pollutant (CN)'),
            'Weather Timestamp (UTC)': airvisual_data.get('AV_Weather Timestamp (UTC)'),
            'Temperature (°C)': airvisual_data.get('AV_Temperature (°C)'),
            'Pressure (hPa)': airvisual_data.get('AV_Pressure (hPa)'),
            'Humidity (%)': airvisual_data.get('AV_Humidity (%)'),
            'Wind Speed (m/s)': airvisual_data.get('AV_Wind Speed (m/s)'),
            'Wind Direction (°)': airvisual_data.get('AV_Wind Direction (°)'),
            'Weather Icon': airvisual_data.get('AV_Weather Icon'),
        })
    else:
        raise ValueError("Airvisual data is empty or missing 'results' field.")

    # Combine OpenAQ data (multiple data per fetch)
    if openaq_data:
        for sensor in openaq_data:
            combined_data.append({
                'Sensor ID': sensor.get('OAQ_Sensor ID'),
                'Sensor Name': sensor.get('OAQ_Sensor Name'),
                'Parameter': sensor.get('OAQ_Parameter'),
                'Units': sensor.get('OAQ_Units'),
                'Display Name': sensor.get('OAQ_Display Name'),
                'Location Name': sensor.get('OAQ_Location Name'),
                'Latitude': sensor.get('OAQ_Latitude'),
                'Longitude': sensor.get('OAQ_Longitude'),
                'Date First Recorded (UTC)': sensor.get('OAQ_Date First Recorded (UTC)'),
                'Date Last Recorded (UTC)': sensor.get('OAQ_Date Last Recorded (UTC)'),
            })
    else:
        raise ValueError("OpenAQ data is empty or missing 'results' field.")

    # Combine OpenWeather data (single data per fetch)
    if openweather_data:
        combined_data.append({
            'City': openweather_data.get('OW_City'),
            'Description': openweather_data.get('OW_Description'),
            'Temperature (C)': openweather_data.get('OW_Temperature (C)'),
            'Feels Like (C)': openweather_data.get('OW_Feels Like (C)'),
            'Minimum Temp (C)': openweather_data.get('OW_Minimum Temp (C)'),
            'Maximum Temp (C)': openweather_data.get('OW_Maximum Temp (C)'),
            'Pressure': openweather_data.get('OW_Pressure'),
            'Humidity': openweather_data.get('OW_Humidity'),
            'Wind Speed': openweather_data.get('OW_Wind Speed'),
            'Time of Record (WIB)': openweather_data.get('OW_Time of Record (WIB)'),
            'Sunrise (Local Time WIB)': openweather_data.get('OW_Sunrise (Local Time WIB)'),
            'Sunset (Local Time WIB)': openweather_data.get('OW_Sunset (Local Time WIB)'),
        })
    else:
        raise ValueError("OpenWeather data is empty or missing 'results' field.")
    
    # Create DataFrame from combined data
    df_combined = pd.DataFrame(combined_data)

    # Generate the file name with timestamp
    now = datetime.now()
    filename = f"combined_weather_data_{now.strftime('%Y%m%d%H%M%S')}.csv"

    # Define the output directory (adjust path as necessary)
    output_dir = '/home/benjakmek/airflow/output/'
    file_path = f"{output_dir}{filename}"

    # Save the combined data to CSV
    df_combined.to_csv(file_path, index=False)

# DAG Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 18),
}

# Define the DAG
with DAG(
    dag_id='orchestra_dag',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False,
) as orchestra_dag:

    # Dummy task to mark the start of the DAG
    start_task = DummyOperator(task_id='start')

    # Check if AirVisual API is ready
    is_airvisual_api_ready = HttpSensor(
        task_id='is_airvisual_api_ready',
        http_conn_id='airvisual',
        endpoint='/v2/city?city=Yogyakarta&state=Yogyakarta&country=Indonesia&key=75eb34bb-91da-4ff0-aeec-711438badbeb',
    )
    
    # Check if BMKG API is ready
    is_bmkg_api_ready = HttpSensor(
        task_id='is_bmkg_api_ready',
        http_conn_id='weather_bmkg',
        endpoint='/publik/prakiraan-cuaca?adm4=34.04.11.2004',
        poke_interval=60,  # Check every minute
        timeout=600,  # Timeout after 10 minutes
        mode='poke',
    )
    
    # Task to check if data is ready
    is_ready_openaq_data = HttpSensor(
        task_id='is_ready_openaq_data',
        http_conn_id='openAQ',
        endpoint='/v3/locations/3037147',
        headers={
            'X-API-Key': '3abcfd7f19217d02d084277877b2bf2a14dd85d359a7a9960505013c703a62d3'
        },
    )
    
    # check if weather API is ready
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='openweather_api',
        endpoint='/data/2.5/weather?q=Yogyakarta&appid=a394145349a7323a58c762a69910fdfd',
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

    # Extract BMKG weather data
    extract_bmkg_weather_data = SimpleHttpOperator(
        task_id='extract_bmkg_weather_data',
        http_conn_id='weather_bmkg',
        endpoint='/publik/prakiraan-cuaca?adm4=34.04.11.2004',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )
    
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
    
    # extract open weather data
    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'openweather_api',
        endpoint='/data/2.5/weather?q=Yogyakarta&appid=a394145349a7323a58c762a69910fdfd',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response= True
    )
    
    # Transform and load AirVisual data
    transform_load_airvisual_data = PythonOperator(
        task_id='transform_load_airvisual_data',
        python_callable=transform_load_airvisual_data,
    )

    # Transform BMKG weather data
    transform_load_bmkg_weather_data = PythonOperator(
        task_id='transform_load_bmkg_weather_data',
        python_callable=transform_load_bmkg_weather_data,
        provide_context=True,
    )

    # Task to transform and load the data
    transform_load_openaq_data = PythonOperator(
        task_id='transform_load_openaq_data',
        python_callable=transform_load_openaq_data
    )
    
    # transform weather data
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_weather_data
    )
    
    # Task to combine and save the data
    combine_and_save_csv_task = PythonOperator(
        task_id='combine_and_save_csv',
        python_callable=combine_and_save_csv,
    )

    # Define dependencies
    start_task >> [is_airvisual_api_ready, is_bmkg_api_ready, is_ready_openaq_data, is_weather_api_ready]
    is_airvisual_api_ready >> extract_airvisual_data >> transform_load_airvisual_data >> combine_and_save_csv_task
    is_bmkg_api_ready >> extract_bmkg_weather_data >> transform_load_bmkg_weather_data >> combine_and_save_csv_task
    is_ready_openaq_data >> extract_openaq_data >> transform_load_openaq_data >> combine_and_save_csv_task
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> combine_and_save_csv_task
