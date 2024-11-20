# Polusi Udara - Data Engineering Project

## Overview

This project aims to collect, process, and analyze air pollution data from various sources to gain insights into air quality trends across different locations. The goal is to build an end-to-end data engineering pipeline that facilitates effective data gathering, transformation, and analysis. The project is ideal for understanding the impact of air pollution on different regions and can be extended to build forecasting models for future air quality.

## Features

- Data Collection from Public APIs: Collect air quality data from Air Visual API and weather data from OpenWeather and BMKG API.
- ETL Pipeline: Build an Extract, Transform, Load (ETL) process to clean and store data in a data warehouse.
- Airflow Integration: Automate the data pipeline using Apache Airflow to ensure continuous data ingestion.
- Data Warehousing: Store the processed data in a cloud data warehouse using PostgreSQL hosted by Aiven for further analysis.
- Interactive Dashboard: Use Streamlit for visualizing trends, comparisons, and generating reports.

## Technologies Used

- **Apache Airflow**: For orchestrating the data pipeline.
- **PostgreSQL**: For data storage and warehousing.
- **Python**: For data extraction, cleaning, and transformations.
- **Streamlit**: For data visualization and generating reports.
- **Aiven**: Hosting Database.

## Project Structure

- **dags/**: Contains Airflow DAGs for pipeline orchestration.
- **scripts/**: Python scripts used for data extraction and transformation.
- **data/**: Sample data files used for testing and validation.
- **visualization/**: Contains dashboards and reports built using BI tools.

## Installation

1. Clone this repository:
   ```sh
   git clone <repo-url>
   ```
2. Set up the Python environment:
   ```sh
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Install Docker and Docker Compose to run Apache Airflow.
4. Start the Airflow setup:
   ```sh
   docker-compose up
   ```

## Running the Pipeline

1. Make sure Docker is running.
2. Access the Airflow web UI at [http://localhost:8080](http://localhost:8080).
3. Trigger the DAG named `air_pollution_pipeline` to start data collection and processing.

## Data Sources

- OpenWeather API
- BMKG API
- Air Visual

## Dashboard

The dashboard provides insights such as:
- Air quality index trends over time
- Air quality Distribution by Weather
- Main Pollutants by Weather Description
- Correllation Between Weather Parameters and AQI
- AQI Predictions
