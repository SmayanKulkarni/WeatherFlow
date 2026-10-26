from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

# Sanity bounds (deg C) for the data quality check
MIN_TEMPERATURE = -90
MAX_TEMPERATURE = 60

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Unique on (location, observed_at) so re-runs within the same API reading don't add duplicates
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_data (
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT,
    weathercode INT,
    observed_at TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (latitude, longitude, observed_at)
);
"""

INSERT_SQL = """
INSERT INTO weather_data
    (latitude, longitude, temperature, windspeed, winddirection, weathercode, observed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (latitude, longitude, observed_at) DO NOTHING
"""

## DAG
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule='@daily',
         catchup=False,
         tags=['weather', 'etl'],
         # Defaults to London; override via "Trigger DAG w/ config" in the UI
         params={
             'latitude': Param(51.5074, type='number', minimum=-90, maximum=90),
             'longitude': Param(-0.1278, type='number', minimum=-180, maximum=180),
         }) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLE_SQL,
    )

    @task()
    def extract_weather_data(params=None):
        """Extract weather data from Open-Meteo API using Airflow Connection."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        response = http_hook.run('/v1/forecast', data={
            'latitude': params['latitude'],
            'longitude': params['longitude'],
            'current_weather': 'true',
        })

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data, params=None):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        return {
            'latitude': params['latitude'],
            'longitude': params['longitude'],
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'observed_at': current_weather['time'],
        }

    @task()
    def validate_weather_data(transformed_data):
        """Fail (without retrying) if the reading is missing or implausible."""
        temperature = transformed_data.get('temperature')
        if temperature is None or not MIN_TEMPERATURE <= temperature <= MAX_TEMPERATURE:
            raise AirflowFailException(f"Implausible temperature: {temperature}")
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load validated data into PostgreSQL."""
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(INSERT_SQL, parameters=(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['observed_at'],
        ))

    ## DAG Workflow- ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    validated_data = validate_weather_data(transformed_data)
    create_table >> load_weather_data(validated_data)
