from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Connection IDs
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_fred_api"

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# DAG definition
with DAG(
    dag_id='fred_etl_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    tags=['etl', 'fred'],
) as dag:

    @task()
    def extract_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        response = http_hook.run(
            endpoint="fred/series/observations",
            data={"series_id": "CUSR0000SEFP01", "api_key": "---add here--", "file_type": "json"}
        )
        return response.json()

    @task()
    def transform_data(data: dict):
        transformed_data = [
            (obs["date"], float(obs["value"]) if obs["value"] != "." else None)
            for obs in data["observations"]
        ]
        return transformed_data

    @task()
    def load_data(data: list):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fred_data (
            date DATE NOT NULL,
            value NUMERIC
        );
        """
        pg_hook.run(create_table_query)

        # Insert data
        insert_query = "INSERT INTO fred_data (date, value) VALUES (%s, %s)"
        pg_hook.insert_rows(table="fred_data", rows=data)

    # Task dependencies
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)
