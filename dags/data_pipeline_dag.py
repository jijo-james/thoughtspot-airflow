import os
from pathlib import Path
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task


from config import load_config
from tasks.fetch_transform_stock import fetch_transform_stock
from tasks.fetch_transform_weather import fetch_transform_weather
from tasks.load_stock_data import load_stock_data
from tasks.load_weather_data import load_weather_data
from tasks.create_joined_table import create_joined_table

default_args = {"retries": 1, "retry_delay": timedelta(seconds=30)}

conf = load_config()


@dag(
    dag_id=f"{os.path.basename(Path(__file__).parent)}_data_pipeline_dag",
    description="Fetches stock & weather data, transforms, and loads to SQLite",
    schedule=None,  # or use a CRON or preset like '@daily'
    max_active_runs=1,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args=default_args,
)
def data_pipeline_dag():
    """
    DAG that:
    1. Fetches stock data from Alpha Vantage and weather data from WeatherAPI
    2. Transforms data using Pandas
    3. Loads data into SQLite (tables: stock_data, weather_data)
    4. Creates a joined_data table in SQLite combining both data sources
    """

    @task(task_id="fetch_transform_stock", do_xcom_push=True)
    def fetch_transform_stock_task():
        return fetch_transform_stock(
            conf.STOCK_BASE_URL,
            conf.STOCK_API_KEY,
            conf.FUNCTION,
            conf.SYMBOL,
            conf.OUTPUTSIZE,
            days_to_keep=7,
        )

    @task(task_id="fetch_transform_weather", do_xcom_push=True)
    def fetch_transform_weather_task():
        return fetch_transform_weather(
            conf.WEATHER_BASE_URL, conf.WEATHER_API_KEY, conf.LOCATION, days=7
        )

    @task(task_id="load_stock_data")
    def load_stock_data_task(stock_records):
        return load_stock_data(conf.DB_NAME, stock_records)

    @task(task_id="load_weather_data")
    def load_weather_data_task(weather_records):
        return load_weather_data(conf.DB_NAME, weather_records)

    @task(task_id="create_joined_table")
    def create_joined_table_task():
        return create_joined_table(conf.DB_NAME)

    # DAG Execution Flow
    stock_data_output = fetch_transform_stock_task()
    weather_data_output = fetch_transform_weather_task()

    stock_loaded = load_stock_data_task(stock_data_output)
    weather_loaded = load_weather_data_task(weather_data_output)

    joined_table_created = create_joined_table_task()

    stock_data_output >> stock_loaded
    weather_data_output >> weather_loaded
    [stock_loaded, weather_loaded] >> joined_table_created


data_pipeline_dag = data_pipeline_dag()
