from airflow.decorators import dag, task
from datetime import datetime, timedelta
from fetch_data import fetch_crypto_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 1),
    "catchup": False,
    "retries": 3,  
    "retry_delay": timedelta(minutes=5),  
    "schedule": "0 * * * *", 
}

@dag(dag_id="crypto_data_pipeline", default_args=default_args)
def crypto_data_pipeline():
    @task
    def fetch_task():
        fetch_crypto_data()

    fetch_task()

crypto_data_pipeline()
