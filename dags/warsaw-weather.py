import os
from datetime import datetime

import dotenv
import requests
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient


@dag(
    dag_id="warsaw-weather",
    schedule="@hourly",
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 2),
    catchup=False,
)
def warsaw_weather():
    dotenv.load_dotenv()
    imgw_api_url = os.getenv("IMGW_API_URL")
    azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    filename = datetime.now().strftime("%Y/%m/%d/weather-%H.csv")
    blob_client = (BlobServiceClient
                   .from_connection_string(azure_storage_connection_string)
                   .get_blob_client(container="weather", blob=filename))

    @task.short_circuit()
    def check_if_weather_snapshot_already_exists():
        return not blob_client.exists()

    @task()
    def load_from_imgw_api_to_azure():
        res = requests.get(imgw_api_url)
        res.raise_for_status()
        blob_client.upload_blob(res.content)

    check_if_weather_snapshot_already_exists() >> load_from_imgw_api_to_azure()


warsaw_weather()
