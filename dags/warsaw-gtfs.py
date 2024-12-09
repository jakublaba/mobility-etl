import hashlib
import os
from datetime import datetime
from zipfile import ZipFile

import dotenv
import requests
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.blob import BlobServiceClient

dotenv.load_dotenv()
GTFS_FEED_URL = os.getenv("GTFS_FEED_URL")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
GTFS_BUCKET = "gtfs"


@dag(
    dag_id="warsaw-gtfs",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 2),
    catchup=False
)
def warsaw_gtfs():
    log = LoggingMixin().log
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    gtfs_dir = "/tmp/gtfs"
    zip_path = f"{gtfs_dir}/warsaw.zip"
    gtfs_files = [
        "agency",
        "calendar_dates",
        "feed_info",
        "routes",
        "shapes",
        "stop_times",
        "stops",
        "trips"
    ]

    def calculate_file_hash(file: str) -> str:
        hash_func = hashlib.sha256()
        chunk_size = 8192
        with open(file, "rb") as f:
            while chunk := f.read(chunk_size):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    @task
    def fetch_gtfs_feed():
        """
        Downloads gtfs feed as a .zip file, saves it locally
        """
        res = requests.get(GTFS_FEED_URL)
        res.raise_for_status()
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(res.content)

    @task.short_circuit()
    def check_for_changes_since_last_feed() -> bool:
        """
        :return: False if last feed was identical, True otherwise (that includes the case where there was no last feed)
        """
        blob_client = blob_service_client.get_blob_client(GTFS_BUCKET, "latest-feed-hash")
        if not blob_client.exists():
            return True
        last_feed_hash = blob_client.download_blob().readall().decode("utf-8")
        current_feed_hash = calculate_file_hash(zip_path)

        return last_feed_hash != current_feed_hash

    @task
    def write_zip_hash():
        """
        Calculates hash of the .zip and saves it to azure blob storage
        """
        file_hash = calculate_file_hash(zip_path)
        blob_client = blob_service_client.get_blob_client(GTFS_BUCKET, "latest-feed-hash")
        blob_client.upload_blob(file_hash, overwrite=True)

    @task
    def unzip_gtfs():
        with ZipFile(zip_path, "r") as z:
            z.extractall(gtfs_dir)

    @task
    def upload_to_azure_storage(file_name: str):
        local_path = f"{gtfs_dir}/{file_name}.txt"
        azure_storage_path = datetime.today().strftime("%Y/%m/%d/") + file_name + ".csv"
        log.info(f"Uploading {local_path} to azure: {azure_storage_path}")
        with open(local_path, "r") as csv:
            blob_client = (BlobServiceClient
                           .from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
                           .get_blob_client(GTFS_BUCKET, azure_storage_path))
            blob_client.upload_blob(csv.read())

    @task
    def clean_up():
        files = ["warsaw.zip", *map(lambda f: f"{f}.txt", gtfs_files)]
        for file in files:
            path = f"{gtfs_dir}/{file}"
            log.info(f"Removing {path}")
            os.remove(path)

    fetch_gtfs_feed() >> check_for_changes_since_last_feed() >> write_zip_hash() >> unzip_gtfs() \
    >> upload_to_azure_storage.expand(file_name=gtfs_files) >> clean_up()


warsaw_gtfs()
