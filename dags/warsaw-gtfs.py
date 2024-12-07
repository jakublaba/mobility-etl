import os
from datetime import datetime
from zipfile import ZipFile

import dotenv
import requests
from airflow.decorators import dag, task
from azure.storage.blob.aio import BlobServiceClient
from pyspark import SparkContext
from pyspark.sql import SparkSession

dotenv.load_dotenv()
GTFS_FEED_URL = os.getenv("GTFS_FEED_URL")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCESS_KEY = os.getenv("AZURE_STORAGE_ACCESS_KEY")
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
    gtfs_dir = "/tmp/gtfs"
    zip_path = f"{gtfs_dir}/gtfs.zip"
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

    @task
    def fetch_gtfs_feed():
        res = requests.get(GTFS_FEED_URL)
        res.raise_for_status()
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(res.content)

    @task
    def unzip_gtfs():
        with ZipFile(zip_path, "r") as z:
            z.extractall(gtfs_dir)

    @task.pyspark(conn_id="spark")
    def combine_with_existing(
            file_name: str,
            spark: SparkSession,
            sc: SparkContext,  # noqa: F841
    ):
        spark.conf.set(
            f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            AZURE_STORAGE_ACCESS_KEY
        )
        blob_exists = (BlobServiceClient
                       .from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
                       .get_blob_client(GTFS_BUCKET, file_name)
                       .exists())
        path = f"wasbs://gtfs@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{file_name}"
        existing = spark.read.parquet(path) if blob_exists else spark.createDataFrame([])
        new = spark.read.csv(os.path.join(gtfs_dir, f"{file_name}.txt"))
        combined = existing.union(new).dropDuplicates()
        combined.write.mode("overwrite").parquet(path)

    fetch_gtfs_feed() >> unzip_gtfs() >> combine_with_existing.expand(file_name=gtfs_files)


warsaw_gtfs()
