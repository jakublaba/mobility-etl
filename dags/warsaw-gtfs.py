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
ABS_CONN_STR = os.getenv("ABS_CONNECTION_STRING")
GTFS_BUCKET = "gtfs"


@dag(
    dag_id="warsaw-gtfs",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 2),
    catchup=False
)
def warsaw_gtfs():
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
    def fetch_gtfs_feed() -> str:
        res = requests.get(GTFS_FEED_URL)
        res.raise_for_status()
        zip_path = "/tmp/gtfs/warsaw.zip"
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(res.content)
        return zip_path

    @task
    def unzip_gtfs(zip_path: str) -> str:
        gtfs_dir = os.path.dirname(zip_path)
        with ZipFile(zip_path, "r") as z:
            z.extractall(gtfs_dir)
        return gtfs_dir

    @task.pyspark(conn_id="spark-conn")
    def combine_with_existing(
            spark: SparkSession,
            _sc: SparkContext,
            gtfs_dir: str,
            file_name: str
    ):
        blob_url = (BlobServiceClient
                    .from_connection_string(ABS_CONN_STR)
                    .get_blob_client(GTFS_BUCKET, file_name)
                    .url)
        existing = spark.read.parquet(blob_url)
        new = spark.read.csv(os.path.join(gtfs_dir, f"{file_name}.txt"))
        combined = existing.union(new).dropDuplicates()
        combined.write.mode("overwrite").parquet(blob_url)

    zip_path_ = fetch_gtfs_feed()
    gtfs_dir_ = unzip_gtfs(zip_path_)
    combine_with_existing.partial(gtfs_dir=gtfs_dir_).expand(file_name=gtfs_files)


warsaw_gtfs()
