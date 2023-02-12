import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
from prefect import task, Flow

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage prefect`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

init_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/"
BUCKET = os.environ.get("GCP_GCS_BUCKET", "datalake-w03")


@task
def download_csv(year: str, month: str, service: str) -> str:
    file_name = f"{service}_tripdata_{year}-{month}.csv"
    request_url = init_url + file_name
    r = requests.get(request_url)
    df = pd.read_csv(io.StringIO(r.text))
    file_name = f"{service}_{year}-{month}.parquet"
    df.to_parquet(file_name, engine="pyarrow")
    return file_name


@task
def upload_to_gcs(file_name: str, service: str) -> None:
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{service}/{file_name}")
    blob.upload_from_filename(file_name)


@Flow
def build_flow(year: str, service: str) -> None:
    for i in range(1, 13):
        month = f"{i:02d}"
        csv_file = download_csv(year, month, service)
        upload_to_gcs(csv_file, service)


if __name__ == "__main__":
    # services = ["fhv", "green", "yellow"]
    # for service in services:
    #     build_flow("2019", service).run()
    build_flow(year="2019", service="fhv")
