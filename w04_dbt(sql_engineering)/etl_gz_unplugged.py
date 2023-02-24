import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""


# switch out the bucketname: 'datalake-w04' is here selected by default

BUCKET = os.environ.get("GCP_GCS_BUCKET", "datalake-w04")


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    # create .data directory if it doesn't exist
    if not os.path.exists(".data"):
        os.mkdir(".data")

    for i in range(12):
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = service + "_tripdata_" + year + "-" + month + ".csv"

        # download it using requests
        request_url = init_url + file_name + ".gz"
        r = requests.get(request_url)

        # write to .data folder
        with open(f".data/{file_name}.gz", "wb") as f:
            f.write(r.content)

        print(f"Local: .data/{file_name}.gz")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}.gz", f".data/{file_name}.gz")
        print(f"GCS: {service}/{file_name}.gz")


if __name__ == "__main__":
    service_input = input("Enter service (yellow, green, or fhv): ")

    services = {
        "yellow": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/",
        "green": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/",
        "fhv": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/",
    }

    if service_input in services:
        init_url = services[service_input]
        print(f"Downloading from {init_url}")
    else:
        print("Invalid input")
        quit()

    year = input("Enter year (2019, 2020): ")
    if year not in ["2019", "2020"]:
        print("Invalid input")
        quit()

    web_to_gcs(year, service_input)
