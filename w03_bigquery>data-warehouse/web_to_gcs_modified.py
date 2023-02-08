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

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "datalake-w03")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv'

        # download it using requests
        request_url = init_url + file_name + '.gz'
        r = requests.get(request_url)
        open(file_name + '.gz', 'wb').write(r.content)
        print(f"Local: {file_name + '.gz'}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name + '.gz'}", file_name + '.gz')
        print(f"GCS: {service}/{file_name + '.gz'}")


web_to_gcs('2019', 'fhv')

