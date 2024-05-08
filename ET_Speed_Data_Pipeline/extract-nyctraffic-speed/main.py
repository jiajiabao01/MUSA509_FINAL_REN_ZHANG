from dotenv import load_dotenv
load_dotenv()

import os
import pathlib
import requests
import functions_framework
from google.cloud import storage

DIRNAME = pathlib.Path(__file__).parent


@functions_framework.http
def extract_nyctraffic_speed(request):
    print('Extracting NYC traffic transit data...')

    # Download the OPA Properties data as a json
    url = 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$order=data_as_of DESC&$limit=460000'
    filename = DIRNAME / 'nyc_traffic_speed.json'

    response = requests.get(url)
    response.raise_for_status()

    with open(filename, 'wb') as f:
        f.write(response.content)

    print(f'Downloaded {filename}')

    # Upload the downloaded file to cloud storage
    BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET')
    blobname = 'nyc_traffic_speed/nyc_traffic_speed.json'

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blobname)
    blob.upload_from_filename(filename)

    print(f'Uploaded {blobname} to {BUCKET_NAME}')

    return f'Downloaded to {filename} and uploaded to gs://{BUCKET_NAME}/{blobname}'
