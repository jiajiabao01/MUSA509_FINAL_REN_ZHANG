from dotenv import load_dotenv
load_dotenv()

import csv
import json
import os
import pathlib

import pyproj
from shapely import wkt
import functions_framework
from google.cloud import storage

DIRNAME = pathlib.Path(__file__).parent


@functions_framework.http
def prepare_nyctraffic_speed(request):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    keys_path = os.path.join(script_dir, '..', 'keys', 'service_account.json')
    print("Absolute path to the keys:", keys_path)


    if os.path.exists(keys_path):
        print("Service account file found.")
    else:
        print("Service account file not found at:", keys_path)


    # credential_path = "/Users/rachelren/Desktop/Upenn/MUSA5090/Final_Project/easyTransit_data_pipeline/keys/service_account.json"
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    raw_filename = DIRNAME / 'nyc_traffic_speed.json'
    prepared_filename = DIRNAME / 'nyc_traffic_speed.jsonl'

    raw_bucket_name = os.getenv('DATA_LAKE_BUCKET')
    prepared_bucket_name=os.getenv('DATA_PREPARED_BUCKET')
    storage_client = storage.Client()
    raw_bucket = storage_client.bucket(raw_bucket_name)
    prepared_bucket = storage_client.bucket(prepared_bucket_name)

    # Download the data from the bucket
    raw_blobname = 'nyc_traffic_speed/nyc_traffic_speed.json'
    blob = raw_bucket.blob(raw_blobname)
    blob.download_to_filename(raw_filename)
    print(f'Downloaded to {raw_filename}')

    # Load the data from the JSON file
    with open(raw_filename, 'r') as f:
        data = json.load(f)
    # Write the data to a JSONL file
    with open(prepared_filename, 'w') as f:
        # If the JSON data is a list, iterate over each element
        if isinstance(data, list):
            for item in data:
                # Write each JSON object to a separate line in the JSONL file
                f.write(json.dumps(item) + '\n')
        else:
            # If the data is a single JSON object, just write it once
            f.write(json.dumps(data) + '\n')

    print(f"Data has been converted and saved to {prepared_filename}")

    # Upload the prepared data to the bucket
    prepared_blobname = 'nyc_traffic_speed/nyc_traffic_speed.jsonl'
    blob = prepared_bucket.blob(prepared_blobname)
    blob.upload_from_filename(prepared_filename)
    print(f'Uploaded to {prepared_blobname}')

    return f'Processed data into {prepared_filename} and uploaded to gs://{prepared_bucket_name}/{prepared_blobname}'