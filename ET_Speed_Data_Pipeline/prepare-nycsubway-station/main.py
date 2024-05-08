from dotenv import load_dotenv
load_dotenv()

import csv
import json
import os
import pathlib
from shapely.wkt import loads as load_wkt
from shapely.geometry import mapping
import functions_framework
from google.cloud import storage

DIRNAME = pathlib.Path(__file__).parent

@functions_framework.http
def prepare_nycsubway_station(request):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    keys_path = os.path.join(script_dir, '..', 'keys', 'service_account.json')
    print("Absolute path to the keys:", keys_path)

    if os.path.exists(keys_path):
        print("Service account file found.")
    else:
        print("Service account file not found at:", keys_path)

    raw_filename = DIRNAME / 'nyc_subway_station.csv'
    prepared_filename = DIRNAME / 'nyc_subway_station.jsonl'

    raw_bucket_name = os.getenv('DATA_LAKE_BUCKET')
    prepared_bucket_name = os.getenv('DATA_PREPARED_BUCKET')
    storage_client = storage.Client()
    raw_bucket = storage_client.bucket(raw_bucket_name)
    prepared_bucket = storage_client.bucket(prepared_bucket_name)

    # Download the data from the bucket
    raw_blobname = 'nyc_subway_station/nyc_subway_station.csv'
    blob = raw_bucket.blob(raw_blobname)
    blob.download_to_filename(raw_filename)
    print(f'Downloaded to {raw_filename}')

    # Load the data from the CSV file and convert to JSONL
    with open(raw_filename, 'r') as f, open(prepared_filename, 'w') as jsonl_file:
        reader = csv.DictReader(f)
        for row in reader:
            if 'the_geom' in row and row['the_geom'].startswith('LINESTRING'):
                try:
                    # Convert WKT to a GeoJSON-like format that BigQuery understands
                    geom = load_wkt(row['the_geom'])
                    # Use shapely's mapping function to convert the geometry to GeoJSON
                    geojson_geom = mapping(geom)
                    # Convert the GeoJSON geometry to a JSON string
                    row['the_geom'] = json.dumps(geojson_geom)
                except Exception as e:
                    print(f"Error processing geometry: {e}")
                    row['the_geom'] = None  # Handle or log the error as appropriate
            jsonl_file.write(json.dumps(row) + '\n')

    print(f"Data has been converted and saved to {prepared_filename}")

    # Upload the prepared data to the bucket
    prepared_blobname = 'nyc_subway_station/nyc_subway_station.jsonl'
    blob = prepared_bucket.blob(prepared_blobname)
    blob.upload_from_filename(prepared_filename)
    print(f'Uploaded to {prepared_blobname}')

    return f'Processed data into {prepared_filename} and uploaded to gs://{prepared_bucket_name}/{prepared_blobname}'
