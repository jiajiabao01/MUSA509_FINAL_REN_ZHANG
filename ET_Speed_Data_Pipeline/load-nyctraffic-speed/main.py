from dotenv import load_dotenv
load_dotenv()
import functions_framework

import os
from google.cloud import bigquery
from google.cloud import storage

bucket_name = os.getenv('DATA_PREPARED_BUCKET')
source_dataset_name = os.getenv('SOURCE_DATASET')
core_dataset_name = os.getenv('CORE_DATASET') 

# Load the data into BigQuery as an external table
prepared_blobname = 'nyc_subway_station/nyc_subway_station.jsonl'
table_name = 'nyc_subway_station'
table_uri = f'gs://{bucket_name}/{prepared_blobname}'

create_table_query_source = f'''
CREATE OR REPLACE EXTERNAL TABLE {source_dataset_name}.{table_name} (
    `the_geom` STRING,
    `object_id`  INTEGER,
    `id` FLOAT64,
    `rt_symbol` STRING,
    `name` STRING,
    `url` STRING,
    `shape_len` FLOAT64,

)
OPTIONS (
  format = 'JSON',
  uris = ['{table_uri}']
)
'''

create_table_query_core = f'''
CREATE OR REPLACE EXTERNAL TABLE {core_dataset_name}.{table_name} (
    `the_geom` STRING,
    `object_id`  INTEGER,
    `id` FLOAT64,
    `rt_symbol` STRING,
    `name` STRING,
    `url` STRING,
    `shape_len` FLOAT64,

)
OPTIONS (
  format = 'JSON',
  uris = ['{table_uri}']
)
'''

@functions_framework.http
def load_nycsubway_station(request):
    bigquery_client = bigquery.Client()
    bigquery_client.query_and_wait(create_table_query_source)
    print(f'Loaded {table_uri} into {source_dataset_name}.{table_name}')
    bigquery_client.query_and_wait(create_table_query_core)
    print(f'Loaded {table_uri} into {core_dataset_name}.{table_name}')
    return f"Loaded {table_uri} into {source_dataset_name}.{table_name}\n\tand internal/native table {core_dataset_name}.{table_name}"
