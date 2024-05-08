from dotenv import load_dotenv
load_dotenv()
import functions_framework

import os
from google.cloud import bigquery
from google.cloud import storage


source_dataset = os.getenv('SOURCE_DATASET')
core_dataset = os.getenv('CORE_DATASET') 

speed_limit_query = f'''
CREATE OR REPLACE TABLE core.nyc_speed_limit_clean AS
    SELECT
      ST_GeogFromGeoJson(the_geom) AS the_geom,
      street,
      postvz_sl,

    FROM
      source.nyc_speed_limit;
'''

crash_query = f'''
CREATE OR REPLACE TABLE core.nyc_crash_clean AS
    SELECT
      ST_GEOGPOINT(longitude, latitude) AS the_geom,
      crash_date,
      crash_time,
      collision_id,

    FROM
      source.nyc_crashes;
'''

crash_AMpeak_query = f'''
CREATE OR REPLACE TABLE core.nyc_crash_clean_AM AS
    SELECT
      ST_GEOGPOINT(longitude, latitude) AS the_geom,
      crash_date,
      crash_time,
      collision_id,

    FROM
      source.nyc_crashes
      
    WHERE
      EXTRACT(HOUR FROM PARSE_TIME('%H:%M', crash_time)) BETWEEN 7 AND 9;
'''
crash_PMpeak_query = f'''
CREATE OR REPLACE TABLE core.nyc_crash_clean_PM AS
    SELECT
      ST_GEOGPOINT(longitude, latitude) AS the_geom,
      crash_date,
      crash_time,
      collision_id,

    FROM
      source.nyc_crashes
      
    WHERE
      EXTRACT(HOUR FROM PARSE_TIME('%H:%M', crash_time)) BETWEEN 16 AND 21;
'''

subway_station_query = f'''
CREATE OR REPLACE TABLE core.nyc_subway_station_clean AS
    SELECT
      ST_GeogFromGeoJson(the_geom) AS the_geom,
      objectid,
      id,
      rt_symbol,
      name,
      url,

    FROM
      source.nyc_subway_station;
'''

@functions_framework.http
def run_sql(request):
    client = bigquery.Client()
    query_job = client.query(speed_limit_query)
    query_job.result() 

    query_job = client.query(crash_query)
    query_job.result()

    query_job = client.query(crash_AMpeak_query)
    query_job.result()
    
    query_job = client.query(crash_PMpeak_query)
    query_job.result()

    query_job = client.query(subway_station_query)
    query_job.result() 
    return f""
