from dotenv import load_dotenv
load_dotenv()
import functions_framework

import os
from google.cloud import bigquery
from google.cloud import storage


source_dataset = os.getenv('SOURCE_DATASET')
core_dataset = os.getenv('CORE_DATASET') 

check_lines_query = f'''
  SELECT COUNT(*) AS total_rows
  FROM `source.nyc_roads`
'''

roads_query = f'''
CREATE OR REPLACE TABLE core.nyc_roads_clean AS
    SELECT
      ST_GeogFromGeoJson(the_geom) AS the_geom,
      physicalid,
      bike_lane,
      st_width,
      trafdir,
      rw_type,
      full_stree,
      st_name,
      bike_trafd,
    FROM
      source.nyc_roads;
'''

closure_query = f'''
CREATE OR REPLACE TABLE core.nyc_closures_clean AS
    SELECT
      ST_GeogFromGeoJson(the_geom) AS the_geom,
      nodeid,
    FROM
      source.nyc_road_closures;
'''

traffic_speed_train_query = f'''
CREATE OR REPLACE TABLE core.nyc_traffic_speed_train AS
SELECT
  link_id,
  AVG(morning_avg_speed) AS overall_morning_avg_speed,
  AVG(afternoon_avg_speed) AS overall_afternoon_avg_speed,
  link_points
FROM(
  SELECT
    link_id,
    DATE(TIMESTAMP(data_as_of)) as date_records,
    AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 7 AND 9 THEN speed 
        ELSE NULL 
    END) as morning_avg_speed,
    ROUND(AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 16 AND 18 THEN speed 
        ELSE NULL 
    END), 2) as afternoon_avg_speed,
    link_points
  FROM
    `source.nyc_traffic_speed`
  WHERE
    EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) IN (7, 8, 9, 16, 17, 18) AND DATE(TIMESTAMP(data_as_of)) BETWEEN 
      (SELECT MIN(DATE(TIMESTAMP(data_as_of))) FROM `source.nyc_traffic_speed`)
      AND (SELECT DATE_ADD(MIN(DATE(TIMESTAMP(data_as_of))), INTERVAL 7 DAY) FROM `source.nyc_traffic_speed`)
  GROUP BY
    link_id,
    date_records,
    link_points
  ORDER BY
    date_records, link_id)
  GROUP BY
    link_id,
    link_points;
'''

traffic_speed_test_query = f'''
CREATE OR REPLACE TABLE core.nyc_traffic_speed_test AS
SELECT
  link_id,
  AVG(morning_avg_speed) AS overall_morning_avg_speed,
  AVG(afternoon_avg_speed) AS overall_afternoon_avg_speed,
  link_points
FROM(
  SELECT
    link_id,
    DATE(TIMESTAMP(data_as_of)) as date_records,
    AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 7 AND 9 THEN speed 
        ELSE NULL 
    END) as morning_avg_speed,
    ROUND(AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 16 AND 18 THEN speed 
        ELSE NULL 
    END), 2) as afternoon_avg_speed,
    link_points
  FROM
    `source.nyc_traffic_speed`
  WHERE
    EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) IN (7, 8, 9, 16, 17, 18) AND DATE(TIMESTAMP(data_as_of)) BETWEEN 
      (SELECT DATE_SUB(MAX(DATE(TIMESTAMP(data_as_of))), INTERVAL 7 DAY) FROM `source.nyc_traffic_speed`)
      AND (SELECT MAX(DATE(TIMESTAMP(data_as_of))) FROM `source.nyc_traffic_speed`)
  GROUP BY
    link_id,
    date_records,
    link_points
  ORDER BY
    date_records, link_id)
  GROUP BY
    link_id,
    link_points;
'''

traffic_speed_train_with_geom_query = f'''
CREATE OR REPLACE TABLE `core.nyc_traffic_speed_train_with_geom` AS
SELECT 
  link_id,
  ST_GeogFromText(
    CONCAT(
      'LINESTRING(', 
      STRING_AGG(
        CONCAT(SPLIT(kv, ',')[OFFSET(1)], ' ', SPLIT(kv, ',')[OFFSET(0)]), 
        ', ' ORDER BY seq
      ),
    ')')
  ) AS geom,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed
FROM (
  SELECT 
    link_id,
    kv,
    ROW_NUMBER() OVER (PARTITION BY link_id ORDER BY kv) as seq,
    overall_morning_avg_speed,
  overall_afternoon_avg_speed
  FROM 
    `core.nyc_traffic_speed_train`,
    UNNEST(SPLIT(link_points, ' ')) as kv
)
WHERE 
  ARRAY_LENGTH(SPLIT(kv, ',')) = 2 AND REGEXP_CONTAINS(kv, r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$') AND NOT REGEXP_CONTAINS(kv, r',-7$') AND NOT REGEXP_CONTAINS(kv, r',-73$')
GROUP BY link_id,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed;
'''

traffic_speed_test_with_geom_query = f'''
CREATE OR REPLACE TABLE `core.nyc_traffic_speed_test_with_geom` AS
SELECT 
  link_id,
  ST_GeogFromText(
    CONCAT(
      'LINESTRING(', 
      STRING_AGG(
        CONCAT(SPLIT(kv, ',')[OFFSET(1)], ' ', SPLIT(kv, ',')[OFFSET(0)]), 
        ', ' ORDER BY seq
      ),
    ')')
  ) AS geom,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed
FROM (
  SELECT 
    link_id,
    kv,
    ROW_NUMBER() OVER (PARTITION BY link_id ORDER BY kv) as seq,
    overall_morning_avg_speed,
    overall_afternoon_avg_speed
  FROM 
    `core.nyc_traffic_speed_test`,
    UNNEST(SPLIT(link_points, ' ')) as kv
)
WHERE 
  ARRAY_LENGTH(SPLIT(kv, ',')) = 2 AND REGEXP_CONTAINS(kv, r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$')
GROUP BY link_id,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed;

'''

speed_limit_query = f'''
CREATE OR REPLACE TABLE core.nyc_speed_limit_clean AS
    SELECT
      ST_GeogFromGeoJson(the_geom) AS the_geom,
      street,
      postvz_sl,
    FROM
      source.nyc_speed_limit;
'''

traffic_speed_query = f'''
CREATE OR REPLACE TABLE core.nyc_traffic_speed_clean AS
SELECT
  link_id,
  AVG(morning_avg_speed) AS overall_morning_avg_speed,
  AVG(afternoon_avg_speed) AS overall_afternoon_avg_speed,
  link_points
FROM(
  SELECT
    link_id,
    DATE(TIMESTAMP(data_as_of)) as date_records,
    AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 7 AND 9 THEN speed 
        ELSE NULL 
    END) as morning_avg_speed,
    ROUND(AVG(CASE 
        WHEN EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) BETWEEN 16 AND 18 THEN speed 
        ELSE NULL 
    END), 2) as afternoon_avg_speed,
    link_points
  FROM
    `source.nyc_traffic_speed`
  WHERE
    EXTRACT(HOUR FROM TIMESTAMP(data_as_of)) IN (7, 8, 9, 16, 17, 18)
  GROUP BY
    link_id,
    date_records,
    link_points
  ORDER BY
    date_records, link_id)
  GROUP BY
    link_id,
    link_points;
'''

traffic_speed_with_geom_query = f'''
CREATE OR REPLACE TABLE `core.nyc_traffic_speed_clean_with_geom` AS
SELECT 
  link_id,
  ST_GeogFromText(
    CONCAT(
      'LINESTRING(', 
      STRING_AGG(
        CONCAT(SPLIT(kv, ',')[OFFSET(1)], ' ', SPLIT(kv, ',')[OFFSET(0)]), 
        ', ' ORDER BY seq
      ),
    ')')
  ) AS geom,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed
FROM (
  SELECT 
    link_id,
    kv,
    ROW_NUMBER() OVER (PARTITION BY link_id ORDER BY kv) as seq,
    overall_morning_avg_speed,
  overall_afternoon_avg_speed
  FROM 
    `core.nyc_traffic_speed_clean`,
    UNNEST(SPLIT(link_points, ' ')) as kv
)
WHERE 
  ARRAY_LENGTH(SPLIT(kv, ',')) = 2 AND REGEXP_CONTAINS(kv, r'^-?\d+(\.\d+)?,-?\d+(\.\d+)?$') AND NOT REGEXP_CONTAINS(kv, r',-7$') AND NOT REGEXP_CONTAINS(kv, r',-73$')
GROUP BY link_id,
  overall_morning_avg_speed,
  overall_afternoon_avg_speed;
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

    query_job = client.query(crash_AMpeak_query)
    query_job.result()
    
    query_job = client.query(crash_PMpeak_query)
    query_job.result()

    query_job = client.query(roads_query)
    query_job.result()

    query_job = client.query(closure_query)
    query_job.result()
    
    query_job = client.query(traffic_speed_with_geom_query)
    query_job.result()

    query_job = client.query(subway_station_query)
    query_job.result() 
    return f""
