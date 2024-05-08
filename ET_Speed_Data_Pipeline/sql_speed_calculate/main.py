#SQL to create a new table with joined data

from dotenv import load_dotenv
load_dotenv()
import functions_framework

import os
from google.cloud import bigquery
from google.cloud import storage


source_dataset = os.getenv('SOURCE_DATASET')
core_dataset = os.getenv('CORE_DATASET') 

traffic_speed_join_query = f'''
CREATE OR REPLACE TABLE core.traffic_speed_join AS
SELECT
    sl.*,  
    ts.link_id,  
    ts.link_name,
    ts.overall_morning_avg_speed,
    ts.overall_afternoon_avg_speed
    
FROM
    `core.nyc_speed_limit_clean` sl
LEFT JOIN
    `core.nyc_traffic_speed_clean_with_geom` ts
ON
    REGEXP_CONTAINS(ts.link_name, CONCAT('.*', sl.street, '.*'))
    OR REGEXP_CONTAINS(sl.street, CONCAT('.*', ts.link_name, '.*'));

'''


traffic_speed_ratio_calculate_query = f'''
CREATE OR REPLACE TABLE core.traffic_flow_calculated AS
SELECT
    *,
    -- Calculate morning speed ratio if morning speed is greater than 0
    CASE 
        WHEN overall_morning_avg_speed > 0 THEN overall_morning_avg_speed / postvz_sl
        ELSE NULL 
    END AS morning_speed_ratio,
    -- Calculate afternoon speed ratio if afternoon speed is greater than 0
    CASE 
        WHEN overall_afternoon_avg_speed > 0 THEN overall_afternoon_avg_speed / postvz_sl
        ELSE NULL 
    END AS afternoon_speed_ratio
FROM
    core.traffic_speed_join
WHERE (overall_morning_avg_speed>0 OR overall_afternoon_avg_speed >0) 
AND postvz_sl>0; 

'''


@functions_framework.http
def run_sql(request):
    client = bigquery.Client()
    query_job = client.query(traffic_speed_join_query)
    query_job.result() 

    query_job = client.query(traffic_speed_ratio_calculate_query)
    query_job.result()

    return f""