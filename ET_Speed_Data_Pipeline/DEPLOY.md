## Deploying

_extract_phl_opa_properties_:

```shell
gcloud functions deploy extract_nyctraffic_speed \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=extract_nyctraffic_speed \
--service-account=data-pipeline@easytransit-422015.iam.gserviceaccount.com \
--memory=4Gi \
--timeout=240s \
--set-env-vars=DATA_LAKE_BUCKET=easy_transit_prepared_data \
--trigger-http \
--no-allow-unauthenticated
```

```shell
gcloud functions call extract_nyctraffic_speed
```

_prepare_nyctraffic_speed:

```shell
gcloud functions deploy prepare_nyctraffic_speed \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=prepare_nyctraffic_speed \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=8Gi \
--timeout=480s \
--set-env-vars='DATA_PREPARED_BUCKET=easy_transit_prepared_data' \
--trigger-http 
```

```shell
gcloud functions call prepare_nyctraffic_speed
```
_load_nyctraffic_speed:

```shell
gcloud functions deploy load_nyctraffic_speed \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=load_nyctraffic_speed \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=8Gi \
--timeout=480s \
--trigger-http \
--set-env-vars= ../env. 

```

```shell
gcloud functions call _nyctraffic_speed
```



_run_sql_:

```shell
gcloud functions deploy run_sql \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=run_sql \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=8Gi \
--timeout=480s \
--set-env-vars='DATA_PREPARED_BUCKET=easy_transit_prepared_data,SOURCE_DATASET=source,CORE_DATASET=core' \
--trigger-http 
```

```shell
gcloud functions call run_sql
```




pipeline workflow:
```shell
gcloud workflows deploy phl-property-data-pipeline \
--source=phl-property-data-pipeline.yaml \
--location=us-central1 \
--service-account='data-pipeline-robot-2024@musa-344004.iam.gserviceaccount.com'

gcloud scheduler jobs create http phl-property-data-pipeline \
--schedule='0 0 * * 1' \
--time-zone='America/New_York' \
--uri='https://workflowexecutions.googleapis.com/v1/projects/musa-344004/locations/us-central1/workflows/phl-property-data-pipeline/executions' \
--oauth-service-account-email='data-pipeline-robot-2024@musa-344004.iam.gserviceaccount.com' \
--oidc-service-account-email='data-pipeline-robot-2024@musa-344004.iam.gserviceaccount.com'
```

```shell
gcloud workflows run phl-property-data-pipeline





_extract_nycsubway_station_:

```shell
gcloud functions deploy extract_nycsubway_station \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=extract_nycsubway_station \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=4Gi \
--timeout=240s \
--set-env-vars='DATA_LAKE_BUCKET=easy_transit_raw_data' \
--trigger-http 
```

```shell
gcloud functions call extract_nycsubway_station
```

_prepare_nycsubway_station:

```shell
gcloud functions deploy prepare_nycsubway_station \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=prepare_nycsubway_station \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=8Gi \
--timeout=480s \
--trigger-http \
--set-env-vars='DATA_PREPARED_BUCKET=easy_transit_prepared_data,DATA_LAKE_BUCKET=easy_transit_raw_data'
```

```shell
gcloud functions call prepare_nycsubway_station
```

_load_nycsubway_station:

```shell
gcloud functions deploy load_nycsubway_station \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=load_nycsubway_station \
--service-account='data-pipeline@easytransit-422015.iam.gserviceaccount.com' \
--memory=8Gi \
--timeout=480s \
--trigger-http \
--set-env-vars='DATA_PREPARED_BUCKET=easy_transit_prepared_data,SOURCE_DATASET=source,CORE_DATASET=core'

```

```shell
gcloud functions call load_nycsubway_station
```