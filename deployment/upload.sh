#!/usr/bin/env bash

for DAG_ELEMENT in $DAGS_PATH; do
  deployment/upload_dags_files.sh $DAG_ELEMENT $COMPOSER_ENV_NAME $REGION_LOCATION
done