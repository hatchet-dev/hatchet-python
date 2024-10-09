#!/bin/bash
#
# Builds python auto-generated protobuf files

set -eux

ROOT_DIR=$(pwd)

# deps
version=7.3.0

# openapi-generator-cli version || npm install @openapitools/openapi-generator-cli -g

dst_dir=./hatchet_sdk/clients/cloud

mkdir -p $dst_dir

tmp_dir=./tmp

# generate into tmp folder
openapi-generator generate -i ./hatchet-cloud/openapi.yaml -g python -o ./tmp --skip-validate-spec \
    --library asyncio \
    --global-property=apiTests=false \
    --global-property=apiDocs=true \
    --global-property=modelTests=false \
    --global-property=modelDocs=true \
    --package-name hatchet_sdk.clients.cloud

mv $tmp_dir/hatchet_sdk/clients/cloud/api_client.py $dst_dir/api_client.py
mv $tmp_dir/hatchet_sdk/clients/cloud/configuration.py $dst_dir/configuration.py
mv $tmp_dir/hatchet_sdk/clients/cloud/api_response.py $dst_dir/api_response.py
mv $tmp_dir/hatchet_sdk/clients/cloud/exceptions.py $dst_dir/exceptions.py
mv $tmp_dir/hatchet_sdk/clients/cloud/__init__.py $dst_dir/__init__.py
mv $tmp_dir/hatchet_sdk/clients/cloud/rest.py $dst_dir/rest.py

openapi-generator generate -i ./hatchet-cloud/openapi.yaml -g python -o . --skip-validate-spec \
    --library asyncio \
    --global-property=apis,models \
    --global-property=apiTests=false \
    --global-property=apiDocs=false \
    --global-property=modelTests=false \
    --global-property=modelDocs=false \
    --package-name hatchet_sdk.clients.cloud

# copy the __init__ files from tmp to the destination since they are not generated for some reason
cp $tmp_dir/hatchet_sdk/clients/cloud/models/__init__.py $dst_dir/models/__init__.py
cp $tmp_dir/hatchet_sdk/clients/cloud/api/__init__.py $dst_dir/api/__init__.py

# remove tmp folder
rm -rf $tmp_dir