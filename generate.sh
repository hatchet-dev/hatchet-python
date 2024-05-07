#!/bin/bash
#
# Builds python auto-generated protobuf files

set -eux

ROOT_DIR=$(pwd)

# deps
version=7.3.0

openapi-generator-cli version || npm install @openapitools/openapi-generator-cli -g

# if [ "$(openapi-generator-cli version)" != "$version" ]; then
#   version-manager set "$version"
# fi

# generate deps from hatchet repo
cd hatchet/ && sh ./hack/oas/generate-server.sh && cd $ROOT_DIR

# generate python rest client

dst_dir=./hatchet_sdk/clients/rest

mkdir -p $dst_dir

tmp_dir=./tmp

# generate into tmp folder
openapi-generator-cli generate -i ./hatchet/bin/oas/openapi.yaml -g python -o ./tmp --skip-validate-spec \
    --global-property=apiTests=false \
    --global-property=apiDocs=true \
    --global-property=modelTests=false \
    --global-property=modelDocs=true \
    --package-name hatchet_sdk.clients.rest

mv $tmp_dir/hatchet_sdk/clients/rest/api_client.py $dst_dir/api_client.py
mv $tmp_dir/hatchet_sdk/clients/rest/configuration.py $dst_dir/configuration.py
mv $tmp_dir/hatchet_sdk/clients/rest/api_response.py $dst_dir/api_response.py
mv $tmp_dir/hatchet_sdk/clients/rest/exceptions.py $dst_dir/exceptions.py
mv $tmp_dir/hatchet_sdk/clients/rest/__init__.py $dst_dir/__init__.py
mv $tmp_dir/hatchet_sdk/clients/rest/rest.py $dst_dir/rest.py

openapi-generator-cli generate -i ./hatchet/bin/oas/openapi.yaml -g python -o . --skip-validate-spec \
    --global-property=apis,models \
    --global-property=apiTests=false \
    --global-property=apiDocs=false \
    --global-property=modelTests=false \
    --global-property=modelDocs=false \
    --package-name hatchet_sdk.clients.rest

# copy the __init__ files from tmp to the destination since they are not generated for some reason
cp $tmp_dir/hatchet_sdk/clients/rest/models/__init__.py $dst_dir/models/__init__.py
cp $tmp_dir/hatchet_sdk/clients/rest/api/__init__.py $dst_dir/api/__init__.py

# remove tmp folder
rm -rf $tmp_dir

poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/dispatcher --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk dispatcher.proto
poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/events --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk events.proto
poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/workflows --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk workflows.proto

# ensure that pre-commit is applied without errors
pre-commit run --all-files || pre-commit run --all-files
