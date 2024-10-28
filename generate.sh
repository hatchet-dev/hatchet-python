#!/bin/bash
#
# Builds python auto-generated protobuf files

set -eux

ROOT_DIR=$(pwd)

# Check if hatchet-cloud submodule exists and has been pulled
if [ -d "hatchet-cloud" ] && [ "$(ls -A hatchet-cloud)" ]; then
    echo "hatchet-cloud submodule detected"
    CLOUD_MODE=true
else
    echo "hatchet-cloud submodule not detected or empty"
    CLOUD_MODE=false
fi

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
    --library asyncio \
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
    --library asyncio \
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

# Generate protobuf files for both hatchet and hatchet-cloud (if exists)
generate_proto() {
    local submodule=$1
    local proto_file=$2
    poetry run python -m grpc_tools.protoc --proto_path=$submodule/api-contracts/$proto_file \
        --python_out=./hatchet_sdk/contracts --pyi_out=./hatchet_sdk/contracts \
        --grpc_python_out=./hatchet_sdk/contracts $proto_file.proto
}

proto_files=("dispatcher" "events" "workflows")

for proto in "${proto_files[@]}"; do
    generate_proto "hatchet" $proto
done

# Fix relative imports in _grpc.py files
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    find ./hatchet_sdk/contracts -type f -name '*_grpc.py' -print0 | xargs -0 sed -i '' 's/^import \([^ ]*\)_pb2/from . import \1_pb2/'
else
    # Linux and others
    find ./hatchet_sdk/contracts -type f -name '*_grpc.py' -print0 | xargs -0 sed -i 's/^import \([^ ]*\)_pb2/from . import \1_pb2/'
fi

if [ "$CLOUD_MODE" = true ]; then
    echo "Generating cloud-specific OpenAPI"
    
    # Generate cloud-specific OpenAPI
    cloud_dst_dir=./hatchet_sdk/clients/cloud_rest
    cloud_tmp_dir=./cloud_tmp

    mkdir -p $cloud_dst_dir

    # generate into cloud tmp folder
    openapi-generator-cli generate -i ./hatchet-cloud/api-contracts/openapi/openapi.yaml -g python -o ./cloud_tmp --skip-validate-spec \
        --library asyncio \
        --global-property=apiTests=false \
        --global-property=apiDocs=true \
        --global-property=modelTests=false \
        --global-property=modelDocs=true \
        --package-name hatchet_sdk.clients.cloud_rest

    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/api_client.py $cloud_dst_dir/api_client.py
    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/configuration.py $cloud_dst_dir/configuration.py
    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/api_response.py $cloud_dst_dir/api_response.py
    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/exceptions.py $cloud_dst_dir/exceptions.py
    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/__init__.py $cloud_dst_dir/__init__.py
    mv $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/rest.py $cloud_dst_dir/rest.py

    openapi-generator-cli generate -i ./hatchet-cloud/api-contracts/openapi/openapi.yaml -g python -o . --skip-validate-spec \
        --library asyncio \
        --global-property=apis,models \
        --global-property=apiTests=false \
        --global-property=apiDocs=false \
        --global-property=modelTests=false \
        --global-property=modelDocs=false \
        --package-name hatchet_sdk.clients.cloud_rest

    # copy the __init__ files from cloud tmp to the destination since they are not generated for some reason
    cp $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/models/__init__.py $cloud_dst_dir/models/__init__.py
    cp $cloud_tmp_dir/hatchet_sdk/clients/cloud_rest/api/__init__.py $cloud_dst_dir/api/__init__.py

    # remove cloud tmp folder
    rm -rf $cloud_tmp_dir

    echo "Generation completed for both OSS and Cloud versions"
else
    echo "Generation completed for OSS version only"
fi

# ensure that pre-commit is applied without errors
pre-commit run --all-files || pre-commit run --all-files

# apply patch to openapi-generator generated code
patch -p1 --no-backup-if-mismatch <./openapi_patch.patch
