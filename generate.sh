#!/bin/bash
#
# Builds python auto-generated protobuf files

set -eux

version=7.3.0

openapi-generator-cli version || npm install @openapitools/openapi-generator-cli -g

if [ "$(openapi-generator-cli version)" != "$version" ]; then
  version-manager set "$version"
fi

poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/dispatcher --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk dispatcher.proto
poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/events --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk events.proto
poetry run python -m grpc_tools.protoc --proto_path=hatchet/api-contracts/workflows --python_out=./hatchet_sdk --pyi_out=./hatchet_sdk --grpc_python_out=./hatchet_sdk workflows.proto

# ensure that pre-commit is applied without errors
pre-commit run --all-files || pre-commit run --all-files
