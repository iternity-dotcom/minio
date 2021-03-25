#!/bin/bash
export SERVER_ENDPOINT="s3.repo2.robert1:443"
export ACCESS_KEY="accessKey"
export SECRET_KEY="secretKey"
export ENABLE_HTTPS="1"
export MINT_MODE="full"

# handle command line arguments
if [ $# -ne 2 ]; then
    echo "usage: run.sh <OUTPUT-LOG-FILE> <ERROR-LOG-FILE>"
    exit 1
fi

output_log_file="$1"
error_log_file="$2"

cp ../../../../../minio-go/functional_tests.go ./main.go
GO111MODULE=on CGO_ENABLED=0 go build -o minio-go main.go

# run tests
./minio-go 1>>"$output_log_file" 2>"$error_log_file"
