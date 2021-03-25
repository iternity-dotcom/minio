#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

cp ../../../../../minio-go/functional_tests.go ./main.go
GO111MODULE=on CGO_ENABLED=0 go build -o minio-go main.go
rm *.go

cp ../../../run/core/aws-sdk-go/main.go .
cp ../../../run/core/aws-sdk-go/go.mod .
cp ../../../run/core/aws-sdk-go/go.sum .
GO111MODULE=on CGO_ENABLED=0 go build -o aws-sdk-go main.go
rm *.go
rm *.mod
rm *.sum

docker build --tag iternity/mint .
