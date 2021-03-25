#!/bin/bash
export SERVER_ENDPOINT="s3.repo2.robert1:443"
export ACCESS_KEY="accessKey"
export SECRET_KEY="secretKey"
export ENABLE_HTTPS="1"


openssl s_client -showcerts -connect "$SERVER_ENDPOINT" </dev/null 2>/dev/null | \
	sudo openssl x509 -outform PEM -out /usr/share/pki/trust/anchors/s3_server_cert.pem

sudo update-ca-certificates --fresh

export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# handle command line arguments
if [ $# -ne 2 ]; then
    echo "usage: iternity-run.sh <OUTPUT-LOG-FILE> <ERROR-LOG-FILE>"
    exit 1
fi

output_log_file="$1"
error_log_file="$2"

# run tests
go build .
./aws-sdk-go 1>>"$output_log_file" 2>"$error_log_file"
