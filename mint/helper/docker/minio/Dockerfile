FROM ubuntu

RUN apt-get update
RUN apt-get install openssl wget curl -y
RUN mkdir -p /root/.minio/certs
RUN openssl \
    req -x509 \
    -newkey rsa:4096 \
    -keyout /root/.minio/certs/private.key \
    -out /root/.minio/certs/public.crt \
    -days 365 \
    -nodes \
    -subj '/C=DE/ST=BW/L=Freiburg/O=iTernity GmbH/OU=Development/CN=minio' \
    -addext "subjectAltName = DNS:minio"

RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio
RUN chmod +x minio

ENTRYPOINT [ "/minio" ]
