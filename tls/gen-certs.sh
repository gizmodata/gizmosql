#!/bin/bash

set -eux

# Ensure OpenSSL is installed
if ! command -v openssl &> /dev/null; then
    echo "Error: OpenSSL is not installed."
    exit 1
fi

SUBJECT_ALT_NAME=${1:-"DNS:$(hostname),DNS:host.docker.internal,DNS:localhost,DNS:example.com,DNS:another.example.com,IP:127.0.0.1"}

# Generate Root CA key and certificate
openssl genrsa -out root-ca.key 4096
chmod 600 root-ca.key

openssl req -x509 -new -nodes \
        -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=Test CA" \
        -key root-ca.key -sha256 -days 10000 \
        -out root-ca.pem -extensions v3_ca

# Generate user certificates
for i in 0 1; do
    openssl genrsa -out cert${i}.key 4096
    chmod 600 cert${i}.key

    openssl req -new -sha256 -key cert${i}.key \
        -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=localhost" \
        -config <(echo "[req]
distinguished_name=req_distinguished_name
[req_distinguished_name]
[SAN]
subjectAltName=${SUBJECT_ALT_NAME}") \
        -out cert${i}.csr

    cat > v3_usr.cnf <<EOF
[ v3_usr_extensions ]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = ${SUBJECT_ALT_NAME}
EOF

    openssl x509 -req -in cert${i}.csr -CA root-ca.pem -CAkey root-ca.key -CAcreateserial \
        -out cert${i}.pem -days 10000 -sha256 -extfile v3_usr.cnf -extensions v3_usr_extensions

    # Convert to PKCS#1 for Java
    openssl pkcs8 -in cert${i}.key -topk8 -nocrypt > cert${i}.pkcs1
done

echo "Certificates generated successfully!"
