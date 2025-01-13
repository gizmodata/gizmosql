#!/bin/bash

set -e

# Generate a private key
openssl genpkey -algorithm RSA -out private_key.pem -pkeyopt rsa_keygen_bits:4096

# Extract the public key from the private key
openssl rsa -pubout -in private_key.pem -out public_key.pem
