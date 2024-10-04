#!/bin/bash

set -e

kubectl config set-context --current --namespace=gizmosql

helm upgrade demo \
     --install . \
     --namespace gizmosql \
     --create-namespace \
     --values values.yaml
