#!/usr/bin/env bash
# Smoke-test an edge -adbc image. The test tools live in a disposable derived image, never
# the published one. The base image's user must survive the driver layer.
set -euo pipefail
image=${1:?usage: test-adbc.sh IMAGE}
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
cp "$here/test-adbc.py" "$context/"
cat > "$context/Dockerfile" <<'EOF'
ARG IMAGE
FROM ${IMAGE}
USER root
# /usr/bin/python3 is the distro's (3.12 on the slim Ubuntu base, 3.13 on the full Debian
# base), never the full image's own virtualenv.
RUN apt-get update && apt-get install --no-install-recommends -y python3-venv \
 && /usr/bin/python3 -m venv /test-env \
 && /test-env/bin/pip install --no-cache-dir 'adbc-driver-manager==1.12.0' 'pyarrow==26.0.0'
COPY test-adbc.py /test-adbc.py
USER app_user
ENTRYPOINT ["/bin/sh", "-ec"]
CMD ["gizmosql_server --backend duckdb >/tmp/server.log 2>&1 & server=$!; trap 'kill $server; cat /tmp/server.log' EXIT; /test-env/bin/python /test-adbc.py"]
EOF
docker build --build-arg "IMAGE=$image" -t gizmosql-edge-adbc-test "$context"
docker run --rm -e GIZMOSQL_USERNAME=smoke -e GIZMOSQL_PASSWORD=smoke gizmosql-edge-adbc-test
if [ "$(docker image inspect "$image" --format '{{.Config.User}}')" != app_user ]; then
  echo "The image no longer runs as app_user" >&2
  exit 1
fi
echo "Image runs as app_user"
