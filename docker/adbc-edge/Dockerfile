# GizmoSQL edge channel + bundled ADBC drivers (the gizmosql-edge "-adbc" tags).
#
# The same driver set and pins as Hardcharger's ADBC images (both run DuckDB 2.0): ten
# drivers from dbc, plus GizmoData's GizmoSQL, Oracle, Db2 and Quack drivers. Those four are
# staged by prepare-adbc-drivers.py outside Docker, which checks each GitHub release asset's
# digest, the ELF architecture and the manifest's entrypoint. Stable and LTS keep
# Dockerfile-adbc.ci (Quack 0.1.x speaks DuckDB 1.x's protocol; Quack 1.x speaks 2.0's).
#
# The image keeps the base image's entrypoint, ports, home and non-root user (app_user).
# BASE_IMAGE is the released edge image pinned by digest, full or slim.
ARG BASE_IMAGE=ghcr.io/gizmodata/gizmosql-edge:latest-slim
FROM ubuntu:24.04 AS drivers
ARG DBC_VERSION=0.3.1
ARG TARGETARCH
RUN apt-get update \
 && apt-get install --no-install-recommends -y ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl --proto =https --proto-redir =https -fsSL https://dbc.columnar.tech/install.sh -o /tmp/install-dbc.sh \
 && sh /tmp/install-dbc.sh --version "${DBC_VERSION}" \
 && for driver in bigquery databricks exasol flightsql mssql mysql postgresql redshift snowflake trino; do \
      /root/.local/bin/dbc install --level system "$driver"; \
    done \
 && mkdir -p /usr/share/doc/gizmosql-adbc \
 && /root/.local/bin/dbc list --level system > /usr/share/doc/gizmosql-adbc/dbc-drivers.txt
COPY drivers/linux_${TARGETARCH}/ /staged/
# /out/lib holds only the driver libraries, so copying it into the final image adds files to
# /usr/local/lib without touching anything else there. dbc installs into versioned
# per-arch directories, so each of its libraries also gets a stable, version-independent
# symlink in /usr/local/lib (where the DuckDB snowflake extension looks).
RUN mkdir -p /out/lib \
 && cp -a /staged/lib/. /out/lib/ \
 && cp -a /staged/manifests/. /etc/adbc/drivers/ \
 && cp -a /staged/notices/. /usr/share/doc/gizmosql-adbc/ \
 && find /etc/adbc/drivers -name 'libadbc_driver_*.so' \
      -exec sh -c 'ln -sf "$1" "/out/lib/$(basename "$1")"' _ {} \; \
 && ls -l /out/lib

FROM ${BASE_IMAGE}
COPY --from=drivers /etc/adbc/drivers/ /etc/adbc/drivers/
COPY --from=drivers /out/lib/ /usr/local/lib/
COPY --from=drivers /usr/share/doc/gizmosql-adbc/ /usr/share/doc/gizmosql-adbc/
ENV SNOWFLAKE_ADBC_DRIVER_PATH=/usr/local/lib/libadbc_driver_snowflake.so
