FROM python:3.12.8

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}" \
    && printf ", TARGETARCH=${TARGETARCH}" \
    && printf ", TARGETVARIANT=${TARGETVARIANT} \n" \
    && printf "With uname -s : " && uname -s \
    && printf "and  uname -m : " && uname -m

RUN apt-get update && \
    apt-get dist-upgrade --yes && \
    apt-get install -y \
    build-essential \
    automake \
    cmake \
    wget \
    gcc \
    git \
    ninja-build \
    libboost-all-dev \
    libgflags-dev \
    numactl \
    sqlite3 \
    vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup the AWS Client (so we can copy S3 files to the container if needed)
RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip  ;; \
         "linux/arm64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip  ;; \
    esac && \
    curl "${AWSCLI_FILE}" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -f awscliv2.zip

# Create an application user
RUN useradd app_user --create-home

ARG APP_DIR=/opt/gizmosql

RUN mkdir --parents ${APP_DIR} && \
    chown app_user:app_user ${APP_DIR} && \
    chown --recursive app_user:app_user /usr/local

# Switch to a less privileged user...
USER app_user

WORKDIR ${APP_DIR}

RUN python3 -m venv ${APP_DIR}/venv && \
    echo ". ${APP_DIR}/venv/bin/activate" >> ~/.bashrc && \
    . ~/.bashrc && \
    pip install --upgrade pip setuptools wheel

# Set the PATH so that the Python Virtual environment is referenced for subsequent RUN steps (hat tip: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Copy the scripts directory into the image (we copy directory-by-directory in order to maximize Docker caching)
COPY --chown=app_user:app_user ./scripts ./scripts

# Get the SQLite3 database file
RUN mkdir data && \
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O data/TPC-H-small.db

# Install Python requirements
COPY --chown=app_user:app_user ./requirements.txt ./
RUN pip install --requirement ./requirements.txt

# Create DuckDB database file
RUN python "scripts/create_duckdb_database_file.py" \
           --file-name="TPC-H-small.duckdb" \
           --file-path="data" \
           --overwrite-file=true \
           --scale-factor=0.01

COPY --chown=app_user:app_user ./CMakeLists.txt ./
COPY --chown=app_user:app_user ./third_party ./third_party
COPY --chown=app_user:app_user ./src ./src

# Run the CMake build (then cleanup)
RUN cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local && \
    cmake --build build --target install && \
    rm -rf build src third_party CMakeLists.txt

COPY --chown=app_user:app_user ./tls ./tls

# Install DuckDB CLI for troubleshooting, etc.
ARG DUCKDB_VERSION="1.1.3"

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip  ;; \
         "linux/arm64")  DUCKDB_FILE=https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-aarch64.zip  ;; \
    esac && \
    curl --output /tmp/duckdb.zip --location ${DUCKDB_FILE} && \
    unzip /tmp/duckdb.zip -d /usr/local/bin && \
    rm /tmp/duckdb.zip

EXPOSE 31337

# Run a test to ensure that the server works...
RUN scripts/test_gizmosql.sh

ENTRYPOINT scripts/start_gizmosql.sh
