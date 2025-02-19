# GizmoSQL
### Copyright © 2025 Gizmo Data LLC - All Rights Reserved

## An [Arrow Flight SQL Server](https://arrow.apache.org/docs/format/FlightSql.html) with [DuckDB](https://duckdb.org) or [SQLite](https://sqlite.org) back-end execution engines

**Note**: This project was forked (and renamed) from: https://github.com/voltrondata/sqlflite

[<img src="https://img.shields.io/badge/github--package-container--image-green.svg?logo=Docker">](https://github.com/gizmodata/gizmosql/pkgs/container/gizmosql)
[<img src="https://img.shields.io/badge/Documentation-dev-yellow.svg?logo=">](https://arrow.apache.org/docs/format/FlightSql.html)
[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql-blue.svg?logo=Github">](https://github.com/gizmodata/gizmosql)
[<img src="https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven">](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[<img src="https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI">](https://pypi.org/project/adbc-driver-flightsql/)
[<img src="https://img.shields.io/badge/PyPI-GizmoSQL%20SQLAlchemy%20Dialect-blue?logo=PyPI">](https://pypi.org/project/sqlalchemy-gizmosql-adbc-dialect/)

## Description

This is the GizmoSQL core repo - used to build an Apache Arrow Flight SQL server implementation using DuckDB or SQLite as a backend database.

It enables authentication via middleware and allows for encrypted connections to the database via TLS.

For more information about Apache Arrow Flight SQL - please see this [article](https://voltrondata.com/resources/apache-arrow-flight-sql-arrow-for-every-database-developer). 

> [!NOTE]   
> GizmoSQL is a licensed-product - you MUST have a valid license key to use it.  See repo: https://github.com/gizmodata/generate-jwt-license to generate a license key file.
> In the docker examples below, you'll see a license file mounted for using the product

## Option 1 - Running from the published Docker image

**Note** - this assumes that you have your Github Access Token stored as an env var named `{GITHUB_ACCESS_TOKEN}`.  See: [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) for more information.

Open a terminal, then pull and run the published Docker image which has everything setup (change: "--detach" to "--interactive" if you wish to see the stdout on your screen) - with command:

```bash
# Authenticate to Github Docker Registry - replace USERNAME with your Github username
echo ${GITHUB_ACCESS_TOKEN} | docker login ghcr.io -u prmoore77 --password-stdin

# Pull and run the Docker image 
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --volume $(pwd)/license_files:/opt/gizmosql/license_files \
           --env LICENSE_KEY_FILENAME="license_files/license_key.jwt" \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull missing \
           ghcr.io/gizmodata/gizmosql:latest
```

The above command will automatically mount a very small TPC-H DuckDB database file.

**Note**: You can disable TLS in the container by setting environment variable: `TLS_ENABLED` to "0" (default is "1" - enabled).  This is not recommended unless you are using an mTLS sidecar in Kubernetes or something similar, as it will be insecure.    

### Optional - open a different database file
When running the Docker image - you can have it run your own DuckDB database file (the database must be built with DuckDB version: 1.2.0).   

Prerequisite: DuckDB CLI   
Install DuckDB CLI version [1.2.0](https://github.com/duckdb/duckdb/releases/tag/v1.2.0) - and make sure the executable is on your PATH.

Platform Downloads:   
[Linux x86-64](https://github.com/duckdb/duckdb/releases/download/v1.2.0/duckdb_cli-linux-amd64.zip)   
[Linux arm64 (aarch64)](https://github.com/duckdb/duckdb/releases/download/v1.2.0/duckdb_cli-linux-aarch64.zip)   
[MacOS Universal](https://github.com/duckdb/duckdb/releases/download/v1.2.0/duckdb_cli-osx-universal.zip)

In this example, we'll generate a new TPC-H Scale Factor 1 (1GB) database file, and then run the docker image to mount it:

```bash
# Generate a TPC-H database in the host's /tmp directory
pushd /tmp

duckdb ./tpch_sf1.duckdb << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);
EOF

# Authenticate to Github Docker Registry - replace USERNAME with your Github username
echo ${GITHUB_ACCESS_TOKEN} | docker login ghcr.io -u USERNAME --password-stdin

# Run the gizmosql docker container image - and mount the host's DuckDB database file created above inside the container
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --volume $(pwd)/license_files:/opt/gizmosql/license_files \
           --env LICENSE_KEY_FILENAME="license_files/license_key.jwt" \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --pull missing \
           --mount type=bind,source=$(pwd),target=/opt/gizmosql/data \
           --env DATABASE_FILENAME="data/tpch_sf1.duckdb" \
           ghcr.io/gizmodata/gizmosql:latest
```

### Running initialization SQL commands
You can now run initialization commands upon container startup by setting environment variable: `INIT_SQL_COMMANDS` to a string of SQL commands separated by semicolons - example value:   

`SET threads = 1; SET memory_limit = '1GB';`.      

Here is a full example of running the Docker image with initialization SQL commands:
```bash
# Authenticate to Github Docker Registry - replace USERNAME with your Github username
echo ${GITHUB_ACCESS_TOKEN} | docker login ghcr.io -u USERNAME --password-stdin

# Pull and run the Docker image 
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --volume $(pwd)/license_files:/opt/gizmosql/license_files \
           --env LICENSE_KEY_FILENAME="license_files/license_key.jwt" \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --env INIT_SQL_COMMANDS="SET threads = 1; SET memory_limit = '1GB';" \
           --pull missing \
           ghcr.io/gizmodata/gizmosql:latest
```

You can also specify a file containing initialization SQL commands by setting environment variable: `INIT_SQL_COMMANDS_FILE` to the path of the file containing the SQL commands - example value: `/tmp/init.sql`.  The file must be mounted inside the container.   

**Note**: for the DuckDB back-end - the following init commands are automatically run for you:   
`SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;`

**Note**: Initialization SQL commands which SELECT data will NOT show the results (this is not supported).

**Note**: Initialization SQL commands which fail will cause the Flight SQL server to abort and exit with a non-zero exit code. 

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the GIZMOSQL_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=gizmosql_password&disableCertificateVerification=true
```

For instructions on setting up the JDBC driver in popular Database IDE tool: [DBeaver Community Edition](https://dbeaver.io) - see this [repo](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver).

**Note** - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's secret key.  Just change the password in the new container by changing the "GIZMOSQL_PASSWORD" env var setting - and then use that to connect via JDBC.  

### Connecting to the server via the new [ADBC Python Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/)

You can now use the new Apache Arrow Python ADBC Flight SQL driver to query the Flight SQL server.  ADBC offers performance advantages over JDBC - because it minimizes serialization/deserialization, and data stays in columnar format at all phases.

You can learn more about ADBC and Flight SQL [here](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc).

Ensure you have Python 3.9+ installed, then open a terminal, then run:
```bash
# Create a Python virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Install the requirements including the new Arrow ADBC Flight SQL driver
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql

# Start the python interactive shell
python
```

In the Python shell - you can then run:
```python
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions


with gizmosql.connect(uri="grpc+tls://localhost:31337",
                        db_kwargs={"username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
                                   "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
   with conn.cursor() as cur:
       cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                   parameters=[24]
                   )
       x = cur.fetch_arrow_table()
       print(x)
```

You should see results:
```text
pyarrow.Table
n_nationkey: int32
n_name: string
----
n_nationkey: [[24]]
n_name: [["UNITED STATES"]]
```

### Connecting via the new `gizmosql_client` CLI tool
You can also use the new `gizmosql_client` CLI tool to connect to the Flight SQL server, and then run a single command.  This tool is built into the Docker image, and is also available as a standalone executable for Linux and MacOS.   

Example (run from the host computer's terminal):
```bash
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "gizmosql_password" \
  --query "SELECT version()" \
  --use-tls \
  --tls-skip-verify
```

That should return:
```text
Results from endpoint 1 of 1
Schema:
version(): string

Results:
version():   [
    "v1.2.0"
  ]

Total: 1
```

### Connecting via [Ibis](https://ibis-project.org)
See: https://github.com/gizmodata/ibis-gizmosql

### Connecting via [SQLAlchemy](https://www.sqlalchemy.org)
See: https://github.com/gizmodata/sqlalchemy-gizmosql-adbc-dialect

### Tear-down
Stop the docker image with:
```bash
docker stop gizmosql
```

## Option 2 - Download and run the gizmosql CLI executable

Download (and unzip) the latest release of the **gizmosql_server** CLI executable from these currently supported platforms:   
[Linux x86-64](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_linux_amd64.zip)   
[Linux arm64](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_linux_arm64.zip)   
[MacOS x86-64](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_macos_amd64.zip)   
[MacOS arm64](https://github.com/gizmodata/gizmosql/releases/latest/download/gizmosql_cli_macos_arm64.zip)   

Then from a terminal - you can run:
```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server --database-filename data/some_db.duckdb --print-queries
```

To see all program options - run:
```bash
gizmosql_server --help
```

## Option 3 - Steps to build the solution manually

In order to run build the solution manually, and run SQLite and DuckDB Flight SQL server, you need to set up a new Python 3.9+ virtual environment on your machine. 
Follow these steps to do so (thanks to David Li!).

1. Clone the repo and build the static library and executable
```bash
git clone https://github.com/gizmodata/gizmosql --recurse-submodules
cd gizmosql

# Build and install the static library and executable
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --target install
```

2. Install Python requirements for ADBC client interaction - (ensure you have Python 3.9+ installed first)
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install --requirement ./requirements.txt
````

3. Get some SQLite3 sample data.
```bash
wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ./data/TPC-H-small.sqlite
```

4. Create a DuckDB database.
```bash
python "scripts/create_duckdb_database_file.py" \
       --file-name="TPC-H-small.duckdb" \
       --file-path="data" \
       --overwrite-file=true \
       --scale-factor=0.01
```

5. Optionally generate TLS certificates for encrypting traffic to/from the Flight SQL server
```bash
pushd tls
./gen-certs.sh
popd
```

6. Start the Flight SQL server (and print client SQL commands as they run using the --print-queries option)
```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server --database-filename data/TPC-H-small.duckdb --print-queries
```

## Selecting different backends
This option allows choosing from two backends: SQLite and DuckDB. It defaults to DuckDB.

```bash
$ GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server --database-filename data/TPC-H-small.duckdb --license-key-filename license_files/license_key.jwt
GizmoSQL - Copyright © 2025 Gizmo Data LLC - All Rights Reserved
----------------------------------------------
Using License Key file: ../license_files/license_key.jwt
License file verification successful!
License ID: "9a19b353-1b6f-40ee-8ef2-378863e1a146"
Licensed to Customer: "Gizmo Data LLC"
Licensed to User: "philip@gizmodata.tech"
License issue date: 2025-01-13 11:11:28
License expiration date: 2026-01-13 11:11:28
Licensed by: "Gizmo Data LLC"
----------------------------------------------
Apache Arrow version: 19.0.1
WARNING - TLS is disabled for the GizmoSQL server - this is insecure.
DuckDB version: v1.2.0
Running Init SQL command: 
SET autoinstall_known_extensions = true;
Running Init SQL command: 
 SET autoload_known_extensions = true;
Using database file: "data/TPC-H-small.duckdb"
Print Queries option is set to: false
GizmoSQL server version:  - with engine: DuckDB - will listen on grpc+tcp://0.0.0.0:31337
GizmoSQL server - started
```

The above call is equivalent to running `gizmosql_server -B duckdb` or `gizmosql --backend duckdb`. To select SQLite run

```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server -B sqlite -D data/TPC-H-small.sqlite --license-key-filename license_files/license_key.jwt
```
or 
```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server --backend sqlite --database-filename data/TPC-H-small.sqlite --license-key-filename license_files/license_key.jwt
```
The above will produce output similar to the following:

```bash
GizmoSQL - Copyright © 2025 Gizmo Data LLC - All Rights Reserved
----------------------------------------------
Using License Key file: license_files/license_key.jwt
License file verification successful!
License ID: "9a19b353-1b6f-40ee-8ef2-378863e1a146"
Licensed to Customer: "Gizmo Data LLC"
Licensed to User: "philip@gizmodata.tech"
License issue date: 2025-01-13 11:11:28
License expiration date: 2026-01-13 11:11:28
Licensed by: "Gizmo Data LLC"
----------------------------------------------
Apache Arrow version: 19.0.1
WARNING - TLS is disabled for the GizmoSQL server - this is insecure.
SQLite version: 3.46.1
Using database file: "/Users/philip/Documents/git/gizmosql/data/TPC-H-small.sqlite"
Print Queries option is set to: false
GizmoSQL server version:  - with engine: SQLite - will listen on grpc+tcp://0.0.0.0:31337
GizmoSQL server - started
```

## Print help
To see all the available options run `gizmosql_server --help`.

```bash
gizmosql_server --help
Allowed options:
  --help                              produce this help message
  --version                           Print the version and exit
  -B [ --backend ] arg (=duckdb)      Specify the database backend. Allowed 
                                      options: duckdb, sqlite.
  -H [ --hostname ] arg               Specify the hostname to listen on for the
                                      GizmoSQL Server.  If not set, we will use
                                      env var: 'GIZMOSQL_HOSTNAME'.  If that 
                                      isn't set, we will use the default of: 
                                      '0.0.0.0'.
  -R [ --port ] arg (=31337)          Specify the port to listen on for the 
                                      GizmoSQL Server.
  -D [ --database-filename ] arg      Specify the database filename (absolute 
                                      or relative to the current working 
                                      directory)
  -U [ --username ] arg               Specify the username to allow to connect 
                                      to the GizmoSQL Server for clients.  If 
                                      not set, we will use env var: 
                                      'GIZMOSQL_USERNAME'.  If that isn't set, 
                                      we will use the default of: 
                                      'gizmosql_username'.
  -P [ --password ] arg               Specify the password to set on the 
                                      GizmoSQL Server for clients to connect 
                                      with.  If not set, we will use env var: 
                                      'GIZMOSQL_PASSWORD'.  If that isn't set, 
                                      the server will exit with failure.
  -S [ --secret-key ] arg             Specify the secret key used to sign JWTs 
                                      issued by the GizmoSQL Server. If it 
                                      isn't set, we use env var: 'SECRET_KEY'. 
                                      If that isn't set, the server will create
                                      a random secret key.
  -T [ --tls ] arg                    Specify the TLS certificate and key file 
                                      paths.
  -I [ --init-sql-commands ] arg      Specify the SQL commands to run on server
                                      startup.  If not set, we will use env 
                                      var: 'INIT_SQL_COMMANDS'.
  -F [ --init-sql-commands-file ] arg Specify a file containing SQL commands to
                                      run on server startup.  If not set, we 
                                      will use env var: 'INIT_SQL_COMMANDS_FILE
                                      '.
  -M [ --mtls-ca-cert-filename ] arg  Specify an optional mTLS CA certificate 
                                      path used to verify clients.  The 
                                      certificate MUST be in PEM format.
  -Q [ --print-queries ]              Print queries run by clients to stdout
  -L [ --license-key-filename ] arg   Specify the license key file path.  If 
                                      not set, we will use env var: 
                                      'LICENSE_KEY_FILENAME'.
```

## Slim Docker image
There is now a slim docker image available, without Python, tls certificate generation, sample database files, etc.   

You must supply the following environment variables to the slim image:
- `DATABASE_FILENAME` - the path to the database file to use
- `GIZMOSQL_PASSWORD` - the password to use for the GizmoSQL server

You can optionally supply the following environment variables:
- `TLS_ENABLED` - set to "1" to enable TLS (default is "0" - disabled)
- `TLS_CERT` - If `TLS_ENABLED` is 1 - provide the path to the TLS certificate file (it must be mounted in the container)
- `TLS_KEY` - If `TLS_ENABLED` is 1 - provide the path to the TLS key file (it must be mounted in the container)

To run that image - use the following command:
```bash
# Authenticate to Github Docker Registry - replace USERNAME with your Github username
echo ${GITHUB_ACCESS_TOKEN} | docker login ghcr.io -u USERNAME --password-stdin

# Pull and run the Docker image 
docker run --name gizmosql-slim \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --volume $(pwd)/license_files:/opt/gizmosql/license_files \
           --env LICENSE_KEY_FILENAME="license_files/license_key.jwt" \
           --env DATABASE_FILENAME="data/some_database.duckdb" \
           --env TLS_ENABLED="0" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull missing \
           ghcr.io/gizmodata/gizmosql:latest-slim
```

See [start_gizmosql_slim.sh](scripts/start_gizmosql_slim.sh) - the container's entrypoint script for more details.
