# 🚀 Querying Remote Databases with the ADBC Scanner Community Extension for DuckDB

The **ADBC Scanner** is a new [DuckDB Community Extension](https://duckdb.org/community_extensions) developed by **[Query.Farm](https://query.farm/duckdb_extension_adbc_scanner.html)** that allows you to connect to and query remote databases directly from DuckDB — via the **[Apache Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/)** framework.

With this extension, you can connect DuckDB to any ADBC-compatible source (like **GizmoSQL**, **Snowflake**, **PostgreSQL**, or **SQLite**) and run SQL queries *remotely*, as if they were local tables.

You can even use ADBC Scanner from a GizmoSQL server - allowing you to connect to remote databases from within GizmoSQL itself, including to other GizmoSQL instances!

---

## 🧩 Overview

- **Extension:** `adbc_scanner`  
- **Author:** Query.Farm  
- **Category:** Community Extension  
- **Purpose:** Query remote databases over ADBC  
- **Supported Backends:** Any database with a compatible ADBC driver  
- **Example Driver Used:** `gizmosql` (the [native GizmoSQL ADBC driver](https://github.com/gizmodata/gizmosql-adbc))

---

## ⚙️ Setup: the GizmoSQL ADBC Driver

For connecting to GizmoSQL, use the [native GizmoSQL ADBC driver](https://github.com/gizmodata/gizmosql-adbc)
— the same Go-backed shared library that powers
[`adbc-driver-gizmosql`](https://pypi.org/project/adbc-driver-gizmosql/) 2.x. Compared to the generic
`flightsql` driver it adds `gizmosql://` URIs (TLS by default), DDL/DML auto-detection with immediate
server-side execution, `RETURNING` support, and OAuth/SSO — while keeping everything the Flight SQL
driver provides.

> **Note:** The GizmoSQL driver isn't available via [Columnar](https://columnar.tech)'s
> [dbc](https://columnar.tech/dbc/) installer yet — install it from the GitHub release artifacts as
> shown below. (`dbc` remains a great way to install *other* drivers — Snowflake, PostgreSQL,
> `flightsql`, etc.)

Download the shared library for your platform from the
[gizmosql-adbc releases](https://github.com/gizmodata/gizmosql-adbc/releases/latest), then register it
with a driver manifest so it can be loaded by name:

```bash
VERSION="v2.0.8"
PLATFORM="macos_arm64"   # or: linux_amd64, linux_arm64, macos_amd64, windows_amd64, windows_arm64

curl -LO "https://github.com/gizmodata/gizmosql-adbc/releases/download/${VERSION}/libadbc_driver_gizmosql-${VERSION}-${PLATFORM}.tar.gz"
tar xzf "libadbc_driver_gizmosql-${VERSION}-${PLATFORM}.tar.gz"
cd "libadbc_driver_gizmosql-${VERSION}-${PLATFORM}"

# Install the shared library + manifest into your user-level ADBC driver directory:
#   Linux:   ~/.config/adbc/drivers/
#   macOS:   ~/Library/Application Support/ADBC/Drivers/
#   Windows: %LOCALAPPDATA%\ADBC\Drivers\
DRIVER_DIR="${HOME}/Library/Application Support/ADBC/Drivers"   # macOS example
mkdir -p "${DRIVER_DIR}"
cp libadbc_driver_gizmosql.* "${DRIVER_DIR}/"
sed -e "s|@VERSION@|${VERSION#v}|" -e "s|@PREFIX@|${DRIVER_DIR}|g" \
    gizmosql.toml.in > "${DRIVER_DIR}/gizmosql.toml"
```

Any directory listed in the `ADBC_DRIVER_PATH` environment variable works too. Once the manifest is in
place, every ADBC driver manager (including the ADBC Scanner) can load the driver by name: `gizmosql`.

> **Docker users:** the `-adbc` variants of the GizmoSQL image
> (e.g. `gizmodata/gizmosql:latest-adbc`, `:latest-slim-adbc`) ship with the GizmoSQL driver (plus a
> curated set of `dbc`-installed drivers) preinstalled system-wide in `/etc/adbc/drivers` — inside
> those containers, `driver 'gizmosql'` just works.

## 🧪 Example: Query GizmoSQL from DuckDB

You can try the extension right now against a public **GizmoSQL** instance hosted by **GizmoData** — no setup required.

### 1️⃣ Launch DuckDB CLI

```bash
duckdb
```

You should see:

```
DuckDB v1.5.5
Connected to a transient in-memory database.
```

---

### 2️⃣ Install and Load the Extension

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

-- Run this to keep your extensions up to date...     
UPDATE EXTENSIONS;     
```

This downloads and registers the ADBC Scanner extension for your DuckDB environment.

---

### 3️⃣ Connect to a Remote GizmoSQL Instance

Create a secret and a connection to the remote GizmoSQL instance:

```sql
CREATE SECRET gizmosql_secret (
     TYPE adbc,
     SCOPE 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337',
     driver 'gizmosql',
     uri 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337',
     username 'adbc-scanner',
     password 'QueryDotFarmRules!123'
 );

ATTACH 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337' AS gizmosql_db (
      TYPE adbc
  );
```

This creates an encrypted and authenticated connection to the **GizmoSQL** service hosted by GizmoData
(`gizmosql://` URIs use TLS by default — append `?transport=tcp` for plaintext).

#### Connecting with Self-Signed Certificates (TLS Skip Verify)

If your GizmoSQL server uses a self-signed certificate (common in development or internal environments), you can skip TLS certificate verification by adding `extra_options` to the secret:

```sql
CREATE SECRET gizmosql_secret (
     TYPE adbc,
     SCOPE 'gizmosql://localhost:31337',
     driver 'gizmosql',
     uri 'gizmosql://localhost:31337',
     username 'gizmosql_user',
     password 'gizmosql_password',
     extra_options MAP {
          'adbc.flight.sql.client_option.tls_skip_verify': 'true'
      }
 );

ATTACH 'gizmosql://localhost:31337' AS gizmosql_db (
      TYPE adbc
  );
```

> **Note:** Only use `tls_skip_verify` for development or trusted internal environments — not in production.

---

### 4️⃣ Run a Remote Query!

Now you can query remote data as if it were local.

```sql
-- Make the remote instance first on the search path so you don't have to type: catalog.schema.table for each SQL statement...
USE gizmosql_db;

-- Select from the table as if it were local
SELECT *
FROM region;
```

Output:

```
┌─────────────┬─────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ r_regionkey │   r_name    │                                                      r_comment                                                      │
│    int32    │   varchar   │                                                       varchar                                                       │
├─────────────┼─────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│           0 │ AFRICA      │ ar packages. regular excuses among the ironic requests cajole fluffily blithely final requests. furiously express p │
│           1 │ AMERICA     │ s are. furiously even pinto bea                                                                                     │
│           2 │ ASIA        │ c, special dependencies around                                                                                      │
│           3 │ EUROPE      │ e dolphins are furiously about the carefully                                                                        │
│           4 │ MIDDLE EAST │  foxes boost furiously along the carefully dogged tithes. slyly regular orbits according to the special epit        │
└─────────────┴─────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔁 Example: GizmoSQL to GizmoSQL

Because GizmoSQL runs DuckDB under the hood, the same extension works **inside a GizmoSQL server** —
letting one GizmoSQL instance query another (or any other ADBC source) with plain SQL, submitted
through any GizmoSQL client (ADBC, JDBC, the CLI, etc.).

The easiest way is to run one of the `-adbc` Docker image variants
(e.g. `gizmodata/gizmosql:latest-slim-adbc`), which have the GizmoSQL ADBC driver preinstalled where
the server's embedded DuckDB can find it by name. (On a bare-metal server, install the driver as shown
above and set `ADBC_DRIVER_PATH` in the server's environment.)

```bash
docker run --name gizmosql \
           --detach --rm --tty --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_USERNAME="gizmosql_user" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           gizmodata/gizmosql:latest-slim-adbc
```

Then, from a client session connected to that server, run:

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

CREATE OR REPLACE SECRET remote_gizmosql (
     TYPE adbc,
     SCOPE 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337',
     driver 'gizmosql',
     uri 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337',
     username 'adbc-scanner',
     password 'QueryDotFarmRules!123'
 );

ATTACH IF NOT EXISTS 'gizmosql://try-gizmosql-adbc.gizmodata.com:31337' AS remote_db (
      TYPE adbc
  );

-- Query the remote GizmoSQL instance through the local one:
SELECT * FROM remote_db.main.region ORDER BY r_regionkey;
```

Your local GizmoSQL server federates the query to the remote GizmoSQL instance and streams the results
back to your client as Arrow record batches — GizmoSQL all the way down. 🐢

---

## ⚙️ Under the Hood

The `adbc_scan()` function executes SQL remotely via the ADBC driver and returns results as Arrow RecordBatches.  
That means:

- Results are **zero-copy streamed** into DuckDB via Arrow IPC.  
- Queries can push computation to the remote side when supported.  
- You can **join local and remote data** seamlessly.

---

## 🔗 Related Resources

- [Official Query.Farm Documentation](https://query.farm/duckdb_extension_adbc_scanner.html)
- [DuckDB Community Extensions Directory](https://duckdb.org/community_extensions)
- [Apache Arrow ADBC Specification](https://arrow.apache.org/adbc/)
- [Query.Farm GitHub](https://github.com/Query-farm)
- [GizmoSQL Open Source Project](https://github.com/gizmodata/gizmosql)
- [GizmoData Site](https://gizmodata.com)

---

## 💡 Next Steps

- Try substituting other remote queries - like this one that does predicate pushdown to the remote GizmoSQL database connection:
  ```sql
  SELECT *
    FROM lineitem 
   WHERE l_linenumber = 3
   LIMIT 100;
  ```

- Explore **pushdown capabilities** with complex filters or aggregations.
- Combine ADBC remote tables with local Parquet, CSV, or in-memory data for hybrid analytics.
- Experiment with other ADBC drivers: PostgreSQL, SQLite, Snowflake, etc.

---

### 🧠 Summary

The **ADBC Scanner** extension by **Query.Farm** opens up a new era of *federated analytics* in DuckDB —  
letting you treat **any remote database** as a native data source, with **Arrow Flight SQL performance** and **DuckDB simplicity**.

> “Query remote data at local speed — all from the DuckDB prompt.” 🦆⚡
