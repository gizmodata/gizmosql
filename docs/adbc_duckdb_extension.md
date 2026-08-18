# 🧲 Querying GizmoSQL with the Columnar ADBC Extension for DuckDB

The **ADBC extension** is a [DuckDB Community Extension](https://duckdb.org/community_extensions)
developed by **[Columnar](https://columnar.tech)** (the team behind the
[`dbc`](https://columnar.tech/dbc/) driver installer) that connects DuckDB to any system with an
[ADBC](https://arrow.apache.org/adbc/) driver — including **GizmoSQL**, via the
[native GizmoSQL ADBC driver](https://github.com/gizmodata/gizmosql-adbc).

It complements Query.Farm's [ADBC Scanner extension](adbc_scanner_duckdb.md): the Columnar extension
is **connection-profile-based** (reusable TOML profiles, no credentials in your SQL) and additionally
supports **writes** — `INSERT`, `COPY`, and `CREATE TABLE AS (SELECT ...)` against the attached
database.

---

## 🧩 Overview

- **Extension:** `adbc`
- **Author:** Columnar ([columnar-tech/duckdb-adbc-client](https://github.com/columnar-tech/duckdb-adbc-client))
- **Category:** Community Extension
- **Purpose:** Read from *and write to* remote databases over ADBC, via connection profiles
- **Supported statements:** catalog lookups, `SELECT`, `INSERT`, `COPY`, `CREATE TABLE AS (SELECT ...)`
- **Driver used here:** `gizmosql` (the native GizmoSQL ADBC driver)

---

## ⚙️ Setup

### 1️⃣ Install the GizmoSQL ADBC driver

The GizmoSQL driver isn't available via `dbc` yet — install it from the
[gizmosql-adbc release artifacts](https://github.com/gizmodata/gizmosql-adbc/releases/latest) as shown
in the [ADBC Scanner guide](adbc_scanner_duckdb.md) (see *Setup: the GizmoSQL ADBC Driver*):
copy the shared library into your ADBC driver directory and generate `gizmosql.toml` from the bundled
manifest template. Once registered, the driver loads by name: `gizmosql`.

> **Docker users:** the `-adbc` GizmoSQL image variants (e.g. `gizmodata/gizmosql:latest-slim-adbc`)
> ship with the driver preinstalled in `/etc/adbc/drivers`.

### 2️⃣ Create a connection profile

The Columnar extension connects through **ADBC connection profiles** — TOML files stored in:

- **Linux:** `~/.config/adbc/profiles/`
- **macOS:** `~/Library/Application Support/ADBC/Profiles/`
- **Windows:** `%LOCALAPPDATA%\ADBC\Profiles\`

Create `gizmosql_demo.toml` in that directory, pointing at the public **GizmoSQL** instance hosted by
GizmoData:

```toml
profile_version = 1
driver = "gizmosql"

[Options]
uri = "gizmosql://try-gizmosql-adbc.gizmodata.com:31337"
username = "adbc-scanner"
password = "QueryDotFarmRules!123"
```

`gizmosql://` URIs use TLS by default (append `?transport=tcp` for plaintext). For a development
server with a self-signed certificate, add:

```toml
"adbc.flight.sql.client_option.tls_skip_verify" = "true"
```

---

## 🧪 Example: Query GizmoSQL from DuckDB

### 1️⃣ Install and load the extension

```sql
INSTALL adbc FROM community;
LOAD adbc;
```

### 2️⃣ Ad-hoc queries with `read_adbc`

`read_adbc()` executes the SQL **on the remote GizmoSQL server** and streams the results back as
Arrow record batches — ideal when you want remote filtering/aggregation (full pushdown, since the
query text runs remotely):

```sql
SELECT *
  FROM read_adbc('profile://gizmosql_demo',
                 'SELECT * FROM region ORDER BY r_regionkey');
```

Output:

```
┌─────────────┬─────────────┬──────────────────────────────────────────────────────────────────────┐
│ r_regionkey │   r_name    │                              r_comment                               │
│    int32    │   varchar   │                               varchar                                │
├─────────────┼─────────────┼──────────────────────────────────────────────────────────────────────┤
│           0 │ AFRICA      │ ar packages. regular excuses among the ironic requests cajole fluf…  │
│           1 │ AMERICA     │ s are. furiously even pinto bea                                      │
│           2 │ ASIA        │ c, special dependencies around                                       │
│           3 │ EUROPE      │ e dolphins are furiously about the carefully                         │
│           4 │ MIDDLE EAST │  foxes boost furiously along the carefully dogged tithes. slyly re…  │
└─────────────┴─────────────┴──────────────────────────────────────────────────────────────────────┘
```

### 3️⃣ Attach GizmoSQL as a database

```sql
ATTACH 'profile://gizmosql_demo' AS gizmosql_db (TYPE adbc);
USE gizmosql_db.main;

SELECT r_regionkey, r_name FROM region ORDER BY r_regionkey;
```

Once attached, the remote GizmoSQL catalog behaves like a local DuckDB database — `SHOW ALL TABLES`,
joins against local data, and (with write permissions on the server) `INSERT` / `COPY` /
`CREATE TABLE AS` all work.

> **Note:** attached-table scans do not push predicates/projections to the server — use `read_adbc()`
> when you want the heavy lifting done remotely.

---

## 🔁 GizmoSQL to GizmoSQL

As with the [ADBC Scanner](adbc_scanner_duckdb.md), this extension also
works *inside* a GizmoSQL server (GizmoSQL runs DuckDB under the hood) — `INSTALL adbc FROM community`
in a GizmoSQL session and attach another GizmoSQL instance by profile. The `-adbc` Docker image
variants have the driver preinstalled; place profile TOML files where the server's user can read them
(`~/.config/adbc/profiles/` for the container user, or a directory you mount).

---

## 🔗 Related Resources

- [Columnar's announcement blog post](https://columnar.tech/blog/announcing-duckdb-adbc-extension/)
- [columnar-tech/duckdb-adbc-client on GitHub](https://github.com/columnar-tech/duckdb-adbc-client)
- [`dbc` — Columnar's ADBC driver installer](https://columnar.tech/dbc/)
- [Native GizmoSQL ADBC driver (gizmosql-adbc)](https://github.com/gizmodata/gizmosql-adbc)
- [ADBC Scanner extension guide](adbc_scanner_duckdb.md)
- [Apache Arrow ADBC Specification](https://arrow.apache.org/adbc/)
