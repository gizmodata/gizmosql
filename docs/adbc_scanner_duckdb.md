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
- **Example Driver Used:** `libadbc_driver_flightsql.dylib` (for Arrow Flight SQL)

---

## ⚙️Setup ADBC Drivers
You can get ADBC Drivers for your target platform pretty easily using [Columnar](https://columnar.tech)'s [dbc](https://columnar.tech/dbc/) tool - under: "Which data system do you want to connect to?" - choose: "Flight SQL":   
![img.png](img.png)

## 🧪 Example: Query GizmoSQL from DuckDB

You can try the extension right now against a public **GizmoSQL** instance hosted by **GizmoData** — no setup required.

### 1️⃣ Launch DuckDB CLI

```bash
duckdb
```

You should see:

```
DuckDB v1.5.0
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

You’ll need the [ADBC FlightSQL driver](https://columnar.tech/dbc/) for your platform.  
Then, create a secret, and then a connection to the remote GizmoSQL instance:

```sql
CREATE SECRET gizmosql_secret (
     TYPE adbc,
     SCOPE 'grpc+tls://try-gizmosql-adbc.gizmodata.com:31337',
     driver '/Users/philip/Downloads/libadbc_driver_flightsql.dylib',  /* Change this path as needed */
     uri 'grpc+tls://try-gizmosql-adbc.gizmodata.com:31337',
     username 'adbc-scanner',
     password 'QueryDotFarmRules!123'
 );

ATTACH 'grpc+tls://try-gizmosql-adbc.gizmodata.com:31337' AS gizmosql_db (
      TYPE adbc
  );
```

This creates an encrypted and authenticated Flight SQL connection to the **GizmoSQL** service hosted by GizmoData.

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
