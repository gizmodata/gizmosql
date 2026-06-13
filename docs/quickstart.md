# 🚀 Quick Start

Get a GizmoSQL server up and running, then run your first query — should take about a minute on a typical laptop.

---

## 1. Install GizmoSQL

The fastest path is the install picker on **[gizmodata.com/gizmosql/install](https://gizmodata.com/gizmosql/install)**: pick your OS, channel (Latest / LTS), and method, and copy the one-line command.

The shortest version (macOS or Linux):

```bash
curl -fsSL https://install.gizmosql.com/install.sh | sh
```

Windows (PowerShell):

```powershell
iwr https://install.gizmosql.com/install.ps1 -OutFile install.ps1
.\install.ps1
```

The script installs `gizmosql_server` and `gizmosql_client` into a writable directory (`~/.local/bin` on POSIX; `%LOCALAPPDATA%\Programs\GizmoSQL` on Windows) and prints how to add it to your `PATH` if it isn't already.

> **Want the LTS channel?** Append `-s -- --channel lts` (POSIX) or `-Channel lts` (PowerShell), or pick LTS in the install picker. See the [LTS Channel guide](lts_channel.md) for the rationale.

Other install methods (Homebrew, Docker, signed Windows MSI, direct download) are also covered by the picker.

---

## 2. Start a server

Pick a username and password, then start the server. By default it opens a TLS port on `31337` and binds an in-memory DuckDB database.

```bash
GIZMOSQL_PASSWORD=tiger gizmosql_server --username scott
```

You'll see a startup banner ending in something like:

```
INFO ... GizmoSQL server version: v1.29.0 - with engine: DuckDB - will listen on grpc+tcp://0.0.0.0:31337
```

> **Already have a server running?** GizmoSQL binds port `31337` by default, so if another GizmoSQL instance (or anything else) is already listening there, startup fails with an "address already in use" / bind error instead of starting. Stop the other server, or pick a different port with `--port 31338` (and pass the same `--port` to `gizmosql_client` when you connect).

Want to persist data? Add `--database-filename my_database.duckdb`. For TLS, see the [Security Guide](security.md).

> **Tip:** GizmoSQL is happy as a long-running shared SQL server, an embedded process inside an app, or [a mobile app on iOS](https://apps.apple.com/us/app/gizmosql/id6761951280). Same protocol, same query layer.

---

## 3. Connect and run a query

In a second terminal, open the client shell:

```bash
GIZMOSQL_PASSWORD=tiger gizmosql_client --username scott
```

Then run something:

```sql
SELECT GIZMOSQL_VERSION(), GIZMOSQL_EDITION();
```

```
┌────────────────────┬────────────────────┐
│ GIZMOSQL_VERSION() │ GIZMOSQL_EDITION() │
│      varchar       │      varchar       │
├────────────────────┼────────────────────┤
│ v1.29.0            │ Core               │
├────────────────────┴────────────────────┤
│ 1 row  2 columns                        │
└─────────────────────────────────────────┘
```

A non-interactive one-liner works too:

```bash
GIZMOSQL_PASSWORD=tiger gizmosql_client --username scott \
  --command "SELECT 'Hello from GizmoSQL' AS greeting;"
```

Or if you'd rather connect from your own code, GizmoSQL speaks **Apache Arrow Flight SQL** — point any Flight SQL client at `grpc+tcp://localhost:31337` with HTTP Basic auth (`scott` / `tiger`).

---

## Try it with sample data (TPC-H)

Want something more interesting than `SELECT 1`? DuckDB's TPC-H extension is preloaded by GizmoSQL, so you can generate a real-looking warehouse dataset with one call:

**Option A — generate from the client** (good for ad-hoc exploration):

```bash
GIZMOSQL_PASSWORD=tiger gizmosql_client --username scott --command "CALL dbgen(sf=1);"
```

**Option B — bake it into the server** so the data is there as soon as the server starts (good when you want a persistent demo database every client connects to):

```bash
GIZMOSQL_PASSWORD=tiger gizmosql_server --username scott \
  --database-filename tpch.duckdb \
  --init-sql-commands "CALL dbgen(sf=1);"
```

> **Heads-up: this writes to your disk.** The TPC-H scale factor roughly equals the dataset size in GB:
>
> | `sf=` | Approx. on-disk size | Good for |
> |---|---|---|
> | `0.01` | ~10 MB | Smoke tests |
> | `0.1`  | ~100 MB | Quick exploration on a laptop |
> | `1`    | ~1 GB | The standard TPC-H benchmark size |
> | `10`   | ~10 GB | Bigger laptop / small server |
> | `100`+ | ~100 GB+ | A real warehouse box |
>
> Pick a smaller scale factor for laptop testing. With Option B you'll also need `--database-filename` (otherwise the data lives in an in-memory DB that vanishes when the server stops).

Once loaded, you have eight standard TPC-H tables to play with — `customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, `supplier`. Try a quick join:

```sql
SELECT n_name, COUNT(*) AS customer_count
FROM nation, customer
WHERE n_nationkey = c_nationkey
GROUP BY n_name
ORDER BY customer_count DESC
LIMIT 5;
```

```
┌─────────┬────────────────┐
│ n_name  │ customer_count │
│ varchar │     bigint     │
├─────────┼────────────────┤
│ IRAN    │             72 │
│ MOROCCO │             72 │
│ CANADA  │             69 │
│ BRAZIL  │             68 │
│ JAPAN   │             67 │
├─────────┴────────────────┤
│ 5 rows  2 columns        │
└──────────────────────────┘
```

For the full 22-query TPC-H benchmark suite (and our 1 TB result on a single machine for $0.17), see [One Trillion Row Challenge](one_trillion_row_challenge.md) and the [GizmoSQL benchmark page](https://gizmodata.com/gizmosql).

---

## What's next?

| Want to… | Read this |
|---|---|
| Connect from Python | [Python ADBC Driver](python_adbc.md) |
| Use the interactive shell features | [Client Shell](client.md) |
| Bulk-load data fast | [Bulk Ingestion](bulk_ingestion.md) |
| Set up TLS / authentication | [Security Guide](security.md) |
| Use OAuth / SSO tokens | [Token Authentication](token_authentication.md) |
| Pin a slower-moving DuckDB engine | [LTS Channel](lts_channel.md) |
| See every CLI flag and env var | [Configuration reference](index.md#configuration-environment-variables) |
| Compare Core vs. Enterprise | [Editions](editions.md) |

---

## Help & feedback

- 💬 Issues, questions, or requests: [github.com/gizmodata/gizmosql/issues](https://github.com/gizmodata/gizmosql/issues)
- 📦 Latest releases: [github.com/gizmodata/gizmosql/releases](https://github.com/gizmodata/gizmosql/releases)
- 🌐 Product page: [gizmodata.com/gizmosql](https://gizmodata.com/gizmosql)
