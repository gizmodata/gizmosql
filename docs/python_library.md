# 🐍 Python Library

The **`gizmosql`** Python package wraps the GizmoSQL Flight SQL server as a managed subprocess. Install it with `pip`, write three lines of Python, and you have a real network endpoint that any Flight SQL client can talk to.

[![PyPI](https://img.shields.io/pypi/v/gizmosql.svg?logo=PyPI)](https://pypi.org/project/gizmosql/)
[![Source](https://img.shields.io/badge/source-gizmodata%2Fgizmosql--py-blue?logo=github)](https://github.com/gizmodata/gizmosql-py)
[![License](https://img.shields.io/pypi/l/gizmosql.svg)](https://github.com/gizmodata/gizmosql-py/blob/main/LICENSE)

---

## Why a Python wrapper?

GizmoSQL is a *network* SQL endpoint built on Apache Arrow Flight SQL. The point of running it from Python isn't to query it from the same process — the [`duckdb`](https://pypi.org/project/duckdb/) PyPI package is better at in-process queries. The point is to expose a SQL endpoint to **other things**:

- **pytest fixtures** that exercise a real Flight SQL server in CI without Docker.
- **Multi-tool / multi-agent workflows** where Python is the orchestrator and other tools (a JDBC driver, Power BI, AI agents, a separate worker process) all connect to the same server.
- **Notebook demos** that need a live server while showing client-side code.
- **"Expose this DuckDB file to the network"** — three lines of Python and any Flight SQL client can query it.

## Install

```bash
pip install gizmosql
# or, with the optional ADBC client for Server.connect():
pip install 'gizmosql[adbc]'
```

The package itself has zero runtime dependencies. The `gizmosql_server` binary is downloaded on first use (~10 MB compressed) into `~/.cache/gizmosql/`; subsequent starts use the cached copy.

> **Supported platforms:** macOS arm64, Linux amd64, Linux arm64, Windows amd64, Windows arm64. Python 3.10+.

## Quick start

```python
import gizmosql

with gizmosql.Server(password="tiger") as srv:
    print(srv.url)            # grpc+tcp://127.0.0.1:42173
    print(srv.username, srv.password)
    # ... point any Flight SQL client at srv.url ...
```

The context manager:
1. Downloads the matching server binary on first use.
2. Picks a free TCP port (so multiple parallel test workers don't collide).
3. Starts the subprocess and blocks until it's accepting connections.
4. SIGTERMs it on exit, escalating to SIGKILL if it doesn't respond.

### Querying from the same Python process

With the `adbc` extra, you can also query the server from the same Python that started it:

```python
import gizmosql

with gizmosql.Server(password="tiger") as srv:
    with srv.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT GIZMOSQL_VERSION(), GIZMOSQL_EDITION()")
        print(cur.fetchall())
```

## Versioning

`gizmosql==X.Y.Z` ships in lock-step with the matching GizmoSQL server tag `vX.Y.Z` — that's the version it'll download and run unless you override:

```python
gizmosql.Server(version="v1.25.1")            # pin a specific server version
gizmosql.Server(channel="lts")                # use the LTS channel
gizmosql.Server(channel="lts", version="v1.24.4")  # both
```

The `GIZMOSQL_VERSION` environment variable also overrides the default. See the [LTS Channel guide](lts_channel.md) for when to pick LTS.

## Common configurations

```python
# Persistent on-disk database with TPC-H sample loaded at startup.
gizmosql.Server(
    password="tiger",
    database_filename="warehouse.duckdb",
    init_sql_commands="CALL dbgen(sf=1);",  # ~1 GB on disk
)

# Listen on all interfaces (default is loopback only).
gizmosql.Server(password="tiger", host="0.0.0.0")

# Pin specific ports (default 0 = OS-picked free port).
gizmosql.Server(password="tiger", port=31337, health_port=31338)

# Forward arbitrary CLI flags to gizmosql_server.
gizmosql.Server(
    password="tiger",
    extra_args=["--print-queries", "--query-log-level", "debug"],
)
```

Any flag accepted by the `gizmosql_server` binary can be passed via `extra_args` — see the [Configuration & Environment Variables reference](index.md#configuration-environment-variables) for the full list.

## pytest fixture pattern

This is what the package was built for. A session-scoped fixture spins up one server for the whole test run; ports are auto-picked, so parallel `pytest-xdist` workers don't collide.

```python
# conftest.py
import pytest
import gizmosql

@pytest.fixture(scope="session")
def gizmosql_server(tmp_path_factory):
    db = tmp_path_factory.mktemp("gizmosql") / "test.duckdb"
    with gizmosql.Server(
        password="testpw",
        database_filename=str(db),
        # Optional: bake a fixture dataset into the server.
        init_sql_commands="""
            CREATE TABLE users (id INTEGER, name VARCHAR);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
        """,
    ) as srv:
        yield srv
```

```python
# test_my_app.py
def test_my_jdbc_client_can_query(gizmosql_server):
    # Hand srv.url to whatever client you're testing.
    my_client.connect(gizmosql_server.url, "gizmosql", "testpw")
    rows = my_client.execute("SELECT * FROM users").fetchall()
    assert len(rows) == 2

def test_via_adbc(gizmosql_server):
    with gizmosql_server.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM users")
        assert cur.fetchone()[0] == 2
```

## Multi-agent / multi-client pattern

```python
import asyncio, gizmosql

async def agent(srv, agent_id):
    with srv.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT 'agent-{agent_id}' AS who, count(*) FROM lineitem")
        return cur.fetchall()

async def main():
    with gizmosql.Server(
        password="tiger",
        database_filename="warehouse.duckdb",
        init_sql_commands="CALL dbgen(sf=0.1);",
    ) as srv:
        results = await asyncio.gather(*(agent(srv, i) for i in range(8)))
        for r in results:
            print(r)

asyncio.run(main())
```

## Server reference

### `gizmosql.Server(...)` parameters

| Parameter | Default | Description |
|---|---|---|
| `database_filename` | `None` (in-memory) | Path passed to `--database-filename`. |
| `username` | `"gizmosql"` | Auth username. |
| `password` | random 32-char token | Auth password. Defaults to a random token so a forgotten `Server()` doesn't leave an open auth-less endpoint. |
| `host` | `"127.0.0.1"` | Bind interface. Use `"0.0.0.0"` to listen on all interfaces. |
| `port` | `0` (auto) | Flight SQL TCP port. `0` asks the OS for a free port. |
| `health_port` | `0` (auto) | Plaintext health-check port (for K8s probes). `0` = auto. |
| `channel` | `"stable"` | `"stable"` or `"lts"`. |
| `version` | package version | Server release tag, e.g. `"v1.25.1"`. |
| `init_sql_commands` | `None` | SQL run at server startup, semicolon-separated. |
| `extra_args` | `()` | Additional argv passed verbatim to `gizmosql_server`. |
| `extra_env` | `None` | Environment overrides (merged onto `os.environ`). |
| `startup_timeout` | `30.0` | Seconds to wait for the server to start accepting connections. |
| `stdout`, `stderr` | parent stderr | Where the server's logs go. Captured streams (pytest, Jupyter) auto-fall-back to `DEVNULL`. |
| `binary` | auto-downloaded | Override the path to the server executable. |

### Properties and methods

```python
srv.url                # "grpc+tcp://host:port"
srv.host, srv.port
srv.username, srv.password
srv.pid                # OS pid of the running subprocess (None if not started)
srv.is_running()       # bool
srv.start()            # idempotent; the with-block calls this for you
srv.stop(timeout=10.0) # SIGTERM, escalating to SIGKILL on timeout
srv.connect(**kwargs)  # adbc_driver_gizmosql.dbapi.Connection (requires [adbc] extra)
srv.config             # dataclass with the resolved configuration
```

## Environment variables

| Variable | Purpose |
|---|---|
| `GIZMOSQL_CACHE_DIR` | Override the binary cache root (default `~/.cache/gizmosql/` on POSIX, `%LOCALAPPDATA%\gizmosql\Cache` on Windows). |
| `GIZMOSQL_VERSION` | Default server version when `Server(version=...)` isn't passed. |
| `GIZMOSQL_RELEASE_REPO` | GitHub repo to fetch releases from (default `gizmodata/gizmosql`). |
| `GIZMOSQL_RELEASE_BASE_URL` | Full base URL override; for testing against a staging release page. |

## Limitations

- **Subprocess only.** The server runs in its own process and clients (including `Server.connect()`) talk to it over the loopback Flight SQL endpoint. This is the right architecture for the intended use cases — the wrapper is a process manager, not an embedded SQL engine.
- **Pre-built binaries** for: macOS arm64, Linux amd64, Linux arm64, Windows amd64, Windows arm64. Other platforms aren't supported.
- **Not a drop-in for `duckdb`.** If you just want in-process SQL from Python, install `duckdb` directly. This package is for the case where you specifically need a Flight SQL endpoint.

## Links

- 📦 [PyPI: `gizmosql`](https://pypi.org/project/gizmosql/)
- 🐙 [Source: `gizmodata/gizmosql-py`](https://github.com/gizmodata/gizmosql-py)
- 🐛 [Issues / requests](https://github.com/gizmodata/gizmosql-py/issues)
- 🚀 [Quick Start (CLI / shell)](quickstart.md)
- 🦆 [LTS Channel guide](lts_channel.md)
- 📦 [Install picker (Homebrew, Docker, etc.)](https://gizmodata.com/gizmosql/install)
