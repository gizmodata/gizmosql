# GizmoSQL Client Shell

The GizmoSQL Client (`gizmosql_client`) is an interactive SQL shell for connecting to a GizmoSQL server via Arrow Flight SQL. It supports interactive queries, scripted workflows, multiple output formats, and OAuth/SSO authentication.

## Quick Start

```bash
# Interactive session (will prompt for password)
gizmosql_client --host my-server.example.com --username admin

# Same thing with short flags
gizmosql_client --host my-server.example.com --username admin

# Connect via URI
gizmosql_client 'gizmosql://my-server.example.com:31337?username=admin'

# Connect via URI with TLS and OAuth
gizmosql_client 'gizmosql://my-server.example.com:31337?useEncryption=true&authType=external'

# Start without connecting, then use .connect interactively
gizmosql_client

# Run a single query (uses env var for password)
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --command "SELECT * FROM employees"

# Pipe SQL from stdin
echo "SELECT 42 AS answer;" | GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --quiet

# Run SQL from a file
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --file queries.sql
```

## Connection Options

Connect using individual flags or a connection URI. The `--uri` option cannot be combined with `--host`, `--port`, `--username`, `--password`, `--tls`, `--tls-roots`, `--tls-skip-verify`, or `--auth-type`.

| Option | Short | Env Var | Default | Description |
|--------|-------|---------|---------|-------------|
| `--uri` | | | | Connection URI (see [Connection URI](#connection-uri) below) |
| `--host` | `-h` | `GIZMOSQL_HOST` | `localhost` | Server hostname |
| `--port` | `-p` | `GIZMOSQL_PORT` | `31337` | Server port |
| `--username` | `-u` | `GIZMOSQL_USER` | | Username |
| `--password` | `-W` | `GIZMOSQL_PASSWORD` | | Force password prompt |
| `--tls` | | `GIZMOSQL_TLS` | off | Enable TLS connection |
| `--tls-roots` | | `GIZMOSQL_TLS_ROOTS` | | Path to CA certificate file (PEM) |
| `--tls-skip-verify` | | | off | Skip server certificate verification |
| `--mtls-cert` | | | | Client certificate for mutual TLS (PEM) |
| `--mtls-key` | | | | Client private key for mutual TLS (PEM) |
| `--auth-type` | | | `password` | Auth type: `password` or `external` (OAuth/SSO) |
| `--oauth-port` | | `GIZMOSQL_OAUTH_PORT` | `31339` | OAuth HTTP server port (fallback) |

### Connection URI

The URI can be passed via the `--uri` flag or as a positional argument:

```bash
# As positional argument
gizmosql_client 'gizmosql://my-server.example.com:31337?username=admin'

# With --uri flag
gizmosql_client --uri 'gizmosql://my-server.example.com:31337?username=admin'

# TLS + OAuth via URI
gizmosql_client 'gizmosql://my-server.example.com:31337?useEncryption=true&authType=external'
```

> **Note:** Always quote URIs in shell commands to prevent `&` from being interpreted by the shell.

**URI format:** `gizmosql://HOST:PORT[?param1=value1&param2=value2]`

| Parameter | Values | Description |
|-----------|--------|-------------|
| `username` | string | Username for authentication |
| `useEncryption` | `true`/`false` | Enable TLS (default: `false`) |
| `disableCertificateVerification` | `true`/`false` | Skip TLS cert verification |
| `tlsRoots` | path | Path to CA certificate file (PEM) |
| `authType` | `password`/`external` | Auth type (default: `password`) |

### Password Resolution

Like `psql`, the password **cannot** be passed directly as a command-line argument value. This is intentional to avoid exposing passwords in shell history and process listings. The password is resolved in the following order:

1. `GIZMOSQL_PASSWORD` environment variable
2. Interactive prompt (if connected to a terminal and a username is provided)

Use `--password` (or `-W`) to force the interactive password prompt (even if `GIZMOSQL_PASSWORD` is set):

```bash
gizmosql_client --host localhost --username admin --password
Password:
```

### TLS Options

| Option | Env Var | Description |
|--------|---------|-------------|
| `--tls` | `GIZMOSQL_TLS` | Enable TLS connection |
| `--tls-roots` | `GIZMOSQL_TLS_ROOTS` | Path to CA certificate file (PEM) |
| `--tls-skip-verify` | | Skip server certificate verification |
| `--mtls-cert` | | Client certificate for mutual TLS (PEM) |
| `--mtls-key` | | Client private key for mutual TLS (PEM) |

**TLS example:**

```bash
gizmosql_client --host my-server.example.com --username admin \
  --tls --tls-roots /path/to/ca.pem
```

**Mutual TLS example:**

```bash
gizmosql_client --host my-server.example.com --username admin \
  --tls --tls-roots /path/to/ca.pem \
  --mtls-cert /path/to/client.crt --mtls-key /path/to/client.key
```

## OAuth / SSO Authentication

For servers configured with [OAuth/SSO](oauth_sso_setup.md), use `--auth-type external` to authenticate via browser-based login:

```bash
gizmosql_client --host my-server.example.com --auth-type external
```

This opens your default browser to the identity provider's login page. After authentication, the client automatically receives the token and connects.

The client automatically discovers the OAuth endpoint URL from the server via a discovery handshake. This ensures the correct URL is used even when the server's OAuth HTTP port uses a different TLS setting than the gRPC port (e.g., `--oauth-disable-tls`). If discovery is unavailable (e.g., connecting to an older server), the client falls back to constructing the URL from `--oauth-port` and the connection's TLS setting.

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--auth-type` | | `password` | Auth type: `password` or `external` |
| `--oauth-port` | `GIZMOSQL_OAUTH_PORT` | `31339` | OAuth HTTP server port (used as fallback if discovery is unavailable) |

**Non-interactive OAuth** (for scripted workflows):

```bash
gizmosql_client --host my-server.example.com --auth-type external \
  --command "SELECT current_user"
```

The browser login still occurs, but after authentication the query executes and the client exits.

## Input Modes

### Interactive Mode

When launched without `--command` or `--file` and connected to a terminal, the client starts an interactive REPL with line editing and history.

If connection parameters (`--host`, `--username`, or their env vars) are provided, the client connects immediately:

```
GizmoSQL Client v1.19.7
Connected to localhost:31337
Type '.help' for help, '.quit' to exit.

gizmosql> SELECT * FROM employees WHERE dept = 'Engineering';
┌────────┬─────────┬─────────────┬────────┐
│   id   │  name   │    dept     │ salary │
│ bigint │ varchar │   varchar   │ double │
├────────┼─────────┼─────────────┼────────┤
│      1 │ Alice   │ Engineering │ 120000 │
│      3 │ Charlie │ Engineering │ 110000 │
│      5 │ Eve     │ Engineering │ 130000 │
├────────┴─────────┴─────────────┴────────┤
│ 3 rows  4 columns                       │
└─────────────────────────────────────────┘
```

If **no** connection parameters are provided, the client starts in **disconnected mode**. You can then use `.connect` to establish a connection:

```
GizmoSQL Client v1.19.7
Not connected. Use '.connect HOST PORT USERNAME' to connect.
Type '.help' for help, '.quit' to exit.

gizmosql> .connect localhost 31337 admin
Password:
Connected to localhost:31337
gizmosql> SELECT 42 AS answer;
```

The `.connect` command also accepts a URI format, which supports TLS and OAuth:

```
gizmosql> .connect gizmosql://my-server.example.com:31337?useEncryption=true&username=admin
Password:
Connected to my-server.example.com:31337

gizmosql> .connect gizmosql://my-server.example.com:31337?useEncryption=true&authType=external
Connected to my-server.example.com:31337
```

See [Connection URI](#connection-uri) for the full list of supported URI parameters.

In disconnected mode, attempting to run SQL or server-dependent commands (`.tables`, `.schema`, `.catalogs`) will display an error message directing you to use `.connect`.

### Dynamic Prompt

When connected, the prompt dynamically shows the current catalog and schema, styled in DuckDB's orange color:

```
gizmosql.main> SELECT 1;
```

The prompt updates automatically after `USE`, `ATTACH`, `DETACH`, `.connect`, and other schema-changing statements.

### Syntax Highlighting

SQL input is highlighted as you type, inspired by [DuckDB v1.5's CLI improvements](https://duckdb.org/2026/03/09/announcing-duckdb-150):

- **Keywords** (`SELECT`, `FROM`, `WHERE`, ...) in green
- **Strings** (single-quoted) in yellow
- **Numbers** in magenta
- **Comments** (`--` and `/* */`) in gray
- **Functions** (identifiers followed by `(`) in cyan
- **Unclosed quotes/brackets** in red as error indicators

Toggle with `.highlight on|off`.

**Multi-line SQL** is supported. The prompt changes to `->` to indicate continuation:

```
gizmosql> SELECT
       ->   name,
       ->   salary
       -> FROM employees
       -> WHERE salary > 100000;
```

**History** is saved to `~/.gizmosql_history` and persists across sessions.

**Tab completion** provides context-aware suggestions as you type:

- **Table names**: Type `select * from line` then press `TAB` to complete table names (e.g., `lineitem`)
- **SQL keywords**: Type `sel` then press `TAB` to complete to `select` (case-preserving: `SEL` → `SELECT`)
- **Dot commands**: Type `.ta` then press `TAB` to complete to `.tables`
- **Schema-qualified names**: Type `main.line` then press `TAB` to complete `main.lineitem`
- **Inline hints**: When there's a single match, a gray hint appears inline (press right arrow to accept)

The completion system uses FlightSQL metadata endpoints to fetch table and schema names. The cache is automatically refreshed after DDL statements (`CREATE`, `DROP`, `ALTER`, `ATTACH`, `DETACH`), `CALL`, `USE`, and after `.connect`. Use `.refresh` to manually refresh the cache.

### Command Mode (`--command`)

Execute a SQL statement and exit:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --command "SELECT name, salary FROM employees ORDER BY salary DESC"
```

Multiple statements separated by semicolons:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --command "CREATE TABLE t (x INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
```

### File Mode (`--file`)

Execute SQL from a file:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --file setup.sql
```

### Pipe / Heredoc Mode

Pipe SQL via stdin:

```bash
echo "SELECT 42 AS answer;" | GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --quiet
```

Heredoc for multi-line scripts:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --quiet <<'EOF'
CREATE TABLE metrics (ts TIMESTAMP, value DOUBLE);
INSERT INTO metrics VALUES (now(), 3.14);
SELECT * FROM metrics;
DROP TABLE metrics;
EOF
```

## Output Modes

The client supports 15 output formats, selectable via CLI flags or the `.mode` dot command.

### CLI Shortcuts

| Flag | Mode | Description |
|------|------|-------------|
| `--box` | `box` | Unicode box drawing (default) |
| `--table` | `table` | ASCII borders |
| `--csv` | `csv` | RFC 4180 CSV |
| `--json` | `json` | JSON array of objects |
| `--markdown` | `markdown` | Markdown table |

### All Available Modes

Set via `.mode <name>` in interactive mode:

| Mode | Description | Example Output |
|------|-------------|---------------|
| `box` | Unicode box drawing (default) | `┌──┬──┐` |
| `table` | ASCII `+--+` borders | `+--+--+` |
| `csv` | Comma-separated values | `a,b\n1,2` |
| `tabs` | Tab-separated values | `a\tb` |
| `json` | JSON array of objects | `[{"a":1}]` |
| `jsonlines` | One JSON object per line (NDJSON) | `{"a":1}` |
| `markdown` | Markdown table | `\| a \| b \|` |
| `line` | One value per line (`col = val`) | `a = 1` |
| `list` | Configurable separator | `a\|b` |
| `html` | HTML `<table>` | `<table>...</table>` |
| `latex` | LaTeX tabular | `\begin{tabular}` |
| `insert` | SQL INSERT statements | `INSERT INTO table VALUES(...)` |
| `quote` | SQL-quoted values | `'value'` |
| `ascii` | Unit/record separators (0x1F/0x1E) | Machine-readable |
| `trash` | Discard output (benchmarking) | *(no output)* |

### Examples

**CSV output to a file:**

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --csv --output results.csv --command "SELECT * FROM employees"
```

**JSON output:**

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --json --command "SELECT name, salary FROM employees"
```

```json
[
  {"name": "Alice", "salary": 120000},
  {"name": "Bob", "salary": 85000}
]
```

**Markdown for documentation:**

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --markdown --command "SELECT name, dept FROM employees LIMIT 3"
```

```
| name    | dept        |
| ------- | ----------- |
| Alice   | Engineering |
| Bob     | Marketing   |
| Charlie | Engineering |
```

**No headers:**

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --csv --no-header --command "SELECT name FROM employees"
```

## Result Display

In interactive mode with `box` or `table` output, results are automatically truncated for readability:

- **Split display**: When truncating, shows the first and last rows with 3 dot indicator rows (`·`) in between (DuckDB-style)
- **In-table footer**: Row and column counts are rendered inside the box border in a merged footer row
- **Row truncation**: Large results show 40 rows by default (20 top + `···` + 20 bottom)
- **Column truncation**: Wide tables are capped to the terminal width; columns that don't fit are omitted
- **Column data types**: A type row with DuckDB-friendly names (e.g., `bigint`, `varchar`, `date`) appears centered below each column name
- **Right-aligned numbers**: Numeric columns are right-aligned for readability

**Example output (small result):**

```
┌────────┬─────────┬─────────────┬────────┐
│   id   │  name   │    dept     │ salary │
│ bigint │ varchar │   varchar   │ double │
├────────┼─────────┼─────────────┼────────┤
│      5 │ Eve     │ Engineering │ 130000 │
│      1 │ Alice   │ Engineering │ 120000 │
│      3 │ Charlie │ Engineering │ 110000 │
├────────┴─────────┴─────────────┴────────┤
│ 3 rows  4 columns                       │
└─────────────────────────────────────────┘
```

**Example output (truncated result with split display):**

```
┌───────────┐
│     x     │
│  bigint   │
├───────────┤
│         1 │
│         2 │
│     ·     │
│     ·     │
│     ·     │
│        99 │
│       100 │
├───────────┤
│ 100 rows  │
│ (4 shown) │
└───────────┘
```

| Scenario | Row Limit | Column Width |
|----------|-----------|--------------|
| Interactive, box/table mode | 40 (default) | Fit to terminal width |
| Interactive, other modes (csv, json, etc.) | No limit | No limit |
| Non-interactive (`-c`, `-f`, pipe) | No limit | No limit |
| Output redirected to file (`-o`) | No limit | No limit |

Use `.maxrows` and `.maxwidth` to customize these defaults.

## Built-in Pager

Inspired by [DuckDB v1.5](https://duckdb.org/2026/03/09/announcing-duckdb-150), the client includes a built-in pager that activates when query results exceed 50 rows (configurable). Instead of truncating output, the pager lets you scroll through the full result set.

**Navigation keys:**

| Key | Action |
|-----|--------|
| `Page Down` / `Space` | Next page |
| `Page Up` | Previous page |
| `j` / `Down Arrow` | Scroll one line down |
| `k` / `Up Arrow` | Scroll one line up |
| `g` / `Home` | Jump to top |
| `G` / `End` | Jump to bottom |
| `q` / `Escape` | Exit pager |

**Configuration:**

```
gizmosql.main> .pager off          -- disable pager
gizmosql.main> .pager on           -- enable pager
gizmosql.main> .pager 100          -- set threshold to 100 rows
```

The pager fetches a bounded buffer from the server (threshold x 20 pages) rather than downloading the entire result set, so it stays responsive even for large tables.

## Last Result Reference (`_`)

Inspired by [DuckDB v1.5](https://duckdb.org/2026/03/09/announcing-duckdb-150), the client caches the most recent query result and lets you reference it as `_` in subsequent queries. The cached result is uploaded to the server as a temporary table via the Flight SQL bulk ingest API, giving you full SQL capabilities — joins, filtering, aggregation, and more.

```
gizmosql.main> SELECT * FROM orders WHERE total > 1000;
...

gizmosql.main> SELECT count(*) FROM _;
┌──────────────┐
│ count_star() │
│    bigint    │
├──────────────┤
│         4271 │
└──────────────┘

gizmosql.main> SELECT o.*, c.name FROM _ o JOIN customers c ON o.custkey = c.custkey;
...
```

**Related commands:**

| Command | Description |
|---------|-------------|
| `.last` | Re-display the cached result without re-querying |
| `.export_last [FILE]` | Export the cached result to an Arrow IPC file (default: `~/.gizmosql_last_result.arrow`) |

The exported IPC file can be loaded by any Arrow-compatible tool (Python/pandas, R, DuckDB, etc.).

## Dot Commands

Dot commands are available in interactive mode and in piped/heredoc input. They start with a `.` and are not sent to the server.

| Command | Description |
|---------|-------------|
| `.bail on\|off` | Stop on error (default: off) |
| `.catalogs` | List all catalogs |
| `.cd DIR` | Change working directory |
| `.connect URI` or `HOST PORT USER` | Connect to a GizmoSQL server |
| `.describe TABLE` | Show table column names and types |
| `.echo on\|off` | Echo input commands (default: off) |
| `.exit` | Exit (same as `.quit`) |
| `.export_last [FILE]` | Export last query result to Arrow IPC file |
| `.headers on\|off` | Toggle column headers (default: on) |
| `.help [PATTERN]` | Show help or commands matching PATTERN |
| `.highlight on\|off` | Toggle SQL syntax highlighting (default: on) |
| `.last` | Re-display the last query result |
| `.maxrows [N]` | Show or set max rows displayed (0=unlimited, default: 40) |
| `.maxwidth [N]` | Show or set max display width (0=auto from terminal) |
| `.mode MODE` | Set output mode |
| `.nullvalue STRING` | Set display string for NULL values (default: `NULL`) |
| `.once FILE` | Redirect next query output to FILE |
| `.output [FILE]` | Redirect all output to FILE (no arg resets to stdout) |
| `.pager on\|off\|N` | Toggle built-in pager or set row threshold (default: on/50) |
| `.prompt MAIN [CONT]` | Customize prompt strings |
| `.quit` | Exit the program |
| `.read FILE` | Execute SQL from FILE |
| `.refresh` | Refresh tab-completion schema cache |
| `.schema [PATTERN]` | Show database schemas |
| `.separator COL [ROW]` | Set column/row separators for list/CSV mode |
| `.shell CMD...` | Execute a system shell command |
| `.show` | Show current settings |
| `.tables [PATTERN] [--flat]` | List tables with schema details (or `--flat` for plain list) |
| `.timer on\|off` | Show query execution time (default: off) |

### Dot Command Examples

**Browse server metadata:**

`.tables` displays a rich, DuckDB-style view with side-by-side boxes showing column names, types, and row counts — inspired by [DuckDB v1.5](https://duckdb.org/2026/03/09/announcing-duckdb-150):

```
gizmosql.main> .tables
 ─────────────────────────────── gizmosql ───────────────────────────────
 ──────────────────────────────── main ─────────────────────────────────
┌───────────────────────┐┌─────────────────────┐┌──────────────────────┐
│       customers       ││       orders        ││      products        │
│                       ││                     ││                      │
│ c_custkey      bigint ││ o_orderkey   bigint ││ p_partkey     bigint │
│ c_name        varchar ││ o_custkey    bigint ││ p_name       varchar │
│ c_address     varchar ││ o_totalprice decimal││ p_brand      varchar │
│                       ││                     ││                      │
│       1500 rows       ││      15000 rows     ││       2000 rows      │
└───────────────────────┘└─────────────────────┘└──────────────────────┘
```

Catalog headers appear in orange and schema headers in blue, matching DuckDB's color palette. Use `.tables --flat` for the traditional tabular listing.

**Describe a table:**

```
gizmosql.main> .describe customers
┌─────────────┬─────────┬─────┬─────────┬─────────┬─────────┐
│ column_name │ column_type │ null │ key  │ default │ extra   │
...
```

**Switch output mode mid-session:**

```
gizmosql> .mode csv
gizmosql> SELECT 1 AS a, 2 AS b;
a,b
1,2
gizmosql> .mode box
```

**Save query output to a file:**

```
gizmosql> .output results.txt
gizmosql> SELECT * FROM employees;
gizmosql> .output
```

**Show current settings:**

```
gizmosql> .show
--- Client ---
     version: v1.19.7
--- Server ---
     version: v1.19.7
     edition: Core
 instance_id: a1b2c3d4-...
      engine: duckdb v1.5.1
       arrow: 23.0.1
--- Session ---
   connected: yes
         uri: gizmosql://localhost:31337?username=admin
        host: localhost
        port: 31337
         tls: off
    username: admin
  session_id: e5f6a7b8-...
        role: admin
     catalog: memory
      schema: main
--- Settings ---
        mode: box
     headers: on
   nullvalue: "NULL"
   separator: "|" "\n"
       timer: off
        echo: off
        bail: off
     maxrows: 40
    maxwidth: 0
   highlight: on
       pager: on (threshold: 50)
```

## Init File

On startup, the client automatically executes `~/.gizmosqlrc` if it exists. This is useful for setting preferences:

```sql
-- ~/.gizmosqlrc
.timer on
.mode table
.headers on
```

Override with a custom init file:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --init my_config.rc
```

Disable init file loading:

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin --no-init
```

## Display Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--quiet` | `-q` | off | Suppress welcome banner |
| `--echo` | `-e` | off | Echo SQL statements before execution |
| `--bail` | | off | Stop on first error |
| `--null` | | `NULL` | String to display for NULL values |
| `--no-header` | | | Disable column headers |

## Environment Variables

All connection parameters can be set via environment variables, making it easy to configure in Docker, CI/CD, or shell profiles:

| Variable | Description |
|----------|-------------|
| `GIZMOSQL_HOST` | Server hostname |
| `GIZMOSQL_PORT` | Server port |
| `GIZMOSQL_USER` | Username |
| `GIZMOSQL_PASSWORD` | Password |
| `GIZMOSQL_TLS` | Enable TLS (`1` or `true`) |
| `GIZMOSQL_TLS_ROOTS` | Path to CA certificate file |
| `GIZMOSQL_OAUTH_PORT` | OAuth HTTP server port |

**Example with environment variables:**

```bash
export GIZMOSQL_HOST=my-server.example.com
export GIZMOSQL_PORT=31337
export GIZMOSQL_USER=admin
export GIZMOSQL_PASSWORD=secret
export GIZMOSQL_TLS=true
export GIZMOSQL_TLS_ROOTS=/etc/ssl/certs/ca.pem

gizmosql_client --command "SELECT version()"
```

## Full CLI Reference

```
Usage: gizmosql_client [OPTIONS] [URI]

GizmoSQL Client Options:
  -?, --help                  Show help message
  -v, --version               Show version

  Connection:
  --uri URI                   Connection URI: gizmosql://HOST:PORT[?params]
                              (cannot be combined with --host, --port, etc.)
  -h, --host HOST             Server host (env: GIZMOSQL_HOST) [localhost]
  -p, --port PORT             Server port (env: GIZMOSQL_PORT) [31337]
  -u, --username USER         Username (env: GIZMOSQL_USER)
  -W, --password              Force password prompt (env: GIZMOSQL_PASSWORD)

  TLS:
  --tls                       Enable TLS (env: GIZMOSQL_TLS)
  --tls-roots FILE            CA certificate file (env: GIZMOSQL_TLS_ROOTS)
  --tls-skip-verify           Skip server certificate verification
  --mtls-cert FILE            Client certificate for mutual TLS
  --mtls-key FILE             Client private key for mutual TLS

  Authentication:
  --auth-type TYPE            Auth type: password (default) or external
  --oauth-port PORT           OAuth server port (env: GIZMOSQL_OAUTH_PORT) [31339]

  Input/Output:
  -c, --command SQL           Execute SQL and exit
  -f, --file FILE             Execute SQL from file and exit
  -o, --output FILE           Write output to file
  --init FILE                 Init file (default: ~/.gizmosqlrc)
  --no-init                   Skip init file

  Output Mode:
  --csv                       CSV output
  --json                      JSON output
  --table                     ASCII table output
  --box                       Unicode box output (default)
  --markdown                  Markdown table output
  --no-header                 Disable column headers

  Display:
  -q, --quiet                 Suppress banner and info messages
  -e, --echo                  Echo SQL before execution
  --bail                      Stop on first error
  --null STRING               NULL display string [NULL]
```

## Common Workflows

### Export query results to CSV

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin \
  --csv --no-header --output export.csv \
  --command "SELECT * FROM sales WHERE date >= '2026-01-01'"
```

### Run a migration script

```bash
gizmosql_client --host prod-server --username admin --password --bail --file migrations/v2.sql
```

### Quick data exploration

```bash
GIZMOSQL_PASSWORD=secret gizmosql_client --host localhost --username admin <<'EOF'
.tables
SELECT COUNT(*) FROM employees;
SELECT dept, AVG(salary) as avg_salary FROM employees GROUP BY dept ORDER BY avg_salary DESC;
EOF
```

### Docker usage

```bash
docker exec -it gizmosql-container /app/gizmosql_client \
  --host localhost --username admin --command "SELECT version()"
```

### CI/CD pipeline

```bash
export GIZMOSQL_HOST=test-db.internal
export GIZMOSQL_USER=ci_runner
export GIZMOSQL_PASSWORD=$DB_PASSWORD

gizmosql_client --quiet --bail --file tests/setup.sql
gizmosql_client --quiet --csv --command "SELECT COUNT(*) FROM test_results WHERE status='fail'" | tail -1
```
