# GizmoSQL Editions

GizmoSQL is available in two editions to meet different needs:

## GizmoSQL Core (Apache 2.0)

**Free and open source** under the Apache License 2.0.

GizmoSQL Core provides a powerful Flight SQL interface for DuckDB and SQLite with:

- Full Flight SQL protocol support
- Authentication (Basic, Bearer, mTLS)
- TLS encryption
- DuckDB and SQLite backends
- Health check endpoints for Kubernetes
- All standard SQL operations

**Build GizmoSQL Core:**
```bash
cmake -DGIZMOSQL_ENTERPRISE=OFF -B build
cmake --build build
```

---

## GizmoSQL Enterprise Edition (Commercial License)

**Commercial license** with additional enterprise features.

GizmoSQL Enterprise includes all Core features plus:

| Feature | Description |
|---------|-------------|
| **Session Instrumentation** | Track instances, sessions, and SQL statements for auditing, monitoring, and debugging. Records are stored in a DuckDB database for analysis. |
| **KILL SESSION** | Terminate active client sessions programmatically via `KILL SESSION 'session-id'` SQL command. Requires admin role. |

### Obtaining a License

Contact GizmoData sales at **sales@gizmodata.com** to obtain an enterprise license.

### Using Your License

Provide your license key file when starting the server:

```bash
./gizmosql_server --database-filename mydb.db \
    --license-key-file /path/to/license.jwt \
    --enable-instrumentation 1
```

Or set via environment variables (ideal for containers):
```bash
export GIZMOSQL_LICENSE_KEY_FILE=/path/to/license.jwt
export GIZMOSQL_ENABLE_INSTRUMENTATION=1
./gizmosql_server --database-filename mydb.db
```

**Docker example:**
```bash
docker run --name gizmosql \
    -e GIZMOSQL_LICENSE_KEY_FILE=/opt/gizmosql/license.jwt \
    -e GIZMOSQL_ENABLE_INSTRUMENTATION=1 \
    -e GIZMOSQL_PASSWORD=mypassword \
    -v /path/to/license.jwt:/opt/gizmosql/license.jwt:ro \
    gizmodata/gizmosql:latest
```

### License Key Format

License keys are JWT tokens signed by GizmoData. They contain:
- Customer information
- Expiration date
- Licensed features

### Without a Valid License

If you attempt to use enterprise features without a valid license:

```
Error: Instrumentation is a commercially licensed enterprise feature.
       Please provide a valid license key file via --license-key-file
       or contact GizmoData sales at sales@gizmodata.com to obtain a license.
```

---

## SQL Functions

GizmoSQL provides a SQL function to query the current edition:

```sql
SELECT GIZMOSQL_EDITION();
```

Returns:
- `'Core'` - When running GizmoSQL Core edition
- `'Enterprise'` - When running GizmoSQL Enterprise Edition with a valid license

---

## Feature Comparison

| Feature | Core | Enterprise |
|---------|:----:|:----------:|
| Flight SQL Protocol | ✓ | ✓ |
| DuckDB Backend | ✓ | ✓ |
| SQLite Backend | ✓ | ✓ |
| TLS Encryption | ✓ | ✓ |
| mTLS Client Auth | ✓ | ✓ |
| Basic Auth | ✓ | ✓ |
| Bearer Token Auth | ✓ | ✓ |
| JWT Token Auth | ✓ | ✓ |
| Health Check Endpoints | ✓ | ✓ |
| Kubernetes Ready | ✓ | ✓ |
| **Session Instrumentation** | - | ✓ |
| **KILL SESSION Command** | - | ✓ |

---

## Startup Banner

**Core Edition:**
```
GizmoSQL Core - Copyright (c) 2026 GizmoData LLC
 Licensed under the Apache License, Version 2.0
 https://www.apache.org/licenses/LICENSE-2.0
```

**Enterprise Edition (with valid license):**
```
GizmoSQL Enterprise Edition - Copyright (c) 2026 GizmoData LLC
 License ID: abc123-def456
 Licensed to: Acme Corporation (customer@company.com)
 License issued: 2025-01-22
 License expires: 2026-01-22
 Licensed by: GizmoData LLC
```
