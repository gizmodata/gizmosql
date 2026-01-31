# Token Authentication

GizmoSQL supports JWT (JSON Web Token) based authentication, allowing you to use externally signed tokens for secure, stateless authentication. This is ideal for integrating with identity providers, service-to-service authentication, and scenarios where you need fine-grained access control.

## Overview

Token authentication in GizmoSQL works as follows:

1. An external system generates a JWT signed with a private key
2. GizmoSQL server is configured with the corresponding public key
3. Clients authenticate by passing `token` as the username and the JWT as the password
4. GizmoSQL validates the token signature, expiration, issuer, and audience

## Generating Tokens

### Using the generate-gizmosql-token Package

GizmoData provides a Python utility for generating GizmoSQL authentication tokens:

[![PyPI version](https://badge.fury.io/py/generate-gizmosql-token.svg)](https://pypi.org/project/generate-gizmosql-token/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/generate-gizmosql-token.svg)](https://pypi.org/project/generate-gizmosql-token/)

**Installation:**
```shell
pip install generate-gizmosql-token
```

**Basic Usage:**
```shell
generate-gizmosql-token \
  --issuer "Your Organization" \
  --audience "GizmoSQL Server" \
  --subject "user@example.com" \
  --role "admin" \
  --token-lifetime-seconds 86400 \
  --output-file-format "output/token.jwt" \
  --private-key-file keys/private_key.pem
```

**Available Roles:**
- `admin` - Full administrative access
- `user` - Standard user access
- `readonly` - Read-only access (SELECT queries only, DuckDB backend)

> For more details, see the [generate-gizmosql-token GitHub repository](https://github.com/gizmodata/generate-gizmosql-token).

### Token Claims

The generated JWT includes the following claims:

| Claim | Description |
|-------|-------------|
| `iss` | Issuer - must match server's `--token-allowed-issuer` |
| `aud` | Audience - must match server's `--token-allowed-audience` |
| `sub` | Subject - typically the user's email or identifier |
| `role` | User role (`admin`, `user`, or `readonly`) |
| `jti` | Unique token ID |
| `iat` | Issued at timestamp |
| `exp` | Expiration timestamp |
| `catalog_access` | *(Optional, Enterprise)* Per-catalog access rules |

## Server Configuration

To enable token authentication, start the GizmoSQL server with the following options:

```bash
gizmosql_server \
   --database-filename data/mydb.duckdb \
   --tls tls/server.pem tls/server.key \
   --token-allowed-issuer "Your Organization" \
   --token-allowed-audience "GizmoSQL Server" \
   --token-signature-verify-cert-path tls/jwt_public.pem
```

**Required Options:**

| Option | Description |
|--------|-------------|
| `--token-allowed-issuer` | The expected `iss` claim value in tokens |
| `--token-allowed-audience` | The expected `aud` claim value in tokens |
| `--token-signature-verify-cert-path` | Path to the public key (PEM format) used to verify token signatures |

> **Note:** The public key must correspond to the private key used to sign the tokens.

## Client Usage

### JDBC

Use `token` as the username and the JWT as the password:

```
jdbc:gizmosql://hostname:31337?useEncryption=true&user=token&password=YOUR_JWT_HERE&disableCertificateVerification=true
```

### ADBC (Python)

```python
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

token = os.getenv("GIZMOSQL_TOKEN")

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "token",
        "password": token,
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true",
    },
    autocommit=True
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table")
        result = cur.fetch_arrow_table()
        print(result)
```

### CLI Client

```bash
gizmosql_client \
  --host localhost \
  --port 31337 \
  --username token \
  --password "$(cat token.jwt)" \
  --query "SELECT 1" \
  --command Execute \
  --use-tls
```

## Catalog-Level Access Control *(Enterprise)*

> **Important:** Catalog-level access control requires **GizmoSQL Enterprise Edition** (v1.15.0+).
> Contact [sales@gizmodata.com](mailto:sales@gizmodata.com) for licensing information.

You can specify fine-grained catalog-level access controls using the `--catalog-access` option when generating tokens:

```shell
generate-gizmosql-token \
  --issuer "Your Organization" \
  --audience "GizmoSQL Server" \
  --subject "analyst@example.com" \
  --role "user" \
  --catalog-access '[{"catalog": "production", "access": "read"}, {"catalog": "staging", "access": "write"}]' \
  --token-lifetime-seconds 86400 \
  --private-key-file keys/private_key.pem
```

### Access Levels

| Level | Description |
|-------|-------------|
| `none` | No access to the catalog |
| `read` | Read-only access (SELECT queries only) |
| `write` | Full access (SELECT, INSERT, UPDATE, DELETE, DDL) |

### Rules

- Rules are evaluated in order; **first match wins**
- Use `"catalog": "*"` as a wildcard to match all catalogs
- If no `--catalog-access` is specified, full access is granted to all catalogs (backward compatible)

### Example Configurations

```shell
# Read-only access to everything
--catalog-access '[{"catalog": "*", "access": "read"}]'

# Write access to staging, read-only to everything else
--catalog-access '[{"catalog": "staging", "access": "write"}, {"catalog": "*", "access": "read"}]'

# Access only to specific catalogs, deny all others
--catalog-access '[{"catalog": "allowed_db", "access": "write"}, {"catalog": "*", "access": "none"}]'
```

> **Note:** The `_gizmosql_instr` instrumentation database has special protection: only admin users can read it, and no one can write to it via client connections (it's system-managed). Token-based `catalog_access` rules do not override this protection.

## Generating Keys

To generate an RSA key pair for token signing:

```bash
# Generate private key
openssl genrsa -out private_key.pem 2048

# Extract public key
openssl rsa -in private_key.pem -pubout -out public_key.pem
```

The `private_key.pem` is used to sign tokens (keep this secure!), and `public_key.pem` is configured on the GizmoSQL server for verification.

## Security Best Practices

1. **Protect private keys** - Store signing keys securely; never commit them to version control
2. **Use short token lifetimes** - Shorter expiration times reduce the window of exposure if a token is compromised
3. **Use TLS** - Always enable TLS encryption for production deployments
4. **Rotate keys periodically** - Implement a key rotation strategy for long-running deployments
5. **Validate all claims** - Ensure issuer and audience are correctly configured to prevent token reuse across services

## Related Resources

- [generate-gizmosql-token on PyPI](https://pypi.org/project/generate-gizmosql-token/)
- [generate-gizmosql-token on GitHub](https://github.com/gizmodata/generate-gizmosql-token)
- [Editions](editions.md) - Feature comparison between Core and Enterprise
