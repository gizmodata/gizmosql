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

### JWKS Auto-Discovery *(Enterprise)*

> **Important:** JWKS auto-discovery requires **GizmoSQL Enterprise Edition**.
> Contact [sales@gizmodata.com](mailto:sales@gizmodata.com) for licensing information.

Instead of providing a static public key file, you can configure GizmoSQL to automatically discover and fetch public keys from a JWKS (JSON Web Key Set) endpoint. This is the standard approach when integrating with Identity Providers (IdPs) like Keycloak, Okta, Auth0, or Azure AD.

**Option 1: Automatic discovery from OIDC issuer** (recommended)

When `--token-allowed-issuer` is set without `--token-signature-verify-cert-path`, GizmoSQL automatically fetches the OIDC discovery document at `{issuer}/.well-known/openid-configuration` and extracts the `jwks_uri`:

```bash
gizmosql_server \
   --database-filename data/mydb.duckdb \
   --tls tls/server.pem tls/server.key \
   --token-allowed-issuer "https://your-idp.com/realms/myrealm" \
   --token-allowed-audience "gizmosql-client" \
   --token-default-role admin
```

**Option 2: Explicit JWKS URI**

If you need to specify the JWKS endpoint directly:

```bash
gizmosql_server \
   --database-filename data/mydb.duckdb \
   --tls tls/server.pem tls/server.key \
   --token-allowed-issuer "https://your-idp.com/realms/myrealm" \
   --token-allowed-audience "gizmosql-client" \
   --token-jwks-uri "https://your-idp.com/realms/myrealm/protocol/openid-connect/certs" \
   --token-default-role admin
```

**JWKS Options:**

| Option | Env Var | Description |
|--------|---------|-------------|
| `--token-jwks-uri` | `GIZMOSQL_TOKEN_JWKS_URI` | Direct JWKS endpoint URL. If not set, auto-discovered from the issuer. |
| `--token-default-role` | `GIZMOSQL_TOKEN_DEFAULT_ROLE` | Default role to assign when IdP tokens lack a `role` claim. |

**Verification priority:**
1. Static cert path (`--token-signature-verify-cert-path`) — used if provided (Core or Enterprise)
2. Explicit JWKS URI (`--token-jwks-uri`) — Enterprise only
3. Auto-discovery from issuer (`--token-allowed-issuer`) — Enterprise only

**JWKS features:**
- Thread-safe key cache with 5-minute TTL
- Automatic key refresh on `kid` (key ID) miss for seamless key rotation
- Support for RSA (RS256/RS384/RS512) and EC (ES256/ES384/ES512) key types

### Default Role for IdP Tokens

Standard OIDC access tokens from identity providers typically do not include a `role` claim. The `--token-default-role` option provides a fallback:

- If the token has a `role` claim, it is used (existing behavior)
- If the token lacks a `role` claim and `--token-default-role` is set, the default is used
- If the token lacks a `role` claim and no default is set, the token is rejected with a descriptive error

This allows you to integrate with any OIDC-compliant IdP without requiring custom claims configuration.

### Authorized Email Filtering *(Enterprise)*

> **Important:** Authorized email filtering requires **GizmoSQL Enterprise Edition**.

When using OAuth/SSO with an IdP configured as an "External" or public application (e.g., Google OAuth), any user with a valid account can authenticate. The `--token-authorized-emails` option lets administrators restrict which authenticated users are actually allowed to connect.

**Configuration:**

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--token-authorized-emails` | `GIZMOSQL_TOKEN_AUTHORIZED_EMAILS` | `*` (all) | Comma-separated list of authorized email patterns |

**Pattern syntax:**
- `*` — allow all authenticated users (default, backward compatible)
- `*@company.com` — allow any user with a `company.com` email
- `user@example.com` — allow a specific email address
- `admin@partner.com,*@company.com` — combine multiple patterns (comma-separated)

Pattern matching is **case-insensitive**: `User@Company.COM` matches `*@company.com`.

**Example:**

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://accounts.google.com" \
  --token-allowed-audience "your-client-id.apps.googleusercontent.com" \
  --token-default-role admin \
  --token-authorized-emails "*@yourcompany.com,partner@external.com"
```

With this configuration:
- `alice@yourcompany.com` — **allowed** (matches `*@yourcompany.com`)
- `partner@external.com` — **allowed** (exact match)
- `random@gmail.com` — **rejected** with error: *"User 'random@gmail.com' is not authorized. Contact your administrator."*

**Notes:**
- This filter applies only to external/bootstrap token authentication (OIDC/SSO). Basic username/password authentication is not affected.
- The email is extracted from the `email` claim in the JWT, falling back to the `sub` claim if `email` is not present.
- If the option is not set or set to `*`, all authenticated users are allowed (backward compatible).

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

## Cross-Instance Token Acceptance

By default, GizmoSQL servers strictly validate that bearer tokens were issued by the same server instance. This is a security measure to ensure that clients reconnect and re-authenticate if they are load-balanced to a different server instance.

However, in **load-balanced deployments** where multiple GizmoSQL server instances share the same secret key, you may want to allow tokens issued by one instance to be accepted by another. This is useful for:

- **High-availability setups** - Clients can seamlessly failover between instances
- **Rolling deployments** - Clients don't need to re-authenticate during server upgrades
- **Horizontal scaling** - New instances can immediately accept existing client sessions

### Enabling Cross-Instance Token Acceptance

To allow tokens from other server instances (with the same secret key), use the `--allow-cross-instance-tokens` flag:

**CLI:**
```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --secret-key "your-shared-secret-key" \
  --allow-cross-instance-tokens true
```

**Environment Variable:**
```bash
export GIZMOSQL_ALLOW_CROSS_INSTANCE_TOKENS=true
gizmosql_server --database-filename data/mydb.duckdb
```

**Library API:**
```cpp
RunFlightSQLServer(
    backend,
    database_filename,
    // ... other parameters ...
    /*allow_cross_instance_tokens=*/true
);
```

### Security Considerations

When enabling cross-instance token acceptance:

1. **Ensure all instances share the same secret key** - Tokens are cryptographically signed with the secret key. If instances use different keys, tokens will still be rejected due to invalid signatures.

2. **Use the same password across instances** - Basic authentication uses the secret key to hash passwords. Different secret keys will cause authentication failures.

3. **Be aware of session state** - While tokens are accepted, session-specific state (such as prepared statements or transaction context) may not be available on a different instance.

4. **Monitor for abuse** - Relaxing instance validation increases the attack surface if a token is compromised. Consider using shorter token lifetimes.

### Behavior Comparison

| Scenario | Strict Mode (default) | Relaxed Mode |
|----------|----------------------|--------------|
| Token from same instance | Accepted | Accepted |
| Token from different instance (same secret) | **Rejected** | Accepted |
| Token with wrong signature | Rejected | Rejected |
| Expired token | Rejected | Rejected |

## SSO/OIDC Authentication (Browser-Based Login) *(Enterprise)*

> **Important:** SSO/OIDC authentication requires **GizmoSQL Enterprise Edition** on the server side
> and **GizmoSQL JDBC Driver v1.5.0 or later** on the client side.
> The JDBC driver itself has no license requirement — it simply won't work unless the server has Enterprise Edition with JWKS enabled.

For interactive desktop tools (DBeaver, IntelliJ, etc.), the GizmoSQL JDBC driver supports browser-based Single Sign-On (SSO) using the OIDC Authorization Code flow with PKCE. When a connection is established, the driver:

1. Opens a browser for the user to authenticate with their Identity Provider
2. Receives an authorization code via a temporary localhost callback server
3. Exchanges the code for an access token using PKCE (no client secret needed)
4. Sends the token to the GizmoSQL server as a Bearer token
5. Automatically refreshes the token using the refresh token when it expires

### JDBC Connection Properties

**Minimal configuration (OIDC discovery):**

```
jdbc:gizmosql://hostname:31337?useEncryption=true&oidc.issuer=https://your-idp.com/realms/myrealm&oidc.clientId=gizmosql-desktop-client
```

**Explicit endpoints (without OIDC discovery):**

```
jdbc:gizmosql://hostname:31337?useEncryption=true&oauth.flow=authorization_code&oauth.clientId=gizmosql-desktop-client&oauth.authorizationUrl=https://your-idp.com/authorize&oauth.tokenUri=https://your-idp.com/token
```

**OIDC Connection Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `oidc.issuer` | OIDC issuer URL (enables auto-discovery of endpoints) | — |
| `oidc.clientId` | OAuth client ID (can also use `oauth.clientId`) | — |
| `oidc.clientSecret` | OAuth client secret (can also use `oauth.clientSecret`). Required by some IdPs such as Google, even for desktop/public apps. | — |
| `oidc.scopes` | OAuth scopes (can also use `oauth.scope`) | `openid` |

**OAuth Connection Properties (explicit endpoints):**

| Property | Description |
|----------|-------------|
| `oauth.flow` | Set to `authorization_code` for browser-based SSO |
| `oauth.clientId` | OAuth client ID |
| `oauth.clientSecret` | OAuth client secret (required by some IdPs such as Google) |
| `oauth.authorizationUrl` | Authorization endpoint URL |
| `oauth.tokenUri` | Token endpoint URL |
| `oauth.scope` | OAuth scopes |

### IdP Configuration

Your Identity Provider must be configured with a **public client** (no client secret) that supports:

- Authorization Code flow with PKCE (`S256` challenge method)
- Redirect URI: `http://127.0.0.1:*` (localhost with any port)
- The client should issue tokens with:
  - `iss` claim matching the server's `--token-allowed-issuer`
  - `aud` claim matching the server's `--token-allowed-audience`

### End-to-End Example with Keycloak

**1. Start Keycloak:**
```bash
docker run -p 8080:8080 \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  quay.io/keycloak/keycloak:latest start-dev
```

**2. Configure Keycloak:**
- Create a realm (e.g., `gizmosql`)
- Create a public client (e.g., `gizmosql-desktop-client`) with PKCE enabled
- Add `http://127.0.0.1:*` as a valid redirect URI
- Create a test user

**3. Start GizmoSQL server:**
```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "http://localhost:8080/realms/gizmosql" \
  --token-allowed-audience "gizmosql-desktop-client" \
  --token-default-role admin
```

**4. Connect via JDBC:**
```
jdbc:gizmosql://localhost:31337?useEncryption=true&disableCertificateVerification=true&oidc.issuer=http://localhost:8080/realms/gizmosql&oidc.clientId=gizmosql-desktop-client
```

The browser will open to the Keycloak login page. After authentication, the connection is established automatically.

## Server-Side OAuth Code Exchange *(Enterprise)*

> **New in v1.17.0.** Requires GizmoSQL Enterprise Edition with the `external_auth` feature.

Server-side OAuth code exchange simplifies client configuration by making the GizmoSQL server a **confidential OAuth client**. Instead of each client needing the OAuth client ID, secret, and scopes in its connection string, the server owns those credentials and handles the entire code exchange flow.

### How It Works

1. The client generates a random UUID and computes `HASH = HMAC-SHA256(secret_key, UUID)`.
2. The client opens a browser to `https://<server>:<oauth_port>/oauth/start?session=HASH`.
3. The server redirects to the Identity Provider's authorization endpoint.
4. The user authenticates with the IdP.
5. The IdP redirects back to the server's `/oauth/callback` with an authorization code.
6. The server exchanges the code for tokens, validates the ID token via JWKS, and issues a GizmoSQL session JWT.
7. The client polls `https://<server>:<oauth_port>/oauth/token/<UUID>` to retrieve the JWT.
8. The client uses the JWT as a Bearer token on the Flight SQL gRPC port.

### Server Configuration

| CLI Flag | Env Var | Default | Description |
|----------|---------|---------|-------------|
| `--oauth-client-id` | `GIZMOSQL_OAUTH_CLIENT_ID` | *(disabled)* | OAuth client ID. Setting this enables the OAuth HTTP server. |
| `--oauth-client-secret` | `GIZMOSQL_OAUTH_CLIENT_SECRET` | | OAuth client secret (confidential, stays on server). |
| `--oauth-scopes` | `GIZMOSQL_OAUTH_SCOPES` | `openid profile email` | OAuth scopes to request. |
| `--oauth-port` | `GIZMOSQL_OAUTH_PORT` | `31339` | Port for the OAuth HTTP(S) server. |
| `--oauth-redirect-uri` | `GIZMOSQL_OAUTH_REDIRECT_URI` | auto-constructed | Override redirect URI when behind a proxy. |

The OAuth server **requires** `--token-allowed-issuer` and `--token-allowed-audience` to be set. OIDC endpoints (authorization, token, JWKS) are auto-discovered from the issuer.

### Example: Google as IdP

**1. Create OAuth credentials in Google Cloud Console:**
- Application type: **Web application**
- Authorized redirect URI: `https://<your-server>:31339/oauth/callback`

**2. Start GizmoSQL:**
```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://accounts.google.com" \
  --token-allowed-audience "462...apps.googleusercontent.com" \
  --token-default-role admin \
  --token-authorized-emails "*@yourcompany.com" \
  --oauth-client-id "462...apps.googleusercontent.com" \
  --oauth-client-secret "GOCSPX-..." \
  --oauth-port 31339
```

**3. Client connection string:**
```
jdbc:gizmosql://hostname:31337?useEncryption=true&authType=oauth
```

### Security Considerations

- The browser only sees the session **hash** in URLs. The raw UUID (needed to retrieve the token) is only known to the polling client.
- Pending sessions expire after 15 minutes.
- Email filtering (`--token-authorized-emails`) applies to OAuth-authenticated users.
- The OAuth HTTP server uses the same TLS certificate as the Flight SQL server when TLS is enabled.

## Security Best Practices

1. **Protect private keys** - Store signing keys securely; never commit them to version control
2. **Use short token lifetimes** - Shorter expiration times reduce the window of exposure if a token is compromised
3. **Use TLS** - Always enable TLS encryption for production deployments
4. **Rotate keys periodically** - Implement a key rotation strategy for long-running deployments
5. **Validate all claims** - Ensure issuer and audience are correctly configured to prevent token reuse across services
6. **Use strict instance validation in single-server deployments** - Only enable cross-instance token acceptance when running multiple load-balanced instances

## Related Resources

- [OAuth / SSO Setup Guide](oauth_sso_setup.md) - Step-by-step setup for Keycloak, Azure AD, Google, AWS Cognito, and Clerk
- [generate-gizmosql-token on PyPI](https://pypi.org/project/generate-gizmosql-token/)
- [generate-gizmosql-token on GitHub](https://github.com/gizmodata/generate-gizmosql-token)
- [Editions](editions.md) - Feature comparison between Core and Enterprise
