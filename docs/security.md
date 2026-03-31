# Security Guide

GizmoSQL provides multiple layers of security to protect your data in transit and at rest. This guide explains each layer, why it matters, and how to configure it — starting from the basics.

## Why Security Matters for a Database

When you connect to a database over a network, three things need to be true:

1. **No one can eavesdrop** on your queries or results (encryption)
2. **You are who you say you are** (authentication)
3. **You can only do what you're allowed to do** (authorization)

Without these protections, anyone on the same network could read your data, impersonate a legitimate user, or modify tables they shouldn't have access to.

GizmoSQL addresses all three with a layered security model:

| Layer | What It Protects | GizmoSQL Feature |
|-------|-----------------|------------------|
| Encryption | Data in transit | TLS, mTLS |
| Authentication | Identity verification | Username/password, JWT tokens, OAuth/SSO |
| Authorization | Access control | Roles, catalog permissions, email filtering |

---

## Layer 1: Encryption (TLS)

### What Is TLS?

**TLS (Transport Layer Security)** encrypts the connection between the client and server so that no one in between can read or tamper with the data. It's the same technology that makes `https://` work in your browser.

Without TLS, your SQL queries, query results, usernames, and passwords are sent as **plain text** over the network. Anyone with access to the network (a shared Wi-Fi, a cloud VPC, a compromised router) could intercept them.

### Enabling TLS

To enable TLS, you need two files:
- A **certificate** (`cert.pem`) — proves your server's identity
- A **private key** (`key.pem`) — the secret that only your server knows

```bash
# Start the server with TLS
gizmosql_server --tls cert.pem key.pem \
  --username admin --password secretpass \
  --database mydata.db
```

Clients then connect with `--tls`:

```bash
# If your server uses a certificate from a well-known CA (Let's Encrypt, DigiCert, etc.),
# the client automatically trusts it using your system's certificate store:
gizmosql_client --host my-server.example.com --username admin --tls

# If your server uses a private/internal CA, provide the CA certificate explicitly:
gizmosql_client --host my-server.example.com --username admin \
  --tls --tls-roots /path/to/ca.pem
```

The client automatically loads trusted CA certificates from your operating system's certificate store (Keychain on macOS, the Windows certificate store, or `/etc/ssl/certs` on Linux). You only need `--tls-roots` when your server's certificate was signed by a CA that isn't in your system's trust store — such as a self-signed certificate or a private corporate CA.

### Skipping Certificate Verification (Development Only)

During development with self-signed certificates, you can skip certificate verification entirely:

```bash
gizmosql_client --host localhost --username admin \
  --tls --tls-skip-verify
```

> **Warning:** `--tls-skip-verify` disables all certificate validation, making the connection vulnerable to man-in-the-middle attacks. **Never use this in production.** It exists solely for local development and testing with self-signed certificates. In production, use a certificate from a well-known CA (automatically trusted) or provide your private CA certificate via `--tls-roots`.

### Getting a TLS Certificate

**For production**, use a certificate from a trusted CA:
- [Let's Encrypt](https://letsencrypt.org/) (free, automated)
- Your organization's internal CA
- Cloud provider certificate services (AWS ACM, GCP Certificate Manager, Azure Key Vault)

**For development/testing**, create a self-signed certificate:

```bash
# Generate a self-signed certificate (valid for 365 days)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
```

> **Warning:** Self-signed certificates are fine for development but should not be used in production. Clients will need `--tls-skip-verify` or the self-signed CA certificate via `--tls-roots`.

### What Happens Without TLS

GizmoSQL will warn you at startup:

```
WARNING - TLS is disabled for the GizmoSQL server - this is NOT secure.
```

This is acceptable only for:
- Local development on `localhost`
- Environments where encryption is handled at a different layer (e.g., a service mesh like Istio)

---

## Layer 2: Mutual TLS (mTLS)

### What Is mTLS?

Standard TLS is **one-way**: the client verifies the server's identity, but the server doesn't verify the client. This is like checking a website's padlock icon — you know you're talking to the real server, but the server doesn't know who you are (until you log in).

**Mutual TLS (mTLS)** adds **two-way verification**: the server also requires the client to present a certificate. This proves the client's identity at the network level, before any username/password exchange occurs.

mTLS is commonly used in:
- Zero-trust network architectures
- Service-to-service communication in microservices
- Environments where IP-based access control isn't sufficient
- Compliance scenarios (PCI-DSS, HIPAA) that require strong client authentication

### Enabling mTLS

**Server side** — provide the CA certificate that signed the client certificates:

```bash
gizmosql_server --tls cert.pem key.pem \
  --mtls-ca-cert-filename client-ca.pem \
  --username admin --password secretpass \
  --database mydata.db
```

**Client side** — provide the client certificate and private key:

```bash
gizmosql_client --host my-server.example.com --username admin \
  --tls --tls-roots server-ca.pem \
  --mtls-cert client.crt --mtls-key client.key
```

### Creating Client Certificates

```bash
# 1. Generate a CA for client certificates (do this once)
openssl req -x509 -newkey rsa:4096 -keyout client-ca-key.pem \
  -out client-ca.pem -days 3650 -nodes -subj "/CN=Client CA"

# 2. Generate a client certificate signed by that CA
openssl req -newkey rsa:2048 -keyout client.key -out client.csr \
  -nodes -subj "/CN=alice@example.com"
openssl x509 -req -in client.csr -CA client-ca.pem -CAkey client-ca-key.pem \
  -CAcreateserial -out client.crt -days 365

# 3. Give client-ca.pem to the server, and client.crt + client.key to the client
```

The server's `--mtls-ca-cert-filename` should point to the CA that signed your client certificates. The client's identity (the `CN` field) is tracked in the session context for audit logging.

---

## Layer 3: Authentication

Authentication verifies *who* is connecting. GizmoSQL supports three methods, from simplest to most sophisticated.

### Method 1: Username and Password

The simplest option. Configure a username and password on the server:

```bash
gizmosql_server --username admin --password 'my-secret-password' \
  --database mydata.db
```

| Option | Env Var | Default |
|--------|---------|---------|
| `--username` | `GIZMOSQL_USERNAME` | `gizmosql_user` |
| `--password` | `GIZMOSQL_PASSWORD` | *(required)* |
| `--secret-key` | `SECRET_KEY` | *(auto-generated)* |

The `--secret-key` is used internally to hash passwords and sign session tokens. If you're running multiple server instances behind a load balancer, they must all share the same secret key so that session tokens are valid across instances.

### Method 2: JWT Token Authentication

For systems that already have an identity provider (IdP), GizmoSQL can accept **JWT (JSON Web Token)** tokens instead of passwords. The client sends the JWT as the password with `username=token`:

```bash
# Client connects with a JWT token
gizmosql_client --host my-server.example.com --username token \
  --tls --tls-roots ca.pem
# When prompted for password, paste the JWT token
```

The server validates the token's signature, issuer, audience, and expiration:

```bash
gizmosql_server --tls cert.pem key.pem \
  --token-allowed-issuer "https://accounts.google.com" \
  --token-allowed-audience "my-app-client-id" \
  --token-signature-verify-cert-path public_key.pem \
  --token-default-role user \
  --database mydata.db
```

| Option | Env Var | Description |
|--------|---------|-------------|
| `--token-allowed-issuer` | `TOKEN_ALLOWED_ISSUER` | Required JWT `iss` claim |
| `--token-allowed-audience` | `TOKEN_ALLOWED_AUDIENCE` | Required JWT `aud` claim |
| `--token-signature-verify-cert-path` | `TOKEN_SIGNATURE_VERIFY_CERT_PATH` | Public key (PEM) to verify token signature |
| `--token-default-role` | `GIZMOSQL_TOKEN_DEFAULT_ROLE` | Role assigned when token has no `role` claim |

> See [Token Authentication](token_authentication.md) for the complete configuration guide, including JWKS auto-discovery (Enterprise).

### Method 3: OAuth / SSO *(Enterprise)*

For the best user experience, GizmoSQL Enterprise supports **OAuth 2.0 / OpenID Connect (OIDC)** — the same "Sign in with Google/Microsoft/Okta" flow used by web applications. Users authenticate in their browser and the token is automatically passed to the client.

```bash
# Server with OAuth
gizmosql_server --tls cert.pem key.pem \
  --token-allowed-issuer "https://your-idp.example.com" \
  --oauth-client-id "your-client-id" \
  --oauth-client-secret "your-client-secret" \
  --database mydata.db
```

```bash
# Client connects with OAuth (opens browser)
gizmosql_client --host my-server.example.com \
  --tls --auth-type external
```

Supported identity providers:
- Keycloak
- Azure AD / Entra ID
- Google Workspace
- AWS Cognito
- Clerk
- Any OIDC-compliant provider

> See [OAuth / SSO Setup](oauth_sso_setup.md) for step-by-step configuration for each provider.

---

## Layer 4: Authorization

Authentication tells you *who* someone is. Authorization tells you *what they can do*.

### User Roles

GizmoSQL has three built-in roles:

| Role | Permissions |
|------|-------------|
| `admin` | Full access: all SQL operations, `KILL SESSION`, instrumentation queries |
| `user` | Standard access: SELECT, INSERT, UPDATE, DELETE, DDL |
| `readonly` | Read-only: SELECT queries only (DuckDB backend) |

The role is determined by:
1. The `role` claim in a JWT/OIDC token (if present)
2. The `--token-default-role` server setting (fallback when token has no role claim)
3. `admin` (default for username/password authentication)

### Catalog-Level Permissions *(Enterprise)*

For multi-tenant or data-mesh architectures, GizmoSQL Enterprise supports fine-grained per-catalog access control via JWT token claims:

```json
{
  "sub": "alice@example.com",
  "catalog_access": [
    {"catalog": "production", "access": "read"},
    {"catalog": "staging", "access": "write"},
    {"catalog": "*", "access": "none"}
  ]
}
```

Rules are evaluated in order — the first match wins. The wildcard `*` acts as a default. In this example, Alice can read from `production`, write to `staging`, and has no access to anything else.

Catalog permissions are enforced at every level:
- SQL queries (SELECT, INSERT, CREATE, etc.)
- Metadata queries (SHOW DATABASES, information_schema)
- Flight SQL metadata RPCs (GetTables, GetDbSchemas)

### Email-Based Filtering *(Enterprise)*

Restrict which authenticated users can connect, based on their email address:

```bash
gizmosql_server --tls cert.pem key.pem \
  --token-authorized-emails "*@mycompany.com,partner@external.com" \
  --database mydata.db
```

| Pattern | Matches |
|---------|---------|
| `*` | All authenticated users (default) |
| `*@mycompany.com` | Any email at mycompany.com |
| `alice@example.com` | Exact match only |
| `*@mycompany.com,bob@partner.com` | Multiple patterns (comma-separated) |

Users whose email doesn't match any pattern receive: `"User 'eve@attacker.com' is not authorized. Contact your administrator."`

---

## Layer 5: Audit & Monitoring

### Authentication Logging

Track who connects and how:

```bash
gizmosql_server --auth-log-level info --database mydata.db
```

Authentication events are logged with structured fields: user, peer IP, auth method, result (success/failure), and reason for failures.

### Query Logging

Track what queries are executed:

```bash
gizmosql_server --query-log-level info --database mydata.db
```

Each query is logged with: user, session ID, SQL statement, duration, and row count.

### Session Instrumentation *(Enterprise)*

For persistent audit trails, enable session instrumentation to record all connections and queries to a DuckDB database:

```bash
gizmosql_server --enable-instrumentation \
  --instrumentation-db-path /var/log/gizmosql/audit.db \
  --database mydata.db
```

Query the instrumentation database to analyze access patterns, identify anomalies, or generate compliance reports.

---

## Security Configuration Quick Reference

### Development (localhost only)

```bash
# Minimal — no TLS, simple password
gizmosql_server --username dev --password devpass --database dev.db
```

### Staging / Internal Network

```bash
# TLS + username/password
gizmosql_server --tls cert.pem key.pem \
  --username admin --password 'staging-password' \
  --secret-key 'shared-key-for-all-instances' \
  --database staging.db
```

### Production

```bash
# TLS + OAuth/SSO + email filtering + audit logging
gizmosql_server --tls cert.pem key.pem \
  --token-allowed-issuer "https://your-idp.example.com" \
  --token-allowed-audience "gizmosql-prod" \
  --oauth-client-id "$OAUTH_CLIENT_ID" \
  --oauth-client-secret "$OAUTH_CLIENT_SECRET" \
  --token-authorized-emails "*@mycompany.com" \
  --token-default-role user \
  --auth-log-level info \
  --query-log-level info \
  --enable-instrumentation \
  --database production.db
```

### High Security (mTLS + catalog permissions)

```bash
# mTLS + JWT tokens with per-catalog access rules
gizmosql_server --tls cert.pem key.pem \
  --mtls-ca-cert-filename client-ca.pem \
  --token-allowed-issuer "https://your-idp.example.com" \
  --token-allowed-audience "gizmosql-prod" \
  --token-authorized-emails "*@mycompany.com" \
  --token-default-role readonly \
  --auth-log-level info \
  --query-log-level info \
  --enable-instrumentation \
  --database production.db
```

---

## Frequently Asked Questions

**Q: Do I need TLS if I'm running on localhost?**
No. If the client and server are on the same machine, there's no network to eavesdrop on. TLS is only needed when data crosses a network boundary.

**Q: What's the difference between TLS and mTLS?**
TLS verifies the server to the client ("Am I talking to the real server?"). mTLS adds client verification to the server ("Is this client allowed to connect?"). Think of TLS as checking a store's business license; mTLS is like the store also checking your ID.

**Q: Should I use OAuth/SSO or username/password?**
If your organization already uses an identity provider (Google Workspace, Azure AD, Okta, Keycloak), OAuth/SSO is strongly recommended. It provides single sign-on, centralized user management, automatic provisioning/deprovisioning, and no passwords to manage. Username/password is fine for development, automation scripts, and service accounts.

**Q: How do I rotate the server's TLS certificate?**
Replace the certificate and key files, then restart the server. Active sessions will continue using the old certificate until they reconnect.

**Q: Can I use Let's Encrypt certificates?**
Yes. Use `certbot` or any ACME client to obtain certificates. Set up automatic renewal and restart the server when certificates are renewed.

**Q: What happens if a JWT token expires mid-session?**
GizmoSQL validates the token at connection time. Once a session is established, it remains valid until the client disconnects. Token expiration does not terminate active sessions.

**Q: How do I revoke a user's access?**
- **OAuth/SSO**: Disable the user in your identity provider. New connections will be rejected.
- **Email filtering**: Remove their email pattern from `--token-authorized-emails`.
- **Active sessions**: Use `KILL SESSION 'session-id'` (Enterprise) to terminate immediately.
- **Username/password**: Change the password and restart the server.
