# OAuth / SSO Setup Guide *(Enterprise)*

This guide provides step-by-step instructions for configuring GizmoSQL with popular identity providers using server-side OAuth. This allows users to authenticate via browser-based Single Sign-On from JDBC clients like DBeaver, IntelliJ, and other desktop tools.

> **Prerequisites:**
> - **GizmoSQL Enterprise Edition** on the server side.
> - **GizmoSQL JDBC Driver v1.5.0 or later** on the client side.
>
> See [Token Authentication](token_authentication.md) for general token auth configuration.

## How It Works

GizmoSQL uses **server-side OAuth** — the GizmoSQL server acts as a confidential OAuth client and handles the entire authorization code exchange. JDBC clients only need `authType=external` in their connection string — no client IDs, secrets, or OAuth configuration is distributed to clients.

1. The client connects with `authType=external` and is directed to a browser login page
2. The user authenticates with their Identity Provider (username/password, MFA, etc.)
3. The IdP redirects back to the GizmoSQL server's `/oauth/callback` endpoint with an authorization code
4. The server exchanges the code for tokens, validates the ID token via JWKS, and issues a GizmoSQL session JWT
5. The client receives the JWT and uses it for all subsequent requests

See [Server-Side OAuth Code Exchange](token_authentication.md#server-side-oauth-code-exchange-enterprise) for full technical details.

## Quick Reference

| Provider | Issuer URL | Client Type | Redirect URI |
|----------|-----------|-------------|--------------|
| [Keycloak](#keycloak) | `http(s)://<host>/realms/<realm>` | Confidential | `https://<server>:31339/oauth/callback` |
| [Azure AD](#azure-ad-microsoft-entra-id) | `https://login.microsoftonline.com/<tenant>/v2.0` | Web + client secret | `https://<server>:31339/oauth/callback` |
| [Google](#google) | `https://accounts.google.com` | Web application | `https://<server>:31339/oauth/callback` |
| [AWS Cognito](#aws-cognito) | `https://cognito-idp.<region>.amazonaws.com/<pool-id>` | Confidential + client secret | `https://<server>:31339/oauth/callback` |
| [Clerk](#clerk) | `https://clerk.<domain>.com` | Confidential | `https://<server>:31339/oauth/callback` |

---

## Keycloak

Keycloak is an open-source identity provider that can be self-hosted. It is recommended for testing and development.

### 1. Start Keycloak

```bash
docker run -d --name keycloak \
  -p 8080:8080 \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  quay.io/keycloak/keycloak:latest start-dev
```

### 2. Configure Keycloak

1. Open `http://localhost:8080` and log in as `admin`/`admin`
2. Create a new realm (e.g., `gizmosql`)
3. Create a client:
   - **Client ID:** `gizmosql-server`
   - **Client type:** OpenID Connect
   - **Client authentication:** ON (confidential client)
   - **Authentication flow:** Check "Standard flow" (Authorization Code)
4. Under **Settings** > **Valid redirect URIs:** add `https://<your-server>:31339/oauth/callback`
5. Under **Credentials**, copy the **Client Secret**
6. Create a test user under **Users** > **Add user**
7. Set a password under the user's **Credentials** tab

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "http://localhost:8080/realms/gizmosql" \
  --token-allowed-audience "gizmosql-server" \
  --token-default-role admin \
  --oauth-client-id "gizmosql-server" \
  --oauth-client-secret "YOUR_KEYCLOAK_CLIENT_SECRET" \
  --oauth-port 31339
```

### 4. Connect via JDBC

```
jdbc:gizmosql://localhost:31337?useEncryption=true&disableCertificateVerification=true&authType=external
```

**Issuer URL format:** `http(s)://<keycloak-host>/realms/<realm-name>`

---

## Azure AD (Microsoft Entra ID)

### 1. Register an Application

1. Go to [Microsoft Entra admin center](https://entra.microsoft.com)
2. Navigate to **Identity** > **Applications** > **App registrations** > **New registration**
3. Enter a name (e.g., `GizmoSQL Server`)
4. Under **Supported account types**, choose the appropriate option:
   - *Single tenant* for your organization only
   - *Multi-tenant* for any Azure AD organization
5. Under **Redirect URI**, select platform **Web**
6. Add `https://<your-server>:31339/oauth/callback` as the redirect URI
7. Click **Register**

### 2. Create a Client Secret

1. In your app registration, go to **Certificates & secrets** > **Client secrets**
2. Click **New client secret**, add a description, and choose an expiration
3. Copy the **Value** (this is your client secret — it's only shown once)

### 3. Configure API Permissions

1. Go to **API permissions** > **Add a permission**
2. Select **Microsoft Graph** > **Delegated permissions**
3. Add: `openid`, `profile`, `email`

### 4. Note Your IDs

- **Application (client) ID:** Found on the app's **Overview** page
- **Directory (tenant) ID:** Found on the app's **Overview** page or under **Microsoft Entra ID** > **Overview**

### 5. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://login.microsoftonline.com/YOUR_TENANT_ID/v2.0" \
  --token-allowed-audience "YOUR_CLIENT_ID" \
  --token-default-role admin \
  --oauth-client-id "YOUR_CLIENT_ID" \
  --oauth-client-secret "YOUR_CLIENT_SECRET" \
  --oauth-port 31339
```

### 6. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&authType=external
```

**Issuer URL format:** `https://login.microsoftonline.com/<tenant-id>/v2.0`

---

## Google

### 1. Create OAuth Credentials

1. Go to the [Google Cloud Console](https://console.cloud.google.com)
2. Select or create a project
3. Navigate to **APIs & Services** > **Credentials**
4. Click **Create Credentials** > **OAuth client ID**
5. Select application type: **Web application**
6. Enter a name (e.g., `GizmoSQL Server`)
7. Under **Authorized redirect URIs**, add `https://<your-server>:31339/oauth/callback`
8. Click **Create** and copy the **Client ID** and **Client Secret**

> **Important:** The client secret is a confidential credential — it stays on the GizmoSQL server and must never be shared with clients.

### 2. Configure Consent Screen

1. Go to **Google Auth Platform** > **Branding** (or **APIs & Services** > **OAuth consent screen**, which redirects to the same place)
2. Fill in app name, support email, and developer contact
3. Under **Audience**, choose **External** (or **Internal** for Google Workspace organizations)
4. Under **Data Access**, add scopes: `openid`, `profile`, `email`
5. For testing, add test users under **Audience** (max 100 while in "Testing" status)

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://accounts.google.com" \
  --token-allowed-audience "YOUR_CLIENT_ID.apps.googleusercontent.com" \
  --token-default-role admin \
  --token-authorized-emails "*@yourcompany.com" \
  --oauth-client-id "YOUR_CLIENT_ID.apps.googleusercontent.com" \
  --oauth-client-secret "GOCSPX-YOUR_CLIENT_SECRET" \
  --oauth-port 31339
```

### 4. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&authType=external
```

**Issuer URL:** `https://accounts.google.com` (fixed, global)

> **Tip:** When using Google as an **External** application, any Google account holder can authenticate. Use `--token-authorized-emails` to restrict access to specific domains or users (e.g., `*@yourcompany.com`). See [Authorized Email Filtering](token_authentication.md#authorized-email-filtering-enterprise) for details.

---

## AWS Cognito

### 1. Create a User Pool

1. Go to the [Amazon Cognito Console](https://console.aws.amazon.com/cognito/home)
2. Click **Create user pool**
3. Configure sign-in experience (email, username, etc.)
4. Configure security, sign-up, and messaging as needed
5. On the **Integrate your app** step:
   - App type: **Confidential client**
   - App client name: `GizmoSQL Server`
   - **Generate a client secret** (check this option)
   - Callback URL: `https://<your-server>:31339/oauth/callback`
   - OAuth 2.0 grant types: **Authorization code grant**
   - OpenID Connect scopes: `openid`, `profile`, `email`
6. Click **Create user pool**

### 2. Configure a Domain

You must set up a Cognito domain for the OAuth endpoints:

1. Go to your user pool > **App integration** > **Domain**
2. Choose a **Cognito domain** prefix (e.g., `gizmosql-auth`)
   - This creates: `https://gizmosql-auth.auth.<region>.amazoncognito.com`

### 3. Note Your IDs

- **User pool ID:** Found on the user pool **Overview** page (format: `<region>_<id>`)
- **Client ID:** Found under **App integration** > **App clients**
- **Client Secret:** Found under **App integration** > **App clients** > your client > **Show client secret**
- **Region:** The AWS region where the user pool was created

### 4. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_AbCdEfGhI" \
  --token-allowed-audience "YOUR_COGNITO_CLIENT_ID" \
  --token-default-role admin \
  --oauth-client-id "YOUR_COGNITO_CLIENT_ID" \
  --oauth-client-secret "YOUR_COGNITO_CLIENT_SECRET" \
  --oauth-port 31339
```

### 5. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&authType=external
```

**Issuer URL format:** `https://cognito-idp.<region>.amazonaws.com/<user-pool-id>`

---

## Clerk

### 1. Create an OAuth Application

1. Go to the [Clerk Dashboard](https://dashboard.clerk.com)
2. Navigate to **OAuth Applications**
3. Click **Add OAuth application**
4. Enter a name (e.g., `GizmoSQL Server`)
5. Choose **Confidential client** as the client type (this cannot be changed later)
6. Add `https://<your-server>:31339/oauth/callback` as a **Redirect URI**
7. Select scopes: `openid`, `profile`, `email`
8. Save and copy the **Client ID** and **Client Secret** (the secret is only shown once)

### 2. Find Your Issuer URL

- **Development:** `https://<verb-noun-##>.clerk.accounts.dev` (e.g., `https://jumping-tiger-00.clerk.accounts.dev`)
- **Production:** `https://clerk.<your-app-domain>.com`
- Verify by checking: `<issuer>/.well-known/openid-configuration`

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://clerk.myapp.com" \
  --token-allowed-audience "YOUR_CLERK_CLIENT_ID" \
  --token-default-role admin \
  --oauth-client-id "YOUR_CLERK_CLIENT_ID" \
  --oauth-client-secret "YOUR_CLERK_CLIENT_SECRET" \
  --oauth-port 31339
```

### 4. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&authType=external
```

**Issuer URL format:** `https://clerk.<your-domain>.com` or `https://<slug>.clerk.accounts.dev`

---

## GizmoSQL Server Configuration Reference

For all providers, the server-side configuration follows the same pattern:

| Server Option | Description |
|---------------|-------------|
| `--oauth-client-id` | OAuth client ID from your IdP. Setting this enables the OAuth HTTP server. |
| `--oauth-client-secret` | OAuth client secret (confidential, stays on server). |
| `--oauth-scopes` | OAuth scopes to request (default: `openid profile email`). |
| `--oauth-port` | Port for the OAuth HTTP(S) server (default: `31339`). |
| `--oauth-base-url` | Override the base URL for the OAuth server (e.g., `https://my-proxy:443`). Redirect URI and discovery URL are derived from this. Auto-constructed from `localhost` if empty. |
| `--oauth-disable-tls` | Disable TLS on the OAuth callback server. **WARNING: localhost only.** |
| `--token-allowed-issuer` | Must match the `iss` claim in tokens from your IdP. |
| `--token-allowed-audience` | Must match the `aud` claim (usually the client ID). |
| `--token-default-role` | Role to assign when IdP tokens lack a `role` claim. |
| `--token-authorized-emails` | Comma-separated list of authorized email patterns (e.g., `*@company.com`). Default: `*` (all). |
| `--token-jwks-uri` | *(Optional)* Explicit JWKS endpoint; auto-discovered from issuer if not set. |

**Environment variables:**

```bash
export GIZMOSQL_OAUTH_CLIENT_ID="your-client-id"
export GIZMOSQL_OAUTH_CLIENT_SECRET="your-client-secret"
export GIZMOSQL_OAUTH_PORT="31339"
# export GIZMOSQL_OAUTH_BASE_URL="https://my-proxy:443"  # Override base URL when behind a reverse proxy
# export GIZMOSQL_OAUTH_DISABLE_TLS="true"  # WARNING: localhost development only
export GIZMOSQL_TOKEN_ALLOWED_ISSUER="https://your-idp.com"
export GIZMOSQL_TOKEN_ALLOWED_AUDIENCE="your-client-id"
export GIZMOSQL_TOKEN_DEFAULT_ROLE="admin"
# export GIZMOSQL_TOKEN_AUTHORIZED_EMAILS="*@yourcompany.com"  # Restrict by email pattern
```

See [Token Authentication](token_authentication.md) for complete server configuration details.

## JDBC Client Configuration

With server-side OAuth, client configuration is minimal:

```
jdbc:gizmosql://hostname:31337?useEncryption=true&authType=external
```

No client IDs, secrets, or OAuth endpoints need to be configured on the client side. The `authType=external` property tells the JDBC driver to use the server's OAuth flow.

## Troubleshooting

### "redirect_uri_mismatch" Error

The IdP rejected the redirect URI. Ensure the redirect URI registered in your IdP exactly matches the GizmoSQL server's OAuth callback URL.

- Default callback URL: `https://<your-server>:31339/oauth/callback`
- If using `--oauth-base-url`, the redirect URI is derived as `<base-url>/oauth/callback` — ensure this matches what's registered in the IdP
- Check for `http` vs `https` mismatches

### "Token validation failed: issuer mismatch"

The `--token-allowed-issuer` on the server doesn't match the `iss` claim in the token.

- Check the exact issuer URL (trailing slashes matter)
- Verify with: `curl <issuer>/.well-known/openid-configuration | jq .issuer`

### "Token validation failed: audience mismatch"

The `--token-allowed-audience` doesn't match the `aud` claim in the token.

- For most providers, the audience is the **Client ID**
- Some providers allow configuring custom audiences

### "role claim is required" / Token Rejected

Standard OIDC tokens don't include a `role` claim. Set `--token-default-role` on the server:

```bash
gizmosql_server --token-default-role admin ...
```

### "Email not authorized" / User Rejected After Login

The user authenticated successfully with the IdP but their email doesn't match any pattern in `--token-authorized-emails`. Either add their email or domain pattern:

```bash
--token-authorized-emails "*@yourcompany.com,partner@external.com"
```

### "SSO/OAuth authentication requires GizmoSQL Enterprise"

JWKS-based token validation requires an Enterprise license. Static cert path verification (`--token-signature-verify-cert-path`) is available in Core edition.
