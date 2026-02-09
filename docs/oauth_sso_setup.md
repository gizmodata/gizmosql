# OAuth / SSO Setup Guide *(Enterprise)*

This guide provides step-by-step instructions for configuring GizmoSQL with popular identity providers using the OIDC Authorization Code + PKCE flow. This allows users to authenticate via browser-based Single Sign-On from JDBC clients like DBeaver, IntelliJ, and other desktop tools.

> **Prerequisites:**
> - **GizmoSQL Enterprise Edition** on the server side.
> - **GizmoSQL JDBC Driver v1.5.0 or later** on the client side.
>
> See [Token Authentication](token_authentication.md) for general token auth configuration.

## How It Works

1. The JDBC driver opens a browser to your Identity Provider's login page
2. The user authenticates (username/password, MFA, etc.)
3. The IdP redirects back to a temporary localhost callback server started by the driver
4. The driver exchanges the authorization code for an access token (using PKCE, no client secret needed)
5. The access token is sent to the GizmoSQL server as a Bearer token
6. The server validates the token via JWKS auto-discovery from the IdP

## Quick Reference

| Provider | Issuer URL | PKCE | Client Secret | Localhost Redirect |
|----------|-----------|------|---------------|-------------------|
| [Keycloak](#keycloak) | `http(s)://<host>/realms/<realm>` | Yes (S256) | Not required | Any port |
| [Azure AD](#azure-ad-microsoft-entra-id) | `https://login.microsoftonline.com/<tenant>/v2.0` | Yes (S256) | Not required | Any port (native app) |
| [Google](#google) | `https://accounts.google.com` | Yes (S256) | **Required** | Any port (Desktop app) |
| [AWS Cognito](#aws-cognito) | `https://cognito-idp.<region>.amazonaws.com/<pool-id>` | Yes (S256 only) | Not required | Exact match |
| [Clerk](#clerk) | `https://clerk.<domain>.com` | Yes (S256) | Not required | Exact match (verify) |

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
   - **Client ID:** `gizmosql-desktop`
   - **Client type:** OpenID Connect
   - **Client authentication:** OFF (public client)
   - **Authentication flow:** Check "Standard flow" (Authorization Code)
4. Under **Settings** > **Valid redirect URIs:** add `http://127.0.0.1/*`
5. Create a test user under **Users** > **Add user**
6. Set a password under the user's **Credentials** tab

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "http://localhost:8080/realms/gizmosql" \
  --token-allowed-audience "gizmosql-desktop" \
  --token-default-role admin
```

### 4. Connect via JDBC

```
jdbc:gizmosql://localhost:31337?useEncryption=true&disableCertificateVerification=true&oidc.issuer=http://localhost:8080/realms/gizmosql&oidc.clientId=gizmosql-desktop
```

**Issuer URL format:** `http(s)://<keycloak-host>/realms/<realm-name>`

---

## Azure AD (Microsoft Entra ID)

### 1. Register an Application

1. Go to [Microsoft Entra admin center](https://entra.microsoft.com)
2. Navigate to **Identity** > **Applications** > **App registrations** > **New registration**
3. Enter a name (e.g., `GizmoSQL Desktop`)
4. Under **Supported account types**, choose the appropriate option:
   - *Single tenant* for your organization only
   - *Multi-tenant* for any Azure AD organization
5. Under **Redirect URI**, select platform **Mobile and desktop applications**
6. Add `http://localhost` as the redirect URI
7. Click **Register**

### 2. Enable Public Client

1. In your app registration, go to **Authentication** > **Advanced settings**
2. Set **Allow public client flows** to **Yes**
3. Save

### 3. Configure API Permissions

1. Go to **API permissions** > **Add a permission**
2. Select **Microsoft Graph** > **Delegated permissions**
3. Add: `openid`, `profile`, `email`

### 4. Note Your IDs

- **Application (client) ID:** Found on the app's **Overview** page
- **Directory (tenant) ID:** Found on the **Overview** page or in **Azure Active Directory** > **Properties**

### 5. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://login.microsoftonline.com/YOUR_TENANT_ID/v2.0" \
  --token-allowed-audience "YOUR_CLIENT_ID" \
  --token-default-role admin
```

### 6. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&oidc.issuer=https://login.microsoftonline.com/YOUR_TENANT_ID/v2.0&oidc.clientId=YOUR_CLIENT_ID&oidc.scopes=openid%20profile%20email%20offline_access
```

> **Note:** Include `offline_access` in scopes to receive refresh tokens for automatic token renewal.

**Issuer URL format:** `https://login.microsoftonline.com/<tenant-id>/v2.0`

---

## Google

### 1. Create OAuth Credentials

1. Go to the [Google Cloud Console](https://console.cloud.google.com)
2. Select or create a project
3. Navigate to **APIs & Services** > **Credentials**
4. Click **Create Credentials** > **OAuth client ID**
5. Select application type: **Desktop app**
6. Enter a name (e.g., `GizmoSQL Desktop`)
7. Click **Create** and copy the **Client ID** and **Client Secret**

> **Note:** Google generates a client secret even for Desktop app clients. While this secret is not truly confidential (it's embedded in the desktop application), Google's token endpoint requires it in the token exchange request. You must include it as `oidc.clientSecret` in the JDBC connection string.

### 2. Configure Consent Screen

1. Go to **APIs & Services** > **OAuth consent screen**
2. Choose **External** (or **Internal** for Google Workspace organizations)
3. Fill in app name, support email, and developer contact
4. Add scopes: `openid`, `profile`, `email`
5. For testing, add test users (max 100 while in "Testing" status)

> **Note:** Google Desktop app clients automatically allow `http://127.0.0.1` redirects on any port. No explicit redirect URI configuration is needed.

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://accounts.google.com" \
  --token-allowed-audience "YOUR_CLIENT_ID.apps.googleusercontent.com" \
  --token-default-role admin
```

### 4. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&oidc.issuer=https://accounts.google.com&oidc.clientId=YOUR_CLIENT_ID.apps.googleusercontent.com&oidc.clientSecret=YOUR_CLIENT_SECRET&oidc.scopes=openid%20profile%20email
```

**Issuer URL:** `https://accounts.google.com` (fixed, global)

---

## AWS Cognito

### 1. Create a User Pool

1. Go to the [Amazon Cognito Console](https://console.aws.amazon.com/cognito/home)
2. Click **Create user pool**
3. Configure sign-in experience (email, username, etc.)
4. Configure security, sign-up, and messaging as needed
5. On the **Integrate your app** step:
   - App type: **Public client**
   - App client name: `GizmoSQL Desktop`
   - **Do NOT generate a client secret** (leave unchecked)
   - Callback URL: `http://localhost/callback`
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
- **Region:** The AWS region where the user pool was created

### 4. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_AbCdEfGhI" \
  --token-allowed-audience "YOUR_COGNITO_CLIENT_ID" \
  --token-default-role admin
```

### 5. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&oidc.issuer=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_AbCdEfGhI&oidc.clientId=YOUR_COGNITO_CLIENT_ID&oidc.scopes=openid%20profile%20email
```

> **Note:** Cognito only supports S256 code challenge method (not plain). The GizmoSQL JDBC driver uses S256 by default.

**Issuer URL format:** `https://cognito-idp.<region>.amazonaws.com/<user-pool-id>`

---

## Clerk

### 1. Create an OAuth Application

1. Go to the [Clerk Dashboard](https://dashboard.clerk.com)
2. Navigate to **OAuth Applications**
3. Click **Add OAuth application**
4. Enter a name (e.g., `GizmoSQL Desktop`)
5. Enable the **Public** toggle (allows PKCE without client secret)
6. Add `http://127.0.0.1/callback` as a **Redirect URI**
7. Select scopes: `openid`, `profile`, `email`
8. Save and copy the **Client ID**

### 2. Find Your Issuer URL

- **Development:** `https://<instance-slug>.clerk.accounts.dev`
- **Production:** `https://clerk.<your-app-domain>.com`
- Verify by checking: `<issuer>/.well-known/openid-configuration`

### 3. Start GizmoSQL Server

```bash
gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls tls/server.pem tls/server.key \
  --token-allowed-issuer "https://clerk.myapp.com" \
  --token-allowed-audience "YOUR_CLERK_CLIENT_ID" \
  --token-default-role admin
```

### 4. Connect via JDBC

```
jdbc:gizmosql://gizmosql.example.com:31337?useEncryption=true&useSystemTrustStore=true&oidc.issuer=https://clerk.myapp.com&oidc.clientId=YOUR_CLERK_CLIENT_ID&oidc.scopes=openid%20profile%20email
```

**Issuer URL format:** `https://clerk.<your-domain>.com` or `https://<slug>.clerk.accounts.dev`

---

## GizmoSQL Server Configuration Reference

For all providers, the server-side configuration follows the same pattern:

| Server Option | Description |
|---------------|-------------|
| `--token-allowed-issuer` | Must match the `iss` claim in tokens from your IdP |
| `--token-allowed-audience` | Must match the `aud` claim (usually the client ID) |
| `--token-default-role` | Role to assign when IdP tokens lack a `role` claim |
| `--token-jwks-uri` | *(Optional)* Explicit JWKS endpoint; auto-discovered from issuer if not set |

**Environment variables:**

```bash
export GIZMOSQL_TOKEN_JWKS_URI="https://your-idp.com/.well-known/jwks.json"   # Optional
export GIZMOSQL_TOKEN_DEFAULT_ROLE="admin"
```

See [Token Authentication](token_authentication.md) for complete server configuration details.

## Troubleshooting

### "redirect_uri_mismatch" Error

The IdP rejected the redirect URI. This happens when the callback URL registered in the IdP doesn't match what the JDBC driver sends.

- The driver uses `http://127.0.0.1:<random-port>/callback`
- **Google:** Desktop app clients handle this automatically (no registration needed)
- **Azure AD:** Register `http://localhost` as a "Mobile and desktop" platform redirect
- **Keycloak:** Use a wildcard pattern: `http://127.0.0.1/*`
- **Cognito/Clerk:** These use exact matching. Register `http://localhost/callback` and test

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

### "SSO/OAuth authentication requires GizmoSQL Enterprise"

JWKS-based token validation requires an Enterprise license. Static cert path verification (`--token-signature-verify-cert-path`) is available in Core edition.
