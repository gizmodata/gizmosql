# Security & Authentication

GizmoSQL provides multiple layers of security to protect your data and connections.

---

## Overview

Security features include:

- 🔐 **TLS Encryption** - Secure data in transit
- 🔑 **Password Authentication** - User credential validation
- 🎫 **JWT Tokens** - Signed authentication tokens
- 🔒 **mTLS** - Mutual TLS for client verification (optional)

---

## TLS Encryption

TLS (Transport Layer Security) encrypts all data transmitted between clients and the GizmoSQL server.

### Enable TLS

<!-- tabs:start -->

#### **Docker**

```bash
docker run --name gizmosql \
  --publish 31337:31337 \
  --env TLS_ENABLED="1" \
  --env GIZMOSQL_PASSWORD="secure_password" \
  gizmodata/gizmosql:latest
```

#### **CLI**

```bash
GIZMOSQL_PASSWORD="secure_password" \
  gizmosql_server \
  --database-filename data/mydb.duckdb \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem
```

<!-- tabs:end -->

### Generate TLS Certificates

For development/testing, generate self-signed certificates:

```bash
# Navigate to TLS directory
cd tls

# Run certificate generation script
./gen-certs.sh
```

For production, use certificates from a trusted Certificate Authority (CA) like:
- Let's Encrypt
- DigiCert
- AWS Certificate Manager
- Azure Key Vault

### Custom TLS Certificates

```bash
docker run --name gizmosql \
  --publish 31337:31337 \
  --env TLS_ENABLED="1" \
  --env TLS_CERT="/certs/server.crt" \
  --env TLS_KEY="/certs/server.key" \
  --env GIZMOSQL_PASSWORD="secure_password" \
  --mount type=bind,source=/path/to/certs,target=/certs \
  gizmodata/gizmosql:latest
```

!> **Production Warning**: Never disable TLS in production environments unless behind a secure proxy or using mTLS sidecars.

---

## Password Authentication

GizmoSQL requires password authentication for all connections.

### Set Password

The password is set via the `GIZMOSQL_PASSWORD` environment variable:

```bash
export GIZMOSQL_PASSWORD="your_secure_password_here"
```

?> **Tip**: Generate strong passwords using:
```bash
openssl rand -base64 32
```

### Password Requirements

While GizmoSQL doesn't enforce password complexity, we recommend:

- Minimum 16 characters
- Mix of uppercase, lowercase, numbers, and symbols
- Avoid common words or patterns
- Use a password manager

### Password in Clients

<!-- tabs:start -->

#### **JDBC**

```text
jdbc:arrow-flight-sql://localhost:31337?user=gizmosql_username&password=your_password
```

#### **Python ADBC**

```python
from adbc_driver_flightsql import dbapi as gizmosql

conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "your_password"
    }
)
```

#### **CLI**

```bash
gizmosql_client \
  --username "gizmosql_username" \
  --password "your_password" \
  --query "SELECT version()"
```

<!-- tabs:end -->

---

## JWT Token Authentication

GizmoSQL automatically issues JWT (JSON Web Token) tokens after successful password authentication.

### How It Works

1. Client authenticates with username/password
2. Server validates credentials
3. Server issues signed JWT token
4. Client uses token for subsequent requests
5. Server validates token signature

### Token Lifespan

Tokens are valid for the lifetime of the server instance. If the server restarts:
- New secret key is generated
- Old tokens become invalid
- Clients must re-authenticate

### Generate Tokens Programmatically

See [generate-gizmosql-token](https://github.com/gizmodata/generate-gizmosql-token) for examples.

### Using Tokens

```python
from adbc_driver_flightsql import dbapi as gizmosql

# Use token directly
conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "authorization_header": f"Bearer {your_jwt_token}"
    }
)
```

---

## Mutual TLS (mTLS)

Mutual TLS provides two-way authentication where both client and server verify each other's certificates.

### Enable mTLS

```bash
gizmosql_server \
  --tls-cert /certs/server.crt \
  --tls-key /certs/server.key \
  --tls-ca /certs/ca.crt \
  --database-filename data/mydb.duckdb
```

### Client Configuration

Clients must present valid certificates signed by the trusted CA:

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "password",
        DatabaseOptions.TLS_ROOT_CERTS.value: "/path/to/ca.crt",
        DatabaseOptions.TLS_CERT_CHAIN.value: "/path/to/client.crt",
        DatabaseOptions.TLS_PRIVATE_KEY.value: "/path/to/client.key"
    }
)
```

---

## Network Security

### Bind to Specific Interface

Restrict network access by binding to specific interfaces:

```bash
# Localhost only
export GIZMOSQL_HOST="127.0.0.1"

# Specific IP
export GIZMOSQL_HOST="192.168.1.100"

# All interfaces (default)
export GIZMOSQL_HOST="0.0.0.0"
```

### Firewall Rules

Configure firewall to allow only trusted sources:

```bash
# UFW (Ubuntu)
sudo ufw allow from 192.168.1.0/24 to any port 31337

# firewalld (RHEL/CentOS)
sudo firewall-cmd --add-rich-rule='rule family="ipv4" source address="192.168.1.0/24" port port="31337" protocol="tcp" accept'

# iptables
sudo iptables -A INPUT -p tcp -s 192.168.1.0/24 --dport 31337 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 31337 -j DROP
```

---

## Kubernetes Security

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: gizmosql-netpol
spec:
  podSelector:
    matchLabels:
      app: gizmosql
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: trusted-namespace
    ports:
    - protocol: TCP
      port: 31337
```

### Secrets Management

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: gizmosql-secret
type: Opaque
stringData:
  password: your-secure-password-here
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gizmosql
spec:
  template:
    spec:
      containers:
      - name: gizmosql
        env:
        - name: GIZMOSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: gizmosql-secret
              key: password
```

### Pod Security Standards

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gizmosql
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: gizmosql
    image: gizmodata/gizmosql:latest
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        drop:
        - ALL
      readOnlyRootFilesystem: false
```

---

## Security Best Practices

### 1. Use Strong Authentication

```bash
# Generate strong password
GIZMOSQL_PASSWORD=$(openssl rand -base64 32)

# Store in secrets manager (example: AWS Secrets Manager)
aws secretsmanager create-secret \
  --name gizmosql/password \
  --secret-string "$GIZMOSQL_PASSWORD"
```

### 2. Always Enable TLS

```bash
TLS_ENABLED="1"  # Never set to "0" in production
```

### 3. Rotate Credentials Regularly

Implement a credential rotation policy:
- Change passwords every 90 days
- Regenerate TLS certificates before expiration
- Update client configurations accordingly

### 4. Use Certificate Verification

Don't skip certificate verification in production:

```python
# ❌ BAD - Skips verification
DatabaseOptions.TLS_SKIP_VERIFY.value: "true"

# ✅ GOOD - Verifies certificate
DatabaseOptions.TLS_ROOT_CERTS.value: "/path/to/ca.crt"
```

### 5. Limit Query Execution Time

Prevent resource exhaustion:

```bash
gizmosql_server --query-timeout 300  # 5 minutes max
```

### 6. Monitor Access Logs

Enable query logging to track access:

```bash
PRINT_QUERIES="1"
```

### 7. Principle of Least Privilege

Grant minimal necessary permissions to database files:

```bash
chmod 600 /data/database.duckdb
chown gizmosql:gizmosql /data/database.duckdb
```

### 8. Regular Updates

Keep GizmoSQL and dependencies updated:

```bash
docker pull gizmodata/gizmosql:latest
```

---

## Compliance Considerations

### GDPR

- Encrypt data in transit (TLS)
- Implement access controls
- Maintain audit logs
- Enable data deletion capabilities

### HIPAA

- Use strong encryption (TLS 1.3)
- Implement authentication and authorization
- Maintain audit trails
- Regular security assessments

### SOC 2

- Document security policies
- Implement access controls
- Monitor and log access
- Incident response procedures

---

## Security Checklist

Before deploying to production:

- [ ] TLS enabled with valid certificates
- [ ] Strong password (min 16 chars)
- [ ] Certificate verification enabled
- [ ] Network access restricted
- [ ] Firewall rules configured
- [ ] Query timeouts set
- [ ] Monitoring and logging enabled
- [ ] Secrets stored securely
- [ ] Regular backup plan
- [ ] Incident response plan
- [ ] Security patch policy

---

## Incident Response

### Suspected Breach

1. **Isolate** - Disconnect affected systems
2. **Assess** - Determine scope of breach
3. **Contain** - Prevent further access
4. **Eradicate** - Remove threat
5. **Recover** - Restore normal operations
6. **Review** - Analyze and improve

### Emergency Procedures

```bash
# Immediately stop server
docker stop gizmosql

# Rotate credentials
export GIZMOSQL_PASSWORD=$(openssl rand -base64 32)

# Review logs
docker logs gizmosql > incident-logs-$(date +%Y%m%d).txt

# Restart with new credentials
docker start gizmosql
```

---

## Vulnerability Reporting

If you discover a security vulnerability:

📧 Email: security@gizmodata.com  
🔐 PGP Key: Available on request

Please include:
- Description of vulnerability
- Steps to reproduce
- Potential impact
- Suggested mitigation

---

## Additional Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CIS Benchmarks](https://www.cisecurity.org/cis-benchmarks/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [Apache Arrow Security](https://arrow.apache.org/security/)

---

## Next Steps

- [Performance tuning](performance.md)
- [Troubleshooting](troubleshooting.md)
- [Configuration options](configuration.md)
