// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

#ifndef GIZMOSQL_CERT_GEN_H
#define GIZMOSQL_CERT_GEN_H

#ifdef __cplusplus
extern "C" {
#endif

/// Generate a self-signed TLS certificate and private key.
///
/// Writes PEM-encoded certificate to cert_path and private key to key_path.
/// The certificate includes Subject Alternative Names for localhost and 0.0.0.0.
///
/// @param cert_path    Output path for the PEM certificate file.
/// @param key_path     Output path for the PEM private key file.
/// @param common_name  Certificate Common Name (e.g., "GizmoSQL iOS").
/// @param validity_days Number of days the certificate is valid.
/// @return 0 on success, non-zero on error.
int gizmosql_generate_self_signed_cert(
    const char* cert_path,
    const char* key_path,
    const char* common_name,
    int validity_days);

#ifdef __cplusplus
}
#endif

#endif // GIZMOSQL_CERT_GEN_H
