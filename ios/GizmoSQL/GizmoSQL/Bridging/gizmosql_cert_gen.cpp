// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
//
// Self-signed TLS certificate generator using OpenSSL/BoringSSL APIs.
// These APIs are already linked transitively through Arrow Flight's gRPC dependency.

#include "gizmosql_cert_gen.h"

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <cstdio>
#include <memory>

namespace {

// RAII helpers for OpenSSL types
struct EVP_PKEY_Deleter { void operator()(EVP_PKEY* p) { EVP_PKEY_free(p); } };
struct X509_Deleter { void operator()(X509* p) { X509_free(p); } };
struct BIO_Deleter { void operator()(BIO* p) { BIO_free_all(p); } };
struct EVP_PKEY_CTX_Deleter { void operator()(EVP_PKEY_CTX* p) { EVP_PKEY_CTX_free(p); } };

using UniqueEVP_PKEY = std::unique_ptr<EVP_PKEY, EVP_PKEY_Deleter>;
using UniqueX509 = std::unique_ptr<X509, X509_Deleter>;
using UniqueBIO = std::unique_ptr<BIO, BIO_Deleter>;
using UniqueEVP_PKEY_CTX = std::unique_ptr<EVP_PKEY_CTX, EVP_PKEY_CTX_Deleter>;

/// Add a Subject Alternative Name extension to the certificate.
bool add_san_extension(X509* cert, const char* san_value) {
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, cert, cert, nullptr, nullptr, 0);

    X509_EXTENSION* ext = X509V3_EXT_nconf_nid(
        nullptr, &ctx, NID_subject_alt_name, san_value);
    if (!ext) return false;

    X509_add_ext(cert, ext, -1);
    X509_EXTENSION_free(ext);
    return true;
}

}  // namespace

extern "C" {

int gizmosql_generate_self_signed_cert(
    const char* cert_path,
    const char* key_path,
    const char* common_name,
    int validity_days) {

    if (!cert_path || !key_path || !common_name || validity_days <= 0) {
        return 1;
    }

    // Generate RSA-4096 key pair
    UniqueEVP_PKEY_CTX pkey_ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
    if (!pkey_ctx) return 2;

    if (EVP_PKEY_keygen_init(pkey_ctx.get()) <= 0) return 3;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(pkey_ctx.get(), 4096) <= 0) return 4;

    EVP_PKEY* raw_pkey = nullptr;
    if (EVP_PKEY_keygen(pkey_ctx.get(), &raw_pkey) <= 0) return 5;
    UniqueEVP_PKEY pkey(raw_pkey);

    // Create X.509 certificate
    UniqueX509 cert(X509_new());
    if (!cert) return 6;

    // Version 3 (0-indexed, so 2 = v3)
    X509_set_version(cert.get(), 2);

    // Serial number: 1
    ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);

    // Validity period
    X509_gmtime_adj(X509_get_notBefore(cert.get()), 0);
    X509_gmtime_adj(X509_get_notAfter(cert.get()),
                    static_cast<long>(validity_days) * 24 * 60 * 60);

    // Set subject and issuer (self-signed, so they're the same)
    X509_NAME* name = X509_get_subject_name(cert.get());
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>(common_name),
                               -1, -1, 0);
    X509_set_issuer_name(cert.get(), name);

    // Set public key
    X509_set_pubkey(cert.get(), pkey.get());

    // Add Subject Alternative Names: localhost, 0.0.0.0, and all common local IPs
    add_san_extension(cert.get(),
        "DNS:localhost,IP:0.0.0.0,IP:127.0.0.1");

    // Sign with SHA-256
    if (!X509_sign(cert.get(), pkey.get(), EVP_sha256())) return 7;

    // Write certificate PEM
    {
        FILE* f = fopen(cert_path, "w");
        if (!f) return 8;
        int rc = PEM_write_X509(f, cert.get());
        fclose(f);
        if (!rc) return 9;
    }

    // Write private key PEM
    {
        FILE* f = fopen(key_path, "w");
        if (!f) return 10;
        int rc = PEM_write_PrivateKey(f, pkey.get(), nullptr, nullptr, 0, nullptr, nullptr);
        fclose(f);
        if (!rc) return 11;
    }

    return 0;
}

}  // extern "C"
