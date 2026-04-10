// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import Foundation
import Security

/// Manages self-signed TLS certificate generation for the GizmoSQL server.
/// Uses Apple's Security framework to generate keys and certificates,
/// then exports as PEM for Arrow Flight's gRPC TLS.
enum CertificateGenerator {
    struct CertPaths {
        let certPath: String
        let keyPath: String
    }

    /// Ensure TLS certificates exist. Generates them on first launch.
    static func ensureCertificates() -> CertPaths {
        let paths = certPaths()

        if FileManager.default.fileExists(atPath: paths.certPath),
           FileManager.default.fileExists(atPath: paths.keyPath) {
            return paths
        }

        // Create the directory if needed
        let dir = (paths.certPath as NSString).deletingLastPathComponent
        try? FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)

        generateSelfSignedCert(certPath: paths.certPath, keyPath: paths.keyPath)
        return paths
    }

    /// Delete existing certificates (e.g., for regeneration).
    static func deleteCertificates() {
        let paths = certPaths()
        try? FileManager.default.removeItem(atPath: paths.certPath)
        try? FileManager.default.removeItem(atPath: paths.keyPath)
    }

    static func certPaths() -> CertPaths {
        let appSupport = FileManager.default.urls(
            for: .applicationSupportDirectory, in: .userDomainMask
        ).first!.appendingPathComponent("GizmoSQL")

        return CertPaths(
            certPath: appSupport.appendingPathComponent("server.pem").path,
            keyPath: appSupport.appendingPathComponent("server.key").path
        )
    }

    // MARK: - Certificate Generation via Security Framework

    private static func generateSelfSignedCert(certPath: String, keyPath: String) {
        // Generate RSA-4096 key pair
        let keyAttrs: [String: Any] = [
            kSecAttrKeyType as String: kSecAttrKeyTypeRSA,
            kSecAttrKeySizeInBits as String: 4096,
        ]

        var error: Unmanaged<CFError>?
        guard let privateKey = SecKeyCreateRandomKey(keyAttrs as CFDictionary, &error) else {
            print("WARNING: Failed to generate RSA key: \(error!.takeRetainedValue())")
            return
        }

        guard let publicKey = SecKeyCopyPublicKey(privateKey) else {
            print("WARNING: Failed to extract public key")
            return
        }

        // Export private key as PEM
        guard let privateKeyData = SecKeyCopyExternalRepresentation(privateKey, &error) as Data? else {
            print("WARNING: Failed to export private key: \(error!.takeRetainedValue())")
            return
        }

        let privateKeyPEM = convertToPEM(data: privateKeyData, label: "RSA PRIVATE KEY")
        try? privateKeyPEM.write(toFile: keyPath, atomically: true, encoding: .utf8)

        // Create self-signed certificate using SecCertificate
        // Apple's Security framework doesn't have a direct API to create X.509 certs,
        // so we build a minimal DER-encoded self-signed certificate manually.
        guard let certData = createSelfSignedCertDER(
            publicKey: publicKey,
            privateKey: privateKey,
            commonName: "GizmoSQL iOS",
            validityDays: 3650
        ) else {
            print("WARNING: Failed to create self-signed certificate")
            return
        }

        let certPEM = convertToPEM(data: certData, label: "CERTIFICATE")
        try? certPEM.write(toFile: certPath, atomically: true, encoding: .utf8)
    }

    private static func convertToPEM(data: Data, label: String) -> String {
        let base64 = data.base64EncodedString(options: [.lineLength76Characters, .endLineWithLineFeed])
        return "-----BEGIN \(label)-----\n\(base64)\n-----END \(label)-----\n"
    }

    // MARK: - DER Certificate Builder

    /// Build a minimal self-signed X.509 v1 certificate in DER format.
    /// This is a simplified implementation sufficient for local TLS.
    private static func createSelfSignedCertDER(
        publicKey: SecKey,
        privateKey: SecKey,
        commonName: String,
        validityDays: Int
    ) -> Data? {
        guard let pubKeyData = SecKeyCopyExternalRepresentation(publicKey, nil) as Data? else {
            return nil
        }

        // Build TBS (To Be Signed) certificate
        var tbs = Data()

        // Version: v1 (default, no explicit version field needed for v1)

        // Serial number: random 16 bytes
        var serial = Data(count: 16)
        _ = serial.withUnsafeMutableBytes { SecRandomCopyBytes(kSecRandomDefault, 16, $0.baseAddress!) }
        serial[0] &= 0x7F  // Ensure positive
        tbs.append(asn1Integer(serial))

        // Signature algorithm: SHA256withRSA (OID 1.2.840.113549.1.1.11)
        tbs.append(sha256WithRSAAlgorithm())

        // Issuer: CN=commonName
        tbs.append(asn1Name(commonName: commonName))

        // Validity
        let now = Date()
        let expiry = Calendar.current.date(byAdding: .day, value: validityDays, to: now)!
        tbs.append(asn1Validity(notBefore: now, notAfter: expiry))

        // Subject: same as issuer (self-signed)
        tbs.append(asn1Name(commonName: commonName))

        // Subject public key info (RSA)
        tbs.append(asn1SubjectPublicKeyInfo(rsaPublicKey: pubKeyData))

        let tbsSequence = asn1Sequence(tbs)

        // Sign the TBS certificate
        guard let signature = sign(data: tbsSequence, with: privateKey) else {
            return nil
        }

        // Build final certificate: SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
        var cert = Data()
        cert.append(tbsSequence)
        cert.append(sha256WithRSAAlgorithm())
        cert.append(asn1BitString(signature))

        return asn1Sequence(cert)
    }

    private static func sign(data: Data, with privateKey: SecKey) -> Data? {
        var error: Unmanaged<CFError>?
        guard let signature = SecKeyCreateSignature(
            privateKey,
            .rsaSignatureMessagePKCS1v15SHA256,
            data as CFData,
            &error
        ) as Data? else {
            return nil
        }
        return signature
    }

    // MARK: - ASN.1 DER Helpers

    private static func asn1Tag(_ tag: UInt8, _ content: Data) -> Data {
        var result = Data([tag])
        let len = content.count
        if len < 128 {
            result.append(UInt8(len))
        } else if len < 256 {
            result.append(contentsOf: [0x81, UInt8(len)])
        } else if len < 65536 {
            result.append(contentsOf: [0x82, UInt8(len >> 8), UInt8(len & 0xFF)])
        } else {
            result.append(contentsOf: [0x83, UInt8(len >> 16), UInt8((len >> 8) & 0xFF), UInt8(len & 0xFF)])
        }
        result.append(content)
        return result
    }

    private static func asn1Sequence(_ content: Data) -> Data {
        asn1Tag(0x30, content)
    }

    private static func asn1Set(_ content: Data) -> Data {
        asn1Tag(0x31, content)
    }

    private static func asn1Integer(_ value: Data) -> Data {
        var v = value
        // Ensure leading zero if high bit set (positive integer)
        if let first = v.first, first & 0x80 != 0 {
            v.insert(0x00, at: 0)
        }
        return asn1Tag(0x02, v)
    }

    private static func asn1BitString(_ content: Data) -> Data {
        var bs = Data([0x00]) // no unused bits
        bs.append(content)
        return asn1Tag(0x03, bs)
    }

    private static func asn1OctetString(_ content: Data) -> Data {
        asn1Tag(0x04, content)
    }

    private static func asn1UTF8String(_ string: String) -> Data {
        asn1Tag(0x0C, Data(string.utf8))
    }

    private static func asn1UTCTime(_ date: Date) -> Data {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyMMddHHmmss'Z'"
        formatter.timeZone = TimeZone(identifier: "UTC")
        let str = formatter.string(from: date)
        return asn1Tag(0x17, Data(str.utf8))
    }

    private static func asn1OID(_ oid: [UInt8]) -> Data {
        asn1Tag(0x06, Data(oid))
    }

    private static func sha256WithRSAAlgorithm() -> Data {
        // OID 1.2.840.113549.1.1.11 (sha256WithRSAEncryption) + NULL params
        let oid = asn1OID([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B])
        let null = Data([0x05, 0x00])
        var seq = oid
        seq.append(null)
        return asn1Sequence(seq)
    }

    private static func asn1Name(commonName: String) -> Data {
        // CN OID: 2.5.4.3
        let cnOID = asn1OID([0x55, 0x04, 0x03])
        let cnValue = asn1UTF8String(commonName)
        var atv = cnOID
        atv.append(cnValue)
        let atvSequence = asn1Sequence(atv)
        let rdn = asn1Set(atvSequence)
        return asn1Sequence(rdn)
    }

    private static func asn1Validity(notBefore: Date, notAfter: Date) -> Data {
        var validity = asn1UTCTime(notBefore)
        validity.append(asn1UTCTime(notAfter))
        return asn1Sequence(validity)
    }

    private static func asn1SubjectPublicKeyInfo(rsaPublicKey: Data) -> Data {
        // Algorithm: rsaEncryption (1.2.840.113549.1.1.1) + NULL
        let rsaOID = asn1OID([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01])
        let null = Data([0x05, 0x00])
        var algo = rsaOID
        algo.append(null)
        let algoSeq = asn1Sequence(algo)

        // The public key from SecKey is in PKCS#1 format, wrap in BIT STRING
        let keyBitString = asn1BitString(rsaPublicKey)

        var spki = algoSeq
        spki.append(keyBitString)
        return asn1Sequence(spki)
    }
}
