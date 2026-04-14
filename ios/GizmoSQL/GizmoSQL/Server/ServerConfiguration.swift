// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import Foundation
import Security

/// Persistent server configuration backed by UserDefaults and Keychain.
struct ServerConfiguration {
    @AppStorageValue("port", defaultValue: 31337) var port: Int
    @AppStorageValue("backend", defaultValue: 0) var backend: Int  // 0 = DuckDB, 1 = SQLite
    @AppStorageValue("databaseFilename", defaultValue: "") var databaseFilename: String
    @AppStorageValue("readOnly", defaultValue: false) var readOnly: Bool
    @AppStorageValue("username", defaultValue: "gizmosql_user") var username: String
    @AppStorageValue("logLevel", defaultValue: "info") var logLevel: String
    @AppStorageValue("initSqlCommands", defaultValue: "") var initSqlCommands: String
    @AppStorageValue("queryTimeout", defaultValue: 0) var queryTimeout: Int
    @AppStorageValue("printQueries", defaultValue: true) var printQueries: Bool
    @AppStorageValue("logFormat", defaultValue: "text") var logFormat: String

    /// Password stored in Keychain for security.
    var password: String {
        get { KeychainHelper.read(key: "gizmosql_password") ?? "" }
        set { KeychainHelper.write(key: "gizmosql_password", value: newValue) }
    }

    /// Secret key stored in Keychain.
    var secretKey: String {
        get { KeychainHelper.read(key: "gizmosql_secret_key") ?? "" }
        set { KeychainHelper.write(key: "gizmosql_secret_key", value: newValue) }
    }

    /// Generate a random password if none is set.
    mutating func ensurePassword() {
        if password.isEmpty {
            password = Self.randomAlphanumeric(length: 16)
        }
    }

    /// Generate a random secret key if none is set.
    mutating func ensureSecretKey() {
        if secretKey.isEmpty {
            secretKey = Self.randomAlphanumeric(length: 32)
        }
    }

    private static func randomAlphanumeric(length: Int) -> String {
        let chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0..<length).map { _ in chars.randomElement()! })
    }
}

// MARK: - AppStorage-like wrapper for non-View contexts

@propertyWrapper
struct AppStorageValue<T> {
    let key: String
    let defaultValue: T

    init(_ key: String, defaultValue: T) {
        self.key = key
        self.defaultValue = defaultValue
    }

    var wrappedValue: T {
        get {
            UserDefaults.standard.object(forKey: key) as? T ?? defaultValue
        }
        set {
            UserDefaults.standard.set(newValue, forKey: key)
        }
    }
}

// MARK: - Keychain Helper

enum KeychainHelper {
    static func write(key: String, value: String) {
        let data = Data(value.utf8)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrAccount as String: key,
            kSecAttrService as String: "com.gizmodata.gizmosql",
        ]
        SecItemDelete(query as CFDictionary)
        var addQuery = query
        addQuery[kSecValueData as String] = data
        SecItemAdd(addQuery as CFDictionary, nil)
    }

    static func read(key: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrAccount as String: key,
            kSecAttrService as String: "com.gizmodata.gizmosql",
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        guard status == errSecSuccess, let data = result as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }
}
