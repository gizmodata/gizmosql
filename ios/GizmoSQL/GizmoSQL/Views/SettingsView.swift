// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI

struct SettingsView: View {
    @EnvironmentObject var serverManager: ServerManager

    @AppStorage("port") private var port = 31337
    @AppStorage("backend") private var backend = 0
    @AppStorage("databaseFilename") private var databaseFilename = ""
    @AppStorage("readOnly") private var readOnly = false
    @AppStorage("username") private var username = "gizmosql_user"
    @AppStorage("logLevel") private var logLevel = "info"
    @AppStorage("initSqlCommands") private var initSqlCommands = ""
    @AppStorage("queryTimeout") private var queryTimeout = 0
    @AppStorage("printQueries") private var printQueries = true
    @AppStorage("logFormat") private var logFormat = "text"

    @State private var password = ""
    @State private var showPassword = false
    @State private var showCertInfo = false

    private var isRunning: Bool { serverManager.state != .stopped }

    var body: some View {
        NavigationStack {
            Form {
                serverSection
                authSection
                loggingSection
                advancedSection
                tlsSection
                aboutSection
            }
            .navigationTitle("Settings")
            .scrollDismissesKeyboard(.interactively)
            .allowsHitTesting(!isRunning)
            .opacity(isRunning ? 0.5 : 1.0)
            .safeAreaInset(edge: .bottom) {
                if isRunning {
                    Text("Stop the server to change settings")
                        .font(.caption)
                        .padding(8)
                        .frame(maxWidth: .infinity)
                        .background(.ultraThinMaterial)
                }
            }
            .onAppear {
                password = KeychainHelper.read(key: "gizmosql_password") ?? ""
            }
        }
    }

    // MARK: - Sections

    private var serverSection: some View {
        Section("Server") {
            HStack {
                Text("Port")
                Spacer()
                TextField("31337", value: $port, format: .number.grouping(.never))
                    .keyboardType(.numberPad)
                    .multilineTextAlignment(.trailing)
                    .frame(width: 80)
            }

            Picker("Backend", selection: $backend) {
                Text("DuckDB").tag(0)
                Text("SQLite").tag(1)
            }

            HStack {
                Text("Database")
                Spacer()
                TextField(":memory:", text: $databaseFilename)
                    .multilineTextAlignment(.trailing)
                    .foregroundStyle(databaseFilename.isEmpty ? .secondary : .primary)
            }

            Toggle("Read-Only", isOn: $readOnly)
        }
    }

    private var authSection: some View {
        Section("Authentication") {
            HStack {
                Text("Username")
                Spacer()
                TextField("gizmosql_user", text: $username)
                    .multilineTextAlignment(.trailing)
                    .autocorrectionDisabled()
                    .textInputAutocapitalization(.never)
            }

            HStack {
                Text("Password")
                Spacer()
                if showPassword {
                    TextField("Required", text: $password)
                        .multilineTextAlignment(.trailing)
                        .autocorrectionDisabled()
                        .textInputAutocapitalization(.never)
                } else {
                    SecureField("Required", text: $password)
                        .multilineTextAlignment(.trailing)
                }
                Button {
                    showPassword.toggle()
                } label: {
                    Image(systemName: showPassword ? "eye.slash" : "eye")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.plain)
            }
            .onChange(of: password) {
                KeychainHelper.write(key: "gizmosql_password", value: password)
            }

            Button("Generate Random Password") {
                password = (0..<16).map { _ in
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".randomElement()!
                }.map(String.init).joined()
            }
        }
    }

    private var loggingSection: some View {
        Section("Logging") {
            Toggle("Print Queries", isOn: $printQueries)

            Picker("Log Level", selection: $logLevel) {
                Text("Debug").tag("debug")
                Text("Info").tag("info")
                Text("Warning").tag("warn")
                Text("Error").tag("error")
            }

            Picker("Log Format", selection: $logFormat) {
                Text("Text").tag("text")
                Text("JSON").tag("json")
            }
        }
    }

    private var advancedSection: some View {
        Section("Advanced") {
            HStack {
                Text("Query Timeout (sec)")
                Spacer()
                TextField("0 = unlimited", value: $queryTimeout, format: .number.grouping(.never))
                    .keyboardType(.numberPad)
                    .multilineTextAlignment(.trailing)
                    .frame(width: 100)
            }

            VStack(alignment: .leading, spacing: 8) {
                Text("Init SQL Commands")
                TextEditor(text: $initSqlCommands)
                    .font(.system(.caption, design: .monospaced))
                    .frame(minHeight: 80)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(.quaternary, lineWidth: 1)
                    )
            }
        }
    }

    private var tlsSection: some View {
        Section("TLS Certificate") {
            let paths = CertificateGenerator.certPaths()
            let certExists = FileManager.default.fileExists(atPath: paths.certPath)

            HStack {
                Image(systemName: certExists ? "lock.fill" : "lock.open")
                    .foregroundStyle(certExists ? .green : .orange)
                Text(certExists ? "Self-signed certificate ready" : "No certificate — will generate on start")
                    .font(.subheadline)
            }

            if certExists {
                Button("Regenerate Certificate", role: .destructive) {
                    CertificateGenerator.deleteCertificates()
                    _ = CertificateGenerator.ensureCertificates()
                }
            }
        }
    }

    private var aboutSection: some View {
        Section {
            HStack {
                Text("Version")
                Spacer()
                Text(AppInfo.version)
                    .foregroundStyle(.secondary)
            }
            HStack {
                Text("Build")
                Spacer()
                Text(AppInfo.build)
                    .foregroundStyle(.secondary)
            }
            Link(destination: URL(string: "https://github.com/gizmodata/gizmosql")!) {
                HStack {
                    Text("Project")
                    Spacer()
                    Text("github.com/gizmodata/gizmosql")
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Image(systemName: "arrow.up.right.square")
                        .foregroundStyle(.secondary)
                }
            }
        } header: {
            Text("About")
        } footer: {
            Text("© 2025 GizmoData LLC · Apache License 2.0")
        }
    }
}

// MARK: - App Info

/// Lightweight accessor for bundle version metadata. Used by `SettingsView`'s
/// About section and the `.about` dot command in `DotCommandProcessor`.
enum AppInfo {
    static var version: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "unknown"
    }

    static var build: String {
        Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "unknown"
    }

    static var versionDisplay: String {
        "v\(version) (build \(build))"
    }
}
