// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI

struct ServerStatusView: View {
    @EnvironmentObject var serverManager: ServerManager

    var body: some View {
        NavigationStack {
            VStack(spacing: 24) {
                // Logo header
                Image("GizmoSQLLogo")
                    .resizable()
                    .scaledToFit()
                    .frame(height: 100)

                // Status indicator
                statusCard

                // Connection info (visible when running)
                if serverManager.state == .running {
                    connectionCard
                }

                Spacer()

                // Start/Stop button
                actionButton
            }
            .padding()
            .frame(maxWidth: 600)
            .frame(maxWidth: .infinity)
            .navigationBarTitleDisplayMode(.inline)
        }
    }

    // MARK: - Status Card

    private var statusCard: some View {
        HStack(spacing: 16) {
            Circle()
                .fill(statusColor)
                .frame(width: 16, height: 16)

            VStack(alignment: .leading, spacing: 4) {
                Text("Server \(serverManager.state.rawValue)")
                    .font(.headline)

                if let version = gizmosql_server_version().flatMap({ String(cString: $0) }) {
                    Text(version)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            if serverManager.state == .running {
                VStack(alignment: .trailing) {
                    Text("\(serverManager.activeSessionCount)")
                        .font(.title2.monospacedDigit())
                        .fontWeight(.semibold)
                    Text("sessions")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .padding()
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12))
    }

    // MARK: - Connection Card

    private var connectionCard: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Connection")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)

            if let ip = serverManager.wifiIPAddress {
                let connString = "grpc+tls://\(ip):\(serverManager.configuration.port)"

                HStack {
                    Text(connString)
                        .font(.system(.body, design: .monospaced))
                        .textSelection(.enabled)

                    Spacer()

                    Button {
                        UIPasteboard.general.string = connString
                    } label: {
                        Image(systemName: "doc.on.doc")
                    }
                }

                HStack(spacing: 8) {
                    Image(systemName: "wifi")
                        .foregroundStyle(.green)
                    Text(ip)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(verbatim: "Port \(serverManager.configuration.port)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            } else {
                HStack {
                    Image(systemName: "wifi.slash")
                        .foregroundStyle(.orange)
                    Text("No WiFi connection — clients cannot connect")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .padding()
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12))
    }

    // MARK: - Action Button

    private var actionButton: some View {
        Button {
            switch serverManager.state {
            case .stopped:
                serverManager.startServer()
            case .running:
                serverManager.stopServer()
            default:
                break
            }
        } label: {
            HStack {
                Image(systemName: serverManager.state == .stopped ? "play.fill" : "stop.fill")
                Text(serverManager.state == .stopped ? "Start Server" : "Stop Server")
            }
            .frame(maxWidth: .infinity)
            .padding()
        }
        .buttonStyle(.borderedProminent)
        .tint(serverManager.state == .stopped ? .green : .red)
        .disabled(serverManager.state == .starting || serverManager.state == .stopping)
    }

    private var statusColor: Color {
        switch serverManager.state {
        case .stopped: return .red
        case .starting, .stopping: return .yellow
        case .running: return .green
        }
    }
}
