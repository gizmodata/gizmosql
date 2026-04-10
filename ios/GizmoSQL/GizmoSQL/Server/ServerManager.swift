// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import Foundation
import Network
import Combine
import UIKit

enum ServerState: String {
    case stopped = "Stopped"
    case starting = "Starting..."
    case running = "Running"
    case stopping = "Stopping..."
}

@MainActor
class ServerManager: ObservableObject {
    @Published var state: ServerState = .stopped
    @Published var logLines: [String] = []
    @Published var wifiIPAddress: String?
    @Published var activeSessionCount: Int = 0
    @Published var errorMessage: String?

    var configuration = ServerConfiguration()

    private var serverThread: Thread?
    private var logMonitorSource: DispatchSourceFileSystemObject?
    private var logFileHandle: FileHandle?
    private var sessionTimer: Timer?
    private var pathMonitor: NWPathMonitor?
    private var backgroundTaskID: UIBackgroundTaskIdentifier = .invalid

    private let maxLogLines = 10_000

    init() {
        startNetworkMonitor()
    }

    deinit {
        pathMonitor?.cancel()
    }

    // MARK: - Server Lifecycle

    func startServer() {
        guard state == .stopped else { return }
        state = .starting
        errorMessage = nil
        logLines.removeAll()

        configuration.ensurePassword()
        configuration.ensureSecretKey()

        // Ensure TLS certificates exist
        let certPaths = CertificateGenerator.ensureCertificates()

        // Prepare log file
        let logFilePath = Self.logFilePath
        FileManager.default.createFile(atPath: logFilePath, contents: nil)

        // Start monitoring log file
        startLogMonitor(path: logFilePath)

        // Build config
        let config = configuration
        let port = Int32(config.port)
        let backend = Int32(config.backend)
        let readOnly = Int32(config.readOnly ? 1 : 0)
        let timeout = Int32(config.queryTimeout)
        let printQueries = Int32(config.printQueries ? 1 : 0)

        // Capture values for the background thread
        let password = config.password
        let username = config.username
        let secretKey = config.secretKey
        let logLevel = config.logLevel
        let logFormat = config.logFormat
        let dbFilename = config.databaseFilename

        // Build init SQL: prepend LOAD statements for any bundled
        // out-of-tree DuckDB extensions before the user's init SQL.
        // These extensions ship inside the app bundle as code-signed
        // dylibs (.duckdb_extension). iOS allows dlopen() of dylibs
        // that live inside the signed app bundle.
        //
        // The bundled .duckdb_extension files have their trailing
        // DuckDB footer stripped (so codesign accepts them as valid
        // Mach-O), so we tell DuckDB to skip the footer/signature
        // checks via allow_unsigned_extensions and
        // allow_extensions_metadata_mismatch.
        var bundledLoads = ""
        let frameworksURL = Bundle.main.bundleURL
            .appendingPathComponent("Frameworks", isDirectory: true)
        var foundAnyBundled = false
        for ext in ["postgres_scanner"] {
            let path = frameworksURL
                .appendingPathComponent("\(ext).duckdb_extension")
                .path
            if FileManager.default.fileExists(atPath: path) {
                let escaped = path.replacingOccurrences(of: "'", with: "''")
                bundledLoads += "LOAD '\(escaped)';"
                foundAnyBundled = true
            }
        }
        if foundAnyBundled {
            bundledLoads = "SET allow_unsigned_extensions = true;" +
                           "SET allow_extensions_metadata_mismatch = true;" +
                           bundledLoads
        }
        let initSql = bundledLoads + config.initSqlCommands

        serverThread = Thread {
            // DuckDB creates ~/.duckdb for extension storage. On iOS, HOME points
            // to the container root which isn't writable. Override HOME to Documents.
            let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
            setenv("HOME", docs.path, 1)
            FileManager.default.changeCurrentDirectoryPath(docs.path)

            // strdup all strings so they stay alive for the blocking call
            let cDb = dbFilename.isEmpty ? nil : strdup(dbFilename)
            let cPass = strdup(password)
            let cUser = strdup(username)
            let cKey = strdup(secretKey)
            let cLevel = strdup(logLevel)
            let cFormat = strdup(logFormat)
            let cLog = strdup(logFilePath)
            let cCert = strdup(certPaths.certPath)
            let cKeyPath = strdup(certPaths.keyPath)
            let cSql = initSql.isEmpty ? nil : strdup(initSql)

            var cConfig = GizmoSQLServerConfig()
            cConfig.port = port
            cConfig.backend = backend
            cConfig.read_only = readOnly
            cConfig.query_timeout = timeout
            cConfig.print_queries = printQueries
            cConfig.database_filename = UnsafePointer(cDb)
            cConfig.password = UnsafePointer(cPass)
            cConfig.username = UnsafePointer(cUser)
            cConfig.secret_key = UnsafePointer(cKey)
            cConfig.log_level = UnsafePointer(cLevel)
            cConfig.log_format = UnsafePointer(cFormat)
            cConfig.log_file = UnsafePointer(cLog)
            cConfig.tls_cert_path = UnsafePointer(cCert)
            cConfig.tls_key_path = UnsafePointer(cKeyPath)
            cConfig.init_sql_commands = UnsafePointer(cSql)
            cConfig.hostname = nil

            _ = gizmosql_server_start(&cConfig)

            // Server has stopped — free the strings
            free(cDb); free(cPass); free(cUser); free(cKey)
            free(cLevel); free(cFormat); free(cLog); free(cCert); free(cKeyPath); free(cSql)

            DispatchQueue.main.async { [weak self] in
                self?.serverDidStop()
            }
        }
        serverThread?.name = "GizmoSQL-Server"
        serverThread?.start()

        // Delay state update slightly to let server bind
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            if self?.state == .starting {
                self?.state = .running
                self?.startSessionPolling()
            }
        }
    }

    func stopServer() {
        guard state == .running else { return }
        state = .stopping
        gizmosql_server_request_shutdown()
    }

    private func serverDidStop() {
        stopSessionPolling()
        stopLogMonitor()
        gizmosql_server_cleanup()
        endBackgroundTask()
        activeSessionCount = 0
        state = .stopped
    }

    // MARK: - Log Monitoring

    private func startLogMonitor(path: String) {
        let fd = open(path, O_RDONLY)
        guard fd >= 0 else { return }

        logFileHandle = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
        // Seek to end — we only want new output
        logFileHandle?.seekToEndOfFile()

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: fd,
            eventMask: .write,
            queue: .global(qos: .utility)
        )

        source.setEventHandler { [weak self] in
            guard let self, let handle = self.logFileHandle else { return }
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }

            let newLines = text.components(separatedBy: "\n").filter { !$0.isEmpty }
            DispatchQueue.main.async {
                self.logLines.append(contentsOf: newLines)
                // Trim if exceeding max
                if self.logLines.count > self.maxLogLines {
                    self.logLines.removeFirst(self.logLines.count - self.maxLogLines)
                }
            }
        }

        source.setCancelHandler {
            close(fd)
        }

        source.resume()
        logMonitorSource = source
    }

    private func stopLogMonitor() {
        logMonitorSource?.cancel()
        logMonitorSource = nil
        logFileHandle = nil
    }

    // MARK: - Session Count Polling

    private func startSessionPolling() {
        sessionTimer = Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.activeSessionCount = Int(gizmosql_server_active_sessions())
            }
        }
    }

    private func stopSessionPolling() {
        sessionTimer?.invalidate()
        sessionTimer = nil
    }

    // MARK: - Network Monitoring

    private func startNetworkMonitor() {
        pathMonitor = NWPathMonitor(requiredInterfaceType: .wifi)
        pathMonitor?.pathUpdateHandler = { [weak self] path in
            let ip = path.status == .satisfied ? Self.getWiFiAddress() : nil
            DispatchQueue.main.async {
                self?.wifiIPAddress = ip
            }
        }
        pathMonitor?.start(queue: .global(qos: .utility))
        // Initial check
        wifiIPAddress = Self.getWiFiAddress()
    }

    // MARK: - Background Handling

    func didEnterBackground() {
        guard state == .running else { return }

        // Request extra time so iOS doesn't immediately suspend the server thread.
        // This gives connected clients a window to finish in-flight queries.
        backgroundTaskID = UIApplication.shared.beginBackgroundTask(
            withName: "GizmoSQL-Server"
        ) { [weak self] in
            // Expiration handler — iOS is about to suspend us.
            self?.endBackgroundTask()
        }
    }

    func didBecomeActive() {
        endBackgroundTask()
    }

    private func endBackgroundTask() {
        guard backgroundTaskID != .invalid else { return }
        UIApplication.shared.endBackgroundTask(backgroundTaskID)
        backgroundTaskID = .invalid
    }

    // MARK: - Helpers

    nonisolated static var logFilePath: String {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
        return docs.appendingPathComponent("gizmosql_server.log").path
    }

    nonisolated static func getWiFiAddress() -> String? {
        var address: String?
        var ifaddr: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&ifaddr) == 0, let firstAddr = ifaddr else { return nil }
        defer { freeifaddrs(ifaddr) }

        var ptr = firstAddr
        while true {
            let interface = ptr.pointee
            let addrFamily = interface.ifa_addr.pointee.sa_family
            if addrFamily == UInt8(AF_INET) {
                let name = String(cString: interface.ifa_name)
                if name == "en0" {
                    var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                    getnameinfo(interface.ifa_addr,
                                socklen_t(interface.ifa_addr.pointee.sa_len),
                                &hostname, socklen_t(hostname.count),
                                nil, 0, NI_NUMERICHOST)
                    address = String(cString: hostname)
                    break
                }
            }
            guard let next = interface.ifa_next else { break }
            ptr = next
        }
        return address
    }
}

// MARK: - Optional C String Helper

extension Optional where Wrapped == String {
    func withOptionalCString<R>(_ body: (UnsafePointer<CChar>?) -> R) -> R {
        switch self {
        case .some(let str): return str.withCString { body($0) }
        case .none: return body(nil)
        }
    }
}
