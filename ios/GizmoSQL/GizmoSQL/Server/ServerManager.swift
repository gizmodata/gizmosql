// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import Foundation
import Network
import Combine
import UIKit
import UserNotifications

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
    private var shouldRestartOnForeground: Bool = false

    /// Disconnect hooks registered by views that own their own in-app
    /// Flight SQL client (QueryView, BrowserView). Called on background
    /// transition BEFORE the server is stopped and BEFORE we read the
    /// session count for the disconnect notification. Each hook closes
    /// its client and returns the number of sessions it closed (0 or 1),
    /// which is subtracted from the total so the app's own clients never
    /// show up as "dropped external sessions" to the user.
    private var inAppClientHooks: [UUID: () -> Int] = [:]

    // UserDefaults key for one-shot notification-permission request
    private static let notifPermissionRequestedKey = "gizmosql.notifPermissionRequested"

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

        let initSql = config.initSqlCommands

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
                self?.requestNotificationPermissionIfNeeded()
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

    // MARK: - In-App Client Disconnect Hooks

    /// Register a hook that disconnects a view's in-app Flight SQL client.
    /// The hook is invoked on background transition BEFORE the server is
    /// stopped and BEFORE we read the session count for the disconnect
    /// notification. It should return the number of sessions it closed
    /// (0 if the client wasn't connected, 1 if it was).
    ///
    /// Returns a token the view can use to unregister on disappear.
    @discardableResult
    func registerInAppClientHook(_ hook: @escaping () -> Int) -> UUID {
        let id = UUID()
        inAppClientHooks[id] = hook
        return id
    }

    func unregisterInAppClientHook(_ id: UUID) {
        inAppClientHooks.removeValue(forKey: id)
    }

    private func runInAppClientHooks() -> Int {
        var closed = 0
        for hook in inAppClientHooks.values {
            closed += hook()
        }
        return closed
    }

    // MARK: - Background Handling

    func didEnterBackground() {
        // Request a background-task window so the server's shutdown handshake
        // has time to run before iOS suspends us.
        backgroundTaskID = UIApplication.shared.beginBackgroundTask(
            withName: "GizmoSQL-Server"
        ) { [weak self] in
            self?.endBackgroundTask()
        }

        guard state == .running else { return }

        // Close the app's own in-app clients first so their sessions don't
        // show up as "dropped external sessions" in the notification. Each
        // hook returns the number of sessions it closed (0 or 1).
        let inAppClosed = runInAppClientHooks()

        // Report only external (non-in-app) sessions to the user. iOS will
        // suspend the gRPC server regardless — a local notification is the
        // only way to reach a user whose phone is no longer in the foreground.
        let externalDropped = max(0, activeSessionCount - inAppClosed)
        if externalDropped > 0 {
            postDisconnectNotification(sessionCount: externalDropped)
        }

        // Remember the running state so we can bring the server back up
        // automatically on return. The in-app views will auto-reconnect
        // via their existing `.onChange(of: serverManager.state)` handlers
        // when state transitions back to `.running`.
        shouldRestartOnForeground = true
        stopServer()
    }

    func didBecomeActive() {
        endBackgroundTask()

        guard shouldRestartOnForeground else { return }
        shouldRestartOnForeground = false

        if state == .stopped {
            startServer()
        } else {
            // The server may still be shutting down from the background transition.
            // Poll briefly until it settles, then start it back up.
            restartWhenStopped()
        }
    }

    private func restartWhenStopped(attempt: Int = 0) {
        let maxAttempts = 50  // 50 × 100ms = 5 seconds
        if state == .stopped {
            startServer()
            return
        }
        if attempt >= maxAttempts { return }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { [weak self] in
            self?.restartWhenStopped(attempt: attempt + 1)
        }
    }

    private func endBackgroundTask() {
        guard backgroundTaskID != .invalid else { return }
        UIApplication.shared.endBackgroundTask(backgroundTaskID)
        backgroundTaskID = .invalid
    }

    // MARK: - Local Notifications

    /// Request notification permission once per install. Called on first
    /// successful server start — the first moment the feature becomes relevant.
    /// Denial is silently respected; notifications simply won't fire later.
    private func requestNotificationPermissionIfNeeded() {
        let defaults = UserDefaults.standard
        guard !defaults.bool(forKey: Self.notifPermissionRequestedKey) else { return }
        defaults.set(true, forKey: Self.notifPermissionRequestedKey)

        UNUserNotificationCenter.current().requestAuthorization(
            options: [.alert, .sound]
        ) { _, _ in
            // Result isn't persisted — we always check current settings at fire time.
        }
    }

    private func postDisconnectNotification(sessionCount: Int) {
        let center = UNUserNotificationCenter.current()
        center.getNotificationSettings { settings in
            guard settings.authorizationStatus == .authorized ||
                  settings.authorizationStatus == .provisional else { return }

            let content = UNMutableNotificationContent()
            content.title = "GizmoSQL server stopped"
            let plural = sessionCount == 1 ? "client was" : "clients were"
            content.body = "\(sessionCount) \(plural) disconnected when the app was backgrounded. Reopen GizmoSQL to restart the server."
            content.sound = .default

            // Fire after 1s — gives iOS a moment to acknowledge the background
            // transition before the notification surfaces.
            let trigger = UNTimeIntervalNotificationTrigger(timeInterval: 1, repeats: false)
            let request = UNNotificationRequest(
                identifier: "gizmosql.disconnect",
                content: content,
                trigger: trigger
            )
            center.add(request, withCompletionHandler: nil)
        }
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
