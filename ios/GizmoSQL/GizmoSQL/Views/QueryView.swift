// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI
import UIKit

// MARK: - Flight SQL Client Wrapper

@MainActor
class FlightSQLClient: ObservableObject {
    private var handle: UnsafeMutableRawPointer?
    @Published var isConnected = false

    // Stored for reconnection
    private var lastPort: Int = 0
    private var lastUsername: String = ""
    private var lastPassword: String = ""
    private var lastTlsCertPath: String = ""

    func connect(port: Int, username: String, password: String, tlsCertPath: String) -> String? {
        disconnect()

        lastPort = port
        lastUsername = username
        lastPassword = password
        lastTlsCertPath = tlsCertPath

        var errorPtr: UnsafeMutablePointer<CChar>?
        handle = gizmosql_client_connect(
            "localhost",
            Int32(port),
            username,
            password,
            tlsCertPath,
            1,  // tls_skip_verify — self-signed cert on localhost
            &errorPtr
        )
        if handle == nil {
            let msg = errorPtr.map { String(cString: $0) } ?? "Unknown error"
            errorPtr.map { free($0) }
            isConnected = false
            return msg
        }
        isConnected = true
        return nil
    }

    /// Attempt to reconnect using stored connection parameters.
    /// Returns nil on success, error string on failure.
    private func reconnect() -> String? {
        guard lastPort > 0 else { return "No previous connection" }
        return connect(port: lastPort, username: lastUsername,
                       password: lastPassword, tlsCertPath: lastTlsCertPath)
    }

    static let defaultMaxRows: Int64 = 500

    func execute(_ sql: String, maxRows: Int64 = FlightSQLClient.defaultMaxRows) -> QueryResult {
        guard handle != nil else { return QueryResult(error: "Not connected") }

        let result = executeOnce(sql, maxRows: maxRows)

        // If the query failed, try reconnecting once (handles stale connections
        // after the app returns from background).
        if result.error != nil {
            if reconnect() == nil {
                return executeOnce(sql, maxRows: maxRows)
            }
        }

        return result
    }

    private func executeOnce(_ sql: String, maxRows: Int64) -> QueryResult {
        guard let handle else { return QueryResult(error: "Not connected") }
        guard let rawResult = gizmosql_client_execute(handle, sql, maxRows) else {
            return QueryResult(error: "Execution failed")
        }
        defer { gizmosql_client_free_result(rawResult) }
        let r = rawResult.pointee
        if let err = r.error {
            return QueryResult(error: String(cString: err), elapsed: r.elapsed_seconds)
        }
        if r.rows_affected >= 0 {
            return QueryResult(rowsAffected: r.rows_affected, elapsed: r.elapsed_seconds)
        }

        var columns: [ColumnInfo] = []
        for i in 0..<Int(r.num_columns) {
            columns.append(ColumnInfo(
                name: String(cString: r.column_names[i]!),
                type: String(cString: r.column_types[i]!)
            ))
        }

        var rows: [[String]] = []
        for row in 0..<Int(r.num_rows) {
            var rowData: [String] = []
            for col in 0..<Int(r.num_columns) {
                let idx = row * Int(r.num_columns) + col
                rowData.append(String(cString: r.values[idx]!))
            }
            rows.append(rowData)
        }

        return QueryResult(
            columns: columns, rows: rows, elapsed: r.elapsed_seconds,
            totalRows: r.total_rows, truncated: r.truncated != 0
        )
    }

    func disconnect() {
        if let handle {
            gizmosql_client_disconnect(handle)
        }
        handle = nil
        isConnected = false
    }

    deinit {
        if let handle {
            gizmosql_client_disconnect(handle)
        }
    }
}

// MARK: - Data Models

struct ColumnInfo {
    let name: String
    let type: String
}

struct QueryResult {
    var columns: [ColumnInfo] = []
    var rows: [[String]] = []
    var error: String?
    var rowsAffected: Int64 = -1
    var elapsed: Double = 0
    var totalRows: Int64 = -2   // -1 = unknown, -2 = not applicable
    var truncated: Bool = false
}

struct OutputLine: Identifiable {
    let id = UUID()
    let segments: [StyledSegment]
}

struct StyledSegment {
    let text: String
    let color: Color
}

// MARK: - Terminal Colors (matching gizmosql_client)

enum TermColor {
    static let background = Color(red: 0.1, green: 0.1, blue: 0.1)
    static let prompt = Color(red: 1.0, green: 0.6, blue: 0.2)    // dark orange (ANSI 208)
    static let error = Color(red: 1.0, green: 0.3, blue: 0.3)
    static let keyword = Color.green
    static let function_ = Color.cyan
    static let string = Color.yellow
    static let number = Color(red: 0.9, green: 0.5, blue: 0.9)    // magenta
    static let comment = Color.gray
    static let border = Color(white: 0.5)
    static let header = Color.white
    static let type = Color(white: 0.5)
    static let data = Color(white: 0.85)
    static let null_ = Color(white: 0.4)
    static let info = Color(white: 0.6)
    static let success = Color.green
}

// MARK: - Box Drawing Renderer

struct BoxRenderer {
    static func render(_ result: QueryResult) -> [OutputLine] {
        guard !result.columns.isEmpty else { return [] }

        let numCols = result.columns.count

        // Calculate column widths
        var widths = result.columns.map { max($0.name.count, $0.type.count) }
        for row in result.rows {
            for (i, cell) in row.enumerated() where i < numCols {
                widths[i] = max(widths[i], cell.count)
            }
        }
        // Minimum width of 4, cap at 40
        widths = widths.map { max(4, min($0, 40)) }

        var lines: [OutputLine] = []

        // ┌───┬───┐
        lines.append(borderLine("┌", "┬", "┐", widths))

        // │ col1 │ col2 │
        lines.append(dataLine(result.columns.map { $0.name }, widths, TermColor.header))

        // │ type1 │ type2 │
        lines.append(dataLine(result.columns.map { $0.type }, widths, TermColor.type))

        // ├───┼───┤
        lines.append(borderLine("├", "┼", "┤", widths))

        // Data rows
        for row in result.rows {
            let colors = row.enumerated().map { (i, cell) -> Color in
                if cell == "NULL" { return TermColor.null_ }
                let type = i < numCols ? result.columns[i].type : ""
                if ["integer", "bigint", "uinteger", "float", "double", "decimal"].contains(type) {
                    return TermColor.number
                }
                return TermColor.data
            }
            lines.append(coloredDataLine(row, widths, colors))
        }

        // └───┴───┘
        lines.append(borderLine("└", "┴", "┘", widths))

        // Footer
        let rowLabel = result.rows.count == 1 ? "row" : "rows"
        let colLabel = numCols == 1 ? "column" : "columns"
        var footerText = "\(result.rows.count) \(rowLabel), \(numCols) \(colLabel)"
        if result.truncated {
            if result.totalRows >= 0 {
                footerText += " (showing \(result.rows.count) of \(result.totalRows))"
            } else {
                footerText += " (truncated)"
            }
        }
        lines.append(OutputLine(segments: [
            StyledSegment(text: footerText, color: TermColor.info)
        ]))

        return lines
    }

    private static func borderLine(_ left: String, _ mid: String, _ right: String,
                                    _ widths: [Int]) -> OutputLine {
        let parts = widths.map { String(repeating: "─", count: $0 + 2) }
        let text = left + parts.joined(separator: mid) + right
        return OutputLine(segments: [StyledSegment(text: text, color: TermColor.border)])
    }

    private static func dataLine(_ values: [String], _ widths: [Int],
                                  _ color: Color) -> OutputLine {
        var segments: [StyledSegment] = []
        segments.append(StyledSegment(text: "│", color: TermColor.border))
        for (i, val) in values.enumerated() {
            let w = i < widths.count ? widths[i] : val.count
            let padded = " " + val.padding(toLength: w, withPad: " ", startingAt: 0) + " "
            segments.append(StyledSegment(text: padded, color: color))
            segments.append(StyledSegment(text: "│", color: TermColor.border))
        }
        return OutputLine(segments: segments)
    }

    private static func coloredDataLine(_ values: [String], _ widths: [Int],
                                         _ colors: [Color]) -> OutputLine {
        var segments: [StyledSegment] = []
        segments.append(StyledSegment(text: "│", color: TermColor.border))
        for (i, val) in values.enumerated() {
            let w = i < widths.count ? widths[i] : val.count
            let display = val.count > w ? String(val.prefix(w - 1)) + "…" : val
            let padded = " " + display.padding(toLength: w, withPad: " ", startingAt: 0) + " "
            let color = i < colors.count ? colors[i] : TermColor.data
            segments.append(StyledSegment(text: padded, color: color))
            segments.append(StyledSegment(text: "│", color: TermColor.border))
        }
        return OutputLine(segments: segments)
    }
}

// MARK: - SQL Syntax Highlighter

struct SQLHighlighter {
    private static let keywords: Set<String> = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
        "GROUP", "BY", "ORDER", "ASC", "DESC", "HAVING", "LIMIT", "OFFSET",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
        "TABLE", "VIEW", "INDEX", "DROP", "ALTER", "ADD", "COLUMN",
        "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK",
        "DEFAULT", "CONSTRAINT", "EXISTS", "IF", "ELSE", "THEN", "WHEN",
        "CASE", "END", "BETWEEN", "LIKE", "ILIKE", "UNION", "ALL",
        "DISTINCT", "WITH", "RECURSIVE", "EXPLAIN", "ANALYZE", "BEGIN",
        "COMMIT", "ROLLBACK", "TRANSACTION", "GRANT", "REVOKE", "SHOW",
        "DESCRIBE", "USE", "DATABASE", "SCHEMA", "TABLES", "COLUMNS",
        "TRUE", "FALSE", "CAST", "REPLACE", "TEMP", "TEMPORARY",
        "COPY", "EXPORT", "IMPORT", "ATTACH", "DETACH", "PIVOT", "UNPIVOT",
        "INSTALL", "LOAD", "CALL", "MACRO", "SUMMARIZE", "USING",
    ]

    static func highlight(_ sql: String) -> [StyledSegment] {
        var segments: [StyledSegment] = []
        var i = sql.startIndex

        while i < sql.endIndex {
            let ch = sql[i]

            // String literal
            if ch == "'" {
                let start = i
                i = sql.index(after: i)
                while i < sql.endIndex && sql[i] != "'" {
                    i = sql.index(after: i)
                }
                if i < sql.endIndex { i = sql.index(after: i) }
                segments.append(StyledSegment(text: String(sql[start..<i]),
                                              color: TermColor.string))
                continue
            }

            // Line comment
            if ch == "-" && sql.index(after: i) < sql.endIndex && sql[sql.index(after: i)] == "-" {
                let start = i
                while i < sql.endIndex && sql[i] != "\n" {
                    i = sql.index(after: i)
                }
                segments.append(StyledSegment(text: String(sql[start..<i]),
                                              color: TermColor.comment))
                continue
            }

            // Number
            if ch.isNumber || (ch == "." && i < sql.endIndex) {
                if ch.isNumber {
                    let start = i
                    while i < sql.endIndex && (sql[i].isNumber || sql[i] == ".") {
                        i = sql.index(after: i)
                    }
                    segments.append(StyledSegment(text: String(sql[start..<i]),
                                                  color: TermColor.number))
                    continue
                }
            }

            // Word (keyword, function, or identifier)
            if ch.isLetter || ch == "_" {
                let start = i
                while i < sql.endIndex && (sql[i].isLetter || sql[i].isNumber || sql[i] == "_") {
                    i = sql.index(after: i)
                }
                let word = String(sql[start..<i])
                let upper = word.uppercased()

                // Check if followed by ( → function
                if i < sql.endIndex && sql[i] == "(" {
                    segments.append(StyledSegment(text: word, color: TermColor.function_))
                } else if keywords.contains(upper) {
                    segments.append(StyledSegment(text: word, color: TermColor.keyword))
                } else {
                    segments.append(StyledSegment(text: word, color: TermColor.data))
                }
                continue
            }

            // Everything else (operators, punctuation, whitespace)
            segments.append(StyledSegment(text: String(ch), color: TermColor.data))
            i = sql.index(after: i)
        }

        return segments
    }
}

// MARK: - Raw Text Field (no smart quotes / dashes)

/// A UITextField wrapper that disables smart punctuation. SwiftUI's TextField
/// doesn't expose smart quotes settings.
///
/// Focus is managed by the UITextField itself; pass an incrementing
/// `focusTrigger` to programmatically request focus (e.g. after a query completes).
struct RawTextField: UIViewRepresentable {
    @Binding var text: String
    @Binding var focusTrigger: Int
    var onSubmit: () -> Void

    func makeUIView(context: Context) -> UITextField {
        let field = UITextField()
        field.delegate = context.coordinator
        field.smartQuotesType = .no
        field.smartDashesType = .no
        field.smartInsertDeleteType = .no
        field.autocorrectionType = .no
        field.autocapitalizationType = .none
        field.spellCheckingType = .no
        field.font = UIFont.monospacedSystemFont(ofSize: 13, weight: .regular)
        field.textColor = .white
        field.tintColor = .white
        field.returnKeyType = .send
        field.keyboardAppearance = .dark
        field.addTarget(context.coordinator,
                        action: #selector(Coordinator.textChanged(_:)),
                        for: .editingChanged)
        return field
    }

    func updateUIView(_ uiView: UITextField, context: Context) {
        if uiView.text != text {
            uiView.text = text
        }
        if context.coordinator.lastFocusTrigger != focusTrigger {
            context.coordinator.lastFocusTrigger = focusTrigger
            DispatchQueue.main.async { uiView.becomeFirstResponder() }
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    class Coordinator: NSObject, UITextFieldDelegate {
        var parent: RawTextField
        var lastFocusTrigger: Int = 0

        init(_ parent: RawTextField) {
            self.parent = parent
        }

        @objc func textChanged(_ sender: UITextField) {
            parent.text = sender.text ?? ""
        }

        func textFieldShouldReturn(_ textField: UITextField) -> Bool {
            parent.onSubmit()
            return false
        }
    }
}

// MARK: - Query View

struct QueryView: View {
    @EnvironmentObject var serverManager: ServerManager
    @StateObject private var client = FlightSQLClient()

    @State private var inputText = ""
    @State private var outputLines: [OutputLine] = []
    @State private var history: [String] = []
    @State private var historyIndex = -1
    @State private var isExecuting = false

    @State private var inputFocusTrigger: Int = 0

    private var isRunning: Bool { serverManager.state == .running }

    var body: some View {
        VStack(spacing: 0) {
            // Output area
            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 1) {
                        ForEach(outputLines) { line in
                            HStack(spacing: 0) {
                                ForEach(Array(line.segments.enumerated()), id: \.offset) { _, seg in
                                    Text(seg.text)
                                        .foregroundStyle(seg.color)
                                }
                            }
                            .font(.system(size: 12, design: .monospaced))
                        }
                        Color.clear.frame(height: 1).id("bottom")
                    }
                    .padding(.horizontal, 8)
                    .padding(.top, 8)
                }
                .background(TermColor.background)
                .onChange(of: outputLines.count) {
                    withAnimation {
                        proxy.scrollTo("bottom", anchor: .bottom)
                    }
                }
            }

            // Input area
            HStack(spacing: 4) {
                Text("gizmosql>")
                    .font(.system(size: 13, design: .monospaced))
                    .foregroundStyle(TermColor.prompt)
                    .fontWeight(.bold)

                RawTextField(
                    text: $inputText,
                    focusTrigger: $inputFocusTrigger,
                    onSubmit: { executeCurrentInput() }
                )
                .frame(height: 24)
                .disabled(!isRunning || isExecuting)

                if isExecuting {
                    ProgressView()
                        .controlSize(.small)
                        .tint(.white)
                } else {
                    Button {
                        UIApplication.shared.sendAction(
                            #selector(UIResponder.resignFirstResponder),
                            to: nil, from: nil, for: nil)
                    } label: {
                        Image(systemName: "keyboard.chevron.compact.down")
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 10)
            .background(Color(red: 0.15, green: 0.15, blue: 0.15))
        }
        .background(TermColor.background)
        .overlay {
            if !isRunning {
                VStack(spacing: 12) {
                    Image(systemName: "terminal")
                        .font(.system(size: 40))
                        .foregroundStyle(.secondary)
                    Text("Start the server to use the SQL client")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(TermColor.background)
            }
        }
        .onChange(of: serverManager.state) { oldState, newState in
            if newState == .running {
                connectToServer()
            } else if oldState == .running {
                client.disconnect()
                appendInfo("Disconnected from server.")
            }
        }
        .onAppear {
            if isRunning && !client.isConnected {
                connectToServer()
            }
        }
    }

    // MARK: - Actions

    private func connectToServer() {
        let config = serverManager.configuration
        let certPath = CertificateGenerator.certPaths().certPath

        appendInfo("Connecting to localhost:\(config.port)...")

        if let error = client.connect(
            port: config.port,
            username: config.username,
            password: config.password,
            tlsCertPath: certPath
        ) {
            appendError("Connection failed: \(error)")
        } else {
            appendInfo("Connected.")
        }
    }

    private func executeCurrentInput() {
        let sql = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !sql.isEmpty else { return }

        // Add to history
        if history.last != sql {
            history.append(sql)
        }
        historyIndex = -1

        // Echo the input with syntax highlighting
        var echoSegments: [StyledSegment] = [
            StyledSegment(text: "gizmosql> ", color: TermColor.prompt)
        ]
        echoSegments.append(contentsOf: SQLHighlighter.highlight(sql))
        outputLines.append(OutputLine(segments: echoSegments))

        inputText = ""
        isExecuting = true

        // Execute on background to keep UI responsive
        let client = self.client
        Task.detached {
            let result = await client.execute(sql)
            await MainActor.run {
                handleResult(result)
                isExecuting = false
                inputFocusTrigger += 1
            }
        }
    }

    private func handleResult(_ result: QueryResult) {
        if let error = result.error {
            appendError(error)
            return
        }

        if result.rowsAffected >= 0 {
            let elapsed = String(format: "%.3f", result.elapsed)
            appendSuccess("\(result.rowsAffected) rows affected (\(elapsed)s)")
            return
        }

        if result.columns.isEmpty {
            let elapsed = String(format: "%.3f", result.elapsed)
            appendSuccess("OK (\(elapsed)s)")
            return
        }

        // Render box table
        let tableLines = BoxRenderer.render(result)
        outputLines.append(contentsOf: tableLines)

        // Timing line
        let elapsed = String(format: "%.3f", result.elapsed)
        outputLines.append(OutputLine(segments: [
            StyledSegment(text: "(\(elapsed)s)", color: TermColor.info)
        ]))
    }

    private func appendError(_ text: String) {
        outputLines.append(OutputLine(segments: [
            StyledSegment(text: "Error: ", color: TermColor.error),
            StyledSegment(text: text, color: TermColor.error),
        ]))
    }

    private func appendInfo(_ text: String) {
        outputLines.append(OutputLine(segments: [
            StyledSegment(text: text, color: TermColor.info)
        ]))
    }

    private func appendSuccess(_ text: String) {
        outputLines.append(OutputLine(segments: [
            StyledSegment(text: text, color: TermColor.success)
        ]))
    }
}
