// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import Foundation
import SwiftUI

// MARK: - Output Mode

enum OutputMode: String, CaseIterable {
    case box
    case csv
    case json

    var description: String {
        switch self {
        case .box:  return "box (bordered table, default)"
        case .csv:  return "csv (comma-separated values)"
        case .json: return "json (array of objects)"
        }
    }
}

// MARK: - Client Settings

/// Settings the in-app SQL client exposes via dot commands. Owned by
/// `QueryView` as a `@StateObject` so changes survive view rebuilds but
/// are scoped to the Query tab.
@MainActor
final class ClientSettings: ObservableObject {
    @Published var timerEnabled: Bool = true
    @Published var headersEnabled: Bool = true
    @Published var outputMode: OutputMode = .box
    @Published var lastResult: QueryResult? = nil
}

// MARK: - Renderers

/// Dispatches `QueryResult` rendering based on the current output mode.
struct ResultRenderer {
    static func render(_ result: QueryResult,
                       mode: OutputMode,
                       showHeaders: Bool) -> [OutputLine] {
        switch mode {
        case .box:
            return BoxRenderer.render(result)
        case .csv:
            return CSVRenderer.render(result, showHeaders: showHeaders)
        case .json:
            return JSONRenderer.render(result)
        }
    }
}

struct CSVRenderer {
    static func render(_ result: QueryResult, showHeaders: Bool) -> [OutputLine] {
        guard !result.columns.isEmpty else { return [] }
        var lines: [OutputLine] = []
        if showHeaders {
            let header = result.columns.map { quote($0.name) }.joined(separator: ",")
            lines.append(OutputLine(segments: [StyledSegment(text: header, color: TermColor.header)]))
        }
        for row in result.rows {
            let line = row.map { quote($0) }.joined(separator: ",")
            lines.append(OutputLine(segments: [StyledSegment(text: line, color: TermColor.data)]))
        }
        return lines
    }

    private static func quote(_ s: String) -> String {
        // RFC 4180: quote if contains comma, quote, or newline; double any inner quotes.
        if s.contains(",") || s.contains("\"") || s.contains("\n") {
            return "\"" + s.replacingOccurrences(of: "\"", with: "\"\"") + "\""
        }
        return s
    }
}

struct JSONRenderer {
    static func render(_ result: QueryResult) -> [OutputLine] {
        guard !result.columns.isEmpty else { return [] }
        var lines: [OutputLine] = []
        lines.append(OutputLine(segments: [StyledSegment(text: "[", color: TermColor.data)]))
        for (i, row) in result.rows.enumerated() {
            var entries: [String] = []
            for (j, cell) in row.enumerated() where j < result.columns.count {
                let key = escape(result.columns[j].name)
                let valueType = result.columns[j].type
                let value = formatValue(cell, type: valueType)
                entries.append("\"\(key)\": \(value)")
            }
            let comma = i < result.rows.count - 1 ? "," : ""
            let body = "  {" + entries.joined(separator: ", ") + "}" + comma
            lines.append(OutputLine(segments: [StyledSegment(text: body, color: TermColor.data)]))
        }
        lines.append(OutputLine(segments: [StyledSegment(text: "]", color: TermColor.data)]))
        return lines
    }

    private static func formatValue(_ cell: String, type: String) -> String {
        if cell == "NULL" { return "null" }
        if ["integer", "bigint", "uinteger", "ubigint", "smallint", "usmallint",
            "tinyint", "utinyint", "float", "double", "decimal"].contains(type) {
            return cell
        }
        if type == "boolean" {
            return cell == "true" ? "true" : "false"
        }
        return "\"" + escape(cell) + "\""
    }

    private static func escape(_ s: String) -> String {
        var out = ""
        for ch in s {
            switch ch {
            case "\"": out += "\\\""
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            default:   out.append(ch)
            }
        }
        return out
    }
}

// MARK: - Dot Command Processor

/// Parses and executes `.command` input inside the iOS SQL client. Commands
/// that fetch data from the server issue regular SQL through the existing
/// Flight SQL client — no new C bridge calls.
///
/// The whole type is `@MainActor` so handlers can call MainActor-isolated
/// APIs (`FlightSQLClient`, `ClientSettings`, `ServerManager`) directly
/// without per-call `await`s.
@MainActor
struct DotCommandProcessor {

    nonisolated static func isDotCommand(_ input: String) -> Bool {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.hasPrefix(".")
    }

    /// Execute a dot command. Returns the output lines to append. Any state
    /// changes are applied directly to `settings`.
    static func execute(_ input: String,
                        client: FlightSQLClient,
                        settings: ClientSettings,
                        serverManager: ServerManager) async -> [OutputLine] {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        let parts = trimmed.split(separator: " ", maxSplits: 1,
                                  omittingEmptySubsequences: true).map(String.init)
        let command = parts.first.map { $0.lowercased() } ?? ""
        let args = parts.count > 1
            ? parts[1].trimmingCharacters(in: .whitespacesAndNewlines) : ""

        switch command {
        case ".help":   return handleHelp(pattern: args)
        case ".about":  return handleAbout()
        case ".timer":  return handleToggle(name: "timer", arg: args,
                                            get: { settings.timerEnabled },
                                            set: { settings.timerEnabled = $0 })
        case ".headers": return handleToggle(name: "headers", arg: args,
                                             get: { settings.headersEnabled },
                                             set: { settings.headersEnabled = $0 })
        case ".mode":   return handleMode(arg: args, settings: settings)
        case ".last":   return handleLast(settings: settings)
        case ".describe": return await handleDescribe(arg: args, client: client,
                                                      settings: settings)
        case ".tables": return await handleTables(arg: args, client: client,
                                                  settings: settings)
        case ".catalogs": return await handleCatalogs(client: client, settings: settings)
        case ".show":   return await handleShow(client: client,
                                                settings: settings,
                                                serverManager: serverManager)
        default:
            return [errorLine("Unknown command: \(command). Type .help for a list.")]
        }
    }

    // MARK: - .help

    private struct HelpEntry {
        let command: String
        let description: String
    }

    private static let helpEntries: [HelpEntry] = [
        HelpEntry(command: ".about",          description: "Show client version and copyright"),
        HelpEntry(command: ".catalogs",       description: "List catalogs available on the server"),
        HelpEntry(command: ".describe TABLE", description: "Show column names and types for TABLE"),
        HelpEntry(command: ".headers on|off", description: "Toggle column headers in output"),
        HelpEntry(command: ".help [PATTERN]", description: "Show this help (filter by PATTERN)"),
        HelpEntry(command: ".last",           description: "Re-display the most recent query result"),
        HelpEntry(command: ".mode [MODE]",    description: "Output mode: box, csv, or json"),
        HelpEntry(command: ".show",           description: "Show server, session, and client settings"),
        HelpEntry(command: ".tables [PAT]",   description: "List tables (optional LIKE pattern)"),
        HelpEntry(command: ".timer on|off",   description: "Toggle query execution time display"),
    ]

    private static func handleHelp(pattern: String) -> [OutputLine] {
        let filter = pattern.lowercased()
        let matches = filter.isEmpty
            ? helpEntries
            : helpEntries.filter {
                $0.command.lowercased().contains(filter) ||
                $0.description.lowercased().contains(filter)
            }
        if matches.isEmpty {
            return [infoLine("No commands matched \"\(pattern)\".")]
        }
        let widest = matches.map { $0.command.count }.max() ?? 0
        var lines: [OutputLine] = []
        for entry in matches {
            let padded = entry.command.padding(toLength: widest, withPad: " ", startingAt: 0)
            lines.append(OutputLine(segments: [
                StyledSegment(text: "  " + padded + "  ", color: TermColor.keyword),
                StyledSegment(text: entry.description, color: TermColor.info),
            ]))
        }
        return lines
    }

    // MARK: - .about

    private static func handleAbout() -> [OutputLine] {
        return [
            infoLine("GizmoSQL iOS \(AppInfo.versionDisplay)"),
            infoLine("Copyright © 2025 GizmoData LLC"),
            infoLine("Apache License 2.0 — https://www.apache.org/licenses/LICENSE-2.0"),
            infoLine("https://github.com/gizmodata/gizmosql"),
        ]
    }

    // MARK: - .timer / .headers toggles

    private static func handleToggle(name: String, arg: String,
                                     get: () -> Bool,
                                     set: (Bool) -> Void) -> [OutputLine] {
        let a = arg.lowercased()
        let newValue: Bool
        if a.isEmpty { newValue = !get() }
        else if ["on", "true", "1", "yes"].contains(a) { newValue = true }
        else if ["off", "false", "0", "no"].contains(a) { newValue = false }
        else { return [errorLine("Usage: .\(name) on|off")] }
        set(newValue)
        return [infoLine("\(name): \(newValue ? "on" : "off")")]
    }

    // MARK: - .mode

    private static func handleMode(arg: String, settings: ClientSettings) -> [OutputLine] {
        if arg.isEmpty {
            var lines: [OutputLine] = [infoLine("Current mode: \(settings.outputMode.rawValue)")]
            for m in OutputMode.allCases {
                let marker = m == settings.outputMode ? "* " : "  "
                lines.append(OutputLine(segments: [
                    StyledSegment(text: marker + m.description, color: TermColor.info),
                ]))
            }
            return lines
        }
        guard let mode = OutputMode(rawValue: arg.lowercased()) else {
            return [errorLine("Unknown mode: \(arg). Valid: box, csv, json.")]
        }
        settings.outputMode = mode
        return [infoLine("mode: \(mode.rawValue)")]
    }

    // MARK: - .last

    private static func handleLast(settings: ClientSettings) -> [OutputLine] {
        guard let result = settings.lastResult else {
            return [infoLine("No previous result.")]
        }
        return ResultRenderer.render(result,
                                     mode: settings.outputMode,
                                     showHeaders: settings.headersEnabled)
    }

    // MARK: - .describe

    private static func handleDescribe(arg: String,
                                       client: FlightSQLClient,
                                       settings: ClientSettings) async -> [OutputLine] {
        guard !arg.isEmpty else { return [errorLine("Usage: .describe TABLE")] }
        // Let DuckDB handle quoting — users can pass qualified names like schema.table
        let sql = "DESCRIBE \(arg)"
        let result = client.execute(sql)
        if let err = result.error { return [errorLine(err)] }
        settings.lastResult = result
        return ResultRenderer.render(result,
                                     mode: settings.outputMode,
                                     showHeaders: settings.headersEnabled)
    }

    // MARK: - .tables

    private static func handleTables(arg: String,
                                     client: FlightSQLClient,
                                     settings: ClientSettings) async -> [OutputLine] {
        // Hide internal schemas AND the GizmoSQL system catalog. The system
        // catalog hosts server-managed metadata helper views (gizmosql_index_info,
        // gizmosql_view_definition) that are not user data, so .tables shouldn't
        // surface them — same rationale as hiding information_schema.
        var where_ = "table_schema NOT IN ('information_schema', 'pg_catalog')"
                   + " AND table_catalog != '\(GizmoSQLConstants.systemCatalogName)'"
        if !arg.isEmpty {
            // Users type a simple LIKE pattern (e.g. `cust%`)
            let escaped = arg.replacingOccurrences(of: "'", with: "''")
            where_ += " AND table_name ILIKE '\(escaped)'"
        }
        let sql = """
        SELECT table_catalog AS catalog,
               table_schema  AS schema,
               table_name    AS name,
               table_type    AS type
        FROM information_schema.tables
        WHERE \(where_)
        ORDER BY catalog, schema, name
        """
        let result = client.execute(sql)
        if let err = result.error { return [errorLine(err)] }
        settings.lastResult = result
        if result.rows.isEmpty {
            return [infoLine("No tables found.")]
        }
        return ResultRenderer.render(result,
                                     mode: settings.outputMode,
                                     showHeaders: settings.headersEnabled)
    }

    // MARK: - .catalogs

    private static func handleCatalogs(client: FlightSQLClient,
                                       settings: ClientSettings) async -> [OutputLine] {
        // Exclude the GizmoSQL system catalog — it's a server-managed in-memory
        // catalog hosting metadata helper views, not user data.
        let sql = """
        SELECT DISTINCT table_catalog AS catalog
        FROM information_schema.tables
        WHERE table_catalog != '\(GizmoSQLConstants.systemCatalogName)'
        ORDER BY catalog
        """
        let result = client.execute(sql)
        if let err = result.error { return [errorLine(err)] }
        settings.lastResult = result
        if result.rows.isEmpty {
            return [infoLine("No catalogs found.")]
        }
        return ResultRenderer.render(result,
                                     mode: settings.outputMode,
                                     showHeaders: settings.headersEnabled)
    }

    // MARK: - .show

    private static func handleShow(client: FlightSQLClient,
                                   settings: ClientSettings,
                                   serverManager: ServerManager) async -> [OutputLine] {
        // Pull server + session info via SQL functions. Same approach as the
        // desktop client's `.show` (command_processor.cpp around line 1051).
        let sql = """
        SELECT GIZMOSQL_VERSION()          AS server_version,
               GIZMOSQL_EDITION()          AS edition,
               GIZMOSQL_CURRENT_INSTANCE() AS instance_id,
               GIZMOSQL_CURRENT_SESSION()  AS session_id,
               GIZMOSQL_USER()             AS "user",
               GIZMOSQL_ROLE()             AS role,
               CURRENT_CATALOG()           AS catalog,
               CURRENT_SCHEMA()            AS "schema"
        """
        let result = client.execute(sql)
        if let err = result.error {
            return [errorLine("Failed to fetch server info: \(err)")]
        }

        // Pull column values from the single-row result.
        let row = result.rows.first ?? []
        func col(_ name: String) -> String {
            guard let idx = result.columns.firstIndex(where: { $0.name == name }) else {
                return "(unknown)"
            }
            return idx < row.count ? row[idx] : "(unknown)"
        }

        let config = serverManager.configuration
        var lines: [OutputLine] = []

        lines.append(sectionHeader("Client"))
        lines.append(kvLine("version", "iOS app"))
        lines.append(kvLine("mode", settings.outputMode.rawValue))
        lines.append(kvLine("timer", settings.timerEnabled ? "on" : "off"))
        lines.append(kvLine("headers", settings.headersEnabled ? "on" : "off"))

        lines.append(sectionHeader("Server"))
        lines.append(kvLine("version", col("server_version")))
        lines.append(kvLine("edition", col("edition")))
        lines.append(kvLine("instance_id", col("instance_id")))
        lines.append(kvLine("state", serverManager.state.rawValue))
        lines.append(kvLine("active sessions", String(serverManager.activeSessionCount)))

        lines.append(sectionHeader("Session"))
        lines.append(kvLine("session_id", col("session_id")))
        lines.append(kvLine("user", col("user")))
        lines.append(kvLine("role", col("role")))
        lines.append(kvLine("catalog", col("catalog")))
        lines.append(kvLine("schema", col("schema")))
        lines.append(kvLine("host", "localhost"))
        lines.append(kvLine("port", String(config.port)))
        lines.append(kvLine("tls", "on (self-signed)"))

        return lines
    }

    // MARK: - Output helpers

    private static func infoLine(_ text: String) -> OutputLine {
        OutputLine(segments: [StyledSegment(text: text, color: TermColor.info)])
    }

    private static func errorLine(_ text: String) -> OutputLine {
        OutputLine(segments: [
            StyledSegment(text: "Error: ", color: TermColor.error),
            StyledSegment(text: text, color: TermColor.error),
        ])
    }

    private static func sectionHeader(_ text: String) -> OutputLine {
        OutputLine(segments: [
            StyledSegment(text: "--- \(text) ---", color: TermColor.header),
        ])
    }

    private static func kvLine(_ key: String, _ value: String) -> OutputLine {
        let padded = key.padding(toLength: 16, withPad: " ", startingAt: 0)
        return OutputLine(segments: [
            StyledSegment(text: "  " + padded + ": ", color: TermColor.info),
            StyledSegment(text: value, color: TermColor.data),
        ])
    }
}
