// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI

// MARK: - Data Models

struct CatalogItem: Identifiable, Hashable {
    let id = UUID()
    let name: String
    let type: String  // duckdb, postgres, sqlite, ducklake, etc.
}

struct SchemaItem: Identifiable, Hashable {
    let id = UUID()
    let catalog: String
    let name: String
}

struct TableItem: Identifiable, Hashable {
    let id = UUID()
    let catalog: String
    let schema: String
    let name: String
    let type: String  // TABLE, VIEW, etc.
}

struct ColumnItem: Identifiable {
    let id = UUID()
    let name: String
    let dataType: String
    let isNullable: Bool
    let ordinal: Int
}

// MARK: - Browser View Model

@MainActor
class BrowserViewModel: ObservableObject {
    @Published var catalogs: [CatalogItem] = []
    @Published var schemas: [SchemaItem] = []
    @Published var tables: [TableItem] = []
    @Published var columns: [ColumnItem] = []
    @Published var previewResult: QueryResult?
    @Published var isLoading = false
    @Published var errorMessage: String?

    private var client: FlightSQLClient?

    func setClient(_ client: FlightSQLClient) {
        self.client = client
    }

    func loadCatalogs() {
        guard let client, client.isConnected else { return }
        isLoading = true
        errorMessage = nil
        Task.detached {
            // duckdb_databases() returns: database_name, database_oid, path,
            // comment, tags, type, readonly, internal, ...
            // Hide internal catalogs (system_catalog, temp, etc.).
            let result = await client.execute(
                "SELECT database_name, type FROM duckdb_databases() " +
                "WHERE internal = false ORDER BY database_name",
                maxRows: 1000)
            await MainActor.run {
                self.isLoading = false
                if let error = result.error {
                    self.errorMessage = error
                    return
                }
                self.catalogs = result.rows.map {
                    CatalogItem(name: $0[0], type: $0[1])
                }
            }
        }
    }

    func loadSchemas(catalog: String) {
        guard let client, client.isConnected else { return }
        isLoading = true
        errorMessage = nil
        // Clear stale data from a previously-viewed catalog
        schemas = []
        tables = []
        columns = []
        let sql = """
            SELECT DISTINCT table_catalog, table_schema
            FROM information_schema.tables
            WHERE table_catalog = '\(catalog.replacingOccurrences(of: "'", with: "''"))'
            ORDER BY table_schema
            """
        Task.detached {
            let result = await client.execute(sql, maxRows: 1000)
            await MainActor.run {
                self.isLoading = false
                if let error = result.error {
                    self.errorMessage = error
                    return
                }
                self.schemas = result.rows.map {
                    SchemaItem(catalog: $0[0], name: $0[1])
                }
            }
        }
    }

    func loadTables(catalog: String, schema: String) {
        guard let client, client.isConnected else { return }
        isLoading = true
        errorMessage = nil
        // Clear stale data from a previously-viewed schema
        tables = []
        columns = []
        let escapedCatalog = catalog.replacingOccurrences(of: "'", with: "''")
        let escapedSchema = schema.replacingOccurrences(of: "'", with: "''")
        let sql = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog = '\(escapedCatalog)'
              AND table_schema = '\(escapedSchema)'
            ORDER BY table_type, table_name
            """
        Task.detached {
            let result = await client.execute(sql, maxRows: 10000)
            await MainActor.run {
                self.isLoading = false
                if let error = result.error {
                    self.errorMessage = error
                    return
                }
                self.tables = result.rows.map {
                    TableItem(catalog: catalog, schema: schema,
                              name: $0[0], type: $0[1])
                }
            }
        }
    }

    func loadColumns(catalog: String, schema: String, table: String) {
        guard let client, client.isConnected else { return }
        isLoading = true
        errorMessage = nil
        // Clear stale data from a previously-viewed table
        columns = []
        let escapedSchema = schema.replacingOccurrences(of: "\"", with: "\"\"")
        let escapedTable = table.replacingOccurrences(of: "\"", with: "\"\"")
        let sql = "DESCRIBE \"\(escapedSchema)\".\"\(escapedTable)\""
        Task.detached {
            // DESCRIBE returns: column_name, column_type, null, key, default, extra
            let result = await client.execute(sql, maxRows: 10000)
            await MainActor.run {
                self.isLoading = false
                if let error = result.error {
                    self.errorMessage = error
                    return
                }
                self.columns = result.rows.enumerated().map { (idx, row) in
                    ColumnItem(
                        name: row[0],
                        dataType: row[1],
                        isNullable: row[2] == "YES",
                        ordinal: idx + 1
                    )
                }
            }
        }
    }

    func loadPreview(catalog: String, schema: String, table: String) {
        guard let client, client.isConnected else { return }
        isLoading = true
        previewResult = nil
        let escapedSchema = schema.replacingOccurrences(of: "\"", with: "\"\"")
        let escapedTable = table.replacingOccurrences(of: "\"", with: "\"\"")
        let sql = "SELECT * FROM \"\(escapedSchema)\".\"\(escapedTable)\" LIMIT 50"
        Task.detached {
            let result = await client.execute(sql, maxRows: 50)
            await MainActor.run {
                self.isLoading = false
                self.previewResult = result
            }
        }
    }

    func dropTable(catalog: String, schema: String, table: String,
                   type: String, completion: @escaping (Bool) -> Void) {
        guard let client, client.isConnected else {
            completion(false)
            return
        }
        let escapedSchema = schema.replacingOccurrences(of: "\"", with: "\"\"")
        let escapedTable = table.replacingOccurrences(of: "\"", with: "\"\"")
        let kind = type == "VIEW" ? "VIEW" : "TABLE"
        let sql = "DROP \(kind) \"\(escapedSchema)\".\"\(escapedTable)\""
        Task.detached {
            let result = await client.execute(sql, maxRows: 0)
            await MainActor.run {
                if result.error != nil {
                    self.errorMessage = result.error
                    completion(false)
                } else {
                    // Remove from local list
                    self.tables.removeAll { $0.schema == schema && $0.name == table }
                    completion(true)
                }
            }
        }
    }
}

// MARK: - Browser View (Root)

struct BrowserView: View {
    @EnvironmentObject var serverManager: ServerManager
    @StateObject private var client = FlightSQLClient()
    @StateObject private var viewModel = BrowserViewModel()

    private var isRunning: Bool { serverManager.state == .running }

    var body: some View {
        NavigationStack {
            Group {
                if !isRunning {
                    notRunningView
                } else if viewModel.catalogs.isEmpty && viewModel.isLoading {
                    ProgressView("Loading...")
                } else if viewModel.catalogs.isEmpty {
                    emptyView
                } else {
                    catalogList
                }
            }
            .navigationTitle("Browser")
            .toolbar {
                if isRunning {
                    ToolbarItem(placement: .topBarTrailing) {
                        Button {
                            refresh()
                        } label: {
                            Image(systemName: "arrow.clockwise")
                        }
                    }
                }
            }
        }
        .onChange(of: serverManager.state) { oldState, newState in
            if newState == .running {
                connectAndLoad()
            } else if oldState == .running {
                client.disconnect()
                viewModel.catalogs = []
            }
        }
        .onAppear {
            if isRunning && !client.isConnected {
                connectAndLoad()
            }
        }
    }

    private var notRunningView: some View {
        VStack(spacing: 12) {
            Image(systemName: "cylinder.split.1x2")
                .font(.system(size: 40))
                .foregroundStyle(.secondary)
            Text("Start the server to browse data")
                .font(.subheadline)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var emptyView: some View {
        VStack(spacing: 12) {
            if let error = viewModel.errorMessage {
                Image(systemName: "exclamationmark.triangle")
                    .font(.system(size: 40))
                    .foregroundStyle(.orange)
                Text(error)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            } else {
                Image(systemName: "tray")
                    .font(.system(size: 40))
                    .foregroundStyle(.secondary)
                Text("No catalogs found")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
            Button("Refresh") { refresh() }
                .buttonStyle(.bordered)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var catalogList: some View {
        List(viewModel.catalogs) { catalog in
            NavigationLink(value: catalog) {
                HStack {
                    Image(systemName: catalogIcon(for: catalog.type))
                        .foregroundStyle(catalogIconColor(for: catalog.type))
                        .frame(width: 24)
                    Text(catalog.name)
                    Spacer()
                    Text(catalog.type)
                        .font(.caption.monospaced())
                        .foregroundStyle(.secondary)
                        .padding(.horizontal, 6)
                        .padding(.vertical, 2)
                        .background(Color.secondary.opacity(0.15))
                        .clipShape(RoundedRectangle(cornerRadius: 4))
                }
            }
        }
        .refreshable { viewModel.loadCatalogs() }
        .navigationDestination(for: CatalogItem.self) { catalog in
            SchemaListView(catalog: catalog, client: client, viewModel: viewModel)
        }
    }

    private func catalogIcon(for type: String) -> String {
        switch type.lowercased() {
        case "ducklake": return "drop.fill"
        case "postgres": return "leaf.fill"
        case "sqlite": return "tray.full"
        case "duckdb": return "cylinder"
        default: return "cylinder"
        }
    }

    private func catalogIconColor(for type: String) -> Color {
        switch type.lowercased() {
        case "ducklake": return .blue
        case "postgres": return .indigo
        case "sqlite": return .orange
        case "duckdb": return .yellow
        default: return .secondary
        }
    }

    private func connectAndLoad() {
        let config = serverManager.configuration
        let certPath = CertificateGenerator.certPaths().certPath
        _ = client.connect(
            port: config.port,
            username: config.username,
            password: config.password,
            tlsCertPath: certPath
        )
        viewModel.setClient(client)
        viewModel.loadCatalogs()
    }

    private func refresh() {
        if !client.isConnected {
            connectAndLoad()
        } else {
            viewModel.loadCatalogs()
        }
    }
}

// MARK: - Schema List

struct SchemaListView: View {
    let catalog: CatalogItem
    let client: FlightSQLClient
    @ObservedObject var viewModel: BrowserViewModel

    var body: some View {
        Group {
            if viewModel.isLoading && viewModel.schemas.isEmpty {
                ProgressView("Loading...")
            } else {
                List(viewModel.schemas) { schema in
                    NavigationLink(value: schema) {
                        Label(schema.name, systemImage: "folder")
                    }
                }
                .refreshable { viewModel.loadSchemas(catalog: catalog.name) }
            }
        }
        .navigationTitle(catalog.name)
        .navigationDestination(for: SchemaItem.self) { schema in
            TableListView(schema: schema, client: client, viewModel: viewModel)
        }
        .onAppear {
            viewModel.loadSchemas(catalog: catalog.name)
        }
    }
}

// MARK: - Table List

struct TableListView: View {
    let schema: SchemaItem
    let client: FlightSQLClient
    @ObservedObject var viewModel: BrowserViewModel

    var body: some View {
        Group {
            if viewModel.isLoading && viewModel.tables.isEmpty {
                ProgressView("Loading...")
            } else if viewModel.tables.isEmpty {
                VStack(spacing: 8) {
                    Image(systemName: "tray")
                        .font(.title)
                        .foregroundStyle(.secondary)
                    Text("No tables")
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(viewModel.tables) { table in
                    NavigationLink(value: table) {
                        HStack {
                            Image(systemName: table.type == "VIEW" ? "eye" : "tablecells")
                                .foregroundStyle(table.type == "VIEW" ? .blue : .primary)
                                .frame(width: 24)
                            Text(table.name)
                            Spacer()
                            Text(table.type)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                .refreshable {
                    viewModel.loadTables(catalog: schema.catalog, schema: schema.name)
                }
            }
        }
        .navigationTitle(schema.name)
        .navigationDestination(for: TableItem.self) { table in
            TableDetailView(table: table, client: client, viewModel: viewModel)
        }
        .onAppear {
            viewModel.loadTables(catalog: schema.catalog, schema: schema.name)
        }
    }
}

// MARK: - Table Detail (Columns + Preview)

struct TableDetailView: View {
    let table: TableItem
    let client: FlightSQLClient
    @ObservedObject var viewModel: BrowserViewModel
    @Environment(\.dismiss) private var dismiss
    @State private var showPreview = false
    @State private var showDropConfirmation = false

    private var dropLabel: String {
        table.type == "VIEW" ? "Drop View" : "Drop Table"
    }

    var body: some View {
        List {
            Section("Columns") {
                if viewModel.isLoading && viewModel.columns.isEmpty {
                    ProgressView()
                } else {
                    ForEach(viewModel.columns) { column in
                        HStack {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(column.name)
                                    .font(.body.monospaced())
                                Text(column.dataType)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                            if !column.isNullable {
                                Text("NOT NULL")
                                    .font(.caption2)
                                    .foregroundStyle(.blue)
                                    .padding(.horizontal, 6)
                                    .padding(.vertical, 2)
                                    .background(.blue.opacity(0.15))
                                    .clipShape(RoundedRectangle(cornerRadius: 4))
                            }
                        }
                    }
                }
            }

            Section {
                Button {
                    showPreview = true
                    viewModel.loadPreview(
                        catalog: table.catalog,
                        schema: table.schema,
                        table: table.name
                    )
                } label: {
                    Label("Preview Data", systemImage: "eye")
                }
            }

            Section {
                Button(role: .destructive) {
                    showDropConfirmation = true
                } label: {
                    Label(dropLabel, systemImage: "trash")
                }
            }
        }
        .navigationTitle(table.name)
        .sheet(isPresented: $showPreview) {
            PreviewSheet(tableName: table.name, viewModel: viewModel)
        }
        .confirmationDialog(
            dropLabel,
            isPresented: $showDropConfirmation,
            titleVisibility: .visible
        ) {
            Button(dropLabel, role: .destructive) {
                viewModel.dropTable(
                    catalog: table.catalog,
                    schema: table.schema,
                    table: table.name,
                    type: table.type
                ) { success in
                    if success { dismiss() }
                }
            }
        } message: {
            Text("Are you sure you want to drop \"\(table.name)\"? This cannot be undone.")
        }
        .onAppear {
            viewModel.loadColumns(
                catalog: table.catalog,
                schema: table.schema,
                table: table.name
            )
        }
    }
}

// MARK: - Data Preview Sheet

struct PreviewSheet: View {
    let tableName: String
    @ObservedObject var viewModel: BrowserViewModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Group {
                if viewModel.isLoading {
                    ProgressView("Loading preview...")
                } else if let result = viewModel.previewResult {
                    if let error = result.error {
                        VStack(spacing: 12) {
                            Image(systemName: "exclamationmark.triangle")
                                .font(.title)
                                .foregroundStyle(.red)
                            Text(error)
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                        }
                    } else {
                        previewTable(result)
                    }
                }
            }
            .navigationTitle("\(tableName) preview")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }

    private func previewTable(_ result: QueryResult) -> some View {
        ScrollView([.horizontal, .vertical]) {
            LazyVStack(alignment: .leading, spacing: 0) {
                // Header row
                HStack(spacing: 0) {
                    ForEach(Array(result.columns.enumerated()), id: \.offset) { _, col in
                        VStack(alignment: .leading, spacing: 2) {
                            Text(col.name)
                                .font(.caption.bold().monospaced())
                            Text(col.type)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        .frame(minWidth: 100, alignment: .leading)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 6)
                    }
                }
                .background(Color(.systemGray5))

                Divider()

                // Data rows
                ForEach(Array(result.rows.enumerated()), id: \.offset) { rowIdx, row in
                    HStack(spacing: 0) {
                        ForEach(Array(row.enumerated()), id: \.offset) { _, cell in
                            Text(cell)
                                .font(.caption.monospaced())
                                .foregroundStyle(cell == "NULL" ? .secondary : .primary)
                                .frame(minWidth: 100, alignment: .leading)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 4)
                                .lineLimit(1)
                        }
                    }
                    .background(rowIdx % 2 == 0 ? Color.clear : Color(.systemGray6))
                }
            }
        }
    }
}
