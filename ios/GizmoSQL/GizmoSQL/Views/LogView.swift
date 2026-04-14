// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI

struct LogView: View {
    @EnvironmentObject var serverManager: ServerManager
    @State private var autoScroll = true

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                if serverManager.logLines.isEmpty {
                    ContentUnavailableView(
                        "No Logs",
                        systemImage: "text.justify.left",
                        description: Text("Start the server to see log output.")
                    )
                } else {
                    ScrollViewReader { proxy in
                        ScrollView {
                            LazyVStack(alignment: .leading, spacing: 2) {
                                ForEach(Array(serverManager.logLines.enumerated()), id: \.offset) { index, line in
                                    Text(line)
                                        .font(.system(.caption2, design: .monospaced))
                                        .foregroundStyle(logColor(for: line))
                                        .textSelection(.enabled)
                                        .id(index)
                                }
                            }
                            .padding(.horizontal, 8)
                            .padding(.vertical, 4)
                        }
                        .onChange(of: serverManager.logLines.count) {
                            if autoScroll, let last = serverManager.logLines.indices.last {
                                withAnimation {
                                    proxy.scrollTo(last, anchor: .bottom)
                                }
                            }
                        }
                    }
                }
            }
            .navigationTitle("Logs")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Toggle(isOn: $autoScroll) {
                        Image(systemName: autoScroll ? "arrow.down.to.line" : "arrow.down.to.line.compact")
                    }
                }

                ToolbarItem(placement: .topBarLeading) {
                    Text("\(serverManager.logLines.count) lines")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    private func logColor(for line: String) -> Color {
        if line.contains("ERROR") || line.contains("FATAL") { return .red }
        if line.contains("WARN") { return .orange }
        if line.contains("DEBUG") { return .secondary }
        return .primary
    }
}
