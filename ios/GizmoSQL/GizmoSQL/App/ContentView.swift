// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI

struct ContentView: View {
    @EnvironmentObject var serverManager: ServerManager

    var body: some View {
        TabView {
            ServerStatusView()
                .tabItem {
                    Label("Server", systemImage: "server.rack")
                }

            QueryView()
                .tabItem {
                    Label("Query", systemImage: "terminal")
                }

            BrowserView()
                .tabItem {
                    Label("Browser", systemImage: "cylinder.split.1x2")
                }

            LogView()
                .tabItem {
                    Label("Logs", systemImage: "text.justify.left")
                }

            SettingsView()
                .tabItem {
                    Label("Settings", systemImage: "gear")
                }
        }
    }
}
