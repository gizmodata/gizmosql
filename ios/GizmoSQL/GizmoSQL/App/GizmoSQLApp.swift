// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

import SwiftUI
import UIKit

@main
struct GizmoSQLApp: App {
    @StateObject private var serverManager = ServerManager()
    @Environment(\.scenePhase) private var scenePhase
    @State private var showSplash = true

    var body: some Scene {
        WindowGroup {
            ZStack {
                ContentView()
                    .environmentObject(serverManager)

                if showSplash {
                    SplashView()
                        .transition(.opacity)
                        .zIndex(1)
                }
            }
            .onAppear {
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
                    withAnimation(.easeOut(duration: 0.4)) {
                        showSplash = false
                    }
                }
            }
        }
        .onChange(of: scenePhase) { _, newPhase in
            switch newPhase {
            case .active:
                serverManager.didBecomeActive()
            case .background:
                serverManager.didEnterBackground()
            default:
                break
            }
        }
    }
}

struct SplashView: View {
    var body: some View {
        ZStack {
            Color(.systemBackground)
                .ignoresSafeArea()

            Image("GizmoSQLLogo")
                .resizable()
                .scaledToFit()
                .frame(maxWidth: 280)
        }
    }
}
