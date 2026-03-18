import SwiftUI

struct ContentView: View {
    @StateObject private var syncViewModel = SyncViewModel()
    @State private var showReminderSync = false

    var body: some View {
        TabView {
            SyncDashboardView(vm: syncViewModel, showReminderSync: $showReminderSync)
                .tabItem {
                    Label("Sync", systemImage: "arrow.triangle.2.circlepath")
                }

            DataBrowserView()
                .tabItem {
                    Label("Browse", systemImage: "magnifyingglass")
                }

            SettingsView(syncViewModel: syncViewModel)
                .tabItem {
                    Label("Settings", systemImage: "gear")
                }
        }
        .onReceive(NotificationCenter.default.publisher(for: .healthBeatSyncReminderTapped)) { _ in
            showReminderSync = true
        }
    }
}
