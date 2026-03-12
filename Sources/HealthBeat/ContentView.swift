import SwiftUI

struct ContentView: View {
    @StateObject private var syncViewModel = SyncViewModel()

    var body: some View {
        TabView {
            SyncDashboardView(vm: syncViewModel)
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
    }
}
