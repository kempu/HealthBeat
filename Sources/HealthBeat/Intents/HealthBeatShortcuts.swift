import AppIntents

enum SyncIntentError: Swift.Error, CustomLocalizedStringResourceConvertible {
    case syncFailed(String)

    var localizedStringResource: LocalizedStringResource {
        switch self {
        case .syncFailed(let message):
            return "Sync failed: \(message)"
        }
    }
}

struct HealthBeatShortcuts: AppShortcutsProvider {
    static var appShortcuts: [AppShortcut] {
        AppShortcut(
            intent: SyncHealthDataIntent(),
            phrases: [
                "Sync health data with \(.applicationName)",
                "Sync \(.applicationName)",
                "Run \(.applicationName) sync"
            ],
            shortTitle: "Sync Health Data",
            systemImageName: "arrow.triangle.2.circlepath"
        )
        AppShortcut(
            intent: FullSyncHealthDataIntent(),
            phrases: [
                "Full sync with \(.applicationName)",
                "Run full \(.applicationName) sync",
                "Backfill health data with \(.applicationName)"
            ],
            shortTitle: "Full Sync Health Data",
            systemImageName: "arrow.triangle.2.circlepath.circle.fill"
        )
    }
}
