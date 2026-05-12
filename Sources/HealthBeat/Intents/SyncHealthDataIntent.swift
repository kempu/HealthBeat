import AppIntents
import Foundation

struct SyncHealthDataIntent: AppIntent {
    static var title: LocalizedStringResource = "Sync Health Data"
    static var description = IntentDescription(
        "Runs an incremental sync of your Apple Health data to your MySQL database.",
        categoryName: "Sync"
    )

    static var openAppWhenRun: Bool = false

    @MainActor
    func perform() async throws -> some IntentResult & ProvidesDialog {
        guard !SyncService.isSyncEffectivelyRunning() else {
            return .result(dialog: "A sync is already in progress.")
        }

        let config = MySQLConfig.load()
        let state = SyncState()
        let service = SyncService(syncState: state)
        service.isBackgroundSync = true
        service.suppressLiveActivity = true
        service.attachEAIfConfigured()

        await service.runIncrementalSync(config: config)

        if let error = state.errorMessage {
            throw SyncIntentError.syncFailed(error)
        }

        let records = state.totalRecords
        return .result(dialog: "Incremental sync complete. \(records) records synced.")
    }
}
