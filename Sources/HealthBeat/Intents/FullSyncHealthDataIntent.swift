import AppIntents
import Foundation

struct FullSyncHealthDataIntent: AppIntent {
    static var title: LocalizedStringResource = "Full Sync Health Data"
    static var description = IntentDescription(
        "Runs a full historical backfill of all your Apple Health data to your MySQL database. This may take a long time.",
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

        await service.runFullSync(config: config)

        if let error = state.errorMessage {
            throw SyncIntentError.syncFailed(error)
        }

        let records = state.totalRecords
        return .result(dialog: "Full sync complete. \(records) records synced.")
    }
}
