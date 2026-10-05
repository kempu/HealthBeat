import Combine
import Foundation
import HealthKit
import SwiftUI
import UIKit

@MainActor
final class SyncViewModel: ObservableObject {

    private(set) var syncState: SyncState
    private let syncService: SyncService
    private var cancellables = Set<AnyCancellable>()
    private var syncTask: Task<Void, Never>?
    private var eaImportPollTask: Task<Void, Never>?

    @Published var prerequisiteIssues: [SyncPrerequisiteIssue] = []
    @Published var showPrerequisiteAlert = false
    @Published var isReminderSync = false

    init() {
        let state = SyncState()
        self.syncState = state
        self.syncService = SyncService(syncState: state)
        self.syncService.attachEAIfConfigured()
        // Forward SyncState changes so SwiftUI views subscribed to this
        // view model re-render whenever any SyncState @Published property changes.
        state.objectWillChange
            .sink { [weak self] in self?.objectWillChange.send() }
            .store(in: &cancellables)

        // MySQL database wiped from Settings → clear ONLY the MySQL baseline so
        // the dashboard prompts a MySQL full sync; EA keeps syncing incrementally.
        NotificationCenter.default.publisher(for: .healthBeatDatabaseDidReset)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in self?.syncState.handleMySQLReset() }
            .store(in: &cancellables)

        // "Re-sync EA from scratch" from EA settings → drop the EA baseline; the
        // dashboard then prompts an EA full sync (which truncates + replaces EA).
        NotificationCenter.default.publisher(for: .healthBeatEAResetRequested)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.syncState.clearEABaseline()
                self?.syncState.persist()
            }
            .store(in: &cancellables)

        // Reload persisted sync state when the app returns to the foreground so
        // syncs that ran out-of-VM (Siri shortcuts, background tasks, observer
        // queries) become visible in the dashboard. Skip while a sync is active
        // in this VM to avoid clobbering live progress.
        NotificationCenter.default.publisher(for: UIApplication.willEnterForegroundNotification)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                guard let self else { return }
                guard !self.syncState.isAnySyncRunning else { return }
                // A full sync mid-delivery (EA still uploading in the background)
                // keeps accurate in-memory state — restoring/cleaning over it would
                // wipe the live progress to 0%. Just refresh the EA upload progress
                // from the uploader's (background-updated) counter instead.
                if self.syncState.fullSyncPhase != .idle {
                    self.refreshEAUploadProgress()
                    return
                }
                self.syncState.restore()
                self.cleanForFullSyncIfNeeded()
            }
            .store(in: &cancellables)

        // EA dump-upload progress (background) feeds the full-sync deliver bar.
        NotificationCenter.default.publisher(for: .eaDumpUploadProgress)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] note in
                guard let self, self.syncState.fullSyncPhase == .delivering,
                      let frac = note.userInfo?["fraction"] as? Double else { return }
                self.syncState.updateFullSyncProgress(ea: frac)
            }
            .store(in: &cancellables)

        // EA files + key delivered → the server now decrypts and imports them in
        // async jobs. EA is NOT baselined yet; poll `bulkImportStatus` until those
        // jobs report `completed` (then mark the baseline) or `failed`.
        NotificationCenter.default.publisher(for: .eaDumpUploadKeyDelivered)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] note in
                guard let self else { return }
                let runID = (note.object as? String)
                    ?? UserDefaults.standard.string(forKey: "pendingEAImportRunID")
                if let runID { self.startEAImportPolling(runID: runID) }
            }
            .store(in: &cancellables)

        NotificationCenter.default.publisher(for: .eaDumpUploadFailed)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] note in
                guard let self else { return }
                self.syncState.markEAFailed()
                let detail = (note.userInfo?["reason"] as? String).map { " (\($0))" } ?? ""
                self.syncState.errorMessage =
                    "EA upload failed\(detail). Make sure the bulk-import endpoint is deployed and the dump size is within EA's upload limit, then tap Retry."
            }
            .store(in: &cancellables)

        // If an EA import was left in progress (key delivered, app then closed),
        // resume polling its server-side status on launch.
        if let importRun = UserDefaults.standard.string(forKey: "pendingEAImportRunID") {
            startEAImportPolling(runID: importRun)
        }

        // If there's no baseline yet, show a clean slate on launch.
        cleanForFullSyncIfNeeded()
    }

    var fullSyncPhase: FullSyncPhase { syncState.fullSyncPhase }
    var fullSyncSteps: [(label: String, status: SyncStepStatus)] { syncState.fullSyncStepStates }
    var eaUploadFailed: Bool { syncState.fsEAFailed }
    /// Files uploaded; EA's server-side import jobs still running (being polled).
    var eaImporting: Bool { syncState.eaImporting }
    var eaImportRowsImported: Int { syncState.eaImportRowsImported }
    var eaImportRowsExpected: Int { syncState.eaImportRowsExpected }
    /// 0…1 import progress when the expected total is known, else 0.
    var eaImportFraction: Double {
        let expected = syncState.eaImportRowsExpected
        guard expected > 0 else { return 0 }
        return min(1, Double(syncState.eaImportRowsImported) / Double(expected))
    }

    /// Re-runs the EA upload for the pending run (the encrypted dump is kept
    /// until both backends confirm, so retry re-uploads without re-exporting).
    func retryEAUpload() {
        guard let runID = UserDefaults.standard.string(forKey: "pendingEAUploadRunID") else {
            syncState.errorMessage = "Nothing to retry — run a full sync first."
            return
        }
        syncState.fsEAFailed = false
        syncState.errorMessage = nil
        if syncState.fullSyncPhase == .idle { syncState.fullSyncPhase = .delivering }
        syncState.currentOperation = "Retrying EA upload…"
        // Stash the live-activity weighting so the background uploader can drive
        // the live activity across EA's slice (base = progress now, span = EA weight),
        // mirroring runFullSync. base already includes any completed MySQL slice.
        UserDefaults.standard.set(syncState.overallProgress, forKey: "eaUpload.liveBase.\(runID)")
        UserDefaults.standard.set(syncState.fullSyncEAWeight, forKey: "eaUpload.liveSpan.\(runID)")
        do { try EADumpUploader.shared.start(runID: runID) }
        catch { syncState.errorMessage = error.localizedDescription }
    }

    /// When no destination has a baseline yet and nothing is running, present a
    /// clean slate — reset every category card + the on-screen total — so it's
    /// obvious all data must be synced. The "needs full sync" banner still tells
    /// the user a full sync is required. If either destination is baselined, the
    /// cards reflect real data and must not be wiped.
    func cleanForFullSyncIfNeeded() {
        // Not while a full sync is in flight (fullSyncPhase != .idle) — its EA
        // delivery may still be uploading; resetting would wipe live progress.
        guard !syncState.mysqlBaselineDone, !syncState.eaBaselineDone,
              !syncState.isAnySyncRunning, syncState.fullSyncPhase == .idle else { return }
        syncState.resetForFullSync()
        syncState.persist()
    }

    /// On returning to the foreground mid-delivery, reconcile the EA phase from
    /// background-updated state (the foreground notifications may have been missed
    /// while suspended): resume import-status polling if the key was already
    /// delivered, else sync the upload bar from the uploader's counters.
    private func refreshEAUploadProgress() {
        // Key already delivered → server is importing; (re)attach the poller.
        if let importRun = UserDefaults.standard.string(forKey: "pendingEAImportRunID") {
            startEAImportPolling(runID: importRun)
            return
        }
        guard let runID = UserDefaults.standard.string(forKey: "pendingEAUploadRunID") else { return }
        let total = UserDefaults.standard.integer(forKey: "eaUpload.total.\(runID)")
        if total > 0, syncState.fullSyncPhase == .delivering {
            let remaining = UserDefaults.standard.integer(forKey: "eaUpload.remaining.\(runID)")
            syncState.updateFullSyncProgress(ea: Double(max(0, total - max(0, remaining))) / Double(total))
        }
        if UserDefaults.standard.bool(forKey: "eaUpload.failed.\(runID)") {
            syncState.markEAFailed()
        }
    }

    /// Polls the EA server's `bulkImportStatus` after the dump key is delivered,
    /// until the decrypt-and-import jobs finish. EA's baseline is marked ONLY on
    /// `completed`; a server-side `failed` surfaces an error and leaves EA needing
    /// a full sync (its local dump is already wiped, so recovery is a fresh Full
    /// Sync, not a re-upload). Survives backgrounding — it's restarted from the
    /// persisted `pendingEAImportRunID` on launch/foreground.
    func startEAImportPolling(runID: String) {
        eaImportPollTask?.cancel()
        UserDefaults.standard.set(runID, forKey: "pendingEAImportRunID")
        syncState.eaImporting = true
        if syncState.fullSyncPhase == .idle { syncState.fullSyncPhase = .delivering }
        syncState.currentOperation = "Uploaded — importing on EA…"
        eaImportPollTask = Task { [weak self] in
            await self?.pollEAImport(runID: runID)
        }
    }

    private func pollEAImport(runID: String) async {
        let svc = EAService(config: EAConfig.load())
        var consecutiveFailures = 0
        // ~30 min ceiling. We keep polling through transient errors (just backing
        // off) rather than silently giving up, so a flaky link doesn't strand the
        // UI on "Importing…" — and we never declare success without a `completed`.
        for _ in 0..<360 {
            if Task.isCancelled { return }
            do {
                let status = try await svc.bulkImportStatus(runID: runID)
                consecutiveFailures = 0
                if let expected = status.rows_expected, expected > 0 {
                    syncState.eaImportRowsExpected = expected
                }
                syncState.eaImportRowsImported = status.rows_imported
                switch status.status {
                case "completed":
                    syncState.markEADone()
                    syncState.markEABaselineDone()
                    UserDefaults.standard.removeObject(forKey: "pendingEAImportRunID")
                    UserDefaults.standard.removeObject(forKey: "pendingEAUploadRunID")
                    syncState.persist()
                    syncService.finalizeFullSyncIfComplete()
                    if syncState.fullSyncPhase != .complete {
                        syncState.currentOperation = "EA import done — finishing MySQL…"
                    }
                    return
                case "failed":
                    syncState.eaImporting = false
                    syncState.endFullSyncProgress()
                    syncState.errorMessage =
                        "EA import failed: \(status.error_message ?? "unknown error"). Run Full Sync to try again."
                    UserDefaults.standard.removeObject(forKey: "pendingEAImportRunID")
                    syncState.persist()
                    return
                default:   // uploading / awaiting_key / processing
                    syncState.currentOperation = "Importing on EA…"
                }
                try? await Task.sleep(nanoseconds: 4_000_000_000)   // 4s while healthy
            } catch {
                consecutiveFailures += 1
                syncState.currentOperation = "Importing on EA — reconnecting…"
                // Back off on errors (cap ~30s) but keep trying; the import runs
                // server-side regardless, so we just need to reach the status API.
                let backoff = min(30, 4 * consecutiveFailures)
                try? await Task.sleep(nanoseconds: UInt64(backoff) * 1_000_000_000)
            }
        }
    }

    var categories: [CategorySyncState] { syncState.categories }
    var isFullSyncRunning: Bool { syncState.isFullSyncRunning }
    var isAnySyncRunning: Bool { syncState.isAnySyncRunning }
    var totalRecords: Int { syncState.totalRecords }
    var lastSyncDate: Date? { syncState.lastSyncDate }
    var overallProgress: Double { syncState.overallProgress }
    var currentOperation: String { syncState.currentOperation }
    var errorMessage: String? { syncState.errorMessage }

    // Per-destination baseline status, combined with the live enable/config flags.
    var mysqlNeedsFullSync: Bool { syncState.mysqlNeedsFullSync(enabled: MySQLConfig.load().enabled) }
    var eaNeedsFullSync: Bool { syncState.eaNeedsFullSync(configured: EAConfig.load().isConfigured) }
    /// True when any enabled destination still needs a full-sync baseline.
    var needsFullSync: Bool { mysqlNeedsFullSync || eaNeedsFullSync }
    /// Human label for the destination(s) a full sync would catch up.
    var fullSyncTargetsLabel: String {
        switch (mysqlNeedsFullSync, eaNeedsFullSync) {
        case (true, true):  return "MySQL and EA"
        case (true, false): return "MySQL"
        case (false, true): return "EA"
        case (false, false): return ""
        }
    }

    var lastSyncLabel: String {
        guard let date = lastSyncDate else { return "Never synced" }
        let rel = RelativeDateTimeFormatter()
        rel.unitsStyle = .full
        return "Last synced \(rel.localizedString(for: date, relativeTo: Date()))"
    }

    func checkPrerequisites() {
        let config = MySQLConfig.load()
        Task {
            let issues = await syncService.validatePrerequisites(config: config)
            self.prerequisiteIssues = issues
        }
    }

    /// The one sync button. Runs a full sync when any enabled destination still
    /// needs a baseline (dump-based export → deliver to just the lagging ones),
    /// otherwise an incremental sync to all baselined destinations.
    func startSync() {
        let config = MySQLConfig.load()
        let needsFullSync = self.needsFullSync
        // Re-attach EA so a now-baselined EA receives the incremental pass.
        syncService.attachEAIfConfigured()
        // Fire off prerequisite validation without blocking the sync.
        Task {
            let issues = await syncService.validatePrerequisites(config: config)
            self.prerequisiteIssues = issues
            if !issues.isEmpty && needsFullSync {
                self.showPrerequisiteAlert = true
            }
        }
        let task = Task {
            if needsFullSync {
                await syncService.runFullSync(config: config)
            } else {
                await syncService.runIncrementalSync(config: config)
            }
            refreshRecordCounts()
            refreshLatestHealthKitDates()
        }
        syncTask = task
        syncService.taskForCancellation = task
    }

    func startReminderSync() {
        guard !isAnySyncRunning else { return }
        isReminderSync = true
        let config = MySQLConfig.load()
        let needsFullSync = self.needsFullSync
        syncService.attachEAIfConfigured()
        syncState.currentOperation = needsFullSync
            ? "Sync triggered by reminder — keep screen unlocked for the export"
            : "Sync triggered by reminder"
        let task = Task {
            if needsFullSync {
                await syncService.runFullSync(config: config)
            } else {
                await syncService.runIncrementalSync(config: config)
            }
            refreshRecordCounts()
            refreshLatestHealthKitDates()
            isReminderSync = false
        }
        syncTask = task
        syncService.taskForCancellation = task
    }

    func cancelSync() {
        syncTask?.cancel()
        syncTask = nil
    }

    func startCategorySync(categoryID: String) {
        let config = MySQLConfig.load()
        syncTask = Task {
            await syncService.runSingleCategorySync(categoryID: categoryID, config: config)
            refreshRecordCounts()
            refreshLatestHealthKitDates()
        }
    }

    /// Re-syncs a list of categories sequentially (used by data validation repair).
    func repairCategories(categoryIDs: [String]) {
        let config = MySQLConfig.load()
        let task = Task {
            for catID in categoryIDs {
                await syncService.runSingleCategorySync(categoryID: catID, config: config)
            }
            refreshRecordCounts()
            refreshLatestHealthKitDates()
        }
        syncTask = task
        syncService.taskForCancellation = task
    }

    func resetCategory(categoryID: String) {
        guard !isAnySyncRunning else { return }
        let config = MySQLConfig.load()
        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)
                try await SchemaService.deleteCategoryData(categoryID: categoryID, mysql: mysql)
                await mysql.disconnect()
                syncState.resetCategoryLocalState(categoryID)
            } catch {
                await mysql.disconnect()
                syncState.errorMessage = "Reset failed: \(error.localizedDescription)"
            }
        }
    }

    func refreshLatestHealthKitDates() {
        Task {
            for i in syncState.categories.indices {
                let date = await latestHKDate(for: syncState.categories[i].id)
                syncState.categories[i].latestHealthKitDate = date
            }
        }
    }

    private func latestHKDate(for catID: String) async -> Date? {
        if catID.hasPrefix("qty_") {
            guard let cat = HealthCategory.allCases.first(where: { "qty_\($0.rawValue)" == catID })
            else { return nil }
            let types = HealthDataTypes.allQuantityTypes.filter { $0.category == cat }
            return await withTaskGroup(of: Date?.self) { group in
                for td in types {
                    guard let hkType = td.hkType else { continue }
                    group.addTask { await HealthKitService.shared.latestSampleDate(for: hkType) }
                }
                var latest: Date? = nil
                for await date in group {
                    if let d = date, latest == nil || d > latest! { latest = d }
                }
                return latest
            }
        }
        switch catID {
        case "cat_category":
            return await withTaskGroup(of: Date?.self) { group in
                for td in HealthDataTypes.allCategoryTypes {
                    guard let hkType = td.hkType else { continue }
                    group.addTask { await HealthKitService.shared.latestSampleDate(for: hkType) }
                }
                var latest: Date? = nil
                for await date in group {
                    if let d = date, latest == nil || d > latest! { latest = d }
                }
                return latest
            }
        case "cat_workouts":
            return await HealthKitService.shared.latestSampleDate(for: .workoutType())
        case "cat_bp":
            guard let t = HKObjectType.correlationType(forIdentifier: .bloodPressure) else { return nil }
            return await HealthKitService.shared.latestSampleDate(for: t)
        case "cat_ecg":
            return await HealthKitService.shared.latestSampleDate(for: .electrocardiogramType())
        case "cat_audiogram":
            return await HealthKitService.shared.latestSampleDate(for: .audiogramSampleType())
        case "cat_workout_routes":
            return await HealthKitService.shared.latestSampleDate(for: HKSeriesType.workoutRoute())
        case "cat_vision":
            return await HealthKitService.shared.latestSampleDate(for: HKObjectType.visionPrescriptionType())
        case "cat_state_of_mind":
            if #available(iOS 18, *) {
                return await HealthKitService.shared.latestSampleDate(for: HKObjectType.stateOfMindType())
            }
            return nil
        default:
            // cat_activity_summaries, cat_medications: use different HK query types — skip
            return nil
        }
    }

    func refreshRecordCounts() {
        let config = MySQLConfig.load()
        // Record counts come from MySQL; skip when that destination is off or has
        // no baseline yet (keep the clean slate — don't repopulate from a stale or
        // empty DB).
        guard config.enabled, syncState.mysqlBaselineDone else { return }
        Task {
            do {
                let mysql = MySQLService()
                try await mysql.connect(config: config)

                // Ensure schema is up-to-date so new tables exist
                let _ = await SchemaService.initializeSchema(mysql: mysql)

                let counts = await SchemaService.recordCounts(mysql: mysql)

                // Query actual per-type counts from the quantity samples table
                let typeRows = try await mysql.query(
                    "SELECT type, COUNT(*) as cnt FROM health_quantity_samples GROUP BY type"
                )
                await mysql.disconnect()

                // Map type → count
                var qtyCountByType: [String: Int] = [:]
                for row in typeRows {
                    if let typeName = row["type"], let cntStr = row["cnt"], let cnt = Int(cntStr) {
                        qtyCountByType[typeName] = cnt
                    }
                }

                // Aggregate counts per HealthCategory
                var qtyCountByCategory: [String: Int] = [:]
                for typeDesc in HealthDataTypes.allQuantityTypes {
                    let catID = "qty_\(typeDesc.category.rawValue)"
                    qtyCountByCategory[catID, default: 0] += qtyCountByType[typeDesc.id] ?? 0
                }

                let catTotal = counts["health_category_samples"] ?? 0
                let workoutTotal = counts["health_workouts"] ?? 0
                let bpTotal = counts["health_blood_pressure"] ?? 0
                let ecgTotal = counts["health_ecg"] ?? 0
                let audioTotal = counts["health_audiograms"] ?? 0
                let activityTotal = counts["health_activity_summaries"] ?? 0
                let routeTotal = counts["health_workout_routes"] ?? 0
                let medTotal = counts["health_medications"] ?? 0

                for i in syncState.categories.indices {
                    let id = syncState.categories[i].id
                    if id.hasPrefix("qty_") {
                        syncState.categories[i].recordCount = qtyCountByCategory[id] ?? 0
                    } else if id == "cat_category" {
                        syncState.categories[i].recordCount = catTotal
                    } else if id == "cat_workouts" {
                        syncState.categories[i].recordCount = workoutTotal
                    } else if id == "cat_bp" {
                        syncState.categories[i].recordCount = bpTotal
                    } else if id == "cat_ecg" {
                        syncState.categories[i].recordCount = ecgTotal
                    } else if id == "cat_audiogram" {
                        syncState.categories[i].recordCount = audioTotal
                    } else if id == "cat_activity_summaries" {
                        syncState.categories[i].recordCount = activityTotal
                    } else if id == "cat_workout_routes" {
                        syncState.categories[i].recordCount = routeTotal
                    } else if id == "cat_medications" {
                        syncState.categories[i].recordCount = medTotal
                    }
                }

                // Use actual table COUNT(*) for the total — this includes any records
                // whose types aren't in the current allQuantityTypes list, and is always
                // accurate regardless of what was synced in the current session.
                let qtyTotal = counts["health_quantity_samples"] ?? 0
                syncState.totalRecords = qtyTotal + catTotal + workoutTotal + bpTotal + ecgTotal + audioTotal + activityTotal + routeTotal + medTotal
                syncState.persist()

            } catch {
                let config = MySQLConfig.load()
                if config.host != MySQLConfig.default.host {
                    syncState.errorMessage = "Could not refresh record counts: \(error.localizedDescription)"
                }
            }
        }
    }
}
