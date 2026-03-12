import ActivityKit
import BackgroundTasks
import CoreLocation
import Foundation
import HealthKit
import UIKit

// Batch size for INSERT IGNORE statements
private let batchSize = 500

// MARK: - Date formatter (shared, MySQL datetime format)
private let sqlDateFormatter: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    f.timeZone = TimeZone(identifier: "UTC")
    f.locale = Locale(identifier: "en_US_POSIX")
    return f
}()

private func sqlDate(_ date: Date) -> String {
    sqlDateFormatter.string(from: date)
}

// MARK: - SyncService

@MainActor
final class SyncService: ObservableObject {

    private let healthKit = HealthKitService.shared
    let syncState: SyncState
    private var mysql: MySQLService?

    // When true, skip live activity and allow resumable sync across background task invocations
    var isBackgroundSync = false

    // When true, never create or update a Live Activity (used for observer-triggered real-time syncs)
    var suppressLiveActivity = false

    /// Set by the caller before `runHistoricalBackfill` so the background-task expiry
    /// handler can cancel the Swift Task when iOS reclaims background time.
    var taskForCancellation: Task<Void, Never>?

    // Class-level flag so BackgroundSyncManager can check whether ANY SyncService instance
    // (foreground or background) is currently running, preventing concurrent MySQL connections
    // from competing for row locks on the same tables.
    @MainActor static private(set) var isSyncRunning = false

    // Live Activity
    private var liveActivity: Activity<SyncActivityAttributes>?
    private var lastLiveActivityUpdate: Date = .distantPast

    init(syncState: SyncState) {
        self.syncState = syncState
        setupCategories()
        syncState.restore()
    }

    private func setupCategories() {
        var cats: [CategorySyncState] = []
        // Quantity categories
        for (cat, types) in HealthDataTypes.quantityTypesByCategory {
            let count = types.count
            cats.append(CategorySyncState(
                id: "qty_\(cat.rawValue)",
                displayName: cat.rawValue,
                systemImage: cat.systemImage,
                status: .idle,
                recordCount: 0,
                lastSyncDate: nil,
                currentProgress: 0,
                totalEstimated: count
            ))
        }
        // Special categories
        let specials: [(String, String, String)] = [
            ("cat_category", "Health Events", "heart.text.square.fill"),
            ("cat_workouts", "Workouts", "dumbbell.fill"),
            ("cat_bp", "Blood Pressure", "drop.fill"),
            ("cat_ecg", "ECG", "waveform.path.ecg.rectangle.fill"),
            ("cat_audiogram", "Audiogram", "ear.badge.waveform"),
            ("cat_activity_summaries", "Activity Rings", "chart.bar.fill"),
            ("cat_workout_routes", "Workout Routes", "map.fill"),
            ("cat_medications", "Medications", "pills.fill"),
            ("cat_vision", "Vision Prescriptions", "eye.fill"),
            ("cat_state_of_mind", "State of Mind", "brain.head.profile"),
        ]
        for (id, name, icon) in specials {
            cats.append(CategorySyncState(
                id: id,
                displayName: name,
                systemImage: icon,
                status: .idle,
                recordCount: 0,
                lastSyncDate: nil,
                currentProgress: 0,
                totalEstimated: 1
            ))
        }
        syncState.categories = cats
    }

    // MARK: - Live Activity

    private func startLiveActivity(isFullSync: Bool) {
        guard !suppressLiveActivity else { return }
        if isBackgroundSync {
            liveActivity = Activity<SyncActivityAttributes>.activities.first
            if liveActivity != nil { return }
            // No existing activity — only create one if the app is currently active.
            // BGProcessingTask keeps the app in .background state, so this only fires when
            // the user has the app open (e.g. they opened the app mid-background-sync).
            guard UIApplication.shared.applicationState == .active else { return }
            // Fall through to create a new activity
        }
        guard ActivityAuthorizationInfo().areActivitiesEnabled else { return }
        let initial = SyncActivityAttributes.ContentState(
            phase: "Connecting",
            operation: "Connecting to MySQL…",
            recordsInserted: 0,
            isFullSync: isFullSync
        )
        do {
            liveActivity = try Activity.request(
                attributes: SyncActivityAttributes(),
                content: ActivityContent(state: initial, staleDate: nil),
                pushType: nil
            )
        } catch {
            // Live Activities not available or denied — sync continues without it
        }
    }

    private func updateLiveActivity(phase: String, operation: String, records: Int) {
        guard !suppressLiveActivity else { return }
        // If no activity yet and we're now in the foreground, try to create one.
        // This covers the case where the user opens the app mid-background-sync.
        if liveActivity == nil {
            startLiveActivity(isFullSync: syncState.isFullSyncRunning)
        }
        guard Date().timeIntervalSince(lastLiveActivityUpdate) >= 1.0 else { return }
        lastLiveActivityUpdate = Date()
        let activity = liveActivity ?? Activity<SyncActivityAttributes>.activities.first
        guard let activity else { return }
        let isFullSync = syncState.isFullSyncRunning
        let state = SyncActivityAttributes.ContentState(
            phase: phase,
            operation: operation,
            recordsInserted: records,
            isFullSync: isFullSync
        )
        let content = ActivityContent(state: state, staleDate: nil)
        // Await the update directly to ensure it completes before moving on
        Task { @MainActor in
            await activity.update(content)
        }
    }

    private func endLiveActivity(totalRecords: Int) {
        guard !suppressLiveActivity else { return }
        let activity = liveActivity ?? Activity<SyncActivityAttributes>.activities.first
        guard let activity else { return }
        let isFullSync = syncState.isFullSyncRunning
        let finalState = SyncActivityAttributes.ContentState(
            phase: "Done",
            operation: "Synced \(totalRecords.formatted()) records",
            recordsInserted: totalRecords,
            isFullSync: isFullSync
        )
        let finalContent = ActivityContent(state: finalState, staleDate: nil)
        // Capture reference and nil out immediately to prevent double-end
        self.liveActivity = nil
        // End with a short delay so the "Done" state is visible before dismissal
        Task { @MainActor in
            await activity.end(finalContent, dismissalPolicy: .after(.now + 5))
        }
    }

    // MARK: - Connection management

    func connectMySQL(config: MySQLConfig) async throws {
        let svc = MySQLService()
        try await svc.connect(config: config)
        self.mysql = svc
    }

    func disconnectMySQL() {
        Task { await mysql?.disconnect() }
        mysql = nil
    }

    // MARK: - Pre-sync validation

    /// Check HealthKit authorization and database schema before syncing.
    /// Returns a list of issues that need user attention.
    func validatePrerequisites(config: MySQLConfig) async -> [SyncPrerequisiteIssue] {
        var issues: [SyncPrerequisiteIssue] = []

        // Check HealthKit availability
        if !healthKit.isAvailable {
            issues.append(.healthDataUnavailable)
            return issues
        }

        // Check if permissions were ever requested
        let permissionsRequested = UserDefaults.standard.bool(forKey: "hk_permissions_requested")
        if !permissionsRequested {
            issues.append(.healthPermissionsNotRequested)
        }

        // Check a sample of key HealthKit types for authorization.
        // authorizationStatus only tracks write permission. For read-only types,
        // .notDetermined means the dialog was never shown (truly not requested),
        // while .sharingDenied means the dialog was shown (read grant/deny is hidden by iOS).
        let criticalTypes: [HKObjectType] = [
            HKObjectType.quantityType(forIdentifier: .heartRate)!,
            HKObjectType.quantityType(forIdentifier: .stepCount)!,
            HKObjectType.quantityType(forIdentifier: .bodyMass)!,
            HKObjectType.workoutType(),
            HKObjectType.quantityType(forIdentifier: .appleSleepingWristTemperature)!,
        ]
        let notRequestedTypes = criticalTypes.filter {
            healthKit.authorizationStatus(for: $0) == .notDetermined
        }
        if !notRequestedTypes.isEmpty {
            issues.append(.somePermissionsDenied(count: notRequestedTypes.count))
        }

        // Check database schema
        do {
            let mysql = MySQLService()
            try await mysql.connect(config: config)
            let requiredTables = [
                "health_quantity_samples", "health_category_samples", "health_workouts",
                "health_blood_pressure", "health_ecg", "health_audiograms",
                "health_activity_summaries", "health_workout_routes", "health_medications",
                "health_vision_prescriptions", "health_state_of_mind",
                "health_sync_log", "location_tracks", "location_geofence_events"
            ]
            var missingTables: [String] = []
            for table in requiredTables {
                let exists = await SchemaService.tableExists(table, mysql: mysql)
                if !exists { missingTables.append(table) }
            }
            await mysql.disconnect()

            if !missingTables.isEmpty {
                issues.append(.missingDatabaseTables(tables: missingTables))
            }
        } catch {
            issues.append(.databaseConnectionFailed(error.localizedDescription))
        }

        return issues
    }

    // MARK: - Full sync

    func runFullSync(config: MySQLConfig) async {
        await runHistoricalBackfill(config: config)
    }

    // MARK: - Single-category sync

    func runSingleCategorySync(categoryID: String, config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        syncState.isFullSyncRunning = true
        SyncService.isSyncRunning = true
        defer { SyncService.isSyncRunning = false }
        syncState.errorMessage = nil
        syncState.currentOperation = "Connecting…"
        startLiveActivity(isFullSync: false)

        let anchor = Date()
        let epoch = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!

        do {
            try await connectMySQL(config: config)
            guard mysql != nil else { throw MySQLError.disconnected }

            let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: mysql!)
            if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }

            syncState.updateCategory(categoryID, status: .syncing)
            syncState.currentOperation = "Syncing…"

            let count: Int
            if categoryID.hasPrefix("qty_") {
                let rawCat = String(categoryID.dropFirst(4))
                guard let cat = HealthCategory(rawValue: rawCat),
                      let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == cat })?.1 else {
                    throw MySQLError.queryError(code: 0, message: "Unknown category: \(categoryID)")
                }
                count = try await backfillQuantityCategory(
                    catID: categoryID, cat: cat, types: types,
                    from: epoch, until: anchor, config: config
                )
            } else {
                count = try await backfillSpecialCategory(
                    catID: categoryID, from: epoch, until: anchor, config: config
                ) { [self] windowStart, windowEnd, activeMySQL in
                    switch categoryID {
                    case "cat_category":          return try await syncCategorySamples(mysql: activeMySQL, since: windowStart, until: windowEnd, insertBatchSize: 50)
                    case "cat_workouts":          return try await syncWorkouts(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_bp":                return try await syncBloodPressure(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_ecg":               return try await syncECG(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_audiogram":         return try await syncAudiograms(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_activity_summaries": return try await syncActivitySummaries(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_workout_routes":    return try await syncWorkoutRoutes(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_medications":       return try await syncMedications(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_vision":            return try await syncVisionPrescriptions(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_state_of_mind":     return try await syncStateOfMind(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    default: return 0
                    }
                }
            }

            syncState.updateCategory(categoryID, status: .completed, recordCount: count, lastSyncDate: Date())
            syncState.lastSyncDate = Date()
            syncState.currentOperation = ""
            // Clear cursor so a future full sync re-visits this category from the beginning
            syncState.backfillCursors.removeValue(forKey: categoryID)
            syncState.persist()
            endLiveActivity(totalRecords: syncState.totalRecords)
            disconnectMySQL()

        } catch is CancellationError {
            disconnectMySQL()
            endLiveActivity(totalRecords: syncState.totalRecords)
            syncState.currentOperation = "Sync cancelled"
            if case .syncing = syncState.categories.first(where: { $0.id == categoryID })?.status {
                syncState.updateCategory(categoryID, status: .idle)
            }
            syncState.persist()
        } catch {
            disconnectMySQL()
            endLiveActivity(totalRecords: syncState.totalRecords)
            syncState.errorMessage = error.localizedDescription
            syncState.currentOperation = ""
            syncState.updateCategory(categoryID, status: .failed(error.localizedDescription))
            syncState.persist()
        }

        syncState.isFullSyncRunning = false
    }

    // MARK: - Historical backfill (windowed, resumable)

    func runHistoricalBackfill(config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        syncState.isFullSyncRunning = true
        SyncService.isSyncRunning = true
        defer { SyncService.isSyncRunning = false }
        syncState.errorMessage = nil
        syncState.currentOperation = "Connecting…"
        startLiveActivity(isFullSync: true)

        if !isBackgroundSync {
            UIApplication.shared.isIdleTimerDisabled = true
        }
        defer {
            if !isBackgroundSync {
                UIApplication.shared.isIdleTimerDisabled = false
            }
        }

        var bgTaskID: UIBackgroundTaskIdentifier = .invalid
        if !isBackgroundSync {
            bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "health-full-sync") {
                self.taskForCancellation?.cancel()
                self.syncState.persist()
                UserDefaults.standard.set(true, forKey: "pendingFullSyncResume")
                let req = BGProcessingTaskRequest(identifier: "ee.klemens.healthbeat.sync")
                req.requiresNetworkConnectivity = true
                req.requiresExternalPower = false
                req.earliestBeginDate = nil
                try? BGTaskScheduler.shared.submit(req)
                UIApplication.shared.endBackgroundTask(bgTaskID)
                bgTaskID = .invalid
            }
        }
        defer {
            if bgTaskID != .invalid {
                UIApplication.shared.endBackgroundTask(bgTaskID)
            }
        }

        let epoch = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
        let historicalStart: Date

        if let previousAnchor = syncState.backfillAnchorDate, syncState.hasCompletedFullSync {
            // A full backfill previously completed. Re-run with a 7-day lookback so samples
            // that arrived in HealthKit after the previous anchor (with past startDates) are
            // captured. INSERT IGNORE makes this safe.
            // Clear cursors and advance anchor to now; hasCompletedFullSync is cleared so that
            // an interruption resumes rather than triggering another lookback.
            historicalStart = previousAnchor.addingTimeInterval(-7 * 24 * 3600)
            syncState.backfillAnchorDate = Date()
            syncState.backfillCursors.removeAll()
            syncState.hasCompletedFullSync = false
            syncState.persist()
        } else if syncState.backfillAnchorDate != nil {
            // Anchor exists but sync hasn't completed — resuming an interrupted backfill.
            historicalStart = epoch
        } else {
            // First-time full sync: backfill all data from year 2000.
            syncState.backfillAnchorDate = Date()
            syncState.persist()
            historicalStart = epoch
        }
        let anchor = syncState.backfillAnchorDate!

        var syncLogID: Int64 = 0
        do {
            try await connectMySQL(config: config)
            guard let initialMySQL = mysql else { throw MySQLError.disconnected }

            let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: initialMySQL)
            if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }

            await cleanupStaleLogEntries(mysql: initialMySQL)
            syncLogID = try await startSyncLog(mysql: initialMySQL, category: "full_sync")

            // Quantity categories — 90-day windowed backfill
            for (cat, types) in HealthDataTypes.quantityTypesByCategory {
                let catID = "qty_\(cat.rawValue)"
                try Task.checkCancellation()
                if syncState.backfillCursors[catID] == anchor { continue }

                syncState.updateCategory(catID, status: .syncing)
                syncState.currentOperation = "Backfilling \(cat.rawValue)…"
                let count = try await backfillQuantityCategory(
                    catID: catID, cat: cat, types: types,
                    from: historicalStart, until: anchor, config: config
                )
                syncState.updateCategory(catID, status: .completed, recordCount: count, lastSyncDate: Date())
                updateLiveActivity(phase: cat.rawValue, operation: "Backfilled \(cat.rawValue) (\(count.formatted()) records)", records: count)
            }

            // Special categories — 90-day windowed backfill
            let specials: [(String, String)] = [
                ("cat_category", "Health Events"),
                ("cat_workouts", "Workouts"),
                ("cat_bp", "Blood Pressure"),
                ("cat_ecg", "ECG"),
                ("cat_audiogram", "Audiograms"),
                ("cat_activity_summaries", "Activity Rings"),
                ("cat_workout_routes", "Workout Routes"),
                ("cat_medications", "Medications"),
                ("cat_vision", "Vision Prescriptions"),
                ("cat_state_of_mind", "State of Mind"),
            ]
            for (catID, displayName) in specials {
                try Task.checkCancellation()
                if syncState.backfillCursors[catID] == anchor { continue }

                syncState.updateCategory(catID, status: .syncing)
                syncState.currentOperation = "Backfilling \(displayName)…"
                let count = try await backfillSpecialCategory(
                    catID: catID, from: historicalStart, until: anchor, config: config
                ) { [self] windowStart, windowEnd, activeMySQL in
                    switch catID {
                    case "cat_category":
                        return try await syncCategorySamples(mysql: activeMySQL, since: windowStart, until: windowEnd, insertBatchSize: 50)
                    case "cat_workouts":
                        return try await syncWorkouts(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_bp":
                        return try await syncBloodPressure(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_ecg":
                        return try await syncECG(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_audiogram":
                        return try await syncAudiograms(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_activity_summaries":
                        return try await syncActivitySummaries(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_workout_routes":
                        return try await syncWorkoutRoutes(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_medications":
                        return try await syncMedications(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_vision":
                        return try await syncVisionPrescriptions(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    case "cat_state_of_mind":
                        return try await syncStateOfMind(mysql: activeMySQL, since: windowStart, until: windowEnd)
                    default:
                        return 0
                    }
                }
                syncState.updateCategory(catID, status: .completed, recordCount: count, lastSyncDate: Date())
                updateLiveActivity(phase: displayName, operation: "Backfilled \(displayName) (\(count.formatted()) records)", records: count)
            }

            // Mark complete. Keep backfillCursors (all at anchor) and backfillAnchorDate so
            // the next Full Sync press detects "allComplete" and re-syncs only the 7-day
            // lookback window rather than re-scanning from epoch.
            syncState.hasCompletedFullSync = true
            syncState.lastSyncDate = Date()
            syncState.currentOperation = "Backfill complete"

            if let currentMySQL = mysql {
                try await completeSyncLog(mysql: currentMySQL, id: syncLogID, count: syncState.totalRecords)
            }
            syncState.persist()
            endLiveActivity(totalRecords: syncState.totalRecords)
            disconnectMySQL()

        } catch is CancellationError {
            if let m = mysql, syncLogID != 0 { await failSyncLog(mysql: m, id: syncLogID, message: "Sync cancelled") }
            disconnectMySQL()
            endLiveActivity(totalRecords: syncState.totalRecords)
            syncState.currentOperation = "Sync cancelled"
            for i in syncState.categories.indices {
                if case .syncing = syncState.categories[i].status {
                    syncState.categories[i].status = .idle
                }
            }
            syncState.persist()
        } catch {
            if let m = mysql, syncLogID != 0 { await failSyncLog(mysql: m, id: syncLogID, message: error.localizedDescription) }
            disconnectMySQL()
            endLiveActivity(totalRecords: syncState.totalRecords)
            syncState.errorMessage = error.localizedDescription
            syncState.currentOperation = ""
            for i in syncState.categories.indices {
                if case .syncing = syncState.categories[i].status {
                    syncState.categories[i].status = .failed(error.localizedDescription)
                }
            }
            syncState.persist()
        }

        syncState.isFullSyncRunning = false
    }

    // MARK: - Backfill helpers

    /// Ensures MySQL is connected, reconnecting if the connection was dropped.
    private func ensureMySQLConnected(config: MySQLConfig) async throws {
        guard mysql != nil else {
            try await connectMySQL(config: config)
            return
        }
        do {
            try await mysql!.execute("SELECT 1")
        } catch {
            disconnectMySQL()
            try await connectMySQL(config: config)
        }
    }

    /// Returns true if the error indicates a dropped MySQL connection that can be retried
    /// after reconnecting (e.g. screen lock, network change, TCP reset).
    private static func isConnectionError(_ error: Error) -> Bool {
        if error is MySQLError {
            switch error as! MySQLError {
            case .connectionFailed, .disconnected, .timeout:
                return true
            default:
                return false
            }
        }
        return false
    }

    /// Backfills a quantity category in 90-day windows from `historicalStart` to `anchor`,
    /// resuming from `syncState.backfillCursors[catID]` if set.
    private func backfillQuantityCategory(
        catID: String,
        cat: HealthCategory,
        types: [QuantityTypeDescriptor],
        from historicalStart: Date,
        until anchor: Date,
        config: MySQLConfig
    ) async throws -> Int {
        let windowSize: TimeInterval = 90 * 24 * 60 * 60
        var cursor = syncState.backfillCursors[catID] ?? historicalStart
        var total = 0
        let totalWindows = Int(ceil(anchor.timeIntervalSince(historicalStart) / windowSize))
        var windowIdx = cursor > historicalStart
            ? Int(ceil(cursor.timeIntervalSince(historicalStart) / windowSize))
            : 0

        while cursor < anchor {
            try Task.checkCancellation()
            try await ensureMySQLConnected(config: config)
            guard let activeMySQL = mysql else { throw MySQLError.disconnected }

            let windowEnd = min(cursor.addingTimeInterval(windowSize), anchor)
            var windowTotal = 0
            var retries = 0
            while true {
                do {
                    windowTotal = 0
                    for typeDesc in types {
                        windowTotal += try await syncQuantityType(
                            typeDesc: typeDesc, mysql: activeMySQL,
                            since: cursor, until: windowEnd,
                            insertBatchSize: 50
                        )
                    }
                    break
                } catch MySQLError.queryError(let code, _) where code == 1213 && retries < 3 {
                    // Deadlock: connection is still valid, just retry after backoff
                    retries += 1
                    try await Task.sleep(nanoseconds: UInt64(retries) * 500_000_000)
                }
            }
            total += windowTotal

            cursor = windowEnd
            windowIdx += 1
            syncState.backfillCursors[catID] = cursor
            syncState.persist()
            syncState.updateCategory(catID, status: .syncing, progress: windowIdx, total: totalWindows)
            let op = "Backfilling \(cat.rawValue): window \(windowIdx)/\(totalWindows)…"
            syncState.currentOperation = op
            updateLiveActivity(phase: cat.rawValue, operation: op, records: total)
        }
        return total
    }

    /// Backfills a special (non-quantity) category in 90-day windows, resuming from cursor.
    private func backfillSpecialCategory(
        catID: String,
        from historicalStart: Date,
        until anchor: Date,
        config: MySQLConfig,
        syncWindow: (Date, Date, MySQLService) async throws -> Int
    ) async throws -> Int {
        let windowSize: TimeInterval = 90 * 24 * 60 * 60
        var cursor = syncState.backfillCursors[catID] ?? historicalStart
        var total = 0
        let totalWindows = Int(ceil(anchor.timeIntervalSince(historicalStart) / windowSize))
        var windowIdx = cursor > historicalStart
            ? Int(ceil(cursor.timeIntervalSince(historicalStart) / windowSize))
            : 0

        while cursor < anchor {
            try Task.checkCancellation()
            try await ensureMySQLConnected(config: config)
            guard let activeMySQL = mysql else { throw MySQLError.disconnected }

            let windowEnd = min(cursor.addingTimeInterval(windowSize), anchor)
            var retries = 0
            var windowTotal = 0
            while true {
                do {
                    windowTotal = try await syncWindow(cursor, windowEnd, activeMySQL)
                    break
                } catch MySQLError.queryError(let code, _) where code == 1213 && retries < 3 {
                    // Deadlock: connection is still valid, just retry after backoff
                    retries += 1
                    try await Task.sleep(nanoseconds: UInt64(retries) * 500_000_000)
                }
            }
            total += windowTotal

            cursor = windowEnd
            windowIdx += 1
            syncState.backfillCursors[catID] = cursor
            syncState.persist()
            syncState.updateCategory(catID, status: .syncing, progress: windowIdx, total: totalWindows)
            let displayName = syncState.categories.first(where: { $0.id == catID })?.displayName ?? catID
            let op = "Backfilling \(displayName): window \(windowIdx)/\(totalWindows)…"
            syncState.currentOperation = op
            updateLiveActivity(phase: displayName, operation: op, records: total)
        }
        return total
    }

    // MARK: - Incremental sync

    func runIncrementalSync(config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        syncState.isIncrementalSyncRunning = true
        SyncService.isSyncRunning = true
        defer { SyncService.isSyncRunning = false }
        syncState.errorMessage = nil
        startLiveActivity(isFullSync: false)

        // Keep screen awake during foreground sync to prevent auto-lock killing HealthKit access
        if !isBackgroundSync {
            UIApplication.shared.isIdleTimerDisabled = true
        }
        defer {
            if !isBackgroundSync {
                UIApplication.shared.isIdleTimerDisabled = false
            }
        }

        // Request extra background execution time if user switches away during sync
        var bgTaskID: UIBackgroundTaskIdentifier = .invalid
        if !isBackgroundSync {
            bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "health-incremental-sync") {
                self.syncState.persist()
                let req = BGProcessingTaskRequest(identifier: "ee.klemens.healthbeat.sync")
                req.requiresNetworkConnectivity = true
                req.earliestBeginDate = nil
                try? BGTaskScheduler.shared.submit(req)
                UIApplication.shared.endBackgroundTask(bgTaskID)
                bgTaskID = .invalid
            }
        }
        defer {
            if bgTaskID != .invalid {
                UIApplication.shared.endBackgroundTask(bgTaskID)
            }
        }

        var logID: Int64 = 0
        do {
            // Pre-first-unlock guard: isProtectedDataAvailable is false only before the very
            // first unlock after boot. The errorDatabaseInaccessible suppression below handles
            // the common screen-locked case (device unlocked at least once since boot).
            if isBackgroundSync {
                guard UIApplication.shared.isProtectedDataAvailable else {
                    syncState.isIncrementalSyncRunning = false
                    return
                }
            }

            try await connectMySQL(config: config)
            guard let mysql = mysql else { throw MySQLError.disconnected }

            // Helper: get a live MySQL connection, reconnecting if the previous one was
            // dropped (e.g. screen locked, network changed). Returns the fresh instance.
            @MainActor func liveMySQL() async throws -> MySQLService {
                try await ensureMySQLConnected(config: config)
                guard let m = self.mysql else { throw MySQLError.disconnected }
                return m
            }

            // Ensure schema is up to date (adds any new tables from updates)
            let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: mysql)
            if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }

            await cleanupStaleLogEntries(mysql: mysql)

            // Find last sync date. If no completed sync exists (e.g. a full sync was interrupted),
            // fall back to a distant past date so we recover all historical data rather than just 24h.
            let lastSync = try await lastCompletedSyncDate(mysql: mysql)
            let distantPast = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
            let since = lastSync ?? distantPast
            // Apply a 7-day lookback for HealthKit queries so late-arriving samples (e.g. apps
            // that backfill historical entries into HealthKit after the fact) are captured.
            // INSERT IGNORE makes re-syncing the overlap window safe and idempotent.
            let querySince = lastSync.map { $0.addingTimeInterval(-7 * 24 * 3600) } ?? distantPast

            let opLabel = lastSync != nil
                ? "Incremental sync from \(since.formatted(date: .abbreviated, time: .shortened))…"
                : "Full historical sync (fetching all data since 2000)…"
            syncState.currentOperation = opLabel

            logID = try await startSyncLog(mysql: mysql, category: "incremental_sync")
            var total = 0
            var failedCategories: [String] = []

            for (cat, types) in HealthDataTypes.quantityTypesByCategory {
                let catID = "qty_\(cat.rawValue)"
                try Task.checkCancellation()

                syncState.updateCategory(catID, status: .syncing)
                var catDelta = 0
                var failedTypes: [String] = []
                var activeMySQL = try await liveMySQL()
                for typeDesc in types {
                    try Task.checkCancellation()
                    do {
                        catDelta += try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: querySince)
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch where SyncService.isConnectionError(error) {
                        // Connection dropped (e.g. screen locked) — reconnect and retry once
                        do {
                            activeMySQL = try await liveMySQL()
                            catDelta += try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: querySince)
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {
                            failedTypes.append(typeDesc.displayName)
                        }
                    } catch {
                        if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                            // Device is locked — silently skip this type, don't mark category as failed
                        } else {
                            failedTypes.append(typeDesc.displayName)
                        }
                    }
                }
                let existing = syncState.categories.first(where: { $0.id == catID })?.recordCount ?? 0
                if failedTypes.isEmpty {
                    syncState.updateCategory(catID, status: .completed, recordCount: existing + catDelta, lastSyncDate: Date())
                } else {
                    failedCategories.append(cat.rawValue)
                    syncState.updateCategory(catID,
                        status: .failed("Failed types: \(failedTypes.joined(separator: ", "))"),
                        recordCount: existing + catDelta, lastSyncDate: Date())
                }
                total += catDelta
                updateLiveActivity(phase: cat.rawValue, operation: "Synced \(cat.rawValue) (\(catDelta) records)", records: total)
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_category", status: .syncing)
            do {
                var m = try await liveMySQL()
                let catCount: Int
                do {
                    catCount = try await syncCategorySamples(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    catCount = try await syncCategorySamples(mysql: m, since: querySince)
                }
                let existingCat = syncState.categories.first(where: { $0.id == "cat_category" })?.recordCount ?? 0
                syncState.updateCategory("cat_category", status: .completed, recordCount: existingCat + catCount, lastSyncDate: Date())
                total += catCount
                updateLiveActivity(phase: "Health Events", operation: "Synced Health Events (\(catCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Category Samples")
                    syncState.updateCategory("cat_category", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_workouts", status: .syncing)
            do {
                var m = try await liveMySQL()
                let workoutCount: Int
                do {
                    workoutCount = try await syncWorkouts(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    workoutCount = try await syncWorkouts(mysql: m, since: querySince)
                }
                let existingWorkouts = syncState.categories.first(where: { $0.id == "cat_workouts" })?.recordCount ?? 0
                syncState.updateCategory("cat_workouts", status: .completed, recordCount: existingWorkouts + workoutCount, lastSyncDate: Date())
                total += workoutCount
                updateLiveActivity(phase: "Workouts", operation: "Synced Workouts (\(workoutCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Workouts")
                    syncState.updateCategory("cat_workouts", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_bp", status: .syncing)
            do {
                var m = try await liveMySQL()
                let bpCount: Int
                do {
                    bpCount = try await syncBloodPressure(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    bpCount = try await syncBloodPressure(mysql: m, since: querySince)
                }
                let existingBP = syncState.categories.first(where: { $0.id == "cat_bp" })?.recordCount ?? 0
                syncState.updateCategory("cat_bp", status: .completed, recordCount: existingBP + bpCount, lastSyncDate: Date())
                total += bpCount
                updateLiveActivity(phase: "Blood Pressure", operation: "Synced Blood Pressure (\(bpCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Blood Pressure")
                    syncState.updateCategory("cat_bp", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_ecg", status: .syncing)
            do {
                var m = try await liveMySQL()
                let ecgCount: Int
                do {
                    ecgCount = try await syncECG(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    ecgCount = try await syncECG(mysql: m, since: querySince)
                }
                let existingECG = syncState.categories.first(where: { $0.id == "cat_ecg" })?.recordCount ?? 0
                syncState.updateCategory("cat_ecg", status: .completed, recordCount: existingECG + ecgCount, lastSyncDate: Date())
                total += ecgCount
                updateLiveActivity(phase: "ECG", operation: "Synced ECG (\(ecgCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("ECG")
                    syncState.updateCategory("cat_ecg", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_audiogram", status: .syncing)
            do {
                var m = try await liveMySQL()
                let audioCount: Int
                do {
                    audioCount = try await syncAudiograms(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    audioCount = try await syncAudiograms(mysql: m, since: querySince)
                }
                let existingAudio = syncState.categories.first(where: { $0.id == "cat_audiogram" })?.recordCount ?? 0
                syncState.updateCategory("cat_audiogram", status: .completed, recordCount: existingAudio + audioCount, lastSyncDate: Date())
                total += audioCount
                updateLiveActivity(phase: "Audiograms", operation: "Synced Audiograms (\(audioCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Audiograms")
                    syncState.updateCategory("cat_audiogram", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_activity_summaries", status: .syncing)
            do {
                var m = try await liveMySQL()
                let activityCount: Int
                do {
                    activityCount = try await syncActivitySummaries(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    activityCount = try await syncActivitySummaries(mysql: m, since: querySince)
                }
                let existingActivity = syncState.categories.first(where: { $0.id == "cat_activity_summaries" })?.recordCount ?? 0
                syncState.updateCategory("cat_activity_summaries", status: .completed, recordCount: existingActivity + activityCount, lastSyncDate: Date())
                total += activityCount
                updateLiveActivity(phase: "Activity Rings", operation: "Synced Activity Rings (\(activityCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Activity Summaries")
                    syncState.updateCategory("cat_activity_summaries", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_workout_routes", status: .syncing)
            do {
                var m = try await liveMySQL()
                let routeCount: Int
                do {
                    routeCount = try await syncWorkoutRoutes(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    routeCount = try await syncWorkoutRoutes(mysql: m, since: querySince)
                }
                let existingRoutes = syncState.categories.first(where: { $0.id == "cat_workout_routes" })?.recordCount ?? 0
                syncState.updateCategory("cat_workout_routes", status: .completed, recordCount: existingRoutes + routeCount, lastSyncDate: Date())
                total += routeCount
                updateLiveActivity(phase: "Workout Routes", operation: "Synced Workout Routes (\(routeCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Workout Routes")
                    syncState.updateCategory("cat_workout_routes", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_medications", status: .syncing)
            do {
                var m = try await liveMySQL()
                let medCount: Int
                do {
                    medCount = try await syncMedications(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    medCount = try await syncMedications(mysql: m, since: querySince)
                }
                let existingMeds = syncState.categories.first(where: { $0.id == "cat_medications" })?.recordCount ?? 0
                syncState.updateCategory("cat_medications", status: .completed, recordCount: existingMeds + medCount, lastSyncDate: Date())
                total += medCount
                updateLiveActivity(phase: "Medications", operation: "Synced Medications (\(medCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Medications")
                    syncState.updateCategory("cat_medications", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_vision", status: .syncing)
            do {
                var m = try await liveMySQL()
                let visionCount: Int
                do {
                    visionCount = try await syncVisionPrescriptions(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    visionCount = try await syncVisionPrescriptions(mysql: m, since: querySince)
                }
                let existingVision = syncState.categories.first(where: { $0.id == "cat_vision" })?.recordCount ?? 0
                syncState.updateCategory("cat_vision", status: .completed, recordCount: existingVision + visionCount, lastSyncDate: Date())
                total += visionCount
                updateLiveActivity(phase: "Vision", operation: "Synced Vision Prescriptions (\(visionCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("Vision Prescriptions")
                    syncState.updateCategory("cat_vision", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            try Task.checkCancellation()
            syncState.updateCategory("cat_state_of_mind", status: .syncing)
            do {
                var m = try await liveMySQL()
                let somCount: Int
                do {
                    somCount = try await syncStateOfMind(mysql: m, since: querySince)
                } catch where SyncService.isConnectionError(error) {
                    m = try await liveMySQL()
                    somCount = try await syncStateOfMind(mysql: m, since: querySince)
                }
                let existingSOM = syncState.categories.first(where: { $0.id == "cat_state_of_mind" })?.recordCount ?? 0
                syncState.updateCategory("cat_state_of_mind", status: .completed, recordCount: existingSOM + somCount, lastSyncDate: Date())
                total += somCount
                updateLiveActivity(phase: "State of Mind", operation: "Synced State of Mind (\(somCount) records)", records: total)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !(isBackgroundSync && (error as? HKError)?.code == .errorDatabaseInaccessible) {
                    failedCategories.append("State of Mind")
                    syncState.updateCategory("cat_state_of_mind", status: .failed(error.localizedDescription), lastSyncDate: Date())
                }
            }

            if !failedCategories.isEmpty {
                syncState.errorMessage = "Sync completed with errors in: \(failedCategories.joined(separator: ", "))"
            }

            let finalMySQL = try await liveMySQL()
            try await completeSyncLog(mysql: finalMySQL, id: logID, count: total)
            syncState.lastSyncDate = Date()
            syncState.currentOperation = "Incremental sync done (\(total) records)"
            if !isBackgroundSync { syncState.persist() }
            endLiveActivity(totalRecords: total)
            disconnectMySQL()

        } catch is CancellationError {
            if let m = self.mysql, logID != 0 { await failSyncLog(mysql: m, id: logID, message: "Sync cancelled") }
            disconnectMySQL()
            endLiveActivity(totalRecords: 0)
            syncState.currentOperation = "Sync cancelled"
            if !isBackgroundSync { syncState.persist() }
        } catch {
            if let m = self.mysql, logID != 0 { await failSyncLog(mysql: m, id: logID, message: error.localizedDescription) }
            disconnectMySQL()
            endLiveActivity(totalRecords: 0)
            syncState.errorMessage = error.localizedDescription
            syncState.currentOperation = ""
            if !isBackgroundSync { syncState.persist() }
        }

        syncState.isIncrementalSyncRunning = false
    }

    // MARK: - Activity summary sync

    private func syncActivitySummaries(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let summaries = try await healthKit.fetchActivitySummaries(from: since, until: until)
        guard !summaries.isEmpty else { return 0 }

        let calendar = Calendar.current
        var total = 0

        for batch in summaries.chunked(into: batchSize) {
            let sql: String? = autoreleasepool {
                let values: [String] = batch.compactMap { summary in
                    guard let date = calendar.date(from: summary.dateComponents(for: calendar)) else { return nil }
                    let dateStr = MySQLEscape.quote(
                        DateFormatter.mysqlDate.string(from: date)
                    )
                    let activeEnergy = summary.activeEnergyBurned.doubleValue(for: .kilocalorie())
                    let activeEnergyGoal = summary.activeEnergyBurnedGoal.doubleValue(for: .kilocalorie())
                    let exerciseTime = summary.appleExerciseTime.doubleValue(for: .minute())
                    let exerciseTimeGoal = summary.appleExerciseTimeGoal.doubleValue(for: .minute())
                    let standHoursDouble = summary.appleStandHours.doubleValue(for: .count())
                    let standHoursGoalDouble = summary.appleStandHoursGoal.doubleValue(for: .count())

                    guard activeEnergy.isFinite, activeEnergyGoal.isFinite,
                          exerciseTime.isFinite, exerciseTimeGoal.isFinite,
                          standHoursDouble.isFinite, standHoursGoalDouble.isFinite else { return nil }

                    let standHours = Int(standHoursDouble)
                    let standHoursGoal = Int(standHoursGoalDouble)
                    return "(\(dateStr), \(activeEnergy), \(activeEnergyGoal), \(exerciseTime), \(exerciseTimeGoal), \(standHours), \(standHoursGoal))"
                }
                guard !values.isEmpty else { return nil }
                return """
                INSERT INTO health_activity_summaries
                  (date, active_energy_burned, active_energy_burned_goal,
                   exercise_time_minutes, exercise_time_goal_minutes,
                   stand_hours, stand_hours_goal)
                VALUES \(values.joined(separator: ","))
                ON DUPLICATE KEY UPDATE
                  active_energy_burned = VALUES(active_energy_burned),
                  active_energy_burned_goal = VALUES(active_energy_burned_goal),
                  exercise_time_minutes = VALUES(exercise_time_minutes),
                  exercise_time_goal_minutes = VALUES(exercise_time_goal_minutes),
                  stand_hours = VALUES(stand_hours),
                  stand_hours_goal = VALUES(stand_hours_goal)
                """
            }
            if let sql {
                try Task.checkCancellation()
                try await mysql.execute(sql)
                total += batch.count
            }
        }
        return total
    }

    // MARK: - Workout route sync

    private func syncWorkoutRoutes(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        var total = 0
        try await healthKit.streamWorkouts(from: since, until: until) { [self] workouts in
            for workout in workouts {
                let routes: [HKWorkoutRoute]
                do {
                    routes = try await healthKit.fetchWorkoutRoutes(for: workout)
                } catch {
                    // Some workouts don't have routes or access is denied — skip
                    continue
                }
                for route in routes {
                    try Task.checkCancellation()
                    let locations: [CLLocation]
                    do {
                        locations = try await healthKit.fetchRouteLocations(for: route)
                    } catch {
                        continue
                    }
                    guard !locations.isEmpty else { continue }

                    let uuid        = MySQLEscape.quote(route.uuid.uuidString)
                    let workoutUUID = MySQLEscape.quote(workout.uuid.uuidString)
                    let startDate   = MySQLEscape.quote(sqlDate(route.startDate))
                    let count       = locations.count

                    let locJSON = locations.map { loc -> String in
                        let ts = MySQLEscape.escapeString(sqlDate(loc.timestamp))
                        return "{\"ts\":\"\(ts)\",\"lat\":\(loc.coordinate.latitude),\"lng\":\(loc.coordinate.longitude),\"alt\":\(loc.altitude),\"hacc\":\(loc.horizontalAccuracy),\"vacc\":\(loc.verticalAccuracy)}"
                    }.joined(separator: ",")
                    let locJSONQuoted = MySQLEscape.quote("[\(locJSON)]")

                    let sql = """
                    INSERT IGNORE INTO health_workout_routes
                      (uuid, workout_uuid, start_date, location_count, locations_json)
                    VALUES (\(uuid), \(workoutUUID), \(startDate), \(count), \(locJSONQuoted))
                    """
                    try await mysql.execute(sql)
                    total += 1
                }
            }
        }
        return total
    }

    // MARK: - Medication sync

    private func syncMedications(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        if #available(iOS 26, *) {
            return try await syncMedicationsIOS26(mysql: mysql, since: since, until: until)
        }
        return 0
    }

    @available(iOS 26, *)
    private func syncMedicationsIOS26(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        var total = 0
        let medications = (try? await healthKit.fetchUserAnnotatedMedications()) ?? []

        if medications.isEmpty {
            // No medications authorized — sync events without names
            let events = try await healthKit.fetchMedicationDoseEvents(from: since, until: until)
            for event in events {
                try Task.checkCancellation()
                total += try await insertMedicationDoseEvent(event, medicationName: nil, mysql: mysql)
            }
            return total
        }

        // Iterate per medication so HealthKit's predicate engine resolves concept identifiers
        // correctly. HKHealthConceptIdentifier does not override isEqual:, so Swift == always
        // returns false — predicate-based filtering is the only reliable matching approach.
        for annotated in medications {
            let concept = annotated.medication
            let conceptPredicate = NSPredicate(
                format: "%K == %@",
                HKPredicateKeyPathMedicationConceptIdentifier,
                concept.identifier
            )
            let events = try await healthKit.fetchMedicationDoseEvents(
                from: since,
                until: until,
                additionalPredicate: conceptPredicate
            )
            for event in events {
                try Task.checkCancellation()
                total += try await insertMedicationDoseEvent(event, medicationName: concept.displayText, mysql: mysql)
            }
        }
        return total
    }

    @available(iOS 26, *)
    private func insertMedicationDoseEvent(
        _ event: HKMedicationDoseEvent,
        medicationName: String?,
        mysql: MySQLService
    ) async throws -> Int {
        let uuid      = MySQLEscape.quote(event.uuid.uuidString)
        let medName   = MySQLEscape.quote(medicationName)
        let dosage    = event.doseQuantity.map { MySQLEscape.quote("\($0) \(event.unit.unitString)") } ?? "NULL"
        let logStatus = MySQLEscape.quote(logStatusString(event.logStatus))
        let start     = MySQLEscape.quote(sqlDate(event.startDate))
        let end       = MySQLEscape.quote(sqlDate(event.endDate))
        let src       = MySQLEscape.quote(event.sourceRevision.source.name)
        let bundle    = MySQLEscape.quote(event.sourceRevision.source.bundleIdentifier)
        let device    = MySQLEscape.quote(event.device?.name)

        let sql = """
        INSERT IGNORE INTO health_medications
          (uuid,medication_name,dosage,log_status,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
        VALUES (\(uuid),\(medName),\(dosage),\(logStatus),\(start),\(end),\(src),\(bundle),\(device),NULL)
        """
        try await mysql.execute(sql)
        return 1
    }

    @available(iOS 26, *)
    private func logStatusString(_ status: HKMedicationDoseEvent.LogStatus) -> String {
        switch status {
        case .taken:               return "taken"
        case .skipped:             return "skipped"
        case .snoozed:             return "snoozed"
        case .notInteracted:       return "notInteracted"
        case .notificationNotSent: return "notificationNotSent"
        case .notLogged:           return "notLogged"
        @unknown default:          return "unknown"
        }
    }

    // MARK: - Sync log helpers

    private func startSyncLog(mysql: MySQLService, category: String) async throws -> Int64 {
        try await mysql.execute(
            "INSERT INTO health_sync_log (category, started_at, status) VALUES ('\(MySQLEscape.escapeString(category))', NOW(), 'running')"
        )
        let rows = try await mysql.query("SELECT LAST_INSERT_ID() as id")
        return rows.first?["id"].flatMap(Int64.init) ?? 0
    }

    private func completeSyncLog(mysql: MySQLService, id: Int64, count: Int) async throws {
        try await mysql.execute(
            "UPDATE health_sync_log SET status='completed', records_synced=\(count), completed_at=NOW() WHERE id=\(id)"
        )
    }

    private func failSyncLog(mysql: MySQLService, id: Int64, message: String) async {
        _ = try? await mysql.execute(
            "UPDATE health_sync_log SET status='failed', completed_at=NOW(), error_message='\(MySQLEscape.escapeString(message))' WHERE id=\(id)"
        )
    }

    /// Marks any leftover 'running' entries as 'failed'. Called at the start of each new sync
    /// to clean up entries that were never closed due to interruptions (screen lock, crash, etc.).
    private func cleanupStaleLogEntries(mysql: MySQLService) async {
        _ = try? await mysql.execute(
            "UPDATE health_sync_log SET status='failed', completed_at=NOW(), error_message='Interrupted' WHERE status='running'"
        )
    }

    private func lastCompletedSyncDate(mysql: MySQLService) async throws -> Date? {
        let rows = try await mysql.query(
            "SELECT completed_at FROM health_sync_log WHERE status='completed' ORDER BY completed_at DESC LIMIT 1"
        )
        guard let dateStr = rows.first?["completed_at"] else { return nil }
        return sqlDateFormatter.date(from: dateStr)
    }

    // MARK: - Quantity sync

    // Streams HealthKit samples in pages using cursor-based HKSampleQuery pagination,
    // inserting each page before requesting the next. Peak memory stays flat regardless
    // of total record count. Uses INSERT IGNORE to safely handle any overlap between pages.
    private func syncQuantityType(
        typeDesc: QuantityTypeDescriptor,
        mysql: MySQLService,
        since: Date?,
        until: Date? = nil,
        insertBatchSize: Int = batchSize,
        onBatchInserted: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let typeName = MySQLEscape.escapeString(typeDesc.id)
        let unitStr  = MySQLEscape.escapeString(typeDesc.unitString)
        var total = 0

        try await healthKit.streamQuantitySamples(typeID: typeDesc.hkIdentifier, from: since, until: until) { hkBatch in
            for batch in hkBatch.chunked(into: insertBatchSize) {
                let sql: String = autoreleasepool {
                    let valuesList = batch.map { s -> String in
                        let uuid   = MySQLEscape.quote(s.uuid.uuidString)
                        let value  = MySQLEscape.quoteDouble(s.quantity.doubleValue(for: typeDesc.unit))
                        let start  = MySQLEscape.quote(sqlDate(s.startDate))
                        let end    = MySQLEscape.quote(sqlDate(s.endDate))
                        let src    = MySQLEscape.quote(s.sourceDisplayName)
                        let bundle = MySQLEscape.quote(s.sourceBundleID)
                        let device = MySQLEscape.quote(s.deviceName)
                        let meta   = MySQLEscape.quote(s.jsonMetadata())
                        return "(\(uuid),'\(typeName)',\(value),'\(unitStr)',\(start),\(end),\(src),\(bundle),\(device),\(meta))"
                    }.joined(separator: ",")
                    return """
                    INSERT IGNORE INTO health_quantity_samples
                      (uuid,type,value,unit,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
                    VALUES \(valuesList)
                    """
                }
                try Task.checkCancellation()
                try await mysql.execute(sql)
                total += batch.count
                onBatchInserted?(total)
            }
        }
        return total
    }

    // MARK: - Category sync

    private func syncCategorySamples(mysql: MySQLService, since: Date?, until: Date? = nil, insertBatchSize: Int = batchSize) async throws -> Int {
        var total = 0
        for typeDesc in HealthDataTypes.allCategoryTypes {
            let typeName = MySQLEscape.escapeString(typeDesc.id)
            try await healthKit.streamCategorySamples(typeID: typeDesc.hkIdentifier, from: since, until: until) { hkBatch in
                for batch in hkBatch.chunked(into: insertBatchSize) {
                    let sql: String = autoreleasepool {
                        let values = batch.map { s -> String in
                            let uuid   = MySQLEscape.quote(s.uuid.uuidString)
                            let value  = s.value
                            let label  = MySQLEscape.quote(typeDesc.valueLabels[value] ?? "\(value)")
                            let start  = MySQLEscape.quote(sqlDate(s.startDate))
                            let end    = MySQLEscape.quote(sqlDate(s.endDate))
                            let src    = MySQLEscape.quote(s.sourceDisplayName)
                            let bundle = MySQLEscape.quote(s.sourceBundleID)
                            let device = MySQLEscape.quote(s.deviceName)
                            return "(\(uuid),'\(typeName)',\(value),\(label),\(start),\(end),\(src),\(bundle),\(device),NULL)"
                        }.joined(separator: ",")
                        return """
                        INSERT IGNORE INTO health_category_samples
                          (uuid,type,value,value_label,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
                        VALUES \(values)
                        """
                    }
                    try Task.checkCancellation()
                    try await mysql.execute(sql)
                    total += batch.count
                }
            }
        }
        return total
    }

    // MARK: - Workout sync

    private func syncWorkouts(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        var total = 0
        try await healthKit.streamWorkouts(from: since, until: until) { workouts in
            for batch in workouts.chunked(into: batchSize) {
                let sql: String = autoreleasepool {
                    let values = batch.map { w -> String in
                        let uuid     = MySQLEscape.quote(w.uuid.uuidString)
                        let actType  = MySQLEscape.quote(w.activityTypeName)
                        let duration = w.duration
                        let energy   = MySQLEscape.quoteDouble(w.totalEnergyBurned?.doubleValue(for: .kilocalorie()))
                        let distance = MySQLEscape.quoteDouble(w.totalDistance?.doubleValue(for: .meter()))
                        let strokes  = MySQLEscape.quoteDouble(w.totalSwimmingStrokeCount?.doubleValue(for: .count()))
                        let flights  = MySQLEscape.quoteDouble(w.totalFlightsClimbed?.doubleValue(for: .count()))
                        let start    = MySQLEscape.quote(sqlDate(w.startDate))
                        let end      = MySQLEscape.quote(sqlDate(w.endDate))
                        let src      = MySQLEscape.quote(w.sourceDisplayName)
                        let bundle   = MySQLEscape.quote(w.sourceBundleID)
                        let device   = MySQLEscape.quote(w.deviceName)
                        return "(\(uuid),\(actType),\(duration),\(energy),\(distance),\(strokes),\(flights),\(start),\(end),\(src),\(bundle),\(device),NULL)"
                    }.joined(separator: ",")
                    return """
                    INSERT IGNORE INTO health_workouts
                      (uuid,activity_type,duration_seconds,total_energy_burned_kcal,total_distance_meters,
                       total_swimming_strokes,total_flights_climbed,start_date,end_date,
                       source_name,source_bundle_id,device_name,metadata)
                    VALUES \(values)
                    """
                }
                try Task.checkCancellation()
                try await mysql.execute(sql)
                total += batch.count
            }
        }
        return total
    }

    // MARK: - Blood pressure sync

    private func syncBloodPressure(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let correlations = try await healthKit.fetchBloodPressure(from: since, until: until)
        guard !correlations.isEmpty else { return 0 }

        var total = 0
        let systolicType = HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic)!
        let diastolicType = HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic)!

        for batch in correlations.chunked(into: batchSize) {
            let values: [String] = batch.compactMap { corr -> String? in
                guard
                    let sys = (corr.objects(for: systolicType) as? Set<HKQuantitySample>)?.first,
                    let dia = (corr.objects(for: diastolicType) as? Set<HKQuantitySample>)?.first
                else { return nil }

                let uuid     = MySQLEscape.quote(corr.uuid.uuidString)
                let sysVal   = sys.quantity.doubleValue(for: .millimeterOfMercury())
                let diaVal   = dia.quantity.doubleValue(for: .millimeterOfMercury())
                let start    = MySQLEscape.quote(sqlDate(corr.startDate))
                let src      = MySQLEscape.quote(corr.sourceRevision.source.name)
                let device   = MySQLEscape.quote(corr.device?.name)
                return "(\(uuid),\(sysVal),\(diaVal),\(start),\(src),\(device),NULL)"
            }

            if values.isEmpty { continue }
            let sql = """
            INSERT IGNORE INTO health_blood_pressure
              (uuid,systolic,diastolic,start_date,source_name,device_name,metadata)
            VALUES \(values.joined(separator: ","))
            """
            try await mysql.execute(sql)
            total += values.count
        }
        return total
    }

    // MARK: - ECG sync

    private func syncECG(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let recordings = try await healthKit.fetchECG(from: since, until: until)
        guard !recordings.isEmpty else { return 0 }

        var total = 0
        for ecg in recordings {
            let uuid   = MySQLEscape.quote(ecg.uuid.uuidString)
            let cls    = MySQLEscape.quote(ecg.classification.label)
            let avgHR  = MySQLEscape.quoteDouble(ecg.averageHeartRate?.doubleValue(for: HKUnit(from: "count/min")))
            let freq   = MySQLEscape.quoteDouble(ecg.samplingFrequency?.doubleValue(for: HKUnit(from: "Hz")))
            let start  = MySQLEscape.quote(sqlDate(ecg.startDate))
            let src    = MySQLEscape.quote(ecg.sourceRevision.source.name)

            // Fetch voltage measurements (Apple Watch ECG uses Lead I equivalent)
            let voltages = try await healthKit.fetchECGVoltageMeasurements(for: ecg)
            let voltageJSON: String
            if voltages.isEmpty {
                voltageJSON = "NULL"
            } else {
                let mvUnit = HKUnit(from: "mV")
                let arr = voltages.compactMap { v -> String? in
                    guard let q = v.quantity(for: .appleWatchSimilarToLeadI) else { return nil }
                    return String(format: "%.6f", q.doubleValue(for: mvUnit))
                }
                voltageJSON = arr.isEmpty ? "NULL" : MySQLEscape.quote("[\(arr.joined(separator: ","))]")
            }

            let sql = """
            INSERT IGNORE INTO health_ecg
              (uuid,classification,average_heart_rate,sampling_frequency,voltage_measurements,start_date,source_name,metadata)
            VALUES (\(uuid),\(cls),\(avgHR),\(freq),\(voltageJSON),\(start),\(src),NULL)
            """
            try await mysql.execute(sql)
            total += 1
        }
        return total
    }

    // MARK: - Audiogram sync

    private func syncAudiograms(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let audiograms = try await healthKit.fetchAudiograms(from: since, until: until)
        guard !audiograms.isEmpty else { return 0 }

        var total = 0
        for ag in audiograms {
            let uuid  = MySQLEscape.quote(ag.uuid.uuidString)
            let start = MySQLEscape.quote(sqlDate(ag.startDate))
            let src   = MySQLEscape.quote(ag.sourceRevision.source.name)

            let points = ag.sensitivityPoints.map { pt -> String in
                let freq = pt.frequency.doubleValue(for: .hertz())
                let leftDB  = pt.leftEarSensitivity?.doubleValue(for: HKUnit.decibelHearingLevel()) ?? 0
                let rightDB = pt.rightEarSensitivity?.doubleValue(for: HKUnit.decibelHearingLevel()) ?? 0
                return "{\"hz\":\(freq),\"l\":\(leftDB),\"r\":\(rightDB)}"
            }
            let jsonStr = MySQLEscape.quote("[\(points.joined(separator: ","))]")

            let sql = """
            INSERT IGNORE INTO health_audiograms
              (uuid,sensitivity_points,start_date,source_name,metadata)
            VALUES (\(uuid),\(jsonStr),\(start),\(src),NULL)
            """
            try await mysql.execute(sql)
            total += 1
        }
        return total
    }

    // MARK: - Vision prescription sync

    private func syncVisionPrescriptions(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let prescriptions = try await healthKit.fetchVisionPrescriptions(from: since, until: until)
        guard !prescriptions.isEmpty else { return 0 }

        // Diopters (sphere/cylinder/addPower), degrees (axis), millimeters (baseCurve/diameter)
        let diopterUnit = HKUnit(from: "D")
        let degreeUnit  = HKUnit.count()
        let mmUnit      = HKUnit.meterUnit(with: .milli)

        var total = 0
        for p in prescriptions {
            let uuid     = MySQLEscape.quote(p.uuid.uuidString)
            let start    = MySQLEscape.quote(sqlDate(p.startDate))
            let end      = MySQLEscape.quote(sqlDate(p.endDate))
            let prescType = p.prescriptionType.rawValue
            let expiry   = p.expirationDate.map { MySQLEscape.quote(sqlDate($0)) } ?? "NULL"
            let src      = MySQLEscape.quote(p.sourceRevision.source.name)
            let bundle   = MySQLEscape.quote(p.sourceRevision.source.bundleIdentifier)
            let device   = MySQLEscape.quote(p.device?.name)

            var rSphere = "NULL", rCyl = "NULL", rAxis = "NULL", rAdd = "NULL"
            var rBase = "NULL", rDiam = "NULL"
            var lSphere = "NULL", lCyl = "NULL", lAxis = "NULL", lAdd = "NULL"
            var lBase = "NULL", lDiam = "NULL"

            if let glasses = p as? HKGlassesPrescription {
                if let r = glasses.rightEye {
                    rSphere = MySQLEscape.quoteDouble(r.sphere.doubleValue(for: diopterUnit))
                    rCyl    = MySQLEscape.quoteDouble(r.cylinder?.doubleValue(for: diopterUnit))
                    rAxis   = MySQLEscape.quoteDouble(r.axis?.doubleValue(for: degreeUnit))
                    rAdd    = MySQLEscape.quoteDouble(r.addPower?.doubleValue(for: diopterUnit))
                }
                if let l = glasses.leftEye {
                    lSphere = MySQLEscape.quoteDouble(l.sphere.doubleValue(for: diopterUnit))
                    lCyl    = MySQLEscape.quoteDouble(l.cylinder?.doubleValue(for: diopterUnit))
                    lAxis   = MySQLEscape.quoteDouble(l.axis?.doubleValue(for: degreeUnit))
                    lAdd    = MySQLEscape.quoteDouble(l.addPower?.doubleValue(for: diopterUnit))
                }
            } else if let contacts = p as? HKContactsPrescription {
                if let r = contacts.rightEye {
                    rSphere = MySQLEscape.quoteDouble(r.sphere.doubleValue(for: diopterUnit))
                    rCyl    = MySQLEscape.quoteDouble(r.cylinder?.doubleValue(for: diopterUnit))
                    rAxis   = MySQLEscape.quoteDouble(r.axis?.doubleValue(for: degreeUnit))
                    rAdd    = MySQLEscape.quoteDouble(r.addPower?.doubleValue(for: diopterUnit))
                    rBase   = MySQLEscape.quoteDouble(r.baseCurve?.doubleValue(for: mmUnit))
                    rDiam   = MySQLEscape.quoteDouble(r.diameter?.doubleValue(for: mmUnit))
                }
                if let l = contacts.leftEye {
                    lSphere = MySQLEscape.quoteDouble(l.sphere.doubleValue(for: diopterUnit))
                    lCyl    = MySQLEscape.quoteDouble(l.cylinder?.doubleValue(for: diopterUnit))
                    lAxis   = MySQLEscape.quoteDouble(l.axis?.doubleValue(for: degreeUnit))
                    lAdd    = MySQLEscape.quoteDouble(l.addPower?.doubleValue(for: diopterUnit))
                    lBase   = MySQLEscape.quoteDouble(l.baseCurve?.doubleValue(for: mmUnit))
                    lDiam   = MySQLEscape.quoteDouble(l.diameter?.doubleValue(for: mmUnit))
                }
            }

            let sql = """
            INSERT IGNORE INTO health_vision_prescriptions
              (uuid,start_date,end_date,prescription_type,
               right_eye_sphere,right_eye_cylinder,right_eye_axis,right_eye_add_power,
               right_eye_base_curve,right_eye_diameter,
               left_eye_sphere,left_eye_cylinder,left_eye_axis,left_eye_add_power,
               left_eye_base_curve,left_eye_diameter,
               expiration_date,source_name,source_bundle_id,device_name)
            VALUES (\(uuid),\(start),\(end),\(prescType),
                    \(rSphere),\(rCyl),\(rAxis),\(rAdd),\(rBase),\(rDiam),
                    \(lSphere),\(lCyl),\(lAxis),\(lAdd),\(lBase),\(lDiam),
                    \(expiry),\(src),\(bundle),\(device))
            """
            try Task.checkCancellation()
            try await mysql.execute(sql)
            total += 1
        }
        return total
    }

    // MARK: - State of Mind sync

    private func syncStateOfMind(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        if #available(iOS 18, *) {
            return try await syncStateOfMindIOS18(mysql: mysql, since: since, until: until)
        }
        return 0
    }

    @available(iOS 18, *)
    private func syncStateOfMindIOS18(mysql: MySQLService, since: Date?, until: Date? = nil) async throws -> Int {
        let samples = try await healthKit.fetchStateOfMind(from: since, until: until)
        guard !samples.isEmpty else { return 0 }

        var total = 0
        for sample in samples {
            let uuid         = MySQLEscape.quote(sample.uuid.uuidString)
            let start        = MySQLEscape.quote(sqlDate(sample.startDate))
            let end          = MySQLEscape.quote(sqlDate(sample.endDate))
            let kind         = sample.kind.rawValue
            let valence      = sample.valence
            let valenceClass = sample.valenceClassification.rawValue
            let src          = MySQLEscape.quote(sample.sourceRevision.source.name)
            let bundle       = MySQLEscape.quote(sample.sourceRevision.source.bundleIdentifier)
            let device       = MySQLEscape.quote(sample.device?.name)

            let labelInts = sample.labels.map { $0.rawValue }
            let assocInts = sample.associations.map { $0.rawValue }
            let labelsJSON = (try? JSONSerialization.data(withJSONObject: labelInts))
                .flatMap { String(data: $0, encoding: .utf8) }
            let assocJSON = (try? JSONSerialization.data(withJSONObject: assocInts))
                .flatMap { String(data: $0, encoding: .utf8) }

            let sql = """
            INSERT IGNORE INTO health_state_of_mind
              (uuid,start_date,end_date,kind,valence,valence_classification,
               labels_json,associations_json,source_name,source_bundle_id,device_name)
            VALUES (\(uuid),\(start),\(end),\(kind),\(valence),\(valenceClass),
                    \(MySQLEscape.quote(labelsJSON)),\(MySQLEscape.quote(assocJSON)),
                    \(src),\(bundle),\(device))
            """
            try Task.checkCancellation()
            try await mysql.execute(sql)
            total += 1
        }
        return total
    }
}

// MARK: - Sync prerequisite issues

enum SyncPrerequisiteIssue: Identifiable {
    case healthDataUnavailable
    case healthPermissionsNotRequested
    case somePermissionsDenied(count: Int)
    case missingDatabaseTables(tables: [String])
    case databaseConnectionFailed(String)

    var id: String {
        switch self {
        case .healthDataUnavailable: return "healthUnavailable"
        case .healthPermissionsNotRequested: return "permissionsNotRequested"
        case .somePermissionsDenied: return "permissionsDenied"
        case .missingDatabaseTables: return "missingTables"
        case .databaseConnectionFailed: return "dbConnectionFailed"
        }
    }

    var title: String {
        switch self {
        case .healthDataUnavailable:
            return "Health Data Unavailable"
        case .healthPermissionsNotRequested:
            return "Health Permissions Not Requested"
        case .somePermissionsDenied(let count):
            return "\(count) Health Permission(s) Denied"
        case .missingDatabaseTables(let tables):
            return "\(tables.count) Database Table(s) Missing"
        case .databaseConnectionFailed:
            return "Database Connection Failed"
        }
    }

    var message: String {
        switch self {
        case .healthDataUnavailable:
            return "HealthKit is not available on this device."
        case .healthPermissionsNotRequested:
            return "Go to Settings → Apple Health Permissions and request access to sync all your health data."
        case .somePermissionsDenied:
            return "Some health data types were denied. Go to Settings → Health Permissions to review and re-request missing permissions."
        case .missingDatabaseTables(let tables):
            return "Tables missing: \(tables.joined(separator: ", ")). Go to Settings → MySQL Connection → Initialize Schema to create them."
        case .databaseConnectionFailed(let err):
            return "Could not connect to MySQL: \(err). Check your connection settings."
        }
    }

    var actionLabel: String {
        switch self {
        case .healthDataUnavailable: return ""
        case .healthPermissionsNotRequested: return "Review Permissions"
        case .somePermissionsDenied: return "Review Permissions"
        case .missingDatabaseTables: return "Initialize Schema"
        case .databaseConnectionFailed: return "Check Settings"
        }
    }
}

// MARK: - DateFormatter helpers

extension DateFormatter {
    static let mysqlDate: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.timeZone = TimeZone(identifier: "UTC")
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()
}

// MARK: - Array chunking

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}

// MARK: - ECG classification label

extension HKElectrocardiogram.Classification {
    var label: String {
        switch self {
        case .notSet:                  return "Not Set"
        case .sinusRhythm:             return "Sinus Rhythm"
        case .atrialFibrillation:      return "Atrial Fibrillation"
        case .inconclusiveLowHeartRate: return "Inconclusive – Low HR"
        case .inconclusiveHighHeartRate: return "Inconclusive – High HR"
        case .inconclusivePoorReading:  return "Inconclusive – Poor Reading"
        case .inconclusiveOther:        return "Inconclusive"
        case .unrecognized:             return "Unrecognized"
        @unknown default:               return "Unknown"
        }
    }
}
