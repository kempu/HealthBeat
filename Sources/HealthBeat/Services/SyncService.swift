import ActivityKit
import BackgroundTasks
import CoreLocation
import CryptoKit
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

    /// Optional secondary destination that receives the same typed batches
    /// as MySQL. Today this is set to an `EABackendWriter` by
    /// `BackgroundSyncManager` when the user has enabled the EA destination
    /// in Settings. Set BEFORE `runIncrementalSync` / `runFullSync`;
    /// nil = MySQL-only. Errors thrown by the writer propagate up exactly
    /// like MySQL errors do — the offending pass is logged as failed and
    /// the cursors are NOT advanced, so the next pass retries cleanly.
    var eaWriter: BackendWriter?

    /// Convenience: attach an `EABackendWriter` so an INCREMENTAL pass mirrors
    /// every batch to EA in addition to MySQL. Attached only when EA is
    /// configured AND already has a full-sync baseline — incremental on top of
    /// an un-baselined EA would leave a permanent history gap, so an
    /// un-baselined EA is skipped here and surfaced as "needs full sync" in the
    /// UI instead. No-op when EA is not configured. (The full-sync path delivers
    /// to EA via the dump uploader, independent of this writer.)
    func attachEAIfConfigured() {
        let eaCfg = EAConfig.load()
        if eaCfg.isConfigured && syncState.eaBaselineDone {
            eaWriter = EABackendWriter(config: eaCfg)
        }
    }

    // When true, skip live activity and allow resumable sync across background task invocations
    var isBackgroundSync = false

    // When true, never create or update a Live Activity (used for observer-triggered real-time syncs)
    var suppressLiveActivity = false

    /// Set by the caller before a sync so the background-task expiry
    /// handler can cancel the Swift Task when iOS reclaims background time.
    var taskForCancellation: Task<Void, Never>?

    // Class-level flag so BackgroundSyncManager can check whether ANY SyncService instance
    // (foreground or background) is currently running, preventing concurrent MySQL connections
    // from competing for row locks on the same tables.
    @MainActor static private(set) var isSyncRunning = false {
        didSet {
            // Seed the activity timestamp on start; `noteSyncActivity` refreshes it as the
            // sync makes progress so a long-but-healthy run isn't mistaken for a stalled one.
            syncActivityDate = isSyncRunning ? Date() : nil
        }
    }
    /// Timestamp of the most recent sync progress (start, then refreshed by `noteSyncActivity`).
    @MainActor static private var syncActivityDate: Date?

    /// Heartbeat. Call from the sync loops on each unit of progress so a legitimately long
    /// sync (e.g. a multi-year full backfill) keeps `isSyncEffectivelyRunning` true and a
    /// second sync instance never opens a competing MySQL connection — the concurrency that
    /// produced "Lock wait timeout exceeded". No-op when no sync is running.
    @MainActor static func noteSyncActivity() {
        if isSyncRunning { syncActivityDate = Date() }
    }

    /// Lets the dump deliver phases (defined in `DumpDrain`/`DumpUpload`
    /// extensions, which can't reach the `private(set)` setter) participate in
    /// the global single-sync guard so a concurrent incremental sync doesn't
    /// open a competing MySQL connection.
    @MainActor static func setGlobalSyncRunning(_ running: Bool) { isSyncRunning = running }

    /// Tracks the last time a full 7-day lookback sweep was performed.
    /// Resets on app termination (non-persisted) — first sync after launch always does a full sweep.
    @MainActor private static var lastDeepSweepDate: Date = .distantPast

    /// Throttle for cleanupStaleLogEntries — no need to run every 3 minutes.
    @MainActor private static var lastStaleCleanup: Date = .distantPast

    /// Returns true if a sync is actively running. If no progress has been reported for longer
    /// than `timeout` seconds (e.g. a suspended/frozen/crashed task that never cleared the flag),
    /// it's considered stale and force-reset to false so background syncs aren't blocked
    /// indefinitely. Healthy syncs call `noteSyncActivity` well within `timeout`.
    @MainActor static func isSyncEffectivelyRunning(timeout: TimeInterval = 300) -> Bool {
        guard isSyncRunning else { return false }
        if let last = syncActivityDate, Date().timeIntervalSince(last) > timeout {
            print("[SyncService] Resetting stale isSyncRunning flag (last activity \(last))")
            isSyncRunning = false
            return false
        }
        return true
    }

    // MARK: - Adaptive lookback

    /// Computes the HealthKit query start date for a given type key.
    /// Routine syncs use a short lookback (1h if cursor is fresh); deep sweeps use the full 7-day
    /// lookback to catch late-arriving samples.
    private func adaptiveQuerySince(
        for key: String,
        cursors: [String: Date],
        globalQuerySince: Date,
        lastSync: Date?,
        forceDeepSweep: Bool
    ) -> Date {
        let distantPast = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
        guard let cursor = cursors[key] else { return globalQuerySince }
        let effective = max(cursor, lastSync ?? distantPast)

        if forceDeepSweep {
            return effective.addingTimeInterval(-7 * 86400)
        }

        let age = Date().timeIntervalSince(effective)
        let lookback: TimeInterval
        if age < 3600 {
            lookback = 3600       // cursor < 1h old -> look back 1h
        } else if age < 86400 {
            lookback = 86400      // cursor < 24h old -> look back 24h
        } else {
            lookback = 7 * 86400  // older -> full 7-day lookback
        }
        return effective.addingTimeInterval(-lookback)
    }

    /// Returns true if all type keys in the list have cursors younger than `threshold`.
    private func allCursorsRecent(_ keys: [String], cursors: [String: Date], threshold: TimeInterval = 300) -> Bool {
        let now = Date()
        return keys.allSatisfy { key in
            guard let cursor = cursors[key] else { return false }
            return now.timeIntervalSince(cursor) < threshold
        }
    }

    // MARK: - Sync unit (for priority sorting)

    private enum SyncUnit {
        case quantityGroup(HealthCategory, [QuantityTypeDescriptor])
        case categoryTypes
        case special(String)  // cat_workouts, cat_bp, cat_ecg, etc.

        var categoryID: String {
            switch self {
            case .quantityGroup(let cat, _): return "qty_\(cat.rawValue)"
            case .categoryTypes: return "cat_category"
            case .special(let id): return id
            }
        }

        var displayLabel: String {
            switch self {
            case .quantityGroup(let cat, _): return cat.rawValue
            case .categoryTypes: return "Health Events"
            case .special(let id):
                switch id {
                case "cat_workouts": return "Workouts"
                case "cat_bp": return "Blood Pressure"
                case "cat_ecg": return "ECG"
                case "cat_audiogram": return "Audiograms"
                case "cat_activity_summaries": return "Activity Rings"
                case "cat_workout_routes": return "Workout Routes"
                case "cat_medications": return "Medications"
                case "cat_vision": return "Vision"
                case "cat_state_of_mind": return "State of Mind"
                default: return id
                }
            }
        }
    }

    // Live Activity
    private var liveActivity: Activity<SyncActivityAttributes>?
    private var lastLiveActivityUpdate: Date = .distantPast
    /// Records exported in the most recent full-sync export — used for the final
    /// "Synced N records" live-activity state when delivery completes.
    private(set) var lastExportRecordCount = 0

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
            phase: "Starting",
            operation: isFullSync ? "Preparing full sync…" : "Starting sync…",
            recordsInserted: 0,
            isFullSync: isFullSync,
            progress: 0
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

    func updateLiveActivity(phase: String, operation: String, records: Int? = nil, progress: Double? = nil) {
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
            recordsInserted: records ?? activity.content.state.recordsInserted,
            isFullSync: isFullSync,
            progress: progress ?? syncState.overallProgress
        )
        let content = ActivityContent(state: state, staleDate: nil)
        // Await the update directly to ensure it completes before moving on
        Task { @MainActor in
            await activity.update(content)
        }
    }

    func endLiveActivity(totalRecords: Int) {
        guard !suppressLiveActivity else { return }
        let activity = liveActivity ?? Activity<SyncActivityAttributes>.activities.first
        guard let activity else { return }
        let isFullSync = syncState.isFullSyncRunning
        let finalState = SyncActivityAttributes.ContentState(
            phase: "Done",
            operation: "Synced \(totalRecords.formatted()) records",
            recordsInserted: totalRecords,
            isFullSync: isFullSync,
            progress: 1.0
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

        // Check database schema — only when the MySQL destination is enabled.
        if config.enabled {
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
        }

        return issues
    }

    // MARK: - Single-category sync

    /// Re-syncs one category over all history to the enabled destinations. Used
    /// by the per-category "Sync" button. Calls the leaf `sync*` methods directly
    /// over the full range (no windowing); `since: nil` means no reconcile and no
    /// UUID accumulation, so even a dense category stays memory-bounded. MySQL is
    /// skipped when its destination is disabled (EA still receives the batch).
    func runSingleCategorySync(categoryID: String, config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        let mysqlEnabled = config.enabled
        syncState.isFullSyncRunning = true
        syncState.fullSyncPhase = .idle   // per-category: recalcOverall drives progress
        SyncService.setGlobalSyncRunning(true)
        defer {
            SyncService.setGlobalSyncRunning(false)
            syncState.isFullSyncRunning = false
            syncState.currentSyncCategoryIDs = []
        }
        syncState.errorMessage = nil
        syncState.currentSyncCategoryIDs = [categoryID]
        syncState.currentOperation = "Connecting…"
        startLiveActivity(isFullSync: false)

        do {
            var mysql: MySQLService? = nil
            if mysqlEnabled {
                try await connectMySQL(config: config)
                guard let m = self.mysql else { throw MySQLError.disconnected }
                let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: m)
                if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }
                mysql = m
            }

            syncState.updateCategory(categoryID, status: .syncing)
            syncState.currentOperation = "Syncing…"

            let count: Int
            if categoryID.hasPrefix("qty_") {
                let rawCat = String(categoryID.dropFirst(4))
                guard let cat = HealthCategory(rawValue: rawCat),
                      let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == cat })?.1 else {
                    throw MySQLError.queryError(code: 0, message: "Unknown category: \(categoryID)")
                }
                var c = 0
                for typeDesc in types {
                    try Task.checkCancellation()
                    c += try await syncQuantityType(typeDesc: typeDesc, mysql: mysql, since: nil)
                }
                count = c
            } else {
                switch categoryID {
                case "cat_category":           count = try await syncCategorySamples(mysql: mysql, since: nil)
                case "cat_workouts":           count = try await syncWorkouts(mysql: mysql, since: nil)
                case "cat_bp":                 count = try await syncBloodPressure(mysql: mysql, since: nil)
                case "cat_ecg":                count = try await syncECG(mysql: mysql, since: nil)
                case "cat_audiogram":          count = try await syncAudiograms(mysql: mysql, since: nil)
                case "cat_activity_summaries": count = try await syncActivitySummaries(mysql: mysql, since: nil)
                case "cat_workout_routes":     count = try await syncWorkoutRoutes(mysql: mysql, since: nil)
                case "cat_medications":        count = try await syncMedications(mysql: mysql, since: nil)
                case "cat_vision":             count = try await syncVisionPrescriptions(mysql: mysql, since: nil)
                case "cat_state_of_mind":      count = try await syncStateOfMind(mysql: mysql, since: nil)
                default:                       count = 0
                }
            }

            syncState.updateCategory(categoryID, status: .completed, recordCount: count, lastSyncDate: Date())
            syncState.lastSyncDate = Date()
            syncState.currentOperation = ""
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
    }

    // MARK: - Phase 1: Encrypted local export (fast, screen-on)

    /// Phase 1 worker: streams every HealthKit table covered by full sync to
    /// encrypted local dump files, driving the existing per-category progress UI
    /// (status, per-type progress bar, live activity). The screen must stay on
    /// because HealthKit is read here — but only here; delivery needs no screen.
    /// Reuses the `sync*` methods with `mysql: nil`, so the dump captures exactly
    /// the `HB*Row` values the live EA path produces — no second mapping to drift.
    /// Does NOT manage the running flags (`runFullSync` owns them). Returns the
    /// run id, or nil on failure/cancel.
    private func exportDump() async -> String? {
        syncState.errorMessage = nil
        syncState.currentOperation = "Preparing export…"
        syncState.currentSyncCategoryIDs = Set(syncState.categories.map(\.id))
        // From-scratch full sync wipes the remote and re-uploads everything —
        // clear stale "Synced" badges + the old total so the screen isn't confusing.
        syncState.resetForFullSync()
        startLiveActivity(isFullSync: true)

        // HealthKit is only readable while unlocked — keep the screen awake for
        // the export, and only the export. Deliver phases never touch HealthKit.
        // Honors the user's "Keep screen on during sync" setting; SyncService is
        // the single authority for the idle timer (the dashboard must not fight it).
        if !isBackgroundSync && UserDefaults.standard.bool(forKey: "keepScreenOnDuringSync") {
            UIApplication.shared.isIdleTimerDisabled = true
        }
        defer { if !isBackgroundSync { UIApplication.shared.isIdleTimerDisabled = false } }

        let runID = "run-" + UUID().uuidString.lowercased()
        let since = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
        let until = Date()
        var grandTotal = 0

        do {
            // Per-run key, kept in the Keychain so a resumed drain can still decrypt.
            let key = SymmetricKey(size: .bits256)
            try DumpKeychain.store(key, runID: runID)
            let runDir = try DumpStore.createRunDir(runID)
            let dump = DumpFileBackendWriter(runDir: runDir, key: key)

            // Point the writer at the dump sink for the export's duration. The
            // sync* methods build the same rows they send to EA live.
            let previousWriter = eaWriter
            eaWriter = dump
            defer { eaWriter = previousWriter }

            let totalCats = Double(max(1, syncState.categories.count))
            var doneCats = 0.0

            // Export one atomic ("special") category, updating its card.
            func exportSpecial(_ catID: String, _ label: String, _ run: () async throws -> Int) async throws {
                try Task.checkCancellation()
                syncState.updateCategory(catID, status: .syncing, progress: 0, total: 1)
                syncState.currentOperation = "Exporting \(label)…"
                let count = try await run()
                grandTotal += count
                doneCats += 1
                // "Exported" — NOT "Synced": the data is in the local dump but
                // not yet delivered to MySQL/EA. Marked complete after delivery.
                syncState.updateCategory(catID, status: .exported, recordCount: count, lastSyncDate: until, progress: 1)
                syncState.totalRecords = grandTotal
                syncState.updateFullSyncProgress(export: doneCats / totalCats)
                updateLiveActivity(phase: "Export", operation: "Exported \(label) (\(count.formatted()))", records: grandTotal)
            }

            // Quantity categories — per-category card + per-type progress bar.
            for (cat, types) in HealthDataTypes.quantityTypesByCategory {
                try Task.checkCancellation()
                let catID = "qty_\(cat.rawValue)"
                syncState.updateCategory(catID, status: .syncing, progress: 0, total: types.count)
                syncState.currentOperation = "Exporting \(cat.rawValue)…"
                var catCount = 0
                for (i, typeDesc) in types.enumerated() {
                    try Task.checkCancellation()
                    SyncService.noteSyncActivity()
                    catCount += try await syncQuantityType(typeDesc: typeDesc, mysql: nil, since: since, until: until)
                    syncState.updateCategory(catID, progress: i + 1)
                    syncState.updateFullSyncProgress(export: (doneCats + Double(i + 1) / Double(types.count)) / totalCats)
                }
                grandTotal += catCount
                doneCats += 1
                syncState.updateCategory(catID, status: .exported, recordCount: catCount, lastSyncDate: until, progress: types.count)
                syncState.totalRecords = grandTotal
                updateLiveActivity(phase: "Export", operation: "Exported \(cat.rawValue) (\(catCount.formatted()))", records: grandTotal)
            }

            try await exportSpecial("cat_category", "Health Events") { try await self.syncCategorySamples(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_workouts", "Workouts") { try await self.syncWorkouts(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_bp", "Blood Pressure") { try await self.syncBloodPressure(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_ecg", "ECG") { try await self.syncECG(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_audiogram", "Audiogram") { try await self.syncAudiograms(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_activity_summaries", "Activity Rings") { try await self.syncActivitySummaries(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_workout_routes", "Workout Routes") { try await self.syncWorkoutRoutes(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_medications", "Medications") { try await self.syncMedications(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_vision", "Vision Prescriptions") { try await self.syncVisionPrescriptions(mysql: nil, since: since, until: until) }
            try await exportSpecial("cat_state_of_mind", "State of Mind") { try await self.syncStateOfMind(mysql: nil, since: since, until: until) }

            // Close files, gather counts, write the manifest.
            let counts = try await dump.finish()
            let manifest = DumpManifest(
                runID: runID,
                schemaVersion: SchemaService.currentSchemaVersion,
                createdAt: ISO8601DateFormatter().string(from: until),
                since: sqlDate(since),
                until: sqlDate(until),
                tables: counts.map {
                    DumpManifest.TableEntry(table: $0.key, rowCount: $0.value, fileName: $0.key.fileName)
                }
            )
            try JSONEncoder().encode(manifest)
                .write(to: DumpStore.manifestURL(runID),
                       options: [.atomic, .completeFileProtectionUntilFirstUserAuthentication])

            UserDefaults.standard.set(runID, forKey: "pendingDumpDrainRunID")
            UserDefaults.standard.set(runID, forKey: "pendingEAUploadRunID")

            // Export finished — delivery continues with no screen needed. Leave the
            // live activity running; runFullSync / the EA uploader end it when the
            // whole sync completes.
            syncState.updateFullSyncProgress(phase: .delivering, export: 1.0)
            syncState.currentOperation = "Export complete (\(grandTotal.formatted()) records) — delivering…"
            lastExportRecordCount = grandTotal
            syncState.persist()
            return runID
        } catch is CancellationError {
            syncState.currentOperation = "Sync cancelled"
            for i in syncState.categories.indices where syncState.categories[i].status.isActive {
                syncState.categories[i].status = .idle
            }
            endLiveActivity(totalRecords: grandTotal)
            DumpStore.wipeRun(runID)
            return nil
        } catch {
            syncState.errorMessage = error.localizedDescription
            syncState.currentOperation = ""
            for i in syncState.categories.indices where syncState.categories[i].status.isActive {
                syncState.categories[i].status = .idle
            }
            endLiveActivity(totalRecords: grandTotal)
            DumpStore.wipeRun(runID)
            return nil
        }
    }

    // MARK: - Full sync (dump-based, replaces the old windowed backfill)

    /// The one full sync. Phase 1 exports all HealthKit data to encrypted local
    /// dumps (screen-on, minutes). Phase 2 delivers to the enabled destinations
    /// WITHOUT needing the screen on:
    ///
    /// - **EA first** — it's fast for the phone (hand the big file to a
    ///   background `URLSession` upload; EA wipes its tables and imports
    ///   server-side), so the phone can then spend its time on the slower drain.
    /// - **MySQL second** — drained on-device; the drain truncates the health
    ///   tables first for a clean replace and resumes across background windows.
    ///
    /// Honors the per-destination enable toggles. Local dumps are wiped once
    /// every required destination confirms (`DumpStore.markDrainDone` /
    /// `markEAKeyDelivered`).
    func runFullSync(config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        let mysqlConfigured = config.enabled
        let eaConfigured = EAConfig.load().isConfigured
        guard mysqlConfigured || eaConfigured else {
            syncState.errorMessage = "No sync destination is enabled. Turn on MySQL or EA in Settings."
            return
        }

        // Deliver only to destinations that still need a baseline (catch-up). A
        // destination that is already baselined + current is left untouched — no
        // re-truncate; incremental keeps it current. The export still reads ALL
        // HealthKit data (the lagging destination needs everything).
        let mysqlNeedsBaseline = syncState.mysqlNeedsFullSync(enabled: mysqlConfigured)
        let eaNeedsBaseline = syncState.eaNeedsFullSync(configured: eaConfigured)
        guard mysqlNeedsBaseline || eaNeedsBaseline else {
            // Both enabled destinations already have a baseline — nothing to full
            // sync. (The dashboard shows "Sync Now" → incremental in this state.)
            return
        }

        // Own the running-flag lifecycle across the whole pass (export + deliver).
        syncState.isFullSyncRunning = true
        SyncService.setGlobalSyncRunning(true)
        syncState.startFullSyncProgress(eaEnabled: eaNeedsBaseline, mysqlEnabled: mysqlNeedsBaseline)
        defer {
            SyncService.setGlobalSyncRunning(false)
            syncState.isFullSyncRunning = false
            syncState.currentSyncCategoryIDs = []
        }

        guard let runID = await exportDump() else {   // screen-on phase
            syncState.endFullSyncProgress()
            return
        }
        DumpStore.setEARequired(eaNeedsBaseline, runID: runID)
        DumpStore.setMySQLRequired(mysqlNeedsBaseline, runID: runID)

        // Phase 2a — EA first (fast: background upload + server-side import).
        if eaNeedsBaseline {
            syncState.currentOperation = "Uploading to EA…"
            // Stash the live-activity progress weighting so the background uploader
            // can advance the live activity (overall = base + EA-weight × fraction)
            // without access to SyncState while suspended.
            UserDefaults.standard.set(syncState.overallProgress, forKey: "eaUpload.liveBase.\(runID)")
            UserDefaults.standard.set(syncState.fullSyncEAWeight, forKey: "eaUpload.liveSpan.\(runID)")
            do { try EADumpUploader.shared.start(runID: runID) }
            catch { syncState.errorMessage = error.localizedDescription }
        }

        // Phase 2b — MySQL drain on-device (slower). Resumable if interrupted.
        if mysqlNeedsBaseline {
            let drained = await drainDump(runID: runID, config: config)
            if drained {
                UserDefaults.standard.removeObject(forKey: "pendingDumpDrainRunID")
                syncState.updateFullSyncProgress(mysql: 1.0)
                syncState.markMySQLBaselineDone()   // MySQL now has a complete baseline
                _ = DumpStore.markDrainDone(runID)   // file-wipe coordination
            }
        } else {
            UserDefaults.standard.removeObject(forKey: "pendingDumpDrainRunID")
        }

        syncState.persist()
        // Finalize only when EVERY enabled destination is done. If EA is still
        // uploading (background) or failed, the EA uploader / its notification
        // finalize (or surface the retry) when it finishes.
        finalizeFullSyncIfComplete()
        if syncState.fullSyncPhase != .complete {
            syncState.currentOperation = syncState.fsEAFailed
                ? "MySQL done — EA upload failed, tap Retry"
                : "MySQL done — EA uploading in the background"
        }
    }

    /// Finishes the full-sync UI once every delivered destination is done: green
    /// category cards, 100% bar, live activity end. Per-destination baselines are
    /// marked at each destination's own completion point (MySQL drain success /
    /// EA key delivery), so this only handles the shared UI finish. Idempotent.
    /// Called after the MySQL drain and from the EA upload completion.
    func finalizeFullSyncIfComplete() {
        guard syncState.fullSyncPhase != .idle, syncState.fullSyncPhase != .complete else { return }
        guard syncState.fullSyncEADone, syncState.fullSyncMySQLDone else { return }
        markAllCategoriesSynced()
        syncState.finishFullSyncProgress()
        syncState.lastSyncDate = Date()
        syncState.currentOperation = "Full sync complete"
        syncState.persist()
        endLiveActivity(totalRecords: lastExportRecordCount)
    }

    /// Marks every category card as Synced (green). Called when a full sync is
    /// fully delivered, and by `SyncViewModel` when the EA upload tail finishes.
    func markAllCategoriesSynced() {
        for i in syncState.categories.indices where syncState.categories[i].status == .exported {
            syncState.categories[i].status = .completed
        }
        syncState.persist()
    }

    // MARK: - Connection helpers

    /// Ensures MySQL is connected, reconnecting if the connection was dropped.
    private func ensureMySQLConnected(config: MySQLConfig) async throws {
        SyncService.noteSyncActivity()  // heartbeat: called per drain step / per live-sync type
        guard mysql != nil else {
            try await connectMySQL(config: config)
            return
        }
        do {
            // Use `query` (not `execute`) because SELECT returns a result set.
            // `execute` only reads one packet, leaving stale data in the buffer.
            _ = try await mysql!.query("SELECT 1")
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

    // MARK: - Incremental sync

    func runIncrementalSync(config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        // Run only if at least one enabled destination has a full-sync baseline;
        // incremental writes to those, and skips any enabled-but-un-baselined
        // destination (which the dashboard flags as "needs full sync"). No
        // baseline anywhere → a full sync (screen-on export) is required first.
        guard syncState.anyBaselineReady(mysqlEnabled: config.enabled,
                                         eaConfigured: EAConfig.load().isConfigured) else { return }
        syncState.isIncrementalSyncRunning = true
        syncState.fullSyncPhase = .idle   // incremental: recalcOverall drives progress
        SyncService.isSyncRunning = true
        defer {
            SyncService.isSyncRunning = false
            syncState.currentSyncCategoryIDs = []
        }
        syncState.errorMessage = nil
        startLiveActivity(isFullSync: false)

        // Keep screen awake during foreground sync to prevent auto-lock killing
        // HealthKit access — honoring the user's "Keep screen on during sync" setting.
        if !isBackgroundSync && UserDefaults.standard.bool(forKey: "keepScreenOnDuringSync") {
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
        var total = 0
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

            // Write to MySQL only when it's enabled AND has a baseline. An
            // enabled-but-un-baselined MySQL is skipped (it needs a full sync);
            // the pass then runs EA-only — the leaf `sync*` methods accept a nil
            // mysql. (`eaWriter` is likewise attached only for a baselined EA.)
            let mysqlEnabled = config.enabled && syncState.mysqlBaselineDone
            guard mysqlEnabled || eaWriter != nil else {
                syncState.currentOperation = "No sync destination is enabled"
                syncState.isIncrementalSyncRunning = false
                return
            }
            if mysqlEnabled {
                try await connectMySQL(config: config)
                guard self.mysql != nil else { throw MySQLError.disconnected }
            }

            // Helper: a live MySQL connection (reconnecting if dropped), or nil
            // when MySQL is disabled.
            @MainActor func liveMySQL() async throws -> MySQLService? {
                guard mysqlEnabled else { return nil }
                try await ensureMySQLConnected(config: config)
                guard let m = self.mysql else { throw MySQLError.disconnected }
                return m
            }

            // Ensure schema is up to date (adds any new tables from updates)
            if let m = self.mysql {
                let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: m)
                if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }

                if Date().timeIntervalSince(SyncService.lastStaleCleanup) > 600 {
                    await cleanupStaleLogEntries(mysql: m)
                    SyncService.lastStaleCleanup = Date()
                }
            }

            // Last completed sync: from the MySQL sync log when available, else
            // from the locally-persisted lastSyncDate (EA-only mode).
            let lastSync: Date? = try await {
                if let m = self.mysql { return try await lastCompletedSyncDate(mysql: m) ?? syncState.lastSyncDate }
                return syncState.lastSyncDate
            }()
            let distantPast = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
            let since = lastSync ?? distantPast
            // Apply a 7-day lookback for HealthKit queries so late-arriving samples (e.g. apps
            // that backfill historical entries into HealthKit after the fact) are captured.
            // INSERT IGNORE makes re-syncing the overlap window safe and idempotent.
            let globalQuerySince = lastSync.map { $0.addingTimeInterval(-7 * 24 * 3600) } ?? distantPast

            // Deep sweep: every 2 hours (or first sync after launch), use full 7-day lookback
            // to catch late-arriving samples. Routine syncs use a short adaptive lookback.
            let forceDeepSweep = Date().timeIntervalSince(SyncService.lastDeepSweepDate) > 7200

            // Per-type incremental cursors: use the later of the global baseline and the
            // type's own cursor so completed types are quickly skipped on re-runs.
            let cursors = syncState.incrementalCursors
            func querySince(for key: String) -> Date {
                adaptiveQuerySince(for: key, cursors: cursors, globalQuerySince: globalQuerySince, lastSync: lastSync, forceDeepSweep: forceDeepSweep)
            }

            let opLabel = lastSync != nil
                ? "Incremental sync from \(since.formatted(date: .abbreviated, time: .shortened))…"
                : "Full historical sync (fetching all data since 2000)…"
            syncState.currentOperation = opLabel

            if let m = self.mysql {
                logID = try await startSyncLog(mysql: m, category: "incremental_sync")
            }
            var failedCategories: [String] = []
            var hkInaccessibleCount = 0

            print("[SyncService] Incremental sync: lastSync=\(lastSync?.description ?? "nil"), globalQuerySince=\(globalQuerySince), cursors=\(cursors.count), isBackground=\(isBackgroundSync), deepSweep=\(forceDeepSweep)")

            // Build all sync units and sort by staleness (oldest-synced first)
            var allUnits: [SyncUnit] = []
            for (cat, types) in HealthDataTypes.quantityTypesByCategory {
                allUnits.append(.quantityGroup(cat, types))
            }
            allUnits.append(.categoryTypes)
            for specialID in ["cat_workouts", "cat_bp", "cat_ecg", "cat_audiogram",
                              "cat_activity_summaries", "cat_workout_routes",
                              "cat_medications", "cat_vision", "cat_state_of_mind"] {
                allUnits.append(.special(specialID))
            }

            let catStates = syncState.categories
            allUnits.sort { a, b in
                let dateA = catStates.first(where: { $0.id == a.categoryID })?.lastSyncDate
                let dateB = catStates.first(where: { $0.id == b.categoryID })?.lastSyncDate
                switch (dateA, dateB) {
                case (nil, nil): return false  // stable order
                case (nil, _): return true     // nil (never synced) goes first
                case (_, nil): return false
                case let (a?, b?): return a < b  // oldest first
                }
            }

            print("[SyncService] Incremental sync order: \(allUnits.map { "\($0.categoryID)(\(catStates.first(where: { $0.id == $0.id })?.lastSyncDate?.description ?? "nil"))" }.joined(separator: ", "))")

            syncState.currentSyncCategoryIDs = Set(allUnits.map(\.categoryID))
            // Drive the overall bar from units processed this pass (0→100%) — NOT
            // from counting green category cards (which start the pass already
            // completed, making the bar begin near 100%). Category cards keep their
            // real status; the one in flight flips to "Syncing…", the rest stay as
            // they were until re-synced.
            let totalUnits = max(1, allUnits.count)
            syncState.beginIncrementalProgress()

            for (idx, unit) in allUnits.enumerated() {
                syncState.setIncrementalProgress(Double(idx) / Double(totalUnits))
                let catID = unit.categoryID
                try Task.checkCancellation()

                // In background non-deep-sweep syncs, skip units where all type cursors
                // are < 5 minutes old — no new data could have arrived since last sync.
                if isBackgroundSync, !forceDeepSweep {
                    let typeKeys: [String]
                    switch unit {
                    case .quantityGroup(_, let types): typeKeys = types.map(\.id)
                    case .categoryTypes: typeKeys = HealthDataTypes.allCategoryTypes.map(\.id)
                    case .special(let id): typeKeys = [id]
                    }
                    if allCursorsRecent(typeKeys, cursors: cursors) {
                        syncState.updateCategory(catID, status: .completed)
                        continue
                    }
                }

                syncState.updateCategory(catID, status: .syncing)

                switch unit {
                case .quantityGroup(let cat, let types):
                    var catDelta = 0
                    var failedTypes: [String] = []
                    var activeMySQL = try await liveMySQL()
                    for typeDesc in types {
                        try Task.checkCancellation()
                        let typeQuerySince = querySince(for: typeDesc.id)
                        do {
                            let count = try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: typeQuerySince)
                            catDelta += count
                            syncState.incrementalCursors[typeDesc.id] = Date()
                            syncState.persist()
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch where SyncService.isConnectionError(error) {
                            do {
                                activeMySQL = try await liveMySQL()
                                let count = try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: typeQuerySince)
                                catDelta += count
                                syncState.incrementalCursors[typeDesc.id] = Date()
                                syncState.persist()
                            } catch is CancellationError {
                                throw CancellationError()
                            } catch {
                                failedTypes.append(typeDesc.displayName)
                            }
                        } catch {
                            if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                                hkInaccessibleCount += 1
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

                case .categoryTypes:
                    do {
                        var m = try await liveMySQL()
                        var catCount = 0
                        var catFailedTypes: [String] = []
                        for typeDesc in HealthDataTypes.allCategoryTypes {
                            try Task.checkCancellation()
                            let typeQuerySince = querySince(for: typeDesc.id)
                            do {
                                let count = try await syncCategoryType(typeDesc: typeDesc, mysql: m, since: typeQuerySince)
                                catCount += count
                                syncState.incrementalCursors[typeDesc.id] = Date()
                                syncState.persist()
                            } catch is CancellationError {
                                throw CancellationError()
                            } catch where SyncService.isConnectionError(error) {
                                do {
                                    m = try await liveMySQL()
                                    let count = try await syncCategoryType(typeDesc: typeDesc, mysql: m, since: typeQuerySince)
                                    catCount += count
                                    syncState.incrementalCursors[typeDesc.id] = Date()
                                    syncState.persist()
                                } catch is CancellationError {
                                    throw CancellationError()
                                } catch {
                                    catFailedTypes.append(typeDesc.displayName)
                                }
                            } catch {
                                if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                                    hkInaccessibleCount += 1
                                } else {
                                    catFailedTypes.append(typeDesc.displayName)
                                }
                            }
                        }
                        let existingCat = syncState.categories.first(where: { $0.id == "cat_category" })?.recordCount ?? 0
                        if catFailedTypes.isEmpty {
                            syncState.updateCategory("cat_category", status: .completed, recordCount: existingCat + catCount, lastSyncDate: Date())
                        } else {
                            failedCategories.append("Category Samples")
                            syncState.updateCategory("cat_category",
                                status: .failed("Failed types: \(catFailedTypes.joined(separator: ", "))"),
                                recordCount: existingCat + catCount, lastSyncDate: Date())
                        }
                        total += catCount
                        updateLiveActivity(phase: "Health Events", operation: "Synced Health Events (\(catCount) records)", records: total)
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                            hkInaccessibleCount += 1
                        } else {
                            failedCategories.append("Category Samples")
                            syncState.updateCategory("cat_category", status: .failed(error.localizedDescription), lastSyncDate: Date())
                        }
                    }

                case .special(let specialID):
                    let specialQuerySince = querySince(for: specialID)
                    do {
                        let count = try await syncSpecialCategory(
                            id: specialID, querySince: specialQuerySince, liveMySQL: liveMySQL)
                        let existing = syncState.categories.first(where: { $0.id == specialID })?.recordCount ?? 0
                        syncState.updateCategory(specialID, status: .completed, recordCount: existing + count, lastSyncDate: Date())
                        syncState.incrementalCursors[specialID] = Date()
                        syncState.persist()
                        total += count
                        updateLiveActivity(phase: unit.displayLabel, operation: "Synced \(unit.displayLabel) (\(count) records)", records: total)
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                            hkInaccessibleCount += 1
                        } else {
                            failedCategories.append(unit.displayLabel)
                            syncState.updateCategory(specialID, status: .failed(error.localizedDescription), lastSyncDate: Date())
                        }
                    }
                }
            }

            if !failedCategories.isEmpty {
                syncState.errorMessage = "Sync completed with errors in: \(failedCategories.joined(separator: ", "))"
            }

            // Two-way place category + geofence definition sync — MySQL-backed,
            // so only when MySQL is enabled.
            if let geoMySQL = try await liveMySQL() {
                do {
                    do { try await GeofenceSyncService.syncPlaceCategories(mysql: geoMySQL) }
                    catch { print("[SyncService] Place category sync failed: \(error)") }

                    let geofencesChanged = try await GeofenceSyncService.syncGeofences(mysql: geoMySQL)
                    if geofencesChanged {
                        LocationService.shared.updateGeofences(GeoFence.loadAll())
                        NotificationCenter.default.post(name: .geofencesDidSync, object: nil)
                    }
                } catch {
                    print("[SyncService] Geofence sync failed: \(error)")
                }
            }

            // Sync-log bookkeeping only when MySQL is the destination.
            if let finalMySQL = try await liveMySQL(), logID != 0 {
                // If HealthKit was inaccessible (device locked) and no records were synced,
                // mark the sync as "skipped" so it doesn't pollute lastCompletedSyncDate.
                if total == 0, hkInaccessibleCount > 0 {
                    print("[SyncService] Incremental sync: HealthKit inaccessible for \(hkInaccessibleCount) types — deleting log entry")
                    await deleteSyncLog(mysql: finalMySQL, id: logID)
                } else {
                    print("[SyncService] Incremental sync completed: \(total) records, \(failedCategories.count) failed categories, \(hkInaccessibleCount) HK-inaccessible")
                    try await completeSyncLog(mysql: finalMySQL, id: logID, count: total)
                }
            }
            if forceDeepSweep {
                SyncService.lastDeepSweepDate = Date()
            }
            syncState.lastSyncDate = Date()
            syncState.setIncrementalProgress(1.0)
            syncState.currentOperation = "Incremental sync done (\(total) records)"
            syncState.persist()
            endLiveActivity(totalRecords: total)
            disconnectMySQL()

        } catch is CancellationError {
            // If some records were synced before cancellation, complete the log (data is valid)
            if total > 0, logID != 0, let m = self.mysql {
                try? await completeSyncLog(mysql: m, id: logID, count: total)
            } else if let m = self.mysql, logID != 0 {
                await failSyncLog(mysql: m, id: logID, message: "Sync cancelled")
            }
            disconnectMySQL()
            endLiveActivity(totalRecords: total)
            syncState.currentOperation = total > 0
                ? "Sync cancelled after \(total) records"
                : "Sync cancelled"
            syncState.persist()
        } catch {
            if let m = self.mysql, logID != 0 { await failSyncLog(mysql: m, id: logID, message: error.localizedDescription) }
            disconnectMySQL()
            endLiveActivity(totalRecords: 0)
            syncState.errorMessage = error.localizedDescription
            syncState.currentOperation = ""
            syncState.persist()
        }

        syncState.endIncrementalProgress()
        syncState.isIncrementalSyncRunning = false
    }

    /// Routes a special category ID to its dedicated sync method with connection-error retry.
    private func syncSpecialCategory(
        id: String,
        querySince: Date,
        liveMySQL: @MainActor () async throws -> MySQLService?
    ) async throws -> Int {
        var m = try await liveMySQL()
        let count: Int
        switch id {
        case "cat_workouts":
            do { count = try await syncWorkouts(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncWorkouts(mysql: m, since: querySince)
            }
        case "cat_bp":
            do { count = try await syncBloodPressure(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncBloodPressure(mysql: m, since: querySince)
            }
        case "cat_ecg":
            do { count = try await syncECG(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncECG(mysql: m, since: querySince)
            }
        case "cat_audiogram":
            do { count = try await syncAudiograms(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncAudiograms(mysql: m, since: querySince)
            }
        case "cat_activity_summaries":
            do { count = try await syncActivitySummaries(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncActivitySummaries(mysql: m, since: querySince)
            }
        case "cat_workout_routes":
            do { count = try await syncWorkoutRoutes(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncWorkoutRoutes(mysql: m, since: querySince)
            }
        case "cat_medications":
            do { count = try await syncMedications(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncMedications(mysql: m, since: querySince)
            }
        case "cat_vision":
            do { count = try await syncVisionPrescriptions(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncVisionPrescriptions(mysql: m, since: querySince)
            }
        case "cat_state_of_mind":
            do { count = try await syncStateOfMind(mysql: m, since: querySince) }
            catch where SyncService.isConnectionError(error) {
                m = try await liveMySQL()
                count = try await syncStateOfMind(mysql: m, since: querySince)
            }
        default:
            count = 0
        }
        return count
    }

    // MARK: - Targeted sync (observer-triggered, single/few categories)

    /// Lightweight sync that only processes the specified categories. Used by BackgroundSyncManager
    /// when an HKObserverQuery fires, so we complete within the ~30s background time limit.
    func runTargetedSync(categoryIDs: Set<String>, config: MySQLConfig) async {
        guard !syncState.isAnySyncRunning else { return }
        // Need at least one baselined destination to write to (same rule as
        // runIncrementalSync); un-baselined destinations wait for a full sync.
        guard syncState.anyBaselineReady(mysqlEnabled: config.enabled,
                                         eaConfigured: EAConfig.load().isConfigured) else { return }
        syncState.isIncrementalSyncRunning = true
        syncState.fullSyncPhase = .idle
        SyncService.isSyncRunning = true
        defer {
            SyncService.isSyncRunning = false
            syncState.currentSyncCategoryIDs = []
        }
        syncState.errorMessage = nil
        syncState.currentSyncCategoryIDs = categoryIDs

        do {
            if isBackgroundSync {
                guard UIApplication.shared.isProtectedDataAvailable else {
                    syncState.isIncrementalSyncRunning = false
                    return
                }
            }

            // Write to MySQL only when enabled AND baselined; otherwise EA-only
            // (leaf methods take nil). `eaWriter` is attached only for a
            // baselined EA.
            let mysqlEnabled = config.enabled && syncState.mysqlBaselineDone
            guard mysqlEnabled || eaWriter != nil else {
                syncState.isIncrementalSyncRunning = false
                return
            }
            if mysqlEnabled {
                try await connectMySQL(config: config)
                guard self.mysql != nil else { throw MySQLError.disconnected }
            }

            if let m = self.mysql {
                let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: m)
                if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }
            }

            let lastSync: Date? = try await {
                if let m = self.mysql { return try await lastCompletedSyncDate(mysql: m) ?? syncState.lastSyncDate }
                return syncState.lastSyncDate
            }()
            let distantPast = Calendar.current.date(from: DateComponents(year: 2000, month: 1, day: 1))!
            let globalQuerySince = lastSync.map { $0.addingTimeInterval(-7 * 24 * 3600) } ?? distantPast

            // Targeted syncs are observer-triggered (new data arrived), so use adaptive
            // lookback with deep sweep forced — we want the full 7-day window here since
            // the observer fired because HealthKit has new/changed samples.
            let cursors = syncState.incrementalCursors
            func querySince(for key: String) -> Date {
                adaptiveQuerySince(for: key, cursors: cursors, globalQuerySince: globalQuerySince, lastSync: lastSync, forceDeepSweep: true)
            }

            @MainActor func liveMySQL() async throws -> MySQLService? {
                guard mysqlEnabled else { return nil }
                try await ensureMySQLConnected(config: config)
                guard let m = self.mysql else { throw MySQLError.disconnected }
                return m
            }

            var total = 0
            var hkInaccessibleCount = 0

            print("[SyncService] Targeted sync: globalQuerySince=\(globalQuerySince), cursors=\(cursors.count), categories=\(categoryIDs.sorted())")

            // Sync only the quantity categories that were triggered — per-type cursors
            for (cat, types) in HealthDataTypes.quantityTypesByCategory {
                let catID = "qty_\(cat.rawValue)"
                guard categoryIDs.contains(catID) else { continue }
                try Task.checkCancellation()

                var catDelta = 0
                var activeMySQL = try await liveMySQL()
                for typeDesc in types {
                    try Task.checkCancellation()
                    let typeQuerySince = querySince(for: typeDesc.id)
                    do {
                        let count = try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: typeQuerySince)
                        catDelta += count
                        syncState.incrementalCursors[typeDesc.id] = Date()
                        syncState.persist()
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch where SyncService.isConnectionError(error) {
                        do {
                            activeMySQL = try await liveMySQL()
                            let count = try await syncQuantityType(typeDesc: typeDesc, mysql: activeMySQL, since: typeQuerySince)
                            catDelta += count
                            syncState.incrementalCursors[typeDesc.id] = Date()
                            syncState.persist()
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {}
                    } catch {
                        if isBackgroundSync, (error as? HKError)?.code == .errorDatabaseInaccessible {
                            hkInaccessibleCount += 1
                        }
                    }
                }
                total += catDelta
            }

            // Sync category samples per-type if triggered
            if categoryIDs.contains("cat_category") {
                try Task.checkCancellation()
                var m = try await liveMySQL()
                for typeDesc in HealthDataTypes.allCategoryTypes {
                    try Task.checkCancellation()
                    let typeQuerySince = querySince(for: typeDesc.id)
                    do {
                        total += try await syncCategoryType(typeDesc: typeDesc, mysql: m, since: typeQuerySince)
                        syncState.incrementalCursors[typeDesc.id] = Date()
                        syncState.persist()
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch where SyncService.isConnectionError(error) {
                        do {
                            m = try await liveMySQL()
                            total += try await syncCategoryType(typeDesc: typeDesc, mysql: m, since: typeQuerySince)
                            syncState.incrementalCursors[typeDesc.id] = Date()
                            syncState.persist()
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {}
                    } catch {}
                }
            }

            if categoryIDs.contains("cat_workouts") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncWorkouts(mysql: m, since: querySince(for: "cat_workouts"))
                    syncState.incrementalCursors["cat_workouts"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_bp") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncBloodPressure(mysql: m, since: querySince(for: "cat_bp"))
                    syncState.incrementalCursors["cat_bp"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_ecg") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncECG(mysql: m, since: querySince(for: "cat_ecg"))
                    syncState.incrementalCursors["cat_ecg"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_audiogram") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncAudiograms(mysql: m, since: querySince(for: "cat_audiogram"))
                    syncState.incrementalCursors["cat_audiogram"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_activity_summaries") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncActivitySummaries(mysql: m, since: querySince(for: "cat_activity_summaries"))
                    syncState.incrementalCursors["cat_activity_summaries"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_workout_routes") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncWorkoutRoutes(mysql: m, since: querySince(for: "cat_workout_routes"))
                    syncState.incrementalCursors["cat_workout_routes"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_medications") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncMedications(mysql: m, since: querySince(for: "cat_medications"))
                    syncState.incrementalCursors["cat_medications"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_vision") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncVisionPrescriptions(mysql: m, since: querySince(for: "cat_vision"))
                    syncState.incrementalCursors["cat_vision"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            if categoryIDs.contains("cat_state_of_mind") {
                try Task.checkCancellation()
                let m = try await liveMySQL()
                do {
                    total += try await syncStateOfMind(mysql: m, since: querySince(for: "cat_state_of_mind"))
                    syncState.incrementalCursors["cat_state_of_mind"] = Date()
                    syncState.persist()
                }
                catch is CancellationError { throw CancellationError() }
                catch {}
            }

            // Two-way place category + geofence definition sync — MySQL-backed.
            if let geoMySQL = try await liveMySQL() {
                do {
                    do { try await GeofenceSyncService.syncPlaceCategories(mysql: geoMySQL) }
                    catch { print("[SyncService] Place category sync failed: \(error)") }

                    let geofencesChanged = try await GeofenceSyncService.syncGeofences(mysql: geoMySQL)
                    if geofencesChanged {
                        LocationService.shared.updateGeofences(GeoFence.loadAll())
                        NotificationCenter.default.post(name: .geofencesDidSync, object: nil)
                    }
                } catch {
                    print("[SyncService] Geofence sync failed: \(error)")
                }
            }

            print("[SyncService] Targeted sync completed: \(total) records, \(hkInaccessibleCount) HK-inaccessible")
            if hkInaccessibleCount > 0 {
                print("[SyncService] Targeted sync: HealthKit inaccessible for \(hkInaccessibleCount) types (device locked) — will retry next opportunity")
            }
            disconnectMySQL()
        } catch is CancellationError {
            disconnectMySQL()
        } catch {
            disconnectMySQL()
            syncState.errorMessage = error.localizedDescription
        }

        syncState.isIncrementalSyncRunning = false
    }

    // MARK: - Activity summary sync

    private func syncActivitySummaries(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let summaries = try await healthKit.fetchActivitySummaries(from: since, until: until)
        guard !summaries.isEmpty else { return 0 }

        let calendar = Calendar.current
        var total = 0

        for batch in summaries.chunked(into: batchSize) {
            var summaryRows: [HBActivitySummaryRow] = []
            let sql: String? = autoreleasepool {
                let values: [String] = batch.compactMap { summary in
                    guard let date = calendar.date(from: summary.dateComponents(for: calendar)) else { return nil }
                    let dateString = DateFormatter.mysqlDate.string(from: date)
                    let dateStr = MySQLEscape.quote(dateString)
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
                    summaryRows.append(HBActivitySummaryRow(
                        date: dateString,
                        active_energy_burned: activeEnergy,
                        active_energy_burned_goal: activeEnergyGoal,
                        exercise_time_minutes: exerciseTime,
                        exercise_time_goal_minutes: exerciseTimeGoal,
                        stand_hours: standHours,
                        stand_hours_goal: standHoursGoal
                    ))
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
                try await mysql?.execute(sql)
                if let eaWriter, !summaryRows.isEmpty {
                    try await eaWriter.writeActivitySummaries(summaryRows)
                }
                total += batch.count
            }
        }
        return total
    }

    // MARK: - Workout route sync

    private func syncWorkoutRoutes(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        // Single-row INSERTs: each row contains locations_json with thousands of GPS
        // points, easily reaching megabytes per row. Batching would exceed max_allowed_packet.
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
                    let locJSONString = "[\(locJSON)]"
                    let locJSONQuoted = MySQLEscape.quote(locJSONString)

                    let sql = """
                    INSERT IGNORE INTO health_workout_routes
                      (uuid, workout_uuid, start_date, location_count, locations_json)
                    VALUES (\(uuid), \(workoutUUID), \(startDate), \(count), \(locJSONQuoted))
                    """
                    try await mysql?.execute(sql)
                    if let eaWriter {
                        try await eaWriter.writeWorkoutRoutes([HBWorkoutRouteRow(
                            uuid: route.uuid.uuidString,
                            workout_uuid: workout.uuid.uuidString,
                            start_date: sqlDate(route.startDate),
                            location_count: count,
                            locations_json: locJSONString
                        )])
                    }
                    total += 1
                }
            }
        }
        return total
    }

    // MARK: - Medication sync

    private func syncMedications(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        if #available(iOS 26, *) {
            return try await syncMedicationsIOS26(mysql: mysql, since: since, until: until)
        }
        return 0
    }

    @available(iOS 26, *)
    private func syncMedicationsIOS26(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        var total = 0
        var valueStrings: [String] = []
        var pendingRows: [HBMedicationRow] = []
        let medications = (try? await healthKit.fetchUserAnnotatedMedications()) ?? []

        if medications.isEmpty {
            let events = try await healthKit.fetchMedicationDoseEvents(from: since, until: until)
            for event in events {
                try Task.checkCancellation()
                valueStrings.append(medicationValueString(event, medicationName: nil))
                pendingRows.append(medicationRow(event, medicationName: nil))
                if valueStrings.count >= batchSize {
                    try await flushMedicationBatch(valueStrings, rows: pendingRows, mysql: mysql)
                    total += valueStrings.count
                    valueStrings.removeAll()
                    pendingRows.removeAll()
                }
            }
        } else {
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
                    valueStrings.append(medicationValueString(event, medicationName: concept.displayText))
                    pendingRows.append(medicationRow(event, medicationName: concept.displayText))
                    if valueStrings.count >= batchSize {
                        try await flushMedicationBatch(valueStrings, rows: pendingRows, mysql: mysql)
                        total += valueStrings.count
                        valueStrings.removeAll()
                        pendingRows.removeAll()
                    }
                }
            }
        }
        if !valueStrings.isEmpty {
            try await flushMedicationBatch(valueStrings, rows: pendingRows, mysql: mysql)
            total += valueStrings.count
        }
        return total
    }

    private func flushMedicationBatch(_ values: [String], rows: [HBMedicationRow], mysql: MySQLService?) async throws {
        try Task.checkCancellation()
        try await mysql?.execute("""
        INSERT IGNORE INTO health_medications
          (uuid,medication_name,dosage,log_status,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
        VALUES \(values.joined(separator: ","))
        """)
        if let eaWriter, !rows.isEmpty {
            try await eaWriter.writeMedications(rows)
        }
    }

    @available(iOS 26, *)
    private func medicationRow(
        _ event: HKMedicationDoseEvent,
        medicationName: String?
    ) -> HBMedicationRow {
        let dosage = event.doseQuantity.map { "\($0) \(event.unit.unitString)" }
        return HBMedicationRow(
            uuid: event.uuid.uuidString,
            medication_name: medicationName,
            dosage: dosage,
            log_status: logStatusString(event.logStatus),
            start_date: sqlDate(event.startDate),
            end_date: sqlDate(event.endDate),
            source_name: event.sourceRevision.source.name,
            source_bundle_id: event.sourceRevision.source.bundleIdentifier,
            device_name: event.device?.name,
            metadata: nil
        )
    }

    @available(iOS 26, *)
    private func medicationValueString(
        _ event: HKMedicationDoseEvent,
        medicationName: String?
    ) -> String {
        let uuid      = MySQLEscape.quote(event.uuid.uuidString)
        let medName   = MySQLEscape.quote(medicationName)
        let dosage    = event.doseQuantity.map { MySQLEscape.quote("\($0) \(event.unit.unitString)") } ?? "NULL"
        let logStatus = MySQLEscape.quote(logStatusString(event.logStatus))
        let start     = MySQLEscape.quote(sqlDate(event.startDate))
        let end       = MySQLEscape.quote(sqlDate(event.endDate))
        let src       = MySQLEscape.quote(event.sourceRevision.source.name)
        let bundle    = MySQLEscape.quote(event.sourceRevision.source.bundleIdentifier)
        let device    = MySQLEscape.quote(event.device?.name)
        return "(\(uuid),\(medName),\(dosage),\(logStatus),\(start),\(end),\(src),\(bundle),\(device),NULL)"
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

    private func deleteSyncLog(mysql: MySQLService, id: Int64) async {
        _ = try? await mysql.execute("DELETE FROM health_sync_log WHERE id=\(id)")
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

    // MARK: - Stale record reconciliation

    /// Reconcile is split into at most this many time slices, each ≥ `minReconcileWindow`.
    /// A bounded slice keeps both the MySQL anti-join DELETE and the single EA reconcile
    /// request small even for the densest types (the Activity quantity types log a sample
    /// every few minutes), which is what was overrunning `innodb_lock_wait_timeout` on
    /// MySQL and dropping the connection on EA when the whole 7-day window went out at once.
    private static let maxReconcileWindows = 48
    private static let minReconcileWindow: TimeInterval = 3600  // 1 hour

    private struct ReconcileWindow {
        let lo: Date
        let hi: Date          // inclusive upper bound (matches the `<=` both backends apply)
        let uuids: [String]
    }

    /// Buckets the synced `(uuid, date)` pairs into ≤ `maxReconcileWindows` time slices.
    /// `date` must be the value stored in the row's `start_date` (the sample's start date),
    /// so a slice's range and its UUID set agree. Only populated slices are returned:
    /// a slice with no synced UUIDs is never reconciled — we don't mass-delete a window's
    /// rows because a query happened to return nothing for it (the finer-grained twin of the
    /// all-empty guard in `reconcileEverywhere`). Both backends are driven by this identical
    /// list, so they stay in lock-step.
    ///
    /// Adjacent slices use a 1 ms-trimmed upper bound so no boundary instant is claimed by two
    /// slices (each carries a different UUID set; an overlap could delete a boundary row). The
    /// final slice keeps `until` inclusive so a sample at exactly `until` is still covered.
    private static func reconcileWindows(
        since: Date, until: Date, validUUIDs: [String], dates: [Date]?
    ) -> [ReconcileWindow] {
        guard let dates, dates.count == validUUIDs.count, until > since else {
            return [ReconcileWindow(lo: since, hi: until, uuids: validUUIDs)]
        }
        let span = until.timeIntervalSince(since)
        let windowSize = max(span / Double(maxReconcileWindows), minReconcileWindow)
        let lastIdx = max(0, Int((span / windowSize).rounded(.up)) - 1)

        var buckets: [Int: [String]] = [:]
        for (uuid, date) in zip(validUUIDs, dates) {
            let raw = Int((date.timeIntervalSince(since) / windowSize).rounded(.down))
            buckets[min(max(raw, 0), lastIdx), default: []].append(uuid)
        }

        return buckets.keys.sorted().map { idx in
            let lo = since.addingTimeInterval(Double(idx) * windowSize)
            let nominalHi = since.addingTimeInterval(Double(idx + 1) * windowSize)
            let hi = nominalHi >= until ? until : nominalHi.addingTimeInterval(-0.001)
            return ReconcileWindow(lo: lo, hi: hi, uuids: buckets[idx]!)
        }
    }

    // Reconciles stale rows against MySQL AND, if EA is enabled, the same slices via
    // `eaWriter.reconcileSlice`. Single call site for every per-type sync method so both
    // destinations stay in lock-step when Apple Health removes a sample. When `dates` is
    // supplied (parallel to `validUUIDs`) the [since, until] range is sub-windowed so each
    // DELETE/request is bounded; sparse callers pass `dates == nil` for a single full-range
    // slice (unchanged behavior, just upgraded to the anti-join DELETE below).
    private func reconcileEverywhere(
        table: String,
        typeColumn: String?,
        typeName: String?,
        since: Date,
        until: Date,
        validUUIDs: [String],
        dates: [Date]? = nil,
        mysql: MySQLService,
        displayLabel: String
    ) async throws {
        guard !validUUIDs.isEmpty else { return }

        let windows = Self.reconcileWindows(
            since: since, until: until, validUUIDs: validUUIDs, dates: dates
        )

        // Stage UUIDs into one connection-scoped temp table, refilled per slice, so we
        // don't churn CREATE/DROP DDL per window.
        let tmpTable = "hb_reconcile_valid_uuids"
        try await mysql.execute("DROP TEMPORARY TABLE IF EXISTS `\(tmpTable)`")
        try await mysql.execute("""
        CREATE TEMPORARY TABLE `\(tmpTable)` (
            uuid VARCHAR(36) NOT NULL PRIMARY KEY
        )
        """)

        var mysqlDeleted = 0
        var eaDeleted = 0
        for window in windows {
            mysqlDeleted += try await reconcileMySQLSlice(
                table: table, typeColumn: typeColumn, typeName: typeName,
                since: window.lo, until: window.hi, validUUIDs: window.uuids,
                mysql: mysql, tmpTable: tmpTable
            )
            if let eaWriter {
                eaDeleted += try await eaWriter.reconcileSlice(
                    table: table, typeColumn: typeColumn, typeValue: typeName,
                    since: sqlDate(window.lo), until: sqlDate(window.hi),
                    validUUIDs: window.uuids
                )
            }
        }

        // Connection-scoped; goes away on disconnect, and the next reconcile drops it first.
        _ = try? await mysql.execute("DROP TEMPORARY TABLE IF EXISTS `\(tmpTable)`")

        if mysqlDeleted > 0 {
            print("[SyncService] Reconciled \(mysqlDeleted) stale \(displayLabel) records (MySQL)")
        }
        if eaDeleted > 0 {
            print("[SyncService] Reconciled \(eaDeleted) stale \(displayLabel) records (EA)")
        }
    }

    // Deletes database rows in one (table, type, [since, until]) slice whose UUID is not in
    // `validUUIDs`, using the shared `tmpTable` (refilled per call). This handles samples
    // deleted or replaced in HealthKit (e.g. a third-party app editing an entry creates a new
    // UUID and deletes the old one). Returns the number of stale rows removed.
    private func reconcileMySQLSlice(
        table: String,
        typeColumn: String?,
        typeName: String?,
        since: Date,
        until: Date,
        validUUIDs: [String],
        mysql: MySQLService,
        tmpTable: String
    ) async throws -> Int {
        guard !validUUIDs.isEmpty else { return 0 }

        // Refill the staging table with just this slice's UUIDs, chunked to stay under
        // max_allowed_packet.
        _ = try await mysql.execute("TRUNCATE TABLE `\(tmpTable)`")
        for chunk in validUUIDs.chunked(into: 1000) {
            let values = chunk.map { "(\(MySQLEscape.quote($0)))" }.joined(separator: ",")
            try await mysql.execute("INSERT IGNORE INTO `\(tmpTable)` (uuid) VALUES \(values)")
        }

        let sinceStr = MySQLEscape.quote(sqlDate(since))
        let untilStr = MySQLEscape.quote(sqlDate(until))
        let typeFilter: String
        if let typeColumn = typeColumn, let typeName = typeName {
            typeFilter = " AND t.`\(typeColumn)` = '\(typeName)'"
        } else {
            typeFilter = ""
        }

        // Anti-join (LEFT JOIN … IS NULL) rather than `NOT IN (SELECT …)`: MySQL plans it as a
        // far lighter, index-driven hash anti-join instead of a per-row subquery over a
        // full-range scan, so it holds locks for a fraction of the time.
        let deleted = try await mysql.execute("""
        DELETE t FROM `\(table)` AS t
        LEFT JOIN `\(tmpTable)` AS v ON v.uuid = t.uuid
        WHERE t.start_date >= \(sinceStr)
          AND t.start_date <= \(untilStr)
          \(typeFilter)
          AND v.uuid IS NULL
        """)

        return Int(deleted)
    }

    // MARK: - Quantity sync

    // Streams HealthKit samples in pages using cursor-based HKSampleQuery pagination,
    // inserting each page before requesting the next. Peak memory stays flat regardless
    // of total record count. Uses INSERT IGNORE to safely handle any overlap between pages.
    // After syncing, removes any database rows in the queried date range whose UUIDs no
    // longer exist in HealthKit (e.g. samples deleted or replaced by a third-party app).
    private func syncQuantityType(
        typeDesc: QuantityTypeDescriptor,
        mysql: MySQLService?,
        since: Date?,
        until: Date? = nil,
        insertBatchSize: Int = batchSize,
        onBatchInserted: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let typeName = MySQLEscape.escapeString(typeDesc.id)
        let unitStr  = MySQLEscape.escapeString(typeDesc.unitString)
        var total = 0
        var syncedUUIDs: [String] = []
        var syncedDates: [Date] = []

        try await healthKit.streamQuantitySamples(typeID: typeDesc.hkIdentifier, from: since, until: until) { hkBatch in
            for batch in hkBatch.chunked(into: insertBatchSize) {
                try Task.checkCancellation()
                // MySQL path only: building the SQL string and tracking UUIDs for
                // reconcile is skipped during export (mysql == nil) so an unwindowed
                // full-history pass doesn't accumulate millions of UUID strings.
                if let mysql {
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
                    // Only track UUIDs when a reconcile will run (since != nil).
                    // A full-range re-sync (since == nil) skips this so a dense
                    // type doesn't accumulate millions of UUID strings.
                    if since != nil {
                        syncedUUIDs.append(contentsOf: batch.map { $0.uuid.uuidString })
                        syncedDates.append(contentsOf: batch.map { $0.startDate })
                    }
                    try await mysql.execute(sql)
                }
                if let eaWriter {
                    let rows: [HBQuantityRow] = batch.map { s in
                        HBQuantityRow(
                            uuid: s.uuid.uuidString,
                            type: typeDesc.id,
                            value: s.quantity.doubleValue(for: typeDesc.unit),
                            unit: typeDesc.unitString,
                            start_date: sqlDate(s.startDate),
                            end_date: sqlDate(s.endDate),
                            source_name: s.sourceDisplayName,
                            source_bundle_id: s.sourceBundleID,
                            device_name: s.deviceName,
                            metadata: s.jsonMetadata()
                        )
                    }
                    try await eaWriter.writeQuantitySamples(rows)
                }
                total += batch.count
                onBatchInserted?(total)
            }
        }

        // Reconcile: remove DB rows whose UUIDs no longer exist in HealthKit for this date range.
        // Only reconcile when re-querying an overlap window (since != nil), not on first full sync.
        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_quantity_samples", typeColumn: "type", typeName: typeName,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, dates: syncedDates,
                mysql: mysql, displayLabel: typeDesc.displayName
            )
        }

        return total
    }

    // MARK: - Category sync

    private func syncCategoryType(
        typeDesc: CategoryTypeDescriptor,
        mysql: MySQLService?,
        since: Date?,
        until: Date? = nil,
        insertBatchSize: Int = batchSize
    ) async throws -> Int {
        let typeName = MySQLEscape.escapeString(typeDesc.id)
        var total = 0
        var syncedUUIDs: [String] = []
        var syncedDates: [Date] = []
        try await healthKit.streamCategorySamples(typeID: typeDesc.hkIdentifier, from: since, until: until) { hkBatch in
            for batch in hkBatch.chunked(into: insertBatchSize) {
                try Task.checkCancellation()
                // MySQL path only — skipped during export (mysql == nil), see syncQuantityType.
                if let mysql {
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
                    // Only track UUIDs when a reconcile will run (since != nil).
                    // A full-range re-sync (since == nil) skips this so a dense
                    // type doesn't accumulate millions of UUID strings.
                    if since != nil {
                        syncedUUIDs.append(contentsOf: batch.map { $0.uuid.uuidString })
                        syncedDates.append(contentsOf: batch.map { $0.startDate })
                    }
                    try await mysql.execute(sql)
                }
                if let eaWriter {
                    let rows: [HBCategoryRow] = batch.map { s in
                        HBCategoryRow(
                            uuid: s.uuid.uuidString,
                            type: typeDesc.id,
                            value: s.value,
                            value_label: typeDesc.valueLabels[s.value] ?? "\(s.value)",
                            start_date: sqlDate(s.startDate),
                            end_date: sqlDate(s.endDate),
                            source_name: s.sourceDisplayName,
                            source_bundle_id: s.sourceBundleID,
                            device_name: s.deviceName,
                            metadata: nil
                        )
                    }
                    try await eaWriter.writeCategorySamples(rows)
                }
                total += batch.count
            }
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_category_samples", typeColumn: "type", typeName: typeName,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, dates: syncedDates,
                mysql: mysql, displayLabel: "\(typeDesc.displayName) category"
            )
        }

        return total
    }

    private func syncCategorySamples(mysql: MySQLService?, since: Date?, until: Date? = nil, insertBatchSize: Int = batchSize) async throws -> Int {
        var total = 0
        for typeDesc in HealthDataTypes.allCategoryTypes {
            total += try await syncCategoryType(typeDesc: typeDesc, mysql: mysql, since: since, until: until, insertBatchSize: insertBatchSize)
        }
        return total
    }

    // MARK: - Workout sync

    private func syncWorkouts(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        var total = 0
        var syncedUUIDs: [String] = []
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
                if let mysql {
                    syncedUUIDs.append(contentsOf: batch.map { $0.uuid.uuidString })
                    try await mysql.execute(sql)
                }
                if let eaWriter {
                    let rows: [HBWorkoutRow] = batch.map { w in
                        HBWorkoutRow(
                            uuid: w.uuid.uuidString,
                            activity_type: w.activityTypeName,
                            duration_seconds: w.duration,
                            total_energy_burned_kcal: w.totalEnergyBurned?.doubleValue(for: .kilocalorie()),
                            total_distance_meters: w.totalDistance?.doubleValue(for: .meter()),
                            total_swimming_strokes: w.totalSwimmingStrokeCount?.doubleValue(for: .count()),
                            total_flights_climbed: w.totalFlightsClimbed?.doubleValue(for: .count()),
                            start_date: sqlDate(w.startDate),
                            end_date: sqlDate(w.endDate),
                            source_name: w.sourceDisplayName,
                            source_bundle_id: w.sourceBundleID,
                            device_name: w.deviceName,
                            metadata: nil
                        )
                    }
                    try await eaWriter.writeWorkouts(rows)
                }
                total += batch.count
            }
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_workouts", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "workout"
            )
        }

        return total
    }

    // MARK: - Blood pressure sync

    private func syncBloodPressure(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let correlations = try await healthKit.fetchBloodPressure(from: since, until: until)
        guard !correlations.isEmpty else { return 0 }

        var total = 0
        var syncedUUIDs: [String] = []
        let systolicType = HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic)!
        let diastolicType = HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic)!

        for batch in correlations.chunked(into: batchSize) {
            // Build typed BP rows + parallel SQL values list in one pass so
            // the EA fan-out below sees exactly the same rows MySQL did.
            var bpRows: [HBBloodPressureRow] = []
            let values: [String] = batch.compactMap { corr -> String? in
                guard
                    let sys = (corr.objects(for: systolicType) as? Set<HKQuantitySample>)?.first,
                    let dia = (corr.objects(for: diastolicType) as? Set<HKQuantitySample>)?.first
                else { return nil }

                let sysVal = sys.quantity.doubleValue(for: .millimeterOfMercury())
                let diaVal = dia.quantity.doubleValue(for: .millimeterOfMercury())

                bpRows.append(HBBloodPressureRow(
                    uuid: corr.uuid.uuidString,
                    systolic: sysVal,
                    diastolic: diaVal,
                    start_date: sqlDate(corr.startDate),
                    source_name: corr.sourceRevision.source.name,
                    device_name: corr.device?.name,
                    metadata: nil
                ))

                let uuid   = MySQLEscape.quote(corr.uuid.uuidString)
                let start  = MySQLEscape.quote(sqlDate(corr.startDate))
                let src    = MySQLEscape.quote(corr.sourceRevision.source.name)
                let device = MySQLEscape.quote(corr.device?.name)
                return "(\(uuid),\(sysVal),\(diaVal),\(start),\(src),\(device),NULL)"
            }

            syncedUUIDs.append(contentsOf: batch.map { $0.uuid.uuidString })
            if values.isEmpty { continue }
            let sql = """
            INSERT IGNORE INTO health_blood_pressure
              (uuid,systolic,diastolic,start_date,source_name,device_name,metadata)
            VALUES \(values.joined(separator: ","))
            """
            try await mysql?.execute(sql)
            if let eaWriter { try await eaWriter.writeBloodPressure(bpRows) }
            total += values.count
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_blood_pressure", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "blood pressure"
            )
        }

        return total
    }

    // MARK: - ECG sync

    private func syncECG(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let recordings = try await healthKit.fetchECG(from: since, until: until)
        guard !recordings.isEmpty else { return 0 }

        // Single-row INSERTs: each row contains voltage_measurements JSON (~30KB).
        // Batching risks exceeding max_allowed_packet, and ECG count is typically small.
        var total = 0
        let syncedUUIDs = recordings.map { $0.uuid.uuidString }
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
            try await mysql?.execute(sql)
            if let eaWriter {
                // Reconstruct the JSON string without SQL escaping for the EA payload.
                let mvUnit = HKUnit(from: "mV")
                let rawVoltages = voltages.compactMap { v -> Double? in
                    v.quantity(for: .appleWatchSimilarToLeadI)?.doubleValue(for: mvUnit)
                }
                let voltageString = rawVoltages.isEmpty ? nil
                    : "[" + rawVoltages.map { String(format: "%.6f", $0) }.joined(separator: ",") + "]"
                try await eaWriter.writeEcg([HBEcgRow(
                    uuid: ecg.uuid.uuidString,
                    classification: ecg.classification.label,
                    average_heart_rate: ecg.averageHeartRate?.doubleValue(for: HKUnit(from: "count/min")),
                    sampling_frequency: ecg.samplingFrequency?.doubleValue(for: HKUnit(from: "Hz")),
                    voltage_measurements: voltageString,
                    start_date: sqlDate(ecg.startDate),
                    source_name: ecg.sourceRevision.source.name,
                    metadata: nil
                )])
            }
            total += 1
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_ecg", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "ECG"
            )
        }

        return total
    }

    // MARK: - Audiogram sync

    private func syncAudiograms(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let audiograms = try await healthKit.fetchAudiograms(from: since, until: until)
        guard !audiograms.isEmpty else { return 0 }

        var total = 0
        let syncedUUIDs = audiograms.map { $0.uuid.uuidString }
        var valueStrings: [String] = []
        var pendingRows: [HBAudiogramRow] = []
        for ag in audiograms {
            let uuid  = MySQLEscape.quote(ag.uuid.uuidString)
            let start = MySQLEscape.quote(sqlDate(ag.startDate))
            let src   = MySQLEscape.quote(ag.sourceRevision.source.name)

            let pointJSON = ag.sensitivityPoints.map { pt -> String in
                let freq = pt.frequency.doubleValue(for: .hertz())
                let leftDB  = pt.leftEarSensitivity?.doubleValue(for: HKUnit.decibelHearingLevel()) ?? 0
                let rightDB = pt.rightEarSensitivity?.doubleValue(for: HKUnit.decibelHearingLevel()) ?? 0
                return "{\"hz\":\(freq),\"l\":\(leftDB),\"r\":\(rightDB)}"
            }.joined(separator: ",")
            let pointsArray = "[\(pointJSON)]"
            let jsonStr = MySQLEscape.quote(pointsArray)

            valueStrings.append("(\(uuid),\(jsonStr),\(start),\(src),NULL)")
            pendingRows.append(HBAudiogramRow(
                uuid: ag.uuid.uuidString,
                sensitivity_points: pointsArray,
                start_date: sqlDate(ag.startDate),
                source_name: ag.sourceRevision.source.name,
                metadata: nil
            ))

            if valueStrings.count >= batchSize {
                try Task.checkCancellation()
                try await mysql?.execute("""
                INSERT IGNORE INTO health_audiograms
                  (uuid,sensitivity_points,start_date,source_name,metadata)
                VALUES \(valueStrings.joined(separator: ","))
                """)
                if let eaWriter { try await eaWriter.writeAudiograms(pendingRows) }
                total += valueStrings.count
                valueStrings.removeAll()
                pendingRows.removeAll()
            }
        }
        if !valueStrings.isEmpty {
            try Task.checkCancellation()
            try await mysql?.execute("""
            INSERT IGNORE INTO health_audiograms
              (uuid,sensitivity_points,start_date,source_name,metadata)
            VALUES \(valueStrings.joined(separator: ","))
            """)
            if let eaWriter { try await eaWriter.writeAudiograms(pendingRows) }
            total += valueStrings.count
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_audiograms", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "audiogram"
            )
        }

        return total
    }

    // MARK: - Vision prescription sync

    private func syncVisionPrescriptions(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let prescriptions = try await healthKit.fetchVisionPrescriptions(from: since, until: until)
        guard !prescriptions.isEmpty else { return 0 }

        // Diopters (sphere/cylinder/addPower), degrees (axis), millimeters (baseCurve/diameter)
        let diopterUnit = HKUnit(from: "D")
        let degreeUnit  = HKUnit.count()
        let mmUnit      = HKUnit.meterUnit(with: .milli)

        var total = 0
        let syncedUUIDs = prescriptions.map { $0.uuid.uuidString }
        var valueStrings: [String] = []
        var pendingRows: [HBVisionRow] = []
        for p in prescriptions {
            let uuid     = MySQLEscape.quote(p.uuid.uuidString)
            let start    = MySQLEscape.quote(sqlDate(p.startDate))
            let end      = MySQLEscape.quote(sqlDate(p.endDate))
            let prescType = p.prescriptionType.rawValue
            let expiry   = p.expirationDate.map { MySQLEscape.quote(sqlDate($0)) } ?? "NULL"
            let src      = MySQLEscape.quote(p.sourceRevision.source.name)
            let bundle   = MySQLEscape.quote(p.sourceRevision.source.bundleIdentifier)
            let device   = MySQLEscape.quote(p.device?.name)

            // Optional<Double> for each eye axis so we can build both the SQL
            // value strings AND the typed HBVisionRow from one source of truth.
            var rSphereD: Double?, rCylD: Double?, rAxisD: Double?, rAddD: Double?
            var rBaseD: Double?, rDiamD: Double?
            var lSphereD: Double?, lCylD: Double?, lAxisD: Double?, lAddD: Double?
            var lBaseD: Double?, lDiamD: Double?

            if let glasses = p as? HKGlassesPrescription {
                if let r = glasses.rightEye {
                    rSphereD = r.sphere.doubleValue(for: diopterUnit)
                    rCylD    = r.cylinder?.doubleValue(for: diopterUnit)
                    rAxisD   = r.axis?.doubleValue(for: degreeUnit)
                    rAddD    = r.addPower?.doubleValue(for: diopterUnit)
                }
                if let l = glasses.leftEye {
                    lSphereD = l.sphere.doubleValue(for: diopterUnit)
                    lCylD    = l.cylinder?.doubleValue(for: diopterUnit)
                    lAxisD   = l.axis?.doubleValue(for: degreeUnit)
                    lAddD    = l.addPower?.doubleValue(for: diopterUnit)
                }
            } else if let contacts = p as? HKContactsPrescription {
                if let r = contacts.rightEye {
                    rSphereD = r.sphere.doubleValue(for: diopterUnit)
                    rCylD    = r.cylinder?.doubleValue(for: diopterUnit)
                    rAxisD   = r.axis?.doubleValue(for: degreeUnit)
                    rAddD    = r.addPower?.doubleValue(for: diopterUnit)
                    rBaseD   = r.baseCurve?.doubleValue(for: mmUnit)
                    rDiamD   = r.diameter?.doubleValue(for: mmUnit)
                }
                if let l = contacts.leftEye {
                    lSphereD = l.sphere.doubleValue(for: diopterUnit)
                    lCylD    = l.cylinder?.doubleValue(for: diopterUnit)
                    lAxisD   = l.axis?.doubleValue(for: degreeUnit)
                    lAddD    = l.addPower?.doubleValue(for: diopterUnit)
                    lBaseD   = l.baseCurve?.doubleValue(for: mmUnit)
                    lDiamD   = l.diameter?.doubleValue(for: mmUnit)
                }
            }

            let rSphere = MySQLEscape.quoteDouble(rSphereD)
            let rCyl    = MySQLEscape.quoteDouble(rCylD)
            let rAxis   = MySQLEscape.quoteDouble(rAxisD)
            let rAdd    = MySQLEscape.quoteDouble(rAddD)
            let rBase   = MySQLEscape.quoteDouble(rBaseD)
            let rDiam   = MySQLEscape.quoteDouble(rDiamD)
            let lSphere = MySQLEscape.quoteDouble(lSphereD)
            let lCyl    = MySQLEscape.quoteDouble(lCylD)
            let lAxis   = MySQLEscape.quoteDouble(lAxisD)
            let lAdd    = MySQLEscape.quoteDouble(lAddD)
            let lBase   = MySQLEscape.quoteDouble(lBaseD)
            let lDiam   = MySQLEscape.quoteDouble(lDiamD)

            valueStrings.append("""
            (\(uuid),\(start),\(end),\(prescType),
                    \(rSphere),\(rCyl),\(rAxis),\(rAdd),\(rBase),\(rDiam),
                    \(lSphere),\(lCyl),\(lAxis),\(lAdd),\(lBase),\(lDiam),
                    \(expiry),\(src),\(bundle),\(device))
            """)

            pendingRows.append(HBVisionRow(
                uuid: p.uuid.uuidString,
                start_date: sqlDate(p.startDate),
                end_date: sqlDate(p.endDate),
                prescription_type: Int(prescType),
                right_eye_sphere: rSphereD,
                right_eye_cylinder: rCylD,
                right_eye_axis: rAxisD,
                right_eye_add_power: rAddD,
                right_eye_base_curve: rBaseD,
                right_eye_diameter: rDiamD,
                left_eye_sphere: lSphereD,
                left_eye_cylinder: lCylD,
                left_eye_axis: lAxisD,
                left_eye_add_power: lAddD,
                left_eye_base_curve: lBaseD,
                left_eye_diameter: lDiamD,
                expiration_date: p.expirationDate.map(sqlDate),
                source_name: p.sourceRevision.source.name,
                source_bundle_id: p.sourceRevision.source.bundleIdentifier,
                device_name: p.device?.name
            ))

            if valueStrings.count >= batchSize {
                try Task.checkCancellation()
                try await mysql?.execute("""
                INSERT IGNORE INTO health_vision_prescriptions
                  (uuid,start_date,end_date,prescription_type,
                   right_eye_sphere,right_eye_cylinder,right_eye_axis,right_eye_add_power,
                   right_eye_base_curve,right_eye_diameter,
                   left_eye_sphere,left_eye_cylinder,left_eye_axis,left_eye_add_power,
                   left_eye_base_curve,left_eye_diameter,
                   expiration_date,source_name,source_bundle_id,device_name)
                VALUES \(valueStrings.joined(separator: ","))
                """)
                if let eaWriter { try await eaWriter.writeVisionPrescriptions(pendingRows) }
                total += valueStrings.count
                valueStrings.removeAll()
                pendingRows.removeAll()
            }
        }
        if !valueStrings.isEmpty {
            try Task.checkCancellation()
            try await mysql?.execute("""
            INSERT IGNORE INTO health_vision_prescriptions
              (uuid,start_date,end_date,prescription_type,
               right_eye_sphere,right_eye_cylinder,right_eye_axis,right_eye_add_power,
               right_eye_base_curve,right_eye_diameter,
               left_eye_sphere,left_eye_cylinder,left_eye_axis,left_eye_add_power,
               left_eye_base_curve,left_eye_diameter,
               expiration_date,source_name,source_bundle_id,device_name)
            VALUES \(valueStrings.joined(separator: ","))
            """)
            if let eaWriter { try await eaWriter.writeVisionPrescriptions(pendingRows) }
            total += valueStrings.count
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_vision_prescriptions", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "vision prescription"
            )
        }

        return total
    }

    // MARK: - State of Mind sync

    private func syncStateOfMind(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        if #available(iOS 18, *) {
            return try await syncStateOfMindIOS18(mysql: mysql, since: since, until: until)
        }
        return 0
    }

    @available(iOS 18, *)
    private func syncStateOfMindIOS18(mysql: MySQLService?, since: Date?, until: Date? = nil) async throws -> Int {
        let samples = try await healthKit.fetchStateOfMind(from: since, until: until)
        guard !samples.isEmpty else { return 0 }

        var total = 0
        let syncedUUIDs = samples.map { $0.uuid.uuidString }
        var valueStrings: [String] = []
        var pendingRows: [HBStateOfMindRow] = []
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

            valueStrings.append("(\(uuid),\(start),\(end),\(kind),\(valence),\(valenceClass),\(MySQLEscape.quote(labelsJSON)),\(MySQLEscape.quote(assocJSON)),\(src),\(bundle),\(device))")

            pendingRows.append(HBStateOfMindRow(
                uuid: sample.uuid.uuidString,
                start_date: sqlDate(sample.startDate),
                end_date: sqlDate(sample.endDate),
                kind: kind,
                valence: valence,
                valence_classification: valenceClass,
                labels_json: labelsJSON,
                associations_json: assocJSON,
                source_name: sample.sourceRevision.source.name,
                source_bundle_id: sample.sourceRevision.source.bundleIdentifier,
                device_name: sample.device?.name
            ))

            if valueStrings.count >= batchSize {
                try Task.checkCancellation()
                try await mysql?.execute("""
                INSERT IGNORE INTO health_state_of_mind
                  (uuid,start_date,end_date,kind,valence,valence_classification,
                   labels_json,associations_json,source_name,source_bundle_id,device_name)
                VALUES \(valueStrings.joined(separator: ","))
                """)
                if let eaWriter { try await eaWriter.writeStateOfMind(pendingRows) }
                total += valueStrings.count
                valueStrings.removeAll()
                pendingRows.removeAll()
            }
        }
        if !valueStrings.isEmpty {
            try Task.checkCancellation()
            try await mysql?.execute("""
            INSERT IGNORE INTO health_state_of_mind
              (uuid,start_date,end_date,kind,valence,valence_classification,
               labels_json,associations_json,source_name,source_bundle_id,device_name)
            VALUES \(valueStrings.joined(separator: ","))
            """)
            if let eaWriter { try await eaWriter.writeStateOfMind(pendingRows) }
            total += valueStrings.count
        }

        if let mysql, let since = since, !syncedUUIDs.isEmpty {
            try await reconcileEverywhere(
                table: "health_state_of_mind", typeColumn: nil, typeName: nil,
                since: since, until: until ?? Date(), validUUIDs: syncedUUIDs, mysql: mysql,
                displayLabel: "state of mind"
            )
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
