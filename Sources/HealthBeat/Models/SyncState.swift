import Foundation

// MARK: - Persistence helpers

struct PersistedCategory: Codable {
    let id: String
    let recordCount: Int
    let lastSyncDate: Date?
    let completed: Bool
}

struct PersistedSnapshot: Codable {
    let lastSyncDate: Date?
    let categories: [PersistedCategory]
    let totalRecords: Int?
    let incrementalCursors: [String: Date]?

    // Per-destination full-sync baseline. Each `…Done` flag is paired with a
    // `…At` change-timestamp so iCloud can merge last-writer-wins (a reset —
    // done=false, newer — beats an older completion). Replaces the old single
    // `hasCompletedFullSync`/`fullSyncStateAt` pair (still decoded below for the
    // one-time upgrade migration in `SyncState.restore()`).
    let mysqlBaselineDone: Bool?
    let mysqlBaselineAt: Date?
    let eaBaselineDone: Bool?
    let eaBaselineAt: Date?

    // Legacy (decode-only) — present in snapshots written before per-destination
    // baselines. Never re-encoded.
    let hasCompletedFullSync: Bool?
    let fullSyncStateAt: Date?
}

// MARK: -

enum SyncStatus: Equatable {
    case idle
    case syncing
    case exported   // data captured to the local dump, not yet delivered to a backend
    case completed
    case failed(String)

    var label: String {
        switch self {
        case .idle: return "Idle"
        case .syncing: return "Syncing…"
        case .exported: return "Exported"
        case .completed: return "Synced"
        case .failed: return "Error"
        }
    }

    var isActive: Bool {
        if case .syncing = self { return true }
        return false
    }
}

/// Phases of the dump-based full sync, used to drive the dashboard banner and
/// the phase-weighted overall progress bar.
enum FullSyncPhase: Equatable {
    case idle          // not full-syncing (incremental drives progress normally)
    case exporting     // reading HealthKit → local dump (screen must stay on)
    case delivering    // uploading to EA / draining to MySQL (screen may sleep)
    case complete
}

/// Status of one high-level full-sync step (Export / EA / MySQL) for the
/// color-coded stepper on the dashboard.
enum SyncStepStatus: Equatable {
    case pending   // gray — not started
    case active    // blue — in progress
    case done      // green — finished
    case failed    // red — needs retry
}

struct CategorySyncState: Identifiable {
    let id: String          // category identifier
    let displayName: String
    let systemImage: String
    var status: SyncStatus
    var recordCount: Int
    var lastSyncDate: Date?
    var currentProgress: Int
    var totalEstimated: Int
    var latestHealthKitDate: Date? = nil  // newest HK sample, queried on demand (not persisted)

    var progressFraction: Double {
        guard totalEstimated > 0 else { return 0 }
        return min(1.0, Double(currentProgress) / Double(totalEstimated))
    }

    var daysBehind: Int? {
        guard let latestHK = latestHealthKitDate,
              let lastSync = lastSyncDate,
              latestHK > lastSync else { return nil }
        let days = Calendar.current.dateComponents([.day], from: lastSync, to: latestHK).day ?? 0
        return days >= 1 ? days : nil
    }
}

@MainActor
class SyncState: ObservableObject {
    @Published var isFullSyncRunning = false
    @Published var isIncrementalSyncRunning = false
    @Published var categories: [CategorySyncState] = []
    @Published var totalRecords: Int = 0
    @Published var lastSyncDate: Date?
    @Published var overallProgress: Double = 0.0
    @Published var currentOperation: String = ""
    @Published var errorMessage: String?
    // Per-destination full-sync baseline. A destination is kept current by
    // automatic incremental sync only once it has a baseline; a destination that
    // is enabled but un-baselined (enabled later, or its data was reset) is
    // surfaced to the user with a "needs full sync" prompt. Each `…At` is the
    // last-change timestamp, used for iCloud last-writer-wins.
    @Published var mysqlBaselineDone: Bool = false
    var mysqlBaselineAt: Date?
    @Published var eaBaselineDone: Bool = false
    var eaBaselineAt: Date?
    @Published var incrementalCursors: [String: Date] = [:]

    func markMySQLBaselineDone() { mysqlBaselineDone = true;  mysqlBaselineAt = Date() }
    func markEABaselineDone()    { eaBaselineDone = true;     eaBaselineAt = Date() }
    /// Clears a destination's baseline (e.g. its data was wiped) — stamps the
    /// LWW timestamp so the cleared state wins over an older completion on merge.
    func clearMySQLBaseline()    { mysqlBaselineDone = false; mysqlBaselineAt = Date() }
    func clearEABaseline()       { eaBaselineDone = false;    eaBaselineAt = Date() }

    /// A configured destination that has no baseline yet — needs a full sync to
    /// catch up to the other one.
    func mysqlNeedsFullSync(enabled: Bool) -> Bool { enabled && !mysqlBaselineDone }
    func eaNeedsFullSync(configured: Bool) -> Bool { configured && !eaBaselineDone }
    /// True when at least one enabled destination has a baseline, so incremental
    /// sync has somewhere to write. (False → a full sync is required first.)
    func anyBaselineReady(mysqlEnabled: Bool, eaConfigured: Bool) -> Bool {
        (mysqlEnabled && mysqlBaselineDone) || (eaConfigured && eaBaselineDone)
    }
    @Published var currentSyncCategoryIDs: Set<String> = []

    /// Drives the dashboard banner + phase-weighted progress during full sync.
    @Published var fullSyncPhase: FullSyncPhase = .idle

    var isAnySyncRunning: Bool { isFullSyncRunning || isIncrementalSyncRunning }

    // MARK: - Full-sync phase-weighted overall progress
    //
    // The full sync spans export (fast, local) + delivery (slow: EA upload +
    // MySQL drain). The overall bar must run 0→100% across ALL of it — not jump
    // to 100% the moment the local export finishes. Each component is a 0…1
    // fraction; the weights (which sum to 1) reflect the enabled destinations.
    private var fsExportFrac = 0.0
    private var fsEAFrac = 0.0
    private var fsMySQLFrac = 0.0
    private var fsWExport = 0.0
    private var fsWEA = 0.0
    private var fsWMySQL = 0.0
    /// True if the EA upload phase failed (surfaced as a red step + retry).
    @Published var fsEAFailed = false
    /// True once the dump is uploaded and the key delivered, while EA's
    /// server-side decrypt-and-import jobs are still running. EA is NOT considered
    /// baselined/done until those jobs report `completed` (polled via
    /// `bulkImportStatus`), so the EA step stays active and the sync isn't
    /// finalized on key-delivery alone.
    @Published var eaImporting = false
    /// Live server-side import progress (rows decrypted+inserted / expected),
    /// polled from `bulkImportStatus`, shown so a long import is visibly advancing.
    @Published var eaImportRowsImported = 0
    @Published var eaImportRowsExpected = 0

    func startFullSyncProgress(eaEnabled: Bool, mysqlEnabled: Bool) {
        fullSyncPhase = .exporting
        fsEAFailed = false
        eaImporting = false
        eaImportRowsImported = 0; eaImportRowsExpected = 0
        fsExportFrac = 0; fsEAFrac = 0; fsMySQLFrac = 0
        fsWExport = 0.15
        if eaEnabled && mysqlEnabled { fsWEA = 0.40; fsWMySQL = 0.45 }
        else if eaEnabled          { fsWEA = 0.85; fsWMySQL = 0.0 }
        else                       { fsWMySQL = 0.85; fsWEA = 0.0 }
        overallProgress = 0
    }

    /// Resume the delivering phase in a fresh state (e.g. a background drain
    /// instance): the export already happened, so it counts as done.
    func resumeDeliverProgress(eaEnabled: Bool, mysqlEnabled: Bool, eaDone: Bool) {
        startFullSyncProgress(eaEnabled: eaEnabled, mysqlEnabled: mysqlEnabled)
        fullSyncPhase = .delivering
        fsExportFrac = 1
        fsEAFrac = eaDone ? 1 : 0
        recomputeFullSyncProgress()
    }

    func updateFullSyncProgress(phase: FullSyncPhase? = nil,
                                export: Double? = nil, ea: Double? = nil, mysql: Double? = nil) {
        if let phase { fullSyncPhase = phase }
        if let export { fsExportFrac = export }
        if let ea { fsEAFrac = ea }
        if let mysql { fsMySQLFrac = mysql }
        recomputeFullSyncProgress()
    }

    private func recomputeFullSyncProgress() {
        overallProgress = min(1.0, fsWExport * fsExportFrac + fsWEA * fsEAFrac + fsWMySQL * fsMySQLFrac)
    }

    /// The 0…1 EA-delivery fraction, for callers that observe EA upload progress.
    var fullSyncEAFraction: Double { fsEAFrac }
    /// EA's weight in the overall bar — lets the background uploader compute the
    /// same overall progress for the live activity (base + weight × upload-fraction).
    var fullSyncEAWeight: Double { fsWEA }

    /// Sync fully delivered: pin the bar at 100%. Stays out of `recalcOverall`
    /// (phase != .idle) until the next sync resets the phase.
    func finishFullSyncProgress() {
        fsExportFrac = 1; fsEAFrac = 1; fsMySQLFrac = 1
        fullSyncPhase = .complete
        overallProgress = 1.0
    }

    func endFullSyncProgress() {
        fullSyncPhase = .idle
    }

    // MARK: - Full-sync step model (drives the color-coded stepper)

    var fullSyncIncludesEA: Bool { fsWEA > 0 }
    var fullSyncIncludesMySQL: Bool { fsWMySQL > 0 }
    // EA is "done" only when the upload finished AND the server-side import
    // completed (`!eaImporting`) — so a MySQL-triggered finalize can't complete
    // the run while EA is still importing.
    var fullSyncEADone: Bool { !fullSyncIncludesEA || (fsEAFrac >= 1 && !eaImporting) }
    var fullSyncMySQLDone: Bool { !fullSyncIncludesMySQL || fsMySQLFrac >= 1 }

    func markEADone() {
        fsEAFailed = false
        eaImporting = false
        fsEAFrac = 1
        recomputeFullSyncProgress()
    }
    func markEAFailed() { fsEAFailed = true; eaImporting = false }

    /// The high-level steps for the current full sync, in order, with status.
    /// Empty when not full-syncing. Export is always present; EA / MySQL appear
    /// only when that destination is part of this sync.
    var fullSyncStepStates: [(label: String, status: SyncStepStatus)] {
        guard fullSyncPhase != .idle else { return [] }
        var steps: [(String, SyncStepStatus)] = []
        steps.append(("Export", fullSyncPhase == .exporting ? .active : .done))
        if fullSyncIncludesEA {
            let s: SyncStepStatus = fsEAFailed ? .failed
                : eaImporting ? .active   // uploaded; server still importing
                : (fsEAFrac >= 1 ? .done : (fullSyncPhase == .delivering ? .active : .pending))
            steps.append(("EA", s))
        }
        if fullSyncIncludesMySQL {
            let s: SyncStepStatus = fsMySQLFrac >= 1 ? .done
                : (fullSyncPhase == .delivering ? .active : .pending)
            steps.append(("MySQL", s))
        }
        return steps
    }

    // MARK: - Incremental progress (explicit, unit-based)
    //
    // Driven directly by `runIncrementalSync` (units processed / total) rather
    // than by counting `.completed` category cards — those start the pass already
    // green, which made the bar begin near 100%. While active, `recalcOverall` is
    // a no-op so per-category updates don't clobber the explicit value, and the
    // category cards keep their real (mostly Synced) state instead of being reset.
    @Published var incrementalProgressActive = false

    func beginIncrementalProgress() {
        incrementalProgressActive = true
        overallProgress = 0
    }
    func setIncrementalProgress(_ fraction: Double) {
        guard incrementalProgressActive else { return }
        overallProgress = min(1, max(0, fraction))
    }
    func endIncrementalProgress() {
        incrementalProgressActive = false
    }

    /// Clears all category statuses, per-category counts, and the on-screen
    /// total at the start of a from-scratch full sync (which wipes the remote and
    /// re-uploads everything) so stale "Synced" badges and an old total don't
    /// confuse the user. Counts grow back as the export proceeds.
    func resetForFullSync() {
        for i in categories.indices {
            categories[i].status = .idle
            categories[i].recordCount = 0
            categories[i].currentProgress = 0
            categories[i].lastSyncDate = nil
        }
        totalRecords = 0
        lastSyncDate = nil
        overallProgress = 0
    }

    func updateCategory(_ id: String, status: SyncStatus? = nil, recordCount: Int? = nil,
                        lastSyncDate: Date? = nil, progress: Int? = nil, total: Int? = nil) {
        guard let idx = categories.firstIndex(where: { $0.id == id }) else { return }
        if let s = status { categories[idx].status = s }
        if let r = recordCount { categories[idx].recordCount = r }
        if let d = lastSyncDate { categories[idx].lastSyncDate = d }
        if let p = progress { categories[idx].currentProgress = p }
        if let t = total { categories[idx].totalEstimated = t }

        recalcOverall()
    }

    /// Called when the MySQL database is wiped from Settings ("Reset Database").
    /// Clears ONLY the MySQL baseline so the dashboard prompts a MySQL full sync;
    /// EA keeps its baseline and stays current via incremental. Incremental
    /// cursors and `lastSyncDate` are preserved (they keep EA on track and let
    /// MySQL resume cleanly after its re-baseline).
    func handleMySQLReset() {
        clearMySQLBaseline()
        errorMessage = nil
        persist()
    }

    func resetCategoryLocalState(_ id: String) {
        // Remove per-type incremental cursors belonging to this category
        let typeIDs = Self.typeIDs(forCategory: id)
        if typeIDs.isEmpty {
            // Special categories use category-level keys
            incrementalCursors.removeValue(forKey: id)
        } else {
            for typeID in typeIDs {
                incrementalCursors.removeValue(forKey: typeID)
            }
        }
        guard let idx = categories.firstIndex(where: { $0.id == id }) else { return }
        categories[idx].status = .idle
        categories[idx].recordCount = 0
        categories[idx].lastSyncDate = nil
        categories[idx].currentProgress = 0
        persist()
    }

    /// Maps a category ID (e.g. "qty_Activity", "cat_category") to the set of HK type identifiers it contains.
    static func typeIDs(forCategory catID: String) -> [String] {
        if catID.hasPrefix("qty_") {
            let rawCat = String(catID.dropFirst(4))
            guard let cat = HealthCategory(rawValue: rawCat) else { return [] }
            return HealthDataTypes.allQuantityTypes.filter { $0.category == cat }.map(\.id)
        }
        if catID == "cat_category" {
            return HealthDataTypes.allCategoryTypes.map(\.id)
        }
        // Special categories (workouts, bp, ecg, etc.) are atomic — no sub-types
        return []
    }

    func recalcOverall() {
        // During full sync the orchestrator drives overallProgress via the
        // phase-weighted model above, and during incremental the unit-based model
        // drives it; don't let per-category updates clobber either.
        guard fullSyncPhase == .idle, !incrementalProgressActive else { return }
        if !currentSyncCategoryIDs.isEmpty {
            let syncCats = categories.filter { currentSyncCategoryIDs.contains($0.id) }
            let total = Double(syncCats.count)
            guard total > 0 else {
                overallProgress = 0
                return
            }
            let completed = Double(syncCats.filter {
                if case .completed = $0.status { return true }
                if case .failed = $0.status { return true }
                return false
            }.count)
            let syncingProgress = syncCats.filter { $0.status.isActive }.map { $0.progressFraction }.reduce(0, +)
            overallProgress = (completed + syncingProgress) / total
        } else {
            let total = Double(categories.count)
            guard total > 0 else {
                overallProgress = 0
                return
            }
            let completedCount = Double(categories.filter { $0.status == .completed }.count)
            let syncingProgress = categories.filter { $0.status.isActive }.map { $0.progressFraction }.reduce(0, +)
            overallProgress = (completedCount + syncingProgress) / total
        }
    }

    // MARK: - Persistence

    private static let userDefaultsKey = "com.healthbeat.syncSnapshot"

    func persist() {
        let snap = PersistedSnapshot(
            lastSyncDate: lastSyncDate,
            categories: categories.map {
                PersistedCategory(
                    id: $0.id,
                    recordCount: $0.recordCount,
                    lastSyncDate: $0.lastSyncDate,
                    completed: $0.status == .completed
                )
            },
            totalRecords: totalRecords,
            incrementalCursors: incrementalCursors.isEmpty ? nil : incrementalCursors,
            mysqlBaselineDone: mysqlBaselineDone,
            mysqlBaselineAt: mysqlBaselineAt,
            eaBaselineDone: eaBaselineDone,
            eaBaselineAt: eaBaselineAt,
            hasCompletedFullSync: nil,   // legacy — no longer written
            fullSyncStateAt: nil
        )
        if let data = try? JSONEncoder().encode(snap) {
            UserDefaults.standard.set(data, forKey: Self.userDefaultsKey)
        }
        iCloudSyncService.shared.pushSyncSnapshot(snap)
    }

    func restore() {
        iCloudSyncService.shared.pullSyncSnapshot()
        guard
            let data = UserDefaults.standard.data(forKey: Self.userDefaultsKey),
            let snap = try? JSONDecoder().decode(PersistedSnapshot.self, from: data)
        else { return }
        lastSyncDate = snap.lastSyncDate
        if let saved = snap.totalRecords { totalRecords = saved }
        incrementalCursors = snap.incrementalCursors ?? [:]

        if snap.mysqlBaselineDone != nil || snap.eaBaselineDone != nil {
            // New per-destination snapshot — load directly.
            mysqlBaselineDone = snap.mysqlBaselineDone ?? false
            mysqlBaselineAt = snap.mysqlBaselineAt
            eaBaselineDone = snap.eaBaselineDone ?? false
            eaBaselineAt = snap.eaBaselineAt
        } else {
            // One-time upgrade migration from the single `hasCompletedFullSync`.
            // The dump-based EA full sync never shipped, so any prior completion
            // (or evidence of a working MySQL sync — a recorded last-sync or live
            // incremental cursors) maps to a MySQL baseline only. EA starts with
            // no baseline, so the dashboard correctly prompts an EA full sync on
            // devices where EA was enabled after MySQL. Using `fullSyncStateAt` /
            // `lastSyncDate` (not "now") as the timestamp keeps a genuine remote
            // reset winning on iCloud merge. A fresh install matches neither and
            // still goes through the full-sync flow for both.
            let hadMySQLBaseline = (snap.hasCompletedFullSync ?? false)
                || lastSyncDate != nil || !incrementalCursors.isEmpty
            if hadMySQLBaseline {
                mysqlBaselineDone = true
                mysqlBaselineAt = snap.fullSyncStateAt ?? lastSyncDate ?? Date(timeIntervalSince1970: 0)
            }
            eaBaselineDone = false
            eaBaselineAt = nil
        }

        for persisted in snap.categories {
            guard let idx = categories.firstIndex(where: { $0.id == persisted.id }) else { continue }
            categories[idx].recordCount = persisted.recordCount
            categories[idx].lastSyncDate = persisted.lastSyncDate
            if persisted.completed { categories[idx].status = .completed }
        }
        recalcOverall()
    }
}
