import Foundation
import CryptoKit

/// Logical tables that the historical export covers. Each maps the in-tree
/// MySQL table name (used by the on-device drain) to the EA `hb_*` table name
/// (used as the bulk-import URL segment and validated server-side).
///
/// This is the single source of truth tying the dump file name, the MySQL
/// drain target, and the EA upload endpoint together. The list mirrors what
/// the full-sync export (`SyncService.exportDump`) produces — the live/bidirectional
/// paths (geofences, place categories, sync log) are intentionally excluded.
enum DumpTable: String, CaseIterable, Codable, Sendable {
    case quantitySamples
    case categorySamples
    case workouts
    case workoutRoutes
    case bloodPressure
    case ecg
    case audiograms
    case activitySummaries
    case medications
    case visionPrescriptions
    case stateOfMind

    /// In-tree MySQL table (drain target). Matches `SyncService`'s INSERT names.
    var sqlTable: String {
        switch self {
        case .quantitySamples:    return "health_quantity_samples"
        case .categorySamples:    return "health_category_samples"
        case .workouts:           return "health_workouts"
        case .workoutRoutes:      return "health_workout_routes"
        case .bloodPressure:      return "health_blood_pressure"
        case .ecg:                return "health_ecg"
        case .audiograms:         return "health_audiograms"
        case .activitySummaries:  return "health_activity_summaries"
        case .medications:        return "health_medications"
        case .visionPrescriptions:return "health_vision_prescriptions"
        case .stateOfMind:        return "health_state_of_mind"
        }
    }

    /// EA `hb_*` table name — the `{table}` segment of `/bulk-import/{run}/{table}`.
    var eaTable: String {
        "hb_" + String(sqlTable.dropFirst("health_".count))
    }

    /// File name inside the run directory.
    var fileName: String { "\(rawValue).ndjson.gz.enc" }
}

/// Written alongside the per-table `.enc` files. Drives the drain and the
/// EA upload, and records the reconcile window so deletes can be replayed.
struct DumpManifest: Codable, Sendable {
    var runID: String
    var schemaVersion: Int
    var createdAt: String              // ISO-8601
    /// Reconcile window covered by this export (UTC, "yyyy-MM-dd HH:mm:ss.SSS").
    var since: String
    var until: String
    var tables: [TableEntry]

    struct TableEntry: Codable, Sendable {
        var table: DumpTable
        var rowCount: Int
        var fileName: String
    }

    static let fileName = "manifest.json"
}

/// A `BackendWriter` that serialises every batch to encrypted NDJSON frames on
/// disk instead of POSTing it. Reusing the writer abstraction means the export
/// captures *exactly* the `HB*Row` values the live EA path produces — no second
/// mapping to drift out of sync. One `DumpFrameWriter` per table, created lazily.
///
/// An actor so the file handles are touched serially and the type is `Sendable`.
actor DumpFileBackendWriter: BackendWriter {
    private let runDir: URL
    private let key: SymmetricKey
    private let encoder: JSONEncoder
    private var writers: [DumpTable: DumpFrameWriter] = [:]
    private var counts: [DumpTable: Int] = [:]

    init(runDir: URL, key: SymmetricKey) {
        self.runDir = runDir
        self.key = key
        let enc = JSONEncoder()
        enc.outputFormatting = []   // compact, one object per line
        self.encoder = enc
    }

    private func writer(for table: DumpTable) throws -> DumpFrameWriter {
        if let w = writers[table] { return w }
        let w = try DumpFrameWriter(fileURL: runDir.appendingPathComponent(table.fileName), key: key)
        writers[table] = w
        return w
    }

    private func append<T: Encodable>(_ rows: [T], to table: DumpTable) throws {
        guard !rows.isEmpty else { return }
        let w = try writer(for: table)
        for row in rows {
            try w.append(line: try encoder.encode(row))
        }
        counts[table, default: 0] += rows.count
    }

    /// Close all open files and return per-table row counts for the manifest.
    func finish() throws -> [DumpTable: Int] {
        for w in writers.values { try w.close() }
        writers.removeAll()
        return counts
    }

    // MARK: BackendWriter — the 11 tables the export covers

    func writeQuantitySamples(_ rows: [HBQuantityRow]) async throws       { try append(rows, to: .quantitySamples) }
    func writeCategorySamples(_ rows: [HBCategoryRow]) async throws       { try append(rows, to: .categorySamples) }
    func writeWorkouts(_ rows: [HBWorkoutRow]) async throws               { try append(rows, to: .workouts) }
    func writeWorkoutRoutes(_ rows: [HBWorkoutRouteRow]) async throws     { try append(rows, to: .workoutRoutes) }
    func writeBloodPressure(_ rows: [HBBloodPressureRow]) async throws    { try append(rows, to: .bloodPressure) }
    func writeEcg(_ rows: [HBEcgRow]) async throws                        { try append(rows, to: .ecg) }
    func writeAudiograms(_ rows: [HBAudiogramRow]) async throws           { try append(rows, to: .audiograms) }
    func writeActivitySummaries(_ rows: [HBActivitySummaryRow]) async throws { try append(rows, to: .activitySummaries) }
    func writeMedications(_ rows: [HBMedicationRow]) async throws         { try append(rows, to: .medications) }
    func writeVisionPrescriptions(_ rows: [HBVisionRow]) async throws     { try append(rows, to: .visionPrescriptions) }
    func writeStateOfMind(_ rows: [HBStateOfMindRow]) async throws        { try append(rows, to: .stateOfMind) }

    // MARK: BackendWriter — not part of the historical health dump (no-ops).
    // Location tracks, geofences, place categories and the sync log flow
    // through the live / bidirectional paths, never the export loop.

    func writeLocationTracks(_ rows: [HBLocationRow]) async throws {}
    func writeGeofenceEvent(_ row: HBGeofenceEventRow) async throws {}
    func writeGeofenceDefinitions(_ rows: [HBGeofenceDefinitionRow]) async throws {}
    func writePlaceCategories(_ rows: [HBPlaceCategoryRow]) async throws {}
    func writeSyncLog(_ row: HBSyncLogRow) async throws {}

    /// Reconcile is skipped during export (the drain / server replay it from the
    /// manifest window), so this is never called — implemented as a no-op.
    func reconcileSlice(
        table: String, typeColumn: String?, typeValue: String?,
        since: String, until: String, validUUIDs: [String]
    ) async throws -> Int { 0 }
}

/// Filesystem layout + lifecycle for export runs. All runs live under one
/// parent directory in Caches so the OS can reclaim space and the startup
/// sweep can find orphans. Files carry `NSFileProtectionComplete`; even so,
/// a leaked file is AES-256-GCM ciphertext and useless without the run key.
enum DumpStore {
    /// `<Caches>/hb-export`
    static var root: URL {
        let caches = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        return caches.appendingPathComponent("hb-export", isDirectory: true)
    }

    static func runDir(_ runID: String) -> URL {
        root.appendingPathComponent(runID, isDirectory: true)
    }

    @discardableResult
    static func createRunDir(_ runID: String) throws -> URL {
        let dir = runDir(runID)
        try FileManager.default.createDirectory(
            at: dir, withIntermediateDirectories: true,
            // Match the dump files: readable by background tasks after first unlock.
            attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        )
        return dir
    }

    static func manifestURL(_ runID: String) -> URL {
        runDir(runID).appendingPathComponent(DumpManifest.fileName)
    }

    static func loadManifest(_ runID: String) -> DumpManifest? {
        guard let data = try? Data(contentsOf: manifestURL(runID)) else { return nil }
        return try? JSONDecoder().decode(DumpManifest.self, from: data)
    }

    /// Delete a run's directory AND its Keychain key. Call after both backends
    /// confirm, or from the startup sweep for orphans.
    static func wipeRun(_ runID: String) {
        try? FileManager.default.removeItem(at: runDir(runID))
        DumpKeychain.delete(runID: runID)
        for k in ["phase.drain.\(runID)", "phase.eaKey.\(runID)", "eaRequired.\(runID)",
                  "mysqlRequired.\(runID)", "pendingDumpDrainRunID", "pendingEAUploadRunID"] {
            UserDefaults.standard.removeObject(forKey: k)
        }
    }

    // MARK: Completion coordination
    //
    // The dump files feed BOTH the MySQL drain and the EA upload. They must not
    // be wiped until every required destination has consumed them. Each phase
    // reports in; `wipeIfComplete` removes the run once all required phases are
    // done. `eaRequired` is recorded at export time (was EA configured?).

    static func setEARequired(_ required: Bool, runID: String) {
        UserDefaults.standard.set(required, forKey: "eaRequired.\(runID)")
    }

    static func setMySQLRequired(_ required: Bool, runID: String) {
        UserDefaults.standard.set(required, forKey: "mysqlRequired.\(runID)")
    }

    /// Whether EA delivery was part of this run (recorded at export time).
    static func isEARequired(_ runID: String) -> Bool {
        UserDefaults.standard.bool(forKey: "eaRequired.\(runID)")
    }

    /// True if EA delivery is done or wasn't required for this run.
    static func isEADelivered(_ runID: String) -> Bool {
        !UserDefaults.standard.bool(forKey: "eaRequired.\(runID)")
            || UserDefaults.standard.bool(forKey: "phase.eaKey.\(runID)")
    }

    /// Marks the MySQL drain done. Returns true if the whole run is now complete
    /// (all required destinations confirmed) — in which case the dump was wiped.
    @discardableResult
    static func markDrainDone(_ runID: String) -> Bool {
        UserDefaults.standard.set(true, forKey: "phase.drain.\(runID)")
        return wipeIfComplete(runID)
    }

    /// Marks the EA key delivered. Returns true if the whole run is now complete.
    @discardableResult
    static func markEAKeyDelivered(_ runID: String) -> Bool {
        UserDefaults.standard.set(true, forKey: "phase.eaKey.\(runID)")
        return wipeIfComplete(runID)
    }

    @discardableResult
    private static func wipeIfComplete(_ runID: String) -> Bool {
        let mysqlRequired = UserDefaults.standard.bool(forKey: "mysqlRequired.\(runID)")
        let eaRequired = UserDefaults.standard.bool(forKey: "eaRequired.\(runID)")
        let drainDone = !mysqlRequired || UserDefaults.standard.bool(forKey: "phase.drain.\(runID)")
        let eaDone = !eaRequired || UserDefaults.standard.bool(forKey: "phase.eaKey.\(runID)")
        if drainDone && eaDone { wipeRun(runID); return true }
        return false
    }

    /// Remove orphaned run directories older than `maxAge` with no active sync.
    /// Belt-and-braces against a crash leaking health data on disk.
    static func sweepOrphans(activeRunID: String?, maxAge: TimeInterval = 3 * 24 * 3600) {
        let fm = FileManager.default
        guard let entries = try? fm.contentsOfDirectory(
            at: root, includingPropertiesForKeys: [.creationDateKey],
            options: [.skipsHiddenFiles]
        ) else { return }
        let cutoff = Date().addingTimeInterval(-maxAge)
        for dir in entries {
            let runID = dir.lastPathComponent
            if runID == activeRunID { continue }
            let created = (try? dir.resourceValues(forKeys: [.creationDateKey]))?.creationDate ?? .distantPast
            if created < cutoff { wipeRun(runID) }
        }
    }
}
