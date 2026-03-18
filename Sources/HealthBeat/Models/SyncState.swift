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
    let hasCompletedFullSync: Bool?
    let backfillCursors: [String: Date]?
    let backfillAnchorDate: Date?
    let incrementalCursors: [String: Date]?
}

// MARK: -

enum SyncStatus: Equatable {
    case idle
    case syncing
    case completed
    case failed(String)

    var label: String {
        switch self {
        case .idle: return "Idle"
        case .syncing: return "Syncing…"
        case .completed: return "Synced"
        case .failed: return "Error"
        }
    }

    var isActive: Bool {
        if case .syncing = self { return true }
        return false
    }
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
    @Published var hasCompletedFullSync: Bool = false
    @Published var backfillCursors: [String: Date] = [:]
    @Published var backfillAnchorDate: Date?
    @Published var incrementalCursors: [String: Date] = [:]
    @Published var currentSyncCategoryIDs: Set<String> = []

    var isAnySyncRunning: Bool { isFullSyncRunning || isIncrementalSyncRunning }

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

    func resetAllLocalState() {
        backfillCursors = [:]
        backfillAnchorDate = nil
        incrementalCursors = [:]
        hasCompletedFullSync = false
        lastSyncDate = nil
        totalRecords = 0
        currentOperation = ""
        errorMessage = nil
        for i in categories.indices {
            categories[i].status = .idle
            categories[i].recordCount = 0
            categories[i].lastSyncDate = nil
            categories[i].currentProgress = 0
        }
        persist()
    }

    func resetCategoryLocalState(_ id: String) {
        backfillCursors.removeValue(forKey: id)
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
            hasCompletedFullSync: hasCompletedFullSync,
            backfillCursors: backfillCursors.isEmpty ? nil : backfillCursors,
            backfillAnchorDate: backfillAnchorDate,
            incrementalCursors: incrementalCursors.isEmpty ? nil : incrementalCursors
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
        hasCompletedFullSync = snap.hasCompletedFullSync ?? false
        backfillCursors = snap.backfillCursors ?? [:]
        backfillAnchorDate = snap.backfillAnchorDate
        incrementalCursors = snap.incrementalCursors ?? [:]
        for persisted in snap.categories {
            guard let idx = categories.firstIndex(where: { $0.id == persisted.id }) else { continue }
            categories[idx].recordCount = persisted.recordCount
            categories[idx].lastSyncDate = persisted.lastSyncDate
            if persisted.completed { categories[idx].status = .completed }
        }
        recalcOverall()
    }
}
