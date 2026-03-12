import Combine
import Foundation
import HealthKit
import SwiftUI

@MainActor
final class SyncViewModel: ObservableObject {

    private(set) var syncState: SyncState
    private let syncService: SyncService
    private var cancellables = Set<AnyCancellable>()
    private var syncTask: Task<Void, Never>?

    @Published var prerequisiteIssues: [SyncPrerequisiteIssue] = []
    @Published var showPrerequisiteAlert = false

    init() {
        let state = SyncState()
        self.syncState = state
        self.syncService = SyncService(syncState: state)
        // Forward SyncState changes so SwiftUI views subscribed to this
        // view model re-render whenever any SyncState @Published property changes.
        state.objectWillChange
            .sink { [weak self] in self?.objectWillChange.send() }
            .store(in: &cancellables)

        // Reset local state when the database is wiped from Settings.
        NotificationCenter.default.publisher(for: .healthBeatDatabaseDidReset)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in self?.syncState.resetAllLocalState() }
            .store(in: &cancellables)
    }

    var categories: [CategorySyncState] { syncState.categories }
    var isFullSyncRunning: Bool { syncState.isFullSyncRunning }
    var isAnySyncRunning: Bool { syncState.isAnySyncRunning }
    var totalRecords: Int { syncState.totalRecords }
    var lastSyncDate: Date? { syncState.lastSyncDate }
    var overallProgress: Double { syncState.overallProgress }
    var currentOperation: String { syncState.currentOperation }
    var errorMessage: String? { syncState.errorMessage }
    var hasCompletedFullSync: Bool { syncState.hasCompletedFullSync }

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

    func startFullSync() {
        let config = MySQLConfig.load()
        // Fire off prerequisite validation without blocking the sync
        Task {
            let issues = await syncService.validatePrerequisites(config: config)
            self.prerequisiteIssues = issues
            if !issues.isEmpty {
                self.showPrerequisiteAlert = true
            }
        }
        let task = Task {
            await syncService.runFullSync(config: config)
            refreshRecordCounts()
            refreshLatestHealthKitDates()
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
