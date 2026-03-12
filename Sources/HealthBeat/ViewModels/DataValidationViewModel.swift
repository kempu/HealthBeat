import Foundation
import HealthKit

enum ScanDepth: String, CaseIterable, Identifiable {
    case quick = "Quick"
    case deep = "Deep"
    var id: String { rawValue }
}

struct DeepStats {
    var missing: Int = 0
    var corrupted: Int = 0
    var fixed: Int = 0
}

struct ValidationResult: Identifiable {
    let id: String
    let displayName: String
    let systemImage: String
    let depth: ScanDepth
    // Quick mode
    let hkCount: Int
    let dbCount: Int
    // Deep mode
    var deepStats: DeepStats?

    var isInSync: Bool {
        if let ds = deepStats { return ds.missing == 0 && ds.corrupted == 0 }
        return hkCount == dbCount
    }
    var missingCount: Int {
        deepStats?.missing ?? max(0, hkCount - dbCount)
    }
    var corruptedCount: Int { deepStats?.corrupted ?? 0 }
    var fixedCount: Int { deepStats?.fixed ?? 0 }
    var totalIssues: Int { missingCount + corruptedCount }
}

@MainActor
final class DataValidationViewModel: ObservableObject {

    @Published var results: [ValidationResult] = []
    @Published var isValidating = false
    @Published var validationDate: Date?
    @Published var errorMessage: String?
    @Published var progress: Int = 0
    @Published var progressTotal: Int = 0
    @Published var currentScanDetail: String = ""
    @Published var scanDepth: ScanDepth = .quick
    @Published var autoFix: Bool = false
    @Published var repairingCategoryID: String?

    let syncViewModel: SyncViewModel
    private var validationTask: Task<Void, Never>?

    init(syncViewModel: SyncViewModel) {
        self.syncViewModel = syncViewModel
    }

    var outOfSyncCount: Int { results.filter { !$0.isInSync }.count }
    var totalMissing: Int { results.map(\.missingCount).reduce(0, +) }
    var totalCorrupted: Int { results.map(\.corruptedCount).reduce(0, +) }

    // MARK: - Public actions

    func runValidation() {
        guard !isValidating, !syncViewModel.isAnySyncRunning else { return }
        validationTask = Task { await performValidation() }
    }

    func cancelValidation() {
        validationTask?.cancel()
        validationTask = nil
    }

    /// Repair a specific category: deep scan with auto-fix for qty/cat; full category sync for specials.
    func repairCategory(_ categoryID: String) {
        guard !isValidating else { return }
        validationTask = Task { await performRepair(categoryID: categoryID) }
    }

    func repairAllMissing() {
        let outOfSync = results.filter { !$0.isInSync }.map(\.id)
        guard !outOfSync.isEmpty, !isValidating else { return }
        validationTask = Task {
            for catID in outOfSync {
                guard !Task.isCancelled else { break }
                await performRepair(categoryID: catID)
            }
        }
    }

    // MARK: - Validation

    private func performValidation() async {
        isValidating = true
        errorMessage = nil
        results = []
        progress = 0
        progressTotal = syncViewModel.categories.count
        currentScanDetail = ""
        defer {
            isValidating = false
            currentScanDetail = ""
        }

        let config = MySQLConfig.load()
        let mysql = MySQLService()
        do {
            try await mysql.connect(config: config)
        } catch {
            errorMessage = "Cannot connect to database: \(error.localizedDescription)"
            return
        }

        var built: [ValidationResult] = []
        for catState in syncViewModel.categories {
            guard !Task.isCancelled else { break }
            currentScanDetail = catState.displayName

            let result: ValidationResult
            if scanDepth == .quick {
                let hkCount = await countHealthKit(for: catState.id)
                let dbCount = await countDB(categoryID: catState.id, mysql: mysql)
                result = ValidationResult(
                    id: catState.id,
                    displayName: catState.displayName,
                    systemImage: catState.systemImage,
                    depth: .quick,
                    hkCount: hkCount,
                    dbCount: dbCount
                )
            } else {
                result = await deepScan(
                    categoryID: catState.id,
                    displayName: catState.displayName,
                    systemImage: catState.systemImage,
                    mysql: mysql,
                    autoFix: autoFix
                )
            }
            built.append(result)
            progress += 1
        }

        await mysql.disconnect()

        if !Task.isCancelled {
            results = built.sorted { lhs, rhs in
                if lhs.isInSync != rhs.isInSync { return !lhs.isInSync }
                return lhs.displayName < rhs.displayName
            }
            validationDate = Date()
        }
    }

    // MARK: - Repair

    private func performRepair(categoryID: String) async {
        guard let existing = results.first(where: { $0.id == categoryID }) else { return }
        isValidating = true
        repairingCategoryID = categoryID
        currentScanDetail = "Repairing \(existing.displayName)…"
        defer {
            isValidating = false
            repairingCategoryID = nil
            currentScanDetail = ""
        }

        // Quantity and category samples: targeted REPLACE INTO via deep scan.
        if categoryID.hasPrefix("qty_") || categoryID == "cat_category" {
            let config = MySQLConfig.load()
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)
            } catch {
                errorMessage = "Cannot connect to database: \(error.localizedDescription)"
                return
            }
            let repaired = await deepScan(
                categoryID: categoryID,
                displayName: existing.displayName,
                systemImage: existing.systemImage,
                mysql: mysql,
                autoFix: true
            )
            await mysql.disconnect()
            if let idx = results.firstIndex(where: { $0.id == categoryID }) {
                results[idx] = repaired
            }

        } else {
            // Special categories: use the category sync (INSERT IGNORE from epoch).
            // This re-syncs only the one category and is safe/idempotent.
            syncViewModel.repairCategories(categoryIDs: [categoryID])
            // Brief pause so the sync task has time to set isAnySyncRunning = true.
            try? await Task.sleep(nanoseconds: 200_000_000)
            // Wait for the sync to finish so we can refresh the result.
            while syncViewModel.isAnySyncRunning {
                try? await Task.sleep(nanoseconds: 300_000_000)
            }
            // Refresh this category's result with a fresh count.
            let config = MySQLConfig.load()
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)
                let hkCount = await countHealthKit(for: categoryID)
                let dbCount = await countDB(categoryID: categoryID, mysql: mysql)
                await mysql.disconnect()
                if let idx = results.firstIndex(where: { $0.id == categoryID }) {
                    results[idx] = ValidationResult(
                        id: categoryID,
                        displayName: existing.displayName,
                        systemImage: existing.systemImage,
                        depth: existing.depth,
                        hkCount: hkCount,
                        dbCount: dbCount
                    )
                }
            } catch {}
        }
        validationDate = Date()
    }

    // MARK: - Deep scan (full history, per-record comparison)

    private func deepScan(
        categoryID: String,
        displayName: String,
        systemImage: String,
        mysql: MySQLService,
        autoFix: Bool
    ) async -> ValidationResult {
        var stats = DeepStats()

        if categoryID.hasPrefix("qty_") {
            let rawCat = String(categoryID.dropFirst(4))
            if let cat = HealthCategory.allCases.first(where: { $0.rawValue == rawCat }),
               let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == cat })?.1 {
                for typeDesc in types {
                    guard !Task.isCancelled else { break }
                    let partial = await deepScanQuantityType(
                        typeDesc: typeDesc, mysql: mysql, autoFix: autoFix, categoryName: displayName
                    )
                    stats.missing += partial.missing
                    stats.corrupted += partial.corrupted
                    stats.fixed += partial.fixed
                }
            }
        } else if categoryID == "cat_category" {
            for typeDesc in HealthDataTypes.allCategoryTypes {
                guard !Task.isCancelled else { break }
                let partial = await deepScanCategoryType(
                    typeDesc: typeDesc, mysql: mysql, autoFix: autoFix, categoryName: displayName
                )
                stats.missing += partial.missing
                stats.corrupted += partial.corrupted
                stats.fixed += partial.fixed
            }
        } else {
            stats = await deepScanSpecialCategory(
                categoryID: categoryID, displayName: displayName, mysql: mysql
            )
        }

        return ValidationResult(
            id: categoryID,
            displayName: displayName,
            systemImage: systemImage,
            depth: .deep,
            hkCount: 0,
            dbCount: 0,
            deepStats: stats
        )
    }

    // MARK: - Quantity sample deep scan

    private func deepScanQuantityType(
        typeDesc: QuantityTypeDescriptor,
        mysql: MySQLService,
        autoFix: Bool,
        categoryName: String
    ) async -> DeepStats {
        var stats = DeepStats()
        var batchNum = 0
        do {
            try await HealthKitService.shared.streamQuantitySamples(
                typeID: typeDesc.hkIdentifier,
                from: nil
            ) { samples in
                if Task.isCancelled { throw CancellationError() }
                batchNum += 1
                currentScanDetail = "\(categoryName) · \(typeDesc.id) · batch \(batchNum)"

                let uuids = samples.map { $0.uuid.uuidString }
                let inList = uuids.map { "'\(MySQLEscape.escapeString($0))'" }.joined(separator: ",")
                let rows = (try? await mysql.query(
                    "SELECT uuid, value, start_date, end_date FROM health_quantity_samples WHERE uuid IN (\(inList))"
                )) ?? []

                var dbMap: [String: (value: Double, start: String, end: String)] = [:]
                for row in rows {
                    if let u = row["uuid"],
                       let v = row["value"].flatMap(Double.init),
                       let s = row["start_date"],
                       let e = row["end_date"] {
                        dbMap[u] = (v, s, e)
                    }
                }

                var toReplace: [HKQuantitySample] = []
                for s in samples {
                    let u = s.uuid.uuidString
                    if let db = dbMap[u] {
                        let hkVal = s.quantity.doubleValue(for: typeDesc.unit)
                        let hkStart = sqlDateStr(s.startDate)
                        let hkEnd = sqlDateStr(s.endDate)
                        if abs(hkVal - db.value) > 1e-9 || hkStart != db.start || hkEnd != db.end {
                            stats.corrupted += 1
                            if autoFix { toReplace.append(s) }
                        }
                    } else {
                        stats.missing += 1
                        if autoFix { toReplace.append(s) }
                    }
                }

                if autoFix, !toReplace.isEmpty {
                    let typeName = typeDesc.id
                    let unitStr = typeDesc.unit.unitString
                    let valuesList = toReplace.map { s -> String in
                        let uuid   = MySQLEscape.quote(s.uuid.uuidString)
                        let value  = MySQLEscape.quoteDouble(s.quantity.doubleValue(for: typeDesc.unit))
                        let start  = MySQLEscape.quote(sqlDateStr(s.startDate))
                        let end    = MySQLEscape.quote(sqlDateStr(s.endDate))
                        let src    = MySQLEscape.quote(s.sourceDisplayName)
                        let bundle = MySQLEscape.quote(s.sourceBundleID)
                        let device = MySQLEscape.quote(s.deviceName)
                        let meta   = MySQLEscape.quote(s.jsonMetadata())
                        return "(\(uuid),'\(MySQLEscape.escapeString(typeName))',\(value),'\(MySQLEscape.escapeString(unitStr))',\(start),\(end),\(src),\(bundle),\(device),\(meta))"
                    }.joined(separator: ",")
                    _ = try? await mysql.execute("""
                        REPLACE INTO health_quantity_samples
                          (uuid,type,value,unit,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
                        VALUES \(valuesList)
                        """)
                    stats.fixed += toReplace.count
                }
            }
        } catch {
            // Non-fatal: partial results are still useful.
        }
        return stats
    }

    // MARK: - Category sample deep scan

    private func deepScanCategoryType(
        typeDesc: CategoryTypeDescriptor,
        mysql: MySQLService,
        autoFix: Bool,
        categoryName: String
    ) async -> DeepStats {
        var stats = DeepStats()
        var batchNum = 0
        do {
            try await HealthKitService.shared.streamCategorySamples(
                typeID: typeDesc.hkIdentifier,
                from: nil
            ) { samples in
                if Task.isCancelled { throw CancellationError() }
                batchNum += 1
                currentScanDetail = "\(categoryName) · \(typeDesc.id) · batch \(batchNum)"

                let uuids = samples.map { $0.uuid.uuidString }
                let inList = uuids.map { "'\(MySQLEscape.escapeString($0))'" }.joined(separator: ",")
                let rows = (try? await mysql.query(
                    "SELECT uuid, value, start_date, end_date FROM health_category_samples WHERE uuid IN (\(inList))"
                )) ?? []

                var dbMap: [String: (value: Int, start: String, end: String)] = [:]
                for row in rows {
                    if let u = row["uuid"],
                       let v = row["value"].flatMap(Int.init),
                       let s = row["start_date"],
                       let e = row["end_date"] {
                        dbMap[u] = (v, s, e)
                    }
                }

                var toReplace: [HKCategorySample] = []
                for s in samples {
                    let u = s.uuid.uuidString
                    if let db = dbMap[u] {
                        let hkStart = sqlDateStr(s.startDate)
                        let hkEnd = sqlDateStr(s.endDate)
                        if s.value != db.value || hkStart != db.start || hkEnd != db.end {
                            stats.corrupted += 1
                            if autoFix { toReplace.append(s) }
                        }
                    } else {
                        stats.missing += 1
                        if autoFix { toReplace.append(s) }
                    }
                }

                if autoFix, !toReplace.isEmpty {
                    let typeName = typeDesc.id
                    let valuesList = toReplace.map { s -> String in
                        let uuid   = MySQLEscape.quote(s.uuid.uuidString)
                        let value  = s.value
                        let label  = MySQLEscape.quote(typeDesc.valueLabels[s.value] ?? "\(s.value)")
                        let start  = MySQLEscape.quote(sqlDateStr(s.startDate))
                        let end    = MySQLEscape.quote(sqlDateStr(s.endDate))
                        let src    = MySQLEscape.quote(s.sourceDisplayName)
                        let bundle = MySQLEscape.quote(s.sourceBundleID)
                        let device = MySQLEscape.quote(s.deviceName)
                        return "(\(uuid),'\(MySQLEscape.escapeString(typeName))',\(value),\(label),\(start),\(end),\(src),\(bundle),\(device),NULL)"
                    }.joined(separator: ",")
                    _ = try? await mysql.execute("""
                        REPLACE INTO health_category_samples
                          (uuid,type,value,value_label,start_date,end_date,source_name,source_bundle_id,device_name,metadata)
                        VALUES \(valuesList)
                        """)
                    stats.fixed += toReplace.count
                }
            }
        } catch {
            // Non-fatal.
        }
        return stats
    }

    // MARK: - Special category deep scan (UUID-only comparison)

    private func deepScanSpecialCategory(
        categoryID: String,
        displayName: String,
        mysql: MySQLService
    ) async -> DeepStats {
        var stats = DeepStats()
        currentScanDetail = "Scanning \(displayName)…"

        // Activity summaries and workout routes: no UUID-level comparison available.
        if categoryID == "cat_activity_summaries" || categoryID == "cat_workout_routes" {
            let hkCount = await countHealthKit(for: categoryID)
            let dbCount = await countDB(categoryID: categoryID, mysql: mysql)
            stats.missing = max(0, hkCount - dbCount)
            return stats
        }

        // Load DB UUIDs for this category.
        let dbUUIDs = Set(await fetchSpecialDBUUIDs(categoryID: categoryID, mysql: mysql))

        switch categoryID {
        case "cat_workouts":
            var checked = 0
            do {
                try await HealthKitService.shared.streamWorkouts(from: nil) { workouts in
                    if Task.isCancelled { throw CancellationError() }
                    for w in workouts where !dbUUIDs.contains(w.uuid.uuidString) {
                        stats.missing += 1
                    }
                    checked += workouts.count
                    currentScanDetail = "\(displayName): \(checked) checked"
                }
            } catch {}

        case "cat_bp":
            let items = (try? await HealthKitService.shared.fetchBloodPressure()) ?? []
            for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }

        case "cat_ecg":
            let items = (try? await HealthKitService.shared.fetchECG()) ?? []
            for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }

        case "cat_audiogram":
            let items = (try? await HealthKitService.shared.fetchAudiograms()) ?? []
            for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }

        case "cat_medications":
            if #available(iOS 26, *) {
                let items = (try? await HealthKitService.shared.fetchMedicationDoseEvents()) ?? []
                for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }
            }

        case "cat_vision":
            let items = (try? await HealthKitService.shared.fetchVisionPrescriptions()) ?? []
            for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }

        case "cat_state_of_mind":
            if #available(iOS 18, *) {
                let items = (try? await HealthKitService.shared.fetchStateOfMind()) ?? []
                for item in items where !dbUUIDs.contains(item.uuid.uuidString) { stats.missing += 1 }
            }

        default:
            break
        }

        return stats
    }

    private func fetchSpecialDBUUIDs(categoryID: String, mysql: MySQLService) async -> [String] {
        let table: String
        switch categoryID {
        case "cat_workouts":       table = "health_workouts"
        case "cat_bp":             table = "health_blood_pressure"
        case "cat_ecg":            table = "health_ecg"
        case "cat_audiogram":      table = "health_audiograms"
        case "cat_medications":    table = "health_medications"
        case "cat_vision":         table = "health_vision_prescriptions"
        case "cat_state_of_mind":  table = "health_state_of_mind"
        default: return []
        }
        let rows = (try? await mysql.query("SELECT uuid FROM \(table)")) ?? []
        return rows.compactMap { $0["uuid"] }
    }

    // MARK: - Quick scan helpers

    private func countDB(categoryID: String, mysql: MySQLService) async -> Int {
        if categoryID.hasPrefix("qty_") {
            let rawCat = String(categoryID.dropFirst(4))
            guard let cat = HealthCategory.allCases.first(where: { $0.rawValue == rawCat }),
                  let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == cat })?.1
            else { return 0 }
            var total = 0
            for typeDesc in types {
                let escaped = MySQLEscape.escapeString(typeDesc.id)
                let rows = (try? await mysql.query(
                    "SELECT COUNT(*) as cnt FROM health_quantity_samples WHERE type = '\(escaped)'"
                )) ?? []
                total += rows.first?["cnt"].flatMap(Int.init) ?? 0
            }
            return total
        }
        let specials: [String: String] = [
            "cat_category":           "health_category_samples",
            "cat_workouts":           "health_workouts",
            "cat_bp":                 "health_blood_pressure",
            "cat_ecg":                "health_ecg",
            "cat_audiogram":          "health_audiograms",
            "cat_activity_summaries": "health_activity_summaries",
            "cat_workout_routes":     "health_workout_routes",
            "cat_medications":        "health_medications",
            "cat_vision":             "health_vision_prescriptions",
            "cat_state_of_mind":      "health_state_of_mind",
        ]
        guard let table = specials[categoryID] else { return 0 }
        let rows = (try? await mysql.query("SELECT COUNT(*) as cnt FROM \(table)")) ?? []
        return rows.first?["cnt"].flatMap(Int.init) ?? 0
    }

    private func countHealthKit(for categoryID: String) async -> Int {
        if categoryID.hasPrefix("qty_") {
            let rawCat = String(categoryID.dropFirst(4))
            guard let cat = HealthCategory.allCases.first(where: { $0.rawValue == rawCat }),
                  let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == cat })?.1
            else { return 0 }
            var total = 0
            for typeDesc in types {
                guard let hkType = HKObjectType.quantityType(forIdentifier: typeDesc.hkIdentifier) else { continue }
                total += await HealthKitService.shared.sampleCount(for: hkType)
            }
            return total
        }
        switch categoryID {
        case "cat_category":
            var total = 0
            for typeDesc in HealthDataTypes.allCategoryTypes {
                guard let hkType = HKObjectType.categoryType(forIdentifier: typeDesc.hkIdentifier) else { continue }
                total += await HealthKitService.shared.sampleCount(for: hkType)
            }
            return total
        case "cat_workouts":
            return await HealthKitService.shared.sampleCount(for: .workoutType())
        case "cat_bp":
            guard let t = HKObjectType.correlationType(forIdentifier: .bloodPressure) else { return 0 }
            return await HealthKitService.shared.sampleCount(for: t)
        case "cat_ecg":
            return await HealthKitService.shared.sampleCount(for: .electrocardiogramType())
        case "cat_audiogram":
            return await HealthKitService.shared.sampleCount(for: .audiogramSampleType())
        case "cat_activity_summaries":
            return await HealthKitService.shared.countActivitySummaries()
        case "cat_workout_routes":
            return await HealthKitService.shared.sampleCount(for: HKSeriesType.workoutRoute())
        case "cat_medications":
            if #available(iOS 26, *) {
                return await HealthKitService.shared.countMedicationDoseEvents()
            }
            return 0
        case "cat_vision":
            return await HealthKitService.shared.sampleCount(for: .visionPrescriptionType())
        case "cat_state_of_mind":
            if #available(iOS 18, *) {
                return await HealthKitService.shared.sampleCount(for: .stateOfMindType())
            }
            return 0
        default:
            return 0
        }
    }

    // MARK: - Utilities

    private func sqlDateStr(_ date: Date) -> String {
        Self.sqlDateFormatter.string(from: date)
    }

    private static let sqlDateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()
}
