import Foundation
import SwiftUI
import HealthKit

// MARK: - Chart time range

enum ChartTimeRange: String, CaseIterable, Identifiable {
    case week = "7D"
    case month = "30D"
    case threeMonths = "90D"
    case year = "1Y"
    case all = "All"

    var id: String { rawValue }

    var sqlInterval: String? {
        switch self {
        case .week:        return "7 DAY"
        case .month:       return "30 DAY"
        case .threeMonths: return "90 DAY"
        case .year:        return "365 DAY"
        case .all:         return nil
        }
    }

    var strideComponent: Calendar.Component {
        switch self {
        case .week:        return .day
        case .month:       return .day
        case .threeMonths: return .weekOfYear
        case .year:        return .month
        case .all:         return .month
        }
    }

    var strideCount: Int {
        switch self {
        case .week:        return 1
        case .month:       return 7
        case .threeMonths: return 2
        case .year:        return 2
        case .all:         return 3
        }
    }
}

// MARK: - View Model

@MainActor
final class DataBrowserViewModel: ObservableObject {

    @Published var selectedCategory: HealthCategory? = nil
    @Published var selectedTypeID: String? = nil
    @Published var records: [HealthRecord] = []
    @Published var workoutRecords: [WorkoutRecord] = []
    @Published var medicationRecords: [MedicationRecord] = []
    @Published var locationRecords: [LocationTrackRecord] = []
    @Published var checkInRecords: [CheckInRecord] = []
    @Published var isLoading = false
    @Published var errorMessage: String?
    @Published var page = 0

    // Filters
    @Published var filterDateFrom: Date? = nil
    @Published var filterDateTo: Date? = nil
    @Published var filterSource: String? = nil
    @Published var availableSources: [String] = []

    private let pageSize = 50

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        f.timeZone = TimeZone(identifier: "UTC")
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private static let sqlDateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss"
        f.timeZone = TimeZone(identifier: "UTC")
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    // The currently selected quantity type descriptor (if applicable)
    var selectedQuantityDescriptor: QuantityTypeDescriptor? {
        guard let id = selectedTypeID else { return nil }
        return HealthDataTypes.quantityDescriptor(for: id)
    }

    // The currently selected category type descriptor (if applicable)
    var selectedCategoryDescriptor: CategoryTypeDescriptor? {
        guard let id = selectedTypeID else { return nil }
        return HealthDataTypes.categoryDescriptor(for: id)
    }

    var totalLoaded: Int {
        if selectedTypeID == "workout" { return workoutRecords.count }
        if selectedTypeID == "medications" { return medicationRecords.count }
        if selectedTypeID == "location_tracks" { return locationRecords.count }
        if selectedTypeID == "location_geofence_events" { return checkInRecords.count }
        return records.count
    }

    // MARK: - Reset filters

    func resetFilters() {
        filterDateFrom = nil
        filterDateTo = nil
        filterSource = nil
    }

    // MARK: - SQL filter clause builder

    private func timestampFilterSQL(prefix: String = "AND") -> String {
        var clauses: [String] = []
        if let from = filterDateFrom {
            clauses.append("timestamp >= '\(Self.sqlDateFormatter.string(from: from))'")
        }
        if let to = filterDateTo {
            let endOfDay = Calendar.current.date(bySettingHour: 23, minute: 59, second: 59, of: to) ?? to
            clauses.append("timestamp <= '\(Self.sqlDateFormatter.string(from: endOfDay))'")
        }
        if clauses.isEmpty { return "" }
        return " \(prefix) " + clauses.joined(separator: " AND ")
    }

    private func dateFilterSQL(prefix: String = "AND") -> String {
        var clauses: [String] = []
        if let from = filterDateFrom {
            clauses.append("start_date >= '\(Self.sqlDateFormatter.string(from: from))'")
        }
        if let to = filterDateTo {
            // Include the full "to" day
            let endOfDay = Calendar.current.date(bySettingHour: 23, minute: 59, second: 59, of: to) ?? to
            clauses.append("start_date <= '\(Self.sqlDateFormatter.string(from: endOfDay))'")
        }
        if let src = filterSource, !src.isEmpty {
            clauses.append("source_name = '\(MySQLEscape.escapeString(src))'")
        }
        if clauses.isEmpty { return "" }
        return " \(prefix) " + clauses.joined(separator: " AND ")
    }

    // MARK: - Load available sources

    func loadSources(config: MySQLConfig) {
        guard let typeID = selectedTypeID else { return }
        if typeID == "location_tracks" || typeID == "location_geofence_events" {
            availableSources = []
            return
        }

        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)

                let table: String
                let whereClause: String
                if typeID == "workout" {
                    table = "health_workouts"
                    whereClause = ""
                } else if typeID == "medications" {
                    table = "health_medications"
                    whereClause = ""
                } else if HealthDataTypes.allCategoryTypes.contains(where: { $0.id == typeID }) {
                    table = "health_category_samples"
                    whereClause = "WHERE type = '\(MySQLEscape.escapeString(typeID))'"
                } else {
                    table = "health_quantity_samples"
                    whereClause = "WHERE type = '\(MySQLEscape.escapeString(typeID))'"
                }

                let rows = try await mysql.query("""
                    SELECT DISTINCT source_name FROM \(table) \(whereClause)
                    ORDER BY source_name ASC
                """)
                await mysql.disconnect()

                availableSources = rows.compactMap { $0["source_name"] }.filter { !$0.isEmpty }
            } catch {
                await mysql.disconnect()
                self.errorMessage = error.localizedDescription
            }
        }
    }

    // MARK: - Load data

    func loadData(config: MySQLConfig) {
        guard let typeID = selectedTypeID else { return }
        isLoading = true
        errorMessage = nil
        page = 0
        records = []
        workoutRecords = []
        medicationRecords = []
        locationRecords = []
        checkInRecords = []

        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)

                if typeID == "workout" {
                    let filters = dateFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT uuid, activity_type, duration_seconds,
                               total_energy_burned_kcal, total_distance_meters,
                               start_date, end_date, source_name
                        FROM health_workouts
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    workoutRecords = rows.compactMap { WorkoutRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "medications" {
                    let filters = dateFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT uuid, medication_name, dosage, start_date, end_date, source_name
                        FROM health_medications
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    medicationRecords = rows.compactMap { MedicationRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if HealthDataTypes.allCategoryTypes.contains(where: { $0.id == typeID }) {
                    let filters = dateFilterSQL()
                    let rows = try await mysql.query("""
                        SELECT uuid, type, value, value_label, start_date, end_date, source_name
                        FROM health_category_samples
                        WHERE type = '\(MySQLEscape.escapeString(typeID))'
                        \(filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    records = rows.compactMap { HealthRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "location_tracks" {
                    let filters = timestampFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT id, latitude, longitude, altitude,
                               horizontal_accuracy, speed, course, timestamp
                        FROM location_tracks
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY timestamp DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    locationRecords = rows.compactMap { LocationTrackRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "location_geofence_events" {
                    let filters = timestampFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT id, place_name, place_type, event_type, latitude, longitude, timestamp
                        FROM location_geofence_events
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY timestamp DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    checkInRecords = rows.compactMap { CheckInRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else {
                    // Quantity type
                    let filters = dateFilterSQL()
                    let rows = try await mysql.query("""
                        SELECT uuid, type, value, unit, start_date, end_date, source_name
                        FROM health_quantity_samples
                        WHERE type = '\(MySQLEscape.escapeString(typeID))'
                        \(filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    records = rows.compactMap { HealthRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                }

                await mysql.disconnect()
            } catch {
                errorMessage = error.localizedDescription
            }
            isLoading = false
        }
    }

    func loadNextPage(config: MySQLConfig) {
        guard !isLoading, let typeID = selectedTypeID else { return }
        page += 1
        isLoading = true

        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: config)

                if typeID == "workout" {
                    let filters = dateFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT uuid, activity_type, duration_seconds,
                               total_energy_burned_kcal, total_distance_meters,
                               start_date, end_date, source_name
                        FROM health_workouts
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    workoutRecords += rows.compactMap { WorkoutRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "medications" {
                    let filters = dateFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT uuid, medication_name, dosage, start_date, end_date, source_name
                        FROM health_medications
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    medicationRecords += rows.compactMap { MedicationRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if HealthDataTypes.allCategoryTypes.contains(where: { $0.id == typeID }) {
                    let filters = dateFilterSQL()
                    let rows = try await mysql.query("""
                        SELECT uuid, type, value, value_label, start_date, end_date, source_name
                        FROM health_category_samples
                        WHERE type = '\(MySQLEscape.escapeString(typeID))'
                        \(filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    records += rows.compactMap { HealthRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "location_tracks" {
                    let filters = timestampFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT id, latitude, longitude, altitude,
                               horizontal_accuracy, speed, course, timestamp
                        FROM location_tracks
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY timestamp DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    locationRecords += rows.compactMap { LocationTrackRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else if typeID == "location_geofence_events" {
                    let filters = timestampFilterSQL(prefix: "WHERE")
                    let rows = try await mysql.query("""
                        SELECT id, place_name, place_type, event_type, latitude, longitude, timestamp
                        FROM location_geofence_events
                        \(filters.isEmpty ? "" : filters)
                        ORDER BY timestamp DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    checkInRecords += rows.compactMap { CheckInRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                } else {
                    let filters = dateFilterSQL()
                    let rows = try await mysql.query("""
                        SELECT uuid, type, value, unit, start_date, end_date, source_name
                        FROM health_quantity_samples
                        WHERE type = '\(MySQLEscape.escapeString(typeID))'
                        \(filters)
                        ORDER BY start_date DESC
                        LIMIT \(pageSize) OFFSET \(page * pageSize)
                    """)
                    records += rows.compactMap { HealthRecord.from(row: $0, dateFormatter: Self.dateFormatter) }
                }

                await mysql.disconnect()
            } catch {
                errorMessage = error.localizedDescription
                page -= 1
            }
            isLoading = false
        }
    }

    // MARK: - Chart data with configurable time range

    func loadChartData(config: MySQLConfig, typeID: String, range: ChartTimeRange = .month) async -> [(Date, Double)] {
        let mysql = MySQLService()
        do {
            try await mysql.connect(config: config)

            var whereClause = "WHERE type = '\(MySQLEscape.escapeString(typeID))'"
            if let interval = range.sqlInterval {
                whereClause += " AND start_date >= DATE_SUB(NOW(), INTERVAL \(interval))"
            }

            let rows = try await mysql.query("""
                SELECT DATE(start_date) as day, AVG(value) as avg_value
                FROM health_quantity_samples
                \(whereClause)
                GROUP BY DATE(start_date)
                ORDER BY day ASC
            """)
            await mysql.disconnect()

            let dayFmt = DateFormatter()
            dayFmt.dateFormat = "yyyy-MM-dd"
            dayFmt.locale = Locale(identifier: "en_US_POSIX")

            return rows.compactMap { row -> (Date, Double)? in
                guard let dayStr = row["day"],
                      let date = dayFmt.date(from: dayStr),
                      let val = row["avg_value"].flatMap(Double.init) else { return nil }
                return (date, val)
            }
        } catch {
            await MainActor.run { self.errorMessage = error.localizedDescription }
            return []
        }
    }
}
