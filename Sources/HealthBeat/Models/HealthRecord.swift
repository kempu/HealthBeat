import Foundation

// Generic row returned by the data browser MySQL queries
struct HealthRecord: Identifiable {
    let id: String
    let startDate: Date
    let endDate: Date
    let value: Double?
    let valueLabel: String?
    let unit: String?
    let sourceName: String?
    let typeLabel: String?

    // Build from a raw MySQL result row
    static func from(row: [String: String], dateFormatter: DateFormatter) -> HealthRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let endDate   = row["end_date"].flatMap   { dateFormatter.date(from: $0) } ?? startDate
        let value     = row["value"].flatMap { Double($0) }
        return HealthRecord(
            id: uuid,
            startDate: startDate,
            endDate: endDate,
            value: value,
            valueLabel: row["value_label"],
            unit: row["unit"],
            sourceName: row["source_name"],
            typeLabel: row["type"]
        )
    }
}

struct MedicationRecord: Identifiable {
    let id: String
    let medicationName: String?
    let dosage: String?
    let startDate: Date
    let endDate: Date?
    let sourceName: String?

    static func from(row: [String: String], dateFormatter: DateFormatter) -> MedicationRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let endDate   = row["end_date"].flatMap { dateFormatter.date(from: $0) }
        return MedicationRecord(
            id: uuid,
            medicationName: row["medication_name"],
            dosage: row["dosage"],
            startDate: startDate,
            endDate: endDate,
            sourceName: row["source_name"]
        )
    }
}

struct LocationTrackRecord: Identifiable {
    let id: Int
    let latitude: Double
    let longitude: Double
    let altitude: Double?
    let horizontalAccuracy: Double?
    let speed: Double?
    let course: Double?
    let timestamp: Date

    static func from(row: [String: String], dateFormatter: DateFormatter) -> LocationTrackRecord? {
        guard let idStr = row["id"], let id = Int(idStr),
              let lat = row["latitude"].flatMap(Double.init),
              let lon = row["longitude"].flatMap(Double.init) else { return nil }
        let timestamp = row["timestamp"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        return LocationTrackRecord(
            id: id,
            latitude: lat,
            longitude: lon,
            altitude: row["altitude"].flatMap(Double.init),
            horizontalAccuracy: row["horizontal_accuracy"].flatMap(Double.init),
            speed: row["speed"].flatMap(Double.init),
            course: row["course"].flatMap(Double.init),
            timestamp: timestamp
        )
    }
}

struct CheckInRecord: Identifiable {
    let id: Int
    let placeName: String
    let placeType: String?
    let eventType: String  // "arrive" | "depart"
    let latitude: Double?
    let longitude: Double?
    let timestamp: Date

    static func from(row: [String: String], dateFormatter: DateFormatter) -> CheckInRecord? {
        guard let idStr = row["id"], let id = Int(idStr),
              let placeName = row["place_name"] else { return nil }
        let timestamp = row["timestamp"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        return CheckInRecord(
            id: id,
            placeName: placeName,
            placeType: row["place_type"],
            eventType: row["event_type"] ?? "arrive",
            latitude: row["latitude"].flatMap(Double.init),
            longitude: row["longitude"].flatMap(Double.init),
            timestamp: timestamp
        )
    }
}

struct WorkoutRecord: Identifiable {
    let id: String
    let activityType: String
    let startDate: Date
    let endDate: Date
    let durationSeconds: Double
    let energyKcal: Double?
    let distanceMeters: Double?
    let sourceName: String?

    var durationFormatted: String {
        let mins = Int(durationSeconds / 60)
        let secs = Int(durationSeconds) % 60
        if mins >= 60 {
            return "\(mins / 60)h \(mins % 60)m"
        }
        return "\(mins)m \(secs)s"
    }

    static func from(row: [String: String], dateFormatter: DateFormatter) -> WorkoutRecord? {
        guard let uuid = row["uuid"], let actType = row["activity_type"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let endDate   = row["end_date"].flatMap   { dateFormatter.date(from: $0) } ?? startDate
        return WorkoutRecord(
            id: uuid,
            activityType: actType,
            startDate: startDate,
            endDate: endDate,
            durationSeconds: row["duration_seconds"].flatMap(Double.init) ?? 0,
            energyKcal: row["total_energy_burned_kcal"].flatMap(Double.init),
            distanceMeters: row["total_distance_meters"].flatMap(Double.init),
            sourceName: row["source_name"]
        )
    }
}
