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

struct BloodPressureRecord: Identifiable {
    let id: String
    let systolic: Double
    let diastolic: Double
    let startDate: Date
    let sourceName: String?
    let deviceName: String?

    static func from(row: [String: String], dateFormatter: DateFormatter) -> BloodPressureRecord? {
        guard let uuid = row["uuid"],
              let systolic = row["systolic"].flatMap(Double.init),
              let diastolic = row["diastolic"].flatMap(Double.init) else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        return BloodPressureRecord(
            id: uuid,
            systolic: systolic,
            diastolic: diastolic,
            startDate: startDate,
            sourceName: row["source_name"],
            deviceName: row["device_name"]
        )
    }
}

struct ECGRecord: Identifiable {
    let id: String
    let classification: String?
    let averageHeartRate: Double?
    let samplingFrequency: Double?
    let voltageMeasurementCount: Int
    let startDate: Date
    let sourceName: String?

    static func from(row: [String: String], dateFormatter: DateFormatter) -> ECGRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        // voltage_measurements is a JSON array; count entries cheaply by counting commas+1 if non-empty
        let voltageJSON = row["voltage_measurements"] ?? ""
        let voltageCount: Int
        if voltageJSON.isEmpty || voltageJSON == "null" || voltageJSON == "[]" {
            voltageCount = 0
        } else if let data = voltageJSON.data(using: .utf8),
                  let arr = try? JSONSerialization.jsonObject(with: data) as? [Any] {
            voltageCount = arr.count
        } else {
            voltageCount = 0
        }
        return ECGRecord(
            id: uuid,
            classification: row["classification"],
            averageHeartRate: row["average_heart_rate"].flatMap(Double.init),
            samplingFrequency: row["sampling_frequency"].flatMap(Double.init),
            voltageMeasurementCount: voltageCount,
            startDate: startDate,
            sourceName: row["source_name"]
        )
    }
}

struct AudiogramRecord: Identifiable {
    let id: String
    let sensitivityPointsJSON: String
    let pointCount: Int
    let startDate: Date
    let sourceName: String?

    static func from(row: [String: String], dateFormatter: DateFormatter) -> AudiogramRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let json = row["sensitivity_points"] ?? ""
        var count = 0
        if !json.isEmpty && json != "null" && json != "[]",
           let data = json.data(using: .utf8),
           let arr = try? JSONSerialization.jsonObject(with: data) as? [Any] {
            count = arr.count
        }
        return AudiogramRecord(
            id: uuid,
            sensitivityPointsJSON: json,
            pointCount: count,
            startDate: startDate,
            sourceName: row["source_name"]
        )
    }
}

struct ActivitySummaryRecord: Identifiable {
    let id: Date
    var date: Date { id }
    let activeEnergyBurned: Double?
    let activeEnergyBurnedGoal: Double?
    let exerciseTimeMinutes: Double?
    let exerciseTimeGoalMinutes: Double?
    let standHours: Int?
    let standHoursGoal: Int?

    static func from(row: [String: String]) -> ActivitySummaryRecord? {
        guard let dateStr = row["date"] else { return nil }
        let dayFmt = DateFormatter()
        dayFmt.dateFormat = "yyyy-MM-dd"
        dayFmt.locale = Locale(identifier: "en_US_POSIX")
        dayFmt.timeZone = TimeZone(identifier: "UTC")
        guard let date = dayFmt.date(from: dateStr) else { return nil }
        return ActivitySummaryRecord(
            id: date,
            activeEnergyBurned: row["active_energy_burned"].flatMap(Double.init),
            activeEnergyBurnedGoal: row["active_energy_burned_goal"].flatMap(Double.init),
            exerciseTimeMinutes: row["exercise_time_minutes"].flatMap(Double.init),
            exerciseTimeGoalMinutes: row["exercise_time_goal_minutes"].flatMap(Double.init),
            standHours: row["stand_hours"].flatMap(Int.init),
            standHoursGoal: row["stand_hours_goal"].flatMap(Int.init)
        )
    }
}

struct WorkoutRouteRecord: Identifiable {
    let id: String
    let workoutUUID: String
    let startDate: Date
    let locationCount: Int

    static func from(row: [String: String], dateFormatter: DateFormatter) -> WorkoutRouteRecord? {
        guard let uuid = row["uuid"], let workoutUUID = row["workout_uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        return WorkoutRouteRecord(
            id: uuid,
            workoutUUID: workoutUUID,
            startDate: startDate,
            locationCount: row["location_count"].flatMap(Int.init) ?? 0
        )
    }
}

struct VisionPrescriptionRecord: Identifiable {
    let id: String
    let startDate: Date
    let endDate: Date
    let prescriptionType: Int
    let rightEyeSphere: Double?
    let rightEyeCylinder: Double?
    let rightEyeAxis: Double?
    let leftEyeSphere: Double?
    let leftEyeCylinder: Double?
    let leftEyeAxis: Double?
    let expirationDate: Date?
    let sourceName: String?

    var prescriptionTypeLabel: String {
        // HKVisionPrescriptionType: 1 = glasses, 2 = contacts
        switch prescriptionType {
        case 1: return "Glasses"
        case 2: return "Contacts"
        default: return "Type \(prescriptionType)"
        }
    }

    static func from(row: [String: String], dateFormatter: DateFormatter) -> VisionPrescriptionRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let endDate   = row["end_date"].flatMap   { dateFormatter.date(from: $0) } ?? startDate
        let expiration = row["expiration_date"].flatMap { dateFormatter.date(from: $0) }
        return VisionPrescriptionRecord(
            id: uuid,
            startDate: startDate,
            endDate: endDate,
            prescriptionType: row["prescription_type"].flatMap(Int.init) ?? 0,
            rightEyeSphere: row["right_eye_sphere"].flatMap(Double.init),
            rightEyeCylinder: row["right_eye_cylinder"].flatMap(Double.init),
            rightEyeAxis: row["right_eye_axis"].flatMap(Double.init),
            leftEyeSphere: row["left_eye_sphere"].flatMap(Double.init),
            leftEyeCylinder: row["left_eye_cylinder"].flatMap(Double.init),
            leftEyeAxis: row["left_eye_axis"].flatMap(Double.init),
            expirationDate: expiration,
            sourceName: row["source_name"]
        )
    }
}

struct StateOfMindRecord: Identifiable {
    let id: String
    let startDate: Date
    let endDate: Date
    let kind: Int
    let valence: Double
    let valenceClassification: Int?
    let labels: [Int]
    let sourceName: String?

    var kindLabel: String {
        // HKStateOfMind.Kind: 1 = momentary, 2 = daily
        switch kind {
        case 1: return "Momentary"
        case 2: return "Daily"
        default: return "Kind \(kind)"
        }
    }

    var valenceLabel: String {
        switch valenceClassification {
        case 1: return "Very Unpleasant"
        case 2: return "Unpleasant"
        case 3: return "Slightly Unpleasant"
        case 4: return "Neutral"
        case 5: return "Slightly Pleasant"
        case 6: return "Pleasant"
        case 7: return "Very Pleasant"
        default: return String(format: "%.2f", valence)
        }
    }

    static func from(row: [String: String], dateFormatter: DateFormatter) -> StateOfMindRecord? {
        guard let uuid = row["uuid"] else { return nil }
        let startDate = row["start_date"].flatMap { dateFormatter.date(from: $0) } ?? Date()
        let endDate   = row["end_date"].flatMap   { dateFormatter.date(from: $0) } ?? startDate
        var labels: [Int] = []
        if let labelsJSON = row["labels_json"], !labelsJSON.isEmpty,
           let data = labelsJSON.data(using: .utf8),
           let arr = try? JSONSerialization.jsonObject(with: data) as? [Int] {
            labels = arr
        }
        return StateOfMindRecord(
            id: uuid,
            startDate: startDate,
            endDate: endDate,
            kind: row["kind"].flatMap(Int.init) ?? 0,
            valence: row["valence"].flatMap(Double.init) ?? 0,
            valenceClassification: row["valence_classification"].flatMap(Int.init),
            labels: labels,
            sourceName: row["source_name"]
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
