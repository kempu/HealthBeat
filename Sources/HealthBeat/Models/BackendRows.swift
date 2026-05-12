import Foundation

/// Typed row structs used by `BackendWriter` to fan a single batch of
/// sample/workout/location/etc. data out to all enabled destinations
/// (MySQL via the existing inline SQL today; EA via `EABackendWriter`).
///
/// Column names match what the `/api/v1/healthbeat/*` ingest endpoints
/// expect — they in turn mirror the EA `hb_*` table schema, which mirrors
/// HealthBeat's own MySQL schema. So the wire JSON is identical to a
/// representation of the existing SQL row.
///
/// All datetimes are emitted as `"yyyy-MM-dd HH:mm:ss.SSS"` UTC to match
/// SyncService's `sqlDate(_:)` formatter; EA accepts that format alongside
/// ISO-8601.

// MARK: - Health quantity/category samples

struct HBQuantityRow: Codable, Sendable {
    let uuid: String
    let type: String
    let value: Double
    let unit: String?
    let start_date: String
    let end_date: String
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
    let metadata: String?       // raw JSON string or nil
}

struct HBCategoryRow: Codable, Sendable {
    let uuid: String
    let type: String
    let value: Int
    let value_label: String?
    let start_date: String
    let end_date: String
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
    let metadata: String?
}

// MARK: - Workouts

struct HBWorkoutRow: Codable, Sendable {
    let uuid: String
    let activity_type: String
    let duration_seconds: Double
    let total_energy_burned_kcal: Double?
    let total_distance_meters: Double?
    let total_swimming_strokes: Double?
    let total_flights_climbed: Double?
    let start_date: String
    let end_date: String
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
    let metadata: String?
}

struct HBWorkoutRouteRow: Codable, Sendable {
    let uuid: String
    let workout_uuid: String
    let start_date: String
    let location_count: Int
    let locations_json: String?
}

// MARK: - Other health entities

struct HBBloodPressureRow: Codable, Sendable {
    let uuid: String
    let systolic: Double
    let diastolic: Double
    let start_date: String
    let source_name: String?
    let device_name: String?
    let metadata: String?
}

struct HBEcgRow: Codable, Sendable {
    let uuid: String
    let classification: String?
    let average_heart_rate: Double?
    let sampling_frequency: Double?
    let voltage_measurements: String?  // raw JSON
    let start_date: String
    let source_name: String?
    let metadata: String?
}

struct HBAudiogramRow: Codable, Sendable {
    let uuid: String
    let sensitivity_points: String?
    let start_date: String
    let source_name: String?
    let metadata: String?
}

struct HBActivitySummaryRow: Codable, Sendable {
    let date: String                      // "yyyy-MM-dd"
    let active_energy_burned: Double?
    let active_energy_burned_goal: Double?
    let exercise_time_minutes: Double?
    let exercise_time_goal_minutes: Double?
    let stand_hours: Int?
    let stand_hours_goal: Int?
}

struct HBMedicationRow: Codable, Sendable {
    let uuid: String
    let medication_name: String?
    let dosage: String?
    let log_status: String?
    let start_date: String
    let end_date: String?
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
    let metadata: String?
}

struct HBVisionRow: Codable, Sendable {
    let uuid: String
    let start_date: String
    let end_date: String
    let prescription_type: Int
    let right_eye_sphere: Double?
    let right_eye_cylinder: Double?
    let right_eye_axis: Double?
    let right_eye_add_power: Double?
    let right_eye_base_curve: Double?
    let right_eye_diameter: Double?
    let left_eye_sphere: Double?
    let left_eye_cylinder: Double?
    let left_eye_axis: Double?
    let left_eye_add_power: Double?
    let left_eye_base_curve: Double?
    let left_eye_diameter: Double?
    let expiration_date: String?
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
}

struct HBStateOfMindRow: Codable, Sendable {
    let uuid: String
    let start_date: String
    let end_date: String
    let kind: Int
    let valence: Double
    let valence_classification: Int?
    let labels_json: String?
    let associations_json: String?
    let source_name: String?
    let source_bundle_id: String?
    let device_name: String?
}

// MARK: - Location

struct HBLocationRow: Codable, Sendable {
    let latitude: Double
    let longitude: Double
    let altitude: Double?
    let horizontal_accuracy: Double?
    let vertical_accuracy: Double?
    let speed: Double?
    let course: Double?
    let timestamp: String
}

struct HBGeofenceEventRow: Codable, Sendable {
    let place_name: String
    let place_type: String?
    let event_type: String              // "arrive" | "depart"
    let latitude: Double?
    let longitude: Double?
    let timestamp: String
}

// MARK: - Bidirectional definitions

struct HBGeofenceDefinitionRow: Codable, Sendable {
    let id: String
    let name: String
    let latitude: Double
    let longitude: Double
    let radius: Double
    let place_category_id: String?
    let origin: String?                  // "app" | "database"
    let is_deleted: Int                  // 0 | 1
    let created_at: String?
    let updated_at: String?
}

struct HBPlaceCategoryRow: Codable, Sendable {
    let id: String
    let name: String
    let system_image: String
    let sort_order: Int?
    let origin: String?
    let is_deleted: Int
    let created_at: String?
    let updated_at: String?
}

// MARK: - Sync log

struct HBSyncLogRow: Codable, Sendable {
    let category: String
    let records_synced: Int
    let started_at: String
    let completed_at: String?
    let status: String                   // "running" | "completed" | "failed"
    let error_message: String?
}
