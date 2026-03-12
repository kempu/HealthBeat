import Foundation

struct SchemaService {

    static let ddl = """
    CREATE TABLE IF NOT EXISTS health_quantity_samples (
      id               BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid             VARCHAR(36) UNIQUE NOT NULL,
      type             VARCHAR(100) NOT NULL,
      value            DOUBLE NOT NULL,
      unit             VARCHAR(50),
      start_date       DATETIME(3) NOT NULL,
      end_date         DATETIME(3) NOT NULL,
      source_name      VARCHAR(255),
      source_bundle_id VARCHAR(255),
      device_name      VARCHAR(255),
      metadata         JSON,
      synced_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_type_date  (type, start_date),
      INDEX idx_start_date (start_date)
    );

    CREATE TABLE IF NOT EXISTS health_category_samples (
      id               BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid             VARCHAR(36) UNIQUE NOT NULL,
      type             VARCHAR(100) NOT NULL,
      value            INT NOT NULL,
      value_label      VARCHAR(100),
      start_date       DATETIME(3) NOT NULL,
      end_date         DATETIME(3) NOT NULL,
      source_name      VARCHAR(255),
      source_bundle_id VARCHAR(255),
      device_name      VARCHAR(255),
      metadata         JSON,
      synced_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_type_date (type, start_date)
    );

    CREATE TABLE IF NOT EXISTS health_workouts (
      id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid                     VARCHAR(36) UNIQUE NOT NULL,
      activity_type            VARCHAR(100) NOT NULL,
      duration_seconds         DOUBLE NOT NULL,
      total_energy_burned_kcal DOUBLE,
      total_distance_meters    DOUBLE,
      total_swimming_strokes   DOUBLE,
      total_flights_climbed    DOUBLE,
      start_date               DATETIME(3) NOT NULL,
      end_date                 DATETIME(3) NOT NULL,
      source_name              VARCHAR(255),
      source_bundle_id         VARCHAR(255),
      device_name              VARCHAR(255),
      metadata                 JSON,
      synced_at                DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_activity_date  (activity_type, start_date)
    );

    CREATE TABLE IF NOT EXISTS health_blood_pressure (
      id          BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid        VARCHAR(36) UNIQUE NOT NULL,
      systolic    DOUBLE NOT NULL,
      diastolic   DOUBLE NOT NULL,
      start_date  DATETIME(3) NOT NULL,
      source_name VARCHAR(255),
      device_name VARCHAR(255),
      metadata    JSON,
      synced_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_start_date (start_date)
    );

    CREATE TABLE IF NOT EXISTS health_ecg (
      id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid                VARCHAR(36) UNIQUE NOT NULL,
      classification      VARCHAR(100),
      average_heart_rate  DOUBLE,
      sampling_frequency  DOUBLE,
      voltage_measurements JSON,
      start_date          DATETIME(3) NOT NULL,
      source_name         VARCHAR(255),
      metadata            JSON,
      synced_at           DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS health_audiograms (
      id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid               VARCHAR(36) UNIQUE NOT NULL,
      sensitivity_points JSON,
      start_date         DATETIME(3) NOT NULL,
      source_name        VARCHAR(255),
      metadata           JSON,
      synced_at          DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS health_activity_summaries (
      date                       DATE NOT NULL,
      active_energy_burned       DOUBLE,
      active_energy_burned_goal  DOUBLE,
      exercise_time_minutes      DOUBLE,
      exercise_time_goal_minutes DOUBLE,
      stand_hours                INT,
      stand_hours_goal           INT,
      synced_at                  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (date)
    );

    CREATE TABLE IF NOT EXISTS health_workout_routes (
      uuid             VARCHAR(36) NOT NULL,
      workout_uuid     VARCHAR(36) NOT NULL,
      start_date       DATETIME(3) NOT NULL,
      location_count   INT NOT NULL DEFAULT 0,
      locations_json   LONGTEXT,
      synced_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (uuid),
      INDEX idx_workout_uuid (workout_uuid)
    );

    CREATE TABLE IF NOT EXISTS health_sync_log (
      id             BIGINT AUTO_INCREMENT PRIMARY KEY,
      category       VARCHAR(100) NOT NULL,
      records_synced INT DEFAULT 0,
      started_at     DATETIME NOT NULL,
      completed_at   DATETIME,
      status         ENUM('running','completed','failed') DEFAULT 'running',
      error_message  TEXT,
      INDEX idx_category (category)
    );

    CREATE TABLE IF NOT EXISTS health_medications (
      id               BIGINT AUTO_INCREMENT PRIMARY KEY,
      uuid             VARCHAR(36) UNIQUE NOT NULL,
      medication_name  VARCHAR(255),
      dosage           VARCHAR(255),
      log_status       VARCHAR(50),
      start_date       DATETIME(3) NOT NULL,
      end_date         DATETIME(3),
      source_name      VARCHAR(255),
      source_bundle_id VARCHAR(255),
      device_name      VARCHAR(255),
      metadata         JSON,
      synced_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_med_name (medication_name),
      INDEX idx_start_date (start_date)
    );

    CREATE TABLE IF NOT EXISTS location_tracks (
      id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
      latitude            DOUBLE NOT NULL,
      longitude           DOUBLE NOT NULL,
      altitude            DOUBLE,
      horizontal_accuracy DOUBLE,
      vertical_accuracy   DOUBLE,
      speed               DOUBLE,
      course              DOUBLE,
      timestamp           DATETIME(3) NOT NULL,
      synced_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_timestamp (timestamp)
    );

    CREATE TABLE IF NOT EXISTS location_geofence_events (
      id          BIGINT AUTO_INCREMENT PRIMARY KEY,
      place_name  VARCHAR(100) NOT NULL,
      place_type  VARCHAR(100),
      event_type  ENUM('arrive','depart') NOT NULL,
      latitude    DOUBLE,
      longitude   DOUBLE,
      timestamp   DATETIME(3) NOT NULL,
      synced_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_place_time (place_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS health_vision_prescriptions (
      uuid                  VARCHAR(36) PRIMARY KEY,
      start_date            DATETIME(3) NOT NULL,
      end_date              DATETIME(3) NOT NULL,
      prescription_type     TINYINT NOT NULL,
      right_eye_sphere      DOUBLE,
      right_eye_cylinder    DOUBLE,
      right_eye_axis        DOUBLE,
      right_eye_add_power   DOUBLE,
      right_eye_base_curve  DOUBLE,
      right_eye_diameter    DOUBLE,
      left_eye_sphere       DOUBLE,
      left_eye_cylinder     DOUBLE,
      left_eye_axis         DOUBLE,
      left_eye_add_power    DOUBLE,
      left_eye_base_curve   DOUBLE,
      left_eye_diameter     DOUBLE,
      expiration_date       DATETIME(3),
      source_name           VARCHAR(255),
      source_bundle_id      VARCHAR(255),
      device_name           VARCHAR(255),
      synced_at             DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      INDEX idx_start_date (start_date)
    );

    CREATE TABLE IF NOT EXISTS health_state_of_mind (
      uuid                    VARCHAR(36) PRIMARY KEY,
      start_date              DATETIME(3) NOT NULL,
      end_date                DATETIME(3) NOT NULL,
      kind                    TINYINT NOT NULL,
      valence                 DOUBLE NOT NULL,
      valence_classification  TINYINT,
      labels_json             TEXT,
      associations_json       TEXT,
      source_name             VARCHAR(255),
      source_bundle_id        VARCHAR(255),
      device_name             VARCHAR(255),
      synced_at               DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      INDEX idx_start_date (start_date)
    );
    """

    static let tableStatements: [String] = ddl
        .components(separatedBy: ";")
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        .filter { !$0.isEmpty }

    // MARK: - Schema versioning & migrations

    /// Current schema version. Bump this when adding new migrations.
    static let currentSchemaVersion = 5

    /// Migrations keyed by target version. Each entry contains ALTER statements
    /// to upgrade from the previous version.
    static let migrations: [Int: [String]] = [
        2: [
            // v2: Drop time zone columns that were briefly added — all dates are UTC,
            // so per-row time zone columns are redundant and confusing.
            // Note: MySQL does not support DROP COLUMN IF EXISTS; error 1091
            // (column doesn't exist) is caught below and treated as benign.
            "ALTER TABLE health_quantity_samples DROP COLUMN source_time_zone",
            "ALTER TABLE health_category_samples DROP COLUMN source_time_zone",
            "ALTER TABLE health_workouts DROP COLUMN source_time_zone",
            "ALTER TABLE health_blood_pressure DROP COLUMN source_time_zone",
            "ALTER TABLE health_ecg DROP COLUMN source_time_zone",
            "ALTER TABLE health_audiograms DROP COLUMN source_time_zone",
            "ALTER TABLE health_activity_summaries DROP COLUMN source_time_zone",
            "ALTER TABLE health_workout_routes DROP COLUMN source_time_zone",
            "ALTER TABLE health_medications DROP COLUMN source_time_zone",
            "ALTER TABLE health_sync_log DROP COLUMN time_zone",
            "ALTER TABLE location_tracks DROP COLUMN time_zone",
            "ALTER TABLE location_geofence_events DROP COLUMN time_zone",
            // Schema version tracking table
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INT NOT NULL PRIMARY KEY,
              applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """,
        ],
        3: [
            // v3: Add place_type column to geofence events for categorizing places.
            // MySQL does not support ADD COLUMN IF NOT EXISTS; error 1060
            // (duplicate column) is caught below and treated as benign.
            "ALTER TABLE location_geofence_events ADD COLUMN place_type VARCHAR(100) AFTER place_name",
        ],
        4: [
            // v4: Add log_status column to health_medications to store
            // HKMedicationDoseEventType (taken/skipped/snoozed/delayed) from iOS 26+.
            "ALTER TABLE health_medications ADD COLUMN log_status VARCHAR(50) AFTER dosage",
        ],
        5: [
            // v5: Index on health_quantity_samples.type so GROUP BY type queries
            // (used by refreshRecordCounts and data validation) are fast on large tables.
            // Error 1061 (duplicate key name) is benign — index already exists.
            "ALTER TABLE health_quantity_samples ADD INDEX idx_hqs_type (type)",
        ],
    ]

    // MARK: - Public API

    /// Create all tables and run any pending migrations. Returns (success, errorMessage).
    static func initializeSchema(mysql: MySQLService) async -> (Bool, String?) {
        // Create base tables
        for stmt in tableStatements {
            do {
                try await mysql.execute(stmt)
            } catch {
                return (false, error.localizedDescription)
            }
        }
        // Run migrations for existing databases
        let migrationResult = await runMigrations(mysql: mysql)
        return migrationResult
    }

    // MARK: - Migration runner

    private static func runMigrations(mysql: MySQLService) async -> (Bool, String?) {
        // Ensure the schema_migrations table exists
        do {
            try await mysql.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                  version INT NOT NULL PRIMARY KEY,
                  applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        } catch {
            return (false, "Failed to create schema_migrations table: \(error.localizedDescription)")
        }

        // Get current version
        let currentVersion: Int
        do {
            let rows = try await mysql.query(
                "SELECT COALESCE(MAX(version), 1) as v FROM schema_migrations"
            )
            currentVersion = rows.first?["v"].flatMap(Int.init) ?? 1
        } catch {
            return (false, "Failed to read schema version: \(error.localizedDescription)")
        }

        // Apply pending migrations (guard against empty/invalid range when already up to date)
        guard currentVersion < currentSchemaVersion else { return (true, nil) }
        for version in (currentVersion + 1)...currentSchemaVersion {
            guard let stmts = migrations[version] else { continue }
            for stmt in stmts {
                do {
                    try await mysql.execute(stmt)
                } catch {
                    // Ignore benign errors: "column doesn't exist" (1091) when
                    // dropping columns on fresh installs, or "duplicate column" (1060)
                    // when adding columns that already exist.
                    let errMsg = error.localizedDescription
                    if errMsg.contains("1091") || errMsg.contains("1060") || errMsg.contains("1061")
                        || errMsg.lowercased().contains("can't drop")
                        || errMsg.lowercased().contains("check that column")
                        || errMsg.lowercased().contains("check that it exists")
                        || errMsg.lowercased().contains("duplicate column")
                        || errMsg.lowercased().contains("duplicate key name") {
                        continue
                    }
                    return (false, "Migration v\(version) failed: \(errMsg)")
                }
            }
            // Record this migration
            do {
                try await mysql.execute(
                    "INSERT IGNORE INTO schema_migrations (version) VALUES (\(version))"
                )
            } catch {
                return (false, "Failed to record migration v\(version): \(error.localizedDescription)")
            }
        }

        return (true, nil)
    }

    // Check if a table exists
    static func tableExists(_ name: String, mysql: MySQLService) async -> Bool {
        do {
            let rows = try await mysql.query(
                "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '\(MySQLEscape.escapeString(name))'"
            )
            return rows.first?["cnt"].flatMap(Int.init) ?? 0 > 0
        } catch {
            return false
        }
    }

    // MARK: - Destructive operations

    /// Deletes all rows from every health data table. Schema and migrations are preserved.
    static func deleteAllHealthData(mysql: MySQLService) async throws {
        let tables = [
            "health_quantity_samples", "health_category_samples", "health_workouts",
            "health_blood_pressure", "health_ecg", "health_audiograms",
            "health_activity_summaries", "health_workout_routes", "health_medications",
            "health_vision_prescriptions", "health_state_of_mind", "health_sync_log",
        ]
        for table in tables {
            try await mysql.execute("DELETE FROM \(table)")
        }
    }

    /// Deletes all rows belonging to a single sync category.
    static func deleteCategoryData(categoryID: String, mysql: MySQLService) async throws {
        if categoryID.hasPrefix("qty_") {
            let catRaw = String(categoryID.dropFirst(4))
            guard let hCat = HealthCategory.allCases.first(where: { $0.rawValue == catRaw }),
                  let types = HealthDataTypes.quantityTypesByCategory.first(where: { $0.0 == hCat })?.1
            else { throw MySQLError.queryError(code: 0, message: "Unknown category: \(categoryID)") }
            let inList = types.map { "'\(MySQLEscape.escapeString($0.id))'" }.joined(separator: ",")
            try await mysql.execute("DELETE FROM health_quantity_samples WHERE type IN (\(inList))")
            return
        }
        let table: String
        switch categoryID {
        case "cat_category":          table = "health_category_samples"
        case "cat_workouts":          table = "health_workouts"
        case "cat_bp":                table = "health_blood_pressure"
        case "cat_ecg":               table = "health_ecg"
        case "cat_audiogram":         table = "health_audiograms"
        case "cat_activity_summaries":table = "health_activity_summaries"
        case "cat_workout_routes":    table = "health_workout_routes"
        case "cat_medications":       table = "health_medications"
        case "cat_vision":            table = "health_vision_prescriptions"
        case "cat_state_of_mind":     table = "health_state_of_mind"
        default: throw MySQLError.queryError(code: 0, message: "Unknown category: \(categoryID)")
        }
        try await mysql.execute("DELETE FROM \(table)")
    }

    // Get record counts for all tables
    static func recordCounts(mysql: MySQLService) async -> [String: Int] {
        let tables = [
            "health_quantity_samples",
            "health_category_samples",
            "health_workouts",
            "health_blood_pressure",
            "health_ecg",
            "health_audiograms",
            "health_activity_summaries",
            "health_workout_routes",
            "health_medications",
            "health_vision_prescriptions",
            "health_state_of_mind",
            "location_tracks",
            "location_geofence_events",
        ]
        var counts: [String: Int] = [:]
        for table in tables {
            let rows = (try? await mysql.query("SELECT COUNT(*) as cnt FROM \(table)")) ?? []
            counts[table] = rows.first?["cnt"].flatMap(Int.init) ?? 0
        }
        return counts
    }
}
