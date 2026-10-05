import Foundation
import CryptoKit

/// Phase 2a — drains the local encrypted dump into MySQL over the existing TCP
/// client. Runs with the screen asleep (it never touches HealthKit) and resumes
/// across iOS background-task windows via a per-table frame cursor.
///
/// Decodes each `HB*Row` back into large, size-bounded `INSERT IGNORE` batches —
/// far bigger than the live path's 500-row batches, since there's no per-batch
/// network round-trip to EA in front of each one. Idempotent: re-running a
/// partially-applied frame is safe (`INSERT IGNORE` / `ON DUPLICATE KEY UPDATE`).
///
/// Reconciliation (deleting rows dropped from Apple Health) is intentionally NOT
/// done here: collecting every UUID of a multi-million-row dense table would blow
/// memory. The subsequent incremental sync reconciles recent windows. A full
/// re-export is an idempotent upsert, so nothing is lost.
extension SyncService {

    /// SQL packet budget per INSERT. Stays well under MySQL's default
    /// `max_allowed_packet` while making each round-trip carry thousands of rows.
    private static var drainSQLBudget: Int { 4 * 1024 * 1024 }   // 4 MiB
    private static var drainRowCap: Int { 5_000 }

    /// Public entry for resuming a drain on its own (e.g. from a background task).
    /// Owns the running-flag lifecycle. `runFullSync` calls `drainDump` directly
    /// because it already holds the flags for the whole full-sync pass.
    @discardableResult
    func runDumpDrain(runID: String, config: MySQLConfig) async -> Bool {
        guard !syncState.isAnySyncRunning else { return false }
        syncState.isFullSyncRunning = true
        SyncService.setGlobalSyncRunning(true)
        defer {
            SyncService.setGlobalSyncRunning(false)
            syncState.isFullSyncRunning = false
            syncState.endFullSyncProgress()
        }
        // This is a resumed deliver (export already happened) — set up the
        // phase-weighted progress so the live activity reflects the drain. Use the
        // run's recorded targets (not the current config) so the stepper matches
        // what this run is actually delivering.
        syncState.resumeDeliverProgress(eaEnabled: DumpStore.isEARequired(runID), mysqlEnabled: true,
                                        eaDone: DumpStore.isEADelivered(runID))
        let complete = await drainDump(runID: runID, config: config)
        if complete {
            UserDefaults.standard.removeObject(forKey: "pendingDumpDrainRunID")
            syncState.updateFullSyncProgress(mysql: 1.0)
            syncState.markMySQLBaselineDone()    // MySQL now has a complete baseline
            _ = DumpStore.markDrainDone(runID)   // file-wipe coordination
            syncState.persist()                  // persist baseline even if EA still uploading
            finalizeFullSyncIfComplete()         // green/100% only if EA also done
        }
        return complete
    }

    /// Loads the local dump into MySQL. Wipes the health tables once per run for a
    /// clean full replace (no stale rows, no cross-run duplicates), then streams
    /// rows in. Resumable per-table via a frame cursor; `INSERT IGNORE` keeps a
    /// re-run of a partially-applied frame safe across background-task windows.
    /// Does NOT manage the running flags — the caller owns them.
    @discardableResult
    func drainDump(runID: String, config: MySQLConfig) async -> Bool {
        guard let manifest = DumpStore.loadManifest(runID) else {
            syncState.errorMessage = "Dump manifest for \(runID) is missing — cannot drain to MySQL"
            return false
        }
        guard let key = DumpKeychain.load(runID: runID) else {
            syncState.errorMessage = "Dump key for \(runID) is missing — cannot decrypt to drain to MySQL"
            return false
        }
        syncState.errorMessage = nil
        syncState.currentOperation = "Connecting to MySQL…"

        let svc = MySQLService()
        let wipedKey = "dumpDrainWiped.\(runID)"
        do {
            try await svc.connect(config: config)
            let (ok, schemaErr) = await SchemaService.initializeSchema(mysql: svc)
            if !ok { throw MySQLError.queryError(code: 0, message: schemaErr ?? "Schema error") }

            // Wipe the health tables once per run. Guarded so a resumed drain
            // doesn't re-truncate rows it already loaded in an earlier window.
            if !UserDefaults.standard.bool(forKey: wipedKey) {
                syncState.currentOperation = "Clearing MySQL health tables…"
                for entry in manifest.tables {
                    try await svc.execute("TRUNCATE TABLE `\(entry.table.sqlTable)`")
                }
                UserDefaults.standard.set(true, forKey: wipedKey)
            }

            let totalTables = max(1, manifest.tables.count)
            for (idx, entry) in manifest.tables.enumerated() {
                try Task.checkCancellation()
                SyncService.noteSyncActivity()
                syncState.currentOperation = "Loading \(entry.table.rawValue) → MySQL…"
                try await drainTable(entry.table, runID: runID, key: key, mysql: svc)
                // Advance the MySQL slice of the phase-weighted overall progress.
                syncState.updateFullSyncProgress(mysql: Double(idx + 1) / Double(totalTables))
                updateLiveActivity(phase: "MySQL", operation: "Loaded \(entry.table.rawValue) → MySQL")
            }
            await svc.disconnect()
            // Clean up per-run drain bookkeeping.
            UserDefaults.standard.removeObject(forKey: wipedKey)
            for entry in manifest.tables {
                UserDefaults.standard.removeObject(forKey: "dumpDrainCursor.\(runID).\(entry.table.rawValue)")
            }
            syncState.currentOperation = "MySQL load complete"
            syncState.persist()
            return true
        } catch is CancellationError {
            await svc.disconnect()
            syncState.currentOperation = "MySQL load paused"
            return false
        } catch {
            await svc.disconnect()
            syncState.errorMessage = error.localizedDescription
            return false
        }
    }

    // MARK: - Per-table drain

    private func drainTable(_ table: DumpTable, runID: String, key: SymmetricKey, mysql: MySQLService) async throws {
        let fileURL = DumpStore.runDir(runID).appendingPathComponent(table.fileName)
        let reader = try DumpFrameReader(fileURL: fileURL, key: key)
        defer { try? reader.close() }

        // Resume: skip frames already committed in a prior background window.
        let cursorKey = "dumpDrainCursor.\(runID).\(table.rawValue)"
        let done = UserDefaults.standard.integer(forKey: cursorKey)
        if done > 0 { try reader.skipFrames(done) }

        let decoder = JSONDecoder()
        let header = Self.insertHeader(for: table)
        let suffix = Self.insertSuffix(for: table)

        while let chunk = try reader.nextChunk() {
            try Task.checkCancellation()
            var tuples: [String] = []
            var bytes = 0
            // One frame inflates to complete NDJSON lines; flush in sub-batches.
            for line in chunk.split(separator: 0x0A) where !line.isEmpty {
                let tuple = try Self.tuple(for: table, line: Data(line), decoder: decoder)
                tuples.append(tuple)
                bytes += tuple.utf8.count + 1
                if bytes >= Self.drainSQLBudget || tuples.count >= Self.drainRowCap {
                    try await flush(tuples, header: header, suffix: suffix, mysql: mysql)
                    tuples.removeAll(keepingCapacity: true)
                    bytes = 0
                }
            }
            try await flush(tuples, header: header, suffix: suffix, mysql: mysql)
            // Commit the frame cursor only after the whole frame is applied.
            UserDefaults.standard.set(reader.frameIndex, forKey: cursorKey)
        }
    }

    private func flush(_ tuples: [String], header: String, suffix: String, mysql: MySQLService) async throws {
        guard !tuples.isEmpty else { return }
        try await mysql.execute(header + " VALUES " + tuples.joined(separator: ",") + suffix)
    }

    // MARK: - INSERT headers (mirror SyncService's live INSERTs)

    private static func insertHeader(for table: DumpTable) -> String {
        switch table {
        case .quantitySamples:
            return "INSERT IGNORE INTO health_quantity_samples (uuid,type,value,unit,start_date,end_date,source_name,source_bundle_id,device_name,metadata)"
        case .categorySamples:
            return "INSERT IGNORE INTO health_category_samples (uuid,type,value,value_label,start_date,end_date,source_name,source_bundle_id,device_name,metadata)"
        case .workouts:
            return "INSERT IGNORE INTO health_workouts (uuid,activity_type,duration_seconds,total_energy_burned_kcal,total_distance_meters,total_swimming_strokes,total_flights_climbed,start_date,end_date,source_name,source_bundle_id,device_name,metadata)"
        case .workoutRoutes:
            return "INSERT IGNORE INTO health_workout_routes (uuid,workout_uuid,start_date,location_count,locations_json)"
        case .bloodPressure:
            return "INSERT IGNORE INTO health_blood_pressure (uuid,systolic,diastolic,start_date,source_name,device_name,metadata)"
        case .ecg:
            return "INSERT IGNORE INTO health_ecg (uuid,classification,average_heart_rate,sampling_frequency,voltage_measurements,start_date,source_name,metadata)"
        case .audiograms:
            return "INSERT IGNORE INTO health_audiograms (uuid,sensitivity_points,start_date,source_name,metadata)"
        case .activitySummaries:
            // Date-keyed: upsert so a re-export refreshes the day's ring totals.
            return "INSERT INTO health_activity_summaries (date,active_energy_burned,active_energy_burned_goal,exercise_time_minutes,exercise_time_goal_minutes,stand_hours,stand_hours_goal)"
        case .medications:
            return "INSERT IGNORE INTO health_medications (uuid,medication_name,dosage,log_status,start_date,end_date,source_name,source_bundle_id,device_name,metadata)"
        case .visionPrescriptions:
            return "INSERT IGNORE INTO health_vision_prescriptions (uuid,start_date,end_date,prescription_type,right_eye_sphere,right_eye_cylinder,right_eye_axis,right_eye_add_power,right_eye_base_curve,right_eye_diameter,left_eye_sphere,left_eye_cylinder,left_eye_axis,left_eye_add_power,left_eye_base_curve,left_eye_diameter,expiration_date,source_name,source_bundle_id,device_name)"
        case .stateOfMind:
            return "INSERT IGNORE INTO health_state_of_mind (uuid,start_date,end_date,kind,valence,valence_classification,labels_json,associations_json,source_name,source_bundle_id,device_name)"
        }
    }

    /// The `ON DUPLICATE KEY UPDATE` tail for the one date-keyed table.
    private static func insertSuffix(for table: DumpTable) -> String {
        guard table == .activitySummaries else { return "" }
        return " ON DUPLICATE KEY UPDATE active_energy_burned=VALUES(active_energy_burned),active_energy_burned_goal=VALUES(active_energy_burned_goal),exercise_time_minutes=VALUES(exercise_time_minutes),exercise_time_goal_minutes=VALUES(exercise_time_goal_minutes),stand_hours=VALUES(stand_hours),stand_hours_goal=VALUES(stand_hours_goal)"
    }

    // MARK: - Row → VALUES tuple (decode then mirror the live SQL exactly)

    private static func tuple(for table: DumpTable, line: Data, decoder: JSONDecoder) throws -> String {
        let q = MySQLEscape.quote
        let qd = MySQLEscape.quoteDouble
        func qi(_ v: Int?) -> String { v.map(String.init) ?? "NULL" }

        switch table {
        case .quantitySamples:
            let r = try decoder.decode(HBQuantityRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.type)),\(qd(r.value)),\(q(r.unit)),\(q(r.start_date)),\(q(r.end_date)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)),\(q(r.metadata)))"
        case .categorySamples:
            let r = try decoder.decode(HBCategoryRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.type)),\(r.value),\(q(r.value_label)),\(q(r.start_date)),\(q(r.end_date)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)),\(q(r.metadata)))"
        case .workouts:
            let r = try decoder.decode(HBWorkoutRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.activity_type)),\(qd(r.duration_seconds)),\(qd(r.total_energy_burned_kcal)),\(qd(r.total_distance_meters)),\(qd(r.total_swimming_strokes)),\(qd(r.total_flights_climbed)),\(q(r.start_date)),\(q(r.end_date)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)),\(q(r.metadata)))"
        case .workoutRoutes:
            let r = try decoder.decode(HBWorkoutRouteRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.workout_uuid)),\(q(r.start_date)),\(r.location_count),\(q(r.locations_json)))"
        case .bloodPressure:
            let r = try decoder.decode(HBBloodPressureRow.self, from: line)
            return "(\(q(r.uuid)),\(qd(r.systolic)),\(qd(r.diastolic)),\(q(r.start_date)),\(q(r.source_name)),\(q(r.device_name)),\(q(r.metadata)))"
        case .ecg:
            let r = try decoder.decode(HBEcgRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.classification)),\(qd(r.average_heart_rate)),\(qd(r.sampling_frequency)),\(q(r.voltage_measurements)),\(q(r.start_date)),\(q(r.source_name)),\(q(r.metadata)))"
        case .audiograms:
            let r = try decoder.decode(HBAudiogramRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.sensitivity_points)),\(q(r.start_date)),\(q(r.source_name)),\(q(r.metadata)))"
        case .activitySummaries:
            let r = try decoder.decode(HBActivitySummaryRow.self, from: line)
            return "(\(q(r.date)),\(qd(r.active_energy_burned)),\(qd(r.active_energy_burned_goal)),\(qd(r.exercise_time_minutes)),\(qd(r.exercise_time_goal_minutes)),\(qi(r.stand_hours)),\(qi(r.stand_hours_goal)))"
        case .medications:
            let r = try decoder.decode(HBMedicationRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.medication_name)),\(q(r.dosage)),\(q(r.log_status)),\(q(r.start_date)),\(q(r.end_date)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)),\(q(r.metadata)))"
        case .visionPrescriptions:
            let r = try decoder.decode(HBVisionRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.start_date)),\(q(r.end_date)),\(r.prescription_type),\(qd(r.right_eye_sphere)),\(qd(r.right_eye_cylinder)),\(qd(r.right_eye_axis)),\(qd(r.right_eye_add_power)),\(qd(r.right_eye_base_curve)),\(qd(r.right_eye_diameter)),\(qd(r.left_eye_sphere)),\(qd(r.left_eye_cylinder)),\(qd(r.left_eye_axis)),\(qd(r.left_eye_add_power)),\(qd(r.left_eye_base_curve)),\(qd(r.left_eye_diameter)),\(q(r.expiration_date)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)))"
        case .stateOfMind:
            let r = try decoder.decode(HBStateOfMindRow.self, from: line)
            return "(\(q(r.uuid)),\(q(r.start_date)),\(q(r.end_date)),\(r.kind),\(qd(r.valence)),\(qi(r.valence_classification)),\(q(r.labels_json)),\(q(r.associations_json)),\(q(r.source_name)),\(q(r.source_bundle_id)),\(q(r.device_name)))"
        }
    }
}
