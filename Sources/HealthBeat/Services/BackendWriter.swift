import Foundation

/// Abstraction over where a sync batch goes. There are two destinations
/// in practice:
///
///   - **MySQL** (existing direct write inside `SyncService` itself; the
///     inline INSERT IGNORE statements have not been pulled out into a
///     dedicated `MySQLBackendWriter` yet — that's a future refactor).
///   - **EA** ({@link EABackendWriter}) — POSTs the same typed batch as
///     JSON to `/api/v1/healthbeat/*` on the user's EA install.
///
/// `SyncService` currently writes to MySQL inline (when enabled), and
/// additionally hands the typed batch to a `BackendWriter` if one is
/// supplied — which is how EA gets every batch the device produces.
/// {@link MultiBackendWriter} fans a single batch out to multiple
/// writers; today that's just `[EABackendWriter]` but the protocol
/// leaves room to add more without further changes to call sites.
///
/// Every method is `async throws`. Errors surface to the caller — never
/// swallowed. A failure on the EA side does NOT abort the MySQL write
/// (and vice-versa) because `SyncService` calls the writer AFTER the
/// MySQL execute succeeds; if the writer throws, the sync is logged as
/// failed for that pass but the MySQL data was already persisted.
protocol BackendWriter: Sendable {
    func writeQuantitySamples(_ rows: [HBQuantityRow]) async throws
    func writeCategorySamples(_ rows: [HBCategoryRow]) async throws
    func writeWorkouts(_ rows: [HBWorkoutRow]) async throws
    func writeWorkoutRoutes(_ rows: [HBWorkoutRouteRow]) async throws
    func writeBloodPressure(_ rows: [HBBloodPressureRow]) async throws
    func writeEcg(_ rows: [HBEcgRow]) async throws
    func writeAudiograms(_ rows: [HBAudiogramRow]) async throws
    func writeActivitySummaries(_ rows: [HBActivitySummaryRow]) async throws
    func writeMedications(_ rows: [HBMedicationRow]) async throws
    func writeVisionPrescriptions(_ rows: [HBVisionRow]) async throws
    func writeStateOfMind(_ rows: [HBStateOfMindRow]) async throws
    func writeLocationTracks(_ rows: [HBLocationRow]) async throws
    func writeGeofenceEvent(_ row: HBGeofenceEventRow) async throws
    func writeGeofenceDefinitions(_ rows: [HBGeofenceDefinitionRow]) async throws
    func writePlaceCategories(_ rows: [HBPlaceCategoryRow]) async throws
    func writeSyncLog(_ row: HBSyncLogRow) async throws

    /// Delete rows in a (table[, type], date-window) slice whose UUID is
    /// NOT in `validUUIDs`. Mirrors `SyncService.reconcileStaleRecords`
    /// so EA stays in lock-step with Apple Health when samples are
    /// removed on-device. `table` is the unprefixed name HealthBeat
    /// itself uses (e.g. `health_quantity_samples`); the EA writer maps
    /// it to the matching `hb_*` table server-side.
    func reconcileSlice(
        table: String,
        typeColumn: String?,
        typeValue: String?,
        since: String,
        until: String,
        validUUIDs: [String]
    ) async throws -> Int
}

/// Writes every batch to the EA `/api/v1/healthbeat/*` ingest API.
struct EABackendWriter: BackendWriter {
    let service: EAService

    init(config: EAConfig) {
        self.service = EAService(config: config)
    }

    func writeQuantitySamples(_ rows: [HBQuantityRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/quantity-samples", records: rows)
    }
    func writeCategorySamples(_ rows: [HBCategoryRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/category-samples", records: rows)
    }
    func writeWorkouts(_ rows: [HBWorkoutRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/workouts", records: rows)
    }
    func writeWorkoutRoutes(_ rows: [HBWorkoutRouteRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/workout-routes", records: rows)
    }
    func writeBloodPressure(_ rows: [HBBloodPressureRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/blood-pressure", records: rows)
    }
    func writeEcg(_ rows: [HBEcgRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/ecg", records: rows)
    }
    func writeAudiograms(_ rows: [HBAudiogramRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/audiograms", records: rows)
    }
    func writeActivitySummaries(_ rows: [HBActivitySummaryRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/activity-summaries", records: rows)
    }
    func writeMedications(_ rows: [HBMedicationRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/medications", records: rows)
    }
    func writeVisionPrescriptions(_ rows: [HBVisionRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/vision-prescriptions", records: rows)
    }
    func writeStateOfMind(_ rows: [HBStateOfMindRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/state-of-mind", records: rows)
    }
    func writeLocationTracks(_ rows: [HBLocationRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/location-tracks", records: rows)
    }
    func writeGeofenceEvent(_ row: HBGeofenceEventRow) async throws {
        try await service.postRecords(endpoint: "/api/v1/healthbeat/geofence-events", records: [row])
    }
    func writeGeofenceDefinitions(_ rows: [HBGeofenceDefinitionRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/geofences/upsert", records: rows)
    }
    func writePlaceCategories(_ rows: [HBPlaceCategoryRow]) async throws {
        guard !rows.isEmpty else { return }
        try await service.postRecords(endpoint: "/api/v1/healthbeat/place-categories/upsert", records: rows)
    }
    func writeSyncLog(_ row: HBSyncLogRow) async throws {
        try await service.postRecords(endpoint: "/api/v1/healthbeat/sync-log", records: [row])
    }

    func reconcileSlice(
        table: String, typeColumn: String?, typeValue: String?,
        since: String, until: String, validUUIDs: [String]
    ) async throws -> Int {
        guard !validUUIDs.isEmpty else { return 0 }
        // HealthBeat's MySQL schema names are `health_*` / `location_*`;
        // EA mirrors them as `hb_*`. Translate prefix client-side so the
        // server protocol doesn't leak both vocabularies.
        let eaTable: String = {
            if table.hasPrefix("health_")   { return "hb_" + String(table.dropFirst("health_".count)) }
            if table.hasPrefix("location_") { return "hb_" + String(table.dropFirst("location_".count)) }
            return table
        }()
        return try await service.postReconcile(
            table: eaTable, typeColumn: typeColumn, typeValue: typeValue,
            since: since, until: until, validUUIDs: validUUIDs
        )
    }
}

/// Fans a single batch out to N writers in sequence. Throws on the
/// first failure; subsequent writers are still invoked because the
/// MySQL write has already happened — we want as many destinations as
/// possible to receive the data even if one fails. The first thrown
/// error is rethrown at the end.
struct MultiBackendWriter: BackendWriter {
    let writers: [BackendWriter]

    private func fanout(_ op: (BackendWriter) async throws -> Void) async throws {
        var firstError: Error?
        for w in writers {
            do { try await op(w) }
            catch { firstError = firstError ?? error }
        }
        if let e = firstError { throw e }
    }

    func writeQuantitySamples(_ rows: [HBQuantityRow]) async throws        { try await fanout { try await $0.writeQuantitySamples(rows) } }
    func writeCategorySamples(_ rows: [HBCategoryRow]) async throws        { try await fanout { try await $0.writeCategorySamples(rows) } }
    func writeWorkouts(_ rows: [HBWorkoutRow]) async throws                { try await fanout { try await $0.writeWorkouts(rows) } }
    func writeWorkoutRoutes(_ rows: [HBWorkoutRouteRow]) async throws      { try await fanout { try await $0.writeWorkoutRoutes(rows) } }
    func writeBloodPressure(_ rows: [HBBloodPressureRow]) async throws     { try await fanout { try await $0.writeBloodPressure(rows) } }
    func writeEcg(_ rows: [HBEcgRow]) async throws                         { try await fanout { try await $0.writeEcg(rows) } }
    func writeAudiograms(_ rows: [HBAudiogramRow]) async throws            { try await fanout { try await $0.writeAudiograms(rows) } }
    func writeActivitySummaries(_ rows: [HBActivitySummaryRow]) async throws { try await fanout { try await $0.writeActivitySummaries(rows) } }
    func writeMedications(_ rows: [HBMedicationRow]) async throws          { try await fanout { try await $0.writeMedications(rows) } }
    func writeVisionPrescriptions(_ rows: [HBVisionRow]) async throws      { try await fanout { try await $0.writeVisionPrescriptions(rows) } }
    func writeStateOfMind(_ rows: [HBStateOfMindRow]) async throws         { try await fanout { try await $0.writeStateOfMind(rows) } }
    func writeLocationTracks(_ rows: [HBLocationRow]) async throws         { try await fanout { try await $0.writeLocationTracks(rows) } }
    func writeGeofenceEvent(_ row: HBGeofenceEventRow) async throws        { try await fanout { try await $0.writeGeofenceEvent(row) } }
    func writeGeofenceDefinitions(_ rows: [HBGeofenceDefinitionRow]) async throws { try await fanout { try await $0.writeGeofenceDefinitions(rows) } }
    func writePlaceCategories(_ rows: [HBPlaceCategoryRow]) async throws   { try await fanout { try await $0.writePlaceCategories(rows) } }
    func writeSyncLog(_ row: HBSyncLogRow) async throws                    { try await fanout { try await $0.writeSyncLog(row) } }

    func reconcileSlice(
        table: String, typeColumn: String?, typeValue: String?,
        since: String, until: String, validUUIDs: [String]
    ) async throws -> Int {
        // Aggregate deletion count across all writers; if multiple
        // destinations are configured, the caller still gets a single
        // "rows removed" number to log.
        var total = 0
        var firstError: Error?
        for w in writers {
            do { total += try await w.reconcileSlice(
                    table: table, typeColumn: typeColumn, typeValue: typeValue,
                    since: since, until: until, validUUIDs: validUUIDs)
            } catch { firstError = firstError ?? error }
        }
        if let e = firstError { throw e }
        return total
    }
}
