import Foundation

// MARK: - Date formatter (MySQL datetime format, matching SyncService)

private let geoSqlDateFormatter: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    f.timeZone = TimeZone(identifier: "UTC")
    f.locale = Locale(identifier: "en_US_POSIX")
    return f
}()

private func sqlDate(_ date: Date) -> String {
    geoSqlDateFormatter.string(from: date)
}

private let geoSqlDateFormatterNoFrac: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm:ss"
    f.timeZone = TimeZone(identifier: "UTC")
    f.locale = Locale(identifier: "en_US_POSIX")
    return f
}()

private func parseDate(_ str: String) -> Date? {
    geoSqlDateFormatter.date(from: str) ?? geoSqlDateFormatterNoFrac.date(from: str)
}

// MARK: - GeofenceSyncService

enum GeofenceSyncService {

    /// Two-way sync of place categories. Must be called before `syncGeofences`
    /// since geofences reference categories via `place_category_id`.
    @discardableResult
    static func syncPlaceCategories(mysql: MySQLService) async throws -> Bool {
        var localCategories = PlaceCategory.loadAllIncludingDeleted()
        let localById = Dictionary(localCategories.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        var changed = false

        // Step 1: Pull all remote place categories (small dataset, full merge every sync
        // avoids cursor/timezone mismatch between device time and MySQL server time)
        let rows = try await mysql.query(
            "SELECT id, name, system_image, sort_order, origin, is_deleted, created_at, updated_at FROM place_category_definitions"
        )

        for row in rows {
            guard let idStr = row["id"], let remoteId = UUID(uuidString: idStr),
                  let name = row["name"],
                  let systemImage = row["system_image"],
                  let sortOrderStr = row["sort_order"], let sortOrder = Int(sortOrderStr),
                  let originStr = row["origin"],
                  let isDeletedStr = row["is_deleted"],
                  let updatedAtStr = row["updated_at"], let remoteUpdatedAt = parseDate(updatedAtStr)
            else { continue }

            let remoteOrigin = PlaceCategory.SyncOrigin(rawValue: originStr) ?? .database
            let remoteIsDeleted = isDeletedStr == "1"

            if let local = localById[remoteId] {
                // Exists locally — last-write-wins
                if remoteUpdatedAt > local.updatedAt {
                    if let idx = localCategories.firstIndex(where: { $0.id == remoteId }) {
                        localCategories[idx].name = name
                        localCategories[idx].systemImage = systemImage
                        localCategories[idx].sortOrder = sortOrder
                        localCategories[idx].origin = remoteOrigin
                        localCategories[idx].isDeleted = remoteIsDeleted
                        localCategories[idx].updatedAt = remoteUpdatedAt
                        changed = true
                    }
                }
                // else local is newer — will push in step 2
            } else if !remoteIsDeleted {
                // New from database
                let cat = PlaceCategory(
                    id: remoteId, name: name, systemImage: systemImage,
                    origin: remoteOrigin, isDeleted: false, updatedAt: remoteUpdatedAt,
                    sortOrder: sortOrder
                )
                localCategories.append(cat)
                changed = true
            }
            // If no local match and is_deleted = true, skip
        }

        // Step 2: Push all local categories (idempotent via ON DUPLICATE KEY UPDATE)
        let categoriesToPush = localCategories

        for cat in categoriesToPush {
            let id = MySQLEscape.quote(cat.id.uuidString)
            let name = MySQLEscape.quote(cat.name)
            let systemImage = MySQLEscape.quote(cat.systemImage)
            let sortOrder = cat.sortOrder
            let origin = MySQLEscape.quote(cat.origin.rawValue)
            let isDeleted = cat.isDeleted ? 1 : 0
            let updatedAt = MySQLEscape.quote(sqlDate(cat.updatedAt))
            let createdAt = updatedAt // use updatedAt as createdAt for push

            let sql = """
            INSERT INTO place_category_definitions (id, name, system_image, sort_order, origin, is_deleted, created_at, updated_at) \
            VALUES (\(id), \(name), \(systemImage), \(sortOrder), \(origin), \(isDeleted), \(createdAt), \(updatedAt)) \
            ON DUPLICATE KEY UPDATE \
            name = IF(VALUES(updated_at) > updated_at, VALUES(name), name), \
            system_image = IF(VALUES(updated_at) > updated_at, VALUES(system_image), system_image), \
            sort_order = IF(VALUES(updated_at) > updated_at, VALUES(sort_order), sort_order), \
            is_deleted = IF(VALUES(updated_at) > updated_at, VALUES(is_deleted), is_deleted), \
            updated_at = IF(VALUES(updated_at) > updated_at, VALUES(updated_at), updated_at)
            """
            try await mysql.execute(sql)
        }

        // Step 3: Purge old soft-deleted records
        try await mysql.execute(
            "DELETE FROM place_category_definitions WHERE is_deleted = 1 AND updated_at < NOW() - INTERVAL 30 DAY"
        )
        let thirtyDaysAgo = Date().addingTimeInterval(-30 * 24 * 3600)
        localCategories.removeAll { $0.isDeleted && $0.updatedAt < thirtyDaysAgo }

        // Step 4: Save
        if changed || !categoriesToPush.isEmpty {
            PlaceCategory.saveAll(localCategories)
        }

        // Step 5: EA bidirectional sync (only if user has enabled the EA
        // destination in Settings). Pull updated rows from EA, merge with
        // local + push every local row up so EA learns about new app-origin
        // categories.
        if EAConfig.load().isConfigured {
            let eaChanged = try await syncPlaceCategoriesWithEA(into: &localCategories)
            if eaChanged {
                changed = true
                PlaceCategory.saveAll(localCategories)
            }
        }

        return changed
    }

    /// EA bidirectional sync for place categories. Mirrors the MySQL flow
    /// but over HTTP. Last-write-wins by `updated_at`.
    private static func syncPlaceCategoriesWithEA(into localCategories: inout [PlaceCategory]) async throws -> Bool {
        let cfg = EAConfig.load()
        guard cfg.isConfigured else { return false }
        let service = EAService(config: cfg)

        var changed = false
        let cursor = UserDefaults.standard.string(forKey: "ea_categories_cursor")
        let remote = try await service.pullCategories(since: cursor)

        var localById = Dictionary(localCategories.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        for row in remote {
            guard let remoteId = UUID(uuidString: row.id) else { continue }
            let remoteUpdatedAt = row.updated_at.flatMap(parseDate) ?? Date()
            let remoteOrigin = PlaceCategory.SyncOrigin(rawValue: row.origin ?? "database") ?? .database
            let remoteIsDeleted = (row.is_deleted) == 1

            if let local = localById[remoteId] {
                if remoteUpdatedAt > local.updatedAt {
                    if let idx = localCategories.firstIndex(where: { $0.id == remoteId }) {
                        localCategories[idx].name = row.name
                        localCategories[idx].systemImage = row.system_image
                        localCategories[idx].sortOrder = row.sort_order ?? localCategories[idx].sortOrder
                        localCategories[idx].origin = remoteOrigin
                        localCategories[idx].isDeleted = remoteIsDeleted
                        localCategories[idx].updatedAt = remoteUpdatedAt
                        localById[remoteId] = localCategories[idx]
                        changed = true
                    }
                }
            } else if !remoteIsDeleted {
                let cat = PlaceCategory(
                    id: remoteId, name: row.name, systemImage: row.system_image,
                    origin: remoteOrigin, isDeleted: false, updatedAt: remoteUpdatedAt,
                    sortOrder: row.sort_order ?? 0
                )
                localCategories.append(cat)
                localById[remoteId] = cat
                changed = true
            }
        }

        // Push every local category up.
        let toPush: [HBPlaceCategoryRow] = localCategories.map { cat in
            HBPlaceCategoryRow(
                id: cat.id.uuidString,
                name: cat.name,
                system_image: cat.systemImage,
                sort_order: cat.sortOrder,
                origin: cat.origin.rawValue,
                is_deleted: cat.isDeleted ? 1 : 0,
                created_at: sqlDate(cat.updatedAt),
                updated_at: sqlDate(cat.updatedAt)
            )
        }
        if !toPush.isEmpty {
            try await service.postRecords(endpoint: "/api/v1/healthbeat/place-categories/upsert", records: toPush)
        }

        // Advance the cursor to "now" so subsequent pulls only fetch deltas.
        UserDefaults.standard.set(sqlDate(Date()), forKey: "ea_categories_cursor")
        return changed
    }

    /// Two-way sync of geofence definitions. Returns true if local geofences were modified.
    static func syncGeofences(mysql: MySQLService) async throws -> Bool {
        var localFences = GeoFence.loadAllIncludingDeleted()
        let localById = Dictionary(localFences.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        var changed = false

        // Diagnostic: verify which database and table we're reading from
        let dbRows = try await mysql.query("SELECT DATABASE() as db")
        let currentDB = dbRows.first?["db"] ?? "unknown"
        let countRows = try await mysql.query("SELECT COUNT(*) as cnt FROM geofence_definitions")
        let serverCount = countRows.first?["cnt"] ?? "?"
        print("[GeofenceSyncService] Connected to database: '\(currentDB)', server row count: \(serverCount)")

        // Step 1: Pull all remote geofences (small dataset, full merge every sync
        // avoids cursor/timezone mismatch between device time and MySQL server time)
        let rows = try await mysql.query(
            "SELECT id, name, latitude, longitude, radius, place_category_id, origin, is_deleted, created_at, updated_at FROM geofence_definitions"
        )

        print("[GeofenceSyncService] Pulled \(rows.count) geofence row(s) from DB, \(localFences.count) local fence(s)")

        for row in rows {
            guard let idStr = row["id"], let remoteId = UUID(uuidString: idStr),
                  let name = row["name"],
                  let latStr = row["latitude"], let latitude = Double(latStr),
                  let lonStr = row["longitude"], let longitude = Double(lonStr),
                  let radiusStr = row["radius"], let radius = Double(radiusStr),
                  let originStr = row["origin"],
                  let isDeletedStr = row["is_deleted"],
                  let updatedAtStr = row["updated_at"], let remoteUpdatedAt = parseDate(updatedAtStr)
            else {
                print("[GeofenceSyncService] Skipping unparseable row: \(row)")
                continue
            }

            let remoteOrigin = GeoFence.SyncOrigin(rawValue: originStr) ?? .database
            let remoteIsDeleted = isDeletedStr == "1"
            let placeCategoryId: UUID? = row["place_category_id"].flatMap { UUID(uuidString: $0) }

            if let local = localById[remoteId] {
                // Exists locally — last-write-wins
                if remoteUpdatedAt > local.updatedAt {
                    if let idx = localFences.firstIndex(where: { $0.id == remoteId }) {
                        localFences[idx].name = name
                        localFences[idx].latitude = latitude
                        localFences[idx].longitude = longitude
                        localFences[idx].radius = radius
                        localFences[idx].placeCategoryId = placeCategoryId
                        localFences[idx].origin = remoteOrigin
                        localFences[idx].isDeleted = remoteIsDeleted
                        localFences[idx].updatedAt = remoteUpdatedAt
                        changed = true
                    }
                }
            } else if !remoteIsDeleted {
                // New from database
                let fence = GeoFence(
                    name: name, latitude: latitude, longitude: longitude, radius: radius,
                    placeCategoryId: placeCategoryId, origin: remoteOrigin,
                    isDeleted: false, updatedAt: remoteUpdatedAt
                )
                // Preserve the remote ID
                var newFence = fence
                newFence.id = remoteId
                localFences.append(newFence)
                changed = true
            }
        }

        // Step 2: Push all local geofences (idempotent via ON DUPLICATE KEY UPDATE)
        let fencesToPush = localFences

        for fence in fencesToPush {
            let id = MySQLEscape.quote(fence.id.uuidString)
            let name = MySQLEscape.quote(fence.name)
            let lat = fence.latitude
            let lon = fence.longitude
            let radius = fence.radius
            let placeCatId = fence.placeCategoryId.map { MySQLEscape.quote($0.uuidString) } ?? "NULL"
            let origin = MySQLEscape.quote(fence.origin.rawValue)
            let isDeleted = fence.isDeleted ? 1 : 0
            let updatedAt = MySQLEscape.quote(sqlDate(fence.updatedAt))
            let createdAt = updatedAt

            let sql = """
            INSERT INTO geofence_definitions (id, name, latitude, longitude, radius, place_category_id, origin, is_deleted, created_at, updated_at) \
            VALUES (\(id), \(name), \(lat), \(lon), \(radius), \(placeCatId), \(origin), \(isDeleted), \(createdAt), \(updatedAt)) \
            ON DUPLICATE KEY UPDATE \
            name = IF(VALUES(updated_at) > updated_at, VALUES(name), name), \
            latitude = IF(VALUES(updated_at) > updated_at, VALUES(latitude), latitude), \
            longitude = IF(VALUES(updated_at) > updated_at, VALUES(longitude), longitude), \
            radius = IF(VALUES(updated_at) > updated_at, VALUES(radius), radius), \
            place_category_id = IF(VALUES(updated_at) > updated_at, VALUES(place_category_id), place_category_id), \
            is_deleted = IF(VALUES(updated_at) > updated_at, VALUES(is_deleted), is_deleted), \
            updated_at = IF(VALUES(updated_at) > updated_at, VALUES(updated_at), updated_at)
            """
            try await mysql.execute(sql)
        }

        // Step 3: Purge old soft-deleted records
        try await mysql.execute(
            "DELETE FROM geofence_definitions WHERE is_deleted = 1 AND updated_at < NOW() - INTERVAL 30 DAY"
        )
        let thirtyDaysAgo = Date().addingTimeInterval(-30 * 24 * 3600)
        localFences.removeAll { $0.isDeleted && $0.updatedAt < thirtyDaysAgo }

        // Step 4: Save
        if changed || !fencesToPush.isEmpty {
            GeoFence.saveAll(localFences)
        }

        // Step 5: EA bidirectional sync — same shape as the categories
        // flow above, but over HTTP against /api/v1/healthbeat/geofences*.
        if EAConfig.load().isConfigured {
            let eaChanged = try await syncGeofencesWithEA(into: &localFences)
            if eaChanged {
                changed = true
                GeoFence.saveAll(localFences)
            }
        }

        let activeCount = localFences.filter { !$0.isDeleted }.count
        print("[GeofenceSyncService] Sync done: changed=\(changed), pushed=\(fencesToPush.count), active=\(activeCount)")

        return changed
    }

    /// EA bidirectional sync for geofences. Same shape as the MySQL flow.
    private static func syncGeofencesWithEA(into localFences: inout [GeoFence]) async throws -> Bool {
        let cfg = EAConfig.load()
        guard cfg.isConfigured else { return false }
        let service = EAService(config: cfg)

        var changed = false
        let cursor = UserDefaults.standard.string(forKey: "ea_geofences_cursor")
        let remote = try await service.pullGeofences(since: cursor)

        var localById = Dictionary(localFences.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        for row in remote {
            guard let remoteId = UUID(uuidString: row.id) else { continue }
            let remoteUpdatedAt = row.updated_at.flatMap(parseDate) ?? Date()
            let remoteOrigin = GeoFence.SyncOrigin(rawValue: row.origin ?? "database") ?? .database
            let remoteIsDeleted = row.is_deleted == 1
            let placeCategoryId = row.place_category_id.flatMap { UUID(uuidString: $0) }

            if let local = localById[remoteId] {
                if remoteUpdatedAt > local.updatedAt {
                    if let idx = localFences.firstIndex(where: { $0.id == remoteId }) {
                        localFences[idx].name = row.name
                        localFences[idx].latitude = row.latitude
                        localFences[idx].longitude = row.longitude
                        localFences[idx].radius = row.radius
                        localFences[idx].placeCategoryId = placeCategoryId
                        localFences[idx].origin = remoteOrigin
                        localFences[idx].isDeleted = remoteIsDeleted
                        localFences[idx].updatedAt = remoteUpdatedAt
                        localById[remoteId] = localFences[idx]
                        changed = true
                    }
                }
            } else if !remoteIsDeleted {
                var fence = GeoFence(
                    name: row.name, latitude: row.latitude, longitude: row.longitude,
                    radius: row.radius, placeCategoryId: placeCategoryId,
                    origin: remoteOrigin, isDeleted: false, updatedAt: remoteUpdatedAt
                )
                fence.id = remoteId
                localFences.append(fence)
                localById[remoteId] = fence
                changed = true
            }
        }

        // Push every local geofence up so EA learns about app-origin rows.
        let toPush: [HBGeofenceDefinitionRow] = localFences.map { fence in
            HBGeofenceDefinitionRow(
                id: fence.id.uuidString,
                name: fence.name,
                latitude: fence.latitude,
                longitude: fence.longitude,
                radius: fence.radius,
                place_category_id: fence.placeCategoryId?.uuidString,
                origin: fence.origin.rawValue,
                is_deleted: fence.isDeleted ? 1 : 0,
                created_at: sqlDate(fence.updatedAt),
                updated_at: sqlDate(fence.updatedAt)
            )
        }
        if !toPush.isEmpty {
            try await service.postRecords(endpoint: "/api/v1/healthbeat/geofences/upsert", records: toPush)
        }

        UserDefaults.standard.set(sqlDate(Date()), forKey: "ea_geofences_cursor")
        return changed
    }
}
