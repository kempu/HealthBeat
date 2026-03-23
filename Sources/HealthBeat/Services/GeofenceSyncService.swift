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

private func parseDate(_ str: String) -> Date? {
    geoSqlDateFormatter.date(from: str)
}

// MARK: - Cursor keys

private let placeCategoryCursorKey = "place_category_sync_cursor"
private let geofenceCursorKey = "geofence_sync_cursor"

// MARK: - GeofenceSyncService

enum GeofenceSyncService {

    /// Two-way sync of place categories. Must be called before `syncGeofences`
    /// since geofences reference categories via `place_category_id`.
    @discardableResult
    static func syncPlaceCategories(mysql: MySQLService) async throws -> Bool {
        let cursor = loadCursor(key: placeCategoryCursorKey)
        var localCategories = PlaceCategory.loadAllIncludingDeleted()
        let localById = Dictionary(localCategories.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        var changed = false

        // Step 1: Pull remote changes
        let cursorStr = cursor.map { MySQLEscape.quote(sqlDate($0)) } ?? "'1970-01-01 00:00:00.000'"
        let rows = try await mysql.query(
            "SELECT id, name, system_image, sort_order, origin, is_deleted, created_at, updated_at FROM place_category_definitions WHERE updated_at > \(cursorStr)"
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

        // Step 2: Push local changes
        let categoriesToPush: [PlaceCategory]
        if let c = cursor {
            categoriesToPush = localCategories.filter { $0.updatedAt > c }
        } else {
            // First sync — push everything
            categoriesToPush = localCategories
        }

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
        saveCursor(key: placeCategoryCursorKey, date: Date())

        return changed
    }

    /// Two-way sync of geofence definitions. Returns true if local geofences were modified.
    static func syncGeofences(mysql: MySQLService) async throws -> Bool {
        let cursor = loadCursor(key: geofenceCursorKey)
        var localFences = GeoFence.loadAllIncludingDeleted()
        let localById = Dictionary(localFences.map { ($0.id, $0) }, uniquingKeysWith: { _, b in b })
        var changed = false

        // Step 1: Pull remote changes
        let cursorStr = cursor.map { MySQLEscape.quote(sqlDate($0)) } ?? "'1970-01-01 00:00:00.000'"
        let rows = try await mysql.query(
            "SELECT id, name, latitude, longitude, radius, place_category_id, origin, is_deleted, created_at, updated_at FROM geofence_definitions WHERE updated_at > \(cursorStr)"
        )

        for row in rows {
            guard let idStr = row["id"], let remoteId = UUID(uuidString: idStr),
                  let name = row["name"],
                  let latStr = row["latitude"], let latitude = Double(latStr),
                  let lonStr = row["longitude"], let longitude = Double(lonStr),
                  let radiusStr = row["radius"], let radius = Double(radiusStr),
                  let originStr = row["origin"],
                  let isDeletedStr = row["is_deleted"],
                  let updatedAtStr = row["updated_at"], let remoteUpdatedAt = parseDate(updatedAtStr)
            else { continue }

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

        // Step 2: Push local changes
        let fencesToPush: [GeoFence]
        if let c = cursor {
            fencesToPush = localFences.filter { $0.updatedAt > c }
        } else {
            fencesToPush = localFences
        }

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
        saveCursor(key: geofenceCursorKey, date: Date())

        return changed
    }

    // MARK: - Cursor persistence

    private static func loadCursor(key: String) -> Date? {
        UserDefaults.standard.object(forKey: key) as? Date
    }

    private static func saveCursor(key: String, date: Date) {
        UserDefaults.standard.set(date, forKey: key)
    }
}
