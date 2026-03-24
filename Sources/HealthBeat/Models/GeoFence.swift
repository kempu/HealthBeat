import Foundation

struct GeoFence: Codable, Identifiable, Equatable {
    var id: UUID = UUID()
    var name: String
    var latitude: Double
    var longitude: Double
    var radius: Double  // meters
    var placeCategoryId: UUID?

    // Sync metadata
    var origin: SyncOrigin = .app
    var isDeleted: Bool = false
    var updatedAt: Date = Date()

    enum SyncOrigin: String, Codable {
        case app
        case database
    }

    private static let userDefaultsKey = "geofences_v1"

    // MARK: - Backward-compatible decoding

    enum CodingKeys: String, CodingKey {
        case id, name, latitude, longitude, radius, placeCategoryId
        case origin, isDeleted, updatedAt
    }

    init(name: String, latitude: Double, longitude: Double, radius: Double,
         placeCategoryId: UUID? = nil, origin: SyncOrigin = .app,
         isDeleted: Bool = false, updatedAt: Date = Date()) {
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.radius = radius
        self.placeCategoryId = placeCategoryId
        self.origin = origin
        self.isDeleted = isDeleted
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decodeIfPresent(UUID.self, forKey: .id) ?? UUID()
        name = try c.decode(String.self, forKey: .name)
        latitude = try c.decode(Double.self, forKey: .latitude)
        longitude = try c.decode(Double.self, forKey: .longitude)
        radius = try c.decode(Double.self, forKey: .radius)
        placeCategoryId = try c.decodeIfPresent(UUID.self, forKey: .placeCategoryId)
        origin = try c.decodeIfPresent(SyncOrigin.self, forKey: .origin) ?? .app
        isDeleted = try c.decodeIfPresent(Bool.self, forKey: .isDeleted) ?? false
        updatedAt = try c.decodeIfPresent(Date.self, forKey: .updatedAt) ?? Date()
    }

    /// Resolved category name, or "Other" if none set.
    var placeCategoryName: String {
        guard let catId = placeCategoryId else { return "Other" }
        return PlaceCategory.loadAll().first(where: { $0.id == catId })?.name ?? "Other"
    }

    /// Resolved category icon, or default pin if none set.
    var placeCategoryIcon: String {
        guard let catId = placeCategoryId else { return "mappin.circle.fill" }
        return PlaceCategory.loadAll().first(where: { $0.id == catId })?.systemImage ?? "mappin.circle.fill"
    }

    // MARK: - Persistence

    /// Returns only non-deleted geofences.
    static func loadAll() -> [GeoFence] {
        loadAllIncludingDeleted().filter { !$0.isDeleted }
    }

    /// Returns all geofences including soft-deleted ones.
    static func loadAllIncludingDeleted() -> [GeoFence] {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let fences = try? JSONDecoder().decode([GeoFence].self, from: data) else {
            return []
        }
        return fences
    }

    static func saveAll(_ fences: [GeoFence]) {
        do {
            let data = try JSONEncoder().encode(fences)
            UserDefaults.standard.set(data, forKey: userDefaultsKey)
        } catch {
            print("[GeoFence] saveAll encode failed: \(error)")
        }
        Task { @MainActor in iCloudSyncService.shared.pushGeofences(fences) }
    }
}
