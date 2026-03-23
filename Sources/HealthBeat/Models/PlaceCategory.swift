import Foundation

struct PlaceCategory: Codable, Identifiable, Equatable, Hashable {
    var id: UUID = UUID()
    var name: String
    var systemImage: String

    // Sync metadata
    var origin: SyncOrigin = .app
    var isDeleted: Bool = false
    var updatedAt: Date = Date()
    var sortOrder: Int = 0

    enum SyncOrigin: String, Codable {
        case app
        case database
    }

    private static let userDefaultsKey = "place_categories_v1"

    /// Built-in defaults used when user has no custom categories yet.
    static let defaults: [PlaceCategory] = [
        PlaceCategory(name: "Home", systemImage: "house.fill"),
        PlaceCategory(name: "Office", systemImage: "building.2.fill"),
        PlaceCategory(name: "Shop", systemImage: "cart.fill"),
        PlaceCategory(name: "Gym", systemImage: "dumbbell.fill"),
        PlaceCategory(name: "School", systemImage: "graduationcap.fill"),
        PlaceCategory(name: "Restaurant", systemImage: "fork.knife"),
        PlaceCategory(name: "Hospital", systemImage: "cross.case.fill"),
        PlaceCategory(name: "Park", systemImage: "leaf.fill"),
        PlaceCategory(name: "Airport", systemImage: "airplane"),
        PlaceCategory(name: "Other", systemImage: "mappin.circle.fill"),
    ]

    // MARK: - Backward-compatible decoding

    enum CodingKeys: String, CodingKey {
        case id, name, systemImage, origin, isDeleted, updatedAt, sortOrder
    }

    init(name: String, systemImage: String) {
        self.name = name
        self.systemImage = systemImage
    }

    init(id: UUID = UUID(), name: String, systemImage: String, origin: SyncOrigin = .app,
         isDeleted: Bool = false, updatedAt: Date = Date(), sortOrder: Int = 0) {
        self.id = id
        self.name = name
        self.systemImage = systemImage
        self.origin = origin
        self.isDeleted = isDeleted
        self.updatedAt = updatedAt
        self.sortOrder = sortOrder
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decodeIfPresent(UUID.self, forKey: .id) ?? UUID()
        name = try c.decode(String.self, forKey: .name)
        systemImage = try c.decode(String.self, forKey: .systemImage)
        origin = try c.decodeIfPresent(SyncOrigin.self, forKey: .origin) ?? .app
        isDeleted = try c.decodeIfPresent(Bool.self, forKey: .isDeleted) ?? false
        updatedAt = try c.decodeIfPresent(Date.self, forKey: .updatedAt) ?? Date()
        sortOrder = try c.decodeIfPresent(Int.self, forKey: .sortOrder) ?? 0
    }

    // MARK: - Persistence

    /// Returns only non-deleted categories, sorted by sortOrder.
    static func loadAll() -> [PlaceCategory] {
        let all = loadAllIncludingDeleted()
        guard !all.isEmpty else {
            saveAll(defaults)
            return defaults
        }
        return all.filter { !$0.isDeleted }.sorted { $0.sortOrder < $1.sortOrder }
    }

    /// Returns all categories including soft-deleted ones.
    static func loadAllIncludingDeleted() -> [PlaceCategory] {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let categories = try? JSONDecoder().decode([PlaceCategory].self, from: data) else {
            return []
        }
        return categories
    }

    static func saveAll(_ categories: [PlaceCategory]) {
        // Stamp sortOrder from array position for non-deleted items
        var stamped = categories
        var order = 0
        for i in stamped.indices where !stamped[i].isDeleted {
            stamped[i].sortOrder = order
            order += 1
        }
        if let data = try? JSONEncoder().encode(stamped) {
            UserDefaults.standard.set(data, forKey: userDefaultsKey)
        }
        Task { @MainActor in iCloudSyncService.shared.pushPlaceCategories(stamped) }
    }
}
