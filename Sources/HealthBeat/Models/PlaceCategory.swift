import Foundation

struct PlaceCategory: Codable, Identifiable, Equatable, Hashable {
    var id: UUID = UUID()
    var name: String
    var systemImage: String

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

    static func loadAll() -> [PlaceCategory] {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let categories = try? JSONDecoder().decode([PlaceCategory].self, from: data) else {
            // Seed with defaults on first access
            saveAll(defaults)
            return defaults
        }
        return categories
    }

    static func saveAll(_ categories: [PlaceCategory]) {
        if let data = try? JSONEncoder().encode(categories) {
            UserDefaults.standard.set(data, forKey: userDefaultsKey)
        }
        Task { @MainActor in iCloudSyncService.shared.pushPlaceCategories(categories) }
    }
}
