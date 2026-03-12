import Foundation

struct GeoFence: Codable, Identifiable, Equatable {
    var id: UUID = UUID()
    var name: String
    var latitude: Double
    var longitude: Double
    var radius: Double  // meters
    var placeCategoryId: UUID?

    private static let userDefaultsKey = "geofences_v1"

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

    static func loadAll() -> [GeoFence] {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let fences = try? JSONDecoder().decode([GeoFence].self, from: data) else {
            return []
        }
        return fences
    }

    static func saveAll(_ fences: [GeoFence]) {
        if let data = try? JSONEncoder().encode(fences) {
            UserDefaults.standard.set(data, forKey: userDefaultsKey)
        }
        Task { @MainActor in iCloudSyncService.shared.pushGeofences(fences) }
    }
}
