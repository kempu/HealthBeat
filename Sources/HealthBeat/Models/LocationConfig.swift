import Foundation

struct LocationConfig: Codable, Equatable {
    var trackingEnabled: Bool = false

    private static let userDefaultsKey = "locationConfig_v1"

    static func load() -> LocationConfig {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let config = try? JSONDecoder().decode(LocationConfig.self, from: data) else {
            return LocationConfig()
        }
        return config
    }

    func save() {
        if let data = try? JSONEncoder().encode(self) {
            UserDefaults.standard.set(data, forKey: LocationConfig.userDefaultsKey)
        }
        Task { @MainActor in iCloudSyncService.shared.pushLocationConfig(self) }
    }
}
