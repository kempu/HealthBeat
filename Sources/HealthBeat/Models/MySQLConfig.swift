import Foundation

struct MySQLConfig: Codable, Equatable {
    var host: String
    var port: UInt16
    var database: String
    var username: String
    var password: String

    static let `default` = MySQLConfig(
        host: "192.168.1.1",
        port: 3306,
        database: "healthbeat",
        username: "healthbeat",
        password: ""
    )

    private static let userDefaultsKey = "mysqlConfig_v1"

    static func load() -> MySQLConfig {
        guard let data = UserDefaults.standard.data(forKey: userDefaultsKey),
              let config = try? JSONDecoder().decode(MySQLConfig.self, from: data) else {
            return .default
        }
        return config
    }

    func save() {
        if let data = try? JSONEncoder().encode(self) {
            UserDefaults.standard.set(data, forKey: MySQLConfig.userDefaultsKey)
        }
        Task { @MainActor in iCloudSyncService.shared.pushMySQLConfig(self) }
    }
}
