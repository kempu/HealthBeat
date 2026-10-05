import Foundation

struct MySQLConfig: Codable, Equatable {
    var host: String
    var port: UInt16
    var database: String
    var username: String
    var password: String
    /// Whether the direct-MySQL destination is active. Mirrors `EAConfig.enabled`
    /// so the user can run EA-only, MySQL-only, or both. Defaults to `true` for
    /// existing installs (the key is absent from older saved configs).
    var enabled: Bool

    init(host: String, port: UInt16, database: String, username: String,
         password: String, enabled: Bool = true) {
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.enabled = enabled
    }

    static let `default` = MySQLConfig(
        host: "192.168.1.1",
        port: 3306,
        database: "healthbeat",
        username: "healthbeat",
        password: ""
    )

    /// True when the MySQL destination should receive writes.
    var isConfigured: Bool { enabled && !host.isEmpty && !database.isEmpty && !username.isEmpty }

    // Custom decode so a saved config from before `enabled` existed still loads
    // (defaulting to enabled) instead of failing and resetting host/credentials.
    enum CodingKeys: String, CodingKey {
        case host, port, database, username, password, enabled
    }
    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        host = try c.decode(String.self, forKey: .host)
        port = try c.decode(UInt16.self, forKey: .port)
        database = try c.decode(String.self, forKey: .database)
        username = try c.decode(String.self, forKey: .username)
        password = try c.decode(String.self, forKey: .password)
        enabled = try c.decodeIfPresent(Bool.self, forKey: .enabled) ?? true
    }

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
