import Foundation
import HealthKit
import SwiftUI
import UIKit

extension Notification.Name {
    static let healthBeatDatabaseDidReset = Notification.Name("com.healthbeat.databaseDidReset")
}

enum ConnectionTestState: Equatable {
    case idle
    case testing
    case success(String)
    case failure(String)
}

@MainActor
final class SettingsViewModel: ObservableObject {

    @Published var config: MySQLConfig = .load()
    @Published var connectionTestState: ConnectionTestState = .idle
    @Published var schemaInitState: ConnectionTestState = .idle
    @Published var resetDatabaseState: ConnectionTestState = .idle
    @Published var permissionsRequested: Bool = UserDefaults.standard.bool(forKey: "hk_permissions_requested")
    @Published var deniedTypes: [HKObjectType] = []
    @Published var grantedTypes: [HKObjectType] = []
    @Published var errorMessage: String?

    private let healthKit = HealthKitService.shared

    init() {
        NotificationCenter.default.addObserver(
            forName: .iCloudSettingsDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.config = .load()
            }
        }
    }

    func saveConfig() {
        config.save()
    }

    // MARK: - Connection test

    func testConnection() {
        guard connectionTestState != .testing else { return }
        connectionTestState = .testing
        let cfg = config
        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: cfg)
                let rows = try await mysql.query("SELECT VERSION() as v")
                await mysql.disconnect()
                let version = rows.first?["v"] ?? "unknown"
                connectionTestState = .success("Connected! MySQL \(version)")
            } catch {
                connectionTestState = .failure(error.localizedDescription)
            }
        }
    }

    // MARK: - Schema init

    func initializeSchema() {
        guard schemaInitState != .testing else { return }
        schemaInitState = .testing
        let cfg = config
        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: cfg)
                let (ok, errMsg) = await SchemaService.initializeSchema(mysql: mysql)
                await mysql.disconnect()
                if ok {
                    schemaInitState = .success("All tables created successfully.")
                } else {
                    schemaInitState = .failure(errMsg ?? "Unknown error")
                }
            } catch {
                schemaInitState = .failure(error.localizedDescription)
            }
        }
    }

    // MARK: - Database reset

    func resetDatabase() {
        guard resetDatabaseState != .testing else { return }
        resetDatabaseState = .testing
        let cfg = config
        Task {
            let mysql = MySQLService()
            do {
                try await mysql.connect(config: cfg)
                try await SchemaService.deleteAllHealthData(mysql: mysql)
                await mysql.disconnect()
                resetDatabaseState = .success("All health records deleted.")
                NotificationCenter.default.post(name: .healthBeatDatabaseDidReset, object: nil)
            } catch {
                await mysql.disconnect()
                resetDatabaseState = .failure(error.localizedDescription)
            }
        }
    }

    // MARK: - HealthKit permissions

    func refreshPermissionsState() {
        let (granted, denied) = healthKit.checkAllPermissionStatuses()
        self.grantedTypes = granted
        self.deniedTypes = denied
        // Derive from actual HealthKit state: if any type moved past .notDetermined,
        // the dialog was shown regardless of what UserDefaults says.
        if !granted.isEmpty {
            permissionsRequested = true
            UserDefaults.standard.set(true, forKey: "hk_permissions_requested")
        } else {
            permissionsRequested = UserDefaults.standard.bool(forKey: "hk_permissions_requested")
        }
    }

    var hasDeniedPermissions: Bool {
        !deniedTypes.isEmpty
    }

    func requestAllPermissions() {
        Task {
            do {
                try await healthKit.requestAllPermissions()
            } catch {
                errorMessage = "HealthKit authorization failed: \(error.localizedDescription)"
            }
            UserDefaults.standard.set(true, forKey: "hk_permissions_requested")
            permissionsRequested = true
            refreshPermissionsState()
        }
    }

    func requestMissingPermissions() {
        guard !deniedTypes.isEmpty else { return }
        let types = Set(deniedTypes)
        Task {
            do {
                try await healthKit.requestPermissions(for: types)
            } catch {
                errorMessage = "HealthKit authorization failed: \(error.localizedDescription)"
            }
            refreshPermissionsState()
        }
    }

    // MARK: - Per-object authorization (medications & vision prescriptions)

    func requestVisionPrescriptionAccess() {
        Task {
            await healthKit.requestVisionPrescriptionAuthorization()
        }
    }

    func requestMedicationAccess() {
        Task {
            if #available(iOS 26, *) {
                await healthKit.requestMedicationAuthorization()
            }
        }
    }
}
