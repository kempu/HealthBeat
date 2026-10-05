import Foundation
import UIKit

struct iCloudDevice: Codable, Identifiable {
    let id: String       // stable UUID
    let name: String     // UIDevice.current.name
    let model: String    // "iPhone" / "iPad"
    var lastSeen: Date
}

@MainActor
final class iCloudSyncService: ObservableObject {

    static let shared = iCloudSyncService()

    // MARK: - Published state

    @Published private(set) var iCloudSyncEnabled: Bool = true
    @Published private(set) var activeAutoSyncDeviceID: String?
    @Published private(set) var registeredDevices: [iCloudDevice] = []

    // MARK: - Device identity

    let currentDeviceID: String

    // MARK: - KV store keys

    private enum KVKey {
        static let enabled        = "icloud_enabled"
        static let activeDevice   = "icloud_active_device_id"
        static let devices        = "icloud_devices"
        static let mysqlConfig    = "mysql_config_v1"
        static let syncSnapshot   = "sync_snapshot_v1"
        static let locationConfig = "location_config_v1"
        static let geofences      = "geofences_v1"
        static let placeCategories = "place_categories_v1"
        static let backupConfig   = "backup_config_v1"
        static let eaConfig       = "ea_config_v1"
    }

    // Local UserDefaults keys (must match the keys used in each model file)
    private enum UDKey {
        static let mysqlConfig     = "mysqlConfig_v1"
        static let syncSnapshot    = "com.healthbeat.syncSnapshot"
        static let locationConfig  = "locationConfig_v1"
        static let geofences       = "geofences_v1"
        static let placeCategories = "place_categories_v1"
        static let backupConfig    = "backupConfig_v1"
        static let eaConfig        = "eaConfig_v1"
    }

    private static let deviceIDKey = "icloud_local_device_id"

    private var kv: NSUbiquitousKeyValueStore { .default }

    private init() {
        if let existing = UserDefaults.standard.string(forKey: iCloudSyncService.deviceIDKey) {
            currentDeviceID = existing
        } else {
            let newID = UUID().uuidString
            UserDefaults.standard.set(newID, forKey: iCloudSyncService.deviceIDKey)
            currentDeviceID = newID
        }
    }

    // MARK: - Computed

    var isCurrentDeviceActiveForAutoSync: Bool {
        guard iCloudSyncEnabled else { return true }
        guard let active = activeAutoSyncDeviceID else { return true }
        return active == currentDeviceID
    }

    var activeDeviceName: String? {
        guard let id = activeAutoSyncDeviceID else { return nil }
        return registeredDevices.first(where: { $0.id == id })?.name
    }

    // MARK: - Lifecycle

    func start() {
        NotificationCenter.default.addObserver(
            forName: NSUbiquitousKeyValueStore.didChangeExternallyNotification,
            object: kv,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.loadPublishedState()
                self?.registerDevice()
                self?.pullAllToUserDefaults()
                NotificationCenter.default.post(name: .iCloudSettingsDidChange, object: nil)
            }
        }

        kv.synchronize()
        loadPublishedState()
        registerDevice()

        // First device to launch claims auto-sync
        if activeAutoSyncDeviceID == nil && iCloudSyncEnabled {
            claimAutoSync()
        }

        pullAllToUserDefaults()
        NotificationCenter.default.post(name: .iCloudSettingsDidChange, object: nil)
    }

    // MARK: - Published state loading

    private func loadPublishedState() {
        // Default is enabled; only set to false if the key explicitly exists and is false
        if kv.object(forKey: KVKey.enabled) != nil {
            iCloudSyncEnabled = kv.bool(forKey: KVKey.enabled)
        }
        activeAutoSyncDeviceID = kv.string(forKey: KVKey.activeDevice)
        registeredDevices = decodeDevices()
    }

    // MARK: - Device registration

    private func registerDevice() {
        var devices = decodeDevices()
        let device = iCloudDevice(
            id: currentDeviceID,
            name: UIDevice.current.name,
            model: UIDevice.current.model,
            lastSeen: Date()
        )
        if let idx = devices.firstIndex(where: { $0.id == currentDeviceID }) {
            devices[idx] = device
        } else {
            devices.append(device)
        }
        encodeAndSetDevices(devices)
        registeredDevices = devices
        kv.synchronize()
    }

    private func decodeDevices() -> [iCloudDevice] {
        guard let data = kv.data(forKey: KVKey.devices) else { return [] }
        return (try? JSONDecoder().decode([iCloudDevice].self, from: data)) ?? []
    }

    private func encodeAndSetDevices(_ devices: [iCloudDevice]) {
        if let data = try? JSONEncoder().encode(devices) {
            kv.set(data, forKey: KVKey.devices)
        }
    }

    // MARK: - Auto-sync claiming

    func claimAutoSync() {
        activeAutoSyncDeviceID = currentDeviceID
        kv.set(currentDeviceID, forKey: KVKey.activeDevice)
        kv.synchronize()
    }

    func releaseAutoSync() {
        activeAutoSyncDeviceID = nil
        kv.removeObject(forKey: KVKey.activeDevice)
        kv.synchronize()
    }

    // MARK: - iCloud sync toggle

    func setEnabled(_ enabled: Bool) {
        iCloudSyncEnabled = enabled
        kv.set(enabled, forKey: KVKey.enabled)
        kv.synchronize()
    }

    // MARK: - Push methods (called from model save() sites)

    func pushMySQLConfig(_ config: MySQLConfig) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(config) {
            kv.set(data, forKey: KVKey.mysqlConfig)
            kv.synchronize()
        }
    }

    func pushSyncSnapshot(_ snapshot: PersistedSnapshot) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(snapshot) {
            kv.set(data, forKey: KVKey.syncSnapshot)
            kv.synchronize()
        }
    }

    func pushLocationConfig(_ config: LocationConfig) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(config) {
            kv.set(data, forKey: KVKey.locationConfig)
            kv.synchronize()
        }
    }

    func pushGeofences(_ fences: [GeoFence]) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(fences) {
            kv.set(data, forKey: KVKey.geofences)
            kv.synchronize()
        }
    }

    func pushPlaceCategories(_ categories: [PlaceCategory]) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(categories) {
            kv.set(data, forKey: KVKey.placeCategories)
            kv.synchronize()
        }
    }

    func pushBackupConfig(_ config: BackupConfig) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(config) {
            kv.set(data, forKey: KVKey.backupConfig)
            kv.synchronize()
        }
    }

    func pushEAConfig(_ config: EAConfig) {
        guard iCloudSyncEnabled else { return }
        if let data = try? JSONEncoder().encode(config) {
            kv.set(data, forKey: KVKey.eaConfig)
            kv.synchronize()
        }
    }

    // MARK: - Pull snapshot with merge (called from SyncState.restore())

    func pullSyncSnapshot() {
        guard let remoteData = kv.data(forKey: KVKey.syncSnapshot),
              let remote = try? JSONDecoder().decode(PersistedSnapshot.self, from: remoteData)
        else { return }

        if let localData = UserDefaults.standard.data(forKey: UDKey.syncSnapshot),
           let local = try? JSONDecoder().decode(PersistedSnapshot.self, from: localData) {
            let merged = mergeSnapshots(local: local, remote: remote)
            if let data = try? JSONEncoder().encode(merged) {
                UserDefaults.standard.set(data, forKey: UDKey.syncSnapshot)
            }
        } else {
            UserDefaults.standard.set(remoteData, forKey: UDKey.syncSnapshot)
        }
    }

    private func mergeSnapshots(local: PersistedSnapshot, remote: PersistedSnapshot) -> PersistedSnapshot {
        let mergedLastSync: Date? = maxDate(local.lastSyncDate, remote.lastSyncDate)
        let mergedTotal: Int? = maxOptional(local.totalRecords, remote.totalRecords)

        // Per-destination full-sync baselines, merged last-writer-wins by each
        // destination's change-timestamp so a reset (done=false, newer) overrides
        // an older completion. A legacy snapshot (single `hasCompletedFullSync`,
        // no per-destination fields) is first normalized to a MySQL-only baseline
        // — the dump-based EA full sync never shipped, so any prior completion
        // maps to MySQL and EA starts un-baselined.
        let lb = Self.normalizedBaselines(local), rb = Self.normalizedBaselines(remote)
        let (mDone, mAt) = Self.lww(lb.mDone, lb.mAt, rb.mDone, rb.mAt)
        let (eDone, eAt) = Self.lww(lb.eDone, lb.eAt, rb.eDone, rb.eAt)

        var categoryMap: [String: PersistedCategory] = [:]
        for cat in local.categories { categoryMap[cat.id] = cat }
        for remoteCat in remote.categories {
            if let localCat = categoryMap[remoteCat.id] {
                categoryMap[remoteCat.id] = PersistedCategory(
                    id: remoteCat.id,
                    recordCount: max(localCat.recordCount, remoteCat.recordCount),
                    lastSyncDate: maxDate(localCat.lastSyncDate, remoteCat.lastSyncDate),
                    completed: localCat.completed || remoteCat.completed
                )
            } else {
                categoryMap[remoteCat.id] = remoteCat
            }
        }

        // Prefer local cursor state — incremental progress is device-specific
        // and should not be discarded when iCloud delivers remote changes from another session.
        // Fall back to remote if local has none (e.g. fresh install restoring from iCloud).
        let mergedIncrementalCursors = local.incrementalCursors ?? remote.incrementalCursors

        return PersistedSnapshot(
            lastSyncDate: mergedLastSync,
            categories: Array(categoryMap.values),
            totalRecords: mergedTotal,
            incrementalCursors: mergedIncrementalCursors,
            mysqlBaselineDone: mDone,
            mysqlBaselineAt: mAt,
            eaBaselineDone: eDone,
            eaBaselineAt: eAt,
            hasCompletedFullSync: nil,   // legacy — no longer written
            fullSyncStateAt: nil
        )
    }

    /// Per-destination baselines for a snapshot, normalizing a legacy single-flag
    /// snapshot to a MySQL-only baseline (see `mergeSnapshots`).
    private static func normalizedBaselines(_ s: PersistedSnapshot)
        -> (mDone: Bool, mAt: Date?, eDone: Bool, eAt: Date?) {
        if s.mysqlBaselineDone != nil || s.eaBaselineDone != nil {
            return (s.mysqlBaselineDone ?? false, s.mysqlBaselineAt,
                    s.eaBaselineDone ?? false, s.eaBaselineAt)
        }
        let hadMySQL = (s.hasCompletedFullSync ?? false)
            || s.lastSyncDate != nil || (s.incrementalCursors?.isEmpty == false)
        return (hadMySQL, hadMySQL ? (s.fullSyncStateAt ?? s.lastSyncDate) : nil, false, nil)
    }

    /// Last-writer-wins merge of one `(done, at)` baseline pair. With both
    /// timestamps the newer wins; with one, that side wins; with neither, OR.
    private static func lww(_ lDone: Bool, _ lAt: Date?, _ rDone: Bool, _ rAt: Date?) -> (Bool, Date?) {
        switch (lAt, rAt) {
        case let (l?, r?):   return (l >= r ? lDone : rDone, l >= r ? l : r)
        case (.some, .none): return (lDone, lAt)
        case (.none, .some): return (rDone, rAt)
        case (.none, .none): return (lDone || rDone, nil)
        }
    }

    private func maxDate(_ a: Date?, _ b: Date?) -> Date? {
        switch (a, b) {
        case (.none, let d), (let d, .none): return d
        case (.some(let l), .some(let r)): return max(l, r)
        }
    }

    private func maxOptional(_ a: Int?, _ b: Int?) -> Int? {
        switch (a, b) {
        case (.none, let n), (let n, .none): return n
        case (.some(let l), .some(let r)): return max(l, r)
        }
    }

    // MARK: - Pull all remote values into UserDefaults

    private func pullAllToUserDefaults() {
        if let data = kv.data(forKey: KVKey.mysqlConfig) {
            UserDefaults.standard.set(data, forKey: UDKey.mysqlConfig)
        }
        pullSyncSnapshot()
        if let data = kv.data(forKey: KVKey.locationConfig) {
            UserDefaults.standard.set(data, forKey: UDKey.locationConfig)
        }
        if let data = kv.data(forKey: KVKey.geofences) {
            UserDefaults.standard.set(data, forKey: UDKey.geofences)
        }
        if let data = kv.data(forKey: KVKey.placeCategories) {
            UserDefaults.standard.set(data, forKey: UDKey.placeCategories)
        }
        if let data = kv.data(forKey: KVKey.backupConfig) {
            UserDefaults.standard.set(data, forKey: UDKey.backupConfig)
        }
        if let data = kv.data(forKey: KVKey.eaConfig) {
            UserDefaults.standard.set(data, forKey: UDKey.eaConfig)
        }
    }
}

extension Notification.Name {
    static let iCloudSettingsDidChange = Notification.Name("iCloudSettingsDidChange")
    static let geofencesDidSync = Notification.Name("geofencesDidSync")
}
