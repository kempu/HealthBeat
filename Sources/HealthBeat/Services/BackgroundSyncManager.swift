import Foundation
import HealthKit
import UIKit
import UserNotifications

/// Manages HKObserverQuery-based background delivery for continuous HealthKit → MySQL sync.
///
/// Other health apps use this pattern: register observer queries for each data type at launch,
/// enable background delivery, and HealthKit wakes the app when new data is written. Unlike
/// BGProcessingTask (which runs when the device is idle/locked), observer callbacks fire
/// close to when data is recorded — the device is typically unlocked, so HealthKit data is accessible.
@MainActor
final class BackgroundSyncManager {

    static let shared = BackgroundSyncManager()

    private let healthStore = HealthKitService.shared.store
    private var observerQueries: [HKObserverQuery] = []
    private var pendingTypes: Set<String> = []
    private var pendingCompletionHandlers: [() -> Void] = []
    private var debounceTask: Task<Void, Never>?
    private var isSyncing = false
    private var lastForegroundSyncDate: Date = .distantPast
    private var lastPeriodicSyncDate: Date = .distantPast

    private init() {}

    // MARK: - Type → Category Mapping

    /// Maps HKObjectType identifiers to sync category IDs used by SyncService.
    private static let typeToCategoryMap: [String: String] = {
        var map: [String: String] = [:]
        // Quantity types → qty_<category>
        for desc in HealthDataTypes.allQuantityTypes {
            map[desc.id] = "qty_\(desc.category.rawValue)"
        }
        // Category types → cat_category
        for desc in HealthDataTypes.allCategoryTypes {
            map[desc.id] = "cat_category"
        }
        // Special types
        map[HKObjectType.workoutType().identifier] = "cat_workouts"
        map[HKObjectType.electrocardiogramType().identifier] = "cat_ecg"
        map[HKObjectType.audiogramSampleType().identifier] = "cat_audiogram"
        map[HKSeriesType.workoutRoute().identifier] = "cat_workout_routes"
        map[HKObjectType.activitySummaryType().identifier] = "cat_activity_summaries"
        if #available(iOS 18, *) {
            map[HKObjectType.stateOfMindType().identifier] = "cat_state_of_mind"
        }
        return map
    }()

    // MARK: - Public API

    /// Call once from AppDelegate.didFinishLaunchingWithOptions to start monitoring HealthKit.
    func startObserving() {
        setupObserverQueries()
        enableBackgroundDelivery()
    }

    /// Re-enables background delivery for all types. Call after granting new HealthKit permissions.
    func reEnableBackgroundDelivery() {
        enableBackgroundDelivery()
    }

    /// Triggers a full incremental sync with a 60-second cooldown. Used for foreground entry
    /// and device unlock events where we want to catch all missed changes comprehensively.
    func triggerForegroundSync() {
        guard Date().timeIntervalSince(lastForegroundSyncDate) > 60 else {
            print("[BackgroundSyncManager] triggerForegroundSync skipped — cooldown active")
            return
        }
        guard UserDefaults.standard.bool(forKey: "backgroundSyncEnabled") else {
            print("[BackgroundSyncManager] triggerForegroundSync skipped — backgroundSyncEnabled is false")
            return
        }
        guard !SyncService.isSyncRunning else {
            print("[BackgroundSyncManager] triggerForegroundSync skipped — sync already running")
            return
        }
        guard iCloudSyncService.shared.isCurrentDeviceActiveForAutoSync else {
            print("[BackgroundSyncManager] triggerForegroundSync skipped — not the active device for auto-sync")
            return
        }
        lastForegroundSyncDate = Date()
        print("[BackgroundSyncManager] triggerForegroundSync starting incremental sync")

        let config = MySQLConfig.load()
        let state = SyncState()
        let service = SyncService(syncState: state)
        service.isBackgroundSync = true
        service.suppressLiveActivity = true
        Task { await service.runIncrementalSync(config: config) }
    }

    /// Throttled sync trigger called from location updates. Since location tracking provides
    /// continuous background execution, this reliably catches any HealthKit changes that
    /// observer queries may have missed.
    func triggerPeriodicSync() {
        guard Date().timeIntervalSince(lastPeriodicSyncDate) > 900 else { return } // 15 minutes
        lastPeriodicSyncDate = Date()
        print("[BackgroundSyncManager] Periodic sync triggered from location update")
        triggerForegroundSync()
    }

    // MARK: - Observer Queries

    private func setupObserverQueries() {
        let readTypes = HealthDataTypes.allReadTypes

        for type in readTypes {
            guard let sampleType = type as? HKSampleType else { continue }

            let query = HKObserverQuery(sampleType: sampleType, predicate: nil) {
                [weak self] _, completionHandler, error in
                Task { @MainActor in
                    guard let self else {
                        completionHandler()
                        return
                    }
                    self.handleObserverUpdate(sampleType: sampleType, error: error, completionHandler: completionHandler)
                }
            }

            healthStore.execute(query)
            observerQueries.append(query)
        }
    }

    private func enableBackgroundDelivery() {
        let readTypes = HealthDataTypes.allReadTypes
        var count = 0

        for type in readTypes {
            guard let sampleType = type as? HKSampleType else { continue }
            count += 1

            healthStore.enableBackgroundDelivery(for: sampleType, frequency: .immediate) { success, error in
                if let error = error {
                    print("[BackgroundSyncManager] enableBackgroundDelivery failed for \(sampleType.identifier): \(error.localizedDescription)")
                }
            }
        }
        print("[BackgroundSyncManager] enableBackgroundDelivery requested for \(count) types")
    }

    // MARK: - Observer Callback Handling

    private func handleObserverUpdate(sampleType: HKSampleType, error: Error?, completionHandler: @escaping () -> Void) {
        if let error = error {
            print("[BackgroundSyncManager] Observer error for \(sampleType.identifier): \(error.localizedDescription)")
            postFailureNotification("HealthKit observer error: \(error.localizedDescription)")
            completionHandler()
            return
        }

        print("[BackgroundSyncManager] Observer fired for \(sampleType.identifier)")
        pendingTypes.insert(sampleType.identifier)
        pendingCompletionHandlers.append(completionHandler)
        debounceAndSync()
    }

    /// Debounce rapid-fire observer callbacks. Multiple types can change at once
    /// (e.g. workout saves distance, energy, heart rate simultaneously). Wait 2s
    /// for all updates to arrive, then trigger a targeted sync for only the changed categories.
    private func debounceAndSync() {
        debounceTask?.cancel()
        debounceTask = Task {
            try? await Task.sleep(nanoseconds: 2_000_000_000) // 2 seconds
            guard !Task.isCancelled else { return }

            let types = pendingTypes
            let completionHandlers = pendingCompletionHandlers
            pendingTypes.removeAll()
            pendingCompletionHandlers.removeAll()

            guard !types.isEmpty else {
                completionHandlers.forEach { $0() }
                return
            }

            print("[BackgroundSyncManager] Debounce fired — \(types.count) types pending: \(types.sorted().joined(separator: ", "))")

            // Extra background execution time as safety net
            var bgTaskID: UIBackgroundTaskIdentifier = .invalid
            bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "hk-observer-sync") {
                UIApplication.shared.endBackgroundTask(bgTaskID)
                bgTaskID = .invalid
            }

            await triggerTargetedSync(changedTypes: types)

            // Signal HealthKit that we're done processing
            completionHandlers.forEach { $0() }
            print("[BackgroundSyncManager] Called \(completionHandlers.count) completion handlers")

            if bgTaskID != .invalid {
                UIApplication.shared.endBackgroundTask(bgTaskID)
            }
        }
    }

    // MARK: - Trigger Sync

    private func triggerTargetedSync(changedTypes: Set<String>) async {
        guard UserDefaults.standard.bool(forKey: "backgroundSyncEnabled") else {
            print("[BackgroundSyncManager] triggerTargetedSync skipped — backgroundSyncEnabled is false")
            return
        }
        guard !isSyncing, !SyncService.isSyncRunning else {
            print("[BackgroundSyncManager] triggerTargetedSync skipped — sync already running")
            return
        }
        guard iCloudSyncService.shared.isCurrentDeviceActiveForAutoSync else {
            print("[BackgroundSyncManager] triggerTargetedSync skipped — not the active device for auto-sync")
            return
        }
        isSyncing = true
        defer { isSyncing = false }

        // Resolve type identifiers to category IDs
        var categoryIDs: Set<String> = []
        for typeID in changedTypes {
            if let catID = Self.typeToCategoryMap[typeID] {
                categoryIDs.insert(catID)
            }
        }
        guard !categoryIDs.isEmpty else {
            print("[BackgroundSyncManager] triggerTargetedSync skipped — no matching categories for changed types")
            return
        }
        print("[BackgroundSyncManager] triggerTargetedSync starting for categories: \(categoryIDs.sorted().joined(separator: ", "))")

        let config = MySQLConfig.load()

        // Request extra background execution time from iOS.
        var bgTaskID: UIBackgroundTaskIdentifier = .invalid
        var syncTask: Task<Void, Never>?
        bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "observer-sync") {
            syncTask?.cancel()
            UIApplication.shared.endBackgroundTask(bgTaskID)
            bgTaskID = .invalid
        }
        defer {
            if bgTaskID != .invalid {
                UIApplication.shared.endBackgroundTask(bgTaskID)
            }
        }

        let state = SyncState()
        let service = SyncService(syncState: state)
        service.isBackgroundSync = true
        service.suppressLiveActivity = true
        let task = Task { await service.runTargetedSync(categoryIDs: categoryIDs, config: config) }
        syncTask = task
        await task.value

        // Post notification on failure. Cancellation (expiry) sets no errorMessage,
        // so this only fires on genuine sync errors.
        if let error = state.errorMessage {
            postFailureNotification(error)
        }
    }

    // MARK: - Failure Notifications

    func postFailureNotification(_ message: String) {
        let content = UNMutableNotificationContent()
        content.title = "HealthBeat Sync Failed"
        content.body = message
        content.sound = .default

        let request = UNNotificationRequest(
            identifier: "sync-failure",
            content: content,
            trigger: nil
        )

        UNUserNotificationCenter.current().add(request) { error in
            if let error = error {
                print("[BackgroundSyncManager] Failed to post notification: \(error.localizedDescription)")
            }
        }
    }

    /// Request notification permission. Call once at app launch.
    static func requestNotificationPermission() {
        UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge]) { _, error in
            if let error = error {
                print("[BackgroundSyncManager] Notification permission error: \(error.localizedDescription)")
            }
        }
    }
}
