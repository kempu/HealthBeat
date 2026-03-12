import CoreLocation
import Foundation
import UIKit

private let sqlDateFormatter: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    f.timeZone = TimeZone(identifier: "UTC")
    f.locale = Locale(identifier: "en_US_POSIX")
    return f
}()

private func sqlDate(_ date: Date) -> String {
    sqlDateFormatter.string(from: date)
}

@MainActor
final class LocationService: NSObject, CLLocationManagerDelegate {

    static let shared = LocationService()

    private let manager = CLLocationManager()
    private var pendingLocations: [CLLocation] = []

    @Published var authorizationStatus: CLAuthorizationStatus = .notDetermined

    var lastKnownLocation: CLLocationCoordinate2D? { manager.location?.coordinate }

    private override init() {
        super.init()
        manager.delegate = self
        manager.desiredAccuracy = kCLLocationAccuracyBest
        manager.distanceFilter = 10
        manager.allowsBackgroundLocationUpdates = true
        manager.pausesLocationUpdatesAutomatically = false
        authorizationStatus = manager.authorizationStatus
    }

    func reloadAndStart() {
        let config = LocationConfig.load()
        if config.trackingEnabled {
            requestPermissionIfNeeded()
            manager.startUpdatingLocation()
        }
        let fences = GeoFence.loadAll()
        updateGeofences(fences)
        // Also request permission for geofencing even if tracking is off
        if !fences.isEmpty {
            requestPermissionIfNeeded()
        }
    }

    func applyConfig(_ config: LocationConfig) {
        if config.trackingEnabled {
            requestPermissionIfNeeded()
            manager.startUpdatingLocation()
        } else {
            manager.stopUpdatingLocation()
        }
        // Always ensure we have Always permission if geofences exist
        let fences = GeoFence.loadAll()
        if !fences.isEmpty {
            requestPermissionIfNeeded()
        }
    }

    func updateGeofences(_ fences: [GeoFence]) {
        for region in manager.monitoredRegions {
            manager.stopMonitoring(for: region)
        }
        for fence in fences {
            let region = CLCircularRegion(
                center: CLLocationCoordinate2D(latitude: fence.latitude, longitude: fence.longitude),
                radius: fence.radius,
                identifier: fence.id.uuidString
            )
            region.notifyOnEntry = true
            region.notifyOnExit = true
            manager.startMonitoring(for: region)
        }
    }

    func flushPendingLocations() async {
        guard UserDefaults.standard.bool(forKey: "backgroundSyncEnabled") else { return }
        guard iCloudSyncService.shared.isCurrentDeviceActiveForAutoSync else { return }
        guard !pendingLocations.isEmpty else { return }
        let toFlush = pendingLocations
        pendingLocations = []

        let config = MySQLConfig.load()
        let mysql = MySQLService()
        do {
            try await mysql.connect(config: config)
            defer { Task { await mysql.disconnect() } }

            let batchSize = 500
            var offset = 0
            while offset < toFlush.count {
                let batch = Array(toFlush[offset..<min(offset + batchSize, toFlush.count)])
                let values = batch.map { loc -> String in
                    let lat  = loc.coordinate.latitude
                    let lon  = loc.coordinate.longitude
                    let alt  = loc.altitude
                    let hacc = loc.horizontalAccuracy
                    let vacc = loc.verticalAccuracy
                    let spd  = loc.speed
                    let crs  = loc.course
                    let ts   = MySQLEscape.quote(sqlDate(loc.timestamp))
                    return "(\(lat),\(lon),\(alt),\(hacc),\(vacc),\(spd),\(crs),\(ts))"
                }.joined(separator: ",")
                let sql = """
                INSERT INTO location_tracks \
                (latitude,longitude,altitude,horizontal_accuracy,vertical_accuracy,speed,course,timestamp) \
                VALUES \(values)
                """
                try await mysql.execute(sql)
                offset += batchSize
            }
        } catch {
            // Re-queue on failure so data is not lost
            pendingLocations = toFlush + pendingLocations
        }
    }

    func logGeofenceEvent(placeName: String, placeType: String?, eventType: String, location: CLLocation?) async {
        guard UserDefaults.standard.bool(forKey: "backgroundSyncEnabled") else { return }
        guard iCloudSyncService.shared.isCurrentDeviceActiveForAutoSync else { return }
        let bgTask = UIApplication.shared.beginBackgroundTask(withName: "geofence-log") {
        }
        defer { UIApplication.shared.endBackgroundTask(bgTask) }

        let config = MySQLConfig.load()
        let mysql = MySQLService()
        do {
            try await mysql.connect(config: config)
            defer { Task { await mysql.disconnect() } }

            let name    = MySQLEscape.quote(placeName)
            let pType   = placeType.map { MySQLEscape.quote($0) } ?? "NULL"
            let evType  = MySQLEscape.quote(eventType)
            let ts      = MySQLEscape.quote(sqlDate(location?.timestamp ?? Date()))
            let lat     = location.map { "\($0.coordinate.latitude)" } ?? "NULL"
            let lon     = location.map { "\($0.coordinate.longitude)" } ?? "NULL"
            let sql  = """
            INSERT INTO location_geofence_events (place_name,place_type,event_type,latitude,longitude,timestamp) \
            VALUES (\(name),\(pType),\(evType),\(lat),\(lon),\(ts))
            """
            try await mysql.execute(sql)
        } catch {
            // Silent — background context, nothing to surface to user
        }
    }

    /// Public method for UI to explicitly request Always authorization.
    func requestAlwaysFromUI() {
        requestPermissionIfNeeded()
    }

    func handleGeofenceLaunch() {
        // Ensure delegate is live so CLLocationManager delivers cold-launch region events
        manager.delegate = self
    }

    // MARK: - Private

    private func requestPermissionIfNeeded() {
        switch manager.authorizationStatus {
        case .notDetermined:
            // First request: ask for Always directly.
            // iOS will first grant "When In Use" provisionally, then show
            // the "Always Allow" prompt later. By calling requestAlwaysAuthorization()
            // upfront, iOS handles the two-step escalation automatically.
            manager.requestAlwaysAuthorization()
        case .authorizedWhenInUse:
            // User previously granted "When In Use" — escalate to "Always".
            // iOS will show the "upgrade to Always" prompt once.
            manager.requestAlwaysAuthorization()
        case .denied, .restricted:
            // Cannot request programmatically — user must change in Settings.
            // The UI shows a warning with an "Open Settings" button.
            break
        case .authorizedAlways:
            break
        @unknown default:
            break
        }
    }

    // MARK: - CLLocationManagerDelegate

    nonisolated func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        Task { @MainActor in
            self.pendingLocations.append(contentsOf: locations)
            if self.pendingLocations.count >= 100 {
                await self.flushPendingLocations()
            }
        }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didEnterRegion region: CLRegion) {
        guard let circularRegion = region as? CLCircularRegion else { return }
        let fences = GeoFence.loadAll()
        let fence = fences.first(where: { $0.id.uuidString == circularRegion.identifier })
        let name = fence?.name ?? circularRegion.identifier
        let placeType = fence?.placeCategoryName
        Task { @MainActor in
            await self.logGeofenceEvent(placeName: name, placeType: placeType, eventType: "arrive", location: nil)
        }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didExitRegion region: CLRegion) {
        guard let circularRegion = region as? CLCircularRegion else { return }
        let fences = GeoFence.loadAll()
        let fence = fences.first(where: { $0.id.uuidString == circularRegion.identifier })
        let name = fence?.name ?? circularRegion.identifier
        let placeType = fence?.placeCategoryName
        Task { @MainActor in
            await self.logGeofenceEvent(placeName: name, placeType: placeType, eventType: "depart", location: nil)
        }
    }

    nonisolated func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {
        let status = manager.authorizationStatus
        Task { @MainActor in
            self.authorizationStatus = status
        }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        // Silent — don't crash on location errors
    }
}
