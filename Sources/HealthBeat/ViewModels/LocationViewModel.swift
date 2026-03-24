import Combine
import CoreLocation
import Foundation

@MainActor
final class LocationViewModel: ObservableObject {
    @Published var config: LocationConfig = .load()
    @Published var geofences: [GeoFence] = GeoFence.loadAll()
    @Published var authorizationStatus: CLAuthorizationStatus = LocationService.shared.authorizationStatus

    private var cancellables = Set<AnyCancellable>()

    init() {
        LocationService.shared.$authorizationStatus
            .receive(on: DispatchQueue.main)
            .sink { [weak self] status in
                self?.authorizationStatus = status
            }
            .store(in: &cancellables)

        NotificationCenter.default.publisher(for: .iCloudSettingsDidChange)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.config = .load()
                self?.geofences = GeoFence.loadAll()
            }
            .store(in: &cancellables)

        NotificationCenter.default.publisher(for: .geofencesDidSync)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.geofences = GeoFence.loadAll()
            }
            .store(in: &cancellables)
    }

    func toggleTracking() {
        config.trackingEnabled.toggle()
        config.save()
        LocationService.shared.applyConfig(config)
    }

    func addGeofence(_ fence: GeoFence) {
        var allFences = GeoFence.loadAllIncludingDeleted()
        allFences.append(fence)
        GeoFence.saveAll(allFences)
        geofences = GeoFence.loadAll()
        LocationService.shared.updateGeofences(geofences)
    }

    func deleteGeofence(at offsets: IndexSet) {
        // Soft-delete: mark as deleted and stamp updatedAt
        var allFences = GeoFence.loadAllIncludingDeleted()
        let visibleFences = geofences
        for offset in offsets {
            let fenceToDelete = visibleFences[offset]
            if let idx = allFences.firstIndex(where: { $0.id == fenceToDelete.id }) {
                allFences[idx].isDeleted = true
                allFences[idx].updatedAt = Date()
            }
        }
        GeoFence.saveAll(allFences)
        geofences = GeoFence.loadAll()
        LocationService.shared.updateGeofences(geofences)
    }

    func updateGeofence(_ fence: GeoFence) {
        var allFences = GeoFence.loadAllIncludingDeleted()
        if let idx = allFences.firstIndex(where: { $0.id == fence.id }) {
            var updated = fence
            updated.updatedAt = Date()
            allFences[idx] = updated
            GeoFence.saveAll(allFences)
            geofences = GeoFence.loadAll()
            LocationService.shared.updateGeofences(geofences)
        }
    }
}
