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
    }

    func toggleTracking() {
        config.trackingEnabled.toggle()
        config.save()
        LocationService.shared.applyConfig(config)
    }

    func addGeofence(_ fence: GeoFence) {
        geofences.append(fence)
        GeoFence.saveAll(geofences)
        LocationService.shared.updateGeofences(geofences)
    }

    func deleteGeofence(at offsets: IndexSet) {
        geofences.remove(atOffsets: offsets)
        GeoFence.saveAll(geofences)
        LocationService.shared.updateGeofences(geofences)
    }

    func updateGeofence(_ fence: GeoFence) {
        if let idx = geofences.firstIndex(where: { $0.id == fence.id }) {
            geofences[idx] = fence
            GeoFence.saveAll(geofences)
            LocationService.shared.updateGeofences(geofences)
        }
    }
}
