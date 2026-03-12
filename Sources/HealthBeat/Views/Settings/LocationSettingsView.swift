import CoreLocation
import MapKit
import SwiftUI

struct LocationSettingsView: View {
    @StateObject private var vm = LocationViewModel()
    @ObservedObject private var iCloud = iCloudSyncService.shared

    var body: some View {
        List {
            if !iCloud.isCurrentDeviceActiveForAutoSync {
                Section {
                    HStack(spacing: 12) {
                        Image(systemName: "location.slash.fill")
                            .foregroundStyle(.orange)
                            .frame(width: 24)
                        VStack(alignment: .leading, spacing: 3) {
                            Text("Reporting Inactive on This Device")
                                .font(.subheadline.weight(.semibold))
                            Text("Location events are only reported to MySQL by \(iCloud.activeDeviceName ?? "the active device"). Go to iCloud Sync settings to make this device active.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .padding(.vertical, 4)
                }
            }

            Section("Background Location") {
                Toggle("Track Location", isOn: Binding(
                    get: { vm.config.trackingEnabled },
                    set: { _ in vm.toggleTracking() }
                ))
                .disabled(!iCloud.isCurrentDeviceActiveForAutoSync)

                LabeledContent("Permission") {
                    Text(authorizationLabel)
                        .foregroundStyle(authorizationColor)
                        .font(.caption)
                }
            }

            if vm.authorizationStatus != .authorizedAlways {
                Section {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("\"Always\" permission required")
                            .font(.subheadline.weight(.semibold))
                        Text("Background location tracking and geofence detection require \"Always Allow\" location access. Please update this in Settings.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        HStack(spacing: 12) {
                            Button("Open Settings") {
                                if let url = URL(string: UIApplication.openSettingsURLString) {
                                    UIApplication.shared.open(url)
                                }
                            }
                            .font(.caption.weight(.semibold))

                            if vm.authorizationStatus == .notDetermined || vm.authorizationStatus == .authorizedWhenInUse {
                                Button("Request Always") {
                                    LocationService.shared.requestAlwaysFromUI()
                                }
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.blue)
                            }
                        }
                    }
                    .padding(.vertical, 4)
                }
            }

            Section {
                ForEach(vm.geofences) { fence in
                    NavigationLink {
                        GeoFenceEditView(mode: .edit(fence), vm: vm)
                    } label: {
                        HStack(spacing: 12) {
                            // Small map thumbnail
                            GeoFenceMapPreview(
                                latitude: fence.latitude,
                                longitude: fence.longitude,
                                radius: fence.radius
                            )
                            .frame(width: 56, height: 56)
                            .clipShape(RoundedRectangle(cornerRadius: 8))

                            VStack(alignment: .leading, spacing: 2) {
                                Text(fence.name)
                                    .font(.subheadline)
                                HStack(spacing: 4) {
                                    Image(systemName: fence.placeCategoryIcon)
                                        .font(.caption2)
                                    Text(fence.placeCategoryName)
                                        .font(.caption)
                                    Text("·")
                                        .font(.caption)
                                    Text("\(Int(fence.radius)) m")
                                        .font(.caption)
                                }
                                .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
                .onDelete { offsets in
                    vm.deleteGeofence(at: offsets)
                }

                NavigationLink {
                    GeoFenceEditView(mode: .add, vm: vm)
                } label: {
                    Label("Add Place", systemImage: "plus.circle.fill")
                        .foregroundStyle(.blue)
                }
            } header: {
                Text("Places")
            } footer: {
                Text("Places trigger arrival and departure events logged to your MySQL database.")
                    .font(.caption)
            }
        }
        .navigationTitle("Location & Places")
        .navigationBarTitleDisplayMode(.inline)
    }

    private var authorizationLabel: String {
        switch vm.authorizationStatus {
        case .notDetermined:      return "Not requested"
        case .denied:             return "Denied"
        case .restricted:         return "Restricted"
        case .authorizedWhenInUse: return "When In Use"
        case .authorizedAlways:   return "Always"
        @unknown default:         return "Unknown"
        }
    }

    private var authorizationColor: Color {
        switch vm.authorizationStatus {
        case .authorizedAlways:    return .green
        case .authorizedWhenInUse: return .orange
        default:                   return .red
        }
    }
}
