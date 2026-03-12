import SwiftUI

struct SettingsView: View {
    let syncViewModel: SyncViewModel
    @StateObject private var vm = SettingsViewModel()
    @ObservedObject private var iCloud = iCloudSyncService.shared
    @AppStorage("keepScreenOnDuringSync") private var keepScreenOnDuringSync = true
    @AppStorage("backgroundSyncEnabled") private var backgroundSyncEnabled = true

    var body: some View {
        NavigationStack {
            List {
                Section {
                    NavigationLink {
                        MySQLSettingsView(vm: vm)
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("cylinder.fill", color: .orange)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("MySQL Connection")
                                    .font(.subheadline.weight(.semibold))
                                Text("\(vm.config.host):\(vm.config.port) / \(vm.config.database)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                        }
                    }

                    NavigationLink {
                        HealthPermissionsView(vm: vm)
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("heart.fill", color: .red)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Apple Health Permissions")
                                    .font(.subheadline.weight(.semibold))
                                Text(vm.permissionsRequested
                     ? (vm.deniedTypes.isEmpty ? "All permissions granted" : "\(vm.deniedTypes.count) permission(s) missing")
                     : "Tap to request permissions")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    NavigationLink {
                        LocationSettingsView()
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("location.fill", color: iCloud.isCurrentDeviceActiveForAutoSync ? .blue : .secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Location & Places")
                                    .font(.subheadline.weight(.semibold))
                                Text(locationSubtitle)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    NavigationLink {
                        PlaceCategoriesView()
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("tag.fill", color: .purple)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Place Categories")
                                    .font(.subheadline.weight(.semibold))
                                Text("\(PlaceCategory.loadAll().count) categories")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    NavigationLink {
                        BackupSettingsView()
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("externaldrive.fill", color: iCloud.isCurrentDeviceActiveForAutoSync ? .green : .secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Backup & Recovery")
                                    .font(.subheadline.weight(.semibold))
                                Text(backupSubtitle)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    NavigationLink {
                        iCloudSyncSettingsView()
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("icloud.fill", color: .cyan)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("iCloud Sync")
                                    .font(.subheadline.weight(.semibold))
                                Text(iCloud.iCloudSyncEnabled ? "Enabled" : "Disabled")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }

                Section("Data Integrity") {
                    NavigationLink {
                        DataValidationView(syncViewModel: syncViewModel)
                    } label: {
                        HStack(spacing: 12) {
                            iconBox("checkmark.shield.fill", color: .green)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Data Validation")
                                    .font(.subheadline.weight(.semibold))
                                Text("Compare Apple Health vs database")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }

                Section("About") {
                    LabeledContent("Version", value: appVersion)
                    LabeledContent("MySQL Protocol", value: "Wire Protocol v4.1")
                    LabeledContent("HealthKit Types", value: "\(HealthDataTypes.allQuantityTypes.count + HealthDataTypes.allCategoryTypes.count)")
                }

                Section("Sync Behavior") {
                    Toggle(isOn: $backgroundSyncEnabled) {
                        HStack(spacing: 12) {
                            iconBox("arrow.triangle.2.circlepath", color: backgroundSyncEnabled ? .blue : .secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Background Sync")
                                    .font(.subheadline.weight(.semibold))
                                Text(backgroundSyncEnabled
                                    ? "Health data, location, and geofence events sync automatically"
                                    : "No data is written to MySQL in the background")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    Toggle(isOn: $keepScreenOnDuringSync) {
                        HStack(spacing: 12) {
                            iconBox("sun.max.fill", color: .yellow)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Keep Screen On")
                                    .font(.subheadline.weight(.semibold))
                                Text("Prevent display sleep during full sync")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }

                BrandFooter()
            }
            .navigationTitle("Settings")
            .onAppear { vm.refreshPermissionsState() }
        }
    }

    private var locationSubtitle: String {
        if !iCloud.isCurrentDeviceActiveForAutoSync {
            let device = iCloud.activeDeviceName ?? "another device"
            return "Reporting via \(device)"
        }
        return LocationConfig.load().trackingEnabled ? "Tracking enabled" : "Tracking disabled"
    }

    private var backupSubtitle: String {
        if !iCloud.isCurrentDeviceActiveForAutoSync {
            let device = iCloud.activeDeviceName ?? "another device"
            return "Auto backup via \(device)"
        }
        let backups = BackupManager.shared.listBackups()
        let config = BackupConfig.load()
        if backups.isEmpty {
            return config.autoBackupEnabled ? "Auto enabled, no backups yet" : "No backups"
        }
        let count = backups.count
        let autoLabel = config.autoBackupEnabled ? "Auto" : "Manual"
        return "\(autoLabel) · \(count) backup\(count == 1 ? "" : "s")"
    }

    private var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }

    private func iconBox(_ systemName: String, color: Color) -> some View {
        ZStack {
            RoundedRectangle(cornerRadius: 8)
                .fill(color)
                .frame(width: 36, height: 36)
            Image(systemName: systemName)
                .font(.system(size: 18))
                .foregroundStyle(.white)
        }
    }
}
