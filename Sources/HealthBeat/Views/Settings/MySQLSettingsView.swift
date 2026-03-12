import SwiftUI

struct MySQLSettingsView: View {
    @ObservedObject var vm: SettingsViewModel
    @ObservedObject private var iCloud = iCloudSyncService.shared
    @State private var savedConfirmation = false
    @State private var showResetConfirm = false

    var body: some View {
        Form {
            if !iCloud.isCurrentDeviceActiveForAutoSync {
                Section {
                    HStack(spacing: 12) {
                        Image(systemName: "arrow.triangle.2.circlepath.circle")
                            .foregroundStyle(.orange)
                            .frame(width: 24)
                        VStack(alignment: .leading, spacing: 3) {
                            Text("Auto Sync on \(iCloud.activeDeviceName ?? "Another Device")")
                                .font(.subheadline.weight(.semibold))
                            Text("Background and automatic syncing only runs on the active device. You can still test the connection and run manual syncs from here.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .padding(.vertical, 4)
                }
            }

            Section("Connection") {
                LabeledContent("Host") {
                    TextField("192.168.1.1", text: $vm.config.host)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                        .multilineTextAlignment(.trailing)
                }
                LabeledContent("Port") {
                    TextField("3306", value: $vm.config.port, format: .number.grouping(.never))
                        .keyboardType(.numberPad)
                        .multilineTextAlignment(.trailing)
                }
                LabeledContent("Database") {
                    TextField("healthbeat", text: $vm.config.database)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .multilineTextAlignment(.trailing)
                }
            }

            Section("Credentials") {
                LabeledContent("Username") {
                    TextField("healthbeat", text: $vm.config.username)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .multilineTextAlignment(.trailing)
                }
                LabeledContent("Password") {
                    SecureField("password", text: $vm.config.password)
                        .multilineTextAlignment(.trailing)
                }
            }

            Section {
                Button {
                    vm.saveConfig()
                    savedConfirmation = true
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
                        savedConfirmation = false
                    }
                } label: {
                    HStack(spacing: 6) {
                        Spacer()
                        if savedConfirmation {
                            Image(systemName: "checkmark")
                                .font(.subheadline.weight(.semibold))
                            Text("Saved")
                                .font(.subheadline.weight(.semibold))
                        } else {
                            Text("Save Settings")
                                .font(.subheadline.weight(.semibold))
                        }
                        Spacer()
                    }
                    .foregroundStyle(savedConfirmation ? .green : .accentColor)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }

            Section("Test & Initialize") {
                Button {
                    vm.testConnection()
                } label: {
                    HStack {
                        Label("Test Connection", systemImage: "network")
                        Spacer()
                        connectionTestIndicator
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)

                Button {
                    vm.initializeSchema()
                } label: {
                    HStack {
                        Label("Initialize / Update Schema", systemImage: "tablecells.badge.ellipsis")
                        Spacer()
                        schemaInitIndicator
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }

            Section {
                Button(role: .destructive) {
                    showResetConfirm = true
                } label: {
                    HStack {
                        Label("Reset Database", systemImage: "trash.fill")
                        Spacer()
                        resetDatabaseIndicator
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .disabled(vm.resetDatabaseState == .testing)
            } header: {
                Text("Danger Zone")
            } footer: {
                Text("Permanently deletes all health records from MySQL. The database schema is preserved. This cannot be undone.")
            }
            .confirmationDialog(
                "Reset Database?",
                isPresented: $showResetConfirm,
                titleVisibility: .visible
            ) {
                Button("Delete All Health Records", role: .destructive) {
                    vm.resetDatabase()
                }
            } message: {
                Text("This permanently deletes all health records from MySQL. The schema and tables are preserved but all data will be gone. This cannot be undone.")
            }

            // Auth tip
            Section {
                VStack(alignment: .leading, spacing: 6) {
                    Text("MySQL 8.0 Note")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text("If connection fails with caching_sha2_password, run on your MySQL server:\nALTER USER 'user'@'%' IDENTIFIED WITH mysql_native_password BY 'password';")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .navigationTitle("MySQL Settings")
    }

    @ViewBuilder
    private var connectionTestIndicator: some View {
        switch vm.connectionTestState {
        case .idle:
            EmptyView()
        case .testing:
            ProgressView().scaleEffect(0.7)
        case .success(let msg):
            Text(msg).font(.caption).foregroundStyle(.green).lineLimit(2)
        case .failure(let msg):
            Text(msg).font(.caption).foregroundStyle(.red).lineLimit(2)
        }
    }

    @ViewBuilder
    private var resetDatabaseIndicator: some View {
        switch vm.resetDatabaseState {
        case .idle:
            EmptyView()
        case .testing:
            ProgressView().scaleEffect(0.7)
        case .success(let msg):
            Text(msg).font(.caption).foregroundStyle(.green).lineLimit(2)
        case .failure(let msg):
            Text(msg).font(.caption).foregroundStyle(.red).lineLimit(2)
        }
    }

    @ViewBuilder
    private var schemaInitIndicator: some View {
        switch vm.schemaInitState {
        case .idle:
            EmptyView()
        case .testing:
            ProgressView().scaleEffect(0.7)
        case .success(let msg):
            Text(msg).font(.caption).foregroundStyle(.green).lineLimit(2)
        case .failure(let msg):
            Text(msg).font(.caption).foregroundStyle(.red).lineLimit(2)
        }
    }
}
