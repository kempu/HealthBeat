import SwiftUI
import UIKit

struct SyncDashboardView: View {
    @ObservedObject var vm: SyncViewModel
    @Binding var showReminderSync: Bool
    @ObservedObject private var iCloud = iCloudSyncService.shared
    @State private var navigateToHealthPermissions = false
    @State private var showSyncCompletedBanner = false

    var body: some View {
        NavigationStack {
            List {
                // Header section
                Section {
                    VStack(spacing: 16) {
                        statusHeader
                        syncButtons
                        if vm.isAnySyncRunning || vm.fullSyncPhase != .idle {
                            overallProgress
                        }
                        if vm.fullSyncPhase != .idle && !vm.fullSyncSteps.isEmpty {
                            fullSyncStepper
                        }
                    }
                    .padding(.vertical, 4)
                }

                // Non-active device banner
                if !iCloud.isCurrentDeviceActiveForAutoSync {
                    Section {
                        noticeBanner(
                            icon: "icloud.slash.fill",
                            color: .orange,
                            title: "Background Sync on \(iCloud.activeDeviceName ?? "Another Device")",
                            message: "Automatic and background syncing only runs on the active device. Manual syncs are available on all devices."
                        )
                    }
                }

                // Per-destination "needs full sync" banner. Both missing → first
                // baseline; one missing → that destination was enabled later or
                // its data was reset, and needs catching up while the other keeps
                // syncing incrementally.
                if vm.needsFullSync && !vm.isAnySyncRunning && vm.fullSyncPhase == .idle {
                    Section {
                        noticeBanner(
                            icon: "exclamationmark.triangle.fill",
                            color: .yellow,
                            title: (vm.mysqlNeedsFullSync && vm.eaNeedsFullSync)
                                ? "No Complete Baseline"
                                : "\(vm.fullSyncTargetsLabel) Needs a Full Sync",
                            message: (vm.mysqlNeedsFullSync && vm.eaNeedsFullSync)
                                ? "No destination has a complete baseline yet. Tap Full Sync to export all Apple Health data to \(vm.fullSyncTargetsLabel); later syncs run incrementally."
                                : "\(vm.fullSyncTargetsLabel) hasn't received a complete copy of your health data yet (it was enabled after the other destination, or its data was reset). Tap Full Sync to catch it up — the other destination keeps syncing normally and won't be re-uploaded."
                        )
                    }
                }

                // Full-sync phase guidance: screen must stay on only while reading
                // Apple Health; once exported, delivery continues in the background.
                if vm.fullSyncPhase == .exporting {
                    Section {
                        noticeBanner(
                            icon: "lock.open.display",
                            color: .blue,
                            title: "Keep Screen On",
                            message: "Reading your Apple Health data — this needs the screen unlocked, and only takes a few minutes."
                        )
                    }
                } else if vm.fullSyncPhase == .delivering && vm.eaImporting {
                    // Files are uploaded; EA is decrypting + importing server-side.
                    Section { importingBanner }
                } else if vm.fullSyncPhase == .delivering {
                    Section { deliveringBanner }
                }

                // Reminder sync banner (only during the screen-on export phase)
                if vm.isReminderSync && vm.fullSyncPhase == .exporting {
                    Section {
                        reminderSyncBanner
                    }
                }

                // Sync completed banner (after reminder sync)
                if showSyncCompletedBanner {
                    Section {
                        noticeBanner(
                            icon: "checkmark.circle.fill",
                            color: .green,
                            title: "Sync Complete",
                            message: "All health data has been synced to the database."
                        )
                    }
                }

                // Error banner
                if let err = vm.errorMessage {
                    Section {
                        noticeBanner(
                            icon: "exclamationmark.triangle.fill",
                            color: .red,
                            title: nil,
                            message: err
                        )
                    }
                }

                // EA upload retry (the encrypted dump is kept until EA confirms)
                if vm.eaUploadFailed && !vm.isAnySyncRunning {
                    Section {
                        Button {
                            vm.retryEAUpload()
                        } label: {
                            Label("Retry EA Upload", systemImage: "arrow.clockwise")
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 10)
                                .background(Color.blue, in: RoundedRectangle(cornerRadius: 10))
                                .foregroundStyle(.white)
                                .font(.subheadline.weight(.semibold))
                                .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                    }
                }

                // Prerequisite issues banner
                if !vm.prerequisiteIssues.isEmpty && !vm.isAnySyncRunning {
                    Section("Action Required") {
                        ForEach(vm.prerequisiteIssues) { issue in
                            VStack(alignment: .leading, spacing: 6) {
                                HStack(spacing: 8) {
                                    Image(systemName: "exclamationmark.circle.fill")
                                        .foregroundStyle(.orange)
                                    Text(issue.title)
                                        .font(.subheadline.weight(.semibold))
                                }
                                Text(issue.message)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                if !issue.actionLabel.isEmpty {
                                    Button(issue.actionLabel) {
                                        handlePrerequisiteAction(issue)
                                    }
                                    .font(.caption.weight(.semibold))
                                }
                            }
                            .padding(.vertical, 4)
                        }
                    }
                }

                // Category cards
                Section("Categories") {
                    ForEach(vm.categories) { cat in
                        CategoryStatusCard(
                            state: cat,
                            onReset: { vm.resetCategory(categoryID: cat.id) },
                            onSync: { vm.startCategorySync(categoryID: cat.id) },
                            isSyncRunning: vm.isAnySyncRunning
                        )
                    }
                }

                BrandFooter()
            }
            .navigationTitle("Health Beat")
            .navigationBarTitleDisplayMode(.large)
            .navigationDestination(isPresented: $navigateToHealthPermissions) {
                HealthPermissionsView(vm: SettingsViewModel())
            }
            .onAppear {
                vm.refreshRecordCounts()
                vm.checkPrerequisites()
                vm.refreshLatestHealthKitDates()
            }
            .onDisappear {
                // Safety net: SyncService owns the idle timer during sync, but make
                // sure we never leave it disabled if the screen goes away.
                UIApplication.shared.isIdleTimerDisabled = false
            }
            .onChange(of: showReminderSync) { _, shouldStart in
                if shouldStart {
                    showReminderSync = false
                    vm.startReminderSync()
                }
            }
            .onChange(of: vm.isReminderSync) { oldValue, newValue in
                if oldValue && !newValue {
                    showSyncCompletedBanner = true
                    Task {
                        try? await Task.sleep(nanoseconds: 10_000_000_000)
                        showSyncCompletedBanner = false
                    }
                }
            }
            .alert("Sync Prerequisites", isPresented: $vm.showPrerequisiteAlert) {
                Button("Continue Anyway") { }
                Button("Cancel Sync", role: .cancel) {
                    vm.cancelSync()
                }
            } message: {
                let titles = vm.prerequisiteIssues.map { $0.title }
                Text("Issues found:\n\(titles.joined(separator: "\n"))\n\nThe sync will continue but some data may be missing. Fix these issues in Settings for a complete sync.")
            }
        }
    }

    private var statusHeader: some View {
        VStack(spacing: 6) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(vm.lastSyncLabel)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                    if vm.totalRecords > 0 {
                        Text("\(vm.totalRecords.formatted()) total records in DB")
                            .font(.headline)
                            .foregroundStyle(.primary)
                    }
                }
                Spacer()
                Image("HeartRate")
                    .resizable()
                    .scaledToFit()
                    .frame(width: 40, height: 40)
                    .foregroundStyle(Color(red: 0.93, green: 0.18, blue: 0.28))
            }
            if vm.isAnySyncRunning, !vm.currentOperation.isEmpty {
                Text(vm.currentOperation)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private var syncButtons: some View {
        HStack(spacing: 12) {
            Button {
                vm.startSync()
            } label: {
                Label(vm.needsFullSync ? "Full Sync" : "Sync Now", systemImage: "arrow.clockwise.icloud.fill")
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 10)
                    .background(Color.blue, in: RoundedRectangle(cornerRadius: 10))
                    .foregroundStyle(.white)
                    .font(.subheadline.weight(.semibold))
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .disabled(vm.isAnySyncRunning)
            .opacity(vm.isAnySyncRunning ? 0.5 : 1)

            if vm.isAnySyncRunning {
                Button {
                    vm.cancelSync()
                } label: {
                    Label("Cancel", systemImage: "xmark.circle.fill")
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 10)
                        .background(Color.red.opacity(0.12), in: RoundedRectangle(cornerRadius: 10))
                        .foregroundStyle(.red)
                        .font(.subheadline.weight(.semibold))
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }
        }
    }

    // High-level full-sync steps (Export → EA → MySQL), color-coded by status.
    private var fullSyncStepper: some View {
        HStack(spacing: 6) {
            ForEach(Array(vm.fullSyncSteps.enumerated()), id: \.offset) { idx, step in
                if idx > 0 {
                    Rectangle()
                        .fill(Color.secondary.opacity(0.25))
                        .frame(height: 1)
                        .frame(maxWidth: .infinity)
                }
                HStack(spacing: 4) {
                    Image(systemName: stepIcon(step.status))
                        .font(.caption2)
                    Text(step.label)
                        .font(.caption2.weight(.semibold))
                        .lineLimit(1)
                }
                .foregroundStyle(stepColor(step.status))
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .background(stepColor(step.status).opacity(0.12), in: Capsule())
                .fixedSize(horizontal: true, vertical: false)
            }
        }
    }

    private func stepColor(_ status: SyncStepStatus) -> Color {
        switch status {
        case .pending: return .secondary
        case .active:  return .blue
        case .done:    return .green
        case .failed:  return .red
        }
    }

    private func stepIcon(_ status: SyncStepStatus) -> String {
        switch status {
        case .pending: return "circle"
        case .active:  return "circle.dotted"
        case .done:    return "checkmark.circle.fill"
        case .failed:  return "exclamationmark.triangle.fill"
        }
    }

    private var overallProgress: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text("Overall Progress")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Text("\(Int(vm.overallProgress * 100))%")
                    .font(.caption.monospacedDigit())
                    .foregroundStyle(.secondary)
            }
            ProgressView(value: vm.overallProgress)
                .tint(.blue)
        }
    }

    private func noticeBanner(icon: String, color: Color, title: String?, message: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: icon)
                .foregroundStyle(color)
            VStack(alignment: .leading, spacing: 2) {
                if let title {
                    Text(title)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.primary)
                }
                Text(message)
                    .font(.caption)
                    .foregroundStyle(title != nil ? .secondary : color)
                    .lineLimit(3)
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(color.opacity(0.10), in: RoundedRectangle(cornerRadius: 10))
        .listRowBackground(Color.clear)
        .listRowInsets(EdgeInsets(top: 4, leading: 16, bottom: 4, trailing: 16))
    }

    /// The "uploading in the background" banner with a subtle, integrated
    /// "Restart upload" affordance (an escape hatch if the upload looks stuck) —
    /// styled as a quiet footnote link inside the same card, not a primary CTA.
    private var deliveringBanner: some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: "moon.zzz.fill")
                .foregroundStyle(.gray)
            VStack(alignment: .leading, spacing: 8) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Syncing in the Background")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.primary)
                    Text("Your data is exported — you can lock your phone now. Uploading continues in the background; keeping the app open makes it finish faster.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(3)
                }
                Button {
                    vm.retryEAUpload()
                } label: {
                    Label("Restart upload", systemImage: "arrow.clockwise")
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.blue)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.gray.opacity(0.10), in: RoundedRectangle(cornerRadius: 10))
        .listRowBackground(Color.clear)
        .listRowInsets(EdgeInsets(top: 4, leading: 16, bottom: 4, trailing: 16))
    }

    /// Server-side import banner with LIVE row progress, so a long import (millions
    /// of rows) is visibly advancing instead of a static "Importing…" that looks hung.
    private var importingBanner: some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: "server.rack")
                .foregroundStyle(.blue)
            VStack(alignment: .leading, spacing: 6) {
                Text("Importing on EA")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.primary)
                Text("EA is decrypting and importing your data on the server — you can close the app; it'll finish on its own.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
                if vm.eaImportRowsExpected > 0 {
                    ProgressView(value: vm.eaImportFraction)
                        .tint(.blue)
                    Text("\(vm.eaImportRowsImported.formatted()) of \(vm.eaImportRowsExpected.formatted()) rows")
                        .font(.caption2).foregroundStyle(.secondary).monospacedDigit()
                } else if vm.eaImportRowsImported > 0 {
                    Text("\(vm.eaImportRowsImported.formatted()) rows imported")
                        .font(.caption2).foregroundStyle(.secondary).monospacedDigit()
                }
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.blue.opacity(0.10), in: RoundedRectangle(cornerRadius: 10))
        .listRowBackground(Color.clear)
        .listRowInsets(EdgeInsets(top: 4, leading: 16, bottom: 4, trailing: 16))
    }

    private var reminderSyncBanner: some View {
        HStack(spacing: 10) {
            Image(systemName: "arrow.clockwise.icloud.fill")
                .foregroundStyle(.blue)
            VStack(alignment: .leading, spacing: 2) {
                Text("Full Sync in Progress")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.primary)
                (Text("Apple Health data is only accessible while the screen is unlocked. ")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                + Text("Keep the app in the foreground and screen unlocked until sync completes.")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary))
                    .lineLimit(4)
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.blue.opacity(0.10), in: RoundedRectangle(cornerRadius: 10))
        .listRowBackground(Color.clear)
        .listRowInsets(EdgeInsets(top: 4, leading: 16, bottom: 4, trailing: 16))
    }

    private func handlePrerequisiteAction(_ issue: SyncPrerequisiteIssue) {
        switch issue {
        case .healthPermissionsNotRequested, .somePermissionsDenied:
            navigateToHealthPermissions = true
        case .missingDatabaseTables:
            let config = MySQLConfig.load()
            Task {
                let mysql = MySQLService()
                do {
                    try await mysql.connect(config: config)
                    let (ok, errMsg) = await SchemaService.initializeSchema(mysql: mysql)
                    await mysql.disconnect()
                    if ok {
                        vm.prerequisiteIssues.removeAll { $0.id == issue.id }
                    } else {
                        vm.syncState.errorMessage = errMsg ?? "Schema initialization failed"
                    }
                } catch {
                    await mysql.disconnect()
                    vm.syncState.errorMessage = "Could not connect to MySQL: \(error.localizedDescription)"
                }
            }
        case .databaseConnectionFailed:
            break
        case .healthDataUnavailable:
            break
        }
    }
}
