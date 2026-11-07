import CoreMotion
import SwiftUI

/// Motion & Fitness access as the app sees it. The one place that decides the
/// status text, colour and which action (if any) the Motion settings offer, so
/// the Settings row and the detail screen never disagree.
enum MotionAccessState: Equatable {
    case unavailable
    case notDetermined
    case authorized
    case denied
    case restricted

    @MainActor static var current: MotionAccessState {
        guard MotionActivityService.isAvailable else { return .unavailable }
        switch MotionActivityService.authorizationStatus {
        case .authorized:    return .authorized
        case .notDetermined: return .notDetermined
        case .restricted:    return .restricted
        case .denied:        return .denied
        @unknown default:    return .denied
        }
    }

    enum Action { case requestAccess, openSettings }

    /// Whether the Sync dashboard lists this state under Action Required:
    /// every state in which motion activity can't sync on a device that has it.
    var needsAttention: Bool {
        switch self {
        case .notDetermined, .denied, .restricted: return true
        case .authorized, .unavailable: return false
        }
    }

    /// The action that would change something in this state; nil when none can.
    var action: Action? {
        switch self {
        case .notDetermined: return .requestAccess
        case .denied:        return .openSettings
        case .authorized, .restricted, .unavailable: return nil
        }
    }

    var title: String {
        switch self {
        case .unavailable:   return "Not Available"
        case .notDetermined: return "Not Yet Allowed"
        case .authorized:    return "Syncing"
        case .denied:        return "Access Off"
        case .restricted:    return "Restricted"
        }
    }

    var detail: String {
        switch self {
        case .unavailable:
            return "This device doesn't record motion activity."
        case .notDetermined:
            return "Allow Motion & Fitness access so Health Beat can read the activity your iPhone records."
        case .authorized:
            return "Motion activity syncs with every health sync."
        case .denied:
            return "Motion & Fitness access is off for Health Beat. Turn it on in iOS Settings."
        case .restricted:
            return "Motion & Fitness access is restricted by Screen Time or a device profile."
        }
    }

    var systemImage: String {
        switch self {
        case .authorized:    return "figure.walk.motion"
        case .notDetermined: return "hand.raised.fill"
        case .denied, .restricted, .unavailable: return "figure.walk.motion.trianglebadge.exclamationmark"
        }
    }

    var color: Color {
        switch self {
        case .authorized:    return .green
        case .notDetermined: return .orange
        case .denied, .restricted: return .red
        case .unavailable:   return .secondary
        }
    }
}

struct MotionSettingsView: View {
    @Environment(\.scenePhase) private var scenePhase
    @State private var state = MotionAccessState.current
    @State private var isRequesting = false
    @State private var lastSync = MotionActivityService.lastLiveSync

    var body: some View {
        List {
            Section {
                HStack(spacing: 14) {
                    Image(systemName: state.systemImage)
                        .font(.system(size: 34))
                        .foregroundStyle(state.color)
                        .contentTransition(.symbolEffect(.replace))
                        .frame(width: 44)
                    VStack(alignment: .leading, spacing: 3) {
                        Text(state.title)
                            .font(.headline)
                            .contentTransition(.opacity)
                        Text(state.detail)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                .padding(.vertical, 4)
                .animation(.spring(duration: 0.35), value: state)

                if let action = state.action {
                    actionButton(action)
                }

                if state == .authorized, let lastSync {
                    LabeledContent("Last synced") {
                        Text(lastSync, style: .relative)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }

            Section("What Is Recorded") {
                Text("Your iPhone classifies its motion all the time: still, walking, running, cycling or in a vehicle, each with a confidence. Health Beat stores each change with its start and end time in MySQL and Executive Assistant.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Text("It can't tell sitting, standing and lying down apart — iOS doesn't record posture.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section("Seven-Day Limit") {
                Text("iOS keeps only the last 7 days of motion activity. Health Beat syncs it in the background with your health data; if it doesn't run for more than a week, the oldest days are gone for good. A full sync can only re-send those 7 days.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .navigationTitle("Motion Activity")
        .onAppear(perform: refresh)
        .onChange(of: scenePhase) { _, phase in
            if phase == .active { refresh() }
        }
        .sensoryFeedback(.success, trigger: state) { old, new in
            old != .authorized && new == .authorized
        }
    }

    @ViewBuilder
    private func actionButton(_ action: MotionAccessState.Action) -> some View {
        switch action {
        case .requestAccess:
            Button {
                Task { await requestAccess() }
            } label: {
                HStack(spacing: 8) {
                    if isRequesting {
                        ProgressView().controlSize(.small)
                    } else {
                        Image(systemName: "checkmark.shield.fill")
                    }
                    Text(isRequesting ? "Waiting for your answer…" : "Allow Motion Access")
                }
                .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .disabled(isRequesting)
        case .openSettings:
            Button {
                if let url = URL(string: UIApplication.openSettingsURLString) {
                    UIApplication.shared.open(url)
                }
            } label: {
                Label("Open iOS Settings", systemImage: "gear")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
        }
    }

    private func requestAccess() async {
        isRequesting = true
        _ = await MotionActivityService.shared.requestAccess()
        isRequesting = false
        withAnimation(.spring(duration: 0.35)) { refresh() }
    }

    private func refresh() {
        state = MotionAccessState.current
        lastSync = MotionActivityService.lastLiveSync
    }
}
