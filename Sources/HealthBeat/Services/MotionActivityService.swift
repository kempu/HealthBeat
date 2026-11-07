import CoreMotion
import CryptoKit
import Foundation
import Security

/// Reads the motion-activity history iOS keeps on the device (CoreMotion), and
/// turns it into `HBMotionActivityRow`s for MySQL + EA.
///
/// CoreMotion classifies the phone's motion continuously into stationary /
/// walking / running / cycling / automotive / unknown (several flags can be set
/// at once, e.g. automotive + stationary at a red light), each with a confidence.
/// None of it reaches Apple Health. iOS keeps only the **last 7 days** of it, so
/// a sync must run at least once a week or the oldest activities are lost for
/// good — and a destination can never be rebuilt beyond those 7 days. That is
/// why the full-sync dump carries this table as merge-only (never truncated).
///
/// One row per CoreMotion activity; it lasts until the next activity starts, so
/// the newest row is open (`end_date` NULL) until a later pass closes it. Row
/// ids are derived from this install's device id + the activity's start, so
/// re-reading an overlapping window upserts the same rows instead of adding new
/// ones.
@MainActor
final class MotionActivityService {

    static let shared = MotionActivityService()

    private let manager = CMMotionActivityManager()
    private let queue: OperationQueue = {
        let q = OperationQueue()
        q.name = "healthbeat.motion-activity"
        q.maxConcurrentOperationCount = 1
        return q
    }()

    private init() {}

    /// How far back iOS keeps motion activity.
    static let historyWindow: TimeInterval = 7 * 24 * 3600

    /// Earliest start a history query can usefully ask for. A minute inside the
    /// window so the oldest edge is never asked for data iOS just aged out.
    static var earliestQueryable: Date { Date().addingTimeInterval(-historyWindow + 60) }

    static var isAvailable: Bool { CMMotionActivityManager.isActivityAvailable() }
    static var authorizationStatus: CMAuthorizationStatus { CMMotionActivityManager.authorizationStatus() }
    static var isAuthorized: Bool { isAvailable && authorizationStatus == .authorized }

    /// Throws the reason motion activity can't be read right now, if any.
    static func ensureReadable() throws {
        guard isAvailable else { throw MotionActivityError.notAvailable }
        switch authorizationStatus {
        case .authorized:    return
        case .notDetermined: throw MotionActivityError.notDetermined
        case .denied:        throw MotionActivityError.denied
        case .restricted:    throw MotionActivityError.restricted
        @unknown default:    throw MotionActivityError.denied
        }
    }

    /// Shows the Motion & Fitness permission prompt (iOS shows it on the first
    /// activity query) and returns the status the user left it in.
    func requestAccess() async -> CMAuthorizationStatus {
        guard Self.isAvailable else { return Self.authorizationStatus }
        let now = Date()
        _ = try? await query(from: now.addingTimeInterval(-60), to: now)
        return Self.authorizationStatus
    }

    /// Every activity iOS recorded in `[from, to]`, oldest first.
    func activities(from: Date, to: Date) async throws -> [CMMotionActivity] {
        try Self.ensureReadable()
        return try await query(from: from, to: to).sorted { $0.startDate < $1.startDate }
    }

    private func query(from: Date, to: Date) async throws -> [CMMotionActivity] {
        try await withCheckedThrowingContinuation { cont in
            manager.queryActivityStarting(from: from, to: to, to: queue) { activities, error in
                if let error {
                    cont.resume(throwing: MotionActivityError.from(error))
                } else {
                    cont.resume(returning: activities ?? [])
                }
            }
        }
    }

    // MARK: - Rows

    /// Builds one row per activity. Activities that share a start time (CoreMotion
    /// re-reporting the same moment) collapse to the last one reported, so every
    /// row id is unique and each row ends where the next one starts.
    static func rows(from activities: [CMMotionActivity], deviceID: String, deviceName: String,
                     formatDate: (Date) -> String) -> [HBMotionActivityRow] {
        var byStart: [CMMotionActivity] = []
        for a in activities {
            if let last = byStart.last, last.startDate == a.startDate {
                byStart[byStart.count - 1] = a
            } else {
                byStart.append(a)
            }
        }
        var rows: [HBMotionActivityRow] = []
        rows.reserveCapacity(byStart.count)
        for (i, a) in byStart.enumerated() {
            let end: Date? = i + 1 < byStart.count ? byStart[i + 1].startDate : nil
            rows.append(HBMotionActivityRow(
                uuid: rowID(deviceID: deviceID, start: a.startDate),
                activity: primaryActivity(a),
                is_stationary: a.stationary ? 1 : 0,
                is_walking: a.walking ? 1 : 0,
                is_running: a.running ? 1 : 0,
                is_automotive: a.automotive ? 1 : 0,
                is_cycling: a.cycling ? 1 : 0,
                is_unknown: a.unknown ? 1 : 0,
                confidence: confidenceLabel(a.confidence),
                start_date: formatDate(a.startDate),
                end_date: end.map(formatDate),
                duration_seconds: end.map { $0.timeIntervalSince(a.startDate) },
                device_name: deviceName
            ))
        }
        return rows
    }

    /// The one label a row is filed under when several flags are set. Being in a
    /// vehicle wins over standing still in it; any movement wins over stationary.
    static func primaryActivity(_ a: CMMotionActivity) -> String {
        if a.automotive { return "automotive" }
        if a.cycling    { return "cycling" }
        if a.running    { return "running" }
        if a.walking    { return "walking" }
        if a.stationary { return "stationary" }
        return "unknown"
    }

    static func confidenceLabel(_ c: CMMotionActivityConfidence) -> String {
        switch c {
        case .low:    return "low"
        case .medium: return "medium"
        case .high:   return "high"
        @unknown default: return "low"
        }
    }

    /// Deterministic UUID-shaped id: SHA-256 of device id + start (ms), first 16
    /// bytes, formatted 8-4-4-4-12. Same activity → same id on every re-read.
    static func rowID(deviceID: String, start: Date) -> String {
        let ms = Int64((start.timeIntervalSince1970 * 1000).rounded())
        let digest = SHA256.hash(data: Data("hb-motion|\(deviceID)|\(ms)".utf8))
        let hex = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
        let c = Array(hex)
        return "\(String(c[0..<8]))-\(String(c[8..<12]))-\(String(c[12..<16]))-\(String(c[16..<20]))-\(String(c[20..<32]))"
    }

    // MARK: - Device identity

    private static let deviceIDService = "ee.klemens.healthbeat.motion-device-id"

    /// A random id for this device, kept in the Keychain so a reinstall keeps it
    /// (and with it the row ids of activities already synced). ThisDeviceOnly: a
    /// new phone restored from a backup records its own motion under its own id.
    static func deviceID() throws -> String {
        let base: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: deviceIDService,
            kSecAttrAccount as String: "device",
        ]
        var query = base
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var out: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &out)
        if status == errSecSuccess, let data = out as? Data, let id = String(data: data, encoding: .utf8), !id.isEmpty {
            return id
        }
        guard status == errSecItemNotFound else { throw keychainError(status) }

        let id = UUID().uuidString.lowercased()
        var add = base
        add[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        add[kSecValueData as String] = Data(id.utf8)
        let addStatus = SecItemAdd(add as CFDictionary, nil)
        guard addStatus == errSecSuccess else { throw keychainError(addStatus) }
        return id
    }

    private static func keychainError(_ status: OSStatus) -> NSError {
        let msg = SecCopyErrorMessageString(status, nil) as String? ?? "Keychain error \(status)"
        return NSError(domain: NSOSStatusErrorDomain, code: Int(status),
                       userInfo: [NSLocalizedDescriptionKey: "Motion device id: \(msg)"])
    }

    // MARK: - Live-sync anchor

    /// Start of the newest activity already written to the live destinations.
    /// The next live pass re-reads from here, so that open activity gets its end
    /// time once the following one is known. Device-local on purpose (another
    /// device's progress says nothing about this phone's motion history).
    private static let anchorKey = "motionActivity.liveAnchor"

    static var liveAnchor: Date? {
        get { UserDefaults.standard.object(forKey: anchorKey) as? Date }
        set { UserDefaults.standard.set(newValue, forKey: anchorKey) }
    }

    /// When a live pass last finished writing motion activity. Lets observer-
    /// triggered syncs carry motion along at most every few minutes.
    private static let lastLiveSyncKey = "motionActivity.lastLiveSyncAt"

    static var lastLiveSync: Date? {
        get { UserDefaults.standard.object(forKey: lastLiveSyncKey) as? Date }
        set { UserDefaults.standard.set(newValue, forKey: lastLiveSyncKey) }
    }

    /// Start of the window the next live pass reads.
    static func liveQueryStart() -> Date {
        max(liveAnchor ?? .distantPast, earliestQueryable)
    }
}

enum MotionActivityError: LocalizedError {
    case notAvailable
    case notDetermined
    case denied
    case restricted
    case query(String)

    var errorDescription: String? {
        switch self {
        case .notAvailable:
            return "This device doesn't record motion activity."
        case .notDetermined:
            return "Motion & Fitness access hasn't been allowed yet. Tap Allow Motion Access on the Sync screen."
        case .denied:
            return "Motion & Fitness access is off for Health Beat. Turn it on in iOS Settings → Privacy & Security → Motion & Fitness."
        case .restricted:
            return "Motion & Fitness access is restricted on this device (Screen Time or a device profile)."
        case .query(let message):
            return "Reading motion activity failed: \(message)"
        }
    }

    /// Access is missing (or the device can't record motion): a setup issue the
    /// dashboard surfaces with its fix, not a failed sync.
    var isAccessIssue: Bool {
        switch self {
        case .notAvailable, .notDetermined, .denied, .restricted: return true
        case .query: return false
        }
    }

    static func from(_ error: Error) -> MotionActivityError {
        let ns = error as NSError
        if ns.domain == CMErrorDomain {
            switch ns.code {
            case Int(CMErrorMotionActivityNotAuthorized.rawValue): return .denied
            case Int(CMErrorMotionActivityNotAvailable.rawValue):  return .notAvailable
            default: break
            }
        }
        return .query(error.localizedDescription)
    }
}
