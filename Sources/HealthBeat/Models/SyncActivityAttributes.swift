import ActivityKit
import Foundation

struct SyncActivityAttributes: ActivityAttributes {
    struct ContentState: Codable, Hashable {
        var phase: String          // e.g. "Activity", "Vitals"
        var operation: String      // current operation text
        var recordsInserted: Int   // cumulative records synced so far
        var isFullSync: Bool
        var progress: Double = 0   // 0…1 overall progress across the whole sync
    }
}
